"""eastmoney 免费数据源插件单元测试。

全部通过 mock client 高层方法 (kline_series / snapshot_all / f10) 完成, 不触网。
覆盖: 符号转换、日K归一化(手→股/区间过滤/停牌过滤)、除权因子口径、
分钟K解析、实时快照字段契约、instruments 形状、五张财务表映射、插件注册。
"""
from __future__ import annotations

from datetime import datetime

import httpx
import polars as pl
import pytest

from app.plugins.eastmoney import client as em_client
from app.plugins.eastmoney.client import (
    code_to_exchange,
    symbol_to_f10_code,
    symbol_to_secid,
)
from app.plugins.eastmoney.provider import EastMoneyProvider, availability

# ---------- 样本数据 ----------

def _daily_rows() -> list[str]:
    return [
        "2026-08-24,100.00,102.00,103.00,99.00,1000,10200000.00,4.00,2.00,2.00,1.00",
        "2026-08-25,102.00,101.00,104.00,100.00,800,8080000.00,3.90,-0.98,-1.00,0.80",
        "2026-08-26,101.00,103.00,105.00,100.50,1200,12460000.00,4.45,1.98,2.00,1.20",
    ]


def _minute_rows() -> list[str]:
    return [
        "2026-08-26 09:31,10.00,10.10,10.20,9.90,500,505000.00,3.0,1.0,0.10,0.5",
        "2026-08-26 09:32,10.10,10.05,10.15,10.00,300,303000.00,1.5,-0.5,-0.05,0.3",
    ]


def _snapshot_rows() -> list[dict]:
    return [
        {"f12": "600519", "f14": "贵州茅台", "f2": 1300.0, "f3": 1.5, "f4": 19.2,
         "f5": 100, "f6": 1.3e9, "f7": 2.0, "f8": 0.5,
         "f15": 1310.0, "f16": 1290.0, "f17": 1295.0, "f18": 1280.8},
        {"f12": "000001", "f14": "平安银行", "f2": 10.0, "f3": -2.0, "f4": -0.2,
         "f5": 200, "f6": 2.0e8, "f7": None, "f8": None,
         "f15": 10.2, "f16": 9.9, "f17": 10.1, "f18": 10.2},
        {"f12": "830799", "f14": "艾融软件", "f2": 15.0, "f3": 0.0, "f4": 0.0,
         "f5": 50, "f6": 7.5e5, "f7": 1.0, "f8": 0.2,
         "f15": 15.2, "f16": 14.8, "f17": 15.0, "f18": 15.0},
    ]


def _zyzb_rows() -> list[dict]:
    return [
        {
            "REPORT_DATE": "2026-06-30 00:00:00", "NOTICE_DATE": "2026-08-15 00:00:00",
            "EPSJB": 35.57, "EPSXS": 35.57, "BPS": 200.98, "MGJYXJJE": 56.5,
            "ROEJQ": 16.75, "ZZCJLL": 15.02, "XSMLL": 89.5, "XSJLL": 50.7,
            "ZCFZL": 12.1, "TOTALOPERATEREVETZ": 1.3, "PARENTNETPROFITTZ": -1.95,
            "CHZZL": 0.07,
        },
        {
            "REPORT_DATE": "2026-03-31 00:00:00", "NOTICE_DATE": "2026-04-25 00:00:00",
            "EPSJB": 21.38, "EPSXS": 21.38, "BPS": 195.35, "MGJYXJJE": 30.1,
            "ROEJQ": 10.2, "ZZCJLL": 9.1, "XSMLL": 88.9, "XSJLL": 49.5,
            "ZCFZL": 11.9, "TOTALOPERATEREVETZ": 2.5, "PARENTNETPROFITTZ": 0.8,
            "CHZZL": 0.04,
        },
    ]


def _statement_rows() -> list[dict]:
    return [
        {
            "REPORT_DATE": "2026-06-30 00:00:00", "NOTICE_DATE": "2026-08-15 00:00:00",
            "TOTAL_OPERATE_INCOME": 92278072083.21, "OPERATE_COST": 81229498398.6,
            "OPERATE_PROFIT": 6543, "SALE_EXPENSE": 100, "MANAGE_EXPENSE": 200,
            "RESEARCH_EXPENSE": 50, "FINANCE_EXPENSE": -30,
            "NONBUSINESS_INCOME": 10, "NONBUSINESS_EXPENSE": 5,
            "TOTAL_PROFIT": 5500, "INCOME_TAX": 1000, "NETPROFIT": 44516880421.86,
            "PARENT_NETPROFIT": 44516880421.86, "DEDUCT_PARENT_NETPROFIT": 44464207646.01,
            "BASIC_EPS": 35.57, "DILUTED_EPS": 35.57,
            "TOTAL_ASSETS": 123, "TOTAL_CURRENT_ASSETS": 45, "TOTAL_NONCURRENT_ASSETS": 78,
            "MONETARYFUNDS": 20, "ACCOUNTS_RECE": 1, "INVENTORY": 2, "FIXED_ASSET": 3,
            "INTANGIBLE_ASSET": 4, "GOODWILL": 0, "TOTAL_LIABILITIES": 30,
            "TOTAL_CURRENT_LIAB": 20, "TOTAL_NONCURRENT_LIAB": 10, "SHORT_LOAN": 5,
            "LONG_LOAN": 3, "ACCOUNTS_PAYABLE": 6, "TOTAL_EQUITY": 93,
            "TOTAL_PARENT_EQUITY": 90, "UNASSIGN_RPOFIT": 50, "MINORITY_EQUITY": 3,
            "NETCASH_OPERATE": 56.5, "NETCASH_INVEST": -10, "NETCASH_FINANCE": -30,
            "CONSTRUCT_LONG_ASSET": 5, "CCE_ADD": 16.5,
        },
    ]


# ---------- 符号转换 ----------

def test_symbol_conversions() -> None:
    assert symbol_to_secid("600519.SH") == "1.600519"
    assert symbol_to_secid("000001.SZ") == "0.000001"
    assert symbol_to_secid("830799.BJ") == "0.830799"
    assert symbol_to_secid("688001") == "1.688001"
    assert symbol_to_secid("300750") == "0.300750"
    assert code_to_exchange("600519") == "SH"
    assert code_to_exchange("000001") == "SZ"
    assert code_to_exchange("830799") == "BJ"
    assert code_to_exchange("920002") == "BJ"
    assert symbol_to_f10_code("600519.SH") == "SH600519"
    assert symbol_to_f10_code("000001.SZ") == "SZ000001"
    assert symbol_to_f10_code("830799.BJ") == "BJ830799"


def test_availability_ok() -> None:
    ok, reason = availability()
    assert ok is True
    assert reason == "ok"


# ---------- daily ----------

def test_daily_parsing_units_and_range(monkeypatch) -> None:
    calls: list[tuple] = []

    def fake_series(self, secid, klt, fqt, lmt=None):
        calls.append((secid, klt, fqt, lmt))
        return _daily_rows()

    monkeypatch.setattr(em_client.EastMoneyClient, "kline_series", fake_series)
    prov = EastMoneyProvider()
    try:
        df = prov.get_daily(
            ["600519.SH", "000001.SZ"],
            start_time=datetime(2026, 8, 25),
            end_time=datetime(2026, 8, 26),
        )
    finally:
        prov.close()

    assert sorted(calls) == [
        ("0.000001", 101, 0, 30), ("1.600519", 101, 0, 30),
    ]
    # 每标的取 25~26 两个交易日, volume 手→股 x100
    assert df.height == 4
    assert set(df.columns) == {"symbol", "date", "open", "high", "low", "close", "volume", "amount"}
    row = df.filter(pl.col("symbol") == "600519.SH").sort("date").row(0, named=True)
    assert row["date"].isoformat() == "2026-08-25"
    assert row["volume"] == 800 * 100
    assert row["open"] == 102.0 and row["close"] == 101.0
    assert row["amount"] == 8080000.0


def test_daily_filters_halt_days(monkeypatch) -> None:
    rows = [
        "2026-08-24,0.00,102.00,0.00,0.00,0,0.00,0.00,0.00,0.00,0.00",  # 停牌
        "2026-08-25,102.00,101.00,104.00,100.00,800,8080000.00,3.90,-0.98,-1.00,0.80",
    ]
    monkeypatch.setattr(
        em_client.EastMoneyClient, "kline_series", lambda self, secid, klt, fqt, lmt=None: rows
    )
    prov = EastMoneyProvider()
    try:
        df = prov.get_daily(["600519.SH"], None, None)
    finally:
        prov.close()
    assert df.height == 1
    assert df["date"][0].isoformat() == "2026-08-25"


def test_daily_empty_on_failure(monkeypatch) -> None:
    def boom(self, secid, klt, fqt, lmt=None):
        raise em_client.EastMoneyError("down")

    monkeypatch.setattr(em_client.EastMoneyClient, "kline_series", boom)
    prov = EastMoneyProvider()
    try:
        df = prov.get_daily(["600519.SH"], None, None)
    finally:
        prov.close()
    assert df.is_empty()


# ---------- adj_factor ----------

def test_adj_factor_is_qfq_over_raw(monkeypatch) -> None:
    raw = [
        "2026-08-24,100.00,100.00,100.00,100.00,100,1000000.00,0,0,0,0",
        "2026-08-25,200.00,200.00,200.00,200.00,100,1000000.00,0,0,0,0",
    ]
    qfq = [
        "2026-08-24,50.00,50.00,50.00,50.00,100,1000000.00,0,0,0,0",
        "2026-08-25,200.00,200.00,200.00,200.00,100,1000000.00,0,0,0,0",
    ]

    def fake_series(self, secid, klt, fqt, lmt=None):
        return qfq if fqt == 1 else raw

    monkeypatch.setattr(em_client.EastMoneyClient, "kline_series", fake_series)
    prov = EastMoneyProvider()
    try:
        df = prov.get_adj_factors(["600519.SH"], None, None)
    finally:
        prov.close()

    assert set(df.columns) == {"symbol", "trade_date", "ex_factor"}
    assert df.height == 2
    factors = {r["trade_date"].isoformat(): r["ex_factor"] for r in df.to_dicts()}
    assert factors["2026-08-24"] == 0.5
    assert factors["2026-08-25"] == 1.0


# ---------- minute ----------

def test_minute_parsing_and_klt_map(monkeypatch) -> None:
    calls: list[tuple] = []
    rows_map = {"1.600519": _minute_rows()}

    def fake_series(self, secid, klt, fqt, lmt=None):
        calls.append((secid, klt, fqt, lmt))
        return rows_map.get(secid, [])

    monkeypatch.setattr(em_client.EastMoneyClient, "kline_series", fake_series)
    prov = EastMoneyProvider()
    try:
        df = prov.get_minute(["600519.SH"], None, None, freq="5m")
    finally:
        prov.close()

    assert calls == [("1.600519", 5, 0, None)]
    assert df.height == 2
    assert set(df.columns) == {"symbol", "datetime", "open", "high", "low", "close", "volume", "amount"}
    row = df.sort("datetime").row(0, named=True)
    assert row["datetime"] == datetime(2026, 8, 26, 9, 31)
    assert row["volume"] == 500 * 100
    assert row["close"] == 10.10


# ---------- realtime ----------

def test_realtime_record_contract(monkeypatch) -> None:
    monkeypatch.setattr(
        em_client.EastMoneyClient, "snapshot_all", lambda self: (_snapshot_rows(), False)
    )
    prov = EastMoneyProvider()
    try:
        records = prov.get_realtime()
    finally:
        prov.close()

    assert len(records) == 3
    by_sym = {r["symbol"]: r for r in records}
    assert set(by_sym) == {"600519.SH", "000001.SZ", "830799.BJ"}
    r = by_sym["600519.SH"]
    assert r["name"] == "贵州茅台"
    assert r["last_price"] == 1300.0
    assert r["prev_close"] == 1280.8
    assert r["volume"] == 100 * 100
    assert r["amount"] == 1.3e9
    assert r["change_pct"] == 0.015       # 1.5% → 小数制
    assert r["change_amount"] == 19.2
    assert r["amplitude"] == 0.02         # 2.0% → 小数制
    assert r["turnover_rate"] == 0.005    # 0.5% → 小数制 (quote_service 内部再 x100)
    assert isinstance(r["timestamp"], int)
    assert r["session"] == "regular"
    # 缺失振幅/换手 → None, 不报错
    assert by_sym["000001.SZ"]["amplitude"] is None


def test_realtime_delayed_session(monkeypatch) -> None:
    """降级到延时行情源时, session 标注 delayed, 供前端提示。"""
    monkeypatch.setattr(
        em_client.EastMoneyClient, "snapshot_all", lambda self: (_snapshot_rows(), True)
    )
    prov = EastMoneyProvider()
    try:
        records = prov.get_realtime()
    finally:
        prov.close()

    assert len(records) == 3
    assert all(r["session"] == "delayed" for r in records)


# ---------- realtime + 指数快照 (ulist 多标的) ----------

def _index_snapshot_rows() -> list[dict]:
    return [
        {"f12": "000001", "f14": "上证指数", "f2": 3986.3, "f3": 0.86, "f4": 34.12,
         "f5": 576656606, "f6": 1.014e12, "f7": 1.51, "f8": None,
         "f15": 3986.3, "f16": 3926.5, "f17": 3926.53, "f18": 3952.18},
    ]


def test_realtime_with_index_symbols(monkeypatch) -> None:
    """指数记录并入实时行情, symbol 从请求列表回填(000001 是上证指数而非深市股票)。"""
    monkeypatch.setattr(
        em_client.EastMoneyClient, "snapshot_all", lambda self: (_snapshot_rows(), False)
    )
    monkeypatch.setattr(
        em_client.EastMoneyClient, "index_snapshot", lambda self, symbols: (_index_snapshot_rows(), False)
    )
    prov = EastMoneyProvider()
    try:
        records = prov.get_realtime(index_symbols=["000001.SH"])
    finally:
        prov.close()

    assert len(records) == 4
    idx = [r for r in records if r["symbol"] == "000001.SH"]
    assert len(idx) == 1
    r = idx[0]
    assert r["name"] == "上证指数"
    assert r["last_price"] == 3986.3
    assert r["prev_close"] == 3952.18
    assert r["change_pct"] == 0.0086   # 0.86% → 小数制
    assert r["amplitude"] == 0.0151    # 1.51% → 小数制
    assert r["volume"] == 576656606 * 100
    assert r["session"] == "regular"


def test_realtime_index_delayed_session(monkeypatch) -> None:
    """指数快照降级到延时源时, 指数记录 session 同样标注 delayed。"""
    monkeypatch.setattr(
        em_client.EastMoneyClient, "snapshot_all", lambda self: (_snapshot_rows(), False)
    )
    monkeypatch.setattr(
        em_client.EastMoneyClient, "index_snapshot", lambda self, symbols: (_index_snapshot_rows(), True)
    )
    prov = EastMoneyProvider()
    try:
        records = prov.get_realtime(index_symbols=["000001.SH"])
    finally:
        prov.close()

    idx = [r for r in records if r["symbol"] == "000001.SH"]
    assert idx and idx[0]["session"] == "delayed"


def test_realtime_index_failure_keeps_stocks(monkeypatch) -> None:
    """指数快照失败不影响股票记录返回。"""

    def boom(self, symbols):
        raise em_client.EastMoneyError("ulist 失败")

    monkeypatch.setattr(
        em_client.EastMoneyClient, "snapshot_all", lambda self: (_snapshot_rows(), False)
    )
    monkeypatch.setattr(em_client.EastMoneyClient, "index_snapshot", boom)
    prov = EastMoneyProvider()
    try:
        records = prov.get_realtime(index_symbols=["000001.SH"])
    finally:
        prov.close()

    assert {r["symbol"] for r in records} == {"600519.SH", "000001.SZ", "830799.BJ"}


# ---------- instruments ----------

def test_instruments_shape(monkeypatch) -> None:
    monkeypatch.setattr(
        em_client.EastMoneyClient, "snapshot_all", lambda self: (_snapshot_rows(), False)
    )
    prov = EastMoneyProvider()
    try:
        items = prov.get_instruments("stock")
    finally:
        prov.close()

    assert len(items) == 3
    item = items[0]
    assert item["symbol"] == "600519.SH"
    assert item["name"] == "贵州茅台"
    assert item["code"] == "600519"
    assert item["exchange"] == "SH"
    assert item["region"] == "CN"
    assert item["type"] == "stock"
    assert item["ext"] == {}
    assert prov.get_instruments("etf") == []


# ---------- financial ----------

_METRIC_COLS = {
    "eps_basic", "eps_diluted", "bps", "ocfps", "roe", "roa", "gross_margin",
    "net_margin", "debt_to_asset_ratio", "revenue_yoy", "net_income_yoy",
    "inventory_turnover",
}


def _fake_f10_for_financials(path, params):
    if path == "NewFinanceAnalysis/ZYZBAjaxNew":
        return {"data": _zyzb_rows()}
    if path == "NewFinanceAnalysis/lrbAjaxNew":
        return {"data": _statement_rows()}
    if path == "NewFinanceAnalysis/zcfzbAjaxNew":
        return {"data": _statement_rows()}
    if path == "NewFinanceAnalysis/xjllbAjaxNew":
        return {"data": _statement_rows()}
    if path == "CapitalStockStructure/PageAjax":
        return {
            "lngbbd": [
                {"END_DATE": "2026-05-28 00:00:00", "TOTAL_SHARES": 1250081601,
                 "UNLIMITED_SHARES": 1250081601, "FREE_SHARES": 1250081601},
                {"END_DATE": "2025-09-01 00:00:00", "TOTAL_SHARES": 1252270215,
                 "UNLIMITED_SHARES": 1252270215, "FREE_SHARES": 1252270215},
            ],
        }
    raise AssertionError(f"unexpected f10 path: {path}")


def test_financial_metrics_mapping(monkeypatch) -> None:
    monkeypatch.setattr(em_client.EastMoneyClient, "f10", lambda self, path, params: _fake_f10_for_financials(path, params))
    prov = EastMoneyProvider()
    try:
        hist = prov.get_financials("metrics", ["600519.SH"], latest_only=False)
        latest = prov.get_financials("metrics", ["600519.SH"], latest_only=True)
    finally:
        prov.close()

    assert hist.height == 2
    assert set(hist.columns) >= _METRIC_COLS
    assert {"symbol", "period_end", "announce_date"} <= set(hist.columns)
    row = hist.row(0, named=True)  # 按 period_end 降序
    assert row["period_end"] == "2026-06-30"
    assert row["announce_date"] == "2026-08-15"
    assert row["symbol"] == "600519.SH"
    assert row["bps"] == 200.98
    assert row["roe"] == 16.75
    assert row["revenue_yoy"] == 1.3
    assert row["net_income_yoy"] == -1.95
    assert latest.height == 1
    assert latest["period_end"][0] == "2026-06-30"


def test_financial_statement_dates_param(monkeypatch) -> None:
    seen: dict[str, str] = {}

    def fake_f10(self, path, params):
        if path == "NewFinanceAnalysis/ZYZBAjaxNew":
            return {"data": _zyzb_rows()}
        seen[path] = params.get("dates", "")
        return {"data": _statement_rows()}

    monkeypatch.setattr(em_client.EastMoneyClient, "f10", fake_f10)
    prov = EastMoneyProvider()
    try:
        prov.get_financials("income", ["600519.SH"], latest_only=False)
        assert seen["NewFinanceAnalysis/lrbAjaxNew"] == "2026-06-30,2026-03-31"
        prov.get_financials("income", ["600519.SH"], latest_only=True)
        assert seen["NewFinanceAnalysis/lrbAjaxNew"] == "2026-06-30"
    finally:
        prov.close()


def test_financial_income_mapping(monkeypatch) -> None:
    monkeypatch.setattr(em_client.EastMoneyClient, "f10", lambda self, path, params: _fake_f10_for_financials(path, params))
    prov = EastMoneyProvider()
    try:
        df = prov.get_financials("income", ["600519.SH"], latest_only=True)
    finally:
        prov.close()

    row = df.row(0, named=True)
    assert row["revenue"] == 92278072083.21
    assert row["net_income"] == 44516880421.86
    assert row["net_income_attributable"] == 44516880421.86
    assert row["basic_eps"] == 35.57


def test_financial_balance_and_cashflow_mapping(monkeypatch) -> None:
    monkeypatch.setattr(em_client.EastMoneyClient, "f10", lambda self, path, params: _fake_f10_for_financials(path, params))
    prov = EastMoneyProvider()
    try:
        bal = prov.get_financials("balance_sheet", ["600519.SH"], latest_only=True)
        cf = prov.get_financials("cash_flow", ["600519.SH"], latest_only=True)
    finally:
        prov.close()

    brow = bal.row(0, named=True)
    assert brow["total_assets"] == 123
    assert brow["cash_and_equivalents"] == 20
    assert brow["equity_attributable"] == 90
    assert brow["retained_earnings"] == 50
    assert brow["minority_interest"] == 3
    crow = cf.row(0, named=True)
    assert crow["net_operating_cash_flow"] == 56.5
    assert crow["net_financing_cash_flow"] == -30
    assert crow["capex"] == 5
    assert crow["net_cash_change"] == 16.5


def test_financial_shares(monkeypatch) -> None:
    monkeypatch.setattr(em_client.EastMoneyClient, "f10", lambda self, path, params: _fake_f10_for_financials(path, params))
    prov = EastMoneyProvider()
    try:
        hist = prov.get_financials("shares", ["600519.SH"], latest_only=False)
        latest = prov.get_financials("shares", ["600519.SH"], latest_only=True)
    finally:
        prov.close()

    assert hist.height == 2
    assert {"symbol", "period_end", "total_shares", "float_shares"} <= set(hist.columns)
    row = hist.row(0, named=True)
    assert row["period_end"] == "2026-05-28"
    assert row["total_shares"] == 1250081601
    assert row["float_shares"] == 1250081601
    assert latest.height == 1


def test_financial_unknown_table_returns_empty() -> None:
    prov = EastMoneyProvider()
    try:
        df = prov.get_financials("nope", ["600519.SH"])
    finally:
        prov.close()
    assert df.is_empty()


def test_test_dataset_financial_preview(monkeypatch) -> None:
    monkeypatch.setattr(em_client.EastMoneyClient, "f10", lambda self, path, params: _fake_f10_for_financials(path, params))
    prov = EastMoneyProvider()
    try:
        out = prov.test_dataset("financial", ["600519.SH"])
    finally:
        prov.close()
    assert out["provider"] == "eastmoney"
    assert out["rows"] == 2
    assert "bps" in out["columns"]


# ---------- 插件注册 (真实 loader 接线) ----------

def test_plugin_registered_via_loader() -> None:
    from app.data_providers import custom as custom_sources

    assert "eastmoney" in custom_sources.names()
    status = {p["name"]: p for p in custom_sources.list_plugins()}["eastmoney"]
    assert status["available"] is True
    for ds in ("daily", "adj_factor", "minute", "realtime", "financial"):
        assert custom_sources.provider_has_dataset("eastmoney", ds)

    prov = custom_sources.get_provider("eastmoney")
    assert prov.builtin is True
    assert prov.name == "eastmoney"
    assert sorted(prov.config.datasets) == ["adj_factor", "daily", "financial", "minute", "realtime"]


# ---------- 客户端降级 (主域名被风控 → push2delay) ----------

def _fake_get_json_for_fallback(url: str, params: dict, **_kwargs) -> dict:
    if "push2delay" in url:
        return {"data": {"total": 1, "diff": [{"f12": "600519"}]}}
    raise em_client.EastMoneyError("IP 风控")


def test_snapshot_all_falls_back_to_delay_host() -> None:
    client = em_client.EastMoneyClient()
    try:
        client._get_json = _fake_get_json_for_fallback
        rows, delayed = client.snapshot_all()
    finally:
        client.close()

    assert rows == [{"f12": "600519"}]
    assert delayed is True


def test_snapshot_all_normal_returns_not_delayed() -> None:
    def fake_ok(url: str, params: dict, **_kwargs) -> dict:
        assert "push2delay" not in url
        return {"data": {"total": 1, "diff": [{"f12": "000001"}]}}

    client = em_client.EastMoneyClient()
    try:
        client._get_json = fake_ok
        rows, delayed = client.snapshot_all()
    finally:
        client.close()

    assert rows == [{"f12": "000001"}]
    assert delayed is False


# ---------- IP 风控冷却 (连续断连 → 暂停拉取 → 冷却后重试同一请求) ----------

class _FakeResp:
    def __init__(self, data: dict) -> None:
        self._data = data

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._data


def _conn_error() -> httpx.RemoteProtocolError:
    return httpx.RemoteProtocolError("Server disconnected without sending a response.")


def _patch_clock(monkeypatch) -> dict:
    """可控时钟: monotonic 返回 clock['t'], sleep 推进 clock['t']。"""
    clock = {"t": 0.0}

    def fake_sleep(secs: float) -> None:
        clock["t"] += secs

    monkeypatch.setattr(em_client.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(em_client.time, "sleep", fake_sleep)
    monkeypatch.setattr(em_client, "_cooldown_until", 0.0)
    monkeypatch.setattr(em_client, "_blocked_until", 0.0)
    monkeypatch.setattr(em_client, "_conn_fail_streak", 0)
    return clock


def test_cooldown_recovers_and_retries_same_request(monkeypatch) -> None:
    """连续 3 次断连触发冷却, 冷却结束后重试同一请求并成功(不丢数据)。"""
    _patch_clock(monkeypatch)
    calls: list[int] = []

    def fake_get(url: str, params: dict) -> _FakeResp:
        calls.append(1)
        if len(calls) <= 3:
            raise _conn_error()
        return _FakeResp({"data": {"ok": 1}})

    client = em_client.EastMoneyClient(min_interval=0.0)
    try:
        client._client.get = fake_get
        out = client._get_json("https://x", {})
    finally:
        client.close()

    assert out == {"data": {"ok": 1}}
    assert len(calls) == 4  # 3 次断连 + 冷却后 1 次成功


def test_conn_streak_resets_on_success(monkeypatch) -> None:
    """断连计数在请求成功后清零: 两次断连不触发冷却(与跨请求累计区分)。"""
    clock = _patch_clock(monkeypatch)
    state = {"n": 0}

    def fake_get(url: str, params: dict) -> _FakeResp:
        state["n"] += 1
        if state["n"] % 3 != 0:  # 每轮前两次断连, 第三次成功
            raise _conn_error()
        return _FakeResp({"ok": state["n"]})

    client = em_client.EastMoneyClient(min_interval=0.0)
    try:
        client._client.get = fake_get
        assert client._get_json("https://x", {}) == {"ok": 3}
        assert client._get_json("https://x", {}) == {"ok": 6}
    finally:
        client.close()

    # 只有 4 次短退避 sleep(1), 未触发 60s 级冷却
    assert clock["t"] < 60


def test_cooldown_exhausted_raises(monkeypatch) -> None:
    """连续断连经历 3 轮冷却(60s/300s/900s)仍失败, 进入硬封锁并抛 EastMoneyError。"""
    clock = _patch_clock(monkeypatch)
    calls: list[int] = []

    def fake_get(url: str, params: dict) -> _FakeResp:
        calls.append(1)
        raise _conn_error()

    client = em_client.EastMoneyClient(min_interval=0.0)
    try:
        client._client.get = fake_get
        with pytest.raises(em_client.EastMoneyError):
            client._get_json("https://x", {})
    finally:
        client.close()

    assert len(calls) == 12  # 3 轮冷却 x 每轮 3 次断连 + 第 4 轮 3 次断连后进入硬封锁
    assert clock["t"] >= 60 + 300 + 900  # 三轮冷却时长叠加
    assert em_client._hard_block_remaining() > 0  # 已进入硬封锁


def test_hard_block_fails_fast_without_request(monkeypatch) -> None:
    """硬封锁期内请求直接快速失败, 不再发起 HTTP(不延长风控窗口)。"""
    _patch_clock(monkeypatch)
    monkeypatch.setattr(em_client, "_blocked_until", 10.0)  # 模拟还有 10s 硬封锁
    calls: list[int] = []

    def fake_get(url: str, params: dict) -> _FakeResp:
        calls.append(1)
        return _FakeResp({"ok": 1})

    client = em_client.EastMoneyClient(min_interval=0.0)
    try:
        client._client.get = fake_get
        with pytest.raises(em_client.EastMoneyError, match="硬封锁"):
            client._get_json("https://x", {})
    finally:
        client.close()

    assert len(calls) == 0  # 未发起任何 HTTP 请求


# ---------- 快照多源轮换: 主源断连时立即切到降级源 (回归: 2026-08-31 主源被封) ----------

def _snapshot_ok() -> dict:
    return {"data": {"total": 1, "diff": [{"f12": "600519"}]}}


def test_snapshot_page_falls_back_to_delay_host_on_conn_error(monkeypatch) -> None:
    """主源断连时同一页立即轮换到 delay 源, 不在死源上走冷却阶梯(否则降级源永远轮不到)。"""
    clock = _patch_clock(monkeypatch)
    calls: list[str] = []

    def fake_get(url: str, params: dict) -> _FakeResp:
        calls.append(url)
        if "push2delay" in url:
            return _FakeResp(_snapshot_ok())
        raise _conn_error()

    client = em_client.EastMoneyClient(min_interval=0.0)
    try:
        client._client.get = fake_get
        out = client.snapshot_page(1, 100)
    finally:
        client.close()

    assert out == {"total": 1, "diff": [{"f12": "600519"}]}
    assert calls == [em_client._SNAPSHOT_URLS[0], em_client._SNAPSHOT_URLS[1]]
    assert clock["t"] < 60  # 未进入 60s 级冷却
    assert em_client._hard_block_remaining() <= 0
    assert em_client._conn_fail_streak == 0  # delay 源成功后断连计数清零


def test_snapshot_delay_host_bypasses_hard_block(monkeypatch) -> None:
    """硬封锁期降级源仍可用: 主源被封不等于降级源被封(风控按域名)。"""
    _patch_clock(monkeypatch)
    monkeypatch.setattr(em_client, "_blocked_until", 10.0)  # 模拟还有 10s 硬封锁
    calls: list[str] = []

    def fake_get(url: str, params: dict) -> _FakeResp:
        calls.append(url)
        assert "push2delay" in url
        return _FakeResp(_snapshot_ok())

    client = em_client.EastMoneyClient(min_interval=0.0)
    try:
        client._client.get = fake_get
        out = client.snapshot_page(1, 100)
    finally:
        client.close()

    assert out == {"total": 1, "diff": [{"f12": "600519"}]}
    assert calls == [em_client._SNAPSHOT_URLS[1]]  # 主源被硬封锁跳过, 只请求降级源


def test_get_json_default_still_retries_conn_errors(monkeypatch) -> None:
    """fail_fast 关闭时(单源端点如 K线/F10)行为不变: 短退避重试而非立即失败。"""
    _patch_clock(monkeypatch)
    calls: list[int] = []

    def fake_get(url: str, params: dict) -> _FakeResp:
        calls.append(1)
        if len(calls) <= 2:
            raise _conn_error()
        return _FakeResp({"ok": 1})

    client = em_client.EastMoneyClient(min_interval=0.0)
    try:
        client._client.get = fake_get
        assert client._get_json("https://x", {}) == {"ok": 1}
    finally:
        client.close()

    assert len(calls) == 3  # 2 次断连(短退避) + 1 次成功


def test_index_snapshot_falls_back_to_delay_host(monkeypatch) -> None:
    """ulist 指数快照: 主源断连立即切到延时源, secids 由 symbol 正确映射。"""
    _patch_clock(monkeypatch)
    seen: list[tuple[str, dict]] = []

    def fake_get(url: str, params: dict) -> _FakeResp:
        seen.append((url, params))
        if "push2delay" in url:
            return _FakeResp({"data": {"total": 1, "diff": [{"f12": "000001", "f14": "上证指数"}]}})
        raise _conn_error()

    client = em_client.EastMoneyClient(min_interval=0.0)
    try:
        client._client.get = fake_get
        rows, delayed = client.index_snapshot(["000001.SH", "399001.SZ"])
    finally:
        client.close()

    assert rows == [{"f12": "000001", "f14": "上证指数"}]
    assert delayed is True
    assert [u for u, _ in seen] == [em_client._ULIST_URLS[0], em_client._ULIST_URLS[1]]
    assert seen[0][1]["secids"] == "1.000001,0.399001"
    assert em_client._hard_block_remaining() <= 0
    assert em_client._conn_fail_streak == 0  # 延时源成功后断连计数清零
