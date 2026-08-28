"""东方财富(免费)数据源 provider。

直连东财公开行情/财务接口(见 client.py), 归一化到项目内部 schema。
方法签名对齐 custom.GenericHTTPProvider / MarketDataProvider, 因此注入
custom loader 注册表后, 各 service 无需改动即可路由到本 provider
(见 plugin.yaml; 数据集 → 能力授予由 tickflow/policy._augment_custom_sources 完成)。

数据范围与限制 (详见 plugin.yaml):
  - daily/adj_factor/minute/realtime: 全市场 A 股 (沪深北)
  - financial: F10 财务 (指标/利润表/资产负债表/现金流/股本结构)
  - 分钟K: 东财仅提供最近约 5 个交易日
  - 财务历史: ZYZB 接口仅返回最近约 9 个报告期 (约 2 年)
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime

import polars as pl

from app.data_providers.base import AssetType
from app.data_providers.normalizer import normalize_adj_factors, normalize_daily
from app.plugins.eastmoney.client import (
    EastMoneyClient,
    _hard_block_remaining,
    code_to_exchange,
    symbol_to_f10_code,
    symbol_to_secid,
)
from app.tickflow.rate_limits import chunked

logger = logging.getLogger(__name__)

_DATASETS = ("daily", "adj_factor", "minute", "realtime", "financial")

# K线/除权/分钟 每批标的数(东财按标的逐个请求, 分批仅为进度反馈与失败隔离)
_BATCH = 20

# 财务拉取并发度: 每 worker 一个独立客户端(各自 0.25s 节流, 并行提速),
# F10 缓存跨 worker 共享 → 同一标的的 ZYZB 每 10 分钟只拉一次。
_FIN_WORKERS = 3

_MINUTE_CANONICAL = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]

# 财务报表 → F10 路径
_F10_METRICS = "NewFinanceAnalysis/ZYZBAjaxNew"
_F10_INCOME = "NewFinanceAnalysis/lrbAjaxNew"
_F10_BALANCE = "NewFinanceAnalysis/zcfzbAjaxNew"
_F10_CASHFLOW = "NewFinanceAnalysis/xjllbAjaxNew"
_F10_SHARES = "CapitalStockStructure/PageAjax"

_F10_STATEMENT_PARAMS = {"companyType": "4", "reportDateType": "0", "reportType": "1"}

# 东财原始字段 → 内部字段 (与前端 StockFinancialDetail 的 FIELD_DEFS 对齐)
_METRICS_MAP = {
    "EPSJB": "eps_basic",
    "EPSXS": "eps_diluted",
    "BPS": "bps",
    "MGJYXJJE": "ocfps",
    "ROEJQ": "roe",
    "ZZCJLL": "roa",
    "XSMLL": "gross_margin",
    "XSJLL": "net_margin",
    "ZCFZL": "debt_to_asset_ratio",
    "TOTALOPERATEREVETZ": "revenue_yoy",
    "PARENTNETPROFITTZ": "net_income_yoy",
    "CHZZL": "inventory_turnover",
}
_INCOME_MAP = {
    "TOTAL_OPERATE_INCOME": "revenue",
    "OPERATE_COST": "operating_cost",
    "OPERATE_PROFIT": "operating_profit",
    "SALE_EXPENSE": "selling_expense",
    "MANAGE_EXPENSE": "admin_expense",
    "RESEARCH_EXPENSE": "rd_expense",
    "FINANCE_EXPENSE": "financial_expense",
    "NONBUSINESS_INCOME": "non_operating_income",
    "NONBUSINESS_EXPENSE": "non_operating_expense",
    "TOTAL_PROFIT": "total_profit",
    "INCOME_TAX": "income_tax",
    "NETPROFIT": "net_income",
    "PARENT_NETPROFIT": "net_income_attributable",
    "DEDUCT_PARENT_NETPROFIT": "net_income_deducted",
    "BASIC_EPS": "basic_eps",
    "DILUTED_EPS": "diluted_eps",
}
_BALANCE_MAP = {
    "TOTAL_ASSETS": "total_assets",
    "TOTAL_CURRENT_ASSETS": "total_current_assets",
    "TOTAL_NONCURRENT_ASSETS": "total_non_current_assets",
    "MONETARYFUNDS": "cash_and_equivalents",
    "ACCOUNTS_RECE": "accounts_receivable",
    "INVENTORY": "inventory",
    "FIXED_ASSET": "fixed_assets",
    "INTANGIBLE_ASSET": "intangible_assets",
    "GOODWILL": "goodwill",
    "TOTAL_LIABILITIES": "total_liabilities",
    "TOTAL_CURRENT_LIAB": "total_current_liabilities",
    "TOTAL_NONCURRENT_LIAB": "total_non_current_liabilities",
    "SHORT_LOAN": "short_term_borrowing",
    "LONG_LOAN": "long_term_borrowing",
    "ACCOUNTS_PAYABLE": "accounts_payable",
    "TOTAL_EQUITY": "total_equity",
    "TOTAL_PARENT_EQUITY": "equity_attributable",
    "UNASSIGN_RPOFIT": "retained_earnings",
    "MINORITY_EQUITY": "minority_interest",
}
_CASHFLOW_MAP = {
    "NETCASH_OPERATE": "net_operating_cash_flow",
    "NETCASH_INVEST": "net_investing_cash_flow",
    "NETCASH_FINANCE": "net_financing_cash_flow",
    "CONSTRUCT_LONG_ASSET": "capex",
    "CCE_ADD": "net_cash_change",
}


@dataclass
class _EastMoneyConfig:
    """轻量 config shim, 让 custom loader 的 list_sources/provider_has_dataset 能识别本 provider。"""

    name: str = "eastmoney"
    display_name: str = "东方财富 (免费)"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def availability() -> tuple[bool, str]:
    """插件可用性自检 (loader._call_check 调用, 无参数)。"""
    try:
        import httpx  # noqa: F401
    except ImportError:  # pragma: no cover
        return False, "缺少 httpx 依赖"
    return True, "ok"


def _fnum(v) -> float | None:
    """东财数值 ('-' / None / '' → None)。"""
    if v in (None, "", "-"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _pct_to_decimal(v) -> float | None:
    """百分比数值 → 小数制 (23.87 → 0.2387)。"""
    n = _fnum(v)
    return n / 100.0 if n is not None else None


def _klt_for_freq(freq: str) -> int:
    digits = "".join(ch for ch in str(freq) if ch.isdigit()) or "1"
    try:
        klt = int(digits)
    except ValueError:
        return 1
    return klt if klt in (1, 5, 15, 30, 60) else 1


def _calc_kline_lmt(start_time: datetime | None, end_time: datetime | None, klt: int) -> int | None:
    """根据日期范围计算东财 K线 lmt 参数: 增量同步传小值减少传输, 全量同步传 None。

    日K(klt=101) 每天1条, 分钟K 每天约 240 条。取所需天数 + 10 天安全余量。
    start_time 为 None 或距今 > 30 天时返回 None(全量)。
    """
    if start_time is None or end_time is None:
        return None
    days = (end_time - start_time).days
    if days > 30:
        return None
    extra = 10  # 安全余量, 覆盖非交易日与首次数据顺序差异
    if klt == 101:
        return max(30, days + extra)
    return max(1200, (days + extra) * 240)


class EastMoneyProvider:
    """内置东方财富(免费)数据源。"""

    name = "eastmoney"
    builtin = True

    def __init__(self) -> None:
        self.config = _EastMoneyConfig()
        self._client = EastMoneyClient(min_interval=0.2)  # 比默认 0.25s 略快, 批量同步提速 ~20%
        # F10 缓存供财务 worker 客户端共享(跨表/跨线程去重), 串行 K线路径不使用
        self._shared_f10_cache: dict[str, tuple[float, dict]] = self._client._f10_cache

    def close(self) -> None:  # loader.load_all 会对每个 provider 调 close
        self._client.close()

    # ================= K线解析 =================
    @staticmethod
    def _parse_kline_rows(rows: list[str]) -> pl.DataFrame:
        """东财 K 线逗号行 → DataFrame。

        字段: date,open,close,high,low,volume(手),amount(元),...(振幅/涨跌幅等忽略)
        """
        if not rows:
            return pl.DataFrame()
        records = []
        for line in rows:
            parts = line.split(",")
            if len(parts) < 7:
                continue
            records.append({
                "date": parts[0],
                "open": parts[1],
                "close": parts[2],
                "high": parts[3],
                "low": parts[4],
                "volume": parts[5],
                "amount": parts[6],
            })
        if not records:
            return pl.DataFrame()
        return pl.DataFrame(records)

    @staticmethod
    def _filter_range(
        df: pl.DataFrame,
        col: str,
        start: datetime | None,
        end: datetime | None,
    ) -> pl.DataFrame:
        """本地按字符串过滤日期/时间范围 (东财接口无区间过滤)。"""
        if df.is_empty() or col not in df.columns:
            return df
        if start is not None:
            df = df.filter(pl.col(col) >= start.strftime("%Y-%m-%d"))
        if end is not None:
            df = df.filter(pl.col(col) <= end.strftime("%Y-%m-%d %H:%M:%S"))
        return df

    @staticmethod
    def _filter_date_range(df: pl.DataFrame, start: datetime | None, end: datetime | None) -> pl.DataFrame:
        """对已规范化的 Date 列做范围过滤 (adj_factor 用)。"""
        if df.is_empty() or "trade_date" not in df.columns:
            return df
        if start is not None:
            df = df.filter(pl.col("trade_date") >= pl.lit(start.date()))
        if end is not None:
            df = df.filter(pl.col("trade_date") <= pl.lit(end.date()))
        return df

    # ================= daily =================
    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done=None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        logger.info("eastmoney daily 拉取开始(%d symbols)", len(symbols))
        lmt = _calc_kline_lmt(start_time, end_time, 101)
        frames: list[pl.DataFrame] = []
        chunks = chunked(symbols, _BATCH)
        for i, chunk in enumerate(chunks):
            aborted = False
            for sym in chunk:
                try:
                    rows = self._client.kline_series(symbol_to_secid(sym), klt=101, fqt=0, lmt=lmt)
                except Exception as e:
                    if _hard_block_remaining() > 0:
                        logger.warning(
                            "eastmoney daily 遇 IP 硬封锁, 提前结束本轮拉取(剩余 %d 只留待下轮)",
                            len(symbols) - i * _BATCH,
                        )
                        aborted = True
                        break
                    logger.warning("eastmoney daily 拉取失败 %s: %s", sym, e)
                    continue
                df = self._parse_kline_rows(rows)
                df = self._filter_range(df, "date", start_time, end_time)
                if df.is_empty():
                    continue
                # 东财 volume 单位为手 → x100 转股 (内部约定), amount 为元
                df = df.with_columns(
                    pl.lit(sym).alias("symbol"),
                    (pl.col("volume").cast(pl.Float64, strict=False) * 100).alias("volume"),
                    pl.col("amount").cast(pl.Float64, strict=False).alias("amount"),
                )
                df = normalize_daily(df, source=self.name)
                if not df.is_empty():
                    frames.append(df)
            if aborted:
                break
            if on_chunk_done:
                on_chunk_done(i + 1, len(chunks))
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    # ================= adj_factor =================
    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done=None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        logger.info("eastmoney adj_factor 拉取开始(%d symbols)", len(symbols))
        lmt = _calc_kline_lmt(start_time, end_time, 101)
        frames: list[pl.DataFrame] = []
        chunks = chunked(symbols, _BATCH)
        for i, chunk in enumerate(chunks):
            aborted = False
            for sym in chunk:
                try:
                    secid = symbol_to_secid(sym)
                    raw_rows = self._client.kline_series(secid, klt=101, fqt=0, lmt=lmt)
                    qfq_rows = self._client.kline_series(secid, klt=101, fqt=1, lmt=lmt)
                except Exception as e:
                    if _hard_block_remaining() > 0:
                        logger.warning(
                            "eastmoney adj_factor 遇 IP 硬封锁, 提前结束本轮拉取(剩余 %d 只留待下轮)",
                            len(symbols) - i * _BATCH,
                        )
                        aborted = True
                        break
                    logger.warning("eastmoney adj_factor 拉取失败 %s: %s", sym, e)
                    continue
                df = self._compute_adj_factors(sym, raw_rows, qfq_rows)
                df = self._filter_date_range(df, start_time, end_time)
                if not df.is_empty():
                    frames.append(df)
            if aborted:
                break
            if on_chunk_done:
                on_chunk_done(i + 1, len(chunks))
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    @staticmethod
    def _compute_adj_factors(symbol: str, raw_rows: list[str], qfq_rows: list[str]) -> pl.DataFrame:
        """ex_factor = qfq_close / raw_close (与 tickflow 复权口径一致)。"""
        raw_close: dict[str, str] = {}
        for line in raw_rows:
            parts = line.split(",")
            if len(parts) >= 3:
                raw_close[parts[0]] = parts[2]
        records = []
        for line in qfq_rows:
            parts = line.split(",")
            if len(parts) < 3:
                continue
            date_s, qfq_close = parts[0], parts[2]
            rc = raw_close.get(date_s)
            if rc in (None, "", "0"):
                continue
            try:
                ex = float(qfq_close) / float(rc)
            except (TypeError, ValueError, ZeroDivisionError):
                continue
            records.append({"symbol": symbol, "trade_date": date_s, "ex_factor": ex})
        return normalize_adj_factors(records, source=EastMoneyProvider.name)

    # ================= minute =================
    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        klt = _klt_for_freq(freq)
        lmt = _calc_kline_lmt(start_time, end_time, klt)
        logger.info("eastmoney minute 拉取开始(%d symbols, klt=%d)", len(symbols), klt)
        frames: list[pl.DataFrame] = []
        chunks = chunked(symbols, _BATCH)
        for i, chunk in enumerate(chunks):
            aborted = False
            for sym in chunk:
                try:
                    rows = self._client.kline_series(symbol_to_secid(sym), klt=klt, fqt=0, lmt=lmt)
                except Exception as e:
                    if _hard_block_remaining() > 0:
                        logger.warning(
                            "eastmoney minute 遇 IP 硬封锁, 提前结束本轮拉取(剩余 %d 只留待下轮)",
                            len(symbols) - i * _BATCH,
                        )
                        aborted = True
                        break
                    logger.warning("eastmoney minute 拉取失败 %s: %s", sym, e)
                    continue
                df = self._parse_kline_rows(rows).rename({"date": "datetime"})
                df = self._filter_range(df, "datetime", start_time, end_time)
                if df.is_empty():
                    continue
                df = df.with_columns(
                    pl.lit(sym).alias("symbol"),
                    # 东财分钟时间戳为北京时间墙钟, 直接解析为 naive datetime
                    pl.col("datetime").str.to_datetime("%Y-%m-%d %H:%M", strict=False),
                    (pl.col("volume").cast(pl.Float64, strict=False) * 100).alias("volume"),
                    pl.col("amount").cast(pl.Float64, strict=False).alias("amount"),
                )
                for col in ("open", "high", "low", "close"):
                    df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
                keep = [c for c in _MINUTE_CANONICAL if c in df.columns]
                if "datetime" not in keep:
                    continue
                df = df.select(keep).drop_nulls(subset=["datetime"])
                if not df.is_empty():
                    frames.append(df)
            if aborted:
                break
            if on_chunk_done:
                on_chunk_done(i + 1, len(chunks))
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    # ================= realtime =================
    def get_realtime(self) -> list[dict]:
        logger.info("eastmoney realtime 拉取开始(全市场快照)")
        try:
            rows, delayed = self._client.snapshot_all()
        except Exception as e:
            logger.warning("eastmoney realtime 拉取失败: %s", e)
            return []
        if delayed:
            logger.warning("eastmoney realtime 已降级为延时行情源(push2delay)")
        fetched_ms = int(time.time() * 1000)
        records = []
        for r in rows:
            code = str(r.get("f12") or "")
            if not code:
                continue
            vol = _fnum(r.get("f5"))
            # 契约与 quote_service._build_quote_extra 对齐: change_pct/amplitude/
            # turnover_rate 为小数制 (内部 x100 回百分数), volume 为股, amount 为元。
            records.append({
                "symbol": f"{code}.{code_to_exchange(code)}",
                "name": r.get("f14"),
                "last_price": _fnum(r.get("f2")),
                "prev_close": _fnum(r.get("f18")),
                "open": _fnum(r.get("f17")),
                "high": _fnum(r.get("f15")),
                "low": _fnum(r.get("f16")),
                "volume": None if vol is None else vol * 100,
                "amount": _fnum(r.get("f6")),
                "change_pct": _pct_to_decimal(r.get("f3")),
                "change_amount": _fnum(r.get("f4")),
                "amplitude": _pct_to_decimal(r.get("f7")),
                "turnover_rate": _pct_to_decimal(r.get("f8")),
                "timestamp": fetched_ms,
                "session": "delayed" if delayed else "regular",
            })
        logger.info("eastmoney realtime: %d 只标的", len(records))
        return records

    # ================= instruments =================
    def get_instruments(self, asset_type: str = "stock") -> list[dict]:
        """返回 tickflow Instrument 形状的行, 供 instrument_sync._flatten_instruments 复用。"""
        if asset_type != "stock":
            return []
        try:
            rows, _delayed = self._client.snapshot_all()
        except Exception as e:
            logger.warning("eastmoney instruments 拉取失败: %s", e)
            return []
        items = []
        for r in rows:
            code = str(r.get("f12") or "")
            if not code:
                continue
            ex = code_to_exchange(code)
            items.append({
                "symbol": f"{code}.{ex}",
                "name": r.get("f14") or code,
                "code": code,
                "exchange": ex,
                "region": "CN",
                "type": "stock",
                "ext": {},
            })
        logger.info("eastmoney instruments: %d 只标的", len(items))
        return items

    # ================= financial =================
    def get_financials(
        self,
        table: str,
        symbols: list[str],
        latest_only: bool = True,
    ) -> pl.DataFrame:
        """拉取一张标准化财务表。列: symbol, period_end[, announce_date], 各表字段。

        table: metrics / income / balance_sheet / cash_flow / shares。
        """
        handler = {
            "metrics": self._fetch_metrics,
            "income": self._fetch_income,
            "balance_sheet": self._fetch_balance_sheet,
            "cash_flow": self._fetch_cash_flow,
            "shares": self._fetch_shares,
        }.get(table)
        if handler is None or not symbols:
            return pl.DataFrame()
        logger.info("eastmoney financial %s 拉取开始(%d symbols, latest_only=%s)",
                    table, len(symbols), latest_only)

        workers = max(1, min(_FIN_WORKERS, len(symbols)))
        # 每 worker 一个独立客户端(各自节流并行提速); F10 缓存共享 → 同标的 ZYZB 只拉一次
        clients = [
            EastMoneyClient(f10_cache=self._shared_f10_cache) for _ in range(workers)
        ]
        frames: list[pl.DataFrame] = []

        def _one(sym: str) -> pl.DataFrame | None:
            client = clients[hash(sym) % workers]
            try:
                return handler(sym, latest_only, client)
            except Exception as e:
                logger.warning("eastmoney financial %s 拉取失败 %s: %s", table, sym, e)
                return None

        try:
            if workers == 1:
                for sym in symbols:
                    df = _one(sym)
                    if df is not None and not df.is_empty():
                        frames.append(df)
            else:
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    for df in ex.map(_one, symbols):
                        if df is not None and not df.is_empty():
                            frames.append(df)
        finally:
            for c in clients:
                c.close()
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    def _report_dates(self, sym: str, latest_only: bool, client: EastMoneyClient) -> list[str]:
        """从 ZYZB(缓存) 取报告期列表, 供三张报表的 dates 参数使用。"""
        payload = client.f10(
            _F10_METRICS, {"type": "0", "code": symbol_to_f10_code(sym)}
        )
        rows = payload.get("data") or []
        dates = [str(r.get("REPORT_DATE") or "")[:10] for r in rows if r.get("REPORT_DATE")]
        if not dates:
            return []
        if latest_only:
            return [max(dates)]
        return dates

    @staticmethod
    def _record(sym: str, period: str, announce: str | None, row: dict, field_map: dict) -> dict:
        rec: dict = {"symbol": sym, "period_end": period, "announce_date": announce}
        for src, dst in field_map.items():
            rec[dst] = _fnum(row.get(src))
        return rec

    def _fetch_metrics(self, sym: str, latest_only: bool, client: EastMoneyClient) -> pl.DataFrame:
        payload = client.f10(
            _F10_METRICS, {"type": "0", "code": symbol_to_f10_code(sym)}
        )
        rows = payload.get("data") or []
        recs = []
        for r in rows:
            period = str(r.get("REPORT_DATE") or "")[:10]
            if not period:
                continue
            announce = str(r.get("NOTICE_DATE") or "")[:10] or None
            recs.append(self._record(sym, period, announce, r, _METRICS_MAP))
        recs.sort(key=lambda r: r["period_end"], reverse=True)
        if latest_only and recs:
            recs = recs[:1]
        return pl.DataFrame(recs)

    def _fetch_statement(
        self, sym: str, latest_only: bool, client: EastMoneyClient,
        path: str, field_map: dict,
    ) -> pl.DataFrame:
        dates = self._report_dates(sym, latest_only, client)
        params = dict(_F10_STATEMENT_PARAMS)
        params["code"] = symbol_to_f10_code(sym)
        if dates:
            params["dates"] = ",".join(dates)
        payload = client.f10(path, params)
        rows = payload.get("data") or []
        recs = []
        for r in rows:
            period = str(r.get("REPORT_DATE") or "")[:10]
            if not period:
                continue
            announce = str(r.get("NOTICE_DATE") or "")[:10] or None
            recs.append(self._record(sym, period, announce, r, field_map))
        recs.sort(key=lambda r: r["period_end"], reverse=True)
        if latest_only and recs:
            recs = recs[:1]
        return pl.DataFrame(recs)

    def _fetch_income(self, sym: str, latest_only: bool, client: EastMoneyClient) -> pl.DataFrame:
        return self._fetch_statement(sym, latest_only, client, _F10_INCOME, _INCOME_MAP)

    def _fetch_balance_sheet(self, sym: str, latest_only: bool, client: EastMoneyClient) -> pl.DataFrame:
        return self._fetch_statement(sym, latest_only, client, _F10_BALANCE, _BALANCE_MAP)

    def _fetch_cash_flow(self, sym: str, latest_only: bool, client: EastMoneyClient) -> pl.DataFrame:
        return self._fetch_statement(sym, latest_only, client, _F10_CASHFLOW, _CASHFLOW_MAP)

    def _fetch_shares(self, sym: str, latest_only: bool, client: EastMoneyClient) -> pl.DataFrame:
        payload = client.f10(_F10_SHARES, {"code": symbol_to_f10_code(sym)})
        rows = payload.get("lngbbd") or []
        recs = []
        for r in rows:
            period = str(r.get("END_DATE") or "")[:10]
            if not period:
                continue
            # 流通股本取无限售条件股份(UNLIMITED_SHARES), 缺失时退化到 FREE_SHARES
            free = r.get("UNLIMITED_SHARES")
            if free in (None, ""):
                free = r.get("FREE_SHARES")
            recs.append({
                "symbol": sym,
                "period_end": period,
                "total_shares": _fnum(r.get("TOTAL_SHARES")),
                "float_shares": _fnum(free),
            })
        recs.sort(key=lambda r: r["period_end"], reverse=True)
        if latest_only and recs:
            recs = recs[:1]
        return pl.DataFrame(recs)

    # ================= 测试(设置页试拉) =================
    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        symbols = symbols or ["600519.SH"]
        if dataset == "daily":
            return _preview(dataset, self.get_daily(symbols, None, None))
        if dataset == "adj_factor":
            return _preview(dataset, self.get_adj_factors(symbols, None, None))
        if dataset == "minute":
            return _preview(dataset, self.get_minute(symbols, None, None))
        if dataset == "realtime":
            rows = self.get_realtime()
            head = rows[:5]
            return {
                "provider": self.name,
                "dataset": "realtime",
                "rows": len(rows),
                "columns": list(head[0].keys()) if head else [],
                "preview": head,
            }
        if dataset == "financial":
            df = self.get_financials("metrics", symbols, latest_only=False)
            return _preview(dataset, df)
        raise ValueError(f"eastmoney 不支持数据集: {dataset}")


def _preview(dataset: str, df: pl.DataFrame) -> dict:
    return {
        "provider": "eastmoney",
        "dataset": dataset,
        "rows": df.height,
        "columns": df.columns,
        "preview": df.head(5).to_dicts() if not df.is_empty() else [],
    }
