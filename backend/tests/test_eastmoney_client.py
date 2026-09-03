"""eastmoney 客户端层单元测试 (不触网, mock snapshot_page)。"""
from __future__ import annotations

from app.plugins.eastmoney import client as em_client


def _snapshot_row(code: str, price: float) -> dict:
    return {"f12": code, "f14": f"股票{code}", "f2": price, "f3": 1.0,
            "f4": 0.1, "f5": 100, "f6": 1e6, "f7": 2.0, "f8": 0.5,
            "f15": price + 1, "f16": price - 1, "f17": price, "f18": price}


def test_snapshot_all_dedupes_cross_page_duplicates(monkeypatch) -> None:
    """风控降级页间主机切换时同一标的机会出现在多页, 必须按 f12 去重且后页覆盖。"""
    rows_a = [_snapshot_row("600519", 1300.0), _snapshot_row("000001", 10.0)]
    rows_b = [_snapshot_row("000001", 10.2), _snapshot_row("830799", 15.0)]
    pages = {1: {"diff": rows_a, "total": 103}, 2: {"diff": rows_b}}
    c = em_client.EastMoneyClient()
    try:
        monkeypatch.setattr(c, "snapshot_page", lambda pn, pz: pages[pn])
        rows, delayed = c.snapshot_all()
    finally:
        c.close()

    assert delayed is False
    assert len(rows) == 3
    by_code = {r["f12"]: r for r in rows}
    assert set(by_code) == {"600519", "000001", "830799"}
    # 后页覆盖前页: 000001 取第二页的价格
    assert by_code["000001"]["f2"] == 10.2
