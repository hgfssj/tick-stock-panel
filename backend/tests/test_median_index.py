"""中位数指数服务单元测试 (纯本地构造数据, 不触网)。"""
from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.services import median_index
from app.tickflow.repository import DataStore, KlineRepository


def _daily_rows() -> pl.DataFrame:
    """3 只股票 × 3 天, 涨跌幅序列手工可算。

    day2 changes: A +10%, B -5%, C 0%  → median  0%
    day3 changes: A +10%, B +5%, C +5% → median +5%
    """
    rows = [
        ("A.SZ", date(2026, 9, 1), 10.0),
        ("A.SZ", date(2026, 9, 2), 11.0),
        ("A.SZ", date(2026, 9, 3), 12.1),
        ("B.SZ", date(2026, 9, 1), 20.0),
        ("B.SZ", date(2026, 9, 2), 19.0),
        ("B.SZ", date(2026, 9, 3), 19.95),
        ("C.SZ", date(2026, 9, 1), 30.0),
        ("C.SZ", date(2026, 9, 2), 30.0),
        ("C.SZ", date(2026, 9, 3), 31.5),
    ]
    return pl.DataFrame({
        "symbol": [r[0] for r in rows],
        "date": [r[1] for r in rows],
        "open": [r[2] for r in rows],
        "high": [r[2] for r in rows],
        "low": [r[2] for r in rows],
        "close": [r[2] for r in rows],
        "volume": [100.0] * len(rows),
        "amount": [1000.0] * len(rows),
        "quote_ts": pl.Series([None] * len(rows), dtype=pl.Int64),
    })


@pytest.fixture()
def repo(tmp_path) -> KlineRepository:
    repo = KlineRepository(DataStore(data_dir=tmp_path))
    repo.append_daily(_daily_rows())
    return repo


def _read_index_rows(repo: KlineRepository) -> pl.DataFrame:
    glob = str(repo.store.data_dir / "kline_index_daily" / "**" / "*.parquet")
    return (
        pl.scan_parquet(glob, hive_partitioning=True)
        .filter(pl.col("symbol") == median_index.MEDIAN_SYMBOL)
        .sort("date")
        .collect()
    )


def test_rebuild_cumulative_index(repo) -> None:
    result = median_index.rebuild(repo)
    assert result["dates"] == 2  # 首日无 prev_close, 不产生中位数

    df = _read_index_rows(repo)
    assert df.height == 2
    day2, day3 = df.to_dicts()
    # day2: median 0% → close 维持 1000
    assert day2["date"] == date(2026, 9, 2)
    assert day2["close"] == pytest.approx(1000.0)
    assert day2["open"] == pytest.approx(1000.0)
    # day3: median +5% → 1000 × 1.05
    assert day3["close"] == pytest.approx(1050.0)
    assert day3["open"] == pytest.approx(1000.0)
    assert day3["high"] == pytest.approx(1050.0)
    assert day3["low"] == pytest.approx(1000.0)


def test_rebuild_registers_instrument(repo) -> None:
    median_index.rebuild(repo)
    assert median_index.MEDIAN_SYMBOL in repo.get_index_symbol_set()
    assert repo.resolve_asset_type(median_index.MEDIAN_SYMBOL) == "index"
    inst = repo.get_index_instruments()
    hit = inst.filter(pl.col("symbol") == median_index.MEDIAN_SYMBOL)
    assert hit["name"][0] == median_index.MEDIAN_NAME


def test_update_today_appends_single_row(repo) -> None:
    median_index.rebuild(repo)
    assert median_index.update_today(repo, date(2026, 9, 3)) is True
    df = _read_index_rows(repo)
    assert df.height == 2  # upsert 不重复
    # 与全量重建结果一致 (链条未断)
    assert df["close"][-1] == pytest.approx(1050.0)


def test_update_today_skips_missing_date(repo) -> None:
    median_index.rebuild(repo)
    assert median_index.update_today(repo, date(2026, 9, 4)) is False


def test_outlier_moves_filtered(repo) -> None:
    """除权/新股首日 >30% 跳变被过滤, 不进入中位数。"""
    rows = _daily_rows().vstack(pl.DataFrame({
        "symbol": ["D.SZ", "D.SZ"],
        "date": [date(2026, 9, 2), date(2026, 9, 3)],
        "open": [5.0, 10.0], "high": [5.0, 10.0], "low": [5.0, 10.0],
        "close": [5.0, 10.0],  # +100% 跳变
        "volume": [100.0, 100.0], "amount": [1000.0, 1000.0],
        "quote_ts": pl.Series([None, None], dtype=pl.Int64),
    }))
    repo.append_daily(rows)
    median_index.rebuild(repo)
    df = _read_index_rows(repo)
    # D.SZ day3 的 +100% 被过滤后, day3 中位数仍是 +5%
    assert df["close"][-1] == pytest.approx(1050.0)
