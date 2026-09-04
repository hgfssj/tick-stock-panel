"""中位数指数 — 全A每日涨跌幅中位数的累计趋势指数。

数据全部来自本地 kline_daily parquet, 零外部请求。
构造: m_t = 当日全A个股 change_pct (close/prev_close-1) 的中位数,
C_t = C_{t-1} × (1 + m_t), 基日 = 1000; OHLC 中 open=昨收, high/low 取 max/min。

说明: 除权日原始价跳变与新股首日涨跌幅会进入个股 change_pct,
统一按 |change| > 30% 过滤 (30% 为北交所上限, 合法波动不越界);
universe 取当前上市股票, 存在幸存者偏差, 仅作趋势参考。
"""
from __future__ import annotations

import logging
from datetime import date

import polars as pl

from app.indicators.pipeline import compute_enriched
from app.parquet import scan_daily_parquet
from app.tickflow.repository import KlineRepository

logger = logging.getLogger(__name__)

MEDIAN_SYMBOL = "MEDIAN.XX"
MEDIAN_NAME = "中位数指数"
MEDIAN_CODE = "MEDIAN"
_BASE_CLOSE = 1000.0
_MAX_ABS_CHANGE = 0.30  # 北交所涨跌停上限; 超出视为除权/新股首日跳变

_INDEX_COLS = ["symbol", "close", "open", "high", "low", "volume", "amount", "quote_ts", "date"]


def _daily_median_change(repo: KlineRepository) -> pl.DataFrame:
    """本地扫描全A日K, 返回 (date, median_change) 帧, 按 date 升序。零外部请求。"""
    glob = str(repo.store.data_dir / "kline_daily" / "**" / "*.parquet")
    return (
        scan_daily_parquet(glob)
        .select("symbol", "date", "close")
        .filter(pl.col("close").is_not_null() & (pl.col("close") > 0))
        .sort("symbol", "date")
        .with_columns(
            (pl.col("close") / pl.col("close").shift(1).over("symbol") - 1).alias("change_pct")
        )
        .filter(
            pl.col("change_pct").is_not_null()
            & pl.col("change_pct").is_finite()
            & (pl.col("change_pct").abs() <= _MAX_ABS_CHANGE)
        )
        .group_by("date")
        .agg(pl.col("change_pct").median().alias("median_change"))
        .sort("date")
        .collect()
    )


def _build_index_rows(medians: pl.DataFrame) -> pl.DataFrame:
    """中位数序列 → 累计指数 OHLC 行 (schema 对齐 kline_index_daily)。"""
    closes: list[float] = []
    c = _BASE_CLOSE
    for m in medians["median_change"].to_list():
        c *= 1.0 + float(m)
        closes.append(c)
    df = medians.select("date").with_columns(pl.Series("close", closes, dtype=pl.Float64))
    df = df.with_columns(pl.col("close").shift(1).fill_null(_BASE_CLOSE).alias("open"))
    return df.with_columns(
        pl.max_horizontal("open", "close").alias("high"),
        pl.min_horizontal("open", "close").alias("low"),
        # 合成指数无真实成交: 置 null 而非 0, 避免量比指标 0/0=NaN 击穿 JSON 序列化
        pl.lit(None, dtype=pl.Float64).alias("volume"),
        pl.lit(None, dtype=pl.Float64).alias("amount"),
        pl.lit(None, dtype=pl.Int64).alias("quote_ts"),
        pl.lit(MEDIAN_SYMBOL).alias("symbol"),
    ).select(_INDEX_COLS)


def ensure_registered(repo: KlineRepository) -> None:
    """把中位数指数注册进 instruments_index 维表 (merge-upsert, 保留已有指数)。"""
    inst = repo.get_index_instruments()
    row = pl.DataFrame({
        "symbol": [MEDIAN_SYMBOL],
        "name": [MEDIAN_NAME],
        "code": [MEDIAN_CODE],
        "asset_type": ["index"],
    })
    if not inst.is_empty() and "symbol" in inst.columns:
        inst = inst.filter(pl.col("symbol") != MEDIAN_SYMBOL)
        merged = pl.concat([inst, row], how="diagonal_relaxed")
    else:
        merged = row
    repo.save_index_instruments(merged)


def _last_index_close(repo: KlineRepository, before: date) -> float | None:
    """读取中位数指数在 before 之前最近一个交易日的收盘。"""
    glob = str(repo.store.data_dir / "kline_index_daily" / "**" / "*.parquet")
    df = (
        scan_daily_parquet(glob)
        .filter(pl.col("symbol") == MEDIAN_SYMBOL)
        .filter(pl.col("date") < before)
        .select("date", "close")
        .sort("date")
        .tail(1)
        .collect()
    )
    if df.is_empty():
        return None
    return float(df["close"][0])


def _persist_rows(repo: KlineRepository, rows: pl.DataFrame) -> None:
    """双写 raw + enriched (读取层只查 kline_index_enriched)。与普通指数同步保持同一模式。"""
    repo.append_index_daily(rows)
    enriched = compute_enriched(rows, factors=None, instruments=None)
    repo.append_index_enriched(enriched)


def rebuild(repo: KlineRepository) -> dict:
    """全量重建中位数指数并写入 kline_index_daily (幂等, 按 symbol+date upsert)。"""
    medians = _daily_median_change(repo)
    if medians.is_empty():
        logger.warning("中位数指数: 本地日K为空, 跳过重建")
        return {"dates": 0}
    rows = _build_index_rows(medians)
    _persist_rows(repo, rows)
    ensure_registered(repo)
    repo.refresh_index_views()
    result = {
        "dates": rows.height,
        "first_date": str(rows["date"][0]),
        "last_date": str(rows["date"][-1]),
        "last_close": round(float(rows["close"][-1]), 4),
    }
    logger.info("中位数指数重建完成: %s", result)
    return result


def update_today(repo: KlineRepository, trade_date: date) -> bool:
    """盘后增量: 只 upsert 当日一行。链断裂(缺历史行)时由 rebuild 修复。"""
    medians = _daily_median_change(repo)
    hit = medians.filter(pl.col("date") == trade_date)
    if hit.is_empty():
        logger.warning("中位数指数: %s 无日K数据, 跳过更新", trade_date)
        return False
    prev_close = _last_index_close(repo, trade_date)
    if prev_close is None:
        prev_close = _BASE_CLOSE
    close = prev_close * (1.0 + float(hit["median_change"][0]))
    row = pl.DataFrame({
        "symbol": [MEDIAN_SYMBOL],
        "close": [close],
        "open": [prev_close],
        "high": [max(prev_close, close)],
        "low": [min(prev_close, close)],
        "volume": pl.Series([None], dtype=pl.Float64),
        "amount": pl.Series([None], dtype=pl.Float64),
        "quote_ts": pl.Series([None], dtype=pl.Int64),
        "date": [trade_date],
    }).select(_INDEX_COLS)
    _persist_rows(repo, row)
    ensure_registered(repo)
    logger.info("中位数指数已更新: %s close=%.2f", trade_date, close)
    return True
