#!/usr/bin/env python
"""一次性回补: 用 TickFlow 免费批量接口向前补一年全A日K。

防限流设计: 走 TickFlow kline.daily.batch (100只/请求, 60rpm 上限),
4 线程并发 (实际 ~4-16rpm, 远低于上限); 不走 eastmoney 逐股接口
(5600 请求, push2his 有硬封锁风险), 不改 provider 配置。
无除权因子、不重算 enriched —— 仅补齐 kline_daily 原始行情。

每块落盘一次 (merge-upsert 幂等), 中断后重跑可从缺口继续。

用法 (从 backend/ 目录运行):
    PYTHONUNBUFFERED=1 .venv/bin/python -m scripts.backfill_daily_2y
    PYTHONUNBUFFERED=1 .venv/bin/python -m scripts.backfill_daily_2y --years 1 --workers 4
"""
from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import polars as pl

from app.services import kline_sync
from app.tickflow.rate_limits import chunked
from app.tickflow.repository import DataStore, KlineRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

_write_lock = threading.Lock()


def _fetch_and_write(chunk: list[str], idx: int, total: int,
                     start: datetime, end: datetime, repo: KlineRepository) -> tuple[int, int, list[str]]:
    """拉取一块标的并立即落盘, 返回 (块序号, 行数, 失败标的)。"""
    t0 = time.time()
    failed: list[str] = []
    df = kline_sync.sync_daily_batch(
        chunk,
        batch_size=None,  # chunk 已是最终批次
        rpm=None,
        start_time=start,
        end_time=end,
        failed_out=failed,
    )
    rows = 0 if df.is_empty() else df.height
    if rows:
        with _write_lock:
            repo.append_daily(df)
    logger.info("批次 %d/%d 完成: %d 行, %.1fs%s", idx, total, rows, time.time() - t0,
                f", 失败 {len(failed)} 只" if failed else "")
    return idx, rows, failed


def main() -> int:
    parser = argparse.ArgumentParser(description="向前回补全A日K (TickFlow 批量)")
    parser.add_argument("--years", type=float, default=1.0, help="向前回补年数 (默认 1)")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    repo = KlineRepository(DataStore())
    data_dir = repo.store.data_dir

    daily_dir = data_dir / "kline_daily"
    dates = sorted(p.name.split("=", 1)[1] for p in daily_dir.glob("date=*"))
    if not dates:
        logger.error("本地无日K分区, 先执行一次完整同步")
        return 1
    earliest = date.fromisoformat(dates[0])
    end = earliest - timedelta(days=1)
    start = earliest - timedelta(days=int(args.years * 365))
    logger.info("本地最早分区 %s, 回补区间 [%s ~ %s]", earliest, start, end)

    inst_path = data_dir / "instruments" / "instruments.parquet"
    symbols = sorted(pl.read_parquet(inst_path, columns=["symbol"])["symbol"].to_list())
    logger.info("universe: %d 只 (instruments.parquet)", len(symbols))

    chunks = chunked(symbols, args.batch_size)
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end, datetime.max.time())

    t0 = time.time()
    total_rows = 0
    all_failed: list[str] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(_fetch_and_write, chunk, i + 1, len(chunks), start_dt, end_dt, repo)
            for i, chunk in enumerate(chunks)
        ]
        for fut in as_completed(futures):
            _, rows, failed = fut.result()
            total_rows += rows
            all_failed.extend(failed)

    logger.info("回补完成: %d 行, 耗时 %.1f 分钟, 失败标的 %d 只",
                total_rows, (time.time() - t0) / 60, len(all_failed))
    if all_failed:
        logger.warning("失败标的样例: %s", all_failed[:10])
    return 0


if __name__ == "__main__":
    sys.exit(main())
