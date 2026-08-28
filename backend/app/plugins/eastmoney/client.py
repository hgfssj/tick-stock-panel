"""东方财富公开接口 HTTP 客户端。

纯 httpx 直连(复用后端核心依赖, 零新增包)。接口均为东财网页端公开 JSON 接口:
  - push2his.eastmoney.com                K线(日/分钟, 不复权/前复权)
  - push2.eastmoney.com                   全市场实时快照 (clist)
  - emweb.securities.eastmoney.com        F10 财务(指标/利润表/资产负债表/现金流/股本)

防护策略:
  - 全局最小请求间隔 (默认 0.25s/请求, 单客户端), 失败指数退避重试(最多 3 次);
  - IP 风控冷却: 连续断连达阈值自动暂停拉取(第1/2/3轮 60s/300s/900s),
    冷却结束后重试同一请求, 避免逐标的跳过造成数据缺失; 状态跨客户端共享(风控按 IP);
  - K线序列与 F10 JSON 按 TTL 缓存(分钟级), 同一轮管道内同一标的只拉一次;
  - 客户端实例可独立创建(线程内各自节流), 供 provider 并发场景按需使用。
"""
from __future__ import annotations

import logging
import threading
import time

import httpx

logger = logging.getLogger(__name__)

# ---- IP 风控冷却 (模块级, 跨客户端共享) ----
_CONN_FAIL_THRESHOLD = 3            # 连续断连 N 次触发冷却
_COOLDOWN_SECONDS = (60.0, 300.0, 900.0)  # 冷却时长逐轮递增
_MAX_COOLDOWN_ROUNDS = 3            # 最多冷却轮数, 耗尽后进入硬封锁
_HARD_BLOCK_SECONDS = 1800.0        # 硬封锁时长: 期间直接快速失败, 不再发起请求
_cooldown_until = 0.0               # 冷却截止时刻 (monotonic)
_blocked_until = 0.0                # 硬封锁截止时刻 (monotonic)
_conn_fail_streak = 0               # 连续断连计数
_cooldown_lock = threading.Lock()


def _is_conn_error(err: Exception) -> bool:
    """断连类错误(东财 IP 风控的典型表现)。"""
    text = str(err)
    return "Server disconnected" in text or "Empty reply" in text


def _note_conn_failure(round_idx: int) -> bool:
    """记录一次断连; 达到阈值时启动冷却。返回是否触发了冷却。"""
    global _cooldown_until, _conn_fail_streak
    with _cooldown_lock:
        _conn_fail_streak += 1
        if _conn_fail_streak >= _CONN_FAIL_THRESHOLD:
            _conn_fail_streak = 0
            secs = _COOLDOWN_SECONDS[min(round_idx, len(_COOLDOWN_SECONDS) - 1)]
            _cooldown_until = time.monotonic() + secs
            logger.warning("eastmoney 连续断连触发 IP 风控冷却 %.0f 秒", secs)
            return True
        return False


def _enter_hard_block() -> None:
    """冷却耗尽后进入硬封锁: 期间所有请求直接快速失败, 不再发起 HTTP。

    避免每只股票重复走完整冷却周期(3 断连+60s+3 断连+300s+3 断连+900s
    约 21 分钟/只), 剩余数千只标的几分钟内即可快速跳过; 同时不再发起
    新请求, 不会延长东财的风控封禁窗口。
    """
    global _blocked_until
    with _cooldown_lock:
        _blocked_until = time.monotonic() + _HARD_BLOCK_SECONDS
    logger.error("eastmoney 冷却耗尽, 进入 %.0f 秒硬封锁(期间请求直接失败)", _HARD_BLOCK_SECONDS)


def _hard_block_remaining() -> float:
    """硬封锁剩余秒数(<=0 表示未封锁)。"""
    global _blocked_until
    with _cooldown_lock:
        return _blocked_until - time.monotonic()


def _reset_conn_streak() -> None:
    """请求成功后清零断连计数。"""
    global _conn_fail_streak
    with _cooldown_lock:
        _conn_fail_streak = 0


def _wait_out_cooldown() -> None:
    """若处于风控冷却期, 阻塞等待冷却结束(不发任何新请求)。"""
    global _cooldown_until
    while True:
        with _cooldown_lock:
            remaining = _cooldown_until - time.monotonic()
        if remaining <= 0:
            return
        logger.warning("eastmoney 风控冷却中, %.0f 秒后恢复请求...", remaining)
        time.sleep(min(remaining, 30.0))

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
# 快照主域名 + 官方延时行情域名(约15分钟延迟)降级路径。push2 触发 IP 风控时自动切换。
_SNAPSHOT_URLS = (
    "https://push2.eastmoney.com/api/qt/clist/get",
    "https://push2delay.eastmoney.com/api/qt/clist/get",
)
_DELAY_HOST_IDX = 1
_F10_BASE = "https://emweb.securities.eastmoney.com/PC_HSF10/"

# K线 fields2: f51..f61 = date,open,close,high,low,volume,amount,振幅,涨跌幅,涨跌额,换手率
_KLINE_FIELDS = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
# 快照: f12 代码 / f14 名称 / f2 最新价 / f3 涨跌幅% / f4 涨跌额 / f5 成交量(手) /
#       f6 成交额(元) / f7 振幅% / f8 换手率% / f15 最高 / f16 最低 / f17 今开 / f18 昨收
_SNAPSHOT_FIELDS = "f12,f14,f2,f3,f4,f5,f6,f7,f8,f15,f16,f17,f18"

# 全市场 A 股: 深主板A + 创业板 + 沪主板A + 科创板 + 北交所
_A_SHARE_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"

_SERIES_TTL = 600.0  # K线序列缓存秒数
_F10_TTL = 600.0     # F10 JSON 缓存秒数


class EastMoneyError(RuntimeError):
    """东财接口请求失败(重试耗尽)。"""


def code_to_market_prefix(code: str) -> str:
    """6 位代码 → secid 市场前缀: 沪市→'1', 深市/北交所→'0'。"""
    return "1" if code.startswith("6") else "0"


def symbol_to_secid(symbol: str) -> str:
    """内部 symbol (600519.SH / 000001.SZ / 830799.BJ) → secid (1.600519 / 0.000001)。"""
    sym = symbol.strip().upper()
    if "." in sym:
        code, _, suffix = sym.partition(".")
        return ("1" if suffix == "SH" else "0") + "." + code
    return code_to_market_prefix(sym) + "." + sym


def code_to_exchange(code: str) -> str:
    """6 位代码 → 交易所后缀 (SH/SZ/BJ)。"""
    if code.startswith("6"):
        return "SH"
    if code.startswith(("4", "8")) or code.startswith("92"):
        return "BJ"
    return "SZ"


def symbol_to_f10_code(symbol: str) -> str:
    """内部 symbol → F10 code (SH600519 / SZ000001 / BJ830799)。"""
    sym = symbol.strip().upper()
    if "." in sym:
        code, _, suffix = sym.partition(".")
        return suffix + code
    return code_to_exchange(sym) + sym


class EastMoneyClient:
    """节流 + 重试 + TTL 缓存的东财 HTTP 客户端。单实例线程安全(共享节流锁)。"""

    def __init__(
        self,
        min_interval: float = 0.25,
        f10_cache: dict[str, tuple[float, dict]] | None = None,
        series_cache: dict[tuple, tuple[float, list[str]]] | None = None,
    ) -> None:
        self._client = httpx.Client(
            headers={"User-Agent": _UA, "Referer": "https://quote.eastmoney.com/"},
            timeout=httpx.Timeout(15.0, connect=10.0),
            follow_redirects=True,
            # 传输层重试: 东财行情接口偶发 "Server disconnected" 断连,
            # 由传输层在单次 get 内部重试, 不消耗上层节流窗口
            transport=httpx.HTTPTransport(retries=2),
        )
        self._min_interval = min_interval
        self._lock = threading.Lock()
        self._last_request_ts = 0.0
        # 缓存可跨客户端共享(如财务并发 worker), 并发写同一 key 顶多多拉一次, 无一致性风险
        self._series_cache = series_cache if series_cache is not None else {}
        self._f10_cache = f10_cache if f10_cache is not None else {}
        # 当前一轮快照成功使用的域名索引(0=实时, 1=延时), 轮内记忆避免每页重试主域名
        self._snapshot_host_idx = 0

    def close(self) -> None:
        self._client.close()

    # ---- 底层请求 ----
    def _get_json(self, url: str, params: dict) -> dict:
        last_err: Exception | None = None
        plain_fails = 0
        cooldown_rounds = 0
        while True:
            blocked = _hard_block_remaining()
            if blocked > 0:
                raise EastMoneyError(
                    f"eastmoney IP 硬封锁中({int(blocked)}s 后恢复), 本轮直接跳过"
                )
            _wait_out_cooldown()  # 冷却期内阻塞, 不发起新请求
            with self._lock:
                wait = self._min_interval - (time.monotonic() - self._last_request_ts)
                if wait > 0:
                    time.sleep(wait)
                self._last_request_ts = time.monotonic()
            try:
                resp = self._client.get(url, params=params)
                resp.raise_for_status()
                _reset_conn_streak()
                return resp.json()
            except (httpx.HTTPError, ValueError) as e:
                last_err = e
                if _is_conn_error(e):
                    # 断连: 累计计数, 达阈值进入风控冷却, 冷却后重试同一请求(不丢弃)
                    if _note_conn_failure(cooldown_rounds):
                        cooldown_rounds += 1
                        if cooldown_rounds > _MAX_COOLDOWN_ROUNDS:
                            _enter_hard_block()
                            break
                        continue
                    time.sleep(1.0)  # 未达阈值: 短退避后重试
                    continue
                # 普通 HTTP 错误: 指数退避重试(最多 3 次)
                plain_fails += 1
                if plain_fails >= 3:
                    break
                time.sleep(1.0 * (2 ** plain_fails))
        raise EastMoneyError(f"GET {url} failed: {last_err}") from last_err

    # ---- K线 ----
    def kline_series(self, secid: str, klt: int, fqt: int, lmt: int | None = None) -> list[str]:
        """拉取 K 线原始行(逗号分隔字符串), 带 TTL 缓存。

        klt: 101=日线, 1/5/15/30/60=分钟线。fqt: 0=不复权, 1=前复权。
        lmt: 返回最近 N 条(None 时默认全量: 日K 1000000, 分钟K 12000)。
        """
        key = (secid, klt, fqt)
        cached = self._series_cache.get(key)
        if cached is not None and time.monotonic() - cached[0] < _SERIES_TTL:
            cached_rows = cached[1]
            # 缓存是全量数据: 如果调用方要的是子集, 截断返回
            if lmt is not None and len(cached_rows) > lmt:
                return cached_rows[-lmt:]
            return cached_rows
        if lmt is None:
            lmt = 1000000 if klt == 101 else 12000
        payload = self._get_json(_KLINE_URL, {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": _KLINE_FIELDS,
            "klt": str(klt),
            "fqt": str(fqt),
            "beg": "0",
            "end": "20500101",
            "lmt": str(lmt),
        })
        data = payload.get("data") or {}
        rows = list(data.get("klines") or [])
        self._series_cache[key] = (time.monotonic(), rows)
        return rows

    # ---- 全市场快照 ----
    def snapshot_page(self, pn: int, pz: int) -> dict:
        """单页快照。主域名失败自动降级延时域名, 并记忆本轮成功域名(轮内不再重试主源)。"""
        params = {
            "pn": str(pn),
            "pz": str(pz),
            "po": "1",
            "np": "1",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": _A_SHARE_FS,
            "fields": _SNAPSHOT_FIELDS,
        }
        last_err: Exception | None = None
        n = len(_SNAPSHOT_URLS)
        for step in range(n):
            idx = (self._snapshot_host_idx + step) % n
            try:
                payload = self._get_json(_SNAPSHOT_URLS[idx], params)
                self._snapshot_host_idx = idx
                return payload.get("data") or {}
            except EastMoneyError as e:
                last_err = e
                logger.warning("eastmoney snapshot host[%d] failed: %s", idx, e)
        raise EastMoneyError(f"all snapshot hosts failed: {last_err}") from last_err

    def snapshot_all(self) -> tuple[list[dict], bool]:
        """分页拉取全市场 A 股实时快照行。返回 (rows, delayed)。

        delayed=True 表示本轮降级到了延时行情源(push2delay, 约15分钟延迟),
        消费方应在数据中标注, 避免把延时行情当实时数据静默呈现。
        """
        self._snapshot_host_idx = 0  # 每轮从主源开始, 主源恢复即可自动回切实时
        page_size = 100  # clist 单页上限
        first = self.snapshot_page(1, page_size)
        rows = list(first.get("diff") or [])
        total = int(first.get("total") or 0)
        pages = (total + page_size - 1) // page_size
        for pn in range(2, pages + 1):
            page = self.snapshot_page(pn, page_size)
            rows.extend(page.get("diff") or [])
        return rows, self._snapshot_host_idx == _DELAY_HOST_IDX

    # ---- F10 财务 ----
    def f10(self, path: str, params: dict) -> dict:
        """GET PC_HSF10/{path}, 带 TTL 缓存。"""
        key = path + "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        cached = self._f10_cache.get(key)
        if cached is not None and time.monotonic() - cached[0] < _F10_TTL:
            return cached[1]
        payload = self._get_json(_F10_BASE + path, params)
        self._f10_cache[key] = (time.monotonic(), payload)
        return payload
