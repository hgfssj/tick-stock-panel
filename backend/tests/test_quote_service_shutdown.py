"""进程退出不得洗掉实时行情开关偏好的回归测试。

复现路径: lifespan shutdown 调 qs.stop() 清理线程; 此前 stop() 内含
_save_enabled(False), uvicorn --reload 热重载会把用户开启的
realtime_quotes_enabled 偏好静默洗成 False, 重启后 boot_check 读到
False 不再启动行情服务。持久化用户意图应只发生在 enable()/disable()。
"""
from app.services.quote_service import QuoteService


def _record_saves(monkeypatch):
    calls: list[bool] = []
    monkeypatch.setattr(
        QuoteService, "_save_enabled", staticmethod(lambda v: calls.append(v))
    )
    return calls


def test_stop_does_not_persist(monkeypatch):
    """stop() 只做资源清理(线程停止), 不写 preferences。"""
    calls = _record_saves(monkeypatch)
    QuoteService().stop()
    assert calls == []


def test_disable_persists_off(monkeypatch):
    """用户主动 disable() 仍持久化关闭。"""
    calls = _record_saves(monkeypatch)
    QuoteService().disable()
    assert calls == [False]
