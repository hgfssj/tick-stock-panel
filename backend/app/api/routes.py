"""API 路由 — 健康检查、能力探测、数据同步。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from app import __version__
from app.config import settings
from app.tickflow import client as tf_client
from app.tickflow.policy import detect_capabilities, tier_label

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "version": __version__,
        # 三态: none(无key/无效) / free(免费key) / api_key(付费档)
        "mode": tf_client.current_mode(),
    }


@router.get("/api/capabilities")
def capabilities() -> dict:
    """前端用来决定哪些功能可用、哪些灰显。"""
    capset = detect_capabilities()
    return {
        "label": tier_label(),
        "capabilities": capset.to_dict(),
    }


@router.post("/api/capabilities/redetect")
def redetect() -> dict:
    """用户在设置页"重新检测"按钮。"""
    capset = detect_capabilities(force=True)
    return {
        "label": tier_label(),
        "capabilities": capset.to_dict(),
    }


@router.post("/api/system/sync")
def manual_sync(request: Request, action: str = Query("push", description="push | pull | status")) -> dict:
    """手动触发数据同步到 GitHub 中转仓库。

    - push: 提交并推送 data/ 变更
    - pull: 从 origin/main 拉取 data/ 更新
    - status: 查看同步状态
    """
    from app.services.data_sync import pull_on_startup, push_after_pipeline, sync_status

    if action == "push":
        push_after_pipeline(settings.data_dir)
        return {"action": "push", "status": "completed"}
    elif action == "pull":
        pull_on_startup(settings.data_dir, delay=0)
        return {"action": "pull", "status": "completed"}
    elif action == "status":
        return {"action": "status", **sync_status(settings.data_dir)}
    else:
        raise HTTPException(status_code=400, detail=f"unknown action: {action}")
