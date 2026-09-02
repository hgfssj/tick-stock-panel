"""GitHub 中转多机数据同步模块。

两个核心方法:
  pull_on_startup() — 启动时后台 git pull (仅 data/ 目录)
  push_after_pipeline() — 盘后管道完成后 git add/commit/push

并发安全: pull 在启动早期执行，写入尚未开始；push 在管道完成后、quote_service
暂停期间执行，不存在写入竞争。
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_GIT_TIMEOUT = 120  # git 操作超时秒数


def pull_on_startup(data_dir: Path, delay: float = 3.0) -> None:
    """启动时延迟执行 git pull，失败仅警告不阻塞。

    通过 threading.Timer 调用，在服务启动 2-3 秒后执行。
    """
    if delay > 0:
        time.sleep(delay)

    repo_root = _repo_root()
    if repo_root is None:
        logger.warning("data sync: 无法定位仓库根目录，跳过 pull")
        return

    logger.info("data sync: pulling from origin/main...")
    ok = _do_pull(repo_root)
    if ok:
        logger.info("data sync: pull 完成")
    else:
        logger.warning("data sync: pull 失败，启动继续")


def push_after_pipeline(data_dir: Path) -> None:
    """盘后管道完成后执行 git add + commit + push，失败记录告警。"""
    repo_root = _repo_root()
    if repo_root is None:
        logger.warning("data sync: 无法定位仓库根目录，跳过 push")
        return

    # 检查是否有远程
    code, stdout, _ = _run_git(["remote", "get-url", "origin"], repo_root)
    if code != 0 or not stdout.strip():
        logger.warning("data sync: 无 origin 远程，跳过 push")
        return

    logger.info("data sync: 准备推送 data/ 变更...")
    ok = _do_push(repo_root)
    if ok:
        logger.info("data sync: push 完成")
    else:
        logger.warning("data sync: push 失败，将在下次管道成功时重试")


def sync_status(data_dir: Path) -> dict:
    """返回同步状态信息供调试/API 使用。"""
    repo_root = _repo_root()
    if repo_root is None:
        return {"error": "无法定位仓库根目录"}

    branch = _run_git(["branch", "--show-current"], repo_root)[1].strip()
    _run_git(["fetch", "origin", "main"], repo_root, timeout=30)

    behind = _run_git(["rev-list", "HEAD..origin/main", "--count"], repo_root)[1].strip()
    ahead = _run_git(["rev-list", "origin/main..HEAD", "--count"], repo_root)[1].strip()
    changed = _run_git(["status", "--short", "data/"], repo_root)[1].strip()

    return {
        "branch": branch,
        "behind": int(behind) if behind.isdigit() else -1,
        "ahead": int(ahead) if ahead.isdigit() else -1,
        "changed_files": changed if changed else None,
    }


# ── 内部实现 ──────────────────────────────────────────────

def _repo_root() -> Path | None:
    """通过 git rev-parse 定位仓库根目录。"""
    code, stdout, stderr = _run_git(
        ["rev-parse", "--show-toplevel"],
        Path.cwd(),
        timeout=10,
    )
    if code == 0 and stdout.strip():
        return Path(stdout.strip())
    return None


def _run_git(
    args: list[str], cwd: Path, timeout: int = _GIT_TIMEOUT, env: dict | None = None
) -> tuple[int, str, str]:
    """执行 git 命令，返回 (returncode, stdout, stderr)。"""
    git_env = os.environ.copy()
    if env:
        git_env.update(env)
    # 禁用交互式提示
    git_env.setdefault("GIT_TERMINAL_PROMPT", "0")

    try:
        r = subprocess.run(
            ["git"] + args,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=git_env,
        )
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        logger.warning("git %s 超时 (%ds)", args[0], timeout)
        return -1, "", "timeout"
    except FileNotFoundError:
        logger.warning("git 命令不可用")
        return -1, "", "git not found"
    except Exception as e:
        logger.warning("git %s 异常: %s", args[0], e)
        return -1, "", str(e)


def _git_identity_args() -> list[str]:
    """返回 git -c 身份参数，优先环境变量，fallback 到已有配置。

    不写入 git config，仅通过 -c 一次性覆盖。
    """
    args: list[str] = []
    name = os.environ.get("GIT_AUTHOR_NAME") or os.environ.get("GIT_COMMITTER_NAME")
    email = os.environ.get("GIT_AUTHOR_EMAIL") or os.environ.get("GIT_COMMITTER_EMAIL")

    if not name:
        code, stdout, _ = _run_git(["config", "--get", "user.name"], Path.cwd(), timeout=5)
        if code == 0:
            name = stdout.strip()
    if not email:
        code, stdout, _ = _run_git(["config", "--get", "user.email"], Path.cwd(), timeout=5)
        if code == 0:
            email = stdout.strip()

    if name:
        args.extend(["-c", f"user.name={name}"])
    if email:
        args.extend(["-c", f"user.email={email}"])
    return args


def _do_pull(repo_root: Path) -> bool:
    """执行 git pull，冲突时 data/ 本地优先。

    步骤:
      1. git fetch origin main
      2. 检查落后情况
      3. 若落后: git merge (fast-forward 优先)
      4. 若冲突: data/ 文件取本地 (--ours), 非 data/ 文件 abort
    """
    # fetch
    code, _, stderr = _run_git(["fetch", "origin", "main"], repo_root, timeout=60)
    if code != 0:
        logger.warning("data sync: fetch 失败: %s", stderr.strip())
        return False

    # 检查是否落后
    code, behind_str, _ = _run_git(
        ["rev-list", "HEAD..origin/main", "--count"], repo_root, timeout=10
    )
    if code != 0 or not behind_str.strip().isdigit():
        return False
    behind = int(behind_str.strip())
    if behind == 0:
        return True  # 已是最新

    logger.info("data sync: 落后 %d 个提交，开始合并", behind)

    # 尝试 merge
    code, _, stderr = _run_git(
        _git_identity_args() + ["merge", "origin/main", "--no-edit"],
        repo_root,
        timeout=60,
    )
    if code == 0:
        return True

    # 冲突处理
    if "CONFLICT" in stderr or "conflict" in stderr.lower():
        logger.warning("data sync: merge 冲突，data/ 取本地 (ours)")
        # 检查是否只有 data/ 冲突
        code2, conflict_files, _ = _run_git(
            ["diff", "--name-only", "--diff-filter=U"], repo_root, timeout=10
        )
        if code2 == 0 and conflict_files.strip():
            only_data = all(
                f.strip().startswith("data/") for f in conflict_files.strip().splitlines()
            )
            if only_data:
                _run_git(["checkout", "--ours", "--", "data/"], repo_root, timeout=10)
                _run_git(["add", "data/"], repo_root, timeout=10)
                _run_git(
                    _git_identity_args()
                    + ["commit", "-m", "sync: merge data/ (keep local)"],
                    repo_root,
                    timeout=10,
                )
                return True

        # 非 data/ 冲突或有其他文件冲突 → abort
        _run_git(["merge", "--abort"], repo_root, timeout=10)
        logger.warning("data sync: 非 data/ 文件冲突，已 abort merge")
        return False

    logger.warning("data sync: merge 失败: %s", stderr.strip())
    return False


def _do_push(repo_root: Path) -> bool:
    """执行 git add + commit + push。

    步骤:
      0. 先合并远程(本地落后时 push 会被拒)
      1. git add data/
      2. 检查 staged 变更
      3. 有变更: git commit
      4. git push origin main
    """
    # 本地落后远程时, push 会被拒 (non-fast-forward), 先合并
    if not _do_pull(repo_root):
        logger.warning("data sync: 合并远程失败, 跳过 push (下次管道成功后重试)")
        return False

    # stage
    code, _, stderr = _run_git(["add", "data/"], repo_root, timeout=30)
    if code != 0:
        logger.warning("data sync: git add 失败: %s", stderr.strip())
        return False

    # 检查是否有变更
    code, _, _ = _run_git(["diff", "--cached", "--quiet"], repo_root, timeout=10)
    if code == 0:
        # 无变更
        return True

    # 检查 git 身份是否可用 (全局配置可能为空字符串)
    if not _git_identity_args():
        logger.warning(
            "data sync: 未配置 git 身份 (user.name/user.email), 跳过 push。"
            "请在仓库内执行: git config user.name 'xxx' && git config user.email 'xxx@yy.com'"
        )
        _run_git(["reset", "HEAD", "data/"], repo_root, timeout=10)
        return False

    # commit
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    code, _, stderr = _run_git(
        _git_identity_args() + ["commit", "-m", f"sync: data update {ts}"],
        repo_root,
        timeout=30,
    )
    if code != 0:
        logger.warning("data sync: commit 失败: %s", stderr.strip())
        _run_git(["reset", "HEAD", "data/"], repo_root, timeout=10)
        return False

    # push
    code, _, stderr = _run_git(["push", "origin", "main"], repo_root, timeout=120)
    if code != 0:
        logger.warning("data sync: push 失败: %s", stderr.strip())
        # 回滚本地 commit，数据变更保留在 staging
        _run_git(["reset", "--soft", "HEAD~1"], repo_root, timeout=10)
        return False

    return True