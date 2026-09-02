#!/usr/bin/env bash
set -euo pipefail
# scripts/sync-data.sh — 手动数据同步工具
# 用法:
#   ./scripts/sync-data.sh pull    从 GitHub 拉取 data/
#   ./scripts/sync-data.sh push    推送 data/ 到 GitHub
#   ./scripts/sync-data.sh status  查看同步状态

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# ---- git 身份 (从环境变量或已有 git config 读取) ----
GIT_NAME="${GIT_AUTHOR_NAME:-${GIT_COMMITTER_NAME:-}}"
GIT_EMAIL="${GIT_AUTHOR_EMAIL:-${GIT_COMMITTER_EMAIL:-}}"
GIT_ID_ARGS=()
if [ -z "$GIT_NAME" ]; then
    GIT_NAME="$(git config --get user.name 2>/dev/null || true)"
fi
if [ -z "$GIT_EMAIL" ]; then
    GIT_EMAIL="$(git config --get user.email 2>/dev/null || true)"
fi
if [ -n "$GIT_NAME" ]; then
    GIT_ID_ARGS+=(-c "user.name=$GIT_NAME")
fi
if [ -n "$GIT_EMAIL" ]; then
    GIT_ID_ARGS+=(-c "user.email=$GIT_EMAIL")
fi

pull() {
    echo "=== Pulling data/ from origin/main ==="
    git fetch origin main
    behind=$(git rev-list HEAD..origin/main --count 2>/dev/null || echo "0")
    if [ "$behind" -eq 0 ]; then
        echo "Already up to date."
        return
    fi
    echo "Behind by $behind commits. Merging..."
    if git merge origin/main --no-edit; then
        echo "Pull succeeded (fast-forward or clean merge)."
    else
        echo "Merge conflict detected. Resolving data/ with local (ours)..."
        git checkout --ours -- data/
        git add data/
        git commit "${GIT_ID_ARGS[@]}" -m "sync: merge data/ (keep local)" || true
        echo "Conflict resolved: data/ kept local version."
    fi
}

push() {
    echo "=== Pushing data/ to origin/main ==="
    git add data/

    if git diff --cached --quiet; then
        echo "No changes to push."
        return
    fi

    TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    git commit "${GIT_ID_ARGS[@]}" -m "sync: data update $TS" || true

    if git push origin main; then
        echo "Push succeeded."
    else
        echo "Push failed. Rolling back local commit..."
        git reset --soft HEAD~1
        echo "Local commit rolled back. Changes remain staged — fix and retry push."
        exit 1
    fi
}

status() {
    echo "=== Sync Status ==="
    echo "Branch: $(git branch --show-current)"
    git fetch origin main 2>/dev/null || true
    behind=$(git rev-list HEAD..origin/main --count 2>/dev/null || echo "?")
    ahead=$(git rev-list origin/main..HEAD --count 2>/dev/null || echo "?")
    echo "Behind origin/main: $behind commits"
    echo "Ahead of origin/main: $ahead commits"
    echo ""
    echo "Uncommitted data/ changes:"
    git status --short data/ || true
}

case "${1:-}" in
    pull)   pull ;;
    push)   push ;;
    status) status ;;
    *)      echo "Usage: $0 {pull|push|status}" ; exit 1 ;;
esac