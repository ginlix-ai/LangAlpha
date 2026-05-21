#!/usr/bin/env bash
# =============================================================================
# LangAlpha 一键停止
# =============================================================================
# 用法：
#   bash scripts/dev/stop.sh             # 停掉 backend + frontend
#   bash scripts/dev/stop.sh --all       # 同时停掉 PostgreSQL + Redis（brew services）
# =============================================================================

set -euo pipefail

GREEN='\033[32m'; YELLOW='\033[33m'; DIM='\033[2m'; NC='\033[0m'
ok()   { printf "  ${GREEN}✓${NC} %s\n" "$1"; }
warn() { printf "  ${YELLOW}⚠${NC} %s\n" "$1"; }
info() { printf "  ${DIM}%s${NC}\n" "$1"; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PID_DIR="$REPO_ROOT/.codebuddy/pids"

STOP_INFRA=false
[[ "${1:-}" == "--all" ]] && STOP_INFRA=true

# 用 PID 文件停（精准）
stop_pid_file() {
    local pid_file=$1 label=$2
    if [[ -f "$pid_file" ]]; then
        local pid
        pid=$(cat "$pid_file" 2>/dev/null || true)
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -9 "$pid" 2>/dev/null || true
            ok "$label 已停止（PID $pid）"
        else
            warn "$label PID 文件存在但进程已不在"
        fi
        rm -f "$pid_file"
    fi
}

# 兜底：按端口杀（防止 PID 文件丢失）
kill_by_port() {
    local port=$1 label=$2
    local pid
    pid=$(lsof -ti:"$port" 2>/dev/null || true)
    if [[ -n "$pid" ]]; then
        kill -9 $pid 2>/dev/null || true
        ok "$label 端口 $port 已清理（PID $pid）"
    fi
}

stop_pid_file "$PID_DIR/backend.pid"  "Backend"
stop_pid_file "$PID_DIR/frontend.pid" "Frontend"
kill_by_port 8000 "Backend"
kill_by_port 5173 "Frontend"

if $STOP_INFRA; then
    info "停掉 PostgreSQL@16 + Redis（brew services）..."
    brew services stop postgresql@16 >/dev/null 2>&1 && ok "PostgreSQL 已停止" || warn "PostgreSQL 停止失败"
    brew services stop redis           >/dev/null 2>&1 && ok "Redis 已停止"      || warn "Redis 停止失败"
fi

echo
printf "${GREEN}全部停止完成${NC}\n"
