#!/usr/bin/env bash
# =============================================================================
# LangAlpha 一键启动（本地手动模式，无 Docker）
# =============================================================================
# 用法：
#   bash scripts/dev/start.sh          # 启动 backend + frontend
#   bash scripts/dev/start.sh --reload # 启动 + backend 热加载
#   bash scripts/dev/start.sh --backend-only
#   bash scripts/dev/start.sh --frontend-only
#
# 前置依赖（已在你机器上验证）：
#   - PostgreSQL@16（brew services start postgresql@16）
#   - Redis@7（brew services start redis）
#   - uv、node、pnpm
#   - .env 已配置（见 .env.bak.docker / 当前 .env）
#
# 此脚本不会进版本库（已加入 .gitignore: scripts/dev/）
# =============================================================================

set -euo pipefail

# ---------- 颜色 ----------
BOLD='\033[1m'; DIM='\033[2m'; GREEN='\033[32m'; CYAN='\033[36m'; YELLOW='\033[33m'; RED='\033[31m'; NC='\033[0m'
ok()    { printf "  ${GREEN}✓${NC} %s\n" "$1"; }
warn()  { printf "  ${YELLOW}⚠${NC} %s\n" "$1"; }
err()   { printf "  ${RED}✗${NC} %s\n" "$1"; }
info()  { printf "  ${DIM}%s${NC}\n" "$1"; }
step()  { printf "\n${BOLD}${CYAN}── %s ──${NC}\n" "$1"; }

# ---------- 路径 ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$REPO_ROOT/.codebuddy/logs"
PID_DIR="$REPO_ROOT/.codebuddy/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

BACKEND_PORT=8000
FRONTEND_PORT=5173

BACKEND_LOG="$LOG_DIR/backend.log"
FRONTEND_LOG="$LOG_DIR/frontend.log"
BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"

# ---------- 参数解析 ----------
RELOAD=false
START_BACKEND=true
START_FRONTEND=true

for arg in "$@"; do
    case "$arg" in
        --reload)         RELOAD=true ;;
        --backend-only)   START_FRONTEND=false ;;
        --frontend-only)  START_BACKEND=false ;;
        -h|--help)
            sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) err "未知参数: $arg"; exit 1 ;;
    esac
done

cd "$REPO_ROOT"

# ---------- 1. 前置依赖检查 ----------
step "1/4 前置依赖检查"

# .env
if [[ ! -f .env ]]; then
    err ".env 不存在！请先 cp .env.example .env 并填入必要 key"
    exit 1
fi
ok ".env 已就绪"

# uv / node / pnpm
for cmd in uv node pnpm; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        err "$cmd 未安装"
        exit 1
    fi
done
ok "uv $(uv --version | awk '{print $2}'), node $(node --version), pnpm $(pnpm --version)"

# Postgres
if ! pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
    warn "PostgreSQL 未启动，尝试 brew services start postgresql@16 ..."
    brew services start postgresql@16 >/dev/null 2>&1 || true
    sleep 2
    if ! pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
        err "PostgreSQL 启动失败，请手动检查"
        exit 1
    fi
fi
ok "PostgreSQL 已运行"

# Redis
if ! redis-cli ping >/dev/null 2>&1; then
    warn "Redis 未启动，尝试 brew services start redis ..."
    brew services start redis >/dev/null 2>&1 || true
    sleep 1
    if ! redis-cli ping >/dev/null 2>&1; then
        err "Redis 启动失败，请手动检查"
        exit 1
    fi
fi
ok "Redis 已运行"

# 端口冲突清理
clear_port() {
    local port=$1 label=$2
    local pid
    pid=$(lsof -ti:"$port" 2>/dev/null || true)
    if [[ -n "$pid" ]]; then
        warn "$label 端口 $port 被占用（PID $pid），清理中..."
        kill -9 $pid 2>/dev/null || true
        sleep 1
    fi
}

# ---------- 2. 装依赖（仅当缺失时） ----------
step "2/4 依赖检查"

if [[ ! -d "$REPO_ROOT/.venv" ]]; then
    info "首次运行，安装 Python 依赖..."
    uv sync --group dev --extra test
fi
ok "Python 虚拟环境就绪（.venv）"

if [[ ! -d "$REPO_ROOT/web/node_modules" ]]; then
    info "首次运行，安装前端依赖..."
    (cd web && pnpm install --frozen-lockfile)
fi
ok "前端依赖就绪（web/node_modules）"

# ---------- 3. 数据库 migration（幂等） ----------
step "3/4 数据库 migration"

# 默认从 .env 读 DB 配置
DB_USER=$(grep -E "^DB_USER=" .env | cut -d= -f2-)
DB_PASSWORD=$(grep -E "^DB_PASSWORD=" .env | cut -d= -f2-)
DB_NAME=$(grep -E "^DB_NAME=" .env | cut -d= -f2-)
DB_HOST=$(grep -E "^DB_HOST=" .env | cut -d= -f2-)
DB_PORT=$(grep -E "^DB_PORT=" .env | cut -d= -f2-)

# 创建数据库（不存在则建）
if ! PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1" >/dev/null 2>&1; then
    info "数据库 $DB_NAME 不存在，创建中..."
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;" >/dev/null
fi

# alembic upgrade（幂等）
uv run alembic upgrade head 2>&1 | tail -3 | sed 's/^/  /'
ok "数据库 schema 已同步"

# ---------- 4. 启动服务 ----------
step "4/4 启动服务"

start_backend() {
    clear_port "$BACKEND_PORT" "Backend"
    local extra_args=""
    if $RELOAD; then extra_args="--reload"; fi
    nohup uv run python server.py --host 0.0.0.0 --port "$BACKEND_PORT" $extra_args \
        > "$BACKEND_LOG" 2>&1 &
    echo $! > "$BACKEND_PID_FILE"
    ok "Backend 启动中（PID $(cat "$BACKEND_PID_FILE")）"

    # 健康检查（最多等 30 秒）
    info "等待 backend 就绪..."
    for i in {1..15}; do
        sleep 2
        if curl -sf "http://localhost:$BACKEND_PORT/health" >/dev/null 2>&1; then
            ok "Backend 健康检查通过：http://localhost:$BACKEND_PORT"
            return 0
        fi
    done
    err "Backend 在 30 秒内未就绪，查看日志：tail -f $BACKEND_LOG"
    return 1
}

start_frontend() {
    clear_port "$FRONTEND_PORT" "Frontend"
    (cd "$REPO_ROOT/web" && nohup pnpm dev --host 0.0.0.0 \
        > "$FRONTEND_LOG" 2>&1 &
        echo $! > "$FRONTEND_PID_FILE")
    ok "Frontend 启动中（PID $(cat "$FRONTEND_PID_FILE")）"

    info "等待 frontend 就绪..."
    for i in {1..10}; do
        sleep 1
        if curl -sf "http://localhost:$FRONTEND_PORT/" >/dev/null 2>&1; then
            ok "Frontend 健康检查通过：http://localhost:$FRONTEND_PORT"
            return 0
        fi
    done
    err "Frontend 在 10 秒内未就绪，查看日志：tail -f $FRONTEND_LOG"
    return 1
}

if $START_BACKEND;  then start_backend  || exit 1; fi
if $START_FRONTEND; then start_frontend || exit 1; fi

# ---------- 完成总结 ----------
echo
printf "${BOLD}${GREEN}🎉 启动完成${NC}\n\n"
$START_FRONTEND && printf "  Web UI:        ${CYAN}http://localhost:$FRONTEND_PORT/${NC}\n"
$START_BACKEND  && printf "  Backend API:   ${CYAN}http://localhost:$BACKEND_PORT/${NC}\n"
$START_BACKEND  && printf "  API 文档:      ${CYAN}http://localhost:$BACKEND_PORT/docs${NC}\n"
echo
printf "  ${DIM}日志：tail -f $LOG_DIR/{backend,frontend}.log${NC}\n"
printf "  ${DIM}停止：bash scripts/dev/stop.sh${NC}\n"
printf "  ${DIM}状态：bash scripts/dev/status.sh${NC}\n"
echo
