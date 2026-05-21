#!/usr/bin/env bash
# =============================================================================
# LangAlpha 服务状态查询
# =============================================================================
# 用法：bash scripts/dev/status.sh

set -euo pipefail

GREEN='\033[32m'; RED='\033[31m'; YELLOW='\033[33m'; DIM='\033[2m'; CYAN='\033[36m'; BOLD='\033[1m'; NC='\033[0m'

print_status() {
    local label=$1 status=$2 detail=$3
    if [[ "$status" == "up" ]]; then
        printf "  ${GREEN}●${NC} %-20s ${GREEN}运行中${NC}  ${DIM}%s${NC}\n" "$label" "$detail"
    elif [[ "$status" == "warn" ]]; then
        printf "  ${YELLOW}●${NC} %-20s ${YELLOW}部分${NC}    ${DIM}%s${NC}\n" "$label" "$detail"
    else
        printf "  ${RED}○${NC} %-20s ${RED}未运行${NC}  ${DIM}%s${NC}\n" "$label" "$detail"
    fi
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

printf "\n${BOLD}LangAlpha 服务状态${NC}\n\n"

# PostgreSQL
if pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
    print_status "PostgreSQL" "up" "localhost:5432"
else
    print_status "PostgreSQL" "down" "未连接"
fi

# Redis
if redis-cli ping >/dev/null 2>&1; then
    print_status "Redis" "up" "localhost:6379"
else
    print_status "Redis" "down" "未连接"
fi

# Backend
if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
    pid=$(lsof -ti:8000 2>/dev/null | head -1)
    print_status "Backend" "up" "http://localhost:8000  (PID $pid)"
else
    print_status "Backend" "down" "http://localhost:8000"
fi

# Frontend
if curl -sf http://localhost:5173/ >/dev/null 2>&1; then
    pid=$(lsof -ti:5173 2>/dev/null | head -1)
    print_status "Frontend" "up" "http://localhost:5173  (PID $pid)"
else
    print_status "Frontend" "down" "http://localhost:5173"
fi

echo
printf "  ${DIM}日志：tail -f $REPO_ROOT/.codebuddy/logs/{backend,frontend}.log${NC}\n"
echo
