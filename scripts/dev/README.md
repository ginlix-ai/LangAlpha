# LangAlpha 本地开发脚本（不进版本库）

> 这个目录已加入 `.gitignore`，**不会被提交到 git**。
> 用于本机一键启停 LangAlpha（无 Docker 模式）。

## 一键启动

```bash
# 启动 backend + frontend
bash scripts/dev/start.sh

# 启动并开启 backend 热加载（开发推荐）
bash scripts/dev/start.sh --reload

# 只启动 backend / 只启动 frontend
bash scripts/dev/start.sh --backend-only
bash scripts/dev/start.sh --frontend-only
```

启动脚本自动完成：
1. ✅ 检查 `.env`、`uv`、`node`、`pnpm`
2. ✅ 自动启动 PostgreSQL@16 / Redis（如未启动）
3. ✅ 端口冲突自动清理（8000 / 5173）
4. ✅ 首次运行自动 `uv sync` + `pnpm install`
5. ✅ 自动建库 + 跑 alembic migration（幂等）
6. ✅ 健康检查（backend 等 30s，frontend 等 10s）

## 一键停止

```bash
# 只停 backend + frontend
bash scripts/dev/stop.sh

# 同时停 PostgreSQL + Redis
bash scripts/dev/stop.sh --all
```

## 查看状态

```bash
bash scripts/dev/status.sh
```

输出示例：
```
LangAlpha 服务状态

  ●  PostgreSQL          运行中  localhost:5432
  ●  Redis               运行中  localhost:6379
  ●  Backend             运行中  http://localhost:8000  (PID 30243)
  ●  Frontend            运行中  http://localhost:5173  (PID 32254)
```

## 日志位置

- Backend：`.codebuddy/logs/backend.log`
- Frontend：`.codebuddy/logs/frontend.log`
- PID 文件：`.codebuddy/pids/{backend,frontend}.pid`

## 环境前置

第一次跑前需要：
```bash
# 装基础设施（mac）
brew install postgresql@16 redis
brew services start postgresql@16
brew services start redis

# 创建 postgres 角色（mac brew 默认用 macOS 用户名作超级用户）
psql -d postgres -c "CREATE ROLE postgres LOGIN SUPERUSER PASSWORD 'postgres';"

# 配置 .env（参考根目录的 .env.example）
cp .env.example .env  # 然后编辑填 LLM key 和 FMP_API_KEY
```

## 注意事项

- `SANDBOX_PROVIDER=docker` 但你**没装 Docker** 时：backend 能启动，但**任何 PTC Agent 调用都会失败**。要跑 Agent 必须：
  - 装 Docker Desktop，或
  - 改成 `SANDBOX_PROVIDER=daytona` 并配置 `DAYTONA_API_KEY`
