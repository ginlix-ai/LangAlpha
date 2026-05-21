# 本地部署指南（无 Docker）

> **适用场景**：macOS / Linux 开发机，无法或不想安装 Docker，使用 `memory` sandbox provider 本地运行完整平台。

## 架构概览

```
┌──────────────────────────────────────────────────────────────┐
│                      本地部署架构                               │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌────────────────────────────┐ │
│  │PostgreSQL│  │  Redis   │  │  Backend (uvicorn:8000)    │ │
│  │  @16     │  │  @7      │  │  ├─ FastAPI (REST + SSE)   │ │
│  │ brew svc │  │ brew svc │  │  ├─ Agent + Memory Sandbox │ │
│  └──────────┘  └──────────┘  │  └─ MCP stdio subprocesses │ │
│                              └────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  Frontend (Vite:5173)                                  │  │
│  │  React 19 + TypeScript + TailwindCSS                   │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  Sandbox: <项目>/.local_sandboxes/<runtime-id>/               │
│  ├── agent.md          (workspace context)                   │
│  ├── work/             (agent analysis outputs: D1-D7 json)  │
│  ├── data/             (fetched raw financial data)           │
│  ├── results/          (final reports)                       │
│  ├── tools/            (MCP tool wrappers)                   │
│  └── mcp_servers/      (MCP server scripts)                  │
└──────────────────────────────────────────────────────────────┘
```

## 前置依赖

| 工具 | 版本 | 安装方式 |
|------|------|---------|
| PostgreSQL | 16+ | `brew install postgresql@16` |
| Redis | 7+ | `brew install redis` |
| Python | 3.12+ | 系统自带或 pyenv |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Node.js | 20+ | `brew install node` 或 nvm |
| pnpm | 9+ | `npm i -g pnpm` |

## 快速开始

### 1. 一键启动

```bash
# 完整启动（检查依赖 → 启动 PG/Redis → migration → backend + frontend）
bash scripts/dev/start.sh --reload

# 仅启动后端
bash scripts/dev/start.sh --backend-only --reload

# 仅启动前端
bash scripts/dev/start.sh --frontend-only
```

### 2. 查看状态

```bash
bash scripts/dev/status.sh
```

输出示例：
```
LangAlpha 服务状态

  ● PostgreSQL          运行中  localhost:5432
  ● Redis               运行中  localhost:6379
  ● Backend             运行中  http://localhost:8000  (PID 12345)
  ● Frontend            运行中  http://localhost:5173  (PID 12346)
```

### 3. 停止服务

```bash
# 停 backend + frontend
bash scripts/dev/stop.sh

# 全部停止（含 PostgreSQL + Redis）
bash scripts/dev/stop.sh --all
```

## 环境配置

### .env 关键配置

```bash
# ======== Sandbox（核心区别）========
SANDBOX_PROVIDER=memory          # 本地进程沙箱，无需 Docker

# ======== 基础设施 ========
DB_HOST=localhost
DB_PORT=5432
DB_USER=<your_pg_user>           # 通常是系统用户名
DB_PASSWORD=<password>
DB_NAME=langalpha

REDIS_URL=redis://localhost:6379

# ======== LLM（至少配一个）========
DASHSCOPE_API_KEY=sk-xxx         # 阿里通义
# 或
OPENAI_API_KEY=sk-xxx            # OpenAI
# 或
ANTHROPIC_API_KEY=sk-xxx         # Anthropic

# ======== 可选 ========
FMP_API_KEY=xxx                  # 金融数据（Financial Modeling Prep）
SERPER_API_KEY=xxx               # Web Search
AUTH_USER_ID=local-dev-user      # 本地开发跳过认证
```

### 首次配置向导

```bash
make config
# 或直接
bash scripts/configure.sh
```

交互式向导会引导配置 LLM、金融数据源、搜索、存储等。

## Memory Sandbox Provider 说明

### 工作原理

Memory provider 在本地用**子进程**代替 Docker/Daytona 容器：

- Agent 生成的 Python 代码直接通过 `asyncio.subprocess` 在宿主机执行
- 工作目录位于项目根目录下 `.local_sandboxes/local-<hash>/`
- 复用宿主机的 Python venv（无需重复安装 pandas/numpy 等）
- 启动时间 < 1s（无容器启动开销）
- 服务重启后自动从磁盘恢复 sandbox 连接

### Sandbox 目录位置

默认查找优先级：
1. `LANGALPHA_LOCAL_SANDBOX_DIR` 环境变量（显式覆盖）
2. **项目根目录 `.local_sandboxes/`**（推荐，与项目共存）
3. `~/.codebuddy/local-sandboxes/`（全局兜底）

sandbox 目录已加入 `.gitignore`，不会被提交。

### 虚拟路径映射

| 虚拟路径 (agent 视角) | 真实路径 (宿主机) |
|---|---|
| `/home/workspace/` | `<项目>/.local_sandboxes/local-<id>/` |
| `/home/workspace/work/` | `<项目>/.local_sandboxes/local-<id>/work/` |
| `/home/workspace/data/` | `<项目>/.local_sandboxes/local-<id>/data/` |
| `/home/workspace/agent.md` | `<项目>/.local_sandboxes/local-<id>/agent.md` |

路径重写在 `LocalRuntime._rewrite_command()` 中完成，对 agent 完全透明。
包括 base64 编码的命令（如 `aglob_files`）也能正确处理。

### 与线上（Daytona）的差异

| 特性 | Memory (本地) | Daytona (线上) |
|------|--------------|----------------|
| 隔离性 | ❌ 无隔离（共享宿主机） | ✅ 完全隔离（MicroVM） |
| 启动速度 | < 1s | ~10s |
| 持久化 | ✅ 磁盘文件（项目目录下） | ✅ 云端快照 |
| Port forwarding | ❌ | ✅ |
| 安全性 | ⚠️ 仅信任环境 | ✅ 生产安全 |
| 多用户 | ❌ 单用户 | ✅ 多租户 |

### 清理沙箱数据

```bash
# 查看所有本地沙箱
ls -la .local_sandboxes/

# 删除所有沙箱（workspace 文件数据会丢失，DB 中的 payload 不受影响）
rm -rf .local_sandboxes/local-*

# 查看单个沙箱的工作数据
find .local_sandboxes/local-<id>/work -type f
find .local_sandboxes/local-<id>/data -type f
```

## 数据库管理

```bash
# 运行所有迁移
uv run alembic upgrade head

# 查看当前版本
uv run alembic current

# 新建迁移
uv run alembic revision -m "description"

# 回滚一步
uv run alembic downgrade -1
```

## 日志和调试

```bash
# 实时日志
tail -f .codebuddy/logs/backend.log
tail -f .codebuddy/logs/frontend.log

# 单独启动 backend（前台，看完整输出）
uv run python server.py --reload

# 单独启动 frontend
cd web && pnpm dev
```

## 常见问题

### Q: 文件面板为什么是空的？

Memory sandbox 在 agent 运行结束后才有文件。如果是新创建的 workspace 且 agent 还没跑完分析，`data/`、`work/` 目录是空的。等 agent 完成后刷新即可看到文件。

### Q: PostgreSQL 连不上？

```bash
# 检查是否在运行
pg_isready -h localhost -p 5432

# 启动
brew services start postgresql@16

# 查看用户名（通常是系统用户）
psql -U $(whoami) -d postgres -c "SELECT 1"
```

### Q: Agent 运行报错 "module not found"？

Memory sandbox 使用宿主机的 Python 环境。确保项目的 MCP 依赖已安装：

```bash
uv sync --group dev --extra test
```

### Q: 切换回 Docker 模式？

```bash
# .env 中修改
SANDBOX_PROVIDER=docker

# 需要 Docker Desktop 运行
docker build -f Dockerfile.sandbox -t langalpha-sandbox .
make up PROVIDER=docker
```

## 目录结构

```
<项目根>/
├── .local_sandboxes/                     # Memory sandbox 数据（gitignored）
│   ├── local-<hash1>/
│   │   ├── agent.md                     # workspace context
│   │   ├── work/                        # agent 分析产出
│   │   │   └── sirius_xxx/
│   │   │       ├── d1_result.json
│   │   │       ├── d2_result.json
│   │   │       └── ...
│   │   ├── data/                        # 原始数据
│   │   ├── results/                     # 最终报告
│   │   ├── tools/                       # MCP tool wrappers
│   │   └── mcp_servers/                 # MCP server scripts
│   └── local-<hash2>/
│       └── ...
│
├── .codebuddy/                           # IDE 工作目录（勿删）
│   ├── logs/
│   │   ├── backend.log
│   │   └── frontend.log
│   └── pids/
│       ├── backend.pid
│       └── frontend.pid
│
├── scripts/dev/
│   ├── start.sh                         # 一键启动
│   ├── stop.sh                          # 一键停止
│   └── status.sh                        # 服务状态
│
└── .env                                 # 环境配置（SANDBOX_PROVIDER=memory）
```
