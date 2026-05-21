# LangAlpha 项目深度解析

> 写给：要在 LangAlpha 之上做"自动化公司估值分析平台"的工程师
> 目标：读完本文，你应该能回答"这个项目每一坨代码到底干啥用、能不能复用"

---

## 一、这个项目本质是什么

LangAlpha 是 **Ginlix 金融研究平台的核心 Agent 服务**。一句话定位：

> 把"investing as one-shot Q&A"变成"investing as a persistent codebase"——给 Agent 一个**持久化 workspace（云沙箱 + Postgres state）**，让研究像写代码一样**层层累积**。

它的核心差异化技术叫 **PTC (Programmatic Tool Calling)**：LLM 不直接调用 JSON tool，而是**写 Python 代码**，丢到 Daytona 云沙箱里执行，沙箱里再去调 MCP server 拿金融数据。这样可以做复杂多步分析、产图表，还不会撑爆 LLM context。

**对我们的意义**：LangAlpha 已经把"让 Agent 自动跑一个长流程任务并把过程持久化"做完了。我们要做的"对美图科技自动估值"，本质就是**把现有 sirius-valuation skill 包装成一条自动化任务**，几乎不用动 Agent 内核。

---

## 二、整体架构分层

```
┌──────────────────────────────────────────────────────────────┐
│  Web (web/, React19+Vite)  /  CLI(libs/ptc-cli)              │  ← 表现层
└──────────────────────────────────────────────────────────────┘
                          │ REST + SSE + WebSocket
┌──────────────────────────────────────────────────────────────┐
│  FastAPI Backend (src/server/)                                │
│  ├─ app/        routers (threads, workspaces, automations…)  │
│  ├─ handlers/   业务编排（chat, workflow, automation）        │
│  ├─ services/   workspace_manager / automation_executor …    │
│  └─ database/   raw psycopg3 SQL                             │
└──────────────────────────────────────────────────────────────┘
                          │
┌──────────────────────────────────────────────────────────────┐
│  Agent Core (src/ptc_agent/)                                  │
│  ├─ agent.py            create_agent() + 25 层 middleware    │
│  ├─ subagents/          general-purpose / research           │
│  ├─ core/sandbox/       sandbox runtime（daytona/docker）    │
│  └─ core/session/       SessionManager 管沙箱生命周期        │
└──────────────────────────────────────────────────────────────┘
                          │
┌──────────────────────────────────────────────────────────────┐
│  Sandbox (Daytona Cloud / 本地 Docker)                        │
│  执行 execute_code → import tools.* → 调 MCP server          │
└──────────────────────────────────────────────────────────────┘
                          │
┌──────────────────────────────────────────────────────────────┐
│  MCP Servers (mcp_servers/) stdio 子进程                      │
│  price_data / fundamentals / macro / options …               │
└──────────────────────────────────────────────────────────────┘
                          │
┌──────────────────────────────────────────────────────────────┐
│  PostgreSQL（双 pool）   +   Redis（SSE buffer / 缓存 / 转向）│
└──────────────────────────────────────────────────────────────┘
```

**关键设计原则**：
- **Agent 不用 StateGraph，用 deepagents 的 `create_agent()`** + 25 层 middleware 装配
- **Workspace ↔ Sandbox 严格 1:1**（每个研究方向独立沙箱）
- **DB 无 ORM**：全部 raw SQL via psycopg3
- **Agent 行为由 middleware 编排**（不是图节点）

---

## 三、目录速查表（哪些必须懂、哪些可以不管）

| 目录 / 文件 | 作用 | 关注度 |
|---|---|---|
| **`server.py`** | 入口，启动 FastAPI uvicorn | ★★★ |
| **`agent_config.yaml`** | Agent 能力：LLM、MCP server、subagent、sandbox provider | ★★★ |
| **`config.yaml`** | 基础设施：CORS、Redis TTL、workflow 超时 | ★★ |
| **`.env` / `.env.example`** | 凭证（DB、Redis、API key、LLM key） | ★★★ |
| **`Makefile`** | `make config` / `make up` / `make migrate` | ★★ |
| **`docker-compose.yml`** | 一键起 PG + Redis + backend + frontend | ★★★ |
| **`Dockerfile.sandbox`** | 沙箱镜像（provider=docker 时用） | ★★ |
| **`deploy/Dockerfile.*`** | backend / web / dev 三个生产镜像 | ★★★ |

### 后端 `src/server/`

| 路径 | 作用 | 关注度 |
|---|---|---|
| **`app/`** | **所有 REST 路由** — 你要加新 API 就在这里加 | ★★★ |
| `app/workspaces.py` | workspace CRUD | ★★★ |
| `app/threads.py` | 对话线程、SSE 流式 chat | ★★★ |
| `app/automations.py` | 定时 / 价格触发任务 CRUD | ★★★ |
| `app/skills.py` | 列出可用 skills | ★★ |
| `app/vault.py` | 工作区密钥保险柜 | ★ |
| `app/setup.py` | 注册所有 router，是新增路由必改的入口 | ★★★ |
| **`services/`** | **业务服务（核心引擎）** | ★★★ |
| `services/workspace_manager.py` | workspace ↔ sandbox 1:1、懒启动 + 空闲停机 | ★★★ |
| `services/automation_executor.py` | 单次自动化执行核心：构造 ChatRequest → 喂 Agent → 排干 SSE | ★★★ |
| `services/automation_scheduler.py` | cron 调度器 | ★★★ |
| `services/background_task_manager.py` | `asyncio.shield` 包住的后台任务（HTTP 断也不停） | ★★ |
| `services/workflow_tracker.py` | 流程追踪 | ★ |
| `services/llm_service.py` | 服务端一次性 LLM 调用（必走 BYOK） | ★ |
| **`handlers/chat/`** | `astream_ptc_workflow / astream_flash_workflow` | ★★★ |
| `handlers/automation_handler.py` | automation router 的业务实现 | ★★ |
| **`database/`** | raw psycopg3 SQL，每个表一个文件 | ★★ |
| **`models/`** | Pydantic 请求/响应模型 | ★★ |

### Agent 内核 `src/ptc_agent/`

| 路径 | 作用 | 关注度 |
|---|---|---|
| `agent/agent.py` | `PTCAgent.create_agent()` 装配 tools + 25 层 middleware | ★★ |
| `agent/middleware/` | 中间件链：注入 agent.md、加载 skill、压缩 context、HITL... | ★★ |
| `agent/middleware/skills.py` | `LoadSkill` 工具实现 + `list_skills()` | ★★★ |
| `agent/subagents/` | general-purpose / research 子 Agent | ★ |
| `agent/flash/` | Flash 模式（无沙箱、轻量） | ★ |
| `core/sandbox/` | Daytona / Docker 沙箱抽象 | ★ |
| `core/session/` | SessionManager 管沙箱生命周期 | ★ |

### 其他

| 路径 | 作用 | 关注度 |
|---|---|---|
| **`skills/`** | **23+ 预制研究 skill**，每个一个 `SKILL.md` | ★★★ |
| **`skills/sirius-valuation/`** | **我们自己的七维度估值 skill（D1-D7）** | ★★★ |
| `mcp_servers/` | MCP 数据源子进程（price/fundamentals/macro/options） | ★ |
| `migrations/versions/` | alembic 迁移（17 张应用表 + LangGraph checkpoint） | ★★ |
| `web/src/pages/` | 前端各页面，参考 `Automations/` 和 `Dashboard/` 写法 | ★★★ |

---

## 四、数据库 schema 关键

```
users
 └── workspaces (1:N) ── 每个 workspace 1:1 一个 Daytona sandbox
       └── conversation_threads (1:N)
             ├── conversation_queries
             ├── conversation_responses (sse_events JSONB，可回放)
             └── conversation_usages
 └── automations (定时任务) ── automation_executions（执行历史）
 └── user_api_keys / user_oauth_tokens (pgcrypto 加密)
 └── watchlists / user_portfolios
 └── market_insights (生成的洞察)
```

**关键点**：
- 没有 ORM，全部 raw SQL via psycopg3
- LangGraph checkpoint 用**独立 pool** 存（隔离应用数据与 Agent state）
- 所有敏感字段（API key、OAuth token）走 pgcrypto 加密

---

## 五、数据流（两条主线必须吃透）

### 5.1 人工对话（用户发消息）

```
POST /api/v1/threads/{id}/messages
  → threads.py (router)
  → handlers/chat/ (resolve LLM, 计费)
  → build_ptc_graph_with_session()      ← 从 WorkspaceManager 拿 sandbox session
  → BackgroundSubagentOrchestrator.astream()
  → SSE events (text_chunk / tool_calls / artifact ...)
  → 客户端流式接收（Redis 同时缓冲，断线重连可回放）
```

### 5.2 自动化运行（我们最关心）

```
AutomationScheduler (定时触发 / 手动 POST /trigger)
  → AutomationExecutor.execute(automation, execution_id)
     ├─ 取 workspace_id（PTC 必须有 workspace；Flash 自动建）
     ├─ 决定 thread_id（new / continue 同一线程）
     ├─ 构造 ChatRequest（instruction 即作为 prompt）
     ├─ 调 astream_ptc_workflow / astream_flash_workflow
     └─ 排干 generator → 更新 execution 状态 → webhook 通知
```

### 🎯 核心洞察

**自动化本质就是"定时模拟用户发了一条消息"**。

这意味着：把"为某公司跑 Sirius 估值"封装成一条**自然语言指令**（例如 *"请对 1357.HK 执行 /sirius-valuation 全流程并将结果写入 report_id=xxx"*），自动化框架就能直接复用，**不需要重写 Agent 循环**。

---

## 六、Skills 机制（sirius-valuation 怎么被 Agent 调用）

**Skill = 目录 + `SKILL.md`（YAML frontmatter）+ 可选 `skill.json` + 脚本 + 知识文档**

Agent 能"看到"并执行 skill 的方式：

1. **`LoadSkill` middleware**：Agent 主动调 `LoadSkill("sirius-valuation")` 把 `SKILL.md` 注入 context
2. **斜杠命令**：用户/指令里写 `/sirius-valuation 美图科技` 触发
3. **自动检测**：某些 middleware 根据关键词激活

### 当前 sirius-valuation 已有流程

```
1. scripts/fetch_data.py --symbol XXX --market YY
   → 输出 data/{symbol}/financial_context.md + engine_result.json

2. 并行分析 D1-D5（按 knowledge/d{N}_*.md 知识指南）
   → 每维度输出 JSON {score, summary, analysis, metrics}

3. D6 综合评估（依赖 D1-D5）

4. D7 定性调整 + 最终估值修正
```

**唯一缺的一步**：把最终结果**持久化到宿主 DB**（目前结果只在沙箱文件系统里）。

→ 解决方案见 `02_iteration_plan.md` 的 Phase 2。

---

## 七、部署机制

### 现状（开发友好）

`docker-compose.yml` 默认起：
- `postgres`（profile=infra，可换成云 DB）
- `redis`（profile=infra，可换成云 Redis）
- `backend`（用 `deploy/Dockerfile.dev`，挂源码 + 热加载）
- `frontend`（node:22-alpine 跑 `pnpm dev`）

**沙箱 provider**：
- `SANDBOX_PROVIDER=daytona`（默认，云沙箱，需 `DAYTONA_API_KEY`）
- `SANDBOX_PROVIDER=docker`（本地，需挂 `/var/run/docker.sock` 走 DooD）

### 生产部署需要补的

1. `docker-compose.prod.yml`：用 `deploy/Dockerfile.backend` + `Dockerfile.web`，去掉源码挂载和 `--reload`
2. 前端 build 静态文件 + nginx 提供
3. DB / Redis 替换为云托管版本（`.env` 改 `DB_HOST` 即可）
4. HTTPS（traefik / nginx + certbot）

→ 详细脚本见 `02_iteration_plan.md` 的 Phase 4。

---

## 八、几个容易踩的坑

1. **不要直接调 `create_llm()`**：服务端工具调用必须走 `LLMService.complete`，否则永远走平台 key 计费
2. **不要在 router 里写业务**：路由薄、handler / service 重，是项目惯例
3. **alembic 用 raw SQL（`op.execute`）**：不要引入 SQLAlchemy ORM 风格的 migration
4. **agent.md 是 workspace 的 single source of truth**：所有 middleware 都会把它注入到模型调用，写报告要尊重这个机制
5. **SSE 事件会写进 `conversation_responses.sse_events`**：自动化跑完后可以"回放" Agent 思考过程——前端展示时可以利用这个
6. **`asyncio.shield` 保护后台任务**：HTTP 连接断了 Agent 不会停，要让用户知道这个机制（断线重连看进度）
7. **workspace 懒启动**：第一次访问会启动 sandbox（30-60s），后续 30 分钟空闲自动停机
8. **memory 别和 state 混淆**：`memory.md` 是跨会话长期记忆，`agent.md` 是当前 workspace 笔记

---

## 九、面向"自动化估值平台"的能力清单

LangAlpha **已经现成的能力（直接用，零成本）**：

| 能力 | 模块 | 我们怎么用 |
|---|---|---|
| ✅ 给某只票自动创建专属 workspace | `WorkspaceManager.create_workspace()` | 每只票一个，命名 `Valuation:{symbol}` |
| ✅ 沙箱里跑长任务 | PTC Agent | 跑 sirius-valuation D1-D7 |
| ✅ 定时批量跑任务 | `AutomationExecutor` + `AutomationScheduler` | cron 触发白名单全量分析 |
| ✅ 跑完后通知 | webhook（`WebhookClient`） | 可选：发飞书/Slack |
| ✅ 全过程可回放 | SSE events 存 DB | 前端详情页展示 Agent 思考过程 |
| ✅ 断线不丢任务 | `asyncio.shield` + Redis | 自动化跑半小时也没事 |
| ✅ 密钥安全 | pgcrypto + vault | FMP key、自定义数据源 key 安全存储 |

**需要我们自己加的（增量）**：

| 缺什么 | 加什么 |
|---|---|
| 量化分数存哪 | 新表 `valuation_reports` + `valuation_universe` |
| 怎么触发分析 | 新 router `/api/v1/valuations/*` + 新 service `ValuationOrchestrator` |
| Skill 结果怎么回库 | skill 加 `scripts/persist_report.py` 通过内部 API 写回 |
| 用户怎么看结果 | 新前端页面 `web/src/pages/Valuations/` |
| 怎么云部署 | `docker-compose.prod.yml` + `scripts/deploy_cloud.sh` |

---

## 十、参考阅读路径（按顺序读完心里有底）

1. `README.md`（全局）→ `CLAUDE.md`（开发约定）
2. `agent_config.yaml`（Agent 能力声明）
3. `src/server/app/setup.py`（看 router 怎么注册）
4. `src/server/app/automations.py` + `src/server/services/automation_executor.py`（**最重要**，自动化是怎么跑的）
5. `src/server/services/workspace_manager.py`（workspace 生命周期）
6. `skills/sirius-valuation/SKILL.md` + `skill.json` + `scripts/fetch_data.py`（我们自己的 skill）
7. `migrations/versions/001_initial_schema.py`（看现有表结构）
8. `web/src/pages/Automations/`（前端实现参考）

---

> 下一步 → 读 `02_iteration_plan.md`
