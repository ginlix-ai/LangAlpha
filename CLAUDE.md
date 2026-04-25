# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

langalpha is the core AI agent service of the Ginlix financial research platform. It runs a LangGraph-based research agent with PTC (Programmatic Tool Calling) — the agent writes and executes Python code in Daytona sandboxes to call MCP-backed financial data tools, produce charts, and analyze data. It also has a Flash mode for quick answers without a sandbox.

## Common Commands

```bash
# Run backend (port 8000)
uv run python server.py --reload

# Run frontend dev server (port 5173)
cd web && pnpm dev

# Lint
uv run ruff check src/                    # backend
cd web && pnpm lint                        # frontend (ESLint 9 flat config)

# Tests — backend
uv run pytest tests/unit/ -v --tb=short                     # unit only (default)
uv run pytest tests/unit/path/to/test.py -v                 # single file
uv run pytest tests/unit/path/to/test.py::test_name -v      # single test
uv run pytest tests/integration/ -v -m integration          # integration (needs DB + Redis + API keys)

# Tests — frontend
cd web && pnpm vitest run                  # all tests (CI)
cd web && pnpm vitest run path/to/test.js  # single file
cd web && pnpm vitest                      # watch mode

# Database setup
make setup-db     # start postgres + redis via docker, run all migrations
make migrate      # run migrations only

# Create a new database migration
uv run alembic revision -m "description of change"

# Check migration status
uv run alembic current
uv run alembic history

# Install dependencies
uv sync --group dev --extra test           # backend
cd web && pnpm install                     # frontend
```

## Architecture Overview

### Backend (`src/`)

| Directory | Purpose |
|---|---|
| `src/server/` | FastAPI app, routers (`app/`), handlers, models, services. Has its own [CLAUDE.md](src/server/CLAUDE.md) with detailed SSE event types and endpoint docs. |
| `src/ptc_agent/` | Core agent library — agent factory, middleware stack, subagents, prompts, sandbox/MCP integration |
| `src/tools/` | LangChain tools: web search, web fetch, market data, SEC filings, crawl |
| `src/llms/` | LLM wrappers, token counting, pricing, model manifest (`models.json`) |
| `src/config/` | Settings (`settings.py`), logging config |
| `src/data_client/` | Financial data protocol abstraction |
| `src/utils/` | Redis cache, shared utilities |

### Frontend (`web/src/`)

React 19 + Vite 7, TypeScript, Tailwind CSS 3, shadcn/ui. State via React Query (`@tanstack/react-query`). Auth via Supabase (optional — disabled locally with `VITE_SUPABASE_URL` unset).

| Directory | Purpose |
|---|---|
| `api/client.js` | Axios instance with Bearer token interceptor |
| `lib/queryKeys.js` | React Query key factory for cache management |
| `contexts/` | `AuthContext` (Supabase session), `ThemeContext` |
| `hooks/` | Shared React Query hooks (`useUser`, `useWorkspaces`, etc.) |
| `pages/ChatAgent/` | Main AI chat interface — SSE streaming via raw `fetch()` + `ReadableStream` |
| `pages/Dashboard/` | Configurable widget gallery (watchlist, portfolio, news, TradingView widgets, mini-chart grid). Per-widget config validated with Zod at the prefs boundary. |
| `pages/MarketView/` | Real-time market chart with WebSocket data |
| `pages/Automations/` | Scheduled automation CRUD |
| `components/ui/` | Primitive UI components (Radix-based) |

Pages are lazy-loaded in `Main.jsx`. Each page group has its own `utils/api.js` for API calls. Path alias: `@` → `web/src/`.

### Key Config Files

| File | Purpose |
|---|---|
| `agent_config.yaml` | Agent capabilities: LLM models, MCP servers, subagents, tools, sandbox config |
| `config.yaml` | Infrastructure: CORS origins, Redis TTLs, workflow timeouts, market data providers |
| `.env` / `.env.example` | Credentials and service URLs |

### Agent Architecture

The agent does NOT use a hand-written `StateGraph`. It uses `create_agent()` from the `deepagents` library, wrapped in a deep middleware stack:

**`src/ptc_agent/agent/agent.py` — `PTCAgent.create_agent()`** assembles:
1. **Tools**: `execute_code`, `bash`, filesystem ops (read/write/edit/glob/grep), `show_widget` (inline HTML visualizations), `web_search`, `web_fetch`, SEC/market tools
2. **Middleware chain** (~25 layers): tool argument parsing → protected paths → error handling → leak detection → file/todo artifact emission → multimodal support → skills → steering → background subagents → HITL → compaction → model retry/fallback → prompt caching → workspace context injection → memory + memo awareness
3. **`BackgroundSubagentOrchestrator`** wraps the agent for parallel background task coordination

**Subagents** (`src/ptc_agent/agent/subagents/`): `general-purpose` and `research` built-in; registry loads additional from `agent_config.yaml`.

**Flash mode** (`src/ptc_agent/agent/flash/`): lightweight agent — no sandbox, no MCP, no subagents, external tools only (web search, market data, SEC).

### PTC Pattern

The core differentiator: the LLM doesn't call MCP tools directly. Instead, it writes Python code via `execute_code` that imports generated wrapper modules and calls MCP-backed functions in the Daytona sandbox. This enables data manipulation, charting, and multi-step analysis in a single code execution.

### Data Flow

```
Client POST /api/v1/threads/{id}/messages
  → threads.py → chat/ (resolve LLM, credit check)
    → build_ptc_graph_with_session() (get sandbox session from WorkspaceManager)
      → BackgroundSubagentOrchestrator.astream()
        → WorkflowStreamHandler.stream_workflow_events()
          → SSE events (message_chunk, tool_calls, tool_call_result, artifact, ...)
```

SSE events are buffered in Redis for reconnection and persisted to `conversation_responses.sse_events` for replay.

### Database

No ORM — raw `psycopg3` async queries with `psycopg_pool.AsyncConnectionPool`. Schema managed by Alembic migrations (`migrations/versions/`). Two separate postgres connection pools: one for app data, one for LangGraph checkpointer state.

Key hierarchy: **User → Workspace (1:1 Daytona sandbox) → Thread → Turns (query + response + usage)**

### MCP Servers

Financial data MCP servers in `mcp_servers/` run as stdio subprocesses, configured in `agent_config.yaml`. `MCPRegistry` manages connections; `ToolFunctionGenerator` creates Python wrapper code uploaded to sandboxes.

### Prompt System

Jinja2 templates in `src/ptc_agent/agent/prompts/templates/`, config in `prompts/config/prompts.yaml`, loaded by `PromptLoader`. Preview with `scripts/utils/render_prompt.py`.

### Long-Term Memory & Memo Store

Two separate stores backed by the LangGraph `BaseStore` API (Postgres-backed in production, in-memory for tests):

- **Long-term memory** — agent-written. User tier `(user_id, "memory")` and workspace tier `(user_id, workspace_id, "memory")`. Persists durable preferences and cross-sandbox knowledge. Read by `MemoryContextMiddleware` which injects `memory.md` into every model call.
- **Memo store** (`src/ptc_agent/agent/memo/`, `src/server/app/memo.py`) — user-managed under `(user_id, "memos")`. Users upload markdown/PDF/CSV/JSON/plain-text via `/api/v1/memo/*`; PDFs are text-extracted server-side, metadata (title, summary, tags) is generated asynchronously by an LLM through `LLMService`, and original bytes go to S3-compatible object storage (Cloudflare R2, Tencent COS, MinIO, AWS S3) via `src/server/services/memo_binary_storage.py` — falling back to inline base64 when no bucket is configured. The agent has read-only filesystem access through `CompositeFilesystemBackend` and is told the memo count via `MemoAwarenessMiddleware` so it can `read_file` / `glob` on demand.

Both stores share a request-scoped cache (`backends/store_cache.py`) so multi-middleware reads of the same key in one turn hit the store once.

### Server Utility LLM Calls

`src/server/services/llm_service.py` (`LLMService.complete`) is the canonical wrapper for one-shot server-side LLM calls (memo metadata, future thread titles / follow-up suggestions / hint messages). It delegates credential resolution to `resolve_llm_config` so BYOK, OAuth, and per-user model preferences are respected — never call `create_llm()` directly from server utilities or the call will always bill through the platform key.

## Conventions

- **Python**: Ruff for linting (only `E741` ignored globally). Python 3.12+. Async-first (`async def` for all handlers/services).
- **Frontend**: ESLint 9 flat config. Tests co-located in `__tests__/` subdirectories using Vitest + Testing Library.
- **Package managers**: `uv` for Python, `pnpm` for frontend.
- **No SQLAlchemy ORM** — all DB access is raw SQL via psycopg3. Alembic is used for migrations only (raw SQL via `op.execute()`).
- **Two config layers**: `.env` for credentials/URLs, YAML files for behavioral settings.
- **Middleware-driven architecture**: agent behavior is composed via middleware, not graph nodes.
