# langalpha

Core AI agent service of the Ginlix financial research platform. Two agents:

- **PTC agent** (default) — the **worker**: does the R&D and produces deliverables. Wired with a full Daytona sandbox and the complete toolset (code execution, MCP financial-data tools, charts, subagent orchestration). PTC = **Programmatic Tool Calling** (see [PTC pattern](#ptc-pattern)).
- **Flash agent** — a fast, lightweight **assistant**: quick lookups, and coordinating between the workspace and the PTC worker. No sandbox; external tools only.

> Single source of truth for AI coding agents. `CLAUDE.md` imports this via `@AGENTS.md`; Codex/Cursor/Copilot read it directly. Edit here, not there.

## Common Commands

```bash
make up                               # full stack (postgres, redis, backend, frontend); auto-detects sandbox
make config                           # interactive setup wizard (LLM, data, sandbox, search)
make help                             # all targets (deploy, prod-up, test-sandbox, …)

uv run python server.py --reload      # backend only, port 8000 (needs DB + Redis)
cd web && pnpm dev                    # frontend dev server, port 5173

make lint                             # Ruff (backend) + ESLint (frontend)

# Backend tests — default run is unit only (integration/slow/regression deselected in pyproject)
uv run pytest tests/unit/ -v --tb=short
uv run pytest -m integration          # hits real APIs — needs DB + Redis + API keys
uv run pytest -m regression           # locks live market-data behavior — needs a running server + live providers

cd web && pnpm test                   # Vitest;  pnpm test:e2e = Playwright;  pnpm typecheck = tsc --noEmit
```

## Architecture

### Backend (`src/`)

| Directory | Purpose |
|---|---|
| `src/server/` | FastAPI app, routers (`app/`), handlers, models, services |
| `src/ptc_agent/` | Core agent library — factory, middleware stack, subagents, prompts, sandbox/MCP integration |
| `src/tools/` | LangChain tools: web search/fetch, market data, SEC filings, crawl |
| `src/llms/` | LLM wrappers, token counting, pricing, model manifest (`manifest/models.json`) |
| `src/data_client/` | Financial data protocol abstraction |
| `src/utils/` | Redis cache, shared utilities |
| `libs/ptc-cli/` | Standalone interactive CLI for the PTC agent (pkg `langalpha-cli`, cmd `ptc-agent`) |

### Frontend (`web/src/`)

React 19 + Vite + TypeScript + Tailwind + shadcn/ui; state via React Query. Path alias `@` → `web/src/`. Non-obvious landmines — dual-mode auth (`VITE_HOST_MODE`), SSE via raw `fetch` (not axios), Zod at the prefs boundary — are documented in **`web/AGENTS.md`**.

### Agent internals

Built with `create_agent()` from **`langchain.agents`** (not a hand-written `StateGraph`), wrapped in a custom middleware stack (some middleware from `deepagents`). `PTCAgent.create_agent()` in `src/ptc_agent/agent/agent.py` assembles the tools (`execute_code`, `bash`, filesystem ops, `show_widget`, web search/fetch, SEC/market), the middleware, and a `BackgroundSubagentOrchestrator` for parallel background tasks.

- **Subagents** (`agent/subagents/`): five built-in (`research`, `general-purpose`, `data-prep`, `equity-analyst`, `report-builder`), all enabled by default; more user-defined ones from `agent_config.yaml`.
- **Flash agent** (`agent/flash/`): the assistant path — also skips MCP and subagents.

### PTC pattern

The core differentiator: the LLM does **not** call MCP tools directly. It writes Python via `execute_code` that imports generated wrapper modules and calls MCP-backed functions in the sandbox — enabling data manipulation, charting, and multi-step analysis in one execution. Financial-data MCP servers live in `mcp_servers/` (stdio subprocesses configured in `agent_config.yaml`); `ToolFunctionGenerator` builds the wrapper code uploaded to sandboxes.

### Data, streaming & database

- Request path: `POST /api/v1/threads/{id}/messages` → resolve LLM + admission → build sandbox-backed graph → stream SSE events, **buffered in Redis** for reconnection and **replayed from the LangGraph checkpoint** (contract in `src/server/AGENTS.md`).
- **No ORM** — raw `psycopg3` async (`AsyncConnectionPool`); Alembic migrations use raw SQL via `op.execute()`. **Two separate pools**: app data + LangGraph checkpointer.
- Hierarchy: **User → Workspace (1:1 Daytona sandbox) → Thread → Turns**.

### Prompts, memory & memos

- **Prompts**: Jinja2 templates in `src/ptc_agent/agent/prompts/templates/`, config in `.../prompts/config/prompts.yaml`, via `PromptLoader`. Preview: `scripts/utils/render_prompt.py`.
- **Long-term memory** (agent-written): `MemoryContextMiddleware` injects `memory.md` into every model call; user + workspace tiers on the LangGraph `BaseStore`.
- **Memo store** (`agent/memo/`, `server/app/memo.py`): user-uploaded docs, read-only to the agent; binaries in S3-compatible storage (`services/memo_binary_storage.py`), base64 fallback.

## Conventions

- **Python 3.12+, async-first.** Ruff for linting (only `E741` ignored globally).
- **Config split**: `.env` for credentials/URLs, YAML (`agent_config.yaml`, `config.yaml`) for behavioral settings.
- **Server-side LLM calls** go through `LLMService.complete`, never `create_llm()` directly (skips BYOK/OAuth/per-user prefs) — contract in `src/server/AGENTS.md`.
- **Package managers**: `uv` (Python), `pnpm` (frontend).
- **Deployment**: `docker-compose.yml` / `docker-compose.prod.yml`; Dockerfiles in `deploy/` + root `Dockerfile.sandbox`; `make deploy` / `make prod-up`.
- **⚠️ Pinned agent-facing docstrings**: the market-data MCP server tools (`mcp_servers/*_mcp_server.py`) and direct market tools (`src/tools/market_data/tool.py`) ship into agent prompts and are snapshot-locked (`tests/unit/mcp_servers/agent_docstring_lock.json`). Any edit fails the default unit suite — don't reword them as a side effect. Editing an MCP tool's signature/docstring also requires bumping `MCP_CLIENT_CODEGEN_VERSION` (`src/ptc_agent/core/tool_generator.py`) — warm sandboxes cache the generated wrappers by that version and won't otherwise pick up the change. See `mcp_servers/AGENT_CONTRACT.md`.
- **⚠️ Multi-worker server**: the backend runs `--workers N` — a request, its SSE consumer, the outbox drainer, and the recovery scanner may each land on **different processes**. Truth lives in Postgres (run ledger + advisory locks); Redis is coordination/transport; process memory is execution context only — never introduce module-level state that a request path consults, and never treat local registries as liveness/status truth. Before touching turn lifecycle, streaming, or subagent ownership, read `src/server/AGENTS.md` § Multi-worker (review checklist); verify with `scripts/multiworker_gate/`.

## Working principles

- **Design for the root cause, not the symptom.** Default to the cleanest, most durable solution over the first fix that clears the error — a larger refactor is right when it's the correct long-term shape. Balance against over-engineering: don't add abstraction a genuinely simple problem doesn't need. When the elegant fix and the cheap fix diverge sharply, name the tradeoff rather than silently picking one.
- **Docstrings explain *why*, not *what*.** Write one only where the code can't speak for itself — a non-obvious invariant, a constraint, the reason behind a surprising choice. Skip them on self-evident code; a docstring that restates the signature is noise. Keep them tight: a summary line plus at most one short paragraph.
- **Verify with real calls first; pin with tests last.** Prove a change works by exercising it end-to-end against the running backend + live upstreams — not with unit tests. Don't reflexively add tests right after an implementation or fix: green units on fresh code are false signal (it can still break against the real API). Write unit tests only to lock a *settled* contract or a fixed regression, once the work is polished — that's the one job they're good for.
