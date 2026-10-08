# langalpha

Core AI agent service of the Ginlix financial research platform. One agent, the **PTC agent**, wired with a full Daytona sandbox and the complete toolset (code execution, MCP financial-data tools, charts, subagent orchestration), in one of two roles per turn. PTC = **Programmatic Tool Calling** (see [PTC pattern](#ptc-pattern)); it names the technique, not a product.

- **Analyst** (default): the agent inside a workspace. Does the R&D and produces the deliverables, which stay in that workspace.
- **Chief of Staff**: the agent for work in no particular workspace ("All workspaces" in the UI). Runs in a `Home` folder on the user's computer beside the workspace folders, where everything it produces is kept. It answers quick questions and anything the workspaces already hold itself, reading their folders and past threads, and hands new work to the workspace's analyst, which reports back when done.

The Chief of Staff sits behind the `all_workspaces_agent` flag. Without it, work outside a workspace runs on the **Flash agent**, a fast no-sandbox assistant (external tools only, plus any MCP tool bound to the direct path), which the Chief of Staff replaces.

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

cd web && pnpm test                   # Vitest;  pnpm test:e2e = Playwright;  pnpm typecheck = tsc -b
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
| `libs/ptc-cli/` | Standalone interactive CLI for the PTC agent (pkg `langalpha-cli`, cmd `ptc-agent`). The `cli` extra, not a server dependency: `uv sync --extra cli`. The server imports nothing from it, and the backend images never install it. |

### Frontend (`web/src/`)

React 19 + Vite + TypeScript + Tailwind + shadcn/ui; state via React Query. Path alias `@` → `web/src/`. Non-obvious landmines — dual-mode auth (`VITE_HOST_MODE`), SSE via raw `fetch` (not the REST client), Zod at the prefs boundary — are documented in **`web/AGENTS.md`**.

### Desktop shell (`desktop/`)

Electron wrapper around the hosted web app. It carries **no web bundle**, only an entry URL, so a web deploy never needs a desktop release. Two editions from one source, selected by a gitignored `config/build.json` written at package time (`scripts/write-build-config.mjs`): `oss` points at the user's own stack and asks for it on first run, `saas` opens on hosted onboarding and then the app. Details, including the OAuth interception invariant, in **`desktop/AGENTS.md`**.

### Agent internals

Built with `create_agent()` from **`langchain.agents`** (not a hand-written `StateGraph`), wrapped in a custom middleware stack (some middleware from `deepagents`). `PTCAgent.create_agent()` in `src/ptc_agent/agent/agent.py` assembles the tools (`execute_code`, `bash`, filesystem ops, `show_widget`, web search/fetch, SEC/market), the middleware, and a `BackgroundSubagentOrchestrator` for parallel background tasks.

- **Subagents** (`agent/subagents/`): five built-in (`research`, `general-purpose`, `data-prep`, `equity-analyst`, `report-builder`), all enabled by default; more user-defined ones from `agent_config.yaml`.
- **Roles** (`AgentRole` in `agent/roles.py`): `analyst` or `chief_of_staff`. The role adds the `<role>` prompt section (`prompts/templates/components/chief_of_staff.md.j2`), the coordination tools (`src/tools/secretary/chief_of_staff.py`: `manage_workspaces`, `delegate_to_analyst`, `agent_output`, `manage_threads`), and the `<activity>` baseline block (`src/tools/secretary/activity.py`: recent workspaces and threads, today's automation runs, holdings), on the main agent only, and withholds the `equity-analyst` subagent, since new analysis goes to a workspace's analyst. `resolve_turn_route` (`src/server/services/turn_runtime.py`) picks the role per turn, `chief_of_staff` in the user's Home, and decides whether work outside a workspace goes to Home or to Flash.
- **Flash agent** (`agent/flash/`): the assistant path with the flag off. It skips subagents and the sandbox, so it reaches MCP only through directly bound tools.

### PTC pattern

The core differentiator: by default the LLM does **not** call MCP tools directly. It writes Python via `execute_code` that imports generated wrapper modules and calls MCP-backed functions in the sandbox — enabling data manipulation, charting, and multi-step analysis in one execution. Financial-data MCP servers run as stdio subprocesses, each living in the `plugins/` bundle that declares it (see below); `ToolFunctionGenerator` builds the wrapper code uploaded to sandboxes.

A tool may instead be bound to the **direct** path, where the model calls it as one JSON tool call and the sandbox is not involved. That is the shape a per-call policy can see and a UI can render, which is what a live order needs, so the `trading` capability group allows no other path. `src/server/services/tool_binding.py` resolves each tool's path and clamps it to what its group permits; a server reachable only over stdio is always a sandbox wrapper, because the direct path dials through the egress relay.

### Data, streaming & database

- Request path: `POST /api/v1/threads/{id}/messages` → resolve LLM + admission → build sandbox-backed graph → stream SSE events, **buffered in Redis** for reconnection and **replayed from the LangGraph checkpoint** (contract in `src/server/AGENTS.md`).
- **No ORM** — raw `psycopg3` async (`AsyncConnectionPool`); Alembic migrations use raw SQL via `op.execute()`. **Two separate pools**: app data + LangGraph checkpointer.
- Hierarchy: **User → Computer → Workspace → Thread → Turns**. A computer owns one Daytona or Docker sandbox; each workspace owns a folder on it named after the workspace, which follows a rename the next time the computer is acquired. Start, stop, resource tier, and always-on apply to the computer and every workspace bound to it. Workspaces on the same computer are mutually trusted: their code shares an OS user and can access sibling files and MCP credentials. Workspace tool selection is a routing convention, not a security boundary; use separate computers for isolation. MCP servers and vault secrets are per user, so a secret reaches every computer the user owns; keeping one away from another workspace takes a separate account.

### Prompts, memory & memos

- **Prompts**: Jinja2 templates in `src/ptc_agent/agent/prompts/templates/`, config in `.../prompts/config/prompts.yaml`, via `PromptLoader`. Preview: `scripts/utils/render_prompt.py`.
- **Long-term memory** (agent-written): user + workspace tiers on the LangGraph `BaseStore`. The memory index rides the per-thread baseline of the runtime context (`agent/middleware/runtime_context/`), frozen at turn start and rebuilt at compaction; a change by another writer reaches the model as a durable row, not a re-render.
- **Memo store** (`agent/memo/`, `server/app/memo.py`): user-uploaded docs, read-only to the agent; binaries in S3-compatible storage (`services/memo_binary_storage.py`), base64 fallback.
- **Scratchpad** (opt-in feature `scratchpad`, `agent/middleware/compaction/notes.py`): a per-thread folder at `.agents/scratchpad/<thread id8>/` for throwaway files, with `note/` for the agent's checkpoint note on each task (the request, then anything significant the context could lose: what the user decided, approaches and why, findings, work delegated and its id, and on a long task the plan and what remains, kept current). Its absolute path reaches the model through the per-thread baseline, since the static prompt is shared across threads. Backed up with the workspace, restored in the deferred second pass, removed from the sandbox when the thread is archived or deleted. For the main agent, one runtime-context row asks for a checkpoint once per summary, when the room left under the summary trigger falls to 40k tokens or 15% of it, whichever is larger, though never before the context is half full. Another checks in after 30 tool calls with no write to the notes, and each summary names the note files after the transcript pointer rather than copying them in.
- **Thread transcripts** (`agent/transcript/`, `server/services/transcripts.py`): each thread rendered from its checkpoint into turn and subagent-run JSONL in the background at turn end, only the segments that changed, stored in `thread_transcripts` (bytes in the rows; with object storage, files over 64 KiB go to blobs). Compaction saves its agent's part live and points the summary at it. The sandbox never holds a copy; the file mount serves them read-only, with a per-computer `threads.jsonl` index generated on read.
- **File mount** (`core/sandbox/livefs_runtime/`, `server/services/livefs/`, endpoint `/api/v1/livefs/*`): a per-computer FUSE daemon serves the server-held files (memory, profile, automations, workflows, memos and transcripts read-only) so Bash and code read and write them as ordinary files. The sandbox paths are symlinks into `/mnt/livefs`, made at the computer root and again in every workspace folder, where commands run, itself a symlink to the current daemon's mount under `/mnt/.livefs/<generation>`, so a daemon is replaced by mounting anew and swapping the link. Each generation is a small tmpfs with the FUSE mount inside: Daytona runs Sysbox, which refuses to unmount FUSE but not the tmpfs, and a lazy unmount of the tmpfs takes the FUSE mount with it. The daemon authenticates with an opaque per-computer token (digest in `livefs_tokens`, about an hour, rewritten under 30 min left, revoked at stop). A save the server refuses reaches the tool result as `NOT SAVED`, and a save that reports its changes (an automation's file, and an `rm` or `mv` of one) shows them there, matched by the call id in the command's starting environment. The tool files who the command runs for (workspace, thread, timezone) under the same id, which a new automation's defaults come from. Unmounted (server unreachable from the sandbox, a Docker container created without `/dev/fuse`), the file tools still reach these files in process, code is refused, and transcripts are not shown.

## Conventions

- **Python 3.13+, async-first.** Ruff for linting (only `E741` ignored globally).
- **Config split**: `.env` for credentials/URLs, YAML (`agent_config.yaml`, `config.yaml`) for behavioral settings.
- **Per-environment overlays**: `APP_ENV` layers a sibling YAML over each config file (`APP_ENV=production` → `agent_config.production.yaml`); nested maps merge, lists replace. Overlays are gitignored, so the committed YAML stays the documented default. Merged in `load_yaml_config` (`src/ptc_agent/config/file_utils.py`), so every reader gets it. The dev compose stack bind-mounts the base files but bakes overlays in at build, so an overlay edit needs `docker compose build backend`.
- **`plugins/` holds the built-in MCP servers and skills** — one Agent Plugins 1.0.0 package per group, the same format a user uploads on the Plugins page, read at config load by `src/ptc_agent/config/plugins.py`. A bundle carries its own files: the server entry points `mcp.json` names, and its skills as directories under `plugins/<bundle>/skills/`. `mcp_servers/` keeps only the runtime they share (`_bootstrap`, the envelope, the output schemas). `mcp.json` is closed (`additionalProperties: false` at every level), so a server's `description`, `instruction`, `tool_exposure_mode` and `vault_blueprints` live in `plugin.json` under `extensions["ai.langalpha"]`, the format's one extension point — the same block an uploaded plugin may use. `agent_config.yaml`'s `mcp.servers` is now the operator's own list; a name declared in both wins there. See `plugins/README.md`.
- **Server-side LLM calls** go through `LLMService.complete`, never `create_llm()` directly (skips BYOK/OAuth/per-user prefs) — contract in `src/server/AGENTS.md`.
- **Package managers**: `uv` (Python), `pnpm` (frontend).
- **Deployment**: `docker-compose.yml` / `docker-compose.prod.yml`; Dockerfiles in `deploy/` + root `Dockerfile.sandbox`; `make deploy` / `make prod-up`.
- **⚠️ Pinned agent-facing docstrings**: the market-data MCP server tools (`plugins/*/*_mcp_server.py`) and direct market tools (`src/tools/market_data/tool.py`) ship into agent prompts and are snapshot-locked (`tests/unit/mcp_servers/agent_docstring_lock.json`). Any edit fails the default unit suite — don't reword them as a side effect. Warm sandboxes cache the generated wrappers by `MCP_CLIENT_CODEGEN_VERSION` (`src/ptc_agent/core/tool_generator.py`), but there is no manual knob to turn: the version is derived from the sandbox runtime source plus a hash of a deterministic emission probe, so a codegen or runtime change invalidates it on its own, and server-file edits resync by content hash. Only a deliberate architecture shift moves its hand-set major. Return annotations are lock-exempt — they publish output schemas via `mcp_servers/_schemas.py`. See `mcp_servers/AGENT_CONTRACT.md`.
- **Tool docstrings are prompt surface**: every agent-facing tool docstring, direct or MCP, follows `src/tools/AGENTS.md` — the model reads it at call time, so each sentence has to change a decision (call this tool or another, what to pass, what to do with the result). The two surfaces split on `Returns:`: one line for a direct tool (a selection signal), the full machine shape for an MCP tool (agent code indexes it).
- **⚠️ Third-party MCP servers launch isolated**: any stdio MCP server whose code we don't own runs via `uvx`/`npx` with pinned versions, never from the shared app venv or sandbox system Python — a shared-env server is coupled to the environment's `mcp` pin and dies on the next SDK major (the `scrapling mcp` failure class). Builtins are exempt: they import through `_bootstrap` and are era-proof by construction. The add/update API warns on shared-env commands (`isolation_warnings`, `src/server/models/mcp_server.py`); a server that still dies pre-handshake gets a classified diagnosis in the logs and discovery status (`classify_startup_failure`, `src/ptc_agent/core/mcp_registry.py`).
- **⚠️ Multi-worker server**: the backend runs `--workers N` — a request, its SSE consumer, the outbox drainer, and the recovery scanner may each land on **different processes**. Truth lives in Postgres (run ledger + advisory locks); Redis is coordination/transport; process memory is execution context only — never introduce module-level state that a request path consults, and never treat local registries as liveness/status truth. Before touching turn lifecycle, streaming, or subagent ownership, read `src/server/AGENTS.md` § Multi-worker (review checklist); verify with `scripts/multiworker_gate/`.

## Working principles

- **Design for the root cause, not the symptom.** Default to the cleanest, most durable solution over the first fix that clears the error — a larger refactor is right when it's the correct long-term shape. Balance against over-engineering: don't add abstraction a genuinely simple problem doesn't need. When the elegant fix and the cheap fix diverge sharply, name the tradeoff rather than silently picking one.
- **Docstrings explain *why*, not *what*.** Write one only where the code can't speak for itself — a non-obvious invariant, a constraint, the reason behind a surprising choice. Skip them on self-evident code; a docstring that restates the signature is noise. Keep them tight: a summary line plus at most one short paragraph.
- **Verify with real calls first; pin with tests last.** Prove a change works by exercising it end-to-end against the running backend + live upstreams — not with unit tests. Don't reflexively add tests right after an implementation or fix: green units on fresh code are false signal (it can still break against the real API). Write unit tests only to lock a *settled* contract or a fixed regression, once the work is polished — that's the one job they're good for.
