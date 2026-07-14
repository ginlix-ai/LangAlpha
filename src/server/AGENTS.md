# langalpha server

FastAPI server for langalpha — REST + SSE APIs for PTC-agent workflow execution, conversation/thread management, and workspaces. Endpoints live under `/api/v1/threads/*` and `/api/v1/workspaces/*` (discover the current set from the routers in `app/`).

> Single source of truth for AI coding agents in `src/server/`. `CLAUDE.md` imports this via `@AGENTS.md`; Codex/Cursor read it directly. Edit here, not there.

## Layering (put code in the right tier)

`app/` = route definitions + request orchestration (no raw SQL — DB access goes through services) · `handlers/` = per-request logic (chat streaming, workflow control, the error funnel) · `services/` = business logic + process singletons (session/workspace managers, `persistence/` DB services) · `database/` = raw-SQL query + pool layer · `models/` = Pydantic request/response · `dependencies/` = FastAPI `Depends` gates (auth + request admission).

## Landmines (non-obvious)

- **Two separate async psycopg3 pools, opened/closed in the `app/setup.py` lifespan** — the LangGraph checkpointer (workflow state; the LangGraph Store shares it) and the conversation-history (app-data) pool. Don't reuse one for the other.
- **Mid-flight reconnection is Redis-only, not the checkpoint.** Live events are buffered in a per-run Redis **Stream** `workflow:stream:{thread_id}:{run_id}` (XADD + `MAXLEN` trim, 24h TTL, 150k-event cap). `GET /api/v1/threads/{id}/messages/stream` XREAD-BLOCK-replays the buffer then tails live; `last_event_id` is a resume cursor (resumes at seq N+1), not post-hoc dedup. An abandoned running workflow is reaped after ~6h; a completed workflow's reconnect returns `410` once its task-info key expires.
- **Historical replay is projected from the LangGraph checkpoint, not from stored SSE events** (re-homed in #315). `GET /api/v1/threads/{id}/messages/replay` (`source=auto`) rebuilds the transcript from checkpoint state via `CheckpointHistoryReader` + a pure projector (`services/history/`), falling back to persisted `conversation_responses.sse_events` only when checkpoint coverage is missing (that column is still dual-written every turn, but it's now the transitional fallback). **So the durable replay contract is the checkpoint state schema: `messages` (a `DeltaChannel`) + the `ui` channel** (`langgraph.graph.ui`; an id-keyed upsert accumulator of compact, non-rederivable records — image-URL maps, `model_fallback` notices).
- **What silently breaks replay of already-stored checkpoints:** renaming a `ui`-record's `name`/`props`, or emitting one without a stable pre-stamped `id` (the reducer upserts by id — keep the projector's field whitelists in sync); putting rederivable or bulk data in graph state (the projector rebuilds artifacts from tool-call *args* + message `additional_kwargs`, and >32KB widget data must be a content-addressed `data_ref`, not inlined); or adding a chat-wire SSE type without a checkpoint- or table-sourced home — `tests/unit/server/services/history/test_event_ledger.py` fails CI until every type is classified, so build the replay source before you emit.
- **Error funnel (`handlers/chat/_common.py`, `classify_error()`):** recoverable errors (DB/connection drop, timeout, network, API 5xx/429) emit a `retry` SSE event (`auto_retry`; thread → `interrupted`, and the client/gateway re-submits) up to 3× (`get_max_workflow_retries`), then convert to a terminal `error`. Non-recoverable *code bugs* (`AttributeError`/`NameError`/`TypeError`/`ImportError`/`SyntaxError`/`KeyError`) fail immediately with a stable `error` payload. Don't add ad-hoc try/except that swallows this path.
- **Cancelled ≠ disconnected:** an explicit cancel stops the workflow; a client disconnect lets it keep running in the background (`BackgroundTaskManager` keeps executing; the run's `in_progress` ledger row is the durable record), and a reconnect resumes the live stream.
- **`steer_only: true`** on `POST .../messages` only steers an in-flight workflow; if none is running it's rejected with an admission-conflict `error` event (`error_type="admission_conflict"`, `code="not_running"`) — it never starts a new turn.

## Config

`.env` = credentials (DB, API keys) + `HOST_MODE=oss|platform` (in `platform` mode langalpha calls the platform to gate a turn; `oss` runs standalone); `config.yaml` = behavioral settings (`workflow_timeout` 3200s, `sse_keepalive_interval` 15s, CORS `allowed_origins`), read via typed accessors in `src/config/settings.py` (`get_workflow_timeout`, …) — not `os.getenv`.

## SSE events

Chat-wire SSE events are emitted from **multiple layers** — the main turn in `handlers/streaming_handler.py` (content deltas `message_chunk`/`tool_call_chunks`/`tool_calls`/`tool_call_result`, plus `artifact`, `interrupt`, `error`/`warning`, `metadata`, …) and the subagent/reconnect sentinels from `services/background_task_manager.py` + `handlers/chat/stream_from_log.py`. No single file lists them all. The **authoritative, CI-enforced** index — with each type's replay home (checkpoint-projected / table-sourced / live-only) — is `tests/unit/server/services/history/test_event_ledger.py`; add a new type there (with a replay home) before you emit it. (`:keepalive` is an SSE comment, not an event.)

## Working principles

- **Keep the tiers honest.** `app/` routes orchestrate; business logic lives in `handlers/`/`services/`, all DB access in `database/`, cross-cutting gates as `Depends`. Don't inline logic or raw SQL into a route because it's shorter.
- **One canonical path per concern — bypassing it is the bug.** Server-side LLM calls → `LLMService.complete` (never `create_llm()`, which skips BYOK/OAuth/prefs); tunables → `config/settings.py` accessors (never `os.getenv`); persistence → `services/persistence/` (never ad-hoc SQL); Redis keys → the `*_key()` builders. Reach for the wrapper before writing a second copy.
- **Async-first, non-blocking.** No sync drivers or blocking I/O on a request/SSE path; bound long work with `asyncio.wait_for`; open/close pools in the lifespan.
- **Errors go through the funnel, never around it.** Let `classify_error` decide recoverable-vs-terminal and don't swallow it; every chat-wire event needs a registered replay home (see Landmines). Sanitize error text before it reaches the wire.
