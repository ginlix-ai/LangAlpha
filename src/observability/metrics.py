"""OTel meter + instruments.

Cardinality discipline: never label metrics with user_id / workspace_id /
thread_id — those go on span attributes (and are hashed by ``hash_id``). Metric labels
are always bounded sets: mode, model, status, op, server, tool, content_type,
subagent_type, trigger.

The instruments are lazily-created module globals via ``get_meter`` so they
survive the test-only provider swap done by the ``otel_capture`` fixture.
"""

from __future__ import annotations

from opentelemetry import metrics

from .db_callbacks import credits_observe, llm_tokens_observe

meter = metrics.get_meter("langalpha")

chat_turns_counter = meter.create_counter(
    "langalpha.chat.turns",
    description="Chat turn outcomes. Status: completed | interrupted | error.",
    unit="{turn}",
)

chat_turn_duration_ms = meter.create_histogram(
    "langalpha.chat.turn.duration_ms",
    description="End-to-end chat turn wall time.",
    unit="ms",
)

chat_turns_in_flight = meter.create_up_down_counter(
    "langalpha.chat.turns.in_flight",
    description="In-flight chat turns by mode.",
    unit="{turn}",
)

# Sourced from conversation_usages via ObservableCounter — DB is canonical.
llm_tokens = meter.create_observable_counter(
    "langalpha.llm.tokens",
    callbacks=[llm_tokens_observe],
    description="LLM tokens by model, billing_type, kind (input = prompt − cached).",
    unit="{token}",
)

# Internal credit usage (platform's own unit-of-account, not USD). token credits
# only apply to platform-billed rows; infrastructure credits apply to both.
credits = meter.create_observable_counter(
    "langalpha.credits",
    callbacks=[credits_observe],
    description="Credits consumed by billing_type and kind (token | infrastructure).",
    unit="{credit}",
)

workspace_created = meter.create_counter(
    "langalpha.workspace.created",
    description="Workspaces created.",
    unit="{workspace}",
)

workspace_cold_start_duration_ms = meter.create_histogram(
    "langalpha.workspace.cold_start.duration_ms",
    description="Sandbox cold-start wall time (only emitted on _restart_workspace paths).",
    unit="ms",
)

workspace_fs_bytes = meter.create_histogram(
    "langalpha.workspace.fs.bytes",
    description="Workspace filesystem op size. op label: read | write | upload | download.",
    unit="By",
)

sandbox_execute_duration_ms = meter.create_histogram(
    "langalpha.sandbox.execute.duration_ms",
    description="PTC sandbox code-execution wall time.",
    unit="ms",
)

subagent_launches = meter.create_counter(
    "langalpha.subagent.launches",
    description="Subagent invocations by type.",
    unit="{launch}",
)

memo_uploaded = meter.create_counter(
    "langalpha.memo.uploaded",
    description="Memo uploads by normalized content_type.",
    unit="{upload}",
)

automation_executions = meter.create_counter(
    "langalpha.automations.executions",
    description="Automation execution outcomes. status: success | failure.",
    unit="{execution}",
)

sse_reconnects = meter.create_counter(
    "langalpha.sse.reconnects",
    description="SSE reconnect requests handled by /reconnect endpoint.",
    unit="{reconnect}",
)

# Hot-path latency — user message arrives → first SSE chunk emitted back to the
# client. Top-level metric for "how snappy does the agent feel". Phases below
# decompose this into setup buckets so a regression isolates to a sub-phase.
hot_path_first_chunk_duration_ms = meter.create_histogram(
    "langalpha.hot_path.first_chunk.duration_ms",
    description="Wall time from chat-message HTTP entry to first non-keepalive SSE event.",
    unit="ms",
)

# Per-phase breakdown of the chat-turn setup path (PTC_TIMING in
# ptc_workflow.py). Phases: db_setup | pre_session | session | graph_build |
# workflow_start. Reuses the existing _phase_times dict at the emit site.
chat_turn_phase_duration_ms = meter.create_histogram(
    "langalpha.chat.turn.phase.duration_ms",
    description="Per-phase wall time inside the chat-turn setup path.",
    unit="ms",
)

# Workspace session acquire — the chunk of `session` phase that's spent inside
# WorkspaceManager.get_session_for_workspace. Three sub-phases reused from the
# existing SESSION_TIMING log: lock_and_init | sandbox_ready | user_data_sync
# (+ asset_sync, file_restore on the cold_resume path).
session_acquire_phase_duration_ms = meter.create_histogram(
    "langalpha.workspace.session.phase.duration_ms",
    description="Per-phase wall time inside WorkspaceManager.get_session_for_workspace.",
    unit="ms",
)

session_acquire_total_ms = meter.create_histogram(
    "langalpha.workspace.session.acquire.total_ms",
    description="Total wall time for get_session_for_workspace when real work was done.",
    unit="ms",
)

# Counter incremented on EVERY get_session_for_workspace call. Lets the
# dashboard show the mix of warm vs cold paths. Bounded label set:
# warm_skip | warm_cooldown | warm_sync | cold_create | cold_resume | cold_recover
session_path_counter = meter.create_counter(
    "langalpha.workspace.session.path",
    description="get_session_for_workspace path mix (warm vs cold breakdown).",
    unit="{call}",
)

# Sandbox asset sync (PTCSandbox.sync_sandbox_assets). Phase labels mirror the
# existing _sync_phases dict in [ASSET_SYNC] log: manifest | uploads |
# tool_modules | mcp_start | finalize.
sandbox_asset_sync_phase_duration_ms = meter.create_histogram(
    "langalpha.sandbox.asset_sync.phase.duration_ms",
    description="Per-phase wall time inside PTCSandbox.sync_sandbox_assets.",
    unit="ms",
)

sandbox_asset_sync_total_ms = meter.create_histogram(
    "langalpha.sandbox.asset_sync.total_ms",
    description="Total wall time for one sync_sandbox_assets call.",
    unit="ms",
)

sandbox_user_data_upload_duration_ms = meter.create_histogram(
    "langalpha.sandbox.user_data_upload.duration_ms",
    description="Wall time for _upload_user_data_files (markdown files into sandbox).",
    unit="ms",
)

# Replay endpoints — read-heavy user-facing routes that re-emit persisted
# sse_events. http_server_duration auto-metric covers latency by route; these
# add business-level labels (private vs public) and event-count semantics that
# http_server_* cannot express.
replay_duration_ms = meter.create_histogram(
    "langalpha.chat.replay.duration_ms",
    description="Wall time to replay a thread's SSE history end-to-end.",
    unit="ms",
)

replay_events_emitted = meter.create_counter(
    "langalpha.chat.replay.events_emitted",
    description="SSE events written to the client during replay (chattiness signal).",
    unit="{event}",
)

# Per-replay distribution of total events emitted (chattiness).
replay_events_distribution = meter.create_histogram(
    "langalpha.chat.replay.events_distribution",
    description="Distribution of total SSE events emitted in a single replay.",
    unit="{event}",
)

# Byte-based metrics. Event count understates cost for tool-heavy replays
# (500 chunks * 50 bytes != 500 chunks * 10 KB). Bytes is the real resource
# axis and drives the size_bucket label below.
replay_bytes_emitted = meter.create_counter(
    "langalpha.chat.replay.bytes_emitted",
    description="Bytes written to the client during replay.",
    unit="By",
)

replay_bytes_distribution = meter.create_histogram(
    "langalpha.chat.replay.bytes_distribution",
    description="Distribution of total bytes streamed in a single replay.",
    unit="By",
)


# Byte-based thresholds; the names map to common resource intuition:
# tiny ≤ 10 KB     — text-only short thread
# small ≤ 100 KB   — typical conversation, no big tool outputs
# medium ≤ 1 MB    — some tool/data results
# large ≤ 10 MB    — data-heavy thread with charts / large outputs
# huge > 10 MB     — outliers; usually where caching/edge wins matter.
_REPLAY_BYTE_BUCKETS = (
    (10 * 1024, "tiny"),
    (100 * 1024, "small"),
    (1024 * 1024, "medium"),
    (10 * 1024 * 1024, "large"),
)


def replay_size_bucket(n_bytes: int) -> str:
    """Map a replay's total streamed byte count to a bounded label value.

    Bucket boundaries are hardcoded by design — metric label values stay
    interpretable across deploys. Tune the constants here once production
    traffic on ``replay.bytes_distribution`` shows the real shape.
    """
    for limit, name in _REPLAY_BYTE_BUCKETS:
        if n_bytes <= limit:
            return name
    return "huge"

# MarketView WebSocket lifecycle. The `/ws/v1/market-data/aggregates/{market}`
# endpoint isn't covered by FastAPIInstrumentor (it does HTTP only) — these
# fill that gap.
ws_connections_active = meter.create_up_down_counter(
    "langalpha.ws.connections.active",
    description="Active MarketView WebSocket connections by market + interval.",
    unit="{connection}",
)

ws_connection_duration_seconds = meter.create_histogram(
    "langalpha.ws.connection.duration_seconds",
    description="MarketView WebSocket connection lifetime.",
    unit="s",
)

ws_messages_sent = meter.create_counter(
    "langalpha.ws.messages_sent",
    description="MarketView WebSocket frames sent to the client.",
    unit="{message}",
)

ws_disconnects = meter.create_counter(
    "langalpha.ws.disconnects",
    description="MarketView WebSocket disconnects by reason (client_close | server_error | shutdown).",
    unit="{disconnect}",
)


def normalize_content_type(content_type: str | None) -> str:
    """Map a free-form MIME / extension to a bounded label set for memo_uploaded.

    The memo upload endpoint accepts user-supplied content-types — emitting the
    raw value as a metric label would blow up cardinality. We map to a small
    fixed set: pdf | md | csv | json | txt | other.
    """
    if not content_type:
        return "other"
    ct = content_type.lower().strip()
    if "pdf" in ct:
        return "pdf"
    if "markdown" in ct or ct.endswith("/md") or ct.endswith(".md"):
        return "md"
    if "csv" in ct:
        return "csv"
    if "json" in ct:
        return "json"
    if ct == "text/plain" or ct.endswith("/txt") or ct.endswith(".txt"):
        return "txt"
    return "other"
