"""Provenance records: a derived index of external data the agent accessed.

Rows are extracted from the accumulated `conversation_responses.sse_events`
(top-level `event == "provenance"` entries) and written delete-then-insert
keyed by conversation_response_id, so the same turn can be re-persisted
(background subagent drains overwrite sse_events repeatedly) without
duplicating rows. All binds pass through the shared NUL sanitizers.
"""

import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row

from src.server.database.conversation import get_db_connection
from src.server.utils.pg_sanitize import SafeJson, strip_pg_nul_str

logger = logging.getLogger(__name__)


# Fields copied straight from each provenance SSE event onto a table row.
_TEXT_FIELDS = (
    "tool_call_id",
    "source_type",
    "identifier",
    "title",
    "result_sha256",
    "result_snippet",
    "agent",
    "provider",
)

# Signed 64-bit range of the result_size BIGINT column.
_BIGINT_MIN = -(2**63)
_BIGINT_MAX = 2**63 - 1

# Cap rows materialized per response. The in-sandbox MCP trace is LLM-authored,
# so a pathological/poisoned turn could emit thousands of entries; bound the
# delete-then-insert (and the SSE/render fan-out downstream) defensively.
_MAX_RECORDS_PER_RESPONSE = 1000

# Columns of one provenance_records row, in INSERT bind order. 15 columns; with
# the _MAX_RECORDS_PER_RESPONSE cap that's 15 * 1000 = 15_000 binds per INSERT,
# well under Postgres's 65_535-parameter ceiling (would only break above ~4_369
# rows). Keep in sync with _row_binds' tuple order.
_INSERT_COLUMNS = (
    "conversation_response_id",
    "conversation_thread_id",
    "turn_index",
    "tool_call_id",
    "source_type",
    "identifier",
    "title",
    "args_fingerprint",
    "args",
    "result_sha256",
    "result_size",
    "result_snippet",
    "agent",
    "provider",
    "source_timestamp",
)


def _coerce_int(value: Any) -> Optional[int]:
    """Best-effort BIGINT for result_size; None for anything out of range.

    The MCP trace is LLM-authored in the sandbox, so a poisoned entry (non-finite
    float, oversized int/string, non-numeric) must coerce to None rather than
    reach the INSERT — an int(float('inf'))/OverflowError or a > BIGINT value
    would abort the batch and (via the best-effort wrapper) silently drop ALL
    provenance for the response.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        candidate = value
    elif isinstance(value, float):
        if not math.isfinite(value):
            return None
        candidate = int(value)
    elif isinstance(value, str):
        try:
            candidate = int(value)
        except ValueError:
            return None
    else:
        return None
    return candidate if _BIGINT_MIN <= candidate <= _BIGINT_MAX else None


def _coerce_str(value: Any) -> Optional[str]:
    """Coerce an untrusted trace field to str-or-None for a TEXT bind.

    Text fields flow partly from LLM-authored sandbox traces; a non-string
    (dict/int/list) must not reach ``strip_pg_nul_str`` (which raises TypeError
    on a non-str) or the hashable ``dedup_key`` (a list/dict makes the membership
    test raise) — either path would abort and silently drop ALL provenance for
    the response. NUL stripping still happens at bind time.
    """
    if value is None or isinstance(value, str):
        return value
    return str(value)


def _coerce_timestamp(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 source-access timestamp to a datetime, or None.

    For mcp_tool entries the timestamp is agent-controlled, so a bad value must
    coerce to None rather than fail the TIMESTAMPTZ bind and drop the set.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def extract_provenance_from_sse_events(
    sse_events: Optional[List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Return normalized provenance rows from accumulated SSE events.

    Filters entries with top-level ``event == "provenance"`` and dedups within
    the turn on ``(source_type, identifier, result_sha256)`` — the same source
    fetched with the same content collapses to one row. ``turn_index`` /
    ``response_id`` are intentionally ignored here (they may be absent at
    persist time and are supplied by the caller).

    Defensive by design: ``source_type`` is NOT NULL in the schema and rows
    flow partly from untrusted in-sandbox traces, so entries with a missing /
    non-string ``source_type`` are skipped and ``result_size`` is coerced to an
    int-or-None — a malformed entry is dropped, never allowed to fail the insert
    (which runs inside the turn-persist transaction).
    """
    if not sse_events:
        return []

    records: List[Dict[str, Any]] = []
    seen: set[tuple] = set()
    for entry in sse_events:
        if not isinstance(entry, dict) or entry.get("event") != "provenance":
            continue
        data = entry.get("data")
        # Flat-field events store fields on the entry itself; fall back to a
        # nested "data" object if a producer ever wraps them.
        source = data if isinstance(data, dict) else entry

        source_type = source.get("source_type")
        if not isinstance(source_type, str) or not source_type:
            continue

        # Coerce every untrusted text field to str-or-None BEFORE the dedup_key
        # is built, so a forged non-string identifier/sha256 can't make the
        # membership test (or a later TEXT bind) raise and drop the whole set.
        record = {field: _coerce_str(source.get(field)) for field in _TEXT_FIELDS}
        record["args_fingerprint"] = source.get("args_fingerprint")
        # Readable tool-call args (secrets already redacted server-side by
        # redact_args before the event was emitted) — JSONB, like args_fingerprint.
        record["args"] = source.get("args")
        record["result_size"] = _coerce_int(source.get("result_size"))
        record["source_timestamp"] = _coerce_timestamp(source.get("timestamp"))

        # Include agent in the dedup key: the main agent and a subagent fetching
        # the same URL/content are distinct accesses and must each keep a row
        # with its own attribution (collapsing them would attribute arbitrarily).
        dedup_key = (
            record["source_type"],
            record["identifier"],
            record["result_sha256"],
            record["agent"],
        )
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        records.append(record)

        if len(records) >= _MAX_RECORDS_PER_RESPONSE:
            logger.warning(
                "[provenance] capped at %d records for one response; "
                "remaining entries dropped",
                _MAX_RECORDS_PER_RESPONSE,
            )
            break

    return records


def _row_binds(
    record: Dict[str, Any],
    *,
    conversation_response_id: str,
    conversation_thread_id: str,
    turn_index: int,
) -> tuple:
    """Bind tuple for one row in ``_INSERT_COLUMNS`` order (NUL-stripped, JSON-wrapped)."""
    fingerprint = record.get("args_fingerprint")
    args = record.get("args")
    return (
        conversation_response_id,
        conversation_thread_id,
        turn_index,
        strip_pg_nul_str(record.get("tool_call_id")),
        strip_pg_nul_str(record.get("source_type")),
        strip_pg_nul_str(record.get("identifier")),
        strip_pg_nul_str(record.get("title")),
        SafeJson(fingerprint) if fingerprint is not None else None,
        SafeJson(args) if args is not None else None,
        strip_pg_nul_str(record.get("result_sha256")),
        record.get("result_size"),
        strip_pg_nul_str(record.get("result_snippet")),
        strip_pg_nul_str(record.get("agent")),
        strip_pg_nul_str(record.get("provider")),
        record.get("source_timestamp"),
    )


async def insert_provenance_records(
    conn,
    *,
    conversation_response_id: str,
    conversation_thread_id: str,
    turn_index: int,
    records: List[Dict[str, Any]],
) -> int:
    """Delete-then-insert provenance rows for one response (idempotent, atomic).

    The whole delete+insert runs in a nested ``conn.transaction()``. When the
    caller is already in a transaction (the turn-persist path) that nesting is a
    SAVEPOINT, so a failed provenance write rolls back ONLY provenance and can
    never poison the surrounding response/usage commit. A per-response advisory
    lock serializes concurrent drains (main collector + orphan collector) so two
    writers can't interleave delete-then-insert into duplicate or transient-empty
    rows. The lock is xact-scoped: on the savepoint path it is held until the
    OUTER turn-persist commit (not released when the savepoint releases), so a
    concurrent same-response drain waits for that commit, not just this write.
    Rows are written as one multi-row INSERT (single parse + round-trip) rather
    than executemany, which re-parses per row on this prepare_threshold=0 pool.
    Every TEXT bind is NUL-stripped and the JSONB ``args_fingerprint`` / ``args``
    binds are wrapped in ``SafeJson``. Returns the number of rows inserted.
    """
    async with conn.cursor() as cur, conn.transaction():
        # Serialize concurrent writers for this response (xact-scoped lock).
        await cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (conversation_response_id,),
        )
        # Delete-then-insert keyed by response_id: re-runs (background drains)
        # replace the prior set rather than accumulating duplicates.
        await cur.execute(
            "DELETE FROM provenance_records WHERE conversation_response_id = %s",
            (conversation_response_id,),
        )

        if not records:
            return 0

        row_placeholder = "(" + ", ".join(["%s"] * len(_INSERT_COLUMNS)) + ")"
        values_clause = ", ".join([row_placeholder] * len(records))
        params: List[Any] = []
        for record in records:
            params.extend(
                _row_binds(
                    record,
                    conversation_response_id=conversation_response_id,
                    conversation_thread_id=conversation_thread_id,
                    turn_index=turn_index,
                )
            )
        await cur.execute(
            f"INSERT INTO provenance_records ({', '.join(_INSERT_COLUMNS)}) "
            f"VALUES {values_clause}",
            params,
        )

    return len(records)


async def sync_provenance_for_response(
    conn,
    *,
    conversation_response_id: str,
    conversation_thread_id: str,
    turn_index: int,
    sse_events: Optional[List[Dict[str, Any]]],
) -> int:
    """Extract provenance from sse_events and (re)write rows for one response.

    Single entry point for both persistence hook sites. Best-effort: never
    raises, so a provenance failure can't break turn persistence.
    """
    try:
        records = extract_provenance_from_sse_events(sse_events)
        return await insert_provenance_records(
            conn,
            conversation_response_id=conversation_response_id,
            conversation_thread_id=conversation_thread_id,
            turn_index=turn_index,
            records=records,
        )
    except Exception as e:
        logger.warning(
            f"[provenance] sync failed for response_id={conversation_response_id}: {e}"
        )
        return 0


async def get_provenance_for_thread(
    conversation_thread_id: str,
) -> List[Dict[str, Any]]:
    """Return all provenance rows for a thread ordered by turn_index."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    provenance_record_id, conversation_response_id,
                    conversation_thread_id, turn_index, tool_call_id,
                    source_type, identifier, title, args_fingerprint, args,
                    result_sha256, result_size, result_snippet, agent,
                    provider, source_timestamp, created_at
                FROM provenance_records
                WHERE conversation_thread_id = %s
                ORDER BY turn_index ASC,
                         source_timestamp ASC NULLS LAST, created_at ASC
                """,
                (conversation_thread_id,),
            )
            rows = await cur.fetchall()
            return [dict(row) for row in rows]
