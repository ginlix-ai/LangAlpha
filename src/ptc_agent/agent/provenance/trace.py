"""Turn an in-sandbox MCP trace into provenance sources.

The trace is what the generated MCP client in the sandbox writes for each MCP
call a script made. It reaches the server two ways: on a code run's tool
result (``artifact["mcp_trace"]``), and on a background command's ledger row
once the command has ended (``command_runs.mcp_trace``). Both read through
:func:`sources_from_trace`, so a source looks the same whichever way it came.

The trace is LLM-authored (the agent's code shares the trace file), so every
field is re-clamped host-side before it can reach an event, a row or a render.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from ptc_agent.agent.provenance.types import (
    RESULT_BODY_MAX_BYTES,
    SNIPPET_MAX_CHARS,
    ProvenanceSource,
    hash_args,
    redact_args,
)

# Host-side bounds on the trace. The generated client is supposed to
# self-limit, but it is code the agent controls, so a poisoned or huge trace
# must not amplify into the DB, the stream or the render.
SHA_MAX_CHARS = 128
IDENT_PART_MAX_CHARS = 256
MAX_TRACE_ENTRIES = 200
# A well-formed ISO-8601 value is 25 to 35 chars; anything longer (or not a
# string) falls back to now().
TIMESTAMP_MAX_CHARS = 64

# Sanity ceiling for the agent-claimed true result size. It is the TRUE byte
# length of an MCP result, so it can exceed the 64 KiB body cap (a truncated
# body); this only rejects a non-numeric, negative or absurd value.
RESULT_SIZE_MAX_BYTES = 256 * 1024 * 1024

# Seeds the record ids of sources replayed from a stored trace, so delivering
# the same trace twice yields the same records.
_RECORD_NS = uuid.UUID("0d7c2f4e-5a8b-4f1e-9c3d-6b2a7e9f1c40")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def truncate(value: Any, limit: int) -> str | None:
    """Clamp a string field to ``limit`` chars; None for a non-string, which
    would otherwise reach the event unbounded."""
    return value[:limit] if isinstance(value, str) else None


def clamp_body_bytes(value: Any, limit: int) -> str | None:
    """Byte-clamp an untrusted trace body on a UTF-8 boundary; None for a
    non-string, so a multibyte split is dropped as the sandbox's decode does."""
    if not isinstance(value, str):
        return None
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    return encoded[:limit].decode("utf-8", errors="ignore")


def coerce_result_size(value: Any) -> int | None:
    """Trust the agent-authored ``result_size`` only as a bounded non-negative
    int (``bool`` excluded, an ``int`` subclass). A verified body overwrites
    it later with the real length; this guards the unverified path."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if value < 0 or value > RESULT_SIZE_MAX_BYTES:
        return None
    return value


def record_id_for(seed: str, index: int) -> str:
    """The record id of the ``index``-th entry of the trace stored under
    ``seed``: the same on every delivery of that trace."""
    return str(uuid.uuid5(_RECORD_NS, f"{seed}:{index}"))


def sources_from_trace(
    trace: Any,
    tool_call_id: str | None = None,
    *,
    record_seed: str | None = None,
) -> list[ProvenanceSource]:
    """One ``mcp_tool`` source per well-formed entry of an MCP trace.

    Entries are ``{server, tool, args, result_sha256, result_size,
    result_snippet, result_body, timestamp}``; one with no server or tool is
    skipped, and only the first ``MAX_TRACE_ENTRIES`` are read. A source from
    a tool result names that call (``tool_call_id``). One replayed from a
    stored trace names none and passes ``record_seed``, which makes its record
    ids stable across deliveries; without it each source gets a fresh id.
    """
    if not isinstance(trace, list):
        return []
    sources: list[ProvenanceSource] = []
    for index, entry in enumerate(trace[:MAX_TRACE_ENTRIES]):
        if not isinstance(entry, dict):
            continue
        server = entry.get("server")
        tool = entry.get("tool")
        if not server or not tool:
            continue
        server = str(server)[:IDENT_PART_MAX_CHARS]
        tool = str(tool)[:IDENT_PART_MAX_CHARS]
        raw_ts = entry.get("timestamp")
        timestamp = (
            raw_ts[:TIMESTAMP_MAX_CHARS]
            if isinstance(raw_ts, str) and raw_ts
            else now_iso()
        )
        sources.append(
            ProvenanceSource(
                record_id=(
                    record_id_for(record_seed, index)
                    if record_seed
                    else str(uuid.uuid4())
                ),
                source_type="mcp_tool",
                identifier=f"{server}:{tool}",
                timestamp=timestamp,
                provider=f"mcp:{server}",
                tool_call_id=tool_call_id,
                # The readable redacted args beside the legacy hash; the
                # deny-list redactor strips secrets and PII before they persist.
                args_fingerprint=hash_args(entry.get("args")),
                args=redact_args(entry.get("args")),
                result_sha256=truncate(entry.get("result_sha256"), SHA_MAX_CHARS),
                result_size=coerce_result_size(entry.get("result_size")),
                result_snippet=truncate(entry.get("result_snippet"), SNIPPET_MAX_CHARS),
                # The body the sandbox captured for THIS call, not the run's
                # whole stdout. Absent once the sandbox's per-run body budget
                # is spent.
                result_body=clamp_body_bytes(
                    entry.get("result_body"), RESULT_BODY_MAX_BYTES
                ),
            )
        )
    return sources
