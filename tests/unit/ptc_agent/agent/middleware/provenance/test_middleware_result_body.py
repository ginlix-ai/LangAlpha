"""Tests for ProvenanceMiddleware's per-access ``result_body`` capture + store.

Each ``_extract_*`` sets ``source.result_body`` from the SAME value that
produced ``result_sha256`` (per-item for fan-out tools). In ``awrap_tool_call``
the body is redacted in full and live-written to the content-addressed store
best-effort AFTER the SSE emit — the ``provenance`` event itself never carries
the body. Neutral placeholder data throughout.

The regression that started this work: ``_extract_execute_code`` must stamp each
mcp_tool source with ITS OWN per-call trace body, never the aggregate ExecuteCode
stdout. That is the most important test here.
"""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.middleware.provenance import ProvenanceMiddleware
from ptc_agent.agent.provenance.types import RESULT_BODY_MAX_BYTES

_WRITER_PATH = (
    "ptc_agent.agent.middleware.provenance.middleware.get_stream_writer"
)
# store_result_bodies is imported lazily inside _flush_bodies via
# `from src.server.database.provenance_bodies import store_result_bodies`, so the
# patch target is the attribute on the source module. The middleware batches a
# turn's bodies into ONE call whose sole positional arg is a list of
# (sha, body, byte_len, content_type) tuples.
_STORE_PATH = "src.server.database.provenance_bodies.store_result_bodies"


def _make_request(name, args, tool_call_id="call-1"):
    return SimpleNamespace(
        tool_call={"name": name, "args": args, "id": tool_call_id}
    )


def _result(content="", artifact=None):
    return SimpleNamespace(content=content, artifact=artifact)


def _canonical(value) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)
    return str(value)


async def _run(middleware, request, result, emitted, *, store=None):
    """Run awrap_tool_call with a patched stream writer + body store.

    Returns the tool result. ``store`` (if given) is patched in as
    store_result_bodies so callers can assert on what got persisted.
    """
    async def handler(_req):
        return result

    if store is None:
        store = AsyncMock()
    with patch(_WRITER_PATH, return_value=emitted.append), patch(
        _STORE_PATH, store
    ):
        out = await middleware.awrap_tool_call(request, handler)
        # The flush is scheduled in the background; drain inside the patch so the
        # task's lazy store_result_bodies import resolves to the mock above.
        await middleware._drain_body_flushes()
        return out


def _stored_items(store):
    """Flatten every (sha, body, byte_len, content_type) tuple across all batched
    store_result_bodies calls (the middleware flushes one list per turn)."""
    return [
        item
        for call in store.await_args_list
        for item in (call.args[0] if call.args else [])
    ]


@pytest.fixture
def middleware():
    return ProvenanceMiddleware()


# ---------------------------------------------------------------------------
# REGRESSION (the bug that started this): each mcp_tool source carries ITS OWN
# per-call trace body — never the aggregate ExecuteCode stdout.
# ---------------------------------------------------------------------------


def test_execute_code_body_is_per_call_not_aggregate(middleware):
    """Construct a trace with multiple entries, each a distinct result_body, and
    assert every yielded source carries its own entry's body — not the others'
    and not the aggregate ExecuteCode stdout."""
    body_a = json.dumps({"price": 101.5, "symbol": "AAA"})
    body_b = json.dumps({"price": 202.5, "symbol": "BBB"})
    body_c = json.dumps({"shares": 333, "symbol": "CCC"})
    aggregate_stdout = "AGGREGATE CODE STDOUT: printed 3 quotes, done."

    artifact = {
        "mcp_trace": [
            {
                "server": "marketdata",
                "tool": "quote",
                "args": {"symbol": "AAA"},
                "result_sha256": hashlib.sha256(body_a.encode()).hexdigest(),
                "result_size": len(body_a.encode()),
                "result_snippet": body_a[:50],
                "result_body": body_a,
            },
            {
                "server": "marketdata",
                "tool": "quote",
                "args": {"symbol": "BBB"},
                "result_sha256": hashlib.sha256(body_b.encode()).hexdigest(),
                "result_size": len(body_b.encode()),
                "result_snippet": body_b[:50],
                "result_body": body_b,
            },
            {
                "server": "positions",
                "tool": "holdings",
                "args": {"symbol": "CCC"},
                "result_sha256": hashlib.sha256(body_c.encode()).hexdigest(),
                "result_size": len(body_c.encode()),
                "result_snippet": body_c[:50],
                "result_body": body_c,
            },
        ]
    }
    request = _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1")
    result = _result(content=aggregate_stdout, artifact=artifact)

    sources = list(middleware._extract_execute_code(request, result))

    assert len(sources) == 3
    bodies = [s.result_body for s in sources]
    # Each source carries its OWN per-call body.
    assert bodies == [body_a, body_b, body_c]
    # No source carries the aggregate ExecuteCode stdout (the old bug).
    assert all(aggregate_stdout not in (b or "") for b in bodies)
    # Bodies are all distinct (no cross-contamination between calls).
    assert len(set(bodies)) == 3
    # Each body hashes to its own row's result_sha256 (verifier ground truth).
    for src in sources:
        assert (
            hashlib.sha256(src.result_body.encode()).hexdigest()
            == src.result_sha256
        )


def test_execute_code_body_absent_when_entry_omits_it(middleware):
    """A trace entry past the sandbox's per-execution budget has no result_body;
    the source's body is None but sha/snippet/size still flow through."""
    body = json.dumps({"ok": True})
    artifact = {
        "mcp_trace": [
            {
                "server": "marketdata",
                "tool": "quote",
                "args": {"symbol": "AAA"},
                "result_sha256": hashlib.sha256(body.encode()).hexdigest(),
                "result_size": len(body.encode()),
                "result_snippet": body[:50],
                # result_body intentionally absent (budget exhausted in-sandbox).
            }
        ]
    }
    request = _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1")
    sources = list(
        middleware._extract_execute_code(request, _result(artifact=artifact))
    )
    assert len(sources) == 1
    assert sources[0].result_body is None
    assert sources[0].result_sha256  # sha still present
    assert sources[0].result_snippet == body[:50]


def test_execute_code_host_reclamps_oversized_trace_body(middleware):
    """The trace is agent-authored; a body exceeding the cap is re-clamped
    host-side to <= RESULT_BODY_MAX_BYTES (defense in depth)."""
    oversized = "q" * (RESULT_BODY_MAX_BYTES + 4096)
    artifact = {
        "mcp_trace": [
            {
                "server": "marketdata",
                "tool": "quote",
                "args": {},
                "result_sha256": "a" * 64,
                "result_size": len(oversized.encode()),
                "result_snippet": "snip",
                "result_body": oversized,
            }
        ]
    }
    request = _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1")
    sources = list(
        middleware._extract_execute_code(request, _result(artifact=artifact))
    )
    assert len(sources) == 1
    assert (
        len(sources[0].result_body.encode("utf-8")) <= RESULT_BODY_MAX_BYTES
    )
    assert sources[0].result_body == oversized[:RESULT_BODY_MAX_BYTES]


# ---------------------------------------------------------------------------
# Per-item fan-out: web_search per item + sec_filing per filing set result_body
# per item, hash-consistent with that item's result_sha256.
# ---------------------------------------------------------------------------


def test_web_search_body_is_per_item_hash_consistent(middleware):
    item_a = {"url": "https://example.com/a", "title": "Alpha", "snippet": "aaa"}
    item_b = {"url": "https://example.com/b", "title": "Beta", "snippet": "bbb"}
    artifact = {"results": [item_a, item_b]}
    request = _make_request("WebSearch", {"query": "q"}, tool_call_id="ws-1")

    sources = list(middleware._extract_web_search(request, _result(artifact=artifact)))

    assert len(sources) == 2
    # Each source's body is its OWN item's canonical form.
    assert sources[0].result_body == _canonical(item_a)
    assert sources[1].result_body == _canonical(item_b)
    assert sources[0].result_body != sources[1].result_body
    # Hash-consistent: each body hashes to that item's result_sha256.
    for src in sources:
        assert (
            hashlib.sha256(src.result_body.encode("utf-8")).hexdigest()
            == src.result_sha256
        )


def test_sec_filing_body_is_per_filing_hash_consistent(middleware):
    filing_a = {"filing_date": "2026-01-01", "source_url": "https://sec.test/1"}
    filing_b = {"filing_date": "2026-02-01", "source_url": "https://sec.test/2"}
    artifact = {
        "type": "sec_filing",
        "symbol": "TESTCO",
        "filings": [filing_a, filing_b],
    }
    request = _make_request("get_sec_filing", {"symbol": "TESTCO"})

    sources = list(
        middleware._extract_sec_filing(
            request, _result(content="x", artifact=artifact)
        )
    )

    assert len(sources) == 2
    assert sources[0].result_body == _canonical(filing_a)
    assert sources[1].result_body == _canonical(filing_b)
    assert sources[0].result_body != sources[1].result_body
    for src in sources:
        assert (
            hashlib.sha256(src.result_body.encode("utf-8")).hexdigest()
            == src.result_sha256
        )


def test_sec_single_filing_body_is_artifact_hash_consistent(middleware):
    """The single-filing branch (10-K/10-Q) hashes the whole artifact; the body
    must be that same artifact value."""
    artifact = {
        "type": "sec_filing",
        "symbol": "TESTCO",
        "source_url": "https://sec.test/10k",
        "text": "filing body text",
    }
    request = _make_request("get_sec_filing", {"symbol": "TESTCO"})
    sources = list(
        middleware._extract_sec_filing(
            request, _result(content="x", artifact=artifact)
        )
    )
    assert len(sources) == 1
    assert sources[0].result_body == _canonical(artifact)
    assert (
        hashlib.sha256(sources[0].result_body.encode("utf-8")).hexdigest()
        == sources[0].result_sha256
    )


def test_web_fetch_body_is_content_hash_consistent(middleware):
    content = "fetched markdown body, the agent reasoned over this"
    request = _make_request("WebFetch", {"url": "https://docs.test/page"})
    sources = list(
        middleware._extract_web_fetch(request, _result(content=content))
    )
    assert len(sources) == 1
    assert sources[0].result_body == _canonical(content)
    assert (
        hashlib.sha256(sources[0].result_body.encode("utf-8")).hexdigest()
        == sources[0].result_sha256
    )


def test_market_data_body_shared_across_symbols(middleware):
    """One market call may yield several symbol rows; each carries the same
    fingerprinted body (the single result they all came from)."""
    artifact = {"indices": ["^AAA", "^BBB"], "data": [1, 2, 3]}
    request = _make_request("get_market_overview", {"indices": ["^AAA", "^BBB"]})
    sources = list(
        middleware._extract_market_data(request, _result(artifact=artifact))
    )
    assert len(sources) == 2
    assert sources[0].result_body == _canonical(artifact)
    assert sources[0].result_body == sources[1].result_body
    for src in sources:
        assert (
            hashlib.sha256(src.result_body.encode("utf-8")).hexdigest()
            == src.result_sha256
        )


# ---------------------------------------------------------------------------
# result_body NEVER rides the SSE event (sse_events gains 0 bytes).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_result_body_not_on_emitted_event(middleware):
    artifact = {
        "results": [
            {"url": "https://x.test/a", "title": "A", "snippet": "long body a"},
        ]
    }
    emitted = []
    await _run(
        middleware,
        _make_request("WebSearch", {"query": "q"}),
        _result(artifact=artifact),
        emitted,
    )
    assert len(emitted) == 1
    event = emitted[0]
    # The body is intentionally absent from the emitted event...
    assert "result_body" not in event
    # ...and the event key set is exactly the pre-body shape (no new keys).
    assert set(event.keys()) == {
        "type",
        "record_id",
        "source_type",
        "identifier",
        "title",
        "detail",
        "provider",
        "tool_call_id",
        "args_fingerprint",
        "args",
        "result_sha256",
        "result_size",
        "result_snippet",
        "timestamp",
        "agent",
    }


# ---------------------------------------------------------------------------
# Redaction before store: the body handed to store_result_body is redacted, and
# the SSE emit still fires. Store is best-effort — a raising store can't break
# the turn.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_body_redacted_before_store_and_emit_unaffected():
    """A known secret in the full body is redacted before store_result_body, and
    the provenance event still emits unchanged."""
    secret = "Bearer super-secret-token-value"
    mw = ProvenanceMiddleware(
        redactor=lambda s: s.replace(secret, "[REDACTED]") if s else s
    )
    content = f"prefix {secret} suffix with more body the agent read"
    store = AsyncMock()
    emitted = []

    result = await _run(
        mw,
        _make_request("WebFetch", {"url": "https://docs.test/x"}),
        _result(content=content),
        emitted,
        store=store,
    )

    # The SSE emit fired (unchanged path).
    assert len(emitted) == 1
    assert emitted[0]["identifier"] == "https://docs.test/x"
    # The tool result is returned unchanged.
    assert result.content == content

    # The batched store call carried a REDACTED body (secret scrubbed).
    store.assert_awaited_once()
    items = _stored_items(store)
    assert len(items) == 1
    stored_sha, stored_body = items[0][0], items[0][1]
    assert stored_sha == emitted[0]["result_sha256"]
    assert secret not in stored_body
    assert "[REDACTED]" in stored_body


@pytest.mark.asyncio
async def test_store_failure_does_not_break_turn():
    """store_result_body raising must not break the tool call or the SSE emit
    (best-effort live write, like WorkspaceContextMiddleware front-matter sync)."""
    mw = ProvenanceMiddleware()
    store = AsyncMock(side_effect=RuntimeError("db down"))
    emitted = []

    result = await _run(
        mw,
        _make_request("WebFetch", {"url": "https://docs.test/y"}),
        _result(content="some body the agent read"),
        emitted,
        store=store,
    )

    # The event still emitted and the result still returned despite the raise.
    assert len(emitted) == 1
    assert emitted[0]["identifier"] == "https://docs.test/y"
    assert result.content == "some body the agent read"
    store.assert_awaited_once()


@pytest.mark.asyncio
async def test_store_skipped_when_no_body(middleware):
    """file_read sets no result_body, so store_result_body is never called even
    though a provenance event is emitted."""
    store = AsyncMock()
    emitted = []
    await _run(
        middleware,
        _make_request("Read", {"file_path": "work/out.csv"}),
        _result(content="real file data"),
        emitted,
        store=store,
    )
    assert len(emitted) == 1  # event still emitted
    store.assert_not_awaited()  # but no body to store


# ---------------------------------------------------------------------------
# Integrity gate: an agent-authored (mcp_tool) sha is trusted only if the body
# we hold reproduces it. A forged or unverifiable pair is dropped AND its sha is
# nulled on the record, closing cross-tenant poisoning + IDOR-by-content-address.
# ---------------------------------------------------------------------------


def _mcp_artifact(body, sha, *, size=None):
    return {
        "mcp_trace": [
            {
                "server": "marketdata",
                "tool": "quote",
                "args": {"symbol": "AAA"},
                "result_sha256": sha,
                "result_size": size if size is not None else len(body.encode()),
                "result_snippet": body[:50],
                "result_body": body,
            }
        ]
    }


@pytest.mark.asyncio
async def test_mcp_body_with_matching_sha_is_stored_and_sha_kept():
    mw = ProvenanceMiddleware()
    body = json.dumps({"price": 101.5, "symbol": "AAA"})
    real_sha = hashlib.sha256(body.encode()).hexdigest()
    store = AsyncMock()
    emitted = []
    await _run(
        mw,
        _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1"),
        _result(content="agg", artifact=_mcp_artifact(body, real_sha)),
        emitted,
        store=store,
    )
    items = _stored_items(store)
    assert len(items) == 1
    assert items[0][0] == real_sha and items[0][1] == body
    # The verified sha survives on the record.
    assert emitted[0]["result_sha256"] == real_sha


@pytest.mark.asyncio
async def test_mcp_body_with_forged_sha_is_dropped_and_record_sha_nulled():
    mw = ProvenanceMiddleware()
    body = json.dumps({"price": 101.5, "symbol": "AAA"})
    forged_sha = "f" * 64  # body does NOT hash to this
    store = AsyncMock()
    emitted = []
    await _run(
        mw,
        _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1"),
        _result(content="agg", artifact=_mcp_artifact(body, forged_sha)),
        emitted,
        store=store,
    )
    # No body persisted under a sha we can't reproduce.
    assert _stored_items(store) == []
    # And the record carries no content-address, so it can't fetch a global
    # body by an attacker-chosen sha.
    assert emitted[0].get("result_sha256") is None


@pytest.mark.asyncio
async def test_mcp_truncated_body_cannot_verify_and_is_dropped():
    """A >64 KiB MCP result: the host holds only the 64 KiB head, which can't
    reproduce the full-result sha, so the body is dropped and the sha nulled —
    honest, since the full bytes were never recoverable on the MCP path anyway."""
    mw = ProvenanceMiddleware()
    full = "z" * (RESULT_BODY_MAX_BYTES + 5000)
    full_sha = hashlib.sha256(full.encode()).hexdigest()  # over the FULL result
    store = AsyncMock()
    emitted = []
    await _run(
        mw,
        _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1"),
        _result(artifact=_mcp_artifact(full, full_sha, size=len(full.encode()))),
        emitted,
        store=store,
    )
    assert _stored_items(store) == []
    assert emitted[0].get("result_sha256") is None


@pytest.mark.asyncio
async def test_host_path_sha_trusted_even_when_body_diverges():
    """Host-computed shas (web/sec/market) are NOT subject to the gate: a redacted
    web_fetch body diverges from its sha by design, yet it is still stored and the
    record keeps its (trusted) sha."""
    secret = "Bearer host-secret-token"
    mw = ProvenanceMiddleware(
        redactor=lambda s: s.replace(secret, "[REDACTED]") if s else s
    )
    content = f"prefix {secret} suffix body"
    store = AsyncMock()
    emitted = []
    await _run(
        mw,
        _make_request("WebFetch", {"url": "https://docs.test/x"}),
        _result(content=content),
        emitted,
        store=store,
    )
    items = _stored_items(store)
    assert len(items) == 1
    assert secret not in items[0][1] and "[REDACTED]" in items[0][1]
    # Host sha is trusted and survives even though the stored body was redacted.
    assert emitted[0]["result_sha256"] and items[0][0] == emitted[0]["result_sha256"]


def test_body_item_byte_len_ignores_result_size(middleware):
    """byte_len is derived purely from the stored body's length; ``result_size``
    is never read, so even a garbage value can't poison the BIGINT bind. The read
    side keys truncation off ``byte_len > len(inline)``, which requires byte_len to
    measure what we actually store — not the (possibly absent/malformed) raw size."""
    body = "hello body"
    src = SimpleNamespace(
        result_body=body,
        result_sha256=hashlib.sha256(body.encode()).hexdigest(),
        result_size="not-an-int",
    )
    item = middleware._body_item(src)
    assert item is not None
    assert item[2] == len(body.encode("utf-8"))


def test_body_item_byte_len_tracks_redacted_length():
    """When redaction SHRINKS the body, byte_len records the post-redaction length
    actually stored, not the larger raw result_size. This is the invariant behind
    the read-side truncation flag: a body that was big pre-redaction but redacted
    below the inline cap must read back as complete (byte_len <= len(inline)), not
    be mislabeled a partial head."""
    secret = "X" * 5000
    body = f"head {secret} tail"
    mw = ProvenanceMiddleware(
        redactor=lambda s: s.replace(secret, "[REDACTED]") if s else s
    )
    src = SimpleNamespace(
        result_body=body,
        result_sha256=hashlib.sha256(body.encode()).hexdigest(),
        result_size=len(body.encode("utf-8")),
    )
    item = mw._body_item(src)
    assert item is not None
    redacted = item[1]
    assert secret not in redacted and "[REDACTED]" in redacted
    # byte_len measures the stored (redacted) body, strictly less than the raw size.
    assert item[2] == len(redacted.encode("utf-8"))
    assert item[2] < len(body.encode("utf-8"))


@pytest.mark.asyncio
async def test_fanout_bodies_flushed_in_one_batch():
    """A multi-result web_search emits per item but flushes ONE batched store call
    carrying a body per item (each its own sha) — N round-trips collapsed to 1."""
    mw = ProvenanceMiddleware()
    artifact = {
        "results": [
            {"url": "https://x.test/a", "title": "A", "snippet": "body a"},
            {"url": "https://x.test/b", "title": "B", "snippet": "body b"},
        ]
    }
    store = AsyncMock()
    emitted = []
    await _run(
        mw,
        _make_request("WebSearch", {"query": "q"}),
        _result(artifact=artifact),
        emitted,
        store=store,
    )
    assert len(emitted) == 2
    # Exactly one batched write for the whole turn.
    store.assert_awaited_once()
    items = _stored_items(store)
    # One item per source, distinct shas matching the emitted events.
    stored_shas = {item[0] for item in items}
    assert stored_shas == {e["result_sha256"] for e in emitted}
    assert len(stored_shas) == 2


def test_result_body_cap_matches_across_layers():
    """The body cap is re-declared (not imported) in the server store to keep that
    module off the agent import graph, so the two copies can drift silently. Pin
    them equal here: drift would split MCP-body verification from truncation."""
    from src.server.database.provenance_bodies import (
        RESULT_BODY_MAX_BYTES as SERVER_CAP,
    )

    assert RESULT_BODY_MAX_BYTES == SERVER_CAP


# ---------------------------------------------------------------------------
# Body store runs OFF the tool-call critical path: scheduled in the background
# during awrap_tool_call, settled at agent end (aafter_agent / _drain).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_body_flush_is_deferred_then_drained(middleware):
    """awrap_tool_call returns WITHOUT awaiting the store write; the write lands
    once the background task is drained. This is the latency win: the tool result
    (and the next LLM step) no longer waits on the DB/object-store write."""
    request = _make_request("WebFetch", {"url": "https://x.test/doc"})
    result = _result(content="external document body")
    emitted: list = []
    store = AsyncMock()

    async def handler(_req):
        return result

    with patch(_WRITER_PATH, return_value=emitted.append), patch(_STORE_PATH, store):
        out = await middleware.awrap_tool_call(request, handler)
        # Scheduled, not yet executed — the tool result already returned.
        store.assert_not_awaited()
        await middleware._drain_body_flushes()
        store.assert_awaited_once()

    assert out is result
    # The drain is idempotent and leaves no dangling tasks.
    assert middleware._body_flush_tasks == set()


@pytest.mark.asyncio
async def test_aafter_agent_drains_pending_flush(middleware):
    """The runtime drain point (aafter_agent) awaits in-flight body writes."""
    request = _make_request("WebFetch", {"url": "https://x.test/doc"})
    result = _result(content="external document body")
    store = AsyncMock()

    async def handler(_req):
        return result

    with patch(_WRITER_PATH, return_value=[].append), patch(_STORE_PATH, store):
        await middleware.awrap_tool_call(request, handler)
        store.assert_not_awaited()
        await middleware.aafter_agent(state=None, runtime=None)
        store.assert_awaited_once()
    assert middleware._body_flush_tasks == set()
