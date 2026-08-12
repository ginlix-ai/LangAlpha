"""Tests for the per-call ``result_body`` channel in the generated MCP client.

The generated ``mcp_client.py`` ``_trace_mcp_call`` emits a byte-capped
``result_body`` (the raw bytes that produced ``result_sha256``) into each trace
entry, while ``result_size`` still records the TRUE full byte length. A
per-execution aggregate guard (``_RESULT_BODY_TRACE_BUDGET_BYTES``) stops
emitting ``result_body`` once the running sum crosses the budget — but snippet,
sha and size always survive — so agent-authored sandbox code can't stream
unbounded bytes into host memory.

These exec the generated module (same approach as test_mcp_trace_generation.py)
so the assertions exercise the real interpolated client code, not a transcript.
"""

from __future__ import annotations

import json

from ptc_agent.agent.provenance.types import (
    RESULT_BODY_MAX_BYTES,
    fingerprint_result,
)
from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.tool_generator import (
    RESULT_BODY_TRACE_BUDGET_BYTES,
    ToolFunctionGenerator,
)


def _builtin_config() -> MCPServerConfig:
    return MCPServerConfig(
        name="market_data",
        transport="stdio",
        command="python",
        args=["-m", "server"],
        env={"API_KEY": "from-os-environ"},
    )


def _render() -> str:
    return ToolFunctionGenerator().generate_mcp_client_code(
        [_builtin_config()], working_dir="/home/workspace"
    )


def _exec_module(code: str) -> dict:
    """Exec the generated client; each call yields a FRESH module-global counter.

    The aggregate guard counter (``_result_body_emitted_bytes``) is a
    module-global accumulating across calls within ONE module instance, so a
    per-test fresh exec isolates the budget state between tests.
    """
    namespace: dict = {}
    exec(compile(code, "mcp_client.py", "exec"), namespace)  # noqa: S102
    return namespace


# ---------------------------------------------------------------------------
# Per-call byte cap: body <= RESULT_BODY_MAX_BYTES, result_size = TRUE length.
# ---------------------------------------------------------------------------


def test_result_body_capped_to_max_bytes_while_size_is_true_length(
    tmp_path, monkeypatch
) -> None:
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))

    ns = _exec_module(_render())
    # A single-byte ASCII char per position so byte length == char length, and
    # the result is comfortably larger than the per-call cap.
    big = "x" * (RESULT_BODY_MAX_BYTES + 5000)
    ns["_trace_mcp_call"]("s", "t", {}, big)

    entry = json.loads(trace_file.read_text(encoding="utf-8").splitlines()[0])

    # The body is capped at the byte ceiling...
    assert len(entry["result_body"].encode("utf-8")) <= RESULT_BODY_MAX_BYTES
    assert entry["result_body"] == big[:RESULT_BODY_MAX_BYTES]
    # ...but result_size carries the TRUE full byte length, not the cap.
    assert entry["result_size"] == len(big.encode("utf-8"))
    assert entry["result_size"] > RESULT_BODY_MAX_BYTES
    # The cap is a strict prefix of the exact bytes that produced the sha, so the
    # body stays hash-consistent with result_sha256 (verifier can trust it).
    sha, _size, _snippet = fingerprint_result(big)
    assert entry["result_sha256"] == sha
    assert big.encode("utf-8").startswith(
        entry["result_body"].encode("utf-8")
    )


def test_result_body_uncapped_when_under_max_bytes(tmp_path, monkeypatch) -> None:
    """A small result is carried whole as the body (no truncation)."""
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))

    ns = _exec_module(_render())
    small = {"rows": [1, 2, 3], "note": "tiny"}
    ns["_trace_mcp_call"]("s", "t", {}, small)

    entry = json.loads(trace_file.read_text(encoding="utf-8").splitlines()[0])
    canonical = json.dumps(
        small, sort_keys=True, default=str, ensure_ascii=False
    )
    assert entry["result_body"] == canonical
    assert entry["result_size"] == len(canonical.encode("utf-8"))


def test_result_body_byte_cap_drops_split_multibyte_char(
    tmp_path, monkeypatch
) -> None:
    """The cap slices on a byte boundary then decodes errors='ignore', so a
    multibyte char straddling the cap is dropped (not mojibake), and the body
    never exceeds the byte ceiling."""
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))

    ns = _exec_module(_render())
    # Land a 3-byte char (€ = 0xE2 0x82 0xAC) straddling the cap boundary: pad
    # with (cap - 1) ASCII bytes so the euro starts at byte (cap - 1).
    payload = "a" * (RESULT_BODY_MAX_BYTES - 1) + "€" + "tail"
    ns["_trace_mcp_call"]("s", "t", {}, payload)

    entry = json.loads(trace_file.read_text(encoding="utf-8").splitlines()[0])
    body_bytes = entry["result_body"].encode("utf-8")
    assert len(body_bytes) <= RESULT_BODY_MAX_BYTES
    # The straddling euro is dropped entirely, leaving only the ASCII prefix.
    assert entry["result_body"] == "a" * (RESULT_BODY_MAX_BYTES - 1)
    # The true size still counts the whole payload including the euro + tail.
    assert entry["result_size"] == len(payload.encode("utf-8"))


# ---------------------------------------------------------------------------
# Aggregate per-execution guard: once the cumulative emitted body bytes cross
# the budget, subsequent calls stop emitting result_body but STILL emit
# snippet + sha256 + size.
# ---------------------------------------------------------------------------


def test_aggregate_guard_stops_emitting_body_but_keeps_sha_snippet_size(
    tmp_path, monkeypatch
) -> None:
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))

    ns = _exec_module(_render())
    trace = ns["_trace_mcp_call"]

    # Each call emits a full per-call cap worth of body. The number of calls to
    # exhaust the aggregate budget is budget / per-call-cap; do a couple extra so
    # at least one trailing call lands past the budget.
    per_call = RESULT_BODY_MAX_BYTES
    calls_to_exhaust = RESULT_BODY_TRACE_BUDGET_BYTES // per_call
    total_calls = calls_to_exhaust + 3
    big = "y" * per_call
    for i in range(total_calls):
        trace("s", f"tool_{i}", {}, big)

    entries = [
        json.loads(line)
        for line in trace_file.read_text(encoding="utf-8").splitlines()
    ]
    assert len(entries) == total_calls

    with_body = [e for e in entries if "result_body" in e]
    without_body = [e for e in entries if "result_body" not in e]

    # The guard kicked in: at least the trailing calls lost their body...
    assert without_body, "expected the aggregate guard to suppress some bodies"
    # ...but it stopped emitting bodies only AFTER the budget was crossed, so the
    # early calls kept theirs (the guard is not all-or-nothing).
    assert with_body, "early calls should still carry a body under budget"

    # Snippet + sha + size survive on EVERY entry, including the guarded ones.
    for e in entries:
        assert e["result_sha256"]
        assert e["result_snippet"]
        assert e["result_size"] == len(big.encode("utf-8"))

    # The first entry (well under budget) carries a body; the very last entry
    # (well past budget) does not — pins the ordering of the guard.
    assert "result_body" in entries[0]
    assert "result_body" not in entries[-1]


def test_aggregate_guard_counter_is_per_module_instance(
    tmp_path, monkeypatch
) -> None:
    """A fresh module exec resets the budget — the counter is a module-global,
    so each execute_code run starts from zero. This holds in production because
    every execute_code spawns a fresh interpreter (both providers run code_run as
    a one-shot `python`, not a persistent kernel), so the module is re-imported
    per run and the counter never accumulates session-wide — the per-execution
    bound the host relies on."""
    monkeypatch.setenv("MCP_TRACE_FILE", str(tmp_path / "a.jsonl"))
    ns1 = _exec_module(_render())
    assert ns1["_result_body_emitted_bytes"] == 0
    ns1["_trace_mcp_call"]("s", "t", {}, "z" * 100)
    assert ns1["_result_body_emitted_bytes"] > 0

    # A second module instance is independent (fresh counter at zero).
    ns2 = _exec_module(_render())
    assert ns2["_result_body_emitted_bytes"] == 0


def test_generated_client_interpolates_budget_and_counter() -> None:
    """The aggregate guard logic + caps are interpolated from the canonical
    constants — guards against a codegen regression that drops the budget."""
    code = _render()
    assert f"_RESULT_BODY_MAX_BYTES = {RESULT_BODY_MAX_BYTES}" in code
    assert (
        f"_RESULT_BODY_TRACE_BUDGET_BYTES = {RESULT_BODY_TRACE_BUDGET_BYTES}"
        in code
    )
    assert "_result_body_emitted_bytes = 0" in code
    # The guard reads the running sum against the budget before emitting a body.
    assert (
        "_result_body_emitted_bytes < _RESULT_BODY_TRACE_BUDGET_BYTES" in code
    )
