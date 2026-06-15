"""Tests for the in-sandbox MCP provenance trace injected into the generated client.

The generated ``mcp_client.py`` is emitted via an f-string, so a brace-doubling
regression silently produces uncompilable code — the ``ast.parse`` test guards
that. The fingerprint helper (``_trace_mcp_call``) must reproduce the host-side
``fingerprint_result`` contract byte-for-byte, so Phase B1 can dedup records
across host and sandbox surfaces.
"""

from __future__ import annotations

import ast
import json

import pytest

from ptc_agent.agent.provenance.types import fingerprint_result
from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.tool_generator import ToolFunctionGenerator


def _builtin_config() -> MCPServerConfig:
    """A builtin (non-workspace) server config — the byte-stable common path."""
    return MCPServerConfig(
        name="market_data",
        transport="stdio",
        command="python",
        args=["-m", "server"],
        env={"API_KEY": "from-os-environ"},
    )


def _workspace_config() -> MCPServerConfig:
    """A workspace server config — exercises the vault + discovery codegen path."""
    cfg = MCPServerConfig(
        name="user_server",
        transport="stdio",
        command="npx",
        args=["some-server"],
        env={"TOKEN": "${vault:TOKEN}"},
    )
    # ``source`` flags the server as workspace-owned (untrusted), enabling the
    # vault/discovery blocks in codegen.
    cfg.source = "workspace"
    return cfg


def _render(config: MCPServerConfig) -> str:
    return ToolFunctionGenerator().generate_mcp_client_code(
        [config], working_dir="/home/workspace"
    )


def _exec_module(code: str) -> dict:
    namespace: dict = {}
    exec(compile(code, "mcp_client.py", "exec"), namespace)  # noqa: S102
    return namespace


# ---------------------------------------------------------------------------
# Brace-doubling regression: rendered module must be valid Python.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "config_factory", [_builtin_config, _workspace_config], ids=["builtin", "workspace"]
)
def test_rendered_module_parses(config_factory) -> None:
    code = _render(config_factory())
    # Raises SyntaxError on any brace-doubling regression in the f-string.
    ast.parse(code)


def test_trace_helper_and_imports_present() -> None:
    code = _render(_builtin_config())
    assert "def _trace_mcp_call(" in code
    assert "import hashlib" in code
    assert "import datetime" in code
    # Tracing is wired through _finalize_mcp_result (def + both transport calls),
    # which records only real data — never error payloads.
    assert "def _finalize_mcp_result(" in code
    assert "_trace_mcp_call(server_name, tool_name, arguments, value)" in code
    assert code.count("_finalize_mcp_result(") >= 3  # def + stdio + sse call sites
    # The dispatcher no longer traces directly; it routes only.
    assert "_trace_mcp_call(server_name, tool_name, arguments, result)" not in code


# ---------------------------------------------------------------------------
# _trace_mcp_call: JSONL shape + fingerprint parity with the host contract.
# ---------------------------------------------------------------------------


def test_trace_writes_jsonl_matching_host_contract(tmp_path, monkeypatch) -> None:
    trace_file = tmp_path / "trace" / "exec.jsonl"  # nested -> lazy makedirs
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))

    ns = _exec_module(_render(_builtin_config()))
    trace = ns["_trace_mcp_call"]

    dict_result = {"b": 2, "a": [3, 2, 1], "nested": {"z": 9}}
    str_result = "a plain string result"

    trace("market_data", "get_stock_daily_prices", {"symbol": "ACME"}, dict_result)
    trace("market_data", "get_company_overview", {"symbol": "ACME"}, str_result)

    lines = trace_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2

    entry0 = json.loads(lines[0])
    sha, size, snippet = fingerprint_result(dict_result)
    assert entry0["server"] == "market_data"
    assert entry0["tool"] == "get_stock_daily_prices"
    assert entry0["args"] == {"symbol": "ACME"}
    assert entry0["result_sha256"] == sha
    assert entry0["result_size"] == size
    assert entry0["result_snippet"] == snippet
    assert isinstance(entry0["timestamp"], str) and entry0["timestamp"]

    entry1 = json.loads(lines[1])
    sha1, size1, snippet1 = fingerprint_result(str_result)
    assert entry1["result_sha256"] == sha1
    assert entry1["result_size"] == size1
    assert entry1["result_snippet"] == snippet1


def test_trace_snippet_truncated_to_500_chars(tmp_path, monkeypatch) -> None:
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))

    ns = _exec_module(_render(_builtin_config()))
    ns["_trace_mcp_call"]("s", "t", {}, "x" * 2000)

    entry = json.loads(trace_file.read_text(encoding="utf-8").splitlines()[0])
    assert len(entry["result_snippet"]) == 500
    # full byte size is recorded even though the snippet is truncated
    assert entry["result_size"] == 2000


def test_trace_is_noop_when_env_unset(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("MCP_TRACE_FILE", raising=False)
    trace_file = tmp_path / "should_not_exist.jsonl"

    ns = _exec_module(_render(_builtin_config()))
    # Pre-seed the env in the module exec namespace would not help; the helper
    # reads os.environ directly, which we've cleared.
    ns["_trace_mcp_call"]("s", "t", {"q": 1}, {"data": "value"})

    assert not trace_file.exists()


def test_trace_never_raises_on_unserializable_result(tmp_path, monkeypatch) -> None:
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))

    ns = _exec_module(_render(_builtin_config()))

    class Weird:
        def __repr__(self) -> str:
            return "weird-object"

    # Must not raise even on a non-dict/list, non-trivially-serializable value.
    ns["_trace_mcp_call"]("s", "t", {}, Weird())

    entry = json.loads(trace_file.read_text(encoding="utf-8").splitlines()[0])
    sha, size, snippet = fingerprint_result(Weird())
    assert entry["result_sha256"] == sha
    assert entry["result_snippet"] == snippet


# ---------------------------------------------------------------------------
# _finalize_mcp_result: record data the agent actually got, not failed calls.
# Provenance must not list error payloads or isError results as "sources",
# while still returning them to the agent unchanged.
# ---------------------------------------------------------------------------


def _text_envelope(payload) -> dict:
    """Wrap a payload as an MCP single-text-block tools/call result envelope."""
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return {"content": [{"type": "text", "text": text}]}


def _finalize(ns, envelope, *, server="market_data", tool="get_stock_data"):
    return ns["_finalize_mcp_result"](server, tool, {"symbol": "ACME"}, envelope)


def test_finalize_traces_real_data(tmp_path, monkeypatch) -> None:
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))
    ns = _exec_module(_render(_builtin_config()))

    value = _finalize(ns, _text_envelope({"rows": [1, 2, 3]}))

    assert value == {"rows": [1, 2, 3]}  # unwrapped + parsed for the agent
    entry = json.loads(trace_file.read_text(encoding="utf-8").splitlines()[0])
    sha, _, _ = fingerprint_result({"rows": [1, 2, 3]})
    assert entry["result_sha256"] == sha
    assert entry["tool"] == "get_stock_data"


def test_finalize_skips_error_dict_but_returns_it(tmp_path, monkeypatch) -> None:
    """Our servers' {"error": ...} convention: agent sees it, provenance doesn't."""
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))
    ns = _exec_module(_render(_builtin_config()))

    err = {"error": "symbol not found", "symbol": "ZZZZ"}
    value = _finalize(ns, _text_envelope(err))

    assert value == err  # returned unchanged to the agent
    assert not trace_file.exists()  # but never recorded as a source


def test_finalize_skips_iserror_result(tmp_path, monkeypatch) -> None:
    """Spec-compliant isError (e.g. a third-party remote MCP that raised)."""
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))
    ns = _exec_module(_render(_builtin_config()))

    envelope = {"content": [{"type": "text", "text": "boom"}], "isError": True}
    value = _finalize(ns, envelope)

    assert value == "boom"
    assert not trace_file.exists()


def test_finalize_records_falsy_error_field_as_data(tmp_path, monkeypatch) -> None:
    """A success payload carrying error: null must NOT be dropped (conservative)."""
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))
    ns = _exec_module(_render(_builtin_config()))

    data = {"error": None, "rows": [1, 2]}
    value = _finalize(ns, _text_envelope(data))

    assert value == data
    assert trace_file.exists()


def test_finalize_records_empty_result(tmp_path, monkeypatch) -> None:
    """Empty-but-successful access (queried, got nothing) is still a data access."""
    trace_file = tmp_path / "trace.jsonl"
    monkeypatch.setenv("MCP_TRACE_FILE", str(trace_file))
    ns = _exec_module(_render(_builtin_config()))

    _finalize(ns, _text_envelope({"count": 0, "data": []}))

    assert trace_file.exists()


def test_is_error_result_predicate() -> None:
    ns = _exec_module(_render(_builtin_config()))
    is_err = ns["_is_error_result"]

    # isError flag on the envelope wins regardless of the unwrapped value.
    assert is_err({"isError": True}, "anything") is True
    # our {"error": ...} convention (truthy error)
    assert is_err({}, {"error": "boom"}) is True
    assert is_err({}, {"error": "boom", "symbol": "X"}) is True
    # falsy / absent error is data, not an error
    assert is_err({}, {"error": ""}) is False
    assert is_err({}, {"error": None, "rows": []}) is False
    assert is_err({}, {"rows": [1, 2]}) is False
    assert is_err({}, "plain text result") is False
