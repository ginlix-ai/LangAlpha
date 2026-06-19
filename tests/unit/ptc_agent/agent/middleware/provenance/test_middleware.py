"""Tests for ProvenanceMiddleware's per-tool source extraction + emission.

Covers web_search multi-URL (shared tool_call_id), web_fetch url-from-args,
SEC multi-filing, file/memo/memory prefix classification, the extractor-raises
safety net, and execute_code mcp_trace extraction. Neutral placeholder data.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ptc_agent.agent.middleware.provenance import ProvenanceMiddleware
from ptc_agent.agent.provenance import hash_args

_WRITER_PATH = (
    "ptc_agent.agent.middleware.provenance.middleware.get_stream_writer"
)


def _make_request(name, args, tool_call_id="call-1"):
    return SimpleNamespace(
        tool_call={"name": name, "args": args, "id": tool_call_id}
    )


def _result(content="", artifact=None):
    return SimpleNamespace(content=content, artifact=artifact)


async def _run(middleware, request, result, emitted):
    async def handler(_req):
        return result

    with patch(_WRITER_PATH, return_value=emitted.append):
        return await middleware.awrap_tool_call(request, handler)


@pytest.fixture
def middleware():
    return ProvenanceMiddleware()


@pytest.mark.asyncio
async def test_web_search_one_event_per_url_shared_tool_call_id(middleware):
    artifact = {
        "type": "web_search",
        "results": [
            {"url": "https://example.com/a", "title": "Alpha"},
            {"url": "https://example.com/b", "title": "Beta"},
        ],
    }
    request = _make_request("WebSearch", {"query": "q"}, tool_call_id="ws-1")
    emitted = []

    result = await _run(middleware, request, _result(artifact=artifact), emitted)

    assert result.artifact is artifact  # result returned unchanged
    assert len(emitted) == 2
    assert [e["identifier"] for e in emitted] == [
        "https://example.com/a",
        "https://example.com/b",
    ]
    assert [e["title"] for e in emitted] == ["Alpha", "Beta"]
    assert {e["source_type"] for e in emitted} == {"web_search"}
    assert {e["tool_call_id"] for e in emitted} == {"ws-1"}
    assert all(e["result_sha256"] for e in emitted)
    assert all(e["agent"] is None for e in emitted)  # never hardcoded


@pytest.mark.asyncio
async def test_web_search_skips_results_without_url(middleware):
    artifact = {"results": [{"title": "no url"}, {"url": "https://x.test"}]}
    emitted = []

    await _run(
        middleware,
        _make_request("WebSearch", {}),
        _result(artifact=artifact),
        emitted,
    )

    assert [e["identifier"] for e in emitted] == ["https://x.test"]


@pytest.mark.asyncio
async def test_web_fetch_identifier_from_args_url(middleware):
    request = _make_request(
        "WebFetch", {"url": "https://docs.test/page", "prompt": "p"}
    )
    emitted = []

    await _run(middleware, request, _result(content="fetched markdown"), emitted)

    assert len(emitted) == 1
    event = emitted[0]
    assert event["source_type"] == "web_fetch"
    assert event["identifier"] == "https://docs.test/page"
    assert event["result_size"] > 0


@pytest.mark.asyncio
async def test_web_fetch_no_url_no_event(middleware):
    emitted = []
    await _run(
        middleware,
        _make_request("WebFetch", {"prompt": "p"}),
        _result(content="x"),
        emitted,
    )
    assert emitted == []


@pytest.mark.asyncio
async def test_sec_multi_filing_extraction(middleware):
    artifact = {
        "type": "sec_filing",
        "symbol": "TESTCO",
        "filing_type": "8-K",
        "filings": [
            {"filing_date": "2026-01-01", "source_url": "https://sec.test/1"},
            {"filing_date": "2026-02-01", "source_url": "https://sec.test/2"},
        ],
    }
    emitted = []

    await _run(
        middleware,
        _make_request("get_sec_filing", {"symbol": "TESTCO"}),
        _result(content="filing markdown", artifact=artifact),
        emitted,
    )

    assert [e["identifier"] for e in emitted] == [
        "https://sec.test/1",
        "https://sec.test/2",
    ]
    assert {e["source_type"] for e in emitted} == {"sec_filing"}
    assert {e["provider"] for e in emitted} == {"edgar"}
    assert {e["title"] for e in emitted} == {"TESTCO"}


@pytest.mark.asyncio
async def test_sec_single_filing_top_level_source_url(middleware):
    artifact = {
        "type": "sec_filing",
        "symbol": "TESTCO",
        "filing_type": "10-K",
        "source_url": "https://sec.test/10k",
    }
    emitted = []

    await _run(
        middleware,
        _make_request("get_sec_filing", {"symbol": "TESTCO"}),
        _result(content="x", artifact=artifact),
        emitted,
    )

    assert len(emitted) == 1
    assert emitted[0]["identifier"] == "https://sec.test/10k"
    assert emitted[0]["provider"] == "edgar"


@pytest.mark.asyncio
async def test_market_data_identifier_from_symbol(middleware):
    emitted = []
    await _run(
        middleware,
        _make_request("get_stock_daily_prices", {"symbol": "TST"}),
        _result(content="prices", artifact={"symbol": "TST"}),
        emitted,
    )

    assert len(emitted) == 1
    event = emitted[0]
    assert event["source_type"] == "market_data"
    assert event["identifier"] == "TST"
    assert event["provider"] == "market_data_proxy"
    # Symbol-bearing market tools tag a data-kind so the panel can distinguish,
    # under one ticker, the several data products it was accessed through.
    assert event["detail"] == "daily_prices"


@pytest.mark.asyncio
async def test_market_data_kind_distinguishes_tools_on_same_symbol(middleware):
    """Two different market tools on the same ticker carry different `detail`
    slugs (so the Sources panel doesn't collapse them into one indistinct row)."""
    overview = []
    await _run(
        middleware,
        _make_request("get_company_overview", {"symbol": "TST"}),
        _result(content="overview", artifact={"symbol": "TST"}),
        overview,
    )
    prices = []
    await _run(
        middleware,
        _make_request("get_stock_daily_prices", {"symbol": "TST"}),
        _result(content="prices", artifact={"symbol": "TST"}),
        prices,
    )
    assert overview[0]["identifier"] == prices[0]["identifier"] == "TST"
    assert overview[0]["detail"] == "company_overview"
    assert prices[0]["detail"] == "daily_prices"


@pytest.mark.asyncio
async def test_market_data_one_event_per_index(middleware):
    emitted = []
    await _run(
        middleware,
        _make_request("get_market_indices", {"indices": ["^AAA", "^BBB"]}),
        _result(content="indices"),
        emitted,
    )

    assert [e["identifier"] for e in emitted] == ["^AAA", "^BBB"]
    assert {e["source_type"] for e in emitted} == {"market_data"}
    assert {e["detail"] for e in emitted} == {"market_index"}


@pytest.mark.asyncio
async def test_market_data_no_symbol_falls_back_to_tool_name(middleware):
    emitted = []
    await _run(
        middleware,
        _make_request("get_market_movers", {"direction": "gainers"}),
        _result(content="movers"),
        emitted,
    )

    assert len(emitted) == 1
    assert emitted[0]["identifier"] == "get_market_movers"
    # Symbol-less tools surface the tool name as the identifier already, so no
    # redundant kind label.
    assert emitted[0]["detail"] is None


@pytest.mark.parametrize(
    "path,expected",
    [
        (".agents/user/memo/notes.md", "memo_read"),
        (".agents/user/memory/memory.md", "memory_read"),
        (".agents/workspace/memory/memory.md", "memory_read"),
        ("work/analysis/output.csv", "file_read"),
        # Absolute sandbox paths classify the same as their relative forms (the
        # agent emits absolute paths; the sandbox root is stripped first).
        ("/home/workspace/.agents/user/memo/x.md", "memo_read"),
        ("/home/workspace/.agents/user/memory/m.md", "memory_read"),
        ("/home/daytona/work/out.csv", "file_read"),
    ],
)
@pytest.mark.asyncio
async def test_file_read_prefix_classification(middleware, path, expected):
    emitted = []
    await _run(
        middleware,
        _make_request("Read", {"file_path": path}),
        _result(content="file body"),
        emitted,
    )

    assert len(emitted) == 1
    assert emitted[0]["source_type"] == expected
    assert emitted[0]["identifier"] == path


@pytest.mark.parametrize(
    "path",
    [
        ".agents/skills/research/SKILL.md",
        ".agents/skills",
        "tools/marketdata_client.py",
        "mcp_servers/server.py",
        ".system/trace/exec.jsonl",
        ".self-improve/notes.md",
        ".agents/threads/t1/state.json",
        ".agents/large_tool_results/r1.json",
        "./.agents/skills/x/SKILL.md",
        # Absolute sandbox paths must skip too — the agent emits these.
        "/home/workspace/.agents/skills/x/SKILL.md",
        "/home/workspace/.system/trace/exec.jsonl",
        # agent.md is the auto-injected per-workspace notebook (scaffolding).
        "agent.md",
        "/home/workspace/agent.md",
    ],
)
@pytest.mark.asyncio
async def test_file_read_skips_agent_infra_paths(middleware, path):
    """Reads of skill docs / generated tool & MCP wrappers / system files / the
    agent.md notebook emit no provenance — scaffolding, not external data."""
    emitted = []
    await _run(
        middleware,
        _make_request("Read", {"file_path": path}),
        _result(content="scaffolding body"),
        emitted,
    )

    assert emitted == []


@pytest.mark.parametrize(
    "path",
    [
        "tools_analysis/output.csv",  # sibling, not the reserved `tools` dir
        ".agents/user/profile/portfolio.json",  # user data, tracked
        "work/tools/helper.py",  # nested, not a root infra dir
    ],
)
@pytest.mark.asyncio
async def test_file_read_keeps_lookalike_paths(middleware, path):
    """Paths that merely resemble an infra root are still tracked (file_read)."""
    emitted = []
    await _run(
        middleware,
        _make_request("Read", {"file_path": path}),
        _result(content="real data"),
        emitted,
    )

    assert len(emitted) == 1
    assert emitted[0]["source_type"] == "file_read"
    assert emitted[0]["identifier"] == path


@pytest.mark.asyncio
async def test_glob_not_tracked(middleware):
    """Glob is directory enumeration, not a data access — emits no provenance."""
    emitted = []
    await _run(
        middleware,
        _make_request("Glob", {"pattern": "**/*.py", "path": "work/src"}),
        _result(content="a.py\nb.py"),
        emitted,
    )

    assert emitted == []


@pytest.mark.asyncio
async def test_grep_uses_path_arg(middleware):
    """Grep reads file content to find matches, so it IS tracked (file_read)."""
    emitted = []
    await _run(
        middleware,
        _make_request("Grep", {"pattern": "revenue", "path": "work/src"}),
        _result(content="report.md: total revenue"),
        emitted,
    )

    assert len(emitted) == 1
    assert emitted[0]["source_type"] == "file_read"
    assert emitted[0]["identifier"] == "work/src"


@pytest.mark.asyncio
async def test_execute_code_one_mcp_tool_event_per_trace_entry(middleware):
    artifact = {
        "mcp_trace": [
            {
                "server": "marketdata",
                "tool": "quote",
                "args": {"symbol": "TST"},
                "result_sha256": "a" * 64,
                "result_size": 12,
                "result_snippet": "snip",
                "timestamp": "2026-01-01T00:00:00+00:00",
            },
            {
                "server": "filings",
                "tool": "lookup",
                "args": {"cik": "0001"},
                "result_sha256": "b" * 64,
                "result_size": 34,
                "result_snippet": "snip2",
                "timestamp": "2026-01-01T00:00:01+00:00",
            },
        ]
    }
    emitted = []

    await _run(
        middleware,
        _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1"),
        _result(content="SUCCESS", artifact=artifact),
        emitted,
    )

    assert len(emitted) == 2
    assert [e["identifier"] for e in emitted] == [
        "marketdata:quote",
        "filings:lookup",
    ]
    assert [e["provider"] for e in emitted] == ["mcp:marketdata", "mcp:filings"]
    assert {e["source_type"] for e in emitted} == {"mcp_tool"}
    assert {e["tool_call_id"] for e in emitted} == {"ec-1"}
    # Args are hashed, never stored raw (may carry secrets/PII).
    assert emitted[0]["args_fingerprint"] == hash_args({"symbol": "TST"})
    assert set(emitted[0]["args_fingerprint"]) == {"sha256"}
    assert emitted[0]["result_sha256"] == "a" * 64
    assert emitted[0]["timestamp"] == "2026-01-01T00:00:00+00:00"


@pytest.mark.asyncio
async def test_execute_code_strips_mcp_trace_from_artifact(middleware):
    """mcp_trace must not survive on the artifact (it would ride tool_call_result)."""
    artifact = {
        "mcp_trace": [
            {
                "server": "marketdata",
                "tool": "quote",
                "args": {"symbol": "TST"},
                "result_sha256": "a" * 64,
                "result_size": 12,
                "result_snippet": "snip",
            }
        ],
        "other": "kept",
    }
    result = await _run(
        middleware,
        _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1"),
        _result(content="SUCCESS", artifact=artifact),
        [],
    )
    assert "mcp_trace" not in result.artifact
    assert result.artifact["other"] == "kept"  # only mcp_trace is removed


@pytest.mark.asyncio
async def test_snippet_redacted_before_emit():
    """The redactor scrubs secrets from snippets the content scan never sees."""
    mw = ProvenanceMiddleware(
        redactor=lambda s: s.replace("sk-secret-value", "[REDACTED]") if s else s
    )
    emitted = []
    await _run(
        mw,
        _make_request("WebFetch", {"url": "https://example.test/x"}),
        _result(content="prefix sk-secret-value suffix"),
        emitted,
    )
    assert len(emitted) == 1
    assert "sk-secret-value" not in emitted[0]["result_snippet"]
    assert "[REDACTED]" in emitted[0]["result_snippet"]


@pytest.mark.asyncio
async def test_execute_code_no_artifact_no_event(middleware):
    emitted = []
    await _run(
        middleware,
        _make_request("ExecuteCode", {"code": "..."}),
        _result(content="SUCCESS"),  # artifact=None
        emitted,
    )
    assert emitted == []


@pytest.mark.asyncio
async def test_unmonitored_tool_passes_through(middleware):
    emitted = []
    result = await _run(
        middleware,
        _make_request("SomethingElse", {"x": 1}),
        _result(content="r"),
        emitted,
    )
    assert result.content == "r"
    assert emitted == []


@pytest.mark.asyncio
async def test_extractor_raises_returns_result_no_exception(middleware):
    """A broken extractor must not break the tool call; result returned as-is."""
    sentinel = _result(content="unchanged")

    def _boom(_request, _result):
        raise RuntimeError("extractor blew up")

    middleware._extractors["WebSearch"] = _boom
    emitted = []

    result = await _run(
        middleware,
        _make_request("WebSearch", {"query": "q"}),
        sentinel,
        emitted,
    )

    assert result is sentinel
    assert emitted == []


@pytest.mark.asyncio
async def test_emit_failure_does_not_break_tool_call(middleware):
    """If the writer raises while emitting, the tool result still returns."""
    artifact = {"results": [{"url": "https://x.test", "title": "t"}]}
    sentinel = _result(artifact=artifact)

    def _bad_writer(_event):
        raise RuntimeError("stream broken")

    async def handler(_req):
        return sentinel

    with patch(_WRITER_PATH, return_value=_bad_writer):
        result = await middleware.awrap_tool_call(
            _make_request("WebSearch", {"query": "q"}), handler
        )

    assert result is sentinel


@pytest.mark.asyncio
async def test_null_writer_is_noop(middleware):
    """get_stream_writer returning None must not raise."""
    artifact = {"results": [{"url": "https://x.test", "title": "t"}]}

    async def handler(_req):
        return _result(artifact=artifact)

    with patch(_WRITER_PATH, return_value=None):
        result = await middleware.awrap_tool_call(
            _make_request("WebSearch", {"query": "q"}), handler
        )

    assert result.artifact is artifact


# ----- error-result guard: don't attest a source the tool never returned ----


@pytest.mark.asyncio
async def test_market_error_artifact_not_recorded(middleware):
    # Market tools catch failures and return {"error": ...} with a success
    # status; that's not a real access, so no provenance is emitted.
    emitted = []
    await _run(
        middleware,
        _make_request("get_stock_daily_prices", {"symbol": "TST"}),
        _result(content="", artifact={"error": "upstream 500"}),
        emitted,
    )
    assert emitted == []


@pytest.mark.asyncio
async def test_web_fetch_error_string_not_recorded(middleware):
    emitted = []
    await _run(
        middleware,
        _make_request("WebFetch", {"url": "https://x.test/y"}),
        _result(content="[error] Failed to fetch https://x.test/y"),
        emitted,
    )
    assert emitted == []


@pytest.mark.asyncio
async def test_web_fetch_bare_failed_to_prefix_not_recorded(middleware):
    # The real web_fetch failure string ("Failed to fetch/process ...") arrives
    # without the [error] marker, so the "failed to " content prefix must catch it.
    emitted = []
    await _run(
        middleware,
        _make_request("WebFetch", {"url": "https://x.test/y"}),
        _result(content="Failed to fetch content from https://x.test/y."),
        emitted,
    )
    assert emitted == []


@pytest.mark.asyncio
async def test_web_fetch_failed_prose_is_recorded(middleware):
    # "failed to " is qualified so legit prose merely starting with "Failed" is
    # not misread as an error and dropped (bare "failed" used to swallow this).
    emitted = []
    await _run(
        middleware,
        _make_request("WebFetch", {"url": "https://x.test/y"}),
        _result(content="Failed Q4 earnings still beat lowered expectations."),
        emitted,
    )
    assert len(emitted) == 1
    assert emitted[0]["identifier"] == "https://x.test/y"


@pytest.mark.asyncio
async def test_execute_code_records_despite_error_content(middleware):
    # ExecuteCode is exempt: its code may error while individual in-sandbox MCP
    # calls succeeded — those guarded trace entries must still be recorded.
    artifact = {
        "mcp_trace": [
            {
                "server": "finance",
                "tool": "get_prices",
                "args": {"symbol": "TST"},
                "result_sha256": "abc",
                "result_size": 10,
                "result_snippet": "ok",
            }
        ]
    }
    emitted = []
    await _run(
        middleware,
        _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1"),
        _result(content="ERROR: boom in user code", artifact=artifact),
        emitted,
    )
    assert len(emitted) == 1
    assert emitted[0]["source_type"] == "mcp_tool"


# ----- host-side caps on the untrusted in-sandbox trace ----------------------


@pytest.mark.asyncio
async def test_execute_code_caps_entries_and_snippet(middleware):
    from ptc_agent.agent.middleware.provenance.middleware import (
        _MAX_TRACE_ENTRIES,
        SNIPPET_MAX_CHARS,
    )

    trace = [
        {
            "server": "finance",
            "tool": f"tool_{i}",
            "result_sha256": "s",
            "result_size": 1,
            "result_snippet": "x" * (SNIPPET_MAX_CHARS + 50),
        }
        for i in range(_MAX_TRACE_ENTRIES + 25)
    ]
    emitted = []
    await _run(
        middleware,
        _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1"),
        _result(content="SUCCESS", artifact={"mcp_trace": trace}),
        emitted,
    )
    assert len(emitted) == _MAX_TRACE_ENTRIES  # entries capped
    assert all(len(e["result_snippet"]) <= SNIPPET_MAX_CHARS for e in emitted)


# ----- redacted args capture: secrets must never leak into emitted sources ---


@pytest.mark.asyncio
async def test_web_search_captures_redacted_args(middleware):
    artifact = {"results": [{"url": "https://x.test", "title": "t"}]}
    emitted = []
    await _run(
        middleware,
        _make_request("WebSearch", {"query": "q", "api_key": "sk-" + "A" * 20}),
        _result(artifact=artifact),
        emitted,
    )
    assert len(emitted) == 1
    assert emitted[0]["args"] == {"query": "q", "api_key": "[redacted]"}


@pytest.mark.asyncio
async def test_web_fetch_captures_redacted_args(middleware):
    emitted = []
    await _run(
        middleware,
        _make_request(
            "WebFetch",
            {"url": "https://docs.test/p", "authorization": "Bearer tkn"},
        ),
        _result(content="md"),
        emitted,
    )
    assert len(emitted) == 1
    assert emitted[0]["args"]["url"] == "https://docs.test/p"
    assert emitted[0]["args"]["authorization"] == "[redacted]"


@pytest.mark.asyncio
async def test_sec_filing_captures_redacted_args(middleware):
    artifact = {
        "symbol": "TESTCO",
        "filings": [{"source_url": "https://sec.test/1"}],
    }
    emitted = []
    await _run(
        middleware,
        _make_request("get_sec_filing", {"symbol": "TESTCO", "token": "abc"}),
        _result(content="x", artifact=artifact),
        emitted,
    )
    assert len(emitted) == 1
    assert emitted[0]["args"]["symbol"] == "TESTCO"
    assert emitted[0]["args"]["token"] == "[redacted]"


@pytest.mark.asyncio
async def test_market_data_captures_redacted_args(middleware):
    emitted = []
    await _run(
        middleware,
        _make_request(
            "get_stock_daily_prices", {"symbol": "AAPL", "api_key": "sk-secret"}
        ),
        _result(content="prices", artifact={"symbol": "AAPL"}),
        emitted,
    )
    assert len(emitted) == 1
    # Planted secret never leaks; the meaningful arg is kept verbatim.
    assert emitted[0]["args"]["symbol"] == "AAPL"
    assert emitted[0]["args"]["api_key"] == "[redacted]"


@pytest.mark.asyncio
async def test_file_read_captures_redacted_args(middleware):
    emitted = []
    await _run(
        middleware,
        _make_request("Read", {"file_path": "work/out.csv"}),
        _result(content="data"),
        emitted,
    )
    assert len(emitted) == 1
    assert emitted[0]["args"] == {"file_path": "work/out.csv"}


@pytest.mark.asyncio
async def test_execute_code_trace_entry_secret_never_leaks(middleware):
    """A planted secret in an MCP trace entry's args is redacted, not stored."""
    artifact = {
        "mcp_trace": [
            {
                "server": "marketdata",
                "tool": "quote",
                "args": {"symbol": "AAPL", "api_key": "sk-secret"},
                "result_sha256": "a" * 64,
                "result_size": 12,
                "result_snippet": "snip",
            }
        ]
    }
    emitted = []
    await _run(
        middleware,
        _make_request("ExecuteCode", {"code": "..."}, tool_call_id="ec-1"),
        _result(content="SUCCESS", artifact=artifact),
        emitted,
    )
    assert len(emitted) == 1
    assert emitted[0]["args"]["symbol"] == "AAPL"
    assert emitted[0]["args"]["api_key"] == "[redacted]"
    # The legacy fingerprint is kept alongside the readable redacted args.
    assert set(emitted[0]["args_fingerprint"]) == {"sha256"}
