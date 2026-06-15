"""Tests that PTCSandbox.execute() surfaces the in-sandbox MCP trace.

The generated client writes a per-execution JSONL trace; ``execute()`` reads it
back (via ``aread_file_text``) on BOTH the success path and the crash ``except``
path (durability — lines are flushed per call before a crash), parsing it into
``ExecutionResult.mcp_trace``.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox.runtime import (
    CodeRunResult,
    ExecResult,
    SandboxProvider,
    SandboxRuntime,
)

WORK_DIR = "/home/workspace"

_TRACE_LINES = [
    {
        "server": "market_data",
        "tool": "get_stock_daily_prices",
        "args": {"symbol": "ACME"},
        "result_sha256": "a" * 64,
        "result_size": 42,
        "result_snippet": "snippet-one",
        "timestamp": "2026-01-01T00:00:00+00:00",
    },
    {
        "server": "market_data",
        "tool": "get_company_overview",
        "args": {"symbol": "ACME"},
        "result_sha256": "b" * 64,
        "result_size": 7,
        "result_snippet": "snippet-two",
        "timestamp": "2026-01-01T00:00:01+00:00",
    },
]


def _make_config() -> CoreConfig:
    return CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )


def _trace_jsonl() -> str:
    return "\n".join(json.dumps(line) for line in _TRACE_LINES) + "\n"


async def _passthrough_runtime_call(func, *args, **kwargs):
    """Mirror _runtime_call: invoke the runtime callable, dropping retry kwargs."""
    for retry_kwarg in ("retry_policy", "allow_reconnect", "retries",
                         "initial_delay_s", "total_timeout"):
        kwargs.pop(retry_kwarg, None)
    return await func(*args, **kwargs)


def _make_sandbox(mock_runtime):
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    sandbox = PTCSandbox(config=_make_config())
    sandbox.runtime = mock_runtime
    sandbox._work_dir = WORK_DIR
    # execute() depends on these collaborators; stub them so we exercise only
    # the trace-collection logic.
    sandbox._runtime_call = AsyncMock(side_effect=_passthrough_runtime_call)
    sandbox._list_result_files = AsyncMock(return_value=[])
    sandbox.aread_file_text = AsyncMock(return_value=_trace_jsonl())
    return sandbox


@pytest.fixture
def mock_runtime():
    runtime = AsyncMock(spec=SandboxRuntime)
    runtime.working_dir = WORK_DIR
    runtime.fetch_working_dir = AsyncMock(return_value=WORK_DIR)
    runtime.exec = AsyncMock(return_value=ExecResult("", "", 0))
    runtime.upload_file = AsyncMock()
    runtime.code_run = AsyncMock(return_value=CodeRunResult("out", "", 0, []))
    return runtime


@pytest.fixture
def mock_provider(mock_runtime):
    provider = AsyncMock(spec=SandboxProvider)
    provider.is_transient_error = MagicMock(return_value=False)
    return provider


@patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
@pytest.mark.asyncio
async def test_mcp_trace_populated_on_success(
    mock_create_provider, mock_provider, mock_runtime
):
    mock_create_provider.return_value = mock_provider
    sandbox = _make_sandbox(mock_runtime)

    result = await sandbox.execute("print('hi')", auto_install=False)

    assert result.success is True
    assert [r["tool"] for r in result.mcp_trace] == [
        "get_stock_daily_prices",
        "get_company_overview",
    ]
    assert result.mcp_trace[0]["result_sha256"] == "a" * 64

    # MCP_TRACE_FILE is injected into the code_run env, under .system/trace.
    _, kwargs = mock_runtime.code_run.call_args
    trace_file = kwargs["env"]["MCP_TRACE_FILE"]
    assert trace_file.startswith(f"{WORK_DIR}/.system/trace/")
    assert trace_file.endswith(".jsonl")


@patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
@pytest.mark.asyncio
async def test_mcp_trace_populated_on_crash(
    mock_create_provider, mock_provider, mock_runtime
):
    mock_create_provider.return_value = mock_provider
    sandbox = _make_sandbox(mock_runtime)
    # Crash during execution — lines flushed before the crash must be recovered.
    mock_runtime.code_run = AsyncMock(side_effect=RuntimeError("boom"))

    result = await sandbox.execute("boom()", auto_install=False)

    assert result.success is False
    assert [r["tool"] for r in result.mcp_trace] == [
        "get_stock_daily_prices",
        "get_company_overview",
    ]
    # The crash path still read the trace file back.
    sandbox.aread_file_text.assert_awaited()


@patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
@pytest.mark.asyncio
async def test_malformed_trace_lines_skipped(
    mock_create_provider, mock_provider, mock_runtime
):
    mock_create_provider.return_value = mock_provider
    sandbox = _make_sandbox(mock_runtime)
    sandbox.aread_file_text = AsyncMock(
        return_value=(
            json.dumps(_TRACE_LINES[0])
            + "\n{ this is not json\n\n"  # malformed + blank lines
            + json.dumps(_TRACE_LINES[1])
            + "\n"
        )
    )

    result = await sandbox.execute("print('hi')", auto_install=False)

    assert len(result.mcp_trace) == 2
    assert result.mcp_trace[0]["tool"] == "get_stock_daily_prices"
    assert result.mcp_trace[1]["tool"] == "get_company_overview"


@patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
@pytest.mark.asyncio
async def test_mcp_trace_empty_when_no_file(
    mock_create_provider, mock_provider, mock_runtime
):
    mock_create_provider.return_value = mock_provider
    sandbox = _make_sandbox(mock_runtime)
    sandbox.aread_file_text = AsyncMock(return_value=None)

    result = await sandbox.execute("print('hi')", auto_install=False)

    assert result.success is True
    assert result.mcp_trace == []
