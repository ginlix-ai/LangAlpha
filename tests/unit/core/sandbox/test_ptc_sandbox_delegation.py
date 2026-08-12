"""
Tests for PTCSandbox delegation to runtime/provider after refactor.

Verifies that PTCSandbox routes operations through the abstract
SandboxRuntime/SandboxProvider interfaces rather than calling
the Daytona SDK directly.

Covers:
- execute_bash_command -> runtime.exec
- aupload_file_bytes -> runtime.upload_file
- adownload_file_bytes -> runtime.download_file
- als_directory -> runtime.list_files
- stop_sandbox -> runtime.stop
- cleanup -> runtime.delete + provider.close
- close -> provider.close
"""

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
    RuntimeState,
    SandboxGoneError,
    SandboxProvider,
    SandboxRuntime,
)


def _make_config(**overrides) -> CoreConfig:
    defaults = dict(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )
    defaults.update(overrides)
    return CoreConfig(**defaults)


@pytest.fixture
def mock_runtime():
    runtime = AsyncMock(spec=SandboxRuntime)
    runtime.id = "mock-runtime-1"
    runtime.working_dir = "/home/workspace"
    runtime.exec = AsyncMock(return_value=ExecResult("output", "", 0))
    runtime.upload_file = AsyncMock()
    runtime.upload_files = AsyncMock()
    runtime.download_file = AsyncMock(return_value=b"data")
    runtime.list_files = AsyncMock(return_value=[{"name": "file.txt", "is_dir": False}])
    runtime.code_run = AsyncMock(return_value=CodeRunResult("result", "", 0, []))
    runtime.get_state = AsyncMock(return_value=RuntimeState.RUNNING)
    runtime.start = AsyncMock()
    runtime.stop = AsyncMock()
    runtime.delete = AsyncMock()
    return runtime


@pytest.fixture
def mock_provider(mock_runtime):
    provider = AsyncMock(spec=SandboxProvider)
    provider.create = AsyncMock(return_value=mock_runtime)
    provider.get = AsyncMock(return_value=mock_runtime)
    provider.close = AsyncMock()
    provider.is_transient_error = MagicMock(return_value=False)
    return provider


class TestPTCSandboxDelegation:
    """Patch create_provider to return mock, verify PTCSandbox routes through runtime."""

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_execute_bash_routes_to_runtime_exec(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        await sandbox.execute_bash_command("ls -la")
        mock_runtime.exec.assert_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_execute_bash_injects_mcp_trace_env_and_harvests(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        # A python script run via Bash must record the same MCP provenance
        # ExecuteCode does: the foreground command carries an MCP_TRACE_FILE env +
        # the wrapper PYTHONPATH, and the harvested trace is returned for the
        # provenance middleware. (Closes the bash provenance bypass.)
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        mock_runtime.fetch_working_dir = AsyncMock(return_value="/home/workspace")
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        trace = [{"server": "marketdata", "tool": "quote", "result_sha256": "a" * 64}]
        with patch.object(
            sandbox, "_collect_mcp_trace", AsyncMock(return_value=trace)
        ) as collect:
            result = await sandbox.execute_bash_command("python analysis.py")

        exec_cmds = [c.args[0] for c in mock_runtime.exec.call_args_list if c.args]
        main_cmd = next(c for c in exec_cmds if "python analysis.py" in c)
        assert "export MCP_TRACE_FILE=" in main_cmd
        assert "export PYTHONPATH=" in main_cmd
        collect.assert_awaited_once()
        assert result["mcp_trace"] == trace

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_aupload_file_bytes_routes_to_runtime(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        await sandbox.aupload_file_bytes("/test/file.txt", b"content")
        mock_runtime.upload_file.assert_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_adownload_file_bytes_routes_to_runtime(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        await sandbox.adownload_file_bytes("/test/file.txt")
        mock_runtime.download_file.assert_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_als_directory_routes_to_runtime(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        await sandbox.als_directory("/home/workspace")
        mock_runtime.list_files.assert_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_stop_sandbox_routes_to_runtime(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        await sandbox.stop_sandbox()
        mock_runtime.stop.assert_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_cleanup_routes_to_runtime_and_provider(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime

        await sandbox.cleanup()
        mock_runtime.delete.assert_called()
        mock_provider.close.assert_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_close_routes_to_provider(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())

        await sandbox.close()
        mock_provider.close.assert_called()

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_unrecoverable_archive_uses_secure_replacement_path(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        mock_runtime.get_state.return_value = RuntimeState.ARCHIVED
        mock_runtime.get_metadata.return_value = {"recoverable": False}
        sandbox = PTCSandbox(config=_make_config())

        with pytest.raises(SandboxGoneError, match="no longer recoverable"):
            await sandbox.reconnect("archived-sandbox")

        mock_runtime.start.assert_not_awaited()


class TestBackgroundBashTrace:
    """The background-bash provenance bypass closure (BashOutput is the only
    result-bearing path for a backgrounded command, so the MCP trace must be
    injected at launch and harvested on the status read that sees completion)."""

    def _sandbox(self, mock_create_provider, mock_provider, mock_runtime):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        mock_create_provider.return_value = mock_provider
        sandbox = PTCSandbox(config=_make_config())
        sandbox.runtime = mock_runtime
        return sandbox

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_launch_injects_trace_env_and_records_path(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.runtime import SessionCommandResult

        sandbox = self._sandbox(mock_create_provider, mock_provider, mock_runtime)
        mock_runtime.fetch_working_dir = AsyncMock(return_value="/home/workspace")
        mock_runtime.create_session = AsyncMock()
        mock_runtime.session_execute = AsyncMock(
            return_value=SessionCommandResult(
                cmd_id="cmd-xyz", exit_code=None, stdout="", stderr=""
            )
        )

        result = await sandbox.execute_bash_command(
            "python long_job.py", background=True
        )

        # The executed bg command carries the MCP trace env (so a backgrounded
        # python script importing the wrappers records its calls) ...
        bg_cmd = mock_runtime.session_execute.call_args.args[1]
        assert "export MCP_TRACE_FILE=" in bg_cmd
        assert "export PYTHONPATH=" in bg_cmd
        assert "python long_job.py" in bg_cmd
        # ... and the trace path is tracked under the returned cmd_id for harvest.
        assert "cmd-xyz" in result["stdout"]
        assert sandbox._bg_trace_paths.get("cmd-xyz", "").endswith(".jsonl")

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_status_harvests_trace_once_on_completion(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.runtime import SessionCommandResult

        sandbox = self._sandbox(mock_create_provider, mock_provider, mock_runtime)
        sandbox._bg_sessions["cmd-1"] = "bg-bash_0001"
        sandbox._bg_trace_paths["cmd-1"] = "/home/workspace/.system/trace/t.jsonl"
        mock_runtime.session_command_logs = AsyncMock(
            return_value=SessionCommandResult(
                cmd_id="cmd-1", exit_code=0, stdout="done", stderr=""
            )
        )
        trace = [{"server": "marketdata", "tool": "quote", "result_sha256": "a" * 64}]
        with patch.object(
            sandbox, "_collect_mcp_trace", AsyncMock(return_value=trace)
        ) as collect:
            result = await sandbox.get_background_command_status("cmd-1")

        collect.assert_awaited_once()
        assert result["mcp_trace"] == trace
        # Trace path is consumed so a second status read can't re-emit it.
        assert "cmd-1" not in sandbox._bg_trace_paths

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_status_while_running_does_not_harvest(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        from ptc_agent.core.sandbox.runtime import SessionCommandResult

        sandbox = self._sandbox(mock_create_provider, mock_provider, mock_runtime)
        sandbox._bg_sessions["cmd-1"] = "bg-bash_0001"
        sandbox._bg_trace_paths["cmd-1"] = "/home/workspace/.system/trace/t.jsonl"
        mock_runtime.session_command_logs = AsyncMock(
            return_value=SessionCommandResult(
                cmd_id="cmd-1", exit_code=None, stdout="...", stderr=""
            )
        )
        with patch.object(
            sandbox, "_collect_mcp_trace", AsyncMock(return_value=[])
        ) as collect:
            result = await sandbox.get_background_command_status("cmd-1")

        collect.assert_not_awaited()
        assert result["mcp_trace"] == []
        # Still running → trace path retained for the eventual completion read.
        assert "cmd-1" in sandbox._bg_trace_paths

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_status_unknown_cmd_returns_empty_trace(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        sandbox = self._sandbox(mock_create_provider, mock_provider, mock_runtime)
        result = await sandbox.get_background_command_status("nope")
        assert result["mcp_trace"] == []

    @patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider")
    @pytest.mark.asyncio
    async def test_stop_drops_trace_mapping(
        self, mock_create_provider, mock_provider, mock_runtime
    ):
        sandbox = self._sandbox(mock_create_provider, mock_provider, mock_runtime)
        sandbox._bg_sessions["cmd-1"] = "bg-bash_0001"
        sandbox._bg_trace_paths["cmd-1"] = "/home/workspace/.system/trace/t.jsonl"
        mock_runtime.delete_session = AsyncMock()

        stopped = await sandbox.stop_background_command("cmd-1")
        assert stopped is True
        # A stopped command yields no output, so nothing to attest — drop the map.
        assert "cmd-1" not in sandbox._bg_trace_paths
