"""
Tests for DaytonaRuntime and DaytonaProvider with mocked Daytona SDK.

Covers:
- DaytonaRuntime delegates all operations to the SDK sandbox object
- DaytonaRuntime maps SDK state strings to RuntimeState enum
- DaytonaRuntime exposes correct capabilities and metadata
- DaytonaProvider.create() returns DaytonaRuntime
- DaytonaProvider.get() returns DaytonaRuntime
- DaytonaProvider.close() delegates to client
- DaytonaProvider.is_transient_error() classifies errors correctly
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import DaytonaConfig
from ptc_agent.core.sandbox.runtime import (
    CodeRunResult,
    ExecResult,
    RuntimeState,
)


# ---------------------------------------------------------------------------
# DaytonaRuntime
# ---------------------------------------------------------------------------


class TestDaytonaRuntime:
    """Mock the SDK sandbox object, verify DaytonaRuntime delegates correctly."""

    @pytest.fixture
    def mock_sdk_sandbox(self):
        sandbox = AsyncMock()
        sandbox.id = "daytona-123"
        sandbox.state = "started"
        sandbox.process.exec = AsyncMock(
            return_value=MagicMock(output="hello", exit_code=0)
        )
        sandbox.process.code_run = AsyncMock(
            return_value=MagicMock(output="42", exit_code=0, artifacts=[])
        )
        sandbox.fs.upload_file = AsyncMock()
        sandbox.fs.upload_files = AsyncMock()
        sandbox.fs.download_file = AsyncMock(return_value=b"content")
        sandbox.fs.list_files = AsyncMock(return_value=[])
        sandbox.start = AsyncMock()
        sandbox.stop = AsyncMock()
        sandbox.delete = AsyncMock()
        sandbox.archive = AsyncMock()
        sandbox.get_work_dir = MagicMock(return_value="/home/workspace")
        sandbox.cpu = "2"
        sandbox.memory = "4096"
        sandbox.created_at = "2026-01-01T00:00:00Z"
        sandbox.auto_stop_interval = 3600
        return sandbox

    @pytest.fixture
    def runtime(self, mock_sdk_sandbox):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaRuntime

        return DaytonaRuntime(mock_sdk_sandbox)

    @pytest.mark.asyncio
    async def test_exec_delegates(self, runtime, mock_sdk_sandbox):
        result = await runtime.exec("echo hello")
        mock_sdk_sandbox.process.exec.assert_called_once()
        assert isinstance(result, ExecResult)
        assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_code_run_delegates(self, runtime, mock_sdk_sandbox):
        result = await runtime.code_run("print(42)")
        mock_sdk_sandbox.process.code_run.assert_called_once()
        assert isinstance(result, CodeRunResult)
        assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_upload_file_delegates(self, runtime, mock_sdk_sandbox):
        from ptc_agent.core.sandbox.providers.daytona import _FS_TIMEOUT_S

        await runtime.upload_file(b"data", "/path/file.txt")
        mock_sdk_sandbox.fs.upload_file.assert_called_once()
        assert (
            mock_sdk_sandbox.fs.upload_file.call_args.kwargs["timeout"]
            == _FS_TIMEOUT_S
        )

    @pytest.mark.asyncio
    async def test_upload_files_delegates(self, runtime, mock_sdk_sandbox):
        from ptc_agent.core.sandbox.providers.daytona import _FS_TIMEOUT_S

        await runtime.upload_files([(b"a", "/a.txt"), (b"b", "/b.txt")])
        mock_sdk_sandbox.fs.upload_files.assert_called_once()
        assert (
            mock_sdk_sandbox.fs.upload_files.call_args.kwargs["timeout"]
            == _FS_TIMEOUT_S
        )

    @pytest.mark.asyncio
    async def test_download_file_delegates(self, runtime, mock_sdk_sandbox):
        from ptc_agent.core.sandbox.providers.daytona import _FS_TIMEOUT_S

        data = await runtime.download_file("/path/file.txt")
        mock_sdk_sandbox.fs.download_file.assert_called_once_with(
            "/path/file.txt", _FS_TIMEOUT_S
        )
        assert data == b"content"

    @pytest.mark.asyncio
    async def test_list_files_delegates(self, runtime, mock_sdk_sandbox):
        files = await runtime.list_files("/home/workspace")
        mock_sdk_sandbox.fs.list_files.assert_called_once()
        assert files == []

    @pytest.mark.asyncio
    async def test_start_delegates(self, runtime, mock_sdk_sandbox):
        await runtime.start()
        mock_sdk_sandbox.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_delegates(self, runtime, mock_sdk_sandbox):
        await runtime.stop()
        mock_sdk_sandbox.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_delegates(self, runtime, mock_sdk_sandbox):
        await runtime.delete()
        mock_sdk_sandbox.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_state_maps_started_to_running(self, runtime, mock_sdk_sandbox):
        mock_sdk_sandbox.state = "started"
        state = await runtime.get_state()
        assert state == RuntimeState.RUNNING

    @pytest.mark.asyncio
    async def test_get_state_maps_stopped(self, runtime, mock_sdk_sandbox):
        mock_sdk_sandbox.state = "stopped"
        state = await runtime.get_state()
        assert state == RuntimeState.STOPPED

    @pytest.mark.asyncio
    async def test_archive_delegates(self, runtime, mock_sdk_sandbox):
        await runtime.archive()
        mock_sdk_sandbox.archive.assert_called_once()

    def test_id_property(self, runtime):
        assert runtime.id == "daytona-123"

    def test_working_dir_property(self, runtime):
        assert runtime.working_dir == "/home/workspace"

    def test_capabilities_includes_archive_and_snapshot(self, runtime):
        caps = runtime.capabilities
        assert "exec" in caps
        assert "code_run" in caps
        assert "file_io" in caps
        assert "archive" in caps
        assert "snapshot" in caps

    @pytest.mark.asyncio
    async def test_get_metadata(self, runtime):
        meta = await runtime.get_metadata()
        assert meta["id"] == "daytona-123"
        assert meta["working_dir"] == "/home/workspace"


# ---------------------------------------------------------------------------
# DaytonaProvider
# ---------------------------------------------------------------------------


class TestDaytonaProvider:
    """Mock AsyncDaytona client, verify provider creates/gets runtimes."""

    @pytest.fixture
    def mock_daytona_client(self):
        client = AsyncMock()
        mock_sandbox = MagicMock()
        mock_sandbox.id = "new-123"
        mock_sandbox.state = "started"
        mock_sandbox.get_work_dir = MagicMock(return_value="/home/workspace")
        client.create = AsyncMock(return_value=mock_sandbox)

        mock_existing = MagicMock()
        mock_existing.id = "existing-456"
        mock_existing.state = "started"
        mock_existing.get_work_dir = MagicMock(return_value="/home/workspace")
        client.get = AsyncMock(return_value=mock_existing)

        client.close = AsyncMock()
        return client

    @patch("ptc_agent.core.sandbox.providers.daytona.AsyncDaytona")
    @pytest.mark.asyncio
    async def test_create_returns_runtime(
        self, MockAsyncDaytona, mock_daytona_client
    ):
        from ptc_agent.core.sandbox.providers.daytona import (
            DaytonaProvider,
            DaytonaRuntime,
        )

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._config = DaytonaConfig(api_key="test-key")
        provider._working_dir = "/home/workspace"
        provider._client = mock_daytona_client

        # Bypass snapshot logic for this unit test
        with patch.object(provider, "_ensure_snapshot", return_value=None):
            runtime = await provider.create(env_vars={"FOO": "bar"})

        assert isinstance(runtime, DaytonaRuntime)
        mock_daytona_client.create.assert_called_once()

    @patch("ptc_agent.core.sandbox.providers.daytona.AsyncDaytona")
    @pytest.mark.asyncio
    async def test_create_raises_when_elevated_tier_snapshot_missing(
        self, MockAsyncDaytona, mock_daytona_client
    ):
        """An elevated tier whose snapshot can't be built must fail loudly, not
        silently create a base-sized (under-provisioned) sandbox for a billed tier."""
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._config = DaytonaConfig(api_key="test-key")  # default_tier=standard
        provider._working_dir = "/home/workspace"
        provider._client = mock_daytona_client

        # performance resolves to real resources; snapshot build fails (None).
        with patch.object(provider, "_ensure_snapshot", return_value=None):
            with pytest.raises(RuntimeError, match="tier snapshot"):
                await provider.create(tier="performance")

        mock_daytona_client.create.assert_not_called()

    def test_snapshot_hash_changes_on_resource_retune(self):
        """C1: retuning a tier's cpu (same packages) must change the snapshot
        hash so the stale-sized snapshot isn't silently reused."""
        from daytona import Resources

        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._working_dir = "/home/workspace"

        baseline = provider._get_snapshot_hash(
            ["pkg-a"], resources=Resources(cpu=1, memory=1, disk=3)
        )
        retuned_cpu = provider._get_snapshot_hash(
            ["pkg-a"], resources=Resources(cpu=2, memory=1, disk=3)
        )
        # Same resources, same packages -> stable (deterministic cache key).
        stable = provider._get_snapshot_hash(
            ["pkg-a"], resources=Resources(cpu=1, memory=1, disk=3)
        )
        assert baseline == stable
        assert baseline != retuned_cpu

    def test_config_rejects_default_tier_missing_from_tiers(self):
        """C4a: the default tier must be present in resource_tiers, else the
        create path can't size base sandboxes — fail fast at config load."""
        from ptc_agent.config.core import ResourceTier

        with pytest.raises(ValueError, match="default_tier"):
            DaytonaConfig(
                default_tier="ghost",
                resource_tiers={
                    "standard": ResourceTier(cpu=1, memory=1, disk=3)
                },
            )

    @pytest.mark.asyncio
    async def test_ensure_snapshot_name_changes_on_resource_retune(self):
        """C1 end-to-end: the built snapshot NAME differs across a resize, so a
        retune yields a new snapshot instead of reusing the stale-sized one."""
        from daytona import Resources

        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._working_dir = "/home/workspace"
        provider._config = DaytonaConfig(api_key="test-key")

        client = AsyncMock()
        client.snapshot.list = AsyncMock(return_value=MagicMock(items=[]))
        client.snapshot.create = AsyncMock()
        provider._client = client

        name_small = await provider._ensure_snapshot(
            ["pkg"], tier="performance",
            resources=Resources(cpu=2, memory=4, disk=5),
        )
        name_big = await provider._ensure_snapshot(
            ["pkg"], tier="performance",
            resources=Resources(cpu=4, memory=8, disk=10),
        )
        assert name_small and name_big
        assert name_small != name_big
        assert name_small.startswith("ptc-base-performance-")

    @patch("ptc_agent.core.sandbox.providers.daytona.AsyncDaytona")
    @pytest.mark.asyncio
    async def test_create_default_tier_resolves_configured_resources(
        self, MockAsyncDaytona, mock_daytona_client
    ):
        """C2: the default tier's configured cpu/mem/disk are now applied
        (resolved and baked into its snapshot) instead of being inert."""
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._config = DaytonaConfig(api_key="test-key")  # standard 1/1/3
        provider._working_dir = "/home/workspace"
        provider._client = mock_daytona_client

        mock_ensure = AsyncMock(return_value="ptc-base-standard-deadbeef")
        with patch.object(provider, "_ensure_snapshot", mock_ensure):
            await provider.create()

        kwargs = mock_ensure.call_args.kwargs
        assert kwargs["tier"] == "standard"
        assert kwargs["resources"].cpu == 1
        assert kwargs["resources"].memory == 1
        assert kwargs["resources"].disk == 3

    @patch("ptc_agent.core.sandbox.providers.daytona.AsyncDaytona")
    @pytest.mark.asyncio
    async def test_create_default_tier_no_raise_when_snapshot_missing(
        self, MockAsyncDaytona, mock_daytona_client
    ):
        """C2 guardrail: the default tier degrades to a base-sized sandbox when
        its snapshot can't be built — base-size ~= default, so no hard failure."""
        from ptc_agent.core.sandbox.providers.daytona import (
            DaytonaProvider,
            DaytonaRuntime,
        )

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._config = DaytonaConfig(api_key="test-key")
        provider._working_dir = "/home/workspace"
        provider._client = mock_daytona_client

        with patch.object(provider, "_ensure_snapshot", return_value=None):
            runtime = await provider.create(tier="standard")

        assert isinstance(runtime, DaytonaRuntime)
        mock_daytona_client.create.assert_called_once()

    @patch("ptc_agent.core.sandbox.providers.daytona.AsyncDaytona")
    @pytest.mark.asyncio
    async def test_create_elevated_tier_no_raise_when_snapshots_disabled(
        self, MockAsyncDaytona, mock_daytona_client
    ):
        """Guardrail: snapshots globally disabled never expected a sized snapshot,
        so an elevated tier degrades to base-sized instead of raising."""
        from ptc_agent.core.sandbox.providers.daytona import (
            DaytonaProvider,
            DaytonaRuntime,
        )

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._config = DaytonaConfig(api_key="test-key", snapshot_enabled=False)
        provider._working_dir = "/home/workspace"
        provider._client = mock_daytona_client

        # _ensure_snapshot runs for real and returns None (disabled), no raise.
        runtime = await provider.create(tier="performance")

        assert isinstance(runtime, DaytonaRuntime)
        mock_daytona_client.create.assert_called_once()

    @patch("ptc_agent.core.sandbox.providers.daytona.AsyncDaytona")
    @pytest.mark.asyncio
    async def test_create_unknown_tier_falls_back_without_raising(
        self, MockAsyncDaytona, mock_daytona_client
    ):
        """C4b: a tier removed from config must stay recoverable — warn + base
        size, not raise (raising would lock the workspace out permanently)."""
        from ptc_agent.core.sandbox.providers.daytona import (
            DaytonaProvider,
            DaytonaRuntime,
        )

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._config = DaytonaConfig(api_key="test-key")
        provider._working_dir = "/home/workspace"
        provider._client = mock_daytona_client

        with patch.object(provider, "_ensure_snapshot", return_value=None):
            runtime = await provider.create(tier="ghost-tier")

        assert isinstance(runtime, DaytonaRuntime)
        mock_daytona_client.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_returns_runtime(self, mock_daytona_client):
        from ptc_agent.core.sandbox.providers.daytona import (
            DaytonaProvider,
            DaytonaRuntime,
        )

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._config = DaytonaConfig(api_key="test-key")
        provider._working_dir = "/home/workspace"
        provider._client = mock_daytona_client
        runtime = await provider.get("existing-456")
        assert isinstance(runtime, DaytonaRuntime)
        mock_daytona_client.get.assert_called_once_with("existing-456")

    @pytest.mark.asyncio
    async def test_close_delegates(self, mock_daytona_client):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._config = DaytonaConfig(api_key="test-key")
        provider._working_dir = "/home/workspace"
        provider._client = mock_daytona_client
        await provider.close()
        mock_daytona_client.close.assert_called_once()

    def test_is_transient_error_connection_reset(self):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        assert provider.is_transient_error(
            ConnectionError("connection reset")
        )

    def test_is_transient_error_not_transient(self):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        assert not provider.is_transient_error(ValueError("bad arg"))

    def test_is_transient_error_timeout(self):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        assert provider.is_transient_error(Exception("timed out"))

    def test_is_transient_error_execution_not_transient(self):
        """'failed to execute command' should NOT be transient."""
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        assert not provider.is_transient_error(
            Exception("failed to execute command: timeout reached")
        )

    def test_is_transient_error_502(self):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        assert provider.is_transient_error(Exception("502 bad gateway"))

    def test_is_transient_error_remote_disconnected(self):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        assert provider.is_transient_error(
            Exception("RemoteDisconnected: peer closed")
        )
