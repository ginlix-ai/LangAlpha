"""
Tests for DaytonaRuntime and DaytonaProvider with mocked Daytona SDK.

Covers:
- DaytonaRuntime delegates all operations to the SDK sandbox object
- DaytonaRuntime maps SDK state strings to RuntimeState enum
- DaytonaRuntime exposes correct abilities and metadata
- DaytonaProvider.create() returns DaytonaRuntime
- DaytonaProvider.get() returns DaytonaRuntime
- DaytonaProvider.close() delegates to client
- DaytonaProvider.is_transient_error() classifies errors correctly
"""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import DaytonaConfig
from ptc_agent.core.sandbox._defaults import SANDBOX_IMAGE_ENV, sandbox_thread_env
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    RuntimeState,
    SandboxGoneError,
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
        from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime

        return DaytonaRuntime(mock_sdk_sandbox)

    @pytest.mark.asyncio
    async def test_exec_delegates(self, runtime, mock_sdk_sandbox):
        result = await runtime.exec("echo hello")
        mock_sdk_sandbox.process.exec.assert_called_once()
        assert isinstance(result, ExecResult)
        assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_upload_file_delegates(self, runtime, mock_sdk_sandbox):
        from ptc_agent.core.sandbox.providers.daytona_runtime import _FS_TIMEOUT_S

        await runtime.upload_file(b"data", "/path/file.txt")
        mock_sdk_sandbox.fs.upload_file.assert_called_once()
        assert (
            mock_sdk_sandbox.fs.upload_file.call_args.kwargs["timeout"]
            == _FS_TIMEOUT_S
        )

    @pytest.mark.asyncio
    async def test_upload_files_delegates(self, runtime, mock_sdk_sandbox):
        from ptc_agent.core.sandbox.providers.daytona_runtime import _FS_TIMEOUT_S

        await runtime.upload_files([(b"a", "/a.txt"), (b"b", "/b.txt")])
        mock_sdk_sandbox.fs.upload_files.assert_called_once()
        assert (
            mock_sdk_sandbox.fs.upload_files.call_args.kwargs["timeout"]
            == _FS_TIMEOUT_S
        )

    @pytest.mark.asyncio
    async def test_download_file_delegates(self, runtime, mock_sdk_sandbox):
        from ptc_agent.core.sandbox.providers.daytona_runtime import _FS_TIMEOUT_S

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
    async def test_get_state_maps_restoring_to_starting(self, runtime, mock_sdk_sandbox):
        """A reconnect racing an archive restore must wait, not call start again."""
        mock_sdk_sandbox.state = "restoring"
        state = await runtime.get_state()
        assert state == RuntimeState.STARTING

    @pytest.mark.asyncio
    async def test_get_state_maps_archiving(self, runtime, mock_sdk_sandbox):
        """Never error: a start mid-archive can break the sandbox for good."""
        mock_sdk_sandbox.state = "archiving"
        state = await runtime.get_state()
        assert state == RuntimeState.ARCHIVING

    @pytest.mark.asyncio
    async def test_recoverable_error_recovers(self, runtime, mock_sdk_sandbox):
        mock_sdk_sandbox.state = "error"
        mock_sdk_sandbox.recoverable = True
        await runtime.recover_from_error(timeout=90)
        mock_sdk_sandbox.recover.assert_awaited_once_with(timeout=90)
        mock_sdk_sandbox.start.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unrecoverable_error_is_gone(self, runtime, mock_sdk_sandbox):
        """Daytona refuses start and recover alike, so a restart fails every open."""
        mock_sdk_sandbox.state = "error"
        mock_sdk_sandbox.recoverable = False
        mock_sdk_sandbox.error_reason = "error starting container: timed out"
        with pytest.raises(SandboxGoneError) as gone:
            await runtime.recover_from_error()
        assert "starting" not in str(gone.value)
        mock_sdk_sandbox.recover.assert_not_awaited()
        mock_sdk_sandbox.start.assert_not_awaited()

    @pytest.mark.parametrize(
        ("state", "recoverable"),
        [("resizing", False), ("error", None)],
    )
    @pytest.mark.asyncio
    async def test_no_verdict_restarts(self, runtime, mock_sdk_sandbox, state, recoverable):
        """Unlisted states read as error and a healthy sandbox reads not recoverable."""
        mock_sdk_sandbox.state = state
        mock_sdk_sandbox.recoverable = recoverable
        await runtime.recover_from_error(timeout=90)
        mock_sdk_sandbox.start.assert_awaited_once_with(timeout=90)
        mock_sdk_sandbox.recover.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_retry_after_a_recovery_that_landed_is_a_no_op(self, runtime, mock_sdk_sandbox):
        mock_sdk_sandbox.state = "started"
        mock_sdk_sandbox.recoverable = False
        await runtime.recover_from_error()
        mock_sdk_sandbox.start.assert_not_awaited()
        mock_sdk_sandbox.recover.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_archive_delegates(self, runtime, mock_sdk_sandbox):
        await runtime.archive()
        mock_sdk_sandbox.archive.assert_called_once()

    def test_id_property(self, runtime):
        assert runtime.id == "daytona-123"

    def test_working_dir_property(self, runtime):
        assert runtime.working_dir == "/home/workspace"

    def test_capabilities_includes_archive_and_snapshot(self, runtime):
        assert runtime.archives
        assert runtime.updates_env
        assert runtime.autostops

    @pytest.mark.asyncio
    async def test_get_metadata(self, runtime):
        meta = await runtime.get_metadata()
        assert meta["id"] == "daytona-123"
        assert meta["working_dir"] == "/home/workspace"

    @pytest.mark.asyncio
    async def test_finished_command_output_is_read_after_its_status(
        self, runtime, mock_sdk_sandbox
    ):
        # Logs fetched alongside the status that shows the command finished
        # may predate its last write, so the ones returned come after it.
        process = mock_sdk_sandbox.process
        process.execute_session_command = AsyncMock(return_value=MagicMock(cmd_id="c1"))
        process.get_session = AsyncMock(
            return_value=MagicMock(commands=[MagicMock(id="c1", exit_code=0)])
        )
        process.get_session_command_logs = AsyncMock(
            side_effect=[MagicMock(stdout="partial", stderr=""), MagicMock(stdout="all", stderr="")]
        )
        await runtime.session_execute("bg-0123456789ab", "job")

        result = await runtime.session_logs("bg-0123456789ab")

        assert (result.exit_code, result.stdout) == (0, "all")

    @pytest.mark.asyncio
    async def test_logs_follow_the_latest_command_not_the_one_started_here(
        self, runtime, mock_sdk_sandbox
    ):
        process = mock_sdk_sandbox.process
        process.execute_session_command = AsyncMock(return_value=MagicMock(cmd_id="c1"))
        process.get_session = AsyncMock(
            return_value=MagicMock(
                commands=[MagicMock(id="c1", exit_code=0), MagicMock(id="c2", exit_code=None)]
            )
        )

        async def logs(session_id, command_id):
            return MagicMock(stdout=command_id, stderr="")

        process.get_session_command_logs = AsyncMock(side_effect=logs)
        await runtime.session_execute("bg-0123456789ab", "job")

        result = await runtime.session_logs("bg-0123456789ab")

        assert (result.cmd_id, result.exit_code, result.stdout) == ("c2", None, "c2")


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
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider
        from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime

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

    def test_snapshot_hash_covers_image_env(self):
        """An env-only edit must move the hash. Nothing else in the hash changes
        when a variable is added to SANDBOX_IMAGE_ENV, so without this the name
        is unchanged, the old snapshot is reused verbatim, and the new variable
        is never built (NODE_PATH shipped dead this way)."""
        from daytona import Resources

        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._working_dir = "/home/workspace"
        res = Resources(cpu=1, memory=1, disk=3)

        baseline = provider._get_snapshot_hash([], resources=res)
        with patch(
            "ptc_agent.core.sandbox.providers._image_recipe.SANDBOX_IMAGE_ENV",
            {**SANDBOX_IMAGE_ENV, "NEW_VAR": "/somewhere"},
        ):
            with_added = provider._get_snapshot_hash([], resources=res)

        assert baseline != with_added

    def test_snapshot_image_bakes_every_shared_env_var(self):
        """Both delivery layers carry the same values: the image ENV here, and
        the create-time injection in PTCSandbox._build_sandbox_env_vars."""
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._working_dir = "/home/workspace"

        dockerfile = provider._create_snapshot_image([]).dockerfile()
        for key, value in SANDBOX_IMAGE_ENV.items():
            assert f"ENV {key}={value}" in dockerfile

    def test_snapshot_image_caps_threads_at_the_tier_cpu(self):
        """The tier's cpu is baked as the BLAS/OpenMP cap. NumPy sizes its pools
        from the host's visible CPU count, which the sandbox's cgroup does not
        mask, so an uncapped 2-vCPU sandbox oversubscribes by an order of
        magnitude. A snapshot is per tier, so the image can carry the number."""
        from daytona import Resources

        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._working_dir = "/home/workspace"

        dockerfile = provider._create_snapshot_image(
            [], resources=Resources(cpu=4, memory=8, disk=10)
        ).dockerfile()
        for key in sandbox_thread_env(4):
            assert f"ENV {key}=4" in dockerfile

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

    @pytest.mark.asyncio
    async def test_prebuilt_snapshot_name_is_the_one_create_looks_for(self):
        """scripts/daytona_snapshots.py builds what ``snapshot_name`` says; a
        sandbox only starts from it if create asks for the same name."""
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._working_dir = "/home/workspace"
        provider._config = DaytonaConfig(api_key="test-key", snapshot_name="la")
        client = AsyncMock()
        client.snapshot.list = AsyncMock(return_value=MagicMock(items=[]))
        provider._client = client

        for tier in provider._config.resource_tiers:
            _, resources = provider._resolve_tier(tier)
            asked = await provider._ensure_snapshot(
                ["pkg"], tier=tier, resources=resources
            )
            assert asked == provider.snapshot_name(["pkg"], tier=tier)
            assert asked.startswith(f"la-{tier}-")

    @staticmethod
    def _snapshot_provider(client):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._working_dir = "/home/workspace"
        provider._config = DaytonaConfig(api_key="test-key")
        provider._client = client
        return provider

    @pytest.mark.asyncio
    @pytest.mark.parametrize("listed", ["pending", "pulling", "building", "snapshotting"])
    async def test_a_build_under_way_is_waited_on_not_duplicated(self, listed, monkeypatch):
        """Another worker's first create, or the prebuild, holds the name in any
        of these on its way to active, and a sandbox started before then fails."""
        monkeypatch.setattr("asyncio.sleep", AsyncMock())
        client = AsyncMock()
        provider = self._snapshot_provider(client)
        choice, resources = provider._resolve_tier(None)
        name = provider.snapshot_name(["pkg"])
        client.snapshot.list = AsyncMock(
            return_value=MagicMock(items=[SimpleNamespace(name=name, state=listed)])
        )
        client.snapshot.get = AsyncMock(
            side_effect=[SimpleNamespace(state=listed), SimpleNamespace(state="active")]
        )

        got = await provider._ensure_snapshot(["pkg"], tier=choice.name, resources=resources)

        assert got == name
        assert client.snapshot.get.await_count == 2
        client.snapshot.create.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("settles", "usable"), [("active", True), ("build_failed", False)])
    async def test_a_name_another_build_took_is_used_only_once_active(
        self, settles, usable, monkeypatch
    ):
        monkeypatch.setattr("asyncio.sleep", AsyncMock())
        client = AsyncMock()
        provider = self._snapshot_provider(client)
        choice, resources = provider._resolve_tier(None)
        name = provider.snapshot_name(["pkg"])
        client.snapshot.list = AsyncMock(return_value=MagicMock(items=[]))
        client.snapshot.create = AsyncMock(
            side_effect=RuntimeError(f"Snapshot with name {name} already exists")
        )
        client.snapshot.get = AsyncMock(
            side_effect=[SimpleNamespace(state="pending"), SimpleNamespace(state=settles)]
        )

        got = await provider._ensure_snapshot(["pkg"], tier=choice.name, resources=resources)

        assert got == (name if usable else None)

    @patch("ptc_agent.core.sandbox.providers.daytona.AsyncDaytona")
    @pytest.mark.asyncio
    async def test_create_default_tier_resolves_configured_resources(
        self, MockAsyncDaytona, mock_daytona_client
    ):
        """C2: the default tier's configured cpu/mem/disk are now applied
        (resolved and baked into its snapshot) instead of being inert."""
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        provider = DaytonaProvider.__new__(DaytonaProvider)
        provider._config = DaytonaConfig(api_key="test-key")  # standard 1/2/5
        provider._working_dir = "/home/workspace"
        provider._client = mock_daytona_client

        mock_ensure = AsyncMock(return_value="ptc-base-standard-deadbeef")
        with patch.object(provider, "_ensure_snapshot", mock_ensure):
            await provider.create()

        kwargs = mock_ensure.call_args.kwargs
        assert kwargs["tier"] == "standard"
        assert kwargs["resources"].cpu == 1
        assert kwargs["resources"].memory == 2
        assert kwargs["resources"].disk == 5

    @patch("ptc_agent.core.sandbox.providers.daytona.AsyncDaytona")
    @pytest.mark.asyncio
    async def test_create_default_tier_no_raise_when_snapshot_missing(
        self, MockAsyncDaytona, mock_daytona_client
    ):
        """C2 guardrail: the default tier degrades to a base-sized sandbox when
        its snapshot can't be built — base-size ~= default, so no hard failure."""
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider
        from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime

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
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider
        from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime

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
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider
        from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime

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
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider
        from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime

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


# ---------------------------------------------------------------------------
# DaytonaProvider.metrics_since
# ---------------------------------------------------------------------------

_T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def _bucket(ts: datetime, **fields) -> SimpleNamespace:
    """An SDK ``SandboxMetrics``: what Daytona did not report is 0."""
    values = {
        "cpu_count": 2,
        "cpu_used_pct": 12.5,
        "disk_total": 10 * 2**30,
        "disk_used": 2**30,
        "mem_total": 4 * 2**30,
        "mem_used": 2**29,
        "mem_cache": 2**28,
    }
    return SimpleNamespace(timestamp=ts, **{**values, **fields})


def _metrics_provider(buckets):
    from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

    provider = DaytonaProvider.__new__(DaytonaProvider)
    sdk_sandbox = MagicMock()
    sdk_sandbox.get_metrics = AsyncMock(return_value=buckets)
    sdk_sandbox.start = AsyncMock()
    provider._client = MagicMock()
    provider._client.get = AsyncMock(return_value=sdk_sandbox)
    return provider, sdk_sandbox


class TestDaytonaMetricsSince:
    def test_reports_metrics(self):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        assert DaytonaProvider.reports_metrics is True

    @pytest.mark.asyncio
    async def test_maps_fields_and_reads_without_starting(self):
        provider, sdk_sandbox = _metrics_provider([_bucket(_T0 + timedelta(minutes=1))])
        until = _T0 + timedelta(hours=1)

        [sample] = await provider.metrics_since("sbx", _T0, until=until)

        provider._client.get.assert_awaited_once_with("sbx")
        sdk_sandbox.start.assert_not_awaited()
        sdk_sandbox.get_metrics.assert_awaited_once_with(_T0, until)
        assert sample.timestamp == _T0 + timedelta(minutes=1)
        assert sample.cpu_used_pct == 12.5
        assert sample.cpu_count == 2
        assert sample.mem_used_bytes == 2**29
        assert sample.mem_cache_bytes == 2**28
        assert sample.mem_total_bytes == 4 * 2**30
        assert sample.disk_used_bytes == 2**30
        assert sample.disk_total_bytes == 10 * 2**30

    @pytest.mark.asyncio
    async def test_without_until_the_window_ends_now(self):
        provider, sdk_sandbox = _metrics_provider([])

        await provider.metrics_since("sbx", _T0)

        end = sdk_sandbox.get_metrics.await_args.args[1]
        assert end.tzinfo is not None
        assert abs(datetime.now(UTC) - end) < timedelta(seconds=5)

    @pytest.mark.asyncio
    async def test_unreported_sizes_are_none_and_idle_cpu_is_a_reading(self):
        provider, _ = _metrics_provider(
            [
                _bucket(
                    _T0 + timedelta(minutes=1),
                    cpu_count=0,
                    cpu_used_pct=0.0,
                    disk_total=0,
                    disk_used=0,
                    mem_total=0,
                )
            ]
        )

        [sample] = await provider.metrics_since("sbx", _T0)

        assert sample.cpu_used_pct == 0.0
        assert sample.cpu_count is None
        assert sample.mem_total_bytes is None
        assert sample.disk_total_bytes is None
        assert sample.disk_used_bytes is None

    @pytest.mark.asyncio
    async def test_window_is_strict_after_since_and_up_to_until(self):
        until = _T0 + timedelta(minutes=10)
        provider, _ = _metrics_provider(
            [
                _bucket(_T0),
                _bucket(_T0 + timedelta(minutes=5)),
                _bucket(until),
                _bucket(until + timedelta(seconds=1)),
            ]
        )

        samples = await provider.metrics_since("sbx", _T0, until=until)

        assert [s.timestamp for s in samples] == [_T0 + timedelta(minutes=5), until]

    @pytest.mark.asyncio
    async def test_samples_are_oldest_first_and_utc(self):
        naive = datetime(2026, 1, 1, 12, 5)
        provider, _ = _metrics_provider(
            [
                _bucket(_T0 + timedelta(minutes=9)),
                _bucket(naive),
                _bucket(_T0 + timedelta(minutes=7)),
            ]
        )

        samples = await provider.metrics_since("sbx", _T0)

        assert [s.timestamp for s in samples] == [
            _T0 + timedelta(minutes=5),
            _T0 + timedelta(minutes=7),
            _T0 + timedelta(minutes=9),
        ]
        assert all(s.timestamp.utcoffset() == timedelta(0) for s in samples)

    @pytest.mark.asyncio
    async def test_sdk_errors_propagate(self):
        provider, sdk_sandbox = _metrics_provider([])
        sdk_sandbox.get_metrics.side_effect = RuntimeError("telemetry down")

        with pytest.raises(RuntimeError, match="telemetry down"):
            await provider.metrics_since("sbx", _T0)

        provider._client.get.side_effect = RuntimeError("lookup failed")
        with pytest.raises(RuntimeError, match="lookup failed"):
            await provider.metrics_since("sbx", _T0)

    @pytest.mark.asyncio
    async def test_a_429_on_the_cause_is_a_rate_limited_error(self):
        from daytona import DaytonaError

        from ptc_agent.core.sandbox.runtime import SandboxRateLimitedError

        provider, sdk_sandbox = _metrics_provider([])
        cause = RuntimeError("too many requests")
        cause.status = 429  # type: ignore[attr-defined]
        error = DaytonaError("Failed to get sandbox metrics\nrate limited")
        error.__cause__ = cause
        sdk_sandbox.get_metrics.side_effect = error

        with pytest.raises(SandboxRateLimitedError, match="Failed to get sandbox metrics"):
            await provider.metrics_since("sbx", _T0)

    @pytest.mark.asyncio
    async def test_any_other_status_propagates_unchanged(self):
        from daytona import DaytonaError

        from ptc_agent.core.sandbox.runtime import SandboxRateLimitedError

        provider, sdk_sandbox = _metrics_provider([])
        error = DaytonaError("not found", status_code=404)
        sdk_sandbox.get_metrics.side_effect = error

        with pytest.raises(DaytonaError) as raised:
            await provider.metrics_since("sbx", _T0)

        assert raised.value is error
        assert not isinstance(raised.value, SandboxRateLimitedError)

    @pytest.mark.asyncio
    async def test_memory_in_use_is_not_adjusted_by_the_cache(self):
        provider, _ = _metrics_provider(
            [_bucket(_T0 + timedelta(minutes=1), mem_used=2**29, mem_cache=2**28)]
        )

        [sample] = await provider.metrics_since("sbx", _T0)

        assert sample.mem_used_bytes == 2**29
        assert sample.mem_cache_bytes == 2**28
