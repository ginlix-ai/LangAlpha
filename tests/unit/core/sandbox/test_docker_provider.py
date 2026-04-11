"""Unit tests for DockerRuntime and DockerProvider with mocked aiodocker.

Covers:
- DockerRuntime state mapping (_DOCKER_STATE_MAP)
- DockerRuntime exec: mock container.exec(), verify ExecResult
- DockerRuntime lifecycle: start/stop/delete delegate to container methods
- DockerRuntime capabilities: no "archive", no "snapshot"
- DockerRuntime archive: raises NotImplementedError
- DockerRuntime tar upload/download: mock container.put_archive/get_archive
- DockerRuntime bind upload/download: use tmp_path for real filesystem
- DockerProvider create: mock aiodocker.Docker client
- DockerProvider create with bind-mount: verify host_config includes Binds
- DockerProvider get: mock container lookup
- DockerProvider close: verify client is closed
- DockerProvider is_transient_error: test classification
- _parse_memory helper: test conversions
"""

from __future__ import annotations

import io
import tarfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import DockerConfig
from ptc_agent.core.sandbox.providers.docker import (
    DockerProvider,
    DockerRuntime,
    _DOCKER_STATE_MAP,
    _parse_memory,
    _parse_proxy_port_range,
)
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    RuntimeState,
    SandboxTransientError,
)


# ---------------------------------------------------------------------------
# Helpers for building mock aiodocker objects
# ---------------------------------------------------------------------------


def _make_mock_container(
    *,
    status: str = "running",
    container_id: str = "abc123",
    mounts: list | None = None,
) -> MagicMock:
    """Build a mock aiodocker DockerContainer."""
    container = MagicMock()
    container.start = AsyncMock()
    container.stop = AsyncMock()
    container.delete = AsyncMock()
    container.put_archive = AsyncMock()

    # show() returns container info dict
    container_info = {
        "State": {"Status": status},
        "Id": container_id,
        "Config": {"WorkingDir": "/home/workspace", "Env": []},
        "Mounts": mounts or [],
    }
    container._container = container_info
    container.show = AsyncMock(return_value=container_info)

    return container


def _make_exec_mock(output: str = "", exit_code: int = 0) -> MagicMock:
    """Build a mock exec object matching the aiodocker exec API."""
    msg = MagicMock()
    msg.data = output.encode("utf-8")

    stream = AsyncMock()
    stream.read_out = AsyncMock(side_effect=[msg, None])

    exec_obj = MagicMock()
    # exec_obj.start() returns an async context manager
    ctx_mgr = AsyncMock()
    ctx_mgr.__aenter__ = AsyncMock(return_value=stream)
    ctx_mgr.__aexit__ = AsyncMock(return_value=False)
    exec_obj.start = MagicMock(return_value=ctx_mgr)
    exec_obj.inspect = AsyncMock(return_value={"ExitCode": exit_code})

    return exec_obj


# ---------------------------------------------------------------------------
# _parse_memory helper
# ---------------------------------------------------------------------------


class TestParseMemory:
    def test_bytes_plain(self):
        assert _parse_memory("1024") == 1024

    def test_kilobytes(self):
        assert _parse_memory("1k") == 1024

    def test_kilobytes_with_b(self):
        assert _parse_memory("1kb") == 1024

    def test_megabytes(self):
        assert _parse_memory("512m") == 512 * 1024**2

    def test_megabytes_with_b(self):
        assert _parse_memory("512mb") == 512 * 1024**2

    def test_gigabytes(self):
        assert _parse_memory("4g") == 4 * 1024**3

    def test_gigabytes_with_b(self):
        assert _parse_memory("4gb") == 4 * 1024**3

    def test_terabytes(self):
        assert _parse_memory("1t") == 1024**4

    def test_fractional(self):
        assert _parse_memory("1.5g") == int(1.5 * 1024**3)

    def test_with_whitespace(self):
        assert _parse_memory("  4g  ") == 4 * 1024**3

    def test_uppercase_is_lowered(self):
        assert _parse_memory("4G") == 4 * 1024**3

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Cannot parse memory limit"):
            _parse_memory("not_a_number")


# ---------------------------------------------------------------------------
# DockerRuntime — state mapping
# ---------------------------------------------------------------------------


class TestDockerRuntimeStateMapping:
    """Verify _DOCKER_STATE_MAP covers expected Docker statuses."""

    def test_running_maps_to_running(self):
        assert _DOCKER_STATE_MAP["running"] == RuntimeState.RUNNING

    def test_created_maps_to_stopped(self):
        assert _DOCKER_STATE_MAP["created"] == RuntimeState.STOPPED

    def test_exited_maps_to_stopped(self):
        assert _DOCKER_STATE_MAP["exited"] == RuntimeState.STOPPED

    def test_paused_maps_to_stopped(self):
        assert _DOCKER_STATE_MAP["paused"] == RuntimeState.STOPPED

    def test_dead_maps_to_error(self):
        assert _DOCKER_STATE_MAP["dead"] == RuntimeState.ERROR

    def test_restarting_maps_to_starting(self):
        assert _DOCKER_STATE_MAP["restarting"] == RuntimeState.STARTING

    def test_removing_maps_to_stopping(self):
        assert _DOCKER_STATE_MAP["removing"] == RuntimeState.STOPPING


# ---------------------------------------------------------------------------
# DockerRuntime — properties and lifecycle
# ---------------------------------------------------------------------------


class TestDockerRuntimeProperties:
    @pytest.fixture
    def container(self):
        return _make_mock_container()

    @pytest.fixture
    def runtime(self, container):
        return DockerRuntime(
            container,
            runtime_id="docker-test123",
            working_dir="/home/workspace",
        )

    def test_id_property(self, runtime):
        assert runtime.id == "docker-test123"

    def test_working_dir_property(self, runtime):
        assert runtime.working_dir == "/home/workspace"

    @pytest.mark.asyncio
    async def test_fetch_working_dir(self, runtime):
        result = await runtime.fetch_working_dir()
        assert result == "/home/workspace"

    def test_capabilities_no_archive(self, runtime):
        caps = runtime.capabilities
        assert "exec" in caps
        assert "code_run" in caps
        assert "file_io" in caps
        assert "archive" not in caps
        assert "snapshot" not in caps

    @pytest.mark.asyncio
    async def test_archive_raises_not_implemented(self, runtime):
        with pytest.raises(NotImplementedError, match="does not support archive"):
            await runtime.archive()

    @pytest.mark.asyncio
    async def test_get_metadata(self, runtime):
        meta = await runtime.get_metadata()
        assert meta["id"] == "docker-test123"
        assert meta["working_dir"] == "/home/workspace"
        assert meta["provider"] == "docker"
        assert meta["dev_mode"] is False
        assert "state" in meta
        assert meta["state"] in {s.value for s in RuntimeState}


class TestDockerRuntimeLifecycle:
    @pytest.fixture
    def container(self):
        return _make_mock_container()

    @pytest.fixture
    def runtime(self, container):
        return DockerRuntime(
            container,
            runtime_id="docker-lc",
            working_dir="/home/workspace",
        )

    @pytest.mark.asyncio
    async def test_start_delegates(self, runtime, container):
        await runtime.start()
        container.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_delegates(self, runtime, container):
        await runtime.stop(timeout=30)
        container.stop.assert_called_once_with(t=30)

    @pytest.mark.asyncio
    async def test_delete_stops_then_force_deletes(self, runtime, container):
        await runtime.delete()
        container.stop.assert_called_once_with(t=5)
        container.delete.assert_called_once_with(force=True)

    @pytest.mark.asyncio
    async def test_delete_ignores_stop_error(self, runtime, container):
        """delete() should still force-remove even if stop() fails."""
        container.stop.side_effect = Exception("already stopped")
        await runtime.delete()
        container.delete.assert_called_once_with(force=True)

    @pytest.mark.asyncio
    async def test_get_state_running(self, runtime, container):
        container._container = {"State": {"Status": "running"}}
        state = await runtime.get_state()
        assert state == RuntimeState.RUNNING
        container.show.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_state_exited(self, runtime, container):
        container.show = AsyncMock(return_value={"State": {"Status": "exited"}})
        state = await runtime.get_state()
        assert state == RuntimeState.STOPPED

    @pytest.mark.asyncio
    async def test_get_state_unknown_defaults_to_error(self, runtime, container):
        container.show = AsyncMock(return_value={"State": {"Status": "something_weird"}})
        state = await runtime.get_state()
        assert state == RuntimeState.ERROR


# ---------------------------------------------------------------------------
# DockerRuntime — exec
# ---------------------------------------------------------------------------


class TestDockerRuntimeExec:
    @pytest.fixture
    def container(self):
        return _make_mock_container()

    @pytest.fixture
    def runtime(self, container):
        return DockerRuntime(
            container,
            runtime_id="docker-exec",
            working_dir="/home/workspace",
        )

    @pytest.mark.asyncio
    async def test_exec_returns_result(self, runtime, container):
        exec_mock = _make_exec_mock("hello world\n", exit_code=0)
        container.exec = AsyncMock(return_value=exec_mock)

        result = await runtime.exec("echo hello world")
        assert isinstance(result, ExecResult)
        assert result.stdout == "hello world\n"
        assert result.exit_code == 0
        assert result.stderr == ""

    @pytest.mark.asyncio
    async def test_exec_passes_workdir(self, runtime, container):
        exec_mock = _make_exec_mock("", exit_code=0)
        container.exec = AsyncMock(return_value=exec_mock)

        await runtime.exec("ls")
        container.exec.assert_called_once_with(
            cmd=["bash", "-c", "ls"],
            workdir="/home/workspace",
        )

    @pytest.mark.asyncio
    async def test_exec_nonzero_exit_code(self, runtime, container):
        exec_mock = _make_exec_mock("", exit_code=1)
        container.exec = AsyncMock(return_value=exec_mock)

        result = await runtime.exec("false")
        assert result.exit_code == 1

    @pytest.mark.asyncio
    async def test_exec_timeout_returns_error(self, runtime, container):
        import asyncio

        container.exec = AsyncMock(side_effect=asyncio.TimeoutError())
        result = await runtime.exec("sleep 999")
        assert result.exit_code == -1
        assert result.stderr == "timeout"

    @pytest.mark.asyncio
    async def test_exec_container_gone_raises_transient(self, runtime, container):
        container.exec = AsyncMock(
            side_effect=Exception("no such container: abc123")
        )
        with pytest.raises(SandboxTransientError):
            await runtime.exec("echo hi")

    @pytest.mark.asyncio
    async def test_exec_generic_error_returns_error_result(self, runtime, container):
        container.exec = AsyncMock(
            side_effect=Exception("something unexpected")
        )
        result = await runtime.exec("echo hi")
        assert result.exit_code == -1
        assert "something unexpected" in result.stderr


# ---------------------------------------------------------------------------
# DockerRuntime — tar upload
# ---------------------------------------------------------------------------


class TestDockerRuntimeTarUpload:
    @pytest.fixture
    def container(self):
        c = _make_mock_container()
        # exec is needed for mkdir -p in _tar_upload
        exec_mock = _make_exec_mock("", exit_code=0)
        c.exec = AsyncMock(return_value=exec_mock)
        return c

    @pytest.fixture
    def runtime(self, container):
        return DockerRuntime(
            container,
            runtime_id="docker-tar-up",
            working_dir="/home/workspace",
            dev_mode=False,
        )

    @pytest.mark.asyncio
    async def test_upload_calls_put_archive(self, runtime, container):
        await runtime.upload_file(b"file content", "/home/workspace/test.txt")
        container.put_archive.assert_called_once()

        # Verify put_archive is called with root "/" and full path in tar entry
        call_args = container.put_archive.call_args
        assert call_args[0][0] == "/"

        # Verify the second arg is valid tar data with full path as entry name
        tar_data = call_args[0][1]
        assert isinstance(tar_data, bytes)
        with tarfile.open(fileobj=io.BytesIO(tar_data), mode="r") as tar:
            members = tar.getmembers()
            assert len(members) == 1
            assert members[0].name == "home/workspace/test.txt"
            f = tar.extractfile(members[0])
            assert f.read() == b"file content"

    @pytest.mark.asyncio
    async def test_upload_creates_parent_dir(self, runtime, container):
        """_tar_upload should exec mkdir -p for the parent directory."""
        await runtime.upload_file(b"data", "/home/workspace/subdir/file.txt")
        # The exec call is for mkdir
        container.exec.assert_called()


# ---------------------------------------------------------------------------
# DockerRuntime — tar download
# ---------------------------------------------------------------------------


class TestDockerRuntimeDownload:
    @pytest.fixture
    def container(self):
        return _make_mock_container()

    @pytest.fixture
    def runtime(self, container):
        return DockerRuntime(
            container,
            runtime_id="docker-download",
            working_dir="/home/workspace",
            dev_mode=False,
        )

    @pytest.mark.asyncio
    async def test_download_via_exec_base64(self, runtime, container):
        """download_file uses exec + base64 to read files."""
        import base64 as b64

        content = b"downloaded content"
        encoded = b64.b64encode(content).decode() + "\n"

        exec_mock = _make_exec_mock(output=encoded, exit_code=0)
        container.exec = AsyncMock(return_value=exec_mock)

        result = await runtime.download_file("/home/workspace/file.txt")
        assert result == content

    @pytest.mark.asyncio
    async def test_download_file_not_found(self, runtime, container):
        """download_file raises FileNotFoundError for missing files."""
        exec_mock = _make_exec_mock(output="", exit_code=1)
        container.exec = AsyncMock(return_value=exec_mock)

        with pytest.raises(FileNotFoundError):
            await runtime.download_file("/home/workspace/missing.txt")


# ---------------------------------------------------------------------------
# DockerRuntime — bind-mount upload/download
# ---------------------------------------------------------------------------


class TestDockerRuntimeBindMount:
    @pytest.fixture
    def container(self):
        return _make_mock_container()

    @pytest.fixture
    def runtime(self, container, tmp_path):
        host_dir = str(tmp_path / "sandbox_work")
        return DockerRuntime(
            container,
            runtime_id="docker-bind",
            working_dir="/home/workspace",
            dev_mode=True,
            host_work_dir=host_dir,
        )

    @pytest.mark.asyncio
    async def test_upload_writes_to_host_fs(self, runtime, tmp_path):
        await runtime.upload_file(b"hello bind", "/home/workspace/test.txt")
        host_file = tmp_path / "sandbox_work" / "test.txt"
        assert host_file.exists()
        assert host_file.read_bytes() == b"hello bind"

    @pytest.mark.asyncio
    async def test_upload_creates_subdirs(self, runtime, tmp_path):
        await runtime.upload_file(b"nested", "/home/workspace/a/b/file.txt")
        host_file = tmp_path / "sandbox_work" / "a" / "b" / "file.txt"
        assert host_file.exists()
        assert host_file.read_bytes() == b"nested"

    @pytest.mark.asyncio
    async def test_download_reads_from_host_fs(self, runtime, tmp_path):
        host_dir = tmp_path / "sandbox_work"
        host_dir.mkdir(parents=True, exist_ok=True)
        (host_dir / "read_me.txt").write_bytes(b"read this")

        data = await runtime.download_file("/home/workspace/read_me.txt")
        assert data == b"read this"

    @pytest.mark.asyncio
    async def test_download_not_found_raises(self, runtime):
        with pytest.raises(FileNotFoundError):
            await runtime.download_file("/home/workspace/does_not_exist.txt")

    @pytest.mark.asyncio
    async def test_bind_mode_does_not_call_put_archive(self, runtime, container):
        """In bind mode, upload should NOT use the Docker tar API."""
        await runtime.upload_file(b"data", "/home/workspace/x.txt")
        container.put_archive.assert_not_called()


# ---------------------------------------------------------------------------
# DockerProvider
# ---------------------------------------------------------------------------


class TestDockerProvider:
    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        # containers.create returns a DockerContainer directly
        mock_container = _make_mock_container(container_id="container-id-abc")
        client.containers.create = AsyncMock(return_value=mock_container)
        # containers.get returns a DockerContainer for reconnecting
        client.containers.get = AsyncMock(return_value=mock_container)

        # images.inspect (for _ensure_image)
        client.images.inspect = AsyncMock()

        client.close = AsyncMock()
        return client

    @pytest.fixture
    def provider(self, mock_client):
        p = DockerProvider.__new__(DockerProvider)
        p._config = DockerConfig(image="test-sandbox:latest")
        p._working_dir = "/home/workspace"
        p._client = mock_client
        return p

    @pytest.mark.asyncio
    async def test_create_returns_docker_runtime(self, provider, mock_client):
        runtime = await provider.create(env_vars={"FOO": "bar"})
        assert isinstance(runtime, DockerRuntime)
        mock_client.containers.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_starts_container(self, provider, mock_client):
        """create() should call container.start() after creation."""
        runtime = await provider.create()
        mock_container = mock_client.containers.create.return_value
        mock_container.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_config_includes_image(self, provider, mock_client):
        await provider.create()
        call_kwargs = mock_client.containers.create.call_args
        config = call_kwargs.kwargs.get("config") or call_kwargs[1].get("config")
        assert config["Image"] == "test-sandbox:latest"

    @pytest.mark.asyncio
    async def test_create_config_includes_env_vars(self, provider, mock_client):
        await provider.create(env_vars={"MY_VAR": "value1", "OTHER": "value2"})
        call_kwargs = mock_client.containers.create.call_args
        config = call_kwargs.kwargs.get("config") or call_kwargs[1].get("config")
        env = config.get("Env", [])
        assert "MY_VAR=value1" in env
        assert "OTHER=value2" in env

    @pytest.mark.asyncio
    async def test_create_no_env_vars_omits_env(self, provider, mock_client):
        await provider.create()
        call_kwargs = mock_client.containers.create.call_args
        config = call_kwargs.kwargs.get("config") or call_kwargs[1].get("config")
        assert "Env" not in config

    @pytest.mark.asyncio
    async def test_create_with_bind_mount(self, mock_client, tmp_path):
        """When dev_mode=True and host_work_dir is set, Binds should appear in host config."""
        host_dir = str(tmp_path / "host_sandbox")
        p = DockerProvider.__new__(DockerProvider)
        p._config = DockerConfig(
            image="test-sandbox:latest",
            dev_mode=True,
            host_work_dir=host_dir,
        )
        p._working_dir = "/home/workspace"
        p._client = mock_client

        await p.create()
        call_kwargs = mock_client.containers.create.call_args
        config = call_kwargs.kwargs.get("config") or call_kwargs[1].get("config")
        host_config = config["HostConfig"]
        assert "Binds" in host_config
        assert any("/home/workspace" in b for b in host_config["Binds"])

    @pytest.mark.asyncio
    async def test_create_without_bind_mount_no_binds(self, provider, mock_client):
        """Default (non-dev) mode should not include Binds."""
        await provider.create()
        call_kwargs = mock_client.containers.create.call_args
        config = call_kwargs.kwargs.get("config") or call_kwargs[1].get("config")
        host_config = config["HostConfig"]
        assert "Binds" not in host_config

    @pytest.mark.asyncio
    async def test_create_installs_mcp_packages(self, provider, mock_client):
        """create() should run npm install -g for each MCP package."""
        mock_container = mock_client.containers.create.return_value
        exec_mock = _make_exec_mock("added 1 package\n", exit_code=0)
        mock_container.exec = AsyncMock(return_value=exec_mock)

        runtime = await provider.create(mcp_packages=["@tavily/mcp-server"])
        assert isinstance(runtime, DockerRuntime)

        # Verify exec was called with npm install -g containing the package
        mock_container.exec.assert_called_once()
        call_args = mock_container.exec.call_args
        cmd = call_args.kwargs.get("cmd") or call_args[1].get("cmd") or call_args[0]
        # cmd is ["bash", "-c", "npm install -g @tavily/mcp-server"]
        assert cmd[0] == "bash"
        assert "npm install -g" in cmd[2]
        assert "@tavily/mcp-server" in cmd[2]

    @pytest.mark.asyncio
    async def test_create_empty_mcp_packages_skips_install(self, provider, mock_client):
        """create() should not call exec when mcp_packages is empty or None."""
        mock_container = mock_client.containers.create.return_value

        # Test with empty list
        await provider.create(mcp_packages=[])
        mock_container.exec.assert_not_called()

        mock_container.exec.reset_mock()

        # Test with None (default)
        await provider.create(mcp_packages=None)
        mock_container.exec.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_returns_runtime(self, provider, mock_client):
        mock_container = mock_client.containers.get.return_value
        mock_container.show = AsyncMock(return_value={
            "State": {"Status": "running"},
            "Config": {"WorkingDir": "/home/workspace", "Env": []},
            "Mounts": [],
        })
        runtime = await provider.get("docker-abc123")
        assert isinstance(runtime, DockerRuntime)
        assert runtime.id == "docker-abc123"

    @pytest.mark.asyncio
    async def test_get_detects_bind_mount(self, provider, mock_client):
        """get() should detect dev_mode from existing bind mounts."""
        mock_container = mock_client.containers.get.return_value
        mock_container.show = AsyncMock(return_value={
            "State": {"Status": "running"},
            "Config": {"WorkingDir": "/home/workspace", "Env": []},
            "Mounts": [
                {
                    "Destination": "/home/workspace",
                    "Source": "/host/path/work",
                    "Type": "bind",
                }
            ],
        })
        runtime = await provider.get("docker-xyz")
        assert runtime._dev_mode is True
        assert runtime._host_work_dir == "/host/path/work"

    @pytest.mark.asyncio
    async def test_get_container_not_found_raises(self, provider, mock_client):
        mock_client.containers.get = AsyncMock(side_effect=Exception("404 not found"))
        with pytest.raises(RuntimeError, match="Docker container not found"):
            await provider.get("nonexistent")

    @pytest.mark.asyncio
    async def test_close_closes_client(self, provider, mock_client):
        await provider.close()
        mock_client.close.assert_called_once()
        assert provider._client is None

    @pytest.mark.asyncio
    async def test_close_idempotent(self, provider, mock_client):
        """Closing twice should not error."""
        await provider.close()
        await provider.close()  # should be no-op since _client is None

    @pytest.mark.asyncio
    async def test_close_handles_client_error(self, mock_client):
        """close() should not raise even if the client fails."""
        mock_client.close = AsyncMock(side_effect=Exception("connection lost"))
        p = DockerProvider.__new__(DockerProvider)
        p._config = DockerConfig()
        p._client = mock_client
        await p.close()  # should not raise
        assert p._client is None


class TestDockerProviderTransientErrors:
    @pytest.fixture
    def provider(self):
        p = DockerProvider.__new__(DockerProvider)
        p._config = DockerConfig()
        p._client = None
        return p

    def test_connection_refused(self, provider):
        assert provider.is_transient_error(Exception("connection refused"))

    def test_connection_reset(self, provider):
        assert provider.is_transient_error(Exception("connection reset"))

    def test_connection_aborted(self, provider):
        assert provider.is_transient_error(Exception("connection aborted"))

    def test_broken_pipe(self, provider):
        assert provider.is_transient_error(Exception("broken pipe"))

    def test_timed_out(self, provider):
        assert provider.is_transient_error(Exception("timed out"))

    def test_timeout(self, provider):
        assert provider.is_transient_error(Exception("timeout"))

    def test_non_transient_value_error(self, provider):
        assert not provider.is_transient_error(ValueError("bad argument"))

    def test_non_transient_generic(self, provider):
        assert not provider.is_transient_error(Exception("file not found"))

    def test_case_insensitive(self, provider):
        assert provider.is_transient_error(Exception("Connection Refused"))


# ---------------------------------------------------------------------------
# DockerProvider — lazy client creation
# ---------------------------------------------------------------------------


class TestDockerProviderLazyClient:
    @pytest.mark.asyncio
    async def test_get_client_creates_lazily(self):
        """_get_client() should import and create aiodocker.Docker on first call."""
        p = DockerProvider(DockerConfig())
        assert p._client is None

        mock_docker_cls = MagicMock()
        mock_docker_instance = MagicMock()
        mock_docker_cls.return_value = mock_docker_instance

        # aiodocker is imported lazily inside _get_client via `import aiodocker`,
        # so we patch it in sys.modules before the call.
        mock_mod = MagicMock()
        mock_mod.Docker = mock_docker_cls
        with patch.dict("sys.modules", {"aiodocker": mock_mod}):
            client = await p._get_client()

        assert client is mock_docker_instance
        assert p._client is mock_docker_instance


# ---------------------------------------------------------------------------
# _parse_proxy_port_range helper
# ---------------------------------------------------------------------------


class TestParseProxyPortRange:
    def test_valid_range(self):
        assert _parse_proxy_port_range("13000-13009") == list(range(13000, 13010))

    def test_single_port(self):
        assert _parse_proxy_port_range("8080-8080") == [8080]

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid proxy port range"):
            _parse_proxy_port_range("8080")

    def test_start_greater_than_end(self):
        with pytest.raises(ValueError, match="start.*>.*end"):
            _parse_proxy_port_range("9000-8000")


# ---------------------------------------------------------------------------
# DockerRuntime — preview URLs (proxy port pool)
# ---------------------------------------------------------------------------


class TestDockerRuntimePreviewUrl:
    @pytest.fixture
    def container(self):
        return _make_mock_container()

    @pytest.fixture
    def runtime(self, container):
        return DockerRuntime(
            container,
            runtime_id="docker-test123",
            working_dir="/home/workspace",
            proxy_ports=[13000, 13001, 13002],
        )

    @pytest.mark.asyncio
    async def test_allocates_proxy_port(self, runtime):
        """get_preview_url allocates a proxy port and starts socat."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        info = await runtime.get_preview_url(8080)
        assert info.url == "http://localhost:13000"
        assert info.token == ""
        assert runtime._port_map[8080] == 13000

    @pytest.mark.asyncio
    async def test_reuses_existing_mapping(self, runtime):
        """Same target port returns same proxy port without re-allocating."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        info1 = await runtime.get_preview_url(8080)
        call_count_after_first = runtime._container.exec.call_count
        info2 = await runtime.get_preview_url(8080)
        assert info1.url == info2.url
        # No additional exec calls for the second request (cached)
        assert runtime._container.exec.call_count == call_count_after_first

    @pytest.mark.asyncio
    async def test_multiple_ports_different_proxies(self, runtime):
        """Different target ports get different proxy ports."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        info1 = await runtime.get_preview_url(8080)
        info2 = await runtime.get_preview_url(3000)
        assert info1.url == "http://localhost:13000"
        assert info2.url == "http://localhost:13001"

    @pytest.mark.asyncio
    async def test_exhausted_pool_raises(self, runtime):
        """RuntimeError when all proxy ports are allocated."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        await runtime.get_preview_url(8080)
        await runtime.get_preview_url(8081)
        await runtime.get_preview_url(8082)
        with pytest.raises(RuntimeError, match="No free proxy ports"):
            await runtime.get_preview_url(8083)

    @pytest.mark.asyncio
    async def test_explicit_base_url(self, container):
        """preview_base_url overrides localhost."""
        runtime = DockerRuntime(
            container,
            runtime_id="docker-test",
            working_dir="/home/workspace",
            proxy_ports=[13000],
            preview_base_url="http://192.168.1.100",
        )
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        info = await runtime.get_preview_url(8080)
        assert info.url == "http://192.168.1.100:13000"

    @pytest.mark.asyncio
    async def test_get_preview_link_reuses_proxy_port(self, runtime):
        """get_preview_link allocates the same proxy port as get_preview_url."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        url_info = await runtime.get_preview_url(8080)
        link_info = await runtime.get_preview_link(8080)
        # Same proxy port even if host may differ (server-side vs browser)
        assert url_info.url.split(":")[-1] == link_info.url.split(":")[-1]

    @pytest.mark.asyncio
    async def test_preview_link_uses_docker_internal_when_available(self, runtime):
        """get_preview_link uses host.docker.internal when DNS resolves."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        DockerRuntime._server_side_host = None  # clear class cache
        mock_loop = AsyncMock()
        mock_loop.getaddrinfo = AsyncMock(return_value=[("", "", 0, "", ("127.0.0.1", 0))])
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            info = await runtime.get_preview_link(8080)
        assert info.url == "http://host.docker.internal:13000"
        DockerRuntime._server_side_host = None  # cleanup

    @pytest.mark.asyncio
    async def test_preview_link_falls_back_to_localhost(self, runtime):
        """get_preview_link falls back to localhost when not in Docker."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        DockerRuntime._server_side_host = None  # clear class cache
        mock_loop = AsyncMock()
        mock_loop.getaddrinfo = AsyncMock(side_effect=OSError)
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            info = await runtime.get_preview_link(8080)
        assert info.url == "http://localhost:13000"
        DockerRuntime._server_side_host = None  # cleanup

    @pytest.mark.asyncio
    async def test_resolve_server_side_host_is_cached(self, runtime):
        """_resolve_server_side_host only does DNS once, then caches."""
        DockerRuntime._server_side_host = None
        mock_loop = AsyncMock()
        mock_loop.getaddrinfo = AsyncMock(side_effect=OSError)
        with patch("asyncio.get_running_loop", return_value=mock_loop) as mock_get_loop:
            await DockerRuntime._resolve_server_side_host()
            await DockerRuntime._resolve_server_side_host()
            assert mock_loop.getaddrinfo.call_count == 1
        DockerRuntime._server_side_host = None  # cleanup

    @pytest.mark.asyncio
    async def test_socat_exec_command(self, runtime):
        """Verify the exec calls include fuser kill, socat start, and fuser check."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        await runtime.get_preview_url(8080)
        # Collect all exec commands (write proxy script, fuser -k, socat nohup, fuser check)
        all_cmds = []
        for call in runtime._container.exec.call_args_list:
            cmd = call[1].get("cmd", call[0][0] if call[0] else None)
            cmd_str = cmd[-1] if isinstance(cmd, list) else str(cmd)
            all_cmds.append(cmd_str)
        # Should have: base64 write, fuser -k, socat nohup, fuser verify
        assert any("fuser -k" in c and "13000" in c for c in all_cmds)
        assert any("socat" in c and "13000" in c and "8080" in c for c in all_cmds)
        assert any("fuser" in c and "13000" in c and "-k" not in c for c in all_cmds)

    @pytest.mark.asyncio
    async def test_kills_stale_forwarder_before_allocating(self, runtime):
        """_allocate_proxy_port kills existing listeners before starting socat."""
        exec_calls = []
        async def capture_exec(*args, **kwargs):
            cmd = kwargs.get("cmd", args[0] if args else None)
            cmd_str = cmd[-1] if isinstance(cmd, list) else str(cmd)
            exec_calls.append(cmd_str)
            return _make_exec_mock(output="12345\n")
        runtime._container.exec = AsyncMock(side_effect=capture_exec)
        await runtime.get_preview_url(8080)
        # fuser -k must come before the socat nohup
        kill_idx = next(i for i, c in enumerate(exec_calls) if "fuser -k" in c)
        socat_idx = next(i for i, c in enumerate(exec_calls) if "socat" in c)
        assert kill_idx < socat_idx

    @pytest.mark.asyncio
    async def test_concurrent_allocations_get_different_ports(self, runtime):
        """Two concurrent _allocate_proxy_port calls get different proxy ports."""
        import asyncio

        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        p1, p2 = await asyncio.gather(
            runtime._allocate_proxy_port(8080),
            runtime._allocate_proxy_port(9090),
        )
        assert p1 != p2
        assert {p1, p2} == {13000, 13001}

    @pytest.mark.asyncio
    async def test_allocate_proxy_port_rolls_back_on_failure(self, runtime):
        """Port reservation is rolled back if exec raises a transient error."""
        # "no such container" triggers SandboxTransientError (re-raised by exec)
        runtime._container.exec = AsyncMock(
            side_effect=RuntimeError("no such container")
        )
        with pytest.raises(SandboxTransientError):
            await runtime._allocate_proxy_port(8080)
        # Port should be released for retry
        assert 8080 not in runtime._port_map
        assert 13000 not in runtime._forwarder_pids


# ---------------------------------------------------------------------------
# DockerRuntime — sessions
# ---------------------------------------------------------------------------


class TestDockerRuntimeSessions:
    @pytest.fixture
    def container(self):
        return _make_mock_container()

    @pytest.fixture
    def runtime(self, container):
        return DockerRuntime(
            container,
            runtime_id="docker-test123",
            working_dir="/home/workspace",
        )

    @pytest.mark.asyncio
    async def test_create_session(self, runtime):
        """create_session creates the session directory."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(exit_code=1)  # dir doesn't exist
        )
        await runtime.create_session("preview-8080")
        assert runtime._container.exec.call_count == 2  # test -d + mkdir -p

    @pytest.mark.asyncio
    async def test_create_session_already_exists(self, runtime):
        """create_session raises when session dir already exists."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(exit_code=0)  # dir exists
        )
        with pytest.raises(RuntimeError, match="Session already exists"):
            await runtime.create_session("preview-8080")

    @pytest.mark.asyncio
    async def test_create_session_invalid_id(self, runtime):
        """create_session rejects invalid session IDs."""
        with pytest.raises(ValueError, match="Invalid session_id"):
            await runtime.create_session("../evil")

    @pytest.mark.asyncio
    async def test_session_execute_async(self, runtime):
        """session_execute with run_async=True returns None exit_code."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="99999\n")
        )
        result = await runtime.session_execute("preview-8080", "python -m http.server 8080", run_async=True)
        assert result.exit_code is None
        assert result.cmd_id  # non-empty
        # Verify base64 encoding is in the exec command
        call_args = runtime._container.exec.call_args
        cmd = call_args[1].get("cmd", call_args[0][0] if call_args[0] else None)
        cmd_str = cmd[-1] if isinstance(cmd, list) else str(cmd)
        assert "base64" in cmd_str
        assert "nohup setsid" in cmd_str

    @pytest.mark.asyncio
    async def test_session_execute_sync(self, runtime):
        """session_execute with run_async=False returns actual exit code."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="hello\n", exit_code=0)
        )
        result = await runtime.session_execute("preview-8080", "echo hello", run_async=False)
        assert result.exit_code == 0
        assert result.stdout == "hello\n"

    @pytest.mark.asyncio
    async def test_session_command_logs_finished(self, runtime):
        """session_command_logs returns exit code when process finished."""
        # Single exec returns stdout, stderr, exit code separated by null bytes
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(
                output="output data\n\x00error data\n\x000\n", exit_code=0
            ),
        )
        result = await runtime.session_command_logs("preview-8080", "abc12345")
        assert result.exit_code == 0
        assert result.stdout == "output data\n"
        assert result.stderr == "error data\n"
        assert result.cmd_id == "abc12345"

    @pytest.mark.asyncio
    async def test_session_command_logs_running(self, runtime):
        """session_command_logs returns None exit_code when process still running."""
        # Exit file doesn't exist yet, so the exit segment is empty
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="\x00\x00", exit_code=0),
        )
        result = await runtime.session_command_logs("preview-8080", "abc12345")
        assert result.exit_code is None

    @pytest.mark.asyncio
    async def test_session_command_logs_invalid_id(self, runtime):
        """session_command_logs rejects invalid command IDs."""
        with pytest.raises(ValueError, match="Invalid command_id"):
            await runtime.session_command_logs("preview-8080", "../etc/passwd")

    @pytest.mark.asyncio
    async def test_delete_session(self, runtime):
        """delete_session kills process groups and removes directory."""
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(exit_code=0)
        )
        await runtime.delete_session("preview-8080")
        call_args = runtime._container.exec.call_args
        cmd = call_args[1].get("cmd", call_args[0][0] if call_args[0] else None)
        cmd_str = cmd[-1] if isinstance(cmd, list) else str(cmd)
        # Kills entire process group (negative PID) and the process itself
        assert 'kill -- -"$pid"' in cmd_str
        assert 'kill "$pid"' in cmd_str
        assert "rm -rf" in cmd_str

    @pytest.mark.asyncio
    async def test_delete_session_invalid_id(self, runtime):
        """delete_session rejects invalid session IDs."""
        with pytest.raises(ValueError, match="Invalid session_id"):
            await runtime.delete_session("../../etc")

    @pytest.mark.asyncio
    async def test_session_execute_invalid_id(self, runtime):
        """session_execute rejects invalid session IDs."""
        with pytest.raises(ValueError, match="Invalid session_id"):
            await runtime.session_execute("../evil", "echo hi")

    @pytest.mark.asyncio
    async def test_session_command_logs_invalid_session_id(self, runtime):
        """session_command_logs rejects invalid session IDs."""
        with pytest.raises(ValueError, match="Invalid session_id"):
            await runtime.session_command_logs("../evil", "abc12345")


# ---------------------------------------------------------------------------
# DockerRuntime — updated capabilities
# ---------------------------------------------------------------------------


class TestDockerRuntimeUpdatedCapabilities:
    def test_capabilities_include_preview_and_sessions(self):
        container = _make_mock_container()
        runtime = DockerRuntime(
            container,
            runtime_id="docker-test",
            working_dir="/home/workspace",
        )
        caps = runtime.capabilities
        assert "preview_url" in caps
        assert "sessions" in caps
        assert "exec" in caps
        assert "code_run" in caps
        assert "file_io" in caps


# ---------------------------------------------------------------------------
# DockerProvider — container creation with proxy ports and Init
# ---------------------------------------------------------------------------


class TestDockerProviderPreviewConfig:
    @pytest.mark.asyncio
    async def test_create_publishes_proxy_ports_with_dynamic_host_ports(self):
        """Container creation uses dynamic host ports (HostPort: '') for proxy ports."""
        config = DockerConfig(preview_proxy_ports="13000-13002")
        provider = DockerProvider(config, working_dir="/home/workspace")

        mock_container = _make_mock_container()
        mock_client = MagicMock()
        mock_client.containers = MagicMock()
        mock_client.containers.create = AsyncMock(return_value=mock_container)
        mock_client.images = MagicMock()
        mock_client.images.inspect = AsyncMock()
        provider._client = mock_client

        await provider.create()

        create_call = mock_client.containers.create.call_args
        container_config = create_call[1]["config"]
        host_config = container_config["HostConfig"]

        assert "PortBindings" in host_config
        assert "13000/tcp" in host_config["PortBindings"]
        # Verify dynamic host ports (empty string = Docker picks)
        assert host_config["PortBindings"]["13000/tcp"] == [{"HostPort": ""}]
        assert host_config["PortBindings"]["13001/tcp"] == [{"HostPort": ""}]
        assert host_config["PortBindings"]["13002/tcp"] == [{"HostPort": ""}]

        assert "ExposedPorts" in container_config
        assert "13000/tcp" in container_config["ExposedPorts"]

    @pytest.mark.asyncio
    async def test_create_includes_init(self):
        """Container creation includes Init: True for zombie reaping."""
        config = DockerConfig()
        provider = DockerProvider(config, working_dir="/home/workspace")

        mock_container = _make_mock_container()
        mock_client = MagicMock()
        mock_client.containers = MagicMock()
        mock_client.containers.create = AsyncMock(return_value=mock_container)
        mock_client.images = MagicMock()
        mock_client.images.inspect = AsyncMock()
        provider._client = mock_client

        await provider.create()

        create_call = mock_client.containers.create.call_args
        host_config = create_call[1]["config"]["HostConfig"]
        assert host_config.get("Init") is True

    @pytest.mark.asyncio
    async def test_create_passes_proxy_ports_to_runtime(self):
        """Runtime receives proxy_ports from provider config."""
        config = DockerConfig(preview_proxy_ports="13000-13002")
        provider = DockerProvider(config, working_dir="/home/workspace")

        mock_container = _make_mock_container()
        mock_client = MagicMock()
        mock_client.containers = MagicMock()
        mock_client.containers.create = AsyncMock(return_value=mock_container)
        mock_client.images = MagicMock()
        mock_client.images.inspect = AsyncMock()
        provider._client = mock_client

        runtime = await provider.create()
        assert runtime._proxy_ports == [13000, 13001, 13002]

    @pytest.mark.asyncio
    async def test_create_passes_preview_base_url(self):
        """Runtime receives preview_base_url from provider config."""
        config = DockerConfig(preview_base_url="http://10.0.0.1")
        provider = DockerProvider(config, working_dir="/home/workspace")

        mock_container = _make_mock_container()
        mock_client = MagicMock()
        mock_client.containers = MagicMock()
        mock_client.containers.create = AsyncMock(return_value=mock_container)
        mock_client.images = MagicMock()
        mock_client.images.inspect = AsyncMock()
        provider._client = mock_client

        runtime = await provider.create()
        assert runtime._preview_base_url == "http://10.0.0.1"

    @pytest.mark.asyncio
    async def test_get_passes_proxy_ports(self):
        """get() passes proxy_ports from provider config to runtime."""
        config = DockerConfig(preview_proxy_ports="13000-13004")
        provider = DockerProvider(config, working_dir="/home/workspace")

        mock_container = _make_mock_container()
        mock_client = MagicMock()
        mock_client.containers = MagicMock()
        mock_client.containers.get = AsyncMock(return_value=mock_container)
        provider._client = mock_client

        runtime = await provider.get("docker-abc123")
        assert runtime._proxy_ports == [13000, 13001, 13002, 13003, 13004]
        assert runtime._preview_base_url is None

    @pytest.mark.asyncio
    async def test_create_reads_dynamic_host_ports(self):
        """create() reads actual host port mappings from container inspect."""
        config = DockerConfig(preview_proxy_ports="13000-13001")
        provider = DockerProvider(config, working_dir="/home/workspace")

        mock_container = _make_mock_container()
        # After start, show() returns port mappings with dynamic host ports
        mock_container.show = AsyncMock(return_value={
            "State": {"Status": "running"},
            "NetworkSettings": {
                "Ports": {
                    "13000/tcp": [{"HostIp": "0.0.0.0", "HostPort": "49152"}],
                    "13001/tcp": [{"HostIp": "0.0.0.0", "HostPort": "49153"}],
                }
            },
        })
        mock_client = MagicMock()
        mock_client.containers = MagicMock()
        mock_client.containers.create = AsyncMock(return_value=mock_container)
        mock_client.images = MagicMock()
        mock_client.images.inspect = AsyncMock()
        provider._client = mock_client

        runtime = await provider.create()
        assert runtime._host_port_map == {13000: 49152, 13001: 49153}

    @pytest.mark.asyncio
    async def test_preview_url_uses_dynamic_host_port(self):
        """Preview URL uses the host port, not the container port."""
        container = _make_mock_container()
        runtime = DockerRuntime(
            container,
            runtime_id="docker-test",
            working_dir="/home/workspace",
            proxy_ports=[13000, 13001],
            host_port_map={13000: 49152, 13001: 49153},
        )
        runtime._container.exec = AsyncMock(
            return_value=_make_exec_mock(output="12345\n")
        )
        info = await runtime.get_preview_url(8080)
        assert info.url == "http://localhost:49152"

    @pytest.mark.asyncio
    async def test_get_reads_dynamic_host_ports(self):
        """get() reads host port mappings from running container inspect."""
        config = DockerConfig(preview_proxy_ports="13000-13001")
        provider = DockerProvider(config, working_dir="/home/workspace")

        mock_container = _make_mock_container()
        mock_container.show = AsyncMock(return_value={
            "State": {"Status": "running"},
            "Mounts": [],
            "NetworkSettings": {
                "Ports": {
                    "13000/tcp": [{"HostIp": "0.0.0.0", "HostPort": "49200"}],
                    "13001/tcp": [{"HostIp": "0.0.0.0", "HostPort": "49201"}],
                }
            },
        })
        mock_client = MagicMock()
        mock_client.containers = MagicMock()
        mock_client.containers.get = AsyncMock(return_value=mock_container)
        provider._client = mock_client

        runtime = await provider.get("docker-abc123")
        assert runtime._host_port_map == {13000: 49200, 13001: 49201}
