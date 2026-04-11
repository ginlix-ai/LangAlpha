"""Docker-specific integration tests for tar vs bind-mount modes.

Requires Docker daemon running + langalpha-sandbox:latest image.
Run: SANDBOX_TEST_PROVIDER=docker uv run pytest tests/integration/sandbox/test_docker_modes.py -v
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio

from ptc_agent.config.core import DockerConfig
from ptc_agent.core.sandbox.providers.docker import DockerProvider, DockerRuntime
from ptc_agent.core.sandbox.runtime import RuntimeState

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        "docker" not in os.getenv(
            "SANDBOX_TEST_PROVIDERS",
            os.getenv("SANDBOX_TEST_PROVIDER", "memory"),
        ).lower().split(","),
        reason="Docker tests require docker in SANDBOX_TEST_PROVIDERS",
    ),
]


# ---------------------------------------------------------------------------
# Tar mode (default, no bind mounts)
# ---------------------------------------------------------------------------


class TestTarMode:
    """Test Docker sandbox in default tar-based file I/O mode."""

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def runtime(self):
        provider = DockerProvider(DockerConfig(
            image=os.environ.get("DOCKER_SANDBOX_IMAGE", "langalpha-sandbox:latest"),
            dev_mode=False,
        ))
        rt = await provider.create(env_vars={"TEST_VAR": "tar_test"})
        yield rt
        try:
            await rt.delete()
        except Exception:
            pass
        await provider.close()

    async def test_exec_echo(self, runtime: DockerRuntime):
        """Basic shell exec should work."""
        result = await runtime.exec("echo hello")
        assert result.exit_code == 0
        assert "hello" in result.stdout

    async def test_exec_env_var(self, runtime: DockerRuntime):
        """Environment variables set at creation should be accessible."""
        result = await runtime.exec("echo $TEST_VAR")
        assert result.exit_code == 0
        assert "tar_test" in result.stdout

    async def test_upload_download_roundtrip(self, runtime: DockerRuntime):
        """Upload a file via tar, then download it and verify content."""
        content = b"tar mode roundtrip content"
        dest = f"{runtime.working_dir}/roundtrip.txt"
        await runtime.upload_file(content, dest)
        downloaded = await runtime.download_file(dest)
        assert downloaded == content

    async def test_upload_nested_path(self, runtime: DockerRuntime):
        """Upload to a nested directory that does not exist yet."""
        content = b"nested content"
        dest = f"{runtime.working_dir}/a/b/c/nested.txt"
        await runtime.upload_file(content, dest)
        downloaded = await runtime.download_file(dest)
        assert downloaded == content

    async def test_code_run_basic(self, runtime: DockerRuntime):
        """code_run should execute Python and return stdout."""
        result = await runtime.code_run("print('hello from docker')")
        assert result.exit_code == 0
        assert "hello from docker" in result.stdout

    async def test_list_files(self, runtime: DockerRuntime):
        """list_files should show uploaded files."""
        await runtime.upload_file(b"x", f"{runtime.working_dir}/listed.txt")
        entries = await runtime.list_files(runtime.working_dir)
        names = [e["name"] if isinstance(e, dict) else e.name for e in entries]
        assert "listed.txt" in names

    async def test_get_state_running(self, runtime: DockerRuntime):
        """A freshly created container should be running."""
        state = await runtime.get_state()
        assert state == RuntimeState.RUNNING

    async def test_capabilities(self, runtime: DockerRuntime):
        """Docker runtime should have exec, code_run, file_io but not archive/snapshot."""
        caps = runtime.capabilities
        assert "exec" in caps
        assert "code_run" in caps
        assert "file_io" in caps
        assert "archive" not in caps
        assert "snapshot" not in caps


# ---------------------------------------------------------------------------
# Bind-mount mode (dev_mode=True)
# ---------------------------------------------------------------------------


class TestBindMountMode:
    """Test Docker sandbox with bind-mounted host directory."""

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def runtime_and_dir(self, tmp_path_factory):
        host_dir = str(tmp_path_factory.mktemp("bind_sandbox"))
        provider = DockerProvider(DockerConfig(
            image=os.environ.get("DOCKER_SANDBOX_IMAGE", "langalpha-sandbox:latest"),
            dev_mode=True,
            host_work_dir=host_dir,
        ))
        rt = await provider.create(env_vars={"TEST_VAR": "bind_test"})
        yield rt, host_dir
        try:
            await rt.delete()
        except Exception:
            pass
        await provider.close()

    async def test_upload_writes_to_host(self, runtime_and_dir):
        """In bind mode, upload_file should write directly to the host filesystem."""
        runtime, host_dir = runtime_and_dir
        content = b"bind mode content"
        await runtime.upload_file(content, f"{runtime.working_dir}/bind_test.txt")
        host_file = os.path.join(host_dir, "bind_test.txt")
        assert os.path.exists(host_file)
        with open(host_file, "rb") as f:
            assert f.read() == content

    async def test_download_reads_from_host(self, runtime_and_dir):
        """In bind mode, download_file should read from the host filesystem."""
        runtime, host_dir = runtime_and_dir
        os.makedirs(host_dir, exist_ok=True)
        host_file = os.path.join(host_dir, "host_written.txt")
        with open(host_file, "wb") as f:
            f.write(b"host written")
        data = await runtime.download_file(f"{runtime.working_dir}/host_written.txt")
        assert data == b"host written"

    async def test_exec_sees_bind_files(self, runtime_and_dir):
        """Files on the host should be visible inside the container via bind mount."""
        runtime, host_dir = runtime_and_dir
        os.makedirs(host_dir, exist_ok=True)
        with open(os.path.join(host_dir, "visible.txt"), "w") as f:
            f.write("I am visible")
        result = await runtime.exec(f"cat {runtime.working_dir}/visible.txt")
        assert result.exit_code == 0
        assert "I am visible" in result.stdout

    async def test_code_run_in_bind_mode(self, runtime_and_dir):
        """code_run should work in bind-mount mode."""
        runtime, _ = runtime_and_dir
        result = await runtime.code_run("print(2 + 2)")
        assert result.exit_code == 0
        assert "4" in result.stdout


# ---------------------------------------------------------------------------
# Chart capture integration
# ---------------------------------------------------------------------------


class TestChartCapture:
    """Test matplotlib chart capture in Docker containers.

    Requires the sandbox image to have matplotlib installed.
    """

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def runtime(self):
        provider = DockerProvider(DockerConfig(
            image=os.environ.get("DOCKER_SANDBOX_IMAGE", "langalpha-sandbox:latest"),
            dev_mode=False,
        ))
        rt = await provider.create()
        yield rt
        try:
            await rt.delete()
        except Exception:
            pass
        await provider.close()

    async def test_chart_capture_produces_artifact(self, runtime: DockerRuntime):
        """code_run with plt.show() should produce a chart artifact."""
        code = """\
import matplotlib.pyplot as plt
plt.figure()
plt.plot([1, 2, 3], [1, 4, 9])
plt.title("Test Chart")
plt.show()
print("done")
"""
        result = await runtime.code_run(code)
        assert result.exit_code == 0
        assert "done" in result.stdout
        assert len(result.artifacts) == 1
        assert result.artifacts[0].type == "image/png"
        assert result.artifacts[0].name == "chart_1.png"
        # Artifact data should be non-empty base64
        assert len(result.artifacts[0].data) > 100

    async def test_multiple_charts(self, runtime: DockerRuntime):
        """Multiple plt.show() calls should produce multiple artifacts."""
        code = """\
import matplotlib.pyplot as plt

plt.figure()
plt.plot([1, 2], [1, 2])
plt.show()

plt.figure()
plt.bar([1, 2], [3, 4])
plt.show()

print("two charts")
"""
        result = await runtime.code_run(code)
        assert result.exit_code == 0
        assert "two charts" in result.stdout
        assert len(result.artifacts) == 2
        assert result.artifacts[0].name == "chart_1.png"
        assert result.artifacts[1].name == "chart_2.png"

    async def test_no_matplotlib_no_crash(self, runtime: DockerRuntime):
        """Code that does not use matplotlib should work fine."""
        result = await runtime.code_run("print('no charts here')")
        assert result.exit_code == 0
        assert "no charts here" in result.stdout
        assert result.artifacts == []


# ---------------------------------------------------------------------------
# Preview URL support
# ---------------------------------------------------------------------------


class TestPreviewUrl:
    """Test preview URL proxy port allocation and socat forwarding."""

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def runtime(self):
        provider = DockerProvider(DockerConfig(
            image=os.environ.get("DOCKER_SANDBOX_IMAGE", "langalpha-sandbox:latest"),
            dev_mode=False,
        ))
        rt = await provider.create()
        yield rt
        try:
            await rt.delete()
        except Exception:
            pass
        await provider.close()

    async def test_get_preview_url_returns_url(self, runtime: DockerRuntime):
        """get_preview_url should return a valid URL with a host port."""
        info = await runtime.get_preview_url(8080)
        assert info.url.startswith("http")
        # URL should contain a port number
        port_str = info.url.rsplit(":", 1)[-1]
        assert port_str.isdigit()

    async def test_get_preview_url_reuses_port(self, runtime: DockerRuntime):
        """Calling get_preview_url twice for the same port returns the same URL."""
        info1 = await runtime.get_preview_url(8080)
        info2 = await runtime.get_preview_url(8080)
        assert info1.url == info2.url

    async def test_different_ports_get_different_urls(self, runtime: DockerRuntime):
        """Different target ports should get different proxy ports."""
        info1 = await runtime.get_preview_url(8080)
        info2 = await runtime.get_preview_url(9090)
        assert info1.url != info2.url

    async def test_get_preview_link_returns_url(self, runtime: DockerRuntime):
        """get_preview_link should return a valid URL."""
        info = await runtime.get_preview_link(8080)
        assert info.url.startswith("http")

    async def test_capabilities_include_preview(self, runtime: DockerRuntime):
        """Docker runtime should report preview_url capability."""
        assert "preview_url" in runtime.capabilities


# ---------------------------------------------------------------------------
# Session support
# ---------------------------------------------------------------------------


class TestSessions:
    """Test session lifecycle (create, execute, logs, delete)."""

    @pytest_asyncio.fixture(scope="class", loop_scope="class")
    async def runtime(self):
        provider = DockerProvider(DockerConfig(
            image=os.environ.get("DOCKER_SANDBOX_IMAGE", "langalpha-sandbox:latest"),
            dev_mode=False,
        ))
        rt = await provider.create()
        yield rt
        try:
            await rt.delete()
        except Exception:
            pass
        await provider.close()

    async def test_session_lifecycle(self, runtime: DockerRuntime):
        """Create a session, run a sync command, check logs, delete."""
        await runtime.create_session("test-session")

        # Sync execute
        result = await runtime.session_execute(
            "test-session", "echo hello-from-session", run_async=False
        )
        assert result.exit_code == 0
        assert "hello-from-session" in result.stdout

        # Delete
        await runtime.delete_session("test-session")

    async def test_async_session_execute(self, runtime: DockerRuntime):
        """Async session execute should start a background process."""
        import asyncio

        await runtime.create_session("async-test")

        result = await runtime.session_execute(
            "async-test",
            "echo async-output && sleep 0.5 && echo done",
            run_async=True,
        )
        assert result.exit_code is None  # async, no exit code yet
        assert result.cmd_id  # should have a command ID

        # Wait for the command to finish
        await asyncio.sleep(2)

        # Check logs
        logs = await runtime.session_command_logs("async-test", result.cmd_id)
        assert logs.exit_code == 0
        assert "async-output" in logs.stdout
        assert "done" in logs.stdout

        await runtime.delete_session("async-test")

    async def test_capabilities_include_sessions(self, runtime: DockerRuntime):
        """Docker runtime should report sessions capability."""
        assert "sessions" in runtime.capabilities
