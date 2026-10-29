"""
Tests for ptc_agent.core.sandbox.runtime — ABCs and data classes.

Covers:
- ExecResult, RuntimeState data classes
- SandboxRuntime ABC contract (cannot instantiate, subclass works, optional abilities, archive)
- SandboxProvider ABC contract (cannot instantiate, is_transient_error default)
"""

import pytest

from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    RuntimeState,
    SandboxProvider,
    SandboxRuntime,
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


class TestExecResult:
    def test_fields(self):
        r = ExecResult(stdout="hello", stderr="", exit_code=0)
        assert r.stdout == "hello"
        assert r.stderr == ""
        assert r.exit_code == 0

    def test_nonzero_exit_code(self):
        r = ExecResult(stdout="", stderr="fail", exit_code=1)
        assert r.exit_code == 1
        assert r.stderr == "fail"


class TestRuntimeState:
    def test_enum_values(self):
        assert RuntimeState.RUNNING == "running"
        assert RuntimeState.STOPPED == "stopped"
        assert RuntimeState.STARTING == "starting"
        assert RuntimeState.STOPPING == "stopping"
        assert RuntimeState.ARCHIVED == "archived"
        assert RuntimeState.ARCHIVING == "archiving"
        assert RuntimeState.ERROR == "error"

    def test_is_string(self):
        assert isinstance(RuntimeState.RUNNING, str)

    def test_all_members(self):
        assert len(RuntimeState) == 7


# ---------------------------------------------------------------------------
# SandboxRuntime ABC
# ---------------------------------------------------------------------------


class _NoSessions:
    """The session methods every runtime must define, for stubs that never use them."""

    async def create_session(self, session_id):
        raise NotImplementedError

    async def session_execute(self, session_id, command):
        raise NotImplementedError

    async def session_command_logs(self, session_id, command_id):
        raise NotImplementedError

    async def session_logs(self, session_id):
        raise NotImplementedError

    async def list_sessions(self):
        raise NotImplementedError

    async def delete_session(self, session_id):
        raise NotImplementedError


class TestSandboxRuntimeABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            SandboxRuntime()

    def test_concrete_subclass_works(self):
        """A minimal stub implementing all abstract methods should instantiate."""

        class StubRuntime(_NoSessions, SandboxRuntime):
            @property
            def id(self) -> str:
                return "stub-1"

            @property
            def working_dir(self) -> str:
                return "/work"

            async def start(self, timeout=120):
                pass

            async def stop(self, timeout=60):
                pass

            async def delete(self):
                pass

            async def get_state(self):
                return RuntimeState.RUNNING

            async def exec(self, command, timeout=60):
                return ExecResult("", "", 0)

            async def upload_file(self, content, dest_path):
                pass

            async def upload_files(self, files):
                pass

            async def download_file(self, path):
                return b""

            async def list_files(self, directory):
                return []

        runtime = StubRuntime()
        assert runtime.id == "stub-1"
        assert runtime.working_dir == "/work"

    def test_default_capabilities(self):
        """A runtime claims no optional ability until it overrides one."""

        class StubRuntime(_NoSessions, SandboxRuntime):
            @property
            def id(self):
                return "s"

            @property
            def working_dir(self):
                return "/"

            async def start(self, timeout=120):
                pass

            async def stop(self, timeout=60):
                pass

            async def delete(self):
                pass

            async def get_state(self):
                return RuntimeState.RUNNING

            async def exec(self, command, timeout=60):
                return ExecResult("", "", 0)

            async def upload_file(self, content, dest_path):
                pass

            async def upload_files(self, files):
                pass

            async def download_file(self, path):
                return b""

            async def list_files(self, directory):
                return []

        runtime = StubRuntime()
        assert not runtime.updates_env
        assert not runtime.archives
        assert not runtime.autostops

    @pytest.mark.asyncio
    async def test_archive_raises_not_implemented(self):
        class StubRuntime(_NoSessions, SandboxRuntime):
            @property
            def id(self):
                return "s"

            @property
            def working_dir(self):
                return "/"

            async def start(self, timeout=120):
                pass

            async def stop(self, timeout=60):
                pass

            async def delete(self):
                pass

            async def get_state(self):
                return RuntimeState.RUNNING

            async def exec(self, command, timeout=60):
                return ExecResult("", "", 0)

            async def upload_file(self, content, dest_path):
                pass

            async def upload_files(self, files):
                pass

            async def download_file(self, path):
                return b""

            async def list_files(self, directory):
                return []

        runtime = StubRuntime()
        with pytest.raises(NotImplementedError):
            await runtime.archive()

    @pytest.mark.asyncio
    async def test_get_metadata_defaults(self):
        class StubRuntime(_NoSessions, SandboxRuntime):
            @property
            def id(self):
                return "meta-1"

            @property
            def working_dir(self):
                return "/home/test"

            async def start(self, timeout=120):
                pass

            async def stop(self, timeout=60):
                pass

            async def delete(self):
                pass

            async def get_state(self):
                return RuntimeState.RUNNING

            async def exec(self, command, timeout=60):
                return ExecResult("", "", 0)

            async def upload_file(self, content, dest_path):
                pass

            async def upload_files(self, files):
                pass

            async def download_file(self, path):
                return b""

            async def list_files(self, directory):
                return []

        runtime = StubRuntime()
        meta = await runtime.get_metadata()
        assert meta == {"id": "meta-1", "working_dir": "/home/test"}


# ---------------------------------------------------------------------------
# SandboxProvider ABC
# ---------------------------------------------------------------------------


class TestSandboxProviderABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            SandboxProvider()

    def test_is_transient_error_default_false(self):
        """Default is_transient_error should return False for any exception."""

        class StubProvider(SandboxProvider):
            async def create(self, *, env_vars=None, **kwargs):
                pass

            async def get(self, sandbox_id):
                pass

            async def close(self):
                pass

        provider = StubProvider()
        assert provider.is_transient_error(ConnectionError("test")) is False
        assert provider.is_transient_error(ValueError("test")) is False
        assert provider.is_transient_error(RuntimeError("test")) is False
