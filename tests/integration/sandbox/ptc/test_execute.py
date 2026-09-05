from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="class")]


class TestExecute:
    """PTCSandbox.execute() -- Python code execution with full orchestration."""

    async def test_execute_simple_code(self, shared_sandbox):
        result = await shared_sandbox.execute("print('hello world')")
        assert result.success is True
        assert "hello world" in result.stdout

    async def test_execute_returns_execution_result(self, shared_sandbox):
        result = await shared_sandbox.execute("x = 42; print(x)")
        assert result.success is True
        assert result.execution_id is not None
        assert result.code_hash is not None
        assert isinstance(result.duration, float)

    async def test_execute_error_code(self, shared_sandbox):
        result = await shared_sandbox.execute("raise RuntimeError('test failure')")
        assert result.success is False

    async def test_execute_creates_code_file(self, shared_sandbox):
        """execute() should save the code to .system/code/ directory."""
        result = await shared_sandbox.execute("print('saved')")
        assert result.success is True
        # Check code dir has files
        ls_result = await shared_sandbox.runtime.exec(f"ls {shared_sandbox._work_dir}/.system/code/")
        assert ls_result.exit_code == 0

    async def test_execute_with_thread_id(self, shared_sandbox):
        result = await shared_sandbox.execute(
            "print('threaded')", thread_id="test-thread-123"
        )
        assert result.success is True
        # Check thread dir was created
        ls_result = await shared_sandbox.runtime.exec(
            f"test -d {shared_sandbox._work_dir}/.agents/threads/test-thread-123/code && echo OK"
        )
        assert "OK" in ls_result.stdout

    async def test_execute_with_file_creation(self, shared_sandbox):
        """A write through the sandbox filesystem succeeds and is readable back."""
        result = await shared_sandbox.execute(
            "import pathlib; p = pathlib.Path('work/exec_probe'); p.mkdir(parents=True, "
            "exist_ok=True); (p / 'out.txt').write_text('data'); "
            "print((p / 'out.txt').read_text())"
        )
        assert result.success is True
        assert "data" in result.stdout

    async def test_execute_increments_counter(self, shared_sandbox):
        initial = shared_sandbox.execution_count
        await shared_sandbox.execute("print(1)")
        assert shared_sandbox.execution_count == initial + 1
        await shared_sandbox.execute("print(2)")
        assert shared_sandbox.execution_count == initial + 2

    async def test_execute_with_pythonpath(self, shared_sandbox):
        """Verify PYTHONPATH is set so _internal/src imports work."""
        # Upload a module to _internal/src
        await shared_sandbox.runtime.upload_file(
            b"INTERNAL_VALUE = 99\n",
            f"{shared_sandbox._work_dir}/_internal/src/test_internal.py",
        )
        result = await shared_sandbox.execute(
            "from test_internal import INTERNAL_VALUE; print(INTERNAL_VALUE)"
        )
        assert result.success is True
        assert "99" in result.stdout
