from __future__ import annotations

import pytest

from ptc_agent.core.sandbox.jobs import Ended

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="class")]


class TestExecute:
    """PTCSandbox.start_code_job() -- Python code run as a job, waited on to its end."""

    async def test_execute_simple_code(self, shared_sandbox):
        result = await shared_sandbox.start_code_job("print('hello world')", wait_s=None)
        assert isinstance(result, Ended) and result.exit_code == 0
        assert "hello world" in result.output

    async def test_execute_returns_a_job_outcome(self, shared_sandbox):
        result = await shared_sandbox.start_code_job("x = 42; print(x)", wait_s=None)
        assert isinstance(result, Ended) and result.exit_code == 0
        assert result.ref.command_id.startswith("bg-")
        assert result.handle.ran_for() >= 0

    async def test_execute_error_code(self, shared_sandbox):
        result = await shared_sandbox.start_code_job(
            "raise RuntimeError('test failure')", wait_s=None
        )
        assert isinstance(result, Ended) and result.exit_code != 0
        assert "test failure" in result.output

    async def test_execute_creates_code_file(self, shared_sandbox):
        """With no thread, the code is saved in .system/code/."""
        result = await shared_sandbox.start_code_job("print('saved')", wait_s=None)
        assert result.exit_code == 0
        ls_result = await shared_sandbox.runtime.exec(
            f"ls {shared_sandbox._work_dir}/.system/code/{result.ref.command_id}.py"
        )
        assert ls_result.exit_code == 0

    async def test_execute_with_thread_id(self, shared_sandbox):
        result = await shared_sandbox.start_code_job(
            "print('threaded')", wait_s=None, thread_id="test-thread-123"
        )
        assert result.exit_code == 0
        ls_result = await shared_sandbox.runtime.exec(
            f"test -d {shared_sandbox._work_dir}/.agents/threads/test-thread-123/code && echo OK"
        )
        assert "OK" in ls_result.stdout

    async def test_execute_with_file_creation(self, shared_sandbox):
        """A write through the sandbox filesystem succeeds and is readable back."""
        result = await shared_sandbox.start_code_job(
            "import pathlib; p = pathlib.Path('work/exec_probe'); p.mkdir(parents=True, "
            "exist_ok=True); (p / 'out.txt').write_text('data'); "
            "print((p / 'out.txt').read_text())",
            wait_s=None,
        )
        assert result.exit_code == 0
        assert "data" in result.output

    async def test_execute_with_pythonpath(self, shared_sandbox):
        """Verify PYTHONPATH is set so _internal/src imports work."""
        # Upload a module to _internal/src
        await shared_sandbox.runtime.upload_file(
            b"INTERNAL_VALUE = 99\n",
            f"{shared_sandbox._work_dir}/_internal/src/test_internal.py",
        )
        result = await shared_sandbox.start_code_job(
            "from test_internal import INTERNAL_VALUE; print(INTERNAL_VALUE)", wait_s=None
        )
        assert result.exit_code == 0
        assert "99" in result.output
