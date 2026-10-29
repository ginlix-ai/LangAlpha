"""The in-sandbox half of the command check, run for real against a local root.

The check's promises live in the shell script and the uploaded client, not in
the host code around them: the server sees its vault values from the check's
own vault file, everything it spawned stops with it, and no file outlives the
run. A runtime whose ``exec`` is a local bash and whose root is a temp dir runs
that exact script, so these pin the promises themselves.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.sandbox import mcp_setup
from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import ExecResult
from ptc_agent.core.tool_generator import ToolFunctionGenerator

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="process groups are POSIX"
)

# A legacy-era stdio server: refuses server/discover, answers initialize and
# tools/list, and reports the key it was started with in its one tool. With
# STRAY_PIDFILE set it first leaves a child behind that ignores SIGTERM, the
# shape of an npx tree that outlives its stdin.
_FAKE_SERVER = textwrap.dedent(
    """
    import json, os, subprocess, sys
    pidfile = os.environ.get("STRAY_PIDFILE")
    if pidfile:
        subprocess.Popen([sys.executable, "-c",
            "import os, signal, time, sys; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(120)",
            pidfile])
    if os.environ.get("HANG"):
        import time
        time.sleep(120)
    for line in sys.stdin:
        msg = json.loads(line)
        mid = msg.get("id")
        if mid is None:
            continue
        if msg.get("method") == "initialize":
            result = {"protocolVersion": "2025-06-18", "capabilities": {"tools": {}},
                      "serverInfo": {"name": "fake", "version": "1"}}
            reply = {"jsonrpc": "2.0", "id": mid, "result": result}
        elif msg.get("method") == "tools/list":
            tool = {"name": "echo_key", "description": "key=" + os.environ.get("API_KEY", ""),
                    "inputSchema": {"type": "object", "properties": {}}}
            reply = {"jsonrpc": "2.0", "id": mid, "result": {"tools": [tool]}}
        else:
            reply = {"jsonrpc": "2.0", "id": mid,
                     "error": {"code": -32601, "message": "Method not found"}}
        print(json.dumps(reply), flush=True)
    """
)


class _LocalRuntime:
    """``exec`` is bash on this machine; ``python3`` is this interpreter."""

    def __init__(self, bin_dir: Path) -> None:
        self._path = f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"
        self.commands: list[str] = []

    async def exec(self, command: str, timeout: int = 60) -> ExecResult:
        self.commands.append(command)
        proc = await asyncio.create_subprocess_exec(
            "bash", "-c", command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env={**os.environ, "PATH": self._path},
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        return ExecResult(out.decode(), "", proc.returncode or 0)

    async def upload_file(self, content: bytes, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(content)


def _sandbox(root: Path, bin_dir: Path) -> SimpleNamespace:
    runtime = _LocalRuntime(bin_dir)

    async def runtime_call(func, *args, retry_policy, total_timeout=None, **kwargs):
        return await func(*args, **kwargs)

    async def download(path: str) -> bytes | None:
        p = Path(path)
        return p.read_bytes() if p.exists() else None

    return SimpleNamespace(
        runtime=runtime,
        _work_dir=str(root),
        _wait_ready=AsyncMock(),
        _runtime_call=runtime_call,
        tool_generator=ToolFunctionGenerator(),
        adownload_file_bytes=download,
    )


@pytest.fixture
def sandbox(tmp_path: Path) -> SimpleNamespace:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    # A wrapper, not a symlink: a venv interpreter reached through a link in
    # another folder loses its venv, and the client needs httpx from it.
    wrapper = bin_dir / "python3"
    wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "$@"\n')
    wrapper.chmod(0o755)
    root = tmp_path / "root"
    root.mkdir()
    (tmp_path / "server.py").write_text(_FAKE_SERVER)
    return _sandbox(root, bin_dir)


def _server(tmp_path: Path, **env: str) -> MCPServerConfig:
    return MCPServerConfig(
        name="fake",
        transport="stdio",
        command=sys.executable,
        args=[str(tmp_path / "server.py")],
        env={"API_KEY": "${vault:FAKE_KEY}", **env},
        source="user",
    )


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    # A zombie still answers kill(0) until its new parent reaps it.
    state = subprocess.run(
        ["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True
    ).stdout.strip()
    return bool(state) and not state.startswith("Z")


def _leftovers(sandbox: SimpleNamespace) -> list[str]:
    internal = Path(sandbox._work_dir) / "_internal"
    return sorted(p.name for p in internal.glob(".mcp_check_*")) if internal.exists() else []


@pytest.mark.asyncio
async def test_the_server_starts_with_the_checks_own_vault_values(sandbox, tmp_path):
    # The computer's own vault file holds another value under the same name;
    # the check must neither read it nor touch it.
    real_vault = Path(sandbox._work_dir) / "_internal" / ".vault_secrets.json"
    real_vault.parent.mkdir(parents=True)
    real_vault.write_text(json.dumps({"FAKE_KEY": "stored-elsewhere"}))

    result = await mcp_setup.check_user_mcp_server(
        sandbox, _server(tmp_path), {"FAKE_KEY": "s3cret-value"}, budget_s=30
    )

    assert result["status"] == "ok", result
    assert [t["description"] for t in result["tools"]] == ["key=s3cret-value"]
    assert result["server_info"] == {"name": "fake", "version": "1"}
    assert json.loads(real_vault.read_text()) == {"FAKE_KEY": "stored-elsewhere"}
    assert _leftovers(sandbox) == []


@pytest.mark.asyncio
async def test_everything_the_command_spawned_is_stopped(sandbox, tmp_path):
    pidfile = tmp_path / "stray.pid"
    result = await mcp_setup.check_user_mcp_server(
        sandbox,
        _server(tmp_path, STRAY_PIDFILE=str(pidfile)),
        {"FAKE_KEY": "v"},
        budget_s=30,
    )

    assert result["status"] == "ok", result
    pid = int(pidfile.read_text())
    deadline = time.monotonic() + 5
    while _alive(pid) and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    assert not _alive(pid)
    assert _leftovers(sandbox) == []


@pytest.mark.asyncio
async def test_a_server_that_never_answers_is_stopped_at_the_budget(sandbox, tmp_path):
    pidfile = tmp_path / "stray.pid"
    started = time.monotonic()
    result = await mcp_setup.check_user_mcp_server(
        sandbox,
        _server(tmp_path, HANG="1", STRAY_PIDFILE=str(pidfile)),
        {"FAKE_KEY": "v"},
        budget_s=2,
    )

    assert result["status"] == "timeout"
    assert time.monotonic() - started < 15
    deadline = time.monotonic() + 5
    while pidfile.exists() and _alive(int(pidfile.read_text())) and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    assert not pidfile.exists() or not _alive(int(pidfile.read_text()))
    assert _leftovers(sandbox) == []


@pytest.mark.asyncio
async def test_a_command_that_is_not_there_reports_it(sandbox, tmp_path):
    server = _server(tmp_path).model_copy(update={"command": "no-such-command-xyz"})
    result = await mcp_setup.check_user_mcp_server(
        sandbox, server, {"FAKE_KEY": "v"}, budget_s=30
    )

    assert result["status"] == "error"
    assert "[Errno 2]" in result["error"]
    assert _leftovers(sandbox) == []


@pytest.mark.asyncio
async def test_the_run_is_never_retried_and_files_go_even_when_it_raises(tmp_path):
    calls: list[tuple] = []

    async def runtime_call(func, *args, retry_policy, total_timeout=None, **kwargs):
        calls.append((getattr(func, "__name__", ""), args, retry_policy))
        if func is runtime.exec and "python3 -c" in args[0]:
            raise ConnectionError("sandbox went away")
        return ExecResult("", "", 0)

    runtime = SimpleNamespace(exec=AsyncMock(), upload_file=AsyncMock())
    fake = SimpleNamespace(
        runtime=runtime,
        _work_dir="/home/workspace",
        _wait_ready=AsyncMock(),
        _runtime_call=runtime_call,
        tool_generator=ToolFunctionGenerator(),
        adownload_file_bytes=AsyncMock(return_value=None),
    )

    with pytest.raises(ConnectionError):
        await mcp_setup.check_user_mcp_server(
            fake, _server(tmp_path), {"FAKE_KEY": "v"}, budget_s=30
        )

    run = [c for c in calls if "python3 -c" in str(c[1][0])]
    assert [c[2] for c in run] == [RetryPolicy.UNSAFE]
    cleanup = calls[-1]
    assert cleanup[1][0].startswith("rm -f ")
    assert ".vault.json" in cleanup[1][0] and ".out.json" in cleanup[1][0]


@pytest.mark.asyncio
async def test_no_vault_file_is_written_when_nothing_is_referenced(tmp_path):
    runtime = SimpleNamespace(exec=AsyncMock(), upload_file=AsyncMock())

    async def runtime_call(func, *args, retry_policy, total_timeout=None, **kwargs):
        return await func(*args, **kwargs)

    runtime.exec.return_value = ExecResult("MCP_CHECK_RC=timeout\n", "", 0)
    fake = SimpleNamespace(
        runtime=runtime,
        _work_dir="/home/workspace",
        _wait_ready=AsyncMock(),
        _runtime_call=runtime_call,
        tool_generator=ToolFunctionGenerator(),
        adownload_file_bytes=AsyncMock(return_value=None),
    )
    server = _server(tmp_path).model_copy(update={"env": {}})

    result = await mcp_setup.check_user_mcp_server(fake, server, {}, budget_s=5)

    assert result["status"] == "timeout"
    uploaded = [c.args[1] for c in runtime.upload_file.await_args_list]
    assert len(uploaded) == 1 and uploaded[0].endswith(".py")


@pytest.mark.asyncio
async def test_a_slow_cleanup_does_not_hold_back_an_answer(sandbox, tmp_path, monkeypatch):
    """The removal retries on a flaky link for as long as it takes; the check
    answers after a short wait and the removal finishes on its own."""
    monkeypatch.setattr(mcp_setup, "CHECK_CLEANUP_WAIT_S", 0.2)
    run = sandbox._runtime_call

    async def slow_removal(func, *args, retry_policy, total_timeout=None, **kwargs):
        if args and str(args[0]).startswith("rm -f "):
            await asyncio.sleep(1.0)
        return await run(func, *args, retry_policy=retry_policy, **kwargs)

    sandbox._runtime_call = slow_removal
    started = time.monotonic()

    result = await mcp_setup.check_user_mcp_server(
        sandbox, _server(tmp_path), {"FAKE_KEY": "k-1"}, budget_s=30
    )

    assert result["status"] == "ok", result
    assert time.monotonic() - started < 15
    pending = [t for t in mcp_setup._check_cleanups if not t.done()]
    assert len(pending) == 1
    await asyncio.wait_for(pending[0], timeout=5)
    assert _leftovers(sandbox) == []


def _hanging(where: str) -> tuple[SimpleNamespace, list[str]]:
    commands: list[str] = []

    async def runtime_call(func, *args, retry_policy, total_timeout=None, **kwargs):
        if func is runtime.exec:
            commands.append(args[0])
            if where == "run" and "python3 -c" in args[0]:
                await asyncio.sleep(30)
        return ExecResult("", "", 0)

    async def wait_ready():
        if where == "wake":
            await asyncio.sleep(30)

    runtime = SimpleNamespace(exec=AsyncMock(), upload_file=AsyncMock())
    fake = SimpleNamespace(
        runtime=runtime,
        _work_dir="/home/workspace",
        _wait_ready=wait_ready,
        _runtime_call=runtime_call,
        tool_generator=ToolFunctionGenerator(),
        adownload_file_bytes=AsyncMock(return_value=None),
    )
    return fake, commands


@pytest.mark.asyncio
async def test_a_run_past_its_deadline_ends_there_and_its_files_still_go(tmp_path):
    fake, commands = _hanging("run")
    loop = asyncio.get_running_loop()
    started = loop.time()

    with pytest.raises(TimeoutError):
        await mcp_setup.check_user_mcp_server(
            fake, _server(tmp_path), {"FAKE_KEY": "v"}, budget_s=30,
            deadline=started + 0.3,
        )

    assert loop.time() - started < 2
    assert commands[-1].startswith("rm -f ") and ".vault.json" in commands[-1]


@pytest.mark.asyncio
async def test_a_computer_that_never_comes_up_ends_at_the_deadline(tmp_path):
    fake, commands = _hanging("wake")
    loop = asyncio.get_running_loop()

    with pytest.raises(TimeoutError):
        await mcp_setup.check_user_mcp_server(
            fake, _server(tmp_path), {"FAKE_KEY": "v"}, budget_s=30,
            deadline=loop.time() + 0.2,
        )

    # Nothing was written, so there is nothing to remove.
    assert commands == []
    fake.runtime.upload_file.assert_not_awaited()
