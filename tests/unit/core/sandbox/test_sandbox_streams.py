"""The provider-independent parts of sandbox streams, and each provider's
stream against a fake of its SDK."""

from __future__ import annotations

import asyncio
import gc
import os
import random
import signal
import subprocess
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from e2b import CommandExitException
from e2b.envd.process import process_pb
from e2b.sandbox_async.commands.command_handle import AsyncCommandHandle
from packaging.version import Version
from protobuf import Oneof

from ptc_agent.core.sandbox.providers import _daytona_stream, _docker_stream, _e2b_stream, _streams
from ptc_agent.core.sandbox.providers._daytona_stream import LineHoldback, LineReplay
from ptc_agent.core.sandbox.providers._streams import PID_MARK, PidLine, pid_marked
from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime
from ptc_agent.core.sandbox.providers.docker import DockerRuntime
from ptc_agent.core.sandbox.providers.e2b_runtime import E2BRuntime
from ptc_agent.core.sandbox.runtime import ExecResult, StreamProcess

MARK = f"{PID_MARK} 4242\n"


class TestPidLine:
    def test_mark_split_anywhere_is_taken_off(self):
        line = PidLine()
        data = f"{MARK}real error\n".encode()
        out = b"".join(line.feed(data[i : i + 3]) for i in range(0, len(data), 3))
        assert out == b"real error\n"
        assert line.pid == 4242

    def test_a_first_line_that_is_not_the_mark_is_kept(self):
        line = PidLine()
        assert line.feed(b"bash: no such file\nmore") == b"bash: no such file\nmore"
        assert line.pid is None
        assert line.feed(b"after") == b"after"

    def test_flush_returns_what_never_got_a_newline(self):
        line = PidLine()
        assert line.feed(b"partial") == b""
        assert line.flush() == b"partial"
        assert line.flush() == b""


async def _run_marked(command: str, pid_file: str) -> tuple[asyncio.subprocess.Process, PidLine]:
    proc = await asyncio.create_subprocess_exec(
        "bash",
        "-c",
        pid_marked(command, pid_file),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    line = PidLine()
    while not line.done:
        chunk = await proc.stderr.readline()
        if not chunk:
            await _reap(proc)
            pytest.fail("the marked shell's stderr ended before its pid line")
        line.feed(chunk)
    return proc, line


async def _reap(proc: asyncio.subprocess.Process, sleeper: str | None = None) -> None:
    """Ends what a failed test left running: the sleeper by its unique command
    line, which outlives the wrapper once it leads a group of its own."""
    if sleeper is not None:
        subprocess.run(["pkill", "-KILL", "-f", sleeper], capture_output=True)
    if proc.returncode is None:
        proc.kill()
    await proc.wait()


@pytest.mark.asyncio
async def test_pid_marked_passes_stdin_streams_and_status(tmp_path):
    proc, line = await _run_marked("cat; echo err >&2; exit 3", str(tmp_path / "pid"))
    out, err = await proc.communicate(b"in\n")
    assert (out, err, proc.returncode) == (b"in\n", b"err\n", 3)
    assert line.pid is not None


@pytest.mark.asyncio
async def test_pid_marked_keeps_the_group_id_in_a_private_file_while_it_runs(tmp_path):
    pid_file = tmp_path / "pid"
    proc, line = await _run_marked("read -r _; exit 4", str(pid_file))
    for _ in range(50):
        if pid_file.exists() and pid_file.read_text():
            break
        await asyncio.sleep(0.02)
    assert pid_file.read_text() == f"{line.pid}\n"
    assert pid_file.stat().st_mode & 0o777 == 0o600
    await proc.communicate(b"go\n")
    assert proc.returncode == 4
    assert not pid_file.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["{} | cat", "{}; echo done"])
async def test_pid_marked_runs_the_command_as_a_group_a_signal_ends_whole(shape, tmp_path):
    sleeper = f"sleep {random.randint(700_000, 799_999)}"
    proc, line = await _run_marked(shape.format(sleeper), str(tmp_path / "pid"))
    try:
        await asyncio.sleep(0.3)
        assert os.getpgid(line.pid) == line.pid
        os.killpg(line.pid, signal.SIGTERM)
        assert await asyncio.wait_for(proc.wait(), 5) == 143
        await asyncio.sleep(0.2)
        left = subprocess.run(["pgrep", "-f", sleeper], capture_output=True, text=True)
        assert left.stdout == ""
    finally:
        await _reap(proc, sleeper)


def test_holdback_sends_only_whole_lines():
    holdback = LineHoldback()
    data = "a中\né\nb".encode()
    assert holdback.lines(data[:3]) == ""
    assert holdback.lines(data[3:7]) == "a中\n"
    assert holdback.lines(data[7:]) == "é\n"
    assert holdback.lines(b"c\n") == "bc\n"


def test_holdback_refuses_bytes_that_are_not_utf8_and_drops_them():
    holdback = LineHoldback()
    with pytest.raises(ValueError):
        holdback.lines(b"\xff\n")
    assert holdback.lines(b"ok\n") == "ok\n"


def test_holdback_refuses_the_write_that_holds_bad_bytes_and_keeps_earlier_ones():
    holdback = LineHoldback()
    assert holdback.lines(b"a") == ""
    with pytest.raises(ValueError):
        holdback.lines(b"ok\n\xff")
    assert holdback.lines(b"b\n") == "ab\n"


def test_holdback_keeps_a_character_an_accepted_write_began_past_a_refused_one():
    holdback = LineHoldback()
    assert holdback.lines("a中".encode()[:3]) == ""
    with pytest.raises(ValueError):
        holdback.lines(b"\xff")
    assert holdback.lines("中\n".encode()[2:]) == "a中\n"


def test_holdback_holds_a_long_unfinished_line_in_linear_time():
    holdback = LineHoldback()
    start = time.perf_counter()
    for _ in range(8192):
        holdback.lines(b"x" * 4096)
    assert time.perf_counter() - start < 2
    assert holdback.lines(b"\n") == "x" * 4096 * 8192 + "\n"


def test_line_replay_holds_a_long_unfinished_line_in_linear_time():
    seen = LineReplay()
    start = time.perf_counter()
    for _ in range(8192):
        seen.take("x" * 4096)
    assert time.perf_counter() - start < 2
    assert seen.held == 4096 * 8192
    assert seen.take("y\nz") == "x" * 4096 * 8192 + "y\n"
    assert seen.held == 1
    assert seen.rest() == "z"


def test_line_replay_delivers_whole_lines_once():
    seen = LineReplay()
    assert seen.take("ab\nc") + seen.take("d\n") == "ab\ncd\n"
    seen.reattach()
    assert seen.take("ab\ncd") + seen.take("\nef\n") == "ef\n"
    assert seen.take("tail") == ""
    assert seen.rest() == "tail"


# ---------------------------------------------------------------------------
# Daytona
# ---------------------------------------------------------------------------


def _daytona(logs, *, ends_after=None):
    """A Daytona runtime whose log socket plays *logs*, one entry per attach.

    Each entry is (stream, text) pairs and then None for a clean close or an
    exception for a drop; past the last, an attach closes clean with nothing.
    The command reads as exited (0) once *ends_after* attaches ran, by
    default all of them.
    """
    attaches = iter(logs)
    ran = [0]
    ends_after = len(logs) if ends_after is None else ends_after

    async def get_logs(session_id, command_id, on_stdout, on_stderr):
        ran[0] += 1
        *chunks, end = next(attaches, [None])
        for name, text in chunks:
            await (on_stdout if name == "out" else on_stderr)(text)
        if end is not None:
            raise end

    async def get_command(session_id, command_id):
        return SimpleNamespace(exit_code=0 if ran[0] >= ends_after else None)

    process = MagicMock()
    process.create_session = AsyncMock()
    process.delete_session = AsyncMock()
    process.execute_session_command = AsyncMock(return_value=SimpleNamespace(cmd_id="cmd"))
    process.get_session_command_logs_async = get_logs
    process.get_session_command = get_command
    process.send_session_command_input = AsyncMock()
    process.exec = AsyncMock(return_value=SimpleNamespace(result="", exit_code=0))
    raw = SimpleNamespace(
        id="sb", process=process, auto_stop_interval=60, refresh_activity=AsyncMock()
    )
    return DaytonaRuntime(raw), process


@pytest.fixture
def no_reattach_delay(monkeypatch):
    monkeypatch.setattr(_daytona_stream, "_REATTACH_FIRST_DELAY_S", 0)


@pytest.mark.asyncio
async def test_daytona_reattach_skips_the_replay(no_reattach_delay):
    runtime, _ = _daytona(
        [
            [("err", MARK[:5]), ("out", "ab\n"), ("err", MARK[5:]), ("out", "c"), Exception("1006")],
            [("err", MARK), ("out", "ab\nc"), ("out", "d\n"), ("err", "oops\n"), Exception("1006")],
            [("err", MARK + "oo"), ("out", "ab\ncd\n"), ("out", "ef\n"), ("err", "ps\nlast\n"), None],
        ]
    )
    stream = await runtime.open_stream("cmd")
    assert await stream.wait() == 0
    assert await stream.stdout.read() == b"ab\ncd\nef\n"
    assert await stream.stderr.read() == b"oops\nlast\n"


@pytest.mark.asyncio
async def test_daytona_a_drop_mid_character_or_mid_marker_leaves_the_bytes_exact(
    no_reattach_delay,
):
    # A drop flushes what the SDK held: half a character as U+FFFD, or the
    # first bytes of a stream marker.
    runtime, _ = _daytona(
        [
            [("err", MARK), ("out", "line1\nli"), ("out", "ne2 \ufffd"), Exception("1006")],
            [("err", MARK), ("out", "line1\nline2 中\n"), ("out", "\x01\x01"), Exception("1006")],
            [("err", MARK), ("out", "line1\nline2 中\nline3\n"), None],
        ]
    )
    stream = await runtime.open_stream("cmd")
    assert await stream.wait() == 0
    assert await stream.stdout.read() == "line1\nline2 中\nline3\n".encode()


@pytest.mark.asyncio
async def test_daytona_reads_the_tail_a_drop_at_the_end_missed(no_reattach_delay):
    runtime, _ = _daytona(
        [
            [("err", MARK), ("out", "ab\n"), Exception("1006")],
            [("err", MARK), ("out", "ab\n"), ("out", '{"type": "result"}\n'), None],
        ],
        ends_after=1,
    )
    stream = await runtime.open_stream("cmd")
    assert await stream.wait() == 0
    assert await stream.stdout.read() == b'ab\n{"type": "result"}\n'


@pytest.mark.asyncio
async def test_daytona_a_tail_that_cannot_be_read_fails_the_stream(no_reattach_delay):
    runtime, process = _daytona(
        [
            [("err", MARK), ("out", "ab\n"), Exception("1006")],
            [("err", MARK), ("out", "a"), Exception("1006")],
        ],
        ends_after=1,
    )
    stream = await runtime.open_stream("cmd")
    assert await stream.wait() == -1
    assert stream.failure == "the end of its output was lost"
    assert await stream.stdout.read() == b"ab\n"
    await stream.close()


@pytest.mark.asyncio
async def test_daytona_writes_whole_lines_in_order(no_reattach_delay):
    runtime, process = _daytona([[("err", MARK), None]])
    sent: list[str] = []

    async def send(session_id, command_id, text):
        await asyncio.sleep(0.01 * (len(sent) % 3))
        sent.append(text)

    process.send_session_command_input = send
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    stream = await runtime.open_stream("cat")
    data = "x中é\n".encode() * 5
    await asyncio.gather(*(stream.write(data[i : i + 1]) for i in range(len(data))))
    assert "".join(sent) == data.decode()
    assert all(text.endswith("\n") for text in sent)
    await stream.write(b"no end yet")
    assert "".join(sent) == data.decode()
    assert await stream.write_eof() is False
    process.get_session_command.return_value = SimpleNamespace(exit_code=137)
    await stream.close()


@pytest.mark.asyncio
async def test_daytona_holds_input_until_the_command_has_started(no_reattach_delay):
    runtime, process = _daytona([[("err", MARK), None]])
    started = asyncio.Event()
    get_logs = process.get_session_command_logs_async

    async def late_logs(*args):
        await started.wait()
        await get_logs(*args)

    process.get_session_command_logs_async = late_logs
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    stream = await runtime.open_stream("cat")
    write = asyncio.create_task(stream.write(b"hi\n"))
    await asyncio.sleep(0.05)
    process.send_session_command_input.assert_not_awaited()
    started.set()
    await asyncio.wait_for(write, 1)
    process.send_session_command_input.assert_awaited_once_with(stream._session_id, "cmd", "hi\n")
    process.get_session_command.return_value = SimpleNamespace(exit_code=0)
    await stream.close()


@pytest.mark.asyncio
async def test_daytona_a_kill_exec_with_no_exit_code_fails_the_stream_and_close_frees_it(no_reattach_delay):
    runtime, process = _daytona([[("err", MARK), None]])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    process.exec = AsyncMock(return_value=SimpleNamespace(result="", exit_code=None))
    stream = await runtime.open_stream("sleep 600")
    await asyncio.wait_for(stream.close(), 20)
    assert stream.failure.startswith("it could not be killed")
    process.delete_session.assert_awaited_once()


@pytest.mark.asyncio
async def test_daytona_a_status_read_that_hangs_is_a_miss(monkeypatch, no_reattach_delay):
    monkeypatch.setattr(_daytona_stream, "_STATUS_READ_S", 0.01)
    runtime, process = _daytona([[("err", MARK), None]])
    reads = [_hang, AsyncMock(return_value=SimpleNamespace(exit_code=143))]

    async def get_command(*args):
        return await reads.pop(0)(*args) if len(reads) > 1 else await reads[0](*args)

    process.get_session_command = get_command
    stream = await runtime.open_stream("sleep 600")
    assert await asyncio.wait_for(stream.wait(), 5) == 143
    assert stream.failure is None
    await stream.close()


@pytest.mark.asyncio
async def test_daytona_signals_the_group_and_close_deletes_its_session():
    runtime, process = _daytona([[("err", MARK), None]])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    stream = await runtime.open_stream("sleep 600")
    command = process.execute_session_command.call_args.args[1].command
    assert command.startswith("bash -c ")

    async def killed(cmd, **kwargs):
        if "-KILL" in cmd:
            process.get_session_command.return_value = SimpleNamespace(exit_code=137)
        return SimpleNamespace(result="", exit_code=0)

    process.exec.side_effect = killed
    await stream.terminate()
    process.exec.assert_awaited_with("kill -TERM -- -4242", cwd="/", timeout=10)
    await stream.close()
    process.exec.assert_awaited_with("kill -KILL -- -4242", cwd="/", timeout=10)
    assert stream.returncode == 137
    process.delete_session.assert_awaited_once()
    await stream.close()
    await stream.kill()
    process.delete_session.assert_awaited_once()


@pytest.mark.asyncio
async def test_daytona_a_signal_cuts_the_reattach_backoff_short(monkeypatch):
    monkeypatch.setattr(_daytona_stream, "_REATTACH_FIRST_DELAY_S", 30)
    runtime, process = _daytona([[("err", MARK), Exception("1006")]])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))

    async def killed(cmd, **kwargs):
        process.get_session_command.return_value = SimpleNamespace(exit_code=143)
        return SimpleNamespace(result="", exit_code=0)

    process.exec.side_effect = killed
    stream = await runtime.open_stream("sleep 600")
    await stream.terminate()
    assert await asyncio.wait_for(stream.wait(), 1) == 143
    assert stream.failure is None
    await stream.close()


@pytest.mark.asyncio
async def test_daytona_an_unfinished_line_counts_against_the_shared_budget(monkeypatch):
    monkeypatch.setattr(_streams, "MAX_BUFFERED", 10)
    runtime, process = _daytona([[("err", MARK), ("out", "x" * 6), ("err", "y" * 5), None]])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    stream = await runtime.open_stream("cmd")
    assert await asyncio.wait_for(stream.wait(), 1) == -1
    assert stream.failure.startswith("more than 10 bytes")
    await stream.close()


@pytest.mark.asyncio
async def test_daytona_an_unfinished_line_counts_in_bytes(monkeypatch):
    monkeypatch.setattr(_streams, "MAX_BUFFERED", 10)
    runtime, process = _daytona([[("err", MARK), ("out", "\U0001f600" * 3), None]])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    stream = await runtime.open_stream("cmd")
    assert await asyncio.wait_for(stream.wait(), 1) == -1
    assert stream.failure.startswith("more than 10 bytes")
    await stream.close()


@pytest.mark.asyncio
async def test_daytona_stops_reading_once_the_stream_failed(monkeypatch, no_reattach_delay):
    monkeypatch.setattr(_streams, "MAX_BUFFERED", 10)
    runtime, process = _daytona([])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    streams: list[StreamProcess] = []
    held_after_attach: list[int] = []

    async def get_logs(session_id, command_id, on_stdout, on_stderr):
        await on_stderr(MARK)
        for _ in range(50):
            await on_stdout("y" * 1000)
        held_after_attach.append(streams[0]._stdout_lines.held)

    process.get_session_command_logs_async = get_logs
    streams.append(await runtime.open_stream("cmd"))
    stream = streams[0]
    assert await asyncio.wait_for(stream.wait(), 1) == -1
    assert stream.failure.startswith("more than 10 bytes")
    for _ in range(20):
        await asyncio.sleep(0)
    assert held_after_attach == [1000]
    await stream.close()


@pytest.mark.asyncio
async def test_daytona_counts_an_open_stream_as_activity():
    runtime, process = _daytona([[("err", MARK), None]])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    runtime.raw.auto_stop_interval = 0.02
    stream = await runtime.open_stream("cat")
    await asyncio.sleep(0.75)
    assert runtime.raw.refresh_activity.await_count >= 2
    process.get_session_command.return_value = SimpleNamespace(exit_code=137)
    await stream.close()


@pytest.mark.asyncio
async def test_a_cancelled_close_still_tears_down_once(no_reattach_delay):
    runtime, process = _daytona([[("err", MARK), None]])
    stream = await runtime.open_stream("true")
    await stream.wait()
    gate = asyncio.Event()

    async def delete(session_id):
        await gate.wait()

    process.delete_session = AsyncMock(side_effect=delete)
    first = asyncio.create_task(stream.close())
    await asyncio.sleep(0.01)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    second = asyncio.create_task(stream.close())
    await asyncio.sleep(0.01)
    assert not second.done()
    gate.set()
    await asyncio.wait_for(second, 1)
    await stream.close()
    process.delete_session.assert_awaited_once()


# ---------------------------------------------------------------------------
# E2B
# ---------------------------------------------------------------------------


def _e2b(monkeypatch, outcome):
    """An E2B runtime whose process ends as *outcome* says: an exit code, an
    exception, or None to run until a kill exec ends it."""
    started: dict = {}
    killed = asyncio.Event()

    async def wait():
        if outcome is None:
            await killed.wait()
            return SimpleNamespace(exit_code=137)
        if isinstance(outcome, BaseException):
            raise outcome
        return SimpleNamespace(exit_code=outcome)

    async def start(sandbox, command, *, on_stdout, on_stderr):
        started.update(command=command, on_stdout=on_stdout, on_stderr=on_stderr)
        return SimpleNamespace(
            pid=7, wait=wait, disconnect=AsyncMock(), _stdout_chunks=[], _stderr_chunks=[]
        )

    async def kill_exec(command, timeout=60, *, env=None):
        killed.set()
        return ExecResult(stdout="", stderr="", exit_code=0)

    async def run_plain(sandbox, command, timeout):
        return await runtime.exec(command, timeout=timeout)

    monkeypatch.setattr(_e2b_stream, "_start_plain_bash", start)
    monkeypatch.setattr(_e2b_stream, "_run_plain", run_plain)
    sandbox = MagicMock()
    sandbox.commands._envd_version = Version("0.5.2")
    sandbox.commands.close_stdin = AsyncMock()
    sandbox.commands.kill = AsyncMock(side_effect=lambda pid: killed.set() or True)
    runtime = E2BRuntime(MagicMock(), "sb", sandbox=sandbox, window_s=3600, max_timeout_s=3600)
    runtime.exec = AsyncMock(side_effect=kill_exec)
    return runtime, sandbox, started


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "status"),
    [
        (0, 0),
        (CommandExitException(stdout="", stderr="", exit_code=3, error=None), 3),
        (CommandExitException(stdout="", stderr="", exit_code=-1, error="signal: killed"), 137),
        (CommandExitException(stdout="", stderr="", exit_code=-1, error="signal: terminated"), 143),
    ],
)
async def test_e2b_reports_the_shells_status(monkeypatch, outcome, status):
    runtime, _, started = _e2b(monkeypatch, outcome)
    stream = await runtime.open_stream("cmd")
    assert started["command"] == pid_marked("cmd", stream._pid_file)
    assert await stream.wait() == status
    assert stream.returncode == status


@pytest.mark.asyncio
async def test_e2b_a_dropped_event_stream_fails_and_kills_the_group(monkeypatch):
    runtime, sandbox, started = _e2b(monkeypatch, Exception("stream reset"))
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    assert await stream.wait() == -1
    assert stream.failure.startswith("its event stream dropped")
    await asyncio.sleep(0.01)
    runtime.exec.assert_awaited_with("kill -KILL -- -4242", timeout=10)
    await stream.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exec_result",
    [ConnectionError("envd unreachable"), ExecResult(stdout="", stderr="timeout", exit_code=-1)],
)
async def test_a_kill_that_cannot_be_sent_fails_the_stream(monkeypatch, exec_result):
    runtime, sandbox, started = _e2b(monkeypatch, None)
    runtime.exec = AsyncMock(side_effect=[exec_result] * 3)
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    await stream.kill()
    assert await asyncio.wait_for(stream.wait(), 1) == -1
    assert stream.failure.startswith("it could not be killed")
    sandbox.commands.kill.assert_not_awaited()
    await stream.close()


@pytest.mark.asyncio
async def test_a_failed_streams_kill_that_cannot_be_sent_joins_its_failure(monkeypatch):
    runtime, _, started = _e2b(monkeypatch, Exception("stream reset"))
    runtime.exec = AsyncMock(side_effect=ConnectionError("envd unreachable"))
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    assert await stream.wait() == -1
    await asyncio.sleep(0.01)
    assert stream.failure == (
        "its event stream dropped: stream reset; it could not be killed: envd unreachable"
    )
    await stream.close()
    assert stream.failure.count("it could not be killed") == 2


@pytest.mark.asyncio
async def test_a_process_that_never_named_its_pid_is_signalled_through_its_file(monkeypatch):
    monkeypatch.setattr(_streams, "_PID_WAIT_S", 0.01)
    runtime, _, _ = _e2b(monkeypatch, None)
    stream = await runtime.open_stream("cmd")
    await stream.terminate()
    assert await asyncio.wait_for(stream.wait(), 1) == 137
    assert stream.failure is None
    command = runtime.exec.await_args.args[0]
    assert stream._pid_file in command
    assert 'kill -TERM -- -"$pgid"' in command
    await stream.close()


@pytest.mark.asyncio
async def test_a_stream_lost_before_its_pid_line_still_kills_the_group(monkeypatch, tmp_path):
    runtime, _, _ = _e2b(monkeypatch, Exception("stream reset"))
    pid_file = str(tmp_path / "pid")
    monkeypatch.setattr(_e2b_stream, "stream_pid_file", lambda: pid_file)
    sleeper = f"sleep {random.randint(800_000, 899_999)}"
    proc = await asyncio.create_subprocess_exec(
        "bash",
        "-c",
        pid_marked(sleeper, pid_file),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )

    try:
        runtime.exec = AsyncMock(side_effect=_run_locally)
        stream = await runtime.open_stream(sleeper)
        assert await stream.wait() == -1
        assert await asyncio.wait_for(proc.wait(), 5) == 137
        assert not os.path.exists(pid_file)
        await stream.close()
    finally:
        await _reap(proc, sleeper)


async def _run_locally(command, timeout=60, *, env=None):
    kill = await asyncio.create_subprocess_exec("bash", "-c", command)
    return ExecResult(stdout="", stderr="", exit_code=await kill.wait())


async def _run_with_kill_stubbed(command, timeout=60, *, env=None):
    return await _run_locally(f"kill() {{ :; }}; {command}", timeout, env=env)


@pytest.mark.asyncio
async def test_a_pid_file_still_empty_is_read_again(monkeypatch, tmp_path):
    runtime, _, _ = _e2b(monkeypatch, Exception("stream reset"))
    pid_file = tmp_path / "pid"
    pid_file.write_text("")
    monkeypatch.setattr(_e2b_stream, "stream_pid_file", lambda: str(pid_file))
    sleeper = await asyncio.create_subprocess_exec(
        "sleep", str(random.randint(800_000, 899_999)), start_new_session=True
    )
    try:
        runtime.exec = AsyncMock(side_effect=_run_locally)
        asyncio.get_running_loop().call_later(0.3, pid_file.write_text, f"{sleeper.pid}\n")
        stream = await runtime.open_stream("cmd")
        assert await stream.wait() == -1
        assert await asyncio.wait_for(sleeper.wait(), 5) == -signal.SIGKILL
        await stream.close()
    finally:
        if sleeper.returncode is None:
            sleeper.kill()


@pytest.mark.asyncio
@pytest.mark.parametrize("held", ["-1\n", "x\n"])
async def test_a_pid_file_that_names_no_group_is_not_read_as_gone(monkeypatch, tmp_path, held):
    runtime, _, _ = _e2b(monkeypatch, Exception("stream reset"))
    pid_file = tmp_path / "pid"
    pid_file.write_text(held)
    monkeypatch.setattr(_e2b_stream, "stream_pid_file", lambda: str(pid_file))
    runtime.exec = AsyncMock(side_effect=_run_with_kill_stubbed)
    stream = await runtime.open_stream("cmd")
    assert await stream.wait() == -1
    await asyncio.sleep(0.2)
    assert stream.failure == (
        "its event stream dropped: stream reset; "
        "it could not be killed: its pid file names no process group"
    )
    await stream.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("502 bad gateway"), ConnectionError("reset")])
async def test_a_lost_write_fails_the_stream_and_kills_the_group(no_reattach_delay, error):
    runtime, process = _daytona([[("err", MARK), None]])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    process.send_session_command_input = AsyncMock(side_effect=error)
    stream = await runtime.open_stream("cat")
    await asyncio.sleep(0.01)
    with pytest.raises(ConnectionError):
        await stream.write(b"hi\n")
    assert await asyncio.wait_for(stream.wait(), 1) == -1
    assert stream.failure.startswith("a write to it was lost")
    await asyncio.sleep(0.01)
    process.exec.assert_awaited_with("kill -KILL -- -4242", cwd="/", timeout=10)
    await stream.close()


async def _hang(*_, **__):
    await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_a_write_cancelled_mid_send_fails_the_stream(no_reattach_delay):
    runtime, process = _daytona([[("err", MARK), None]])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    process.send_session_command_input = _hang
    stream = await runtime.open_stream("cat")
    await asyncio.sleep(0.01)
    write = asyncio.create_task(stream.write(b"hi\n"))
    await asyncio.sleep(0.01)
    write.cancel()
    with pytest.raises(asyncio.CancelledError):
        await write
    assert await asyncio.wait_for(stream.wait(), 1) == -1
    assert stream.failure == "a write to it was lost: CancelledError"
    await asyncio.sleep(0.01)
    process.exec.assert_awaited_with("kill -KILL -- -4242", cwd="/", timeout=10)
    await stream.close()


@pytest.mark.asyncio
async def test_a_stdin_close_cancelled_mid_send_fails_the_stream(monkeypatch):
    runtime, sandbox, started = _e2b(monkeypatch, None)
    sandbox.commands.close_stdin = _hang
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    close = asyncio.create_task(stream.write_eof())
    await asyncio.sleep(0.01)
    close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close
    assert await asyncio.wait_for(stream.wait(), 1) == -1
    assert stream.failure == "closing its stdin was lost: CancelledError"
    await stream.close()


@pytest.mark.asyncio
async def test_a_kill_exec_that_never_answers_fails_the_stream_and_close_returns(monkeypatch, no_reattach_delay):
    monkeypatch.setattr(_streams, "_KILL_WAIT_S", 0.01)
    runtime, process = _daytona([[("err", MARK), None]])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    process.exec = AsyncMock(side_effect=_hang)
    stream = await runtime.open_stream("sleep 600")
    await asyncio.wait_for(stream.close(), 1)
    assert stream.failure == "it could not be killed: TimeoutError"
    process.delete_session.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_release_that_never_answers_does_not_hold_close(monkeypatch, no_reattach_delay):
    monkeypatch.setattr(_streams, "_RELEASE_WAIT_S", 0.01)
    runtime, process = _daytona([[("err", MARK), None]])
    process.delete_session = AsyncMock(side_effect=_hang)
    stream = await runtime.open_stream("cat")
    assert await asyncio.wait_for(stream.wait(), 1) == 0
    await asyncio.wait_for(stream.close(), 1)
    process.delete_session.assert_awaited_once()


@pytest.mark.asyncio
async def test_e2b_signals_through_plain_bash_not_exec(monkeypatch):
    runtime, sandbox, started = _e2b(monkeypatch, None)
    ran: list[str] = []

    async def run_plain(sb, command, timeout):
        ran.append(command)
        await sandbox.commands.kill(7)
        return ExecResult(stdout="", stderr="", exit_code=0)

    monkeypatch.setattr(_e2b_stream, "_run_plain", run_plain)
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    await stream.kill()
    assert await asyncio.wait_for(stream.wait(), 1) == 137
    assert ran == ["kill -KILL -- -4242"]
    runtime.exec.assert_not_awaited()
    await stream.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "status"),
    [
        (SimpleNamespace(exit_code=0), 0),
        (CommandExitException(stdout="", stderr="no such process", exit_code=1, error=None), 1),
        (CommandExitException(stdout="", stderr="", exit_code=-1, error="signal: killed"), 137),
    ],
)
async def test_run_plain_reports_the_shells_status_and_lets_go(monkeypatch, outcome, status):
    commands: list[str] = []
    disconnect = AsyncMock()

    async def wait():
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def start(sandbox, command, *, on_stdout, on_stderr, cwd=None):
        commands.append((command, cwd))
        return SimpleNamespace(wait=wait, disconnect=disconnect)

    monkeypatch.setattr(_e2b_stream, "_start_plain_bash", start)
    result = await _e2b_stream._run_plain(MagicMock(), "kill -TERM -- -42", timeout=10)
    assert (commands, result.exit_code) == ([("kill -TERM -- -42", "/")], status)
    disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_lost_stdin_close_fails_the_stream(monkeypatch):
    runtime, sandbox, started = _e2b(monkeypatch, None)
    sandbox.commands.close_stdin = AsyncMock(side_effect=RuntimeError("envd unreachable"))
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    with pytest.raises(RuntimeError):
        await stream.write_eof()
    assert await asyncio.wait_for(stream.wait(), 1) == -1
    assert stream.failure.startswith("closing its stdin was lost")
    await stream.close()


def test_failure_is_abstract_so_a_delegating_stream_must_forward_it():
    assert {"failure", "returncode"} <= StreamProcess.__abstractmethods__


@pytest.mark.asyncio
async def test_e2b_stops_keeping_the_sandbox_up_once_the_process_ends(monkeypatch):
    runtime, _, started = _e2b(monkeypatch, None)
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    await stream.kill()
    await stream.wait()
    for _ in range(5):
        await asyncio.sleep(0)
    assert not stream._tasks
    await stream.close()


@pytest.mark.asyncio
async def test_e2b_keeps_a_short_idle_window_renewed(monkeypatch):
    runtime, _, started = _e2b(monkeypatch, None)
    runtime._window_s = 0.4
    runtime.note_activity = AsyncMock()
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    await asyncio.sleep(0.35)
    assert runtime.note_activity.await_count >= 2
    await stream.close()


@pytest.mark.asyncio
async def test_e2b_output_reaches_the_readers_and_stdin_closes(monkeypatch):
    runtime, sandbox, started = _e2b(monkeypatch, None)
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    started["on_stdout"]("中")
    started["on_stderr"]("err")
    assert await stream.write_eof() is True
    sandbox.commands.close_stdin.assert_awaited_once_with(7)
    await stream.close()
    assert await stream.stdout.read() == "中".encode()
    assert await stream.stderr.read() == b"err"


@pytest.mark.asyncio
async def test_e2b_handle_keeps_none_of_the_output_it_delivers(monkeypatch):
    runtime, _, _ = _e2b(monkeypatch, None)
    handles: list[AsyncCommandHandle] = []

    def event(name, value):
        return process_pb.StartResponse(event=process_pb.ProcessEvent(event=Oneof(name, value)))

    async def events():
        for name, chunk in [("stderr", MARK.encode()), ("stdout", b"a\n"), ("stderr", b"e\n")]:
            yield event("data", process_pb.ProcessEvent.DataEvent(output=Oneof(name, chunk)))
        yield event("end", process_pb.ProcessEvent.EndEvent(exit_code=0, exited=True))

    async def start(sandbox, command, *, on_stdout, on_stderr):
        handles.append(
            AsyncCommandHandle(
                pid=7,
                handle_kill=AsyncMock(),
                events=events(),
                on_stdout=on_stdout,
                on_stderr=on_stderr,
            )
        )
        return handles[0]

    monkeypatch.setattr(_e2b_stream, "_start_plain_bash", start)
    stream = await runtime.open_stream("cmd")
    assert await asyncio.wait_for(stream.wait(), 1) == 0
    assert await stream.stdout.read() == b"a\n"
    assert await stream.stderr.read() == b"e\n"
    assert (handles[0]._stdout_chunks, handles[0]._stderr_chunks) == ([], [])
    await stream.close()


@pytest.mark.asyncio
async def test_e2b_without_a_stdin_close_says_so(monkeypatch):
    runtime, sandbox, started = _e2b(monkeypatch, None)
    sandbox.commands._envd_version = Version("0.5.1")
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    assert await stream.write_eof() is False
    sandbox.commands.close_stdin.assert_not_awaited()
    await stream.close()


@pytest.mark.asyncio
async def test_output_left_unread_past_the_shared_budget_fails_the_stream(monkeypatch):
    monkeypatch.setattr(_streams, "MAX_BUFFERED", 10)
    runtime, _, started = _e2b(monkeypatch, None)
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    started["on_stdout"]("x" * 6)
    started["on_stderr"]("y" * 4)
    assert stream.returncode is None
    started["on_stderr"]("z")
    assert stream.returncode == -1
    assert stream.failure.startswith("more than 10 bytes")
    assert await stream.stdout.read() == b"x" * 6
    await asyncio.sleep(0.01)
    runtime.exec.assert_awaited_with("kill -KILL -- -4242", timeout=10)
    with pytest.raises(ConnectionError):
        await stream.write(b"x\n")
    await stream.close()


@pytest.mark.asyncio
async def test_a_write_after_the_end_is_a_connection_error(monkeypatch):
    runtime, _, _ = _e2b(monkeypatch, 0)
    stream = await runtime.open_stream("cmd")
    await stream.wait()
    with pytest.raises(ConnectionError):
        await stream.write(b"x")


# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_docker_reads_both_outputs_then_the_exit_status(monkeypatch):
    messages = iter(
        [
            SimpleNamespace(stream=2, data=MARK.encode()),
            SimpleNamespace(stream=1, data=b"out"),
            SimpleNamespace(stream=2, data=b"err"),
            None,
        ]
    )
    attach = MagicMock()
    attach.__aenter__ = AsyncMock(return_value=attach)
    attach.__aexit__ = AsyncMock(return_value=None)
    attach.read_out = AsyncMock(side_effect=lambda: next(messages))
    states = iter([{"Running": True}, {"Running": False, "ExitCode": 143}])
    exec_obj = MagicMock()
    exec_obj.start.return_value = attach
    exec_obj.inspect = AsyncMock(side_effect=lambda: next(states))
    container = MagicMock()
    container.exec = AsyncMock(return_value=exec_obj)
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    stream = await DockerRuntime(container, runtime_id="d", working_dir="/home/w").open_stream("cmd")
    assert await stream.wait() == 143
    assert await stream.stdout.read() == b"out"
    assert await stream.stderr.read() == b"err"
    kwargs = container.exec.call_args.kwargs
    assert (kwargs["stdin"], kwargs["workdir"]) == (True, "/home/w")
    attach.__aexit__.assert_not_awaited()
    await stream.close()
    attach.__aexit__.assert_awaited_once()


def _docker(messages, states):
    """A Docker runtime whose exec socket plays *messages* (then holds open)
    and whose exec inspect answers *states* (the last one from then on)."""
    pending = iter(messages)

    async def read_out():
        try:
            return next(pending)
        except StopIteration:
            await asyncio.Event().wait()

    async def inspect():
        state = states[0] if len(states) == 1 else states.pop(0)
        if isinstance(state, Exception):
            raise state
        return state

    attach = MagicMock()
    attach.__aenter__ = AsyncMock(return_value=attach)
    attach.__aexit__ = AsyncMock(return_value=None)
    attach.read_out = read_out
    exec_obj = MagicMock()
    exec_obj.start.return_value = attach
    exec_obj.inspect = inspect
    container = MagicMock()
    container.exec = AsyncMock(return_value=exec_obj)
    runtime = DockerRuntime(container, runtime_id="d", working_dir="/home/w")
    runtime._exec = AsyncMock(return_value=ExecResult(stdout="", stderr="", exit_code=0))
    return runtime


@pytest.mark.asyncio
async def test_docker_a_socket_closed_under_an_unconfirmed_exit_fails_and_kills(monkeypatch):
    monkeypatch.setattr(_docker_stream, "_STATUS_WAIT_S", 0.05)
    runtime = _docker([SimpleNamespace(stream=2, data=MARK.encode()), None], [{"Running": True}])
    stream = await runtime.open_stream("cmd")
    assert await asyncio.wait_for(stream.wait(), 1) == -1
    assert stream.failure == "its exec socket closed and its exit could not be confirmed"
    await asyncio.sleep(0.01)
    runtime._exec.assert_awaited_with("kill -KILL -- -4242", 10, workdir="/")
    await stream.close()


@pytest.mark.asyncio
async def test_docker_signals_from_the_root_not_the_working_dir(monkeypatch):
    monkeypatch.setattr(_streams, "_CLOSE_WAIT_S", 0.01)
    runtime = _docker([SimpleNamespace(stream=2, data=MARK.encode())], [{"Running": True}])
    stream = await runtime.open_stream("cmd")
    await stream.terminate()
    runtime._exec.assert_awaited_once_with("kill -TERM -- -4242", 10, workdir="/")
    await stream.close()


@pytest.mark.asyncio
async def test_docker_reads_bytes_that_are_not_utf8_as_replacement_characters():
    out = [b"a\xe4", b"\xb8\xad\xff\n", b"\xe4"]
    runtime = _docker(
        [SimpleNamespace(stream=2, data=MARK.encode())]
        + [SimpleNamespace(stream=1, data=chunk) for chunk in out]
        + [None],
        [{"Running": False, "ExitCode": 0}],
    )
    stream = await runtime.open_stream("cmd")
    assert await asyncio.wait_for(stream.wait(), 1) == 0
    assert await stream.stdout.read() == "a中\ufffd\n\ufffd".encode()
    await stream.close()


def test_holdback_refuses_the_write_that_takes_an_unfinished_line_past_the_limit(monkeypatch):
    monkeypatch.setattr(_streams, "_LINE_LIMIT", 8)
    holdback = LineHoldback()
    assert holdback.lines(b"abcd") == ""
    with pytest.raises(ValueError, match="longer than 8 bytes"):
        holdback.lines(b"efghi")
    assert holdback.lines(b"efgh\n") == "abcdefgh\n"
    assert holdback.lines(b"x" * 9 + b"\n" + b"y" * 8) == "x" * 9 + "\n"


@pytest.mark.asyncio
async def test_close_returns_in_its_deadline_when_no_stage_answers(monkeypatch):
    assert _streams._PID_WAIT_S < _streams._CLOSE_WAIT_S
    assert _streams._CLOSE_WAIT_S + _streams._RELEASE_WAIT_S < 15
    monkeypatch.setattr(_streams, "_CLOSE_WAIT_S", 0.05)
    monkeypatch.setattr(_streams, "_RELEASE_WAIT_S", 0.05)
    runtime, process = _daytona([])
    process.get_session_command_logs_async = _hang
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    process.exec = AsyncMock(side_effect=_hang)
    process.delete_session = AsyncMock(side_effect=_hang)
    stream = await runtime.open_stream("sleep 600")
    await asyncio.wait_for(stream.close(), 1)
    assert stream.failure == "it could not be killed: TimeoutError"
    assert stream.returncode == -1


def test_holdback_holds_many_tiny_writes_in_one_object():
    holdback = LineHoldback()
    for _ in range(100_000):
        holdback.lines(b"ab")
    held = [o for v in vars(holdback).values() for o in gc.get_referents(v)]
    assert sum(isinstance(o, (str, bytes, bytearray)) for o in held) < 10
    assert holdback.lines(b"\n") == "ab" * 100_000 + "\n"


@pytest.mark.asyncio
async def test_docker_an_inspect_that_fails_is_asked_again_before_the_deadline():
    runtime = _docker(
        [SimpleNamespace(stream=2, data=MARK.encode()), None],
        [ConnectionError("daemon busy"), {"Running": False, "ExitCode": 0}],
    )
    stream = await runtime.open_stream("cmd")
    assert await asyncio.wait_for(stream.wait(), 1) == 0
    assert stream.failure is None
    await stream.close()
    runtime._exec.assert_not_awaited()


def test_line_replay_holds_many_tiny_fragments_in_one_object():
    seen = LineReplay()
    for _ in range(100_000):
        assert seen.take("中") == ""
    assert seen.held == 3 * 100_000
    held = [o for v in vars(seen).values() for o in gc.get_referents(v)]
    assert sum(isinstance(o, (str, bytes, bytearray)) for o in held) < 10
    assert seen.take("\n") == "中" * 100_000 + "\n"


@pytest.mark.asyncio
async def test_close_settles_a_write_blocked_on_a_stalled_send(monkeypatch, no_reattach_delay):
    monkeypatch.setattr(_streams, "_CLOSE_WAIT_S", 0.05)
    runtime, process = _daytona([[("err", MARK), None]])
    process.get_session_command = AsyncMock(return_value=SimpleNamespace(exit_code=None))
    process.send_session_command_input = AsyncMock(side_effect=_hang)
    stream = await runtime.open_stream("cat")
    write = asyncio.create_task(stream.write(b"hi\n"))
    await asyncio.sleep(0.05)
    process.send_session_command_input.assert_awaited_once()
    await asyncio.wait_for(stream.close(), 1)
    with pytest.raises(ConnectionError, match="The stream was closed"):
        await asyncio.wait_for(write, 1)


@pytest.mark.asyncio
async def test_close_settles_a_write_eof_blocked_on_a_stalled_send(monkeypatch):
    runtime, sandbox, started = _e2b(monkeypatch, None)
    sandbox.commands.close_stdin = AsyncMock(side_effect=_hang)
    stream = await runtime.open_stream("cmd")
    started["on_stderr"](MARK)
    write_eof = asyncio.create_task(stream.write_eof())
    await asyncio.sleep(0.05)
    sandbox.commands.close_stdin.assert_awaited_once()
    await asyncio.wait_for(stream.close(), 1)
    with pytest.raises(ConnectionError, match="The stream was closed"):
        await asyncio.wait_for(write_eof, 1)


@pytest.mark.asyncio
async def test_docker_a_hung_inspect_does_not_outlast_the_status_window(monkeypatch):
    monkeypatch.setattr(_docker_stream, "_STATUS_WAIT_S", 0.05)
    runtime = _docker([SimpleNamespace(stream=2, data=MARK.encode()), None], [{"Running": False}])
    runtime.raw.exec.return_value.inspect = _hang
    stream = await runtime.open_stream("cmd")
    assert await asyncio.wait_for(stream.wait(), 1) == -1
    assert stream.failure == "its exec socket closed and its exit could not be confirmed"
    await stream.close()


@pytest.mark.asyncio
async def test_docker_a_write_after_its_stdin_closed_is_refused_and_the_process_runs_on(
    monkeypatch,
):
    monkeypatch.setattr(_streams, "_CLOSE_WAIT_S", 0.01)
    runtime = _docker([SimpleNamespace(stream=2, data=MARK.encode())], [{"Running": True}])
    attach = runtime.raw.exec.return_value.start.return_value
    attach.write_in = AsyncMock(side_effect=RuntimeError("Cannot write to closing transport"))
    stream = await runtime.open_stream("cmd")
    assert await stream.write_eof() is True
    with pytest.raises(ConnectionError, match="Its stdin is closed"):
        await stream.write(b"more\n")
    assert await stream.write_eof() is True
    assert stream.failure is None and stream.returncode is None
    attach.write_in.assert_not_awaited()
    runtime._exec.assert_not_awaited()
    await stream.close()
