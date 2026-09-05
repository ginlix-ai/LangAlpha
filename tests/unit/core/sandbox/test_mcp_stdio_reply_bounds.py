"""The stdio reply reader must be bounded like the HTTP one.

A stdio server is already an arbitrary process in the sandbox, so this is about
staying diagnosable, not about trust: unbounded, the pump thread accumulates a
flooding server's output into an unbounded queue until the interpreter is
OOM-killed with nothing to report. The bound is reachable from background
discovery too, which spawns through the same path.
"""

import json
import queue
import threading
import time

import pytest

from ptc_agent.core.sandbox import mcp_client_runtime as m

# A skippable notification (no id), so a flood exercises the size bound rather
# than tripping the invalid-framing path first. 33 chars with the newline.
_NOISE = '{"jsonrpc":"2.0","method":"log"}\n'
_SMALL_CAP = 64  # two _NOISE lines breach it, one does not


class _CountingLines:
    """A capped-readline source that records how far the pump actually read.

    ``consumed`` counts source lines the reader has started on; ``remaining``
    is what a reader that stopped early left unread.
    """

    def __init__(self, lines):
        self._lines = list(lines)
        self.consumed = 0
        self._buf = ""

    def readline(self, size=-1):
        if not self._buf:
            if not self._lines:
                return ""
            self._buf = self._lines.pop(0)
            self.consumed += 1
        nl = self._buf.find("\n")
        end = len(self._buf) if nl < 0 else nl + 1
        if size is not None and size >= 0:
            end = min(end, size)
        piece, self._buf = self._buf[:end], self._buf[end:]
        return piece

    @property
    def remaining(self):
        return len(self._buf) + sum(len(line) for line in self._lines)


class _Sink:
    def __init__(self):
        self.written = []

    def write(self, data):
        self.written.append(data)

    def flush(self):
        pass


class _FakeProc:
    """The subset of subprocess.Popen the spawn/read path touches."""

    def __init__(self, stdout_lines=(), stderr_lines=()):
        self.stdout = _CountingLines(stdout_lines)
        self.stderr = _CountingLines(stderr_lines)
        self.stdin = _Sink()
        self.killed = False

    def kill(self):
        self.killed = True

    def poll(self):
        return None


class _FakeSubprocess:
    """Stands in for the ``subprocess`` module; every spawn gets a fresh proc."""

    PIPE = -1

    def __init__(self, stdout_lines=(), stderr_lines=()):
        self._stdout_lines = list(stdout_lines)
        self._stderr_lines = list(stderr_lines)
        self.procs = []

    def Popen(self, cmd, **kwargs):  # noqa: N802 - mirrors subprocess.Popen
        proc = _FakeProc(self._stdout_lines, self._stderr_lines)
        self.procs.append(proc)
        return proc


@pytest.fixture
def configured(monkeypatch):
    """One builtin stdio server, cold registries."""
    monkeypatch.setattr(
        m,
        "_SERVER_CONFIGS",
        {
            "srv": m._normalize(
                "srv",
                {"transport": "stdio", "untrusted": False, "command": "/bin/true"},
            )
        },
    )
    monkeypatch.setattr(m, "_PROTO", {})
    monkeypatch.setattr(m, "_server_processes", {})


def _install(monkeypatch, *, stdout=(), stderr=()) -> _FakeSubprocess:
    fake = _FakeSubprocess(stdout_lines=stdout, stderr_lines=stderr)
    monkeypatch.setattr(m, "subprocess", fake)
    return fake


def _drain_to_terminator(q, timeout=2.0):
    """Return (lines, terminator) — the pump ends on the sentinel or EOF."""
    lines = []
    while True:
        item = q.get(timeout=timeout)
        if item is None or item is m._STDIO_OVERSIZE:
            return lines, item
        lines.append(item)


def _wait_until(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


class TestPumpBound:
    def test_a_stdout_flood_ends_in_the_oversize_sentinel(self, configured, monkeypatch):
        monkeypatch.setattr(m, "_REPLY_MAX_BYTES", _SMALL_CAP)
        fake = _install(monkeypatch, stdout=[_NOISE] * 500)

        proc = m._spawn_mcp_process("srv")
        lines, terminator = _drain_to_terminator(proc.mcp_stdout_queue)

        assert terminator is m._STDIO_OVERSIZE
        assert lines == [_NOISE]  # everything under the cap still got through
        assert proc.stdout.consumed == 2  # pumping stopped at the breach
        assert proc.mcp_stdout_queue.empty()  # no EOF sentinel after it
        assert fake.procs == [proc]

    def test_under_cap_traffic_reaches_the_reader_unchanged(self, configured, monkeypatch):
        monkeypatch.setattr(m, "_REPLY_MAX_BYTES", _SMALL_CAP)
        reply = '{"jsonrpc":"2.0","id":7,"result":{"ok":true}}\n'
        _install(monkeypatch, stdout=[reply])

        proc = m._spawn_mcp_process("srv")
        assert m._read_reply("srv", proc, 7, 5.0)["result"] == {"ok": True}
        assert proc.mcp_stdout_queue.get(timeout=2) is None  # plain EOF
        assert not proc.killed

    def test_consumed_replies_retire_from_the_budget(self, configured, monkeypatch):
        """The cap bounds UNCONSUMED backlog, not lifetime volume: replies the
        reader has already taken off the queue must free their bytes, or any
        long session of ordinary calls eventually murders a healthy server."""
        monkeypatch.setattr(m, "_REPLY_MAX_BYTES", _SMALL_CAP)
        r1 = '{"jsonrpc":"2.0","id":1,"result":1}\n'
        r2 = '{"jsonrpc":"2.0","id":2,"result":2}\n'
        assert len(r1) < _SMALL_CAP < len(r1) + len(r2)  # only the SUM breaches

        paced = _PacedLines([r1, r2])

        class _Sub:
            PIPE = -1

            @staticmethod
            def Popen(cmd, **kwargs):  # noqa: N802 - mirrors subprocess.Popen
                proc = _FakeProc()
                proc.stdout = paced
                return proc

        monkeypatch.setattr(m, "subprocess", _Sub())
        proc = m._spawn_mcp_process("srv")

        assert m._read_reply("srv", proc, 1, 5.0)["result"] == 1
        paced.gate.set()  # release r2 only after r1 was consumed
        assert m._read_reply("srv", proc, 2, 5.0)["result"] == 2
        assert not proc.killed
        assert proc.mcp_stdout_queue.get(timeout=2) is None  # clean EOF, no sentinel

    def test_a_flood_that_never_newlines_still_trips_the_cap(self, configured, monkeypatch):
        """The budget must charge partially-assembled lines: charging only on
        the newline lets a \\r progress meter grow one line without bound —
        the pump OOMs exactly where it was meant to protect."""
        monkeypatch.setattr(m, "_REPLY_MAX_BYTES", _SMALL_CAP)
        monkeypatch.setattr(m, "_STDIO_CHUNK_CHARS", 16)
        source = _EndlessUnterminated()

        class _Sub:
            PIPE = -1

            @staticmethod
            def Popen(cmd, **kwargs):  # noqa: N802 - mirrors subprocess.Popen
                proc = _FakeProc()
                proc.stdout = source
                return proc

        monkeypatch.setattr(m, "subprocess", _Sub())
        proc = m._spawn_mcp_process("srv")

        assert proc.mcp_stdout_queue.get(timeout=2) is m._STDIO_OVERSIZE
        assert source.pieces < 4096  # stopped by the budget, not the fake's floor


class _EndlessUnterminated:
    """A stdout stream stuck mid-line: pieces flow, the newline never comes.

    Line ITERATION over it blocks forever — the pre-fix shape, where nothing
    was charged until a line completed.
    """

    def __init__(self):
        self.pieces = 0

    def readline(self, size=-1):
        self.pieces += 1
        if self.pieces > 4096:
            return ""  # let the daemon pump wind down once the test is over
        return "y" * 16

    def __iter__(self):
        threading.Event().wait()


class _PacedLines:
    """A line source that holds each line until the test opens the gate."""

    def __init__(self, lines):
        self._lines = list(lines)
        self.gate = threading.Event()
        self.gate.set()  # the first line flows immediately
        self._buf = ""

    def readline(self, size=-1):
        if not self._buf:
            if not self._lines:
                return ""
            assert self.gate.wait(2.0), "test never released the next line"
            self.gate.clear()
            self._buf = self._lines.pop(0)
        nl = self._buf.find("\n")
        end = len(self._buf) if nl < 0 else nl + 1
        if size is not None and size >= 0:
            end = min(end, size)
        piece, self._buf = self._buf[:end], self._buf[end:]
        return piece


class TestReaderRefusal:
    def test_the_sentinel_kills_the_server_and_names_the_cause(self, configured):
        proc = _FakeProc()
        proc.mcp_stdout_queue = queue.Queue()
        proc.mcp_stdout_queue.put(m._STDIO_OVERSIZE)
        m._server_processes["srv"] = proc
        m._PROTO["srv"] = {"mode": "legacy", "version": "2025-11-25", "session_id": None}

        with pytest.raises(RuntimeError, match="reply_too_large"):
            m._read_reply("srv", proc, 1, 5.0)

        assert proc.killed
        assert "srv" not in m._server_processes  # next call respawns
        assert "srv" not in m._PROTO

    def test_both_transports_report_the_same_tag(self, configured):
        proc = _FakeProc()
        proc.mcp_stdout_queue = queue.Queue()
        proc.mcp_stdout_queue.put(m._STDIO_OVERSIZE)
        with pytest.raises(RuntimeError) as stdio_err:
            m._read_reply("srv", proc, 1, 5.0)

        with pytest.raises(RuntimeError) as http_err:
            m._parse_http_reply(_OversizeBody(), 1, "srv")

        assert "[reply_too_large]" in str(stdio_err.value)
        assert "[reply_too_large]" in str(http_err.value)

    def test_a_request_flood_is_killed_at_the_refusal_cap(self, configured):
        # The refusal write is the reader's one blocking stdin operation: a
        # server that streams requests while never draining stdin would wedge
        # the writer against a full pipe with the per-server lock held.
        proc = _FakeProc()
        proc.mcp_stdout_queue = queue.Queue()
        proc.mcp_budget_lock = threading.Lock()
        proc.mcp_outstanding = [10**9]
        for i in range(m._REFUSAL_MAX + 5):
            proc.mcp_stdout_queue.put(
                json.dumps({"jsonrpc": "2.0", "id": i, "method": "roots/list"})
            )
        m._server_processes["srv"] = proc

        with pytest.raises(RuntimeError, match=r"\[request_flood\]"):
            m._read_reply("srv", proc, 10**6, 5.0)

        assert proc.killed
        assert "srv" not in m._server_processes
        assert len(proc.stdin.written) == m._REFUSAL_MAX


class _OversizeBody:
    """A streamed httpx response whose body blows past the shared cap."""

    headers = {"content-type": "application/json"}

    def iter_bytes(self):
        yield b"x" * (m._REPLY_MAX_BYTES + 1)


class TestStderrTailBound:
    def test_a_giant_stderr_line_is_truncated_head_first(self, configured, monkeypatch):
        monkeypatch.setattr(m, "_STDERR_LINE_MAX_BYTES", 32)
        _install(monkeypatch, stderr=["boom " * 100_000 + "\n", "short\n"])

        proc = m._spawn_mcp_process("srv")
        assert _wait_until(lambda: len(proc.mcp_stderr_tail) == 2)

        tail = list(proc.mcp_stderr_tail)
        assert [len(line) for line in tail] == [32, 5]
        assert tail[0].startswith("boom ")  # head kept, not the tail end

    def test_a_line_that_never_completes_still_yields_its_head(self, configured, monkeypatch):
        """The crash tail must fill while the flood is still running: waiting
        for the newline leaves the tail empty (and the assembling line
        unbounded) right when a crash report needs it."""
        monkeypatch.setattr(m, "_STDERR_LINE_MAX_BYTES", 32)
        source = _StuckMidLine()

        class _Sub:
            PIPE = -1

            @staticmethod
            def Popen(cmd, **kwargs):  # noqa: N802 - mirrors subprocess.Popen
                proc = _FakeProc()
                proc.stderr = source
                return proc

        monkeypatch.setattr(m, "subprocess", _Sub())
        proc = m._spawn_mcp_process("srv")

        assert _wait_until(lambda: len(proc.mcp_stderr_tail) == 1)
        assert _wait_until(lambda: source.pieces > 10)
        # The head landed once; the rest of the endless line is dropped.
        assert list(proc.mcp_stderr_tail) == ["spew spew spew! "]


class _StuckMidLine:
    """A stderr stream stuck mid-line; line iteration would block forever."""

    def __init__(self):
        self.pieces = 0

    def readline(self, size=-1):
        self.pieces += 1
        if self.pieces > 4096:
            return ""  # let the daemon drain wind down once the test is over
        return "spew spew spew! "  # 16 chars, never a newline

    def __iter__(self):
        threading.Event().wait()


class TestDiscoveryPath:
    """Discovery auto-fires at session resolve and spawns through the same path."""

    def test_a_flooding_server_is_reported_not_absorbed(self, configured, monkeypatch):
        monkeypatch.setattr(m, "_REPLY_MAX_BYTES", _SMALL_CAP)
        fake = _install(monkeypatch, stdout=[_NOISE] * 500)

        result = m.discover("srv")

        assert result["status"] == "error"
        assert "reply_too_large" in result["error"]
        assert all(p.killed for p in fake.procs)  # nothing left flooding
