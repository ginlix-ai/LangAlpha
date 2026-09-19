"""The MCP supervisor's wire contract over a real AF_UNIX socket.

What is worth locking only shows up on the wire: the non-terminal ack before
exactly one terminal frame, ids that stay with their own caller while two calls
are in flight, and the refusals a client must not retry against its own config.
A fake ``ServerBackend`` stands in for the client runtime, so framing and policy
are what is under test rather than a subprocess.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import tempfile
import threading
import time
from typing import Any

import pytest

from ptc_agent.core.sandbox.supervisor_runtime import daemon as d
from ptc_agent.core.sandbox.supervisor_runtime import protocol as p

#: Every read is bounded, so a framing regression fails the test instead of
#: parking the suite on a blocking recv.
READ_TIMEOUT_S = 5.0
#: ``_ServerStat.last_used`` starts at 0.0, so the reaper measures a server with
#: no recorded call from the epoch, and any window under ~1.8e9 s reaps it on
#: the first tick. Park the window past that to keep reaping out of these tests.
IDLE_WINDOW_S = 1e12
#: No heartbeat within a test's lifetime; the readers tolerate one regardless.
HEARTBEAT_S = 3600.0

# Read the payload key off the builder so a rename in protocol.py moves this
# file with it rather than surfacing as a missing key.
_REPLY_KEY = next(
    key
    for key, value in p.reply_frame(0, {"probe": 1}, cold=False, elapsed_ms=0.0).items()
    if value == {"probe": 1}
)


class FakeBackend(d.ServerBackend):
    """In-memory stand-in for the client runtime: no processes, same seam."""

    def __init__(self, names, *, config_version=0):
        self._names = set(names)
        self._running: set[str] = set()
        self._lock = threading.Lock()
        self._config_version = config_version
        self.fingerprint: Any = "secret-0"
        self.error: Exception | None = None
        self.gate: threading.Event | None = None
        self.gates: dict[str, threading.Event] = {}
        #: Released on entry to ``call``, so a test can prove a call is in
        #: flight without polling for it.
        self.entered = threading.Semaphore(0)
        self.drops: list[str] = []
        self.dependencies: dict[str, set[str] | None] = {}
        self.calls: list[tuple[str, str]] = []
        self.timeouts: list[tuple[str, float]] = []

    def names(self):
        return set(self._names)

    def is_running(self, server):
        with self._lock:
            return server in self._running

    def running(self):
        with self._lock:
            return sorted(self._running)

    def call(self, server, tool, args):
        self.calls.append((server, tool))
        self.entered.release()
        gate = self.gates.get(server, self.gate)
        if gate is not None:
            gate.wait(READ_TIMEOUT_S)
        if self.error is not None:
            raise self.error
        with self._lock:
            self._running.add(server)
        return {
            "jsonrpc": "2.0",
            "result": {"server": server, "tool": tool, "args": args},
        }

    def call_with_timeout(self, server, tool, args, timeout):
        self.timeouts.append((server, timeout))
        return self.call(server, tool, args)

    def drop(self, server):
        with self._lock:
            self._running.discard(server)
        self.drops.append(server)

    def secret_fingerprint(self):
        return self.fingerprint

    def secret_dependencies(self, server):
        return self.dependencies.get(server)

    def config_version(self):
        return self._config_version

    def shutdown(self):
        pass


class _Client:
    """One connection: framed writes, bounded reads, buffered line splitting."""

    def __init__(self, path):
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.settimeout(READ_TIMEOUT_S)
        self._sock.connect(path)
        self._buf = b""

    def send(self, request):
        self._sock.sendall(p.encode(request))

    def frame(self):
        while b"\n" not in self._buf:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise AssertionError("connection closed before a terminal frame")
            self._buf += chunk
        line, self._buf = self._buf.split(b"\n", 1)
        return p.decode(line)

    def frames(self):
        """Every frame up to and including the terminal one."""
        collected = []
        while True:
            collected.append(self.frame())
            if collected[-1]["type"] in p.TERMINAL_TYPES:
                return collected

    def exchange(self, request):
        self.send(request)
        return self.frames()

    def close(self):
        self._sock.close()


class _Harness:
    def __init__(self, supervisor):
        self.supervisor = supervisor
        self.clients: list[_Client] = []

    def connect(self):
        self.clients.append(_Client(self.supervisor.socket_path))
        return self.clients[-1]


@pytest.fixture
def start_supervisor():
    """Bind a Supervisor, serve it from a daemon thread, tear it down after."""
    running: list[tuple[_Harness, threading.Thread, str]] = []

    def _start(backend, *, root=None, **options):
        # pytest's tmp_path runs ~120 chars on macOS while AF_UNIX caps sun_path
        # at 104, so the socket gets its own short directory.
        directory = tempfile.mkdtemp(prefix="mcpsup-")
        supervisor = d.Supervisor(
            os.path.join(directory, "s.sock"),
            backend,
            root=root,
            idle_window_s=IDLE_WINDOW_S,
            heartbeat_s=HEARTBEAT_S,
            log=lambda message: None,
            **options,
        )
        supervisor.bind()
        thread = threading.Thread(target=supervisor.serve_forever, daemon=True)
        thread.start()
        harness = _Harness(supervisor)
        running.append((harness, thread, directory))
        return harness

    yield _start

    for harness, thread, directory in running:
        for client in harness.clients:
            client.close()
        _stop_serving(harness.supervisor)
        thread.join(timeout=READ_TIMEOUT_S)
        assert not thread.is_alive()
        shutil.rmtree(directory, ignore_errors=True)


def _stop_serving(supervisor):
    """Stop over the wire, then close."""
    try:
        client = _Client(supervisor.socket_path)
        try:
            client.exchange(p.simple_request(0, p.OP_SHUTDOWN))
        finally:
            client.close()
        # The stop flag is only read between accepts, so one more connection
        # returns the blocked accept now instead of after its one second poll.
        _Client(supervisor.socket_path).close()
    except OSError:
        pass  # already stopped: the stale-daemon refusal stops the daemon itself
    supervisor.close()


def _write_config(tmp_path, payload):
    path = tmp_path / "mcp_client_config.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _call(request_id, server, **kwargs):
    kwargs.setdefault("tool", "quote")
    kwargs.setdefault("args", {})
    return p.call_request(request_id, server=server, **kwargs)


def _wait_until(predicate, timeout=READ_TIMEOUT_S):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition did not become true")


def test_call_emits_ack_then_reply_and_flips_cold(start_supervisor):
    backend = FakeBackend({"alpha"})
    client = start_supervisor(backend).connect()
    args = {"nested": {"values": [1, 2.5, None]}, "text": "ü"}

    ack, reply = client.exchange(_call(1, "alpha", args=args))

    assert [ack["type"], reply["type"]] == [p.TYPE_ACK, p.TYPE_REPLY]
    assert (ack["id"], ack["server"], ack["cold"]) == (1, "alpha", True)
    assert reply["id"] == 1
    # The backend's JSON-RPC dict is relayed whole; the daemon settles nothing.
    assert reply[_REPLY_KEY] == {
        "jsonrpc": "2.0",
        "result": {"server": "alpha", "tool": "quote", "args": args},
    }
    assert "elapsed_ms" in reply
    assert reply["elapsed_ms"] >= 0.0

    second = client.exchange(_call(2, "alpha"))

    assert [f["type"] for f in second] == [p.TYPE_ACK, p.TYPE_REPLY]
    assert second[0]["cold"] is False
    assert second[1]["cold"] is False


def test_two_calls_in_flight_keep_their_own_ids(start_supervisor):
    backend = FakeBackend({"alpha", "beta"})
    backend.gate = threading.Event()
    harness = start_supervisor(backend)
    first, second = harness.connect(), harness.connect()

    first.send(_call(11, "alpha", tool="one"))
    second.send(_call(22, "beta", tool="two"))
    # Both are inside backend.call before either is allowed to return.
    assert backend.entered.acquire(timeout=READ_TIMEOUT_S)
    assert backend.entered.acquire(timeout=READ_TIMEOUT_S)
    backend.gate.set()

    first_frames, second_frames = first.frames(), second.frames()

    assert [f["type"] for f in first_frames] == [p.TYPE_ACK, p.TYPE_REPLY]
    assert [f["type"] for f in second_frames] == [p.TYPE_ACK, p.TYPE_REPLY]
    assert {f["id"] for f in first_frames} == {11}
    assert {f["id"] for f in second_frames} == {22}
    assert first_frames[-1][_REPLY_KEY]["result"]["tool"] == "one"
    assert second_frames[-1][_REPLY_KEY]["result"]["tool"] == "two"


def test_queue_deadline_includes_the_server_lock_wait(start_supervisor):
    backend = FakeBackend({"alpha"})
    backend.gate = threading.Event()
    harness = start_supervisor(backend, call_timeout_s=0.15)
    active, queued = harness.connect(), harness.connect()
    active.send(_call(1, "alpha", tool="active"))
    assert backend.entered.acquire(timeout=READ_TIMEOUT_S)

    frames = queued.exchange(_call(2, "alpha", tool="queued"))

    assert [frame["type"] for frame in frames] == [p.TYPE_ERROR]
    assert frames[0]["code"] == "queue_timeout"
    assert backend.calls == [("alpha", "active")]
    backend.gate.set()
    assert active.frames()[-1]["type"] == p.TYPE_REPLY


def test_server_queue_rejects_beyond_its_bound(start_supervisor):
    backend = FakeBackend({"alpha"})
    backend.gate = threading.Event()
    harness = start_supervisor(backend, max_server_waiters=1)
    active, queued, rejected = (
        harness.connect(),
        harness.connect(),
        harness.connect(),
    )
    active.send(_call(1, "alpha", tool="active"))
    assert backend.entered.acquire(timeout=READ_TIMEOUT_S)
    queued.send(_call(2, "alpha", tool="queued"))
    queue_state = harness.supervisor._queue_for("alpha")
    _wait_until(lambda: queue_state.waiters == 1)

    frames = rejected.exchange(_call(3, "alpha", tool="rejected"))

    assert [frame["type"] for frame in frames] == [p.TYPE_ERROR]
    assert frames[0]["code"] == "server_busy"
    backend.gate.set()
    assert active.frames()[-1]["type"] == p.TYPE_REPLY
    assert queued.frames()[-1]["type"] == p.TYPE_REPLY


def test_queued_calls_do_not_consume_global_dispatch_slots(start_supervisor):
    backend = FakeBackend({"alpha", "beta"})
    backend.gates["alpha"] = threading.Event()
    harness = start_supervisor(backend, max_inflight=2)
    active = harness.connect()
    active.send(_call(1, "alpha", tool="active"))
    assert backend.entered.acquire(timeout=READ_TIMEOUT_S)

    queued = [harness.connect() for _ in range(4)]
    for request_id, client in enumerate(queued, start=10):
        client.send(_call(request_id, "alpha", tool="queued"))
    queue_state = harness.supervisor._queue_for("alpha")
    _wait_until(lambda: queue_state.waiters == len(queued))

    other = harness.connect().exchange(_call(30, "beta", tool="other"))

    assert other[-1]["type"] == p.TYPE_REPLY
    assert ("beta", "other") in backend.calls
    for client in queued:
        client.close()
    backend.gates["alpha"].set()
    assert active.frames()[-1]["type"] == p.TYPE_REPLY


def test_disconnect_cancels_a_call_while_it_is_queued(start_supervisor):
    backend = FakeBackend({"alpha"})
    backend.gate = threading.Event()
    harness = start_supervisor(backend)
    active, queued = harness.connect(), harness.connect()
    active.send(_call(1, "alpha", tool="active"))
    assert backend.entered.acquire(timeout=READ_TIMEOUT_S)
    queued.send(_call(2, "alpha", tool="cancelled"))
    queue_state = harness.supervisor._queue_for("alpha")
    _wait_until(lambda: queue_state.waiters == 1)

    queued.close()
    _wait_until(lambda: queue_state.waiters == 0)
    backend.gate.set()

    assert active.frames()[-1]["type"] == p.TYPE_REPLY
    assert backend.calls == [("alpha", "active")]


def test_server_missing_from_the_config_is_not_enabled(start_supervisor, tmp_path):
    harness = start_supervisor(FakeBackend({"alpha", "beta"}))
    config = _write_config(tmp_path, {"workspace_id": "ws-1", "servers": {"alpha": {}}})

    frames = harness.connect().exchange(
        _call(1, "beta", workspace_id="ws-1", config_path=config)
    )

    assert [f["type"] for f in frames] == [p.TYPE_ERROR]
    assert frames[0]["code"] == p.ERR_NOT_ENABLED
    assert frames[0]["message"] == "MCP server 'beta' is not enabled for this workspace"
    assert p.ERR_NOT_ENABLED in p.FATAL_CODES


def test_unknown_server_is_refused_before_any_ack(start_supervisor):
    backend = FakeBackend({"alpha"})
    harness = start_supervisor(backend)

    frames = harness.connect().exchange(_call(1, "ghost"))

    assert [f["type"] for f in frames] == [p.TYPE_ERROR]
    assert frames[0]["code"] == p.ERR_UNKNOWN_SERVER
    assert p.ERR_UNKNOWN_SERVER in p.FATAL_CODES
    assert backend.entered.acquire(blocking=False) is False


def test_config_for_another_workspace_is_a_mismatch(start_supervisor, tmp_path):
    harness = start_supervisor(FakeBackend({"alpha"}))
    config = _write_config(
        tmp_path, {"workspace_id": "ws-other", "servers": {"alpha": {}}}
    )

    frames = harness.connect().exchange(
        _call(1, "alpha", workspace_id="ws-mine", config_path=config)
    )

    assert [f["type"] for f in frames] == [p.TYPE_ERROR]
    assert frames[0]["code"] == p.ERR_CONFIG_MISMATCH
    assert "ws-other" in frames[0]["message"]
    assert p.ERR_CONFIG_MISMATCH in p.FATAL_CODES


def test_caller_config_newer_than_the_daemon_is_stale(start_supervisor, tmp_path):
    harness = start_supervisor(FakeBackend({"alpha"}, config_version=3))
    # The version compared is the one in the caller's config file on disk, not
    # the config_version field call_request puts in the request.
    config = _write_config(
        tmp_path,
        {"workspace_id": "ws", "servers": {"alpha": {}}, "computer_config_version": 7},
    )

    frames = harness.connect().exchange(
        _call(1, "alpha", workspace_id="ws", config_path=config, config_version=7)
    )

    assert [f["type"] for f in frames] == [p.TYPE_ERROR]
    assert frames[0]["code"] == "draining"
    _wait_until(harness.supervisor._stop.is_set)


def test_stale_daemon_drains_inflight_and_refuses_new_calls(start_supervisor, tmp_path):
    backend = FakeBackend({"alpha"}, config_version=3)
    backend.gate = threading.Event()
    harness = start_supervisor(backend, drain_timeout_s=1.0)
    active = harness.connect()
    active.send(_call(1, "alpha", tool="active"))
    assert backend.entered.acquire(timeout=READ_TIMEOUT_S)
    config = _write_config(
        tmp_path,
        {"workspace_id": "ws", "servers": {"alpha": {}}, "computer_config_version": 7},
    )

    stale = harness.connect().exchange(
        _call(2, "alpha", workspace_id="ws", config_path=config)
    )
    refused = harness.connect().exchange(_call(3, "alpha"))

    assert stale[0]["code"] == "draining"
    assert refused[0]["code"] == "draining"
    assert not harness.supervisor._stop.is_set()
    backend.gate.set()
    assert active.frames()[-1]["type"] == p.TYPE_REPLY
    _wait_until(harness.supervisor._stop.is_set)


def test_absent_config_file_means_the_union(start_supervisor, tmp_path):
    harness = start_supervisor(FakeBackend({"alpha"}))
    missing = str(tmp_path / "never-written.json")

    frames = harness.connect().exchange(
        _call(1, "alpha", workspace_id="ws-1", config_path=missing)
    )

    assert [f["type"] for f in frames] == [p.TYPE_ACK, p.TYPE_REPLY]


def test_health_reports_the_known_servers(start_supervisor):
    backend = FakeBackend({"beta", "alpha"})
    harness = start_supervisor(backend)

    frames = harness.connect().exchange(p.simple_request(9, p.OP_HEALTH))

    assert [f["type"] for f in frames] == [p.TYPE_RESULT]
    assert p.TYPE_RESULT in p.TERMINAL_TYPES
    data = frames[0]["data"]
    assert data["ok"] is True
    assert data["pid"] == os.getpid()
    assert data["config_version"] == 0
    assert data["known_servers"] == sorted(backend.names())
    assert data["servers"] == {}


def test_backend_failure_is_transport_and_the_daemon_keeps_serving(start_supervisor):
    backend = FakeBackend({"alpha"})
    backend.error = RuntimeError("upstream died")
    harness = start_supervisor(backend)

    frames = harness.connect().exchange(_call(1, "alpha"))

    assert [f["type"] for f in frames] == [p.TYPE_ACK, p.TYPE_ERROR]
    assert frames[-1]["code"] == p.ERR_TRANSPORT
    assert "RuntimeError" in frames[-1]["message"]

    backend.error = None
    later = harness.connect().exchange(_call(2, "alpha"))

    assert later[-1]["type"] == p.TYPE_REPLY
    assert later[-1][_REPLY_KEY]["result"]["tool"] == "quote"


def test_secret_rotation_drops_running_servers_and_recools(start_supervisor):
    backend = FakeBackend({"alpha"})
    client = start_supervisor(backend).connect()

    first = client.exchange(_call(1, "alpha"))
    assert first[0]["cold"] is True
    assert backend.running() == ["alpha"]

    backend.fingerprint = "secret-1"
    second = client.exchange(_call(2, "alpha"))

    assert backend.drops == ["alpha"]
    assert second[0]["cold"] is True
    assert second[-1]["cold"] is True


def test_secret_rotation_drops_only_dependent_servers(start_supervisor):
    backend = FakeBackend({"alpha", "beta", "public"})
    backend.fingerprint = [
        ["/secrets/x", 1, 10],
        ["/secrets/y", 1, 10],
    ]
    backend.dependencies = {
        "alpha": {"/secrets/x"},
        "beta": {"/secrets/y"},
        "public": set(),
    }
    client = start_supervisor(backend).connect()
    for request_id, server in enumerate(("alpha", "beta", "public"), start=1):
        client.exchange(_call(request_id, server))

    backend.fingerprint = [
        ["/secrets/x", 2, 11],
        ["/secrets/y", 1, 10],
    ]
    frames = client.exchange(_call(4, "public"))

    assert backend.drops == ["alpha"]
    assert frames[-1]["type"] == p.TYPE_REPLY
    assert backend.running() == ["beta", "public"]


def test_secret_rotation_waits_for_a_dependent_inflight_call(start_supervisor):
    backend = FakeBackend({"alpha", "beta"})
    backend.fingerprint = [["/secrets/x", 1, 10]]
    backend.dependencies = {"alpha": {"/secrets/x"}, "beta": set()}
    harness = start_supervisor(backend)
    assert harness.connect().exchange(_call(1, "alpha"))[-1]["type"] == p.TYPE_REPLY
    assert backend.entered.acquire(timeout=READ_TIMEOUT_S)
    backend.gates["alpha"] = threading.Event()
    active = harness.connect()
    active.send(_call(2, "alpha", tool="active"))
    assert backend.entered.acquire(timeout=READ_TIMEOUT_S)
    backend.fingerprint = [["/secrets/x", 2, 10]]

    assert harness.connect().exchange(_call(3, "beta"))[-1]["type"] == p.TYPE_REPLY
    assert backend.drops == []
    backend.gates["alpha"].set()

    assert active.frames()[-1]["type"] == p.TYPE_REPLY
    assert backend.drops == ["alpha"]


def test_encode_decode_round_trip_stays_one_line():
    frame = p.reply_frame(
        7,
        {"jsonrpc": "2.0", "result": {"text": "line1\nline2", "n": [1, 2.5, None]}},
        cold=True,
        elapsed_ms=12.3456,
    )

    raw = p.encode(frame)

    assert raw.endswith(b"\n")
    # The payload's own newline is escaped, so the line stays the framing unit.
    assert raw.count(b"\n") == 1
    assert p.decode(raw) == frame


def test_frame_constants_are_frozen():
    assert p.TYPE_REPLY in p.TERMINAL_TYPES
    assert p.TYPE_RESULT in p.TERMINAL_TYPES
    assert p.TYPE_ERROR in p.TERMINAL_TYPES
    assert p.TYPE_ACK not in p.TERMINAL_TYPES
    assert p.TYPE_HEARTBEAT not in p.TERMINAL_TYPES
    # Literals, not references: the sandbox side runs from a shipped copy, so a
    # rename has to be a deliberate edit at both ends.
    assert p.VERSION == 1
    assert p.SOCKET_REL_PATH == "_internal/system/mcp-supervisor.sock"
    assert p.PACKAGE_NAME == "supervisor"


def test_the_sandbox_client_carries_the_same_wire_constants():
    """The client hand-builds its frames so a wrapper never imports the daemon.

    That duplication is the point, and this is what keeps it honest: the two
    copies are asserted equal here rather than at runtime in the sandbox.
    """
    from ptc_agent.core.sandbox import mcp_client_runtime as client

    assert client._SUPERVISOR_PROTOCOL_VERSION == p.VERSION
    assert client._SUPERVISOR_PACKAGE == p.PACKAGE_NAME
    assert p.SOCKET_REL_PATH.endswith("/" + client._SUPERVISOR_SOCKET_REL)
    assert p.LOG_REL_PATH.endswith("/" + client._SUPERVISOR_LOG_REL)
    assert set(client._SUPERVISOR_ERR_FATAL) == set(p.FATAL_CODES)


# ---------------------------------------------------------------------------
# The client half of the same contract: id uniqueness and the ack-gated retry.
# ---------------------------------------------------------------------------


class TestClientRequestIdentity:
    """One daemon serves every interpreter on the computer.

    The id used to be a per-interpreter counter, so two concurrent
    ``execute_code`` runs both started at 1 and collided in the daemon's
    in-flight table: one reply reached the wrong waiter and the other request
    was dropped.
    """

    def test_the_id_carries_the_process(self):
        from ptc_agent.core.sandbox import mcp_client_runtime as c

        c._message_id_counter = 0
        first, second = c._next_supervisor_id(), c._next_supervisor_id()
        assert first != second
        assert first >> 32 == os.getpid()
        # Same counter value, different process: still different ids.
        c._message_id_counter = 0
        assert c._next_supervisor_id() != (first & 0xFFFFFFFF)

    def test_the_id_stays_an_integer(self):
        # A warm sandbox's daemon predates this change and rejects a non-int
        # id before the handshake, so the wire type cannot move to a hex uuid.
        from ptc_agent.core.sandbox import mcp_client_runtime as c

        assert isinstance(c._next_supervisor_id(), int)


def test_in_process_stdio_queue_wait_uses_the_call_deadline(monkeypatch):
    from ptc_agent.core.sandbox import mcp_client_runtime as c

    seen = {}

    class BusyLock:
        def acquire(self, *, timeout):
            seen["timeout"] = timeout
            return False

        def release(self):
            raise AssertionError("an unacquired lock must not be released")

    monkeypatch.setattr(c, "_get_server_lock", lambda server: BusyLock())

    with pytest.raises(RuntimeError, match="queue_timeout"):
        c._stdio_reply("alpha", "one", {}, timeout=0.05)
    assert 0 < seen["timeout"] <= 0.05


class TestAckGatedRetry:
    """A call the daemon accepted is never resent.

    The retry used to resend on any ``OSError``, including one raised after the
    daemon had acked and begun executing, so a single tool call could run up to
    three times. For a ``trading`` server that is a duplicated order.
    """

    def test_a_break_after_the_ack_raises_instead_of_retrying(self, monkeypatch):
        from ptc_agent.core.sandbox import mcp_client_runtime as c

        attempts = []

        def broke(path, request):
            attempts.append(request["id"])
            raise c._SupervisorBroke(OSError("peer reset"), acked=True)

        monkeypatch.setattr(c, "_supervisor_unavailable", False)
        monkeypatch.setattr(c, "_socket_alive", lambda path: True)
        monkeypatch.setattr(c, "_workspace_view", lambda: {})
        monkeypatch.setattr(c, "_supervisor_exchange", broke)
        monkeypatch.delenv("MCP_SUPERVISOR", raising=False)

        with pytest.raises(RuntimeError, match="may have run"):
            c._supervisor_reply("alpha", "one", {})
        assert len(attempts) == 1

    def test_a_break_before_the_ack_is_retried_with_a_fresh_id(self, monkeypatch):
        from ptc_agent.core.sandbox import mcp_client_runtime as c

        attempts = []

        def broke(path, request):
            attempts.append(request["id"])
            raise c._SupervisorBroke(OSError("no such socket"), acked=False)

        monkeypatch.setattr(c, "_supervisor_unavailable", False)
        monkeypatch.setattr(c, "_socket_alive", lambda path: True)
        monkeypatch.setattr(c, "_workspace_view", lambda: {})
        monkeypatch.setattr(c, "_supervisor_exchange", broke)
        monkeypatch.delenv("MCP_SUPERVISOR", raising=False)

        # None is the in-process fallback, which is safe because nothing ran.
        assert c._supervisor_reply("alpha", "one", {}) is None
        assert len(attempts) == 2
        assert attempts[0] != attempts[1]

    @pytest.mark.parametrize("code", ["draining", "queue_timeout", "server_busy"])
    def test_exhausted_pre_ack_retries_fall_back_in_process(
        self, monkeypatch, capsys, code
    ):
        from ptc_agent.core.sandbox import mcp_client_runtime as c

        attempts = []
        delays = []

        def refused(path, request):
            attempts.append(request["id"])
            return "error", {"type": "error", "code": code, "message": "wait"}

        monkeypatch.setattr(c, "_supervisor_unavailable", False)
        monkeypatch.setattr(c, "_socket_alive", lambda path: True)
        monkeypatch.setattr(c, "_workspace_view", lambda: {})
        monkeypatch.setattr(c, "_supervisor_exchange", refused)
        monkeypatch.setattr(c.time, "sleep", delays.append)
        monkeypatch.delenv("MCP_SUPERVISOR", raising=False)

        assert c._supervisor_reply("alpha", "one", {}) is None
        assert len(attempts) == c._SUPERVISOR_RETRY_ATTEMPTS
        assert len(set(attempts)) == len(attempts)
        assert capsys.readouterr().err.splitlines() == [
            f"MCP supervisor could not dispatch server alpha [{code}]; calling in process"
        ]
        expected = [0.1, 0.2, 0.4] if code == "draining" else [0.05, 0.1, 0.15]
        assert delays == pytest.approx(expected)

    @pytest.mark.parametrize(
        "code", ["transport", "not_enabled", "unknown_server", "config_mismatch"]
    )
    def test_terminal_failure_raises_without_retry_or_fallback(self, monkeypatch, code):
        from ptc_agent.core.sandbox import mcp_client_runtime as c

        attempts = []

        def refused(path, request):
            attempts.append(request["id"])
            return "error", {"type": "error", "code": code, "message": "refused"}

        monkeypatch.setattr(c, "_supervisor_unavailable", False)
        monkeypatch.setattr(c, "_socket_alive", lambda path: True)
        monkeypatch.setattr(c, "_workspace_view", lambda: {})
        monkeypatch.setattr(c, "_supervisor_exchange", refused)
        monkeypatch.delenv("MCP_SUPERVISOR", raising=False)

        with pytest.raises(
            RuntimeError, match="may have run" if code == "transport" else "refused"
        ):
            c._supervisor_reply("alpha", "one", {})
        assert len(attempts) == 1

    def test_draining_reconnects_and_returns_the_fresh_daemons_reply(self, monkeypatch):
        from ptc_agent.core.sandbox import mcp_client_runtime as c

        attempts = []

        def exchange(path, request):
            attempts.append(request["id"])
            if len(attempts) == 1:
                return "error", {
                    "type": "error",
                    "code": "draining",
                    "message": "old daemon",
                }
            return "reply", {"type": "reply", "reply": {"result": {"ok": True}}}

        monkeypatch.setattr(c, "_supervisor_unavailable", False)
        monkeypatch.setattr(c, "_socket_alive", lambda path: True)
        monkeypatch.setattr(c, "_workspace_view", lambda: {})
        monkeypatch.setattr(c, "_supervisor_exchange", exchange)
        monkeypatch.setattr(c.time, "sleep", lambda seconds: None)
        monkeypatch.delenv("MCP_SUPERVISOR", raising=False)

        assert c._supervisor_reply("alpha", "one", {}) == {"result": {"ok": True}}
        assert len(attempts) == 2


class TestTheCallersConfigPathIsAHint:
    """The daemon reads a workspace view only from inside its own computer."""

    @staticmethod
    def _root(tmp_path):
        root = os.path.realpath(str(tmp_path / "computer"))
        os.makedirs(os.path.join(root, "beta-2", ".agents", "tools"))
        return root

    @staticmethod
    def _view(root, folder, payload):
        path = os.path.join(root, folder, ".agents", "tools", "mcp_client_config.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        return path

    def test_a_view_inside_the_root_is_honored(self, tmp_path):
        root = self._root(tmp_path)
        sup = d.Supervisor("unused.sock", FakeBackend({"alpha"}), root=root)
        view = self._view(root, "beta-2", {"workspace_id": "ws-b", "servers": {}})
        assert sup._contained_config_path(view) == view
        rooted = self._view(root, "", {"workspace_id": "ws-root", "servers": {}})
        assert sup._contained_config_path(rooted) == rooted

    def test_a_path_outside_the_root_or_through_a_link_reads_as_no_view(self, tmp_path):
        root = self._root(tmp_path)
        sup = d.Supervisor("unused.sock", FakeBackend({"alpha"}), root=root)
        forged = str(tmp_path / "mcp_client_config.json")
        with open(forged, "w", encoding="utf-8") as fh:
            json.dump({"workspace_id": "ws-b", "servers": {}}, fh)
        assert sup._contained_config_path(forged) == ""
        link = os.path.join(root, "evil", ".agents", "tools", "mcp_client_config.json")
        os.makedirs(os.path.dirname(link))
        os.symlink(forged, link)
        assert sup._contained_config_path(link) == ""
        # Too deep to be a folder's view, and a file of another name.
        assert (
            sup._contained_config_path(
                os.path.join(
                    root, "a", "b", ".agents", "tools", "mcp_client_config.json"
                )
            )
            == ""
        )
        assert sup._contained_config_path(os.path.join(root, "beta-2", "x.json")) == ""

    def test_a_forged_view_cannot_disable_a_server(self, start_supervisor, tmp_path):
        root = self._root(tmp_path)
        forged = str(tmp_path / "mcp_client_config.json")
        with open(forged, "w", encoding="utf-8") as fh:
            json.dump({"workspace_id": "ws-x", "servers": {"nothing": {}}}, fh)
        harness = start_supervisor(FakeBackend({"alpha"}), root=root)

        frames = harness.connect().exchange(
            _call(1, "alpha", workspace_id="ws-x", config_path=forged)
        )

        # Ignored, so the union answers, exactly as an absent file does.
        assert [f["type"] for f in frames] == [p.TYPE_ACK, p.TYPE_REPLY]

    def test_without_a_root_the_path_is_taken_as_given(self, tmp_path):
        sup = d.Supervisor("unused.sock", FakeBackend({"alpha"}))
        assert sup._contained_config_path("/anywhere/x.json") == "/anywhere/x.json"


class _PreAckFailingBackend(FakeBackend):
    def is_running(self, server):
        raise RuntimeError("liveness probe exploded")


def test_a_failure_before_the_ack_still_ends_in_a_terminal_frame(start_supervisor):
    backend = _PreAckFailingBackend({"alpha"})
    client = start_supervisor(backend).connect()

    frames = client.exchange(_call(1, "alpha"))

    assert [f["type"] for f in frames] == [p.TYPE_ERROR]
    assert frames[0]["code"] == p.ERR_INTERNAL
    assert "liveness probe exploded" in frames[0]["message"]
    # Nothing ran, so the client may fall back in process.
    assert backend.entered.acquire(blocking=False) is False
    assert p.ERR_INTERNAL not in p.FATAL_CODES
