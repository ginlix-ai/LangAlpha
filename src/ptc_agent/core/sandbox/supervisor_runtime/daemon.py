"""The per-computer MCP supervisor.

One daemon owns every sandbox-side MCP server process on a computer. Each
``execute_code`` interpreter is a fresh process, so before this existed every
execution paid a handshake per server it touched -- up to 30 s for a uvx/npx
server, and again for the next execution a second later. The daemon keeps the
processes across executions and across the workspaces that share the computer,
so only the first call after an idle window is cold.

It is not a trust boundary. Workspaces on one computer share an OS user, so the
effective-server-set check below enforces the workspace's *selection*, which is
a convention the agent follows, and a second computer remains the hard
boundary.

Stdlib only: this ships into the sandbox and runs on its bare python3.
"""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
from typing import Any, Callable

from . import protocol as p

#: How long a server survives with no calls before it is reaped. Long enough
#: that a multi-step analysis never re-handshakes, short enough that an idle
#: computer is not holding a browser-bearing server all day.
DEFAULT_IDLE_WINDOW_S = 900.0
#: Cadence of non-terminal frames while a call is outstanding.
DEFAULT_HEARTBEAT_S = 15.0
#: One call's queue plus request/reply budget.
DEFAULT_CALL_TIMEOUT_S = 120.0
#: A superseded daemon gives acknowledged calls this long to finish.
DEFAULT_DRAIN_TIMEOUT_S = 30.0
#: Ceiling on concurrently dispatched calls. Per-server queues are admitted
#: before this slot is taken.
MAX_INFLIGHT = 32
#: Calls waiting behind one shared stdio server. The active call is separate.
MAX_SERVER_WAITERS = 8
_QUEUE_POLL_S = 0.05
_REAP_TICK_S = 5.0
#: A request line past this is a framing error, not a call: tool arguments that
#: large belong in a file.
MAX_REQUEST_BYTES = 8 * 1024 * 1024


class ServerBackend:
    """What the daemon needs from the MCP client runtime.

    Split out so the protocol can be exercised against a fake without a real
    MCP server, and so the daemon never reaches into the runtime's globals.
    """

    def names(self) -> set[str]:
        raise NotImplementedError

    def is_running(self, server: str) -> bool:
        raise NotImplementedError

    def call(self, server: str, tool: str, args: dict[str, Any]) -> dict[str, Any]:
        """Raw JSON-RPC reply. The caller settles it, so errors stay typed."""
        raise NotImplementedError

    def call_with_timeout(
        self, server: str, tool: str, args: dict[str, Any], timeout: float
    ) -> dict[str, Any]:
        return self.call(server, tool, args)

    def drop(self, server: str) -> None:
        raise NotImplementedError

    def running(self) -> list[str]:
        raise NotImplementedError

    def secret_fingerprint(self) -> Any:
        """Opaque, comparable stamp of the on-disk secret material."""
        return None

    def secret_dependencies(self, server: str) -> set[str] | None:
        """Credential files read by a server, or None when unknowable."""
        return None

    def serializes_calls(self, server: str) -> bool:
        return True

    def config_version(self) -> int:
        return 0

    def workspace_config_relpath(self) -> str:
        """Where a workspace's ``mcp_client_config.json`` sits inside its folder."""
        return ".agents/tools/mcp_client_config.json"

    def shutdown(self) -> None:
        raise NotImplementedError


class ClientBackend(ServerBackend):
    """Backend over the generated ``mcp_client`` module.

    Imported lazily by :func:`main` so this module stays importable on the host,
    where no generated client exists.
    """

    def __init__(self, client: Any) -> None:
        self._c = client

    def names(self) -> set[str]:
        return set(self._c._SERVER_CONFIGS)

    def is_running(self, server: str) -> bool:
        return server in self._c.running_servers()

    def running(self) -> list[str]:
        return list(self._c.running_servers())

    def call(self, server: str, tool: str, args: dict[str, Any]) -> dict[str, Any]:
        return self._c.raw_tool_reply(server, tool, args)

    def call_with_timeout(
        self, server: str, tool: str, args: dict[str, Any], timeout: float
    ) -> dict[str, Any]:
        return self._c.raw_tool_reply(server, tool, args, timeout=timeout)

    def drop(self, server: str) -> None:
        self._c.drop_server(server)

    def secret_fingerprint(self) -> Any:
        return self._c.secret_fingerprint()

    def secret_dependencies(self, server: str) -> set[str] | None:
        dependencies = self._c.server_secret_dependencies(server)
        return None if dependencies is None else set(dependencies)

    def serializes_calls(self, server: str) -> bool:
        return self._c.serializes_calls(server)

    def config_version(self) -> int:
        return int(getattr(self._c, "_CONFIG_VERSION", 0) or 0)

    def workspace_config_relpath(self) -> str:
        layout = getattr(self._c, "_WS_LAYOUT", None) or {}
        return str(
            layout.get("MCP_CLIENT_CONFIG_FILE") or super().workspace_config_relpath()
        )

    def shutdown(self) -> None:
        self._c.cleanup_mcp_servers()


class _ServerStat:
    __slots__ = ("calls", "cold_starts", "inflight", "last_used", "started_at")

    def __init__(self) -> None:
        self.calls = 0
        self.cold_starts = 0
        # Now, not 0.0: the row is created when a server is first reached for,
        # and a 0.0 here reads as "idle since 1970" to the reaper, which would
        # drop a live server on the next tick whatever the window.
        self.last_used = time.time()
        self.started_at = 0.0
        self.inflight = 0


class _ServerQueue:
    __slots__ = ("guard", "lock", "waiters")

    def __init__(self) -> None:
        self.guard = threading.Lock()
        self.lock = threading.Lock()
        self.waiters = 0


class Supervisor:
    """Accepts connections, enforces the caller's view, owns the processes."""

    def __init__(
        self,
        socket_path: str,
        backend: ServerBackend,
        *,
        root: str | None = None,
        idle_window_s: float = DEFAULT_IDLE_WINDOW_S,
        heartbeat_s: float = DEFAULT_HEARTBEAT_S,
        call_timeout_s: float = DEFAULT_CALL_TIMEOUT_S,
        drain_timeout_s: float = DEFAULT_DRAIN_TIMEOUT_S,
        max_server_waiters: int = MAX_SERVER_WAITERS,
        max_inflight: int = MAX_INFLIGHT,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self.socket_path = socket_path
        self.backend = backend
        # The computer this daemon serves. A caller's config_path is honored
        # only when it is a workspace view inside it; without a root (a
        # host-side harness) the path is taken as given.
        self.root = os.path.realpath(root) if root else None
        self.idle_window_s = idle_window_s
        self.heartbeat_s = heartbeat_s
        self.call_timeout_s = call_timeout_s
        self.drain_timeout_s = drain_timeout_s
        self.max_server_waiters = max_server_waiters
        self._log = log or _stderr_log
        self._stats: dict[str, _ServerStat] = {}
        self._stats_lock = threading.Lock()
        self._inflight: dict[int, tuple[float, _Conn]] = {}
        self._inflight_lock = threading.Lock()
        self._inflight_slots = threading.Semaphore(max_inflight)
        self._secret_stamp = backend.secret_fingerprint()
        self._secret_lock = threading.Lock()
        self._pending_secret_drops: set[str] = set()
        self._unknown_dependency_logged: set[str] = set()
        self._server_queues: dict[str, _ServerQueue] = {}
        self._server_queues_lock = threading.Lock()
        self._started_at = time.time()
        self._stop = threading.Event()
        self._draining = threading.Event()
        self._drain_lock = threading.Lock()
        self._drain_waiter_started = False
        self._sock: socket.socket | None = None
        self._config_cache: dict[str, tuple[float, int, dict[str, Any]]] = {}
        self._config_lock = threading.Lock()

    # -- lifecycle ---------------------------------------------------------
    def bind(self) -> None:
        """Bind the listening socket, clearing a socket no daemon still owns."""
        directory = os.path.dirname(self.socket_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        if os.path.exists(self.socket_path) and not _socket_is_live(self.socket_path):
            # Only after the liveness probe: unlinking a live daemon's socket
            # would leave it listening on a path nothing can reach, and the
            # next call would start a second daemon owning duplicate servers.
            try:
                os.unlink(self.socket_path)
            except OSError:
                pass
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(self.socket_path)
        os.chmod(self.socket_path, 0o600)
        sock.listen(64)
        self._sock = sock

    def serve_forever(self) -> None:
        if self._sock is None:
            self.bind()
        assert self._sock is not None
        threading.Thread(target=self._reaper, name="mcp-reaper", daemon=True).start()
        threading.Thread(
            target=self._heartbeats, name="mcp-heartbeat", daemon=True
        ).start()
        self._log(f"supervisor listening on {self.socket_path}")
        # Bound once: ``close`` clears the attribute, and reading it per accept
        # would race that into an AttributeError instead of the OSError the
        # loop is written to exit on.
        sock = self._sock
        sock.settimeout(1.0)
        try:
            while not self._stop.is_set():
                try:
                    client, _ = sock.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                threading.Thread(
                    target=self._serve_conn, args=(client,), daemon=True
                ).start()
        finally:
            self.close()

    def close(self) -> None:
        self._stop.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        try:
            if os.path.exists(self.socket_path) and not _socket_is_live(
                self.socket_path
            ):
                os.unlink(self.socket_path)
        except OSError:
            pass
        try:
            self.backend.shutdown()
        except Exception as exc:  # noqa: BLE001 - shutdown is best effort
            self._log(f"shutdown error: {exc}")

    # -- connection handling ----------------------------------------------
    def _serve_conn(self, client: socket.socket) -> None:
        conn = _Conn(client)
        try:
            for line in conn.lines():
                if not line.strip():
                    continue
                self._dispatch(conn, line)
        except OSError:
            pass
        finally:
            conn.close()

    def _dispatch(self, conn: _Conn, line: bytes) -> None:
        try:
            request = p.decode(line)
        except (ValueError, UnicodeDecodeError) as exc:
            conn.send(p.error_frame(0, p.ERR_BAD_REQUEST, f"unparseable frame: {exc}"))
            return
        request_id = request.get("id")
        if not isinstance(request_id, int):
            conn.send(p.error_frame(0, p.ERR_BAD_REQUEST, "missing integer id"))
            return
        if request.get("v") != p.VERSION:
            conn.send(
                p.error_frame(
                    request_id,
                    p.ERR_BAD_REQUEST,
                    f"protocol v{request.get('v')} not understood by this daemon",
                )
            )
            return
        op = request.get("op")
        if op == p.OP_HEALTH:
            conn.send(p.result_frame(request_id, self.health()))
            return
        if op == p.OP_SHUTDOWN:
            conn.send(p.result_frame(request_id, {"stopping": True}))
            self._stop.set()
            return
        if op != p.OP_CALL:
            conn.send(
                p.error_frame(request_id, p.ERR_BAD_REQUEST, f"unknown op {op!r}")
            )
            return
        if self._draining.is_set():
            self._log_rejection(
                request_id, str(request.get("server") or ""), "draining"
            )
            conn.send(
                p.error_frame(
                    request_id,
                    "draining",
                    "supervisor is draining acknowledged calls; retry",
                )
            )
            return
        arrived = time.monotonic()
        threading.Thread(
            target=self._run_call,
            args=(conn, request_id, request, arrived),
            daemon=True,
        ).start()

    def _run_call(
        self,
        conn: _Conn,
        request_id: int,
        request: dict[str, Any],
        arrived: float,
    ) -> None:
        started = time.time()
        deadline = arrived + self.call_timeout_s
        server_lock: threading.Lock | None = None
        slot_acquired = False
        dispatched = False
        server = str(request.get("server") or "")
        tool = str(request.get("tool") or "")
        args = request.get("args")
        try:
            if not server or not tool or not isinstance(args, dict):
                conn.send(
                    p.error_frame(
                        request_id, p.ERR_BAD_REQUEST, "server, tool and args required"
                    )
                )
                return
            # Anything that fails before the ack still owes the caller a
            # terminal frame; a bare thread death would leave it waiting out
            # the whole read timeout for a call that never started.
            try:
                refusal = self._refuse(request, server)
                if refusal is not None:
                    conn.send(p.error_frame(request_id, refusal[0], refusal[1]))
                    if refusal[0] == "draining":
                        self._start_drain_waiter()
                    return
                self._refresh_secrets()
            except Exception as exc:  # noqa: BLE001 - reported, never swallowed
                self._log(f"pre-ack failure {server}.{tool}: {exc}")
                conn.send(
                    p.error_frame(
                        request_id, p.ERR_INTERNAL, f"{type(exc).__name__}: {exc}"
                    )
                )
                return

            server_lock, refusal = self._acquire_server_turn(
                conn, request_id, server, arrived, deadline
            )
            if refusal is not None:
                if not conn.closed:
                    conn.send(p.error_frame(request_id, refusal[0], refusal[1]))
                return
            slot_acquired, refusal = self._acquire_dispatch_slot(
                conn, request_id, server, deadline
            )
            if refusal is not None:
                if not conn.closed:
                    conn.send(p.error_frame(request_id, refusal[0], refusal[1]))
                return

            try:
                cold = not self.backend.is_running(server)
            except Exception as exc:  # noqa: BLE001 - reported, never swallowed
                self._log(f"event=pre_ack_failure server={server} error={exc}")
                conn.send(
                    p.error_frame(
                        request_id, p.ERR_INTERNAL, f"{type(exc).__name__}: {exc}"
                    )
                )
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._log_rejection(request_id, server, "queue_timeout")
                conn.send(
                    p.error_frame(
                        request_id,
                        "queue_timeout",
                        f"MCP server {server} queue wait exhausted the call deadline",
                    )
                )
                return
            with self._drain_lock:
                if self._draining.is_set():
                    self._log_rejection(request_id, server, "draining")
                    conn.send(
                        p.error_frame(
                            request_id,
                            "draining",
                            "supervisor is draining acknowledged calls; retry",
                        )
                    )
                    return
                if not conn.send(p.ack_frame(request_id, server, cold=cold)):
                    self._log_rejection(request_id, server, "client_disconnected")
                    return
                dispatched = True
                with self._inflight_lock:
                    self._inflight[request_id] = (started, conn)
            # Claimed before the call, not after it: a reply is what records a
            # call, and a tool that takes longer than the idle window would
            # otherwise be reaped out from under itself.
            self._claim(server)
            try:
                reply = self.backend.call_with_timeout(server, tool, args, remaining)
            except Exception as exc:  # noqa: BLE001 - one bad server is not fatal
                self._log(f"call failed {server}.{tool}: {exc}")
                conn.send(
                    p.error_frame(
                        request_id, p.ERR_TRANSPORT, f"{type(exc).__name__}: {exc}"
                    )
                )
                return
            finally:
                self._release(server)
            self._record(server, cold=cold)
            conn.send(
                p.reply_frame(
                    request_id,
                    reply,
                    cold=cold,
                    elapsed_ms=(time.time() - started) * 1000.0,
                )
            )
        finally:
            if dispatched:
                with self._inflight_lock:
                    self._inflight.pop(request_id, None)
            if slot_acquired:
                self._inflight_slots.release()
            if server_lock is not None:
                server_lock.release()

    def _queue_for(self, server: str) -> _ServerQueue:
        with self._server_queues_lock:
            queue_state = self._server_queues.get(server)
            if queue_state is None:
                queue_state = _ServerQueue()
                self._server_queues[server] = queue_state
            return queue_state

    def _acquire_server_turn(
        self,
        conn: _Conn,
        request_id: int,
        server: str,
        arrived: float,
        deadline: float,
    ) -> tuple[threading.Lock | None, tuple[str, str] | None]:
        if not self.backend.serializes_calls(server):
            self._log(
                f"event=queue_arrival server={server} request_id={request_id} depth=0"
            )
            self._log(
                f"event=lock_wait server={server} request_id={request_id} wait_ms=0.000"
            )
            return None, None

        queue_state = self._queue_for(server)
        with queue_state.guard:
            acquired = queue_state.lock.acquire(blocking=False)
            depth = queue_state.waiters
            self._log(
                f"event=queue_arrival server={server} request_id={request_id} "
                f"depth={depth}"
            )
            if not acquired:
                if queue_state.waiters >= self.max_server_waiters:
                    self._log_lock_wait(request_id, server, arrived)
                    self._log_rejection(request_id, server, "server_busy")
                    return None, (
                        "server_busy",
                        f"MCP server {server} already has a full call queue",
                    )
                queue_state.waiters += 1

        if acquired:
            self._log_lock_wait(request_id, server, arrived)
            return queue_state.lock, None

        try:
            while True:
                if conn.closed:
                    self._log_lock_wait(request_id, server, arrived)
                    self._log_rejection(request_id, server, "client_disconnected")
                    return None, ("client_disconnected", "client disconnected")
                if self._draining.is_set():
                    self._log_lock_wait(request_id, server, arrived)
                    self._log_rejection(request_id, server, "draining")
                    return None, (
                        "draining",
                        "supervisor is draining acknowledged calls; retry",
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._log_lock_wait(request_id, server, arrived)
                    self._log_rejection(request_id, server, "queue_timeout")
                    return None, (
                        "queue_timeout",
                        f"MCP server {server} queue wait exhausted the call deadline",
                    )
                if queue_state.lock.acquire(timeout=min(_QUEUE_POLL_S, remaining)):
                    self._log_lock_wait(request_id, server, arrived)
                    return queue_state.lock, None
        finally:
            with queue_state.guard:
                queue_state.waiters -= 1

    def _acquire_dispatch_slot(
        self, conn: _Conn, request_id: int, server: str, deadline: float
    ) -> tuple[bool, tuple[str, str] | None]:
        while True:
            if conn.closed:
                self._log_rejection(request_id, server, "client_disconnected")
                return False, ("client_disconnected", "client disconnected")
            if self._draining.is_set():
                self._log_rejection(request_id, server, "draining")
                return False, (
                    "draining",
                    "supervisor is draining acknowledged calls; retry",
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._log_rejection(request_id, server, "queue_timeout")
                return False, (
                    "queue_timeout",
                    f"MCP server {server} queue wait exhausted the call deadline",
                )
            if self._inflight_slots.acquire(timeout=min(_QUEUE_POLL_S, remaining)):
                return True, None

    def _log_rejection(self, request_id: int, server: str, reason: str) -> None:
        self._log(
            f"event=call_rejected server={server} request_id={request_id} "
            f"reason={reason}"
        )

    def _log_lock_wait(self, request_id: int, server: str, arrived: float) -> None:
        self._log(
            f"event=lock_wait server={server} request_id={request_id} "
            f"wait_ms={(time.monotonic() - arrived) * 1000.0:.3f}"
        )

    # -- policy ------------------------------------------------------------
    def _refuse(self, request: dict[str, Any], server: str) -> tuple[str, str] | None:
        """The workspace-selection check, or None when the call may proceed."""
        if self._draining.is_set():
            return (
                "draining",
                "supervisor is draining acknowledged calls; retry",
            )
        view = self._workspace_view(request)
        if view is not None:
            # Before the membership check: a server the caller's newer config
            # added is unknown to this daemon, and the right answer to that is
            # to stand down, not to refuse it as nonexistent.
            caller_version = int(view.get("computer_config_version") or 0)
            daemon_version = self.backend.config_version()
            if daemon_version and caller_version > daemon_version:
                self._log(
                    "event=stale_daemon "
                    f"caller_version={caller_version} daemon_version={daemon_version}"
                )
                with self._drain_lock:
                    self._draining.set()
                return (
                    "draining",
                    "supervisor is running a superseded MCP config and is draining; retry",
                )
        if server not in self.backend.names():
            return (p.ERR_UNKNOWN_SERVER, f"Unknown MCP server: {server}")
        if view is None:
            # No config on disk is a computer whose overlay has not been built
            # yet (a warm sandbox mid-upgrade). Denying every server there
            # would take MCP away from a workspace that legitimately has it,
            # so an absent view means the union, exactly as the client's own
            # fallback does.
            return None
        want = str(request.get("workspace_id") or "")
        have = str(view.get("workspace_id") or "")
        if want and have and want != have:
            return (
                p.ERR_CONFIG_MISMATCH,
                f"config at {request.get('config_path')} belongs to {have}",
            )
        servers = view.get("servers")
        if isinstance(servers, dict) and server not in servers:
            return (
                p.ERR_NOT_ENABLED,
                f"MCP server '{server}' is not enabled for this workspace",
            )
        return None

    def _start_drain_waiter(self) -> None:
        with self._drain_lock:
            if self._drain_waiter_started:
                return
            self._drain_waiter_started = True
        threading.Thread(target=self._drain, name="mcp-drain", daemon=True).start()

    def _drain(self) -> None:
        started = time.monotonic()
        deadline = started + self.drain_timeout_s
        self._log(f"event=drain_start timeout_s={self.drain_timeout_s:.3f}")
        remaining = 0
        while True:
            with self._inflight_lock:
                remaining = len(self._inflight)
            if not remaining or time.monotonic() >= deadline:
                break
            time.sleep(_QUEUE_POLL_S)
        elapsed_ms = (time.monotonic() - started) * 1000.0
        self._log(
            f"event=drain_finish remaining={remaining} "
            f"timed_out={str(bool(remaining)).lower()} elapsed_ms={elapsed_ms:.3f}"
        )
        self._stop.set()
        try:
            wake = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            wake.settimeout(0.2)
            wake.connect(self.socket_path)
            wake.close()
        except OSError:
            pass

    def _contained_config_path(self, hint: str) -> str:
        """The workspace view ``hint`` names, or "" when it is not one.

        The caller chooses the string, so it is a hint, not an authority: the
        file has to be a workspace view on this computer, either the root's
        own or one folder's, resolved with symlinks followed. Anything else
        (a file the caller wrote, a sibling's view reached through a link)
        reads as no view at all.
        """
        if not hint:
            return ""
        if self.root is None:
            return hint
        rel = self.backend.workspace_config_relpath()
        real = os.path.realpath(hint)
        prefix = self.root.rstrip(os.sep) + os.sep
        if not real.startswith(prefix):
            return ""
        remainder = real[len(prefix) :]
        if remainder == rel:
            return real
        folder, _, tail = remainder.partition(os.sep)
        if folder and tail == rel:
            return real
        return ""

    def _workspace_view(self, request: dict[str, Any]) -> dict[str, Any] | None:
        """The caller's ``mcp_client_config.json``, cached on (mtime, size)."""
        path = self._contained_config_path(str(request.get("config_path") or ""))
        if not path:
            return None
        try:
            st = os.stat(path)
        except OSError:
            return None
        key = (st.st_mtime_ns, st.st_size)
        with self._config_lock:
            cached = self._config_cache.get(path)
            if cached is not None and (cached[0], cached[1]) == key:
                return cached[2]
        try:
            with open(path, encoding="utf-8") as fh:
                view = json.load(fh)
        except (OSError, ValueError):
            return None
        if not isinstance(view, dict):
            return None
        with self._config_lock:
            self._config_cache[path] = (key[0], key[1], view)
        return view

    def _refresh_secrets(self) -> None:
        """Respawn only servers whose credential files changed."""
        with self._secret_lock:
            stamp = self.backend.secret_fingerprint()
            if stamp == self._secret_stamp:
                return
            previous = _fingerprints_by_path(self._secret_stamp)
            current = _fingerprints_by_path(stamp)
            self._secret_stamp = stamp
            changed = None
            if previous is not None and current is not None:
                changed = {
                    path
                    for path in previous.keys() | current.keys()
                    if previous.get(path) != current.get(path)
                }
            for name in self.backend.running():
                dependencies = self.backend.secret_dependencies(name)
                if dependencies is None:
                    if name not in self._unknown_dependency_logged:
                        self._unknown_dependency_logged.add(name)
                        self._log(
                            f"event=secret_dependency_fallback server={name} "
                            "dependency=all"
                        )
                    affected = True
                    files = "all"
                else:
                    affected = bool(dependencies) and (
                        changed is None or bool(dependencies & changed)
                    )
                    files = ",".join(sorted(dependencies & (changed or dependencies)))
                if not affected:
                    continue
                with self._stats_lock:
                    stat = self._stats.get(name)
                    active = stat.inflight if stat is not None else 0
                    if active:
                        self._pending_secret_drops.add(name)
                action = "defer" if active else "drop"
                self._log(
                    f"event=server_drop server={name} reason=secret_rotation "
                    f"action={action} files={files}"
                )
                if not active:
                    try:
                        self.backend.drop(name)
                    except Exception as exc:  # noqa: BLE001 - rotation is best effort
                        self._log(
                            f"event=server_drop_failed server={name} "
                            f"reason=secret_rotation error={exc}"
                        )

    # -- background --------------------------------------------------------
    def _claim(self, server: str) -> None:
        with self._stats_lock:
            stat = self._stats.setdefault(server, _ServerStat())
            stat.inflight += 1
            stat.last_used = time.time()

    def _release(self, server: str) -> None:
        drop = False
        with self._stats_lock:
            stat = self._stats.get(server)
            if stat is not None:
                stat.inflight = max(0, stat.inflight - 1)
                stat.last_used = time.time()
                if not stat.inflight and server in self._pending_secret_drops:
                    self._pending_secret_drops.remove(server)
                    drop = True
        if drop:
            self._log(
                f"event=server_drop server={server} reason=secret_rotation "
                "action=drop_after_inflight"
            )
            try:
                self.backend.drop(server)
            except Exception as exc:  # noqa: BLE001 - rotation is best effort
                self._log(
                    f"event=server_drop_failed server={server} "
                    f"reason=secret_rotation error={exc}"
                )

    def _reaper(self) -> None:
        while not self._stop.wait(_REAP_TICK_S):
            now = time.time()
            with self._stats_lock:
                idle = []
                for name in self.backend.running():
                    stat = self._stats.get(name)
                    if stat is None:
                        # Running with no row is a server the daemon has not
                        # been asked for yet, so it is not idle either.
                        self._stats[name] = _ServerStat()
                        continue
                    if stat.inflight:
                        continue
                    if now - stat.last_used > self.idle_window_s:
                        idle.append(name)
            for name in idle:
                self._log(f"idle reap: {name}")
                try:
                    self.backend.drop(name)
                except Exception as exc:  # noqa: BLE001 - reaping is best effort
                    self._log(f"reap failed {name}: {exc}")

    def _heartbeats(self) -> None:
        while not self._stop.wait(self.heartbeat_s):
            now = time.time()
            with self._inflight_lock:
                outstanding = list(self._inflight.items())
            for request_id, (started, conn) in outstanding:
                conn.send(p.heartbeat_frame(request_id, (now - started) * 1000.0))

    def _record(self, server: str, *, cold: bool) -> None:
        with self._stats_lock:
            stat = self._stats.setdefault(server, _ServerStat())
            stat.calls += 1
            stat.last_used = time.time()
            if cold:
                stat.cold_starts += 1
                stat.started_at = stat.last_used

    def health(self) -> dict[str, Any]:
        running = set(self.backend.running())
        with self._stats_lock:
            servers = {
                name: {
                    "state": "running" if name in running else "stopped",
                    "calls": stat.calls,
                    "cold_starts": stat.cold_starts,
                    "last_used": stat.last_used,
                    "started_at": stat.started_at,
                }
                for name, stat in sorted(self._stats.items())
            }
        for name in sorted(running - set(servers)):
            servers[name] = {
                "state": "running",
                "calls": 0,
                "cold_starts": 0,
                "last_used": 0.0,
                "started_at": 0.0,
            }
        return {
            "ok": True,
            "pid": os.getpid(),
            "uptime_s": round(time.time() - self._started_at, 3),
            "config_version": self.backend.config_version(),
            "idle_window_s": self.idle_window_s,
            "known_servers": sorted(self.backend.names()),
            "servers": servers,
        }


class _Conn:
    """One client connection: buffered line reads, serialised frame writes."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._write_lock = threading.Lock()
        self._closed = False

    def lines(self):
        buf = b""
        while True:
            chunk = self._sock.recv(65536)
            if not chunk:
                if buf.strip():
                    yield buf
                return
            buf += chunk
            if len(buf) > MAX_REQUEST_BYTES:
                self.send(p.error_frame(0, p.ERR_BAD_REQUEST, "request line too large"))
                return
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                yield line

    @property
    def closed(self) -> bool:
        with self._write_lock:
            return self._closed

    def send(self, frame: dict[str, Any]) -> bool:
        payload = p.encode(frame)
        with self._write_lock:
            if self._closed:
                return False
            try:
                self._sock.sendall(payload)
            except OSError:
                self._closed = True
                return False
        return True

    def close(self) -> None:
        with self._write_lock:
            self._closed = True
        try:
            self._sock.close()
        except OSError:
            pass


def _socket_is_live(path: str) -> bool:
    """True when something is still accepting on ``path``."""
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(1.0)
    try:
        probe.connect(path)
        return True
    except OSError:
        return False
    finally:
        probe.close()


def _fingerprints_by_path(stamp: Any) -> dict[str, tuple[Any, ...]] | None:
    """Normalize path stamps while accepting the old opaque backend seam."""
    if isinstance(stamp, dict):
        return {str(path): (value,) for path, value in stamp.items()}
    if not isinstance(stamp, (list, tuple)):
        return None
    normalized = {}
    for row in stamp:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            return None
        normalized[str(row[0])] = tuple(row[1:])
    return normalized


def _stderr_log(message: str) -> None:
    print(f"[mcp-supervisor] {message}", file=sys.stderr, flush=True)  # noqa: T201


def main(argv: list[str] | None = None) -> int:
    """Run the daemon for the computer rooted at ``argv[0]``.

    Single instance per computer: the exclusive lock is taken before the bind,
    so two executions racing to start one produce exactly one daemon and the
    loser exits without touching the socket.
    """
    import fcntl

    argv = list(sys.argv[1:] if argv is None else argv)
    root = argv[0] if argv else os.environ.get("MCP_SUPERVISOR_ROOT", "")
    if not root:
        _stderr_log("usage: python -m supervisor <computer_root>")
        return 2
    socket_path = os.path.join(root, p.SOCKET_REL_PATH)
    os.makedirs(os.path.dirname(socket_path), exist_ok=True)
    lock_path = socket_path + ".lock"
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        _stderr_log("another supervisor holds this computer; exiting")
        os.close(lock_fd)
        return 0

    import mcp_client  # noqa: PLC0415 - generated, sandbox-only

    idle = float(os.environ.get("MCP_SUPERVISOR_IDLE_S") or DEFAULT_IDLE_WINDOW_S)
    supervisor = Supervisor(
        socket_path, ClientBackend(mcp_client), root=root, idle_window_s=idle
    )
    try:
        supervisor.bind()
    except OSError as exc:
        _stderr_log(f"bind failed: {exc}")
        return 1
    supervisor.serve_forever()
    return 0
