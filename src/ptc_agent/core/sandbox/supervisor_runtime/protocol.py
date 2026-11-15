"""Wire format between the sandbox MCP client and the supervisor daemon.

Newline-delimited JSON over a Unix socket: one request per line, then zero or
more non-terminal frames and exactly one terminal frame, every frame carrying
the request's ``id``. Non-terminal frames exist so a 120 s tool call is
distinguishable from a wedged one -- the reader re-arms its deadline on each
frame rather than holding one long blocking read.

Both ends ship into the sandbox and run on its bare python3, so this module is
stdlib-only. It is also imported on the host by the protocol tests, which is
why the framing lives here rather than inside the daemon.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
from typing import Any

#: Bumped only when a frame's meaning changes. A daemon that does not
#: recognise the caller's version refuses rather than guessing.
VERSION = 1

#: Relative to the computer root. Not a ``SandboxLayout`` ClassVar: the socket
#: is the daemon's own artifact and nothing outside this package addresses it.
SOCKET_REL_PATH = "_internal/system/mcp-supervisor.sock"
LOG_REL_PATH = "_internal/system/mcp-supervisor.log"

#: A Unix socket's address holds 103 bytes on macOS (104 with the NUL; Linux
#: allows 107), and a computer root in iCloud Drive or deep under non-ASCII
#: names leaves too few for ``SOCKET_REL_PATH``.
SUN_PATH_MAX = 103
#: Under the login's home, for a root too long: only its own user can write
#: there, so nothing else can take the name first.
SHORT_SOCKET_DIR = ".langalpha/s"


def socket_path(root: str, home: str | None = None) -> str:
    """Where the daemon of the computer rooted at ``root`` listens.

    Beside the computer's other internal files when that fits a socket's
    address, else a name hashed from the root under the login's home, so the
    daemon, its starter and every client derive the same path from the root
    alone. The sandbox client carries a copy (``mcp_client_runtime``).
    """
    root = _root(root)
    short = _short_name(root)
    if short is None:
        return f"{root}/{SOCKET_REL_PATH}"
    home = (os.path.expanduser("~") if home is None else home).rstrip("/")
    return f"{home}/{short}"


def socket_path_sh(root: str) -> str:
    """``socket_path`` as one shell word for the starter, which runs on the
    computer: only the machine knows the login's home, so it is ``$HOME``."""
    root = _root(root)
    short = _short_name(root)
    if short is None:
        return shlex.quote(f"{root}/{SOCKET_REL_PATH}")
    return f'"$HOME"/{shlex.quote(short)}'


def _root(root: str) -> str:
    return root.rstrip("/") or "/"


def _short_name(root: str) -> str | None:
    """The socket's name under the home, None when the root's own fits."""
    if len(os.fsencode(f"{root}/{SOCKET_REL_PATH}")) <= SUN_PATH_MAX:
        return None
    digest = hashlib.sha256(os.fsencode(root)).hexdigest()[:16]
    return f"{SHORT_SOCKET_DIR}/{digest}.sock"


#: Sandbox-side import name of the package this module ships as.
PACKAGE_NAME = "supervisor"

# -- operations -------------------------------------------------------------
OP_CALL = "call"
OP_HEALTH = "health"
OP_SHUTDOWN = "shutdown"
#: Stop taking calls, let acknowledged ones finish, then exit, as a superseded
#: daemon does.
OP_RETIRE = "retire"

# -- frame types ------------------------------------------------------------
#: Non-terminal: the daemon accepted the call and is about to dispatch it.
TYPE_ACK = "ack"
#: Non-terminal: the call is still outstanding.
TYPE_HEARTBEAT = "heartbeat"
#: Terminal: carries the raw JSON-RPC reply for the client to settle.
TYPE_REPLY = "reply"
#: Terminal: carries structured data for a non-call op.
TYPE_RESULT = "result"
#: Terminal: the call never reached a server, or the daemon refused it.
TYPE_ERROR = "error"

TERMINAL_TYPES = frozenset({TYPE_REPLY, TYPE_RESULT, TYPE_ERROR})

# -- error codes ------------------------------------------------------------
ERR_BAD_REQUEST = "bad_request"
ERR_UNKNOWN_SERVER = "unknown_server"
ERR_NOT_ENABLED = "not_enabled"
ERR_CONFIG_MISMATCH = "config_mismatch"
ERR_STALE_DAEMON = "stale_daemon"
ERR_TRANSPORT = "transport"
ERR_INTERNAL = "internal"

#: Codes the client must NOT retry in-process against its own config: the
#: refusal is a fact about the workspace, not a daemon failure, so falling back
#: would turn an explicit denial into a silent success.
FATAL_CODES = frozenset({ERR_NOT_ENABLED, ERR_UNKNOWN_SERVER, ERR_CONFIG_MISMATCH})


def encode(frame: dict[str, Any]) -> bytes:
    """One frame as a single line of UTF-8 JSON.

    ``ensure_ascii`` stays on: the line is the framing unit, and a lone
    surrogate from a hostile server name would otherwise make the line
    undecodable on the far side and desynchronise the stream.
    """
    return (json.dumps(frame, ensure_ascii=True, default=str) + "\n").encode("utf-8")


def decode(line: bytes | str) -> dict[str, Any]:
    """Parse one frame; raises ValueError for anything that is not an object."""
    if isinstance(line, bytes):
        line = line.decode("utf-8", errors="replace")
    parsed = json.loads(line)
    if not isinstance(parsed, dict):
        msg = "frame is not a JSON object"
        raise ValueError(msg)
    return parsed


def call_request(
    request_id: int,
    *,
    server: str,
    tool: str,
    args: dict[str, Any],
    workspace_id: str = "",
    config_path: str = "",
    config_version: int = 0,
) -> dict[str, Any]:
    """A ``call`` request.

    ``config_path`` names the caller's ``mcp_client_config.json`` rather than
    shipping its contents: the daemon reads the file itself, so what it
    enforces is the same view the workspace's own wrappers were built from.
    """
    return {
        "v": VERSION,
        "id": request_id,
        "op": OP_CALL,
        "server": server,
        "tool": tool,
        "args": args,
        "workspace_id": workspace_id,
        "config_path": config_path,
        "config_version": config_version,
    }


def simple_request(request_id: int, op: str) -> dict[str, Any]:
    """A ``health``, ``shutdown`` or ``retire`` request."""
    return {"v": VERSION, "id": request_id, "op": op}


def error_frame(request_id: int, code: str, message: str) -> dict[str, Any]:
    return {
        "v": VERSION,
        "id": request_id,
        "type": TYPE_ERROR,
        "code": code,
        "message": message,
    }


def reply_frame(
    request_id: int, reply: dict[str, Any], *, cold: bool, elapsed_ms: float
) -> dict[str, Any]:
    return {
        "v": VERSION,
        "id": request_id,
        "type": TYPE_REPLY,
        "reply": reply,
        "cold": cold,
        "elapsed_ms": round(elapsed_ms, 3),
    }


def result_frame(request_id: int, data: dict[str, Any]) -> dict[str, Any]:
    return {"v": VERSION, "id": request_id, "type": TYPE_RESULT, "data": data}


def ack_frame(request_id: int, server: str, *, cold: bool) -> dict[str, Any]:
    return {
        "v": VERSION,
        "id": request_id,
        "type": TYPE_ACK,
        "server": server,
        "cold": cold,
    }


def heartbeat_frame(request_id: int, elapsed_ms: float) -> dict[str, Any]:
    return {
        "v": VERSION,
        "id": request_id,
        "type": TYPE_HEARTBEAT,
        "elapsed_ms": round(elapsed_ms, 3),
    }
