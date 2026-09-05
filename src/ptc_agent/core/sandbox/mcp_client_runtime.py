"""MCP client runtime for the PTC sandbox.

Static: ALL per-workspace variance (server set, working dir, trace caps) is
applied by ``_apply_config_dict`` from a generated JSON epilogue that
``tool_generator.generate_mcp_client_code`` appends when composing the
uploaded ``tools/mcp_client.py`` (discovery probes get their own composed
copy at a unique path, so concurrent probes never share state). Wrapper
modules import ``_call_mcp_tool`` from here.

Runs on the sandbox's bare python3 - stdlib + httpx only, no host packages.
"""

import collections
import dataclasses
import datetime
import hashlib
import json
import os
import queue
import re as _re
import subprocess
import sys
import threading
from typing import Any
import time
import httpx

# Global registry of MCP server processes (for stdio)
_server_processes: dict[str, subprocess.Popen] = {}
_server_locks: dict[str, threading.RLock] = {}
_locks_guard = threading.Lock()
_message_id_counter = 0
_message_id_lock = threading.Lock()

# Negotiated protocol state per server, published only after a spawn+negotiate
# transaction completes: {"mode": "modern"|"legacy", "version": str,
# "session_id": str|None}. Per-interpreter (each execute_code is a fresh
# process), so a silent server pays one bounded probe per interpreter.
_PROTO: dict[str, dict] = {}

# Where a 2026-era server stamps its identity on a discover result. The legacy
# handshake returns the same object under `serverInfo`, so both eras answer the
# same question and `_server_identity` reads whichever one applies.
_SERVER_INFO_META_KEY = "io.modelcontextprotocol/serverInfo"

_CLIENT_INFO = {"name": "open-ptc-client", "version": "2.0.0"}
# Modern spec revisions this client speaks, newest first.
_MODERN_VERSIONS = ("2026-07-28",)
# Version offered on the legacy initialize fallback — the latest handshake
# revision, so pre-2026 servers negotiate the newest era they support.
_LEGACY_OFFER = "2025-11-25"
_PROBE_TIMEOUT = 10.0
_CALL_TIMEOUT = 120.0

# HTTP wall clocks, in seconds. An HTTP handshake is multi-phase, so each phase
# gets its own deadline carved from the outer budget: one clock shared across
# phases lets a hung server/discover probe hand the legacy initialize an
# already-expired deadline, and the fallback that exists for exactly that
# server can then never send its first byte.
_HTTP_EXCHANGE_BUDGET = 30.0  # a handshake or a tools/list, end to end
_HTTP_PHASE_FLOOR = 10.0  # no phase starts on an expired clock
_HTTP_CALL_BUDGET = 65.0  # one tools/call send; above the relay's 55s wall

# Ceiling on what one reply may accumulate before the reader gives up, on both
# transports: a server that floods otherwise OOM-kills the interpreter instead
# of failing diagnosably. 16 MiB sits far above any real JSON-RPC reply.
_REPLY_MAX_BYTES = 16 * 1024 * 1024
# The stdout pump reads in pieces of at most this size so the budget above can
# bite mid-line; readline() still returns early at each newline.
_STDIO_CHUNK_CHARS = 65536
# Cap on each retained stderr line (head kept — a crash line names its cause
# first). The tail's deque bounds line COUNT; this bounds line size, so one
# newline-free blob can't make the tail itself unbounded.
_STDERR_LINE_MAX_BYTES = 4096
# Server-initiated requests answered with -32601 per reply read. The refusal
# write is the one blocking stdin operation in the reader: a server that floods
# requests while never draining stdin would wedge the writer against a full
# pipe (~64 KiB) with the per-server lock held. 16 refusals ≈ 1.4 KiB of stdin
# — nowhere near pipe capacity — and no legitimate server sends more per call.
_REFUSAL_MAX = 16

# ---------------------------------------------------------------------------
# Configuration. Shape produced by tool_generator.generate_client_config():
#   {"working_dir": str,
#    "servers": {name: {transport, untrusted, command?, args?, url?, env?,
#                        env_keys?, headers?, discovery_uses_secrets?,
#                        relay_bound?}},
#    "result_body_max_bytes": int, "result_body_trace_budget_bytes": int}
# Each entry is normalized ONCE into a frozen _ServerCfg, so nothing downstream
# re-derives trust or re-reads an untyped key. These names are seeded by the
# _apply_config_dict({}) call below (standalone lint/unit-test import), then
# overwritten by the generated epilogue's call with the real values — the
# defaults live only in _apply_config_dict / _normalize, so declaring the names
# here just keeps them typed.
# ---------------------------------------------------------------------------

_SERVER_CONFIGS: "dict[str, _ServerCfg]"
_WORK_DIR: str
_INTERNAL_ROOT: str
_VAULT_SECRETS_FILE: str
_EGRESS_RELAY_FILE: str
_RESULT_BODY_MAX_BYTES: int
_RESULT_BODY_TRACE_BUDGET_BYTES: int


@dataclasses.dataclass(frozen=True)
class _ServerCfg:
    """One server's config entry, normalized at apply time."""

    name: str
    transport: str
    untrusted: bool
    command: str
    args: tuple
    env: dict
    env_keys: tuple
    url: str
    headers: dict
    discovery_uses_secrets: bool
    relay_bound: bool


def _normalize(name: str, entry: dict) -> _ServerCfg:
    return _ServerCfg(
        name=name,
        transport=entry.get("transport") or "stdio",
        # The host computes trust; a missing flag fails CLOSED. Guessing
        # untrusted costs a builtin its inherited env; guessing trusted hands a
        # user server the sandbox's whole environment and host-var substitution.
        untrusted=bool(entry.get("untrusted", True)),
        command=entry.get("command") or "",
        args=tuple(entry.get("args") or ()),
        env=dict(entry.get("env") or {}),
        env_keys=tuple(entry.get("env_keys") or ()),
        url=entry.get("url") or "",
        headers=dict(entry.get("headers") or {}),
        discovery_uses_secrets=bool(entry.get("discovery_uses_secrets")),
        relay_bound=bool(entry.get("relay_bound")),
    )


def _apply_config_dict(cfg: dict) -> None:
    """(Re)initialize module state from a config dict (generated epilogue)."""
    global _SERVER_CONFIGS, _WORK_DIR, _INTERNAL_ROOT, _VAULT_SECRETS_FILE
    global _EGRESS_RELAY_FILE, _RESULT_BODY_MAX_BYTES
    global _RESULT_BODY_TRACE_BUDGET_BYTES
    _SERVER_CONFIGS = {
        _name: _normalize(_name, _entry)
        for _name, _entry in (cfg.get("servers") or {}).items()
    }
    _WORK_DIR = cfg.get("working_dir") or "/home/workspace"
    _INTERNAL_ROOT = _WORK_DIR + "/_internal"
    _VAULT_SECRETS_FILE = _INTERNAL_ROOT + "/.vault_secrets.json"
    _EGRESS_RELAY_FILE = _INTERNAL_ROOT + "/.egress_relay.json"
    _RESULT_BODY_MAX_BYTES = int(cfg.get("result_body_max_bytes") or 65536)
    _RESULT_BODY_TRACE_BUDGET_BYTES = int(
        cfg.get("result_body_trace_budget_bytes") or 4 * 1024 * 1024
    )


_apply_config_dict({})  # seed standalone-import defaults through the one path


def _server_cfg(server_name: str) -> _ServerCfg:
    """The named server's normalized config; the one unknown-server error."""
    cfg = _SERVER_CONFIGS.get(server_name)
    if cfg is None:
        msg = f"Unknown MCP server: {server_name}"
        raise ValueError(msg)
    return cfg


# ---------------------------------------------------------------------------
# Vault secrets — ${vault:NAME} resolution for untrusted servers, file only.
# ---------------------------------------------------------------------------

# Matches ${vault:NAME} — mirrors mcp_sanitize.VAULT_REF_RE. Only this exact
# form resolves; a bare ${VAR} is intentionally NOT a vault reference.
_VAULT_REF_RE = _re.compile(r"\$\{vault:([A-Za-z_][A-Za-z0-9_]{0,127})\}")


def _load_vault() -> dict:
    """Load the workspace vault. Returns {} when the file is absent (discovery)."""
    try:
        with open(_VAULT_SECRETS_FILE) as _f:
            return json.load(_f)
    except (FileNotFoundError, ValueError, OSError):
        return {}


def _resolve_vault_refs(value, vault, *, missing, discovery=False):
    """Substitute ${vault:NAME} refs in ``value`` against ``vault`` only.

    Unresolvable refs are recorded in ``missing`` (by NAME, never value). In
    discovery mode they become an inert empty string so tools/list still runs.
    There is NO fallback to os.environ — that is the whole point.
    """
    def _sub(match):
        name = match.group(1)
        if name in vault:
            return vault[name]
        missing.append(name)
        return "" if discovery else match.group(0)

    return _VAULT_REF_RE.sub(_sub, value)


def _resolve_all(cfg, values, *, discovery=False):
    """Resolve ${vault:NAME} across one server's values, vault-only.

    Secret-less discovery (default) resolves every ref inert, so ``tools/list``
    still runs; ``discovery_uses_secrets`` opts in a server that needs auth even
    to list. Normal calls always resolve, and every missing secret is named
    together in ONE error (names only, never values).
    """
    vault = _load_vault() if (not discovery or cfg.discovery_uses_secrets) else {}
    missing = []
    resolved = [
        _resolve_vault_refs(str(_v), vault, missing=missing, discovery=discovery)
        for _v in values
    ]
    if missing and not discovery:
        raise RuntimeError(
            "Missing vault secret(s) for server "
            + repr(cfg.name) + ": " + ", ".join(sorted(set(missing)))
        )
    return resolved


def _build_proc_env(cfg, *, discovery=False):
    """Build the stdio subprocess env.

    Builtin servers inherit os.environ. Untrusted servers get a MINIMAL scoped
    env (PATH/HOME plus only their own declared env values), with ${vault:NAME}
    refs resolved vault-only — never the sandbox's full os.environ, never a
    host-env fallback.
    """
    if not cfg.untrusted:
        proc_env = os.environ.copy()
        for key in cfg.env_keys:
            if key in os.environ:
                proc_env[key] = os.environ[key]
    else:
        proc_env = {}
        for _k in ("PATH", "HOME", "LANG", "LC_ALL"):
            if _k in os.environ:
                proc_env[_k] = os.environ[_k]
        names = list(cfg.env)
        values = _resolve_all(cfg, [cfg.env[n] for n in names], discovery=discovery)
        proc_env.update(dict(zip(names, values)))

    internal_root = _INTERNAL_ROOT
    existing_pythonpath = proc_env.get("PYTHONPATH", "")
    extra_paths = [_WORK_DIR, internal_root + "/src", internal_root]
    proc_env["PYTHONPATH"] = ":".join(
        [p for p in [existing_pythonpath, *extra_paths] if p]
    )
    return proc_env


def _resolve_cmd_args(cfg, *, discovery=False):
    """Resolve ${vault:NAME} refs in a stdio server's args, vault-only.

    Builtin args pass through unchanged; an untrusted server's args resolve like
    its env — so a credential moved into args by import resolves at spawn
    instead of leaking as a literal.
    """
    if not cfg.untrusted:
        return list(cfg.args)
    return _resolve_all(cfg, cfg.args, discovery=discovery)


def _resolve_http(cfg, *, discovery=False):
    """Return (url, headers) for an http request.

    Relay-bound servers dial the relay instead. Builtin servers keep the legacy
    ${VAR}-from-os.environ URL resolution and send no extra headers. Untrusted
    servers resolve ${vault:NAME} refs in BOTH the URL and headers vault-only
    (no host-env fallback) and send the resolved headers.
    """
    if cfg.relay_bound:
        return _resolve_relay(cfg)

    if not cfg.untrusted:
        def _env_sub(match):
            return os.environ.get(match.group(1), match.group(0))

        return _re.sub(r"\$\{([^}]+)\}", _env_sub, cfg.url), {}

    names = list(cfg.headers)
    resolved = _resolve_all(
        cfg, [cfg.url, *(cfg.headers[n] for n in names)], discovery=discovery
    )
    return resolved[0], dict(zip(names, resolved[1:]))


# ---------------------------------------------------------------------------
# Egress relay — the only path an OAuth-connected server is reachable by.
# ---------------------------------------------------------------------------

# Relay rejection codes (X-Relay-Error header) -> actionable guidance.
# Must cover every member of src.server.services.egress.RelayError. This module
# runs inside the sandbox and cannot import server code, so the duplication is
# structural; tests/unit/core/test_relay_error_hints.py fails on any drift.
_RELAY_ERROR_HINTS = {
    "needs_reauth": "the OAuth connection needs re-authorization; reconnect the server in Plugins",
    "relay_auth": "this sandbox's relay credentials are invalid or expired",
    "bad_request": "the relay rejected this JSON-RPC frame as malformed or oversized",
    "not_found": "no active grant for this server; reconnect it in Plugins",
    "method_blocked": "the HTTP method is not permitted by this connection's policy",
    "tool_blocked": "the tool is not permitted by this connection's policy",
    "policy_missing": "this connection's capability policy has not been computed yet; reconnect the server in Plugins",
    "refresh_in_progress": "the vendor token is being refreshed; retry in a few seconds",
    "destination_blocked": "the relay refused to dial this server's address",
    "upstream_unreachable": "the relay could not reach the vendor's server",
    "vendor_redirect": "the vendor redirected this endpoint, so its URL has moved; update the server URL in Plugins",
    "limited_rate": "rate limit reached for this connection; retry shortly",
    "limited_concurrency": "too many concurrent calls for this connection; retry shortly",
    "relay_disabled": "the egress relay is disabled on this deployment",
    "wall_clock": "the call exceeded the relay's time budget",
}


def _load_relay_credentials() -> dict:
    """Read the relay credential file fresh (it is re-minted host-side)."""
    for _attempt in (0, 1):
        try:
            with open(_EGRESS_RELAY_FILE) as _f:
                return json.load(_f)
        except ValueError:
            time.sleep(0.2)  # caught mid-rewrite; one retry
        except (FileNotFoundError, OSError):
            break
    return {}


def _relay_error(response, server_name: str):
    """Actionable message when the RELAY (not the vendor) rejected the call.

    Returns None for vendor responses — the relay's response-header allowlist
    guarantees X-Relay-Error only ever originates from the relay itself.
    """
    code = response.headers.get("x-relay-error")
    if not code:
        return None
    hint = _RELAY_ERROR_HINTS.get(code, code)
    return f"MCP server {server_name}: relay rejected the call [{code}]: {hint}"


def _resolve_relay(cfg):
    """(url, headers) for an OAuth-connected server: always the egress relay.

    The grant id comes ONLY from the credential file the host re-mints per
    session — a baked-in copy would outlive the grant it names.
    """
    creds = _load_relay_credentials()
    grant_id = (creds.get("grants") or {}).get(cfg.name)
    base = (creds.get("relay_base_url") or "").rstrip("/")
    token = creds.get("token") or ""
    if not (grant_id and base and token):
        raise RuntimeError(
            f"MCP server {cfg.name} is OAuth-connected but this sandbox has "
            "no relay credentials - the egress relay may be disabled, or the "
            "binding failed at session start; check the connection in Plugins"
        )
    return base + "/v1/egress/" + str(grant_id), {"Authorization": "Bearer " + token}


# ---------------------------------------------------------------------------
# Provenance trace + result unwrapping.
# ---------------------------------------------------------------------------

# Per-execution running sum of emitted result_body bytes. Each execute_code runs
# in a FRESH interpreter process — both the Daytona and Docker providers spawn a
# new `python` per code_run (a one-shot run, NOT a persistent kernel/session), so
# this module is re-imported and the counter resets to 0 every run. It therefore
# accumulates only across the MCP calls within ONE execute_code, never session-
# wide. Caps supplied via the config
# file from agent/provenance/types.py (RESULT_BODY_MAX_BYTES = per-call body cap;
# the budget is the aggregate ceiling): once the running sum crosses the budget
# we stop emitting result_body (snippet/sha/size still recorded) to keep a
# cooperative run's trace small. This is a courtesy bound only — agent code can
# write MCP_TRACE_FILE directly; the hard host-memory bound is in
# sandbox/execution.py:_collect_mcp_trace, which sizes the file before reading it.
_result_body_emitted_bytes = 0


def _trace_mcp_call(server: str, tool: str, args: Any, result: Any) -> None:
    """Append one JSONL provenance line for an MCP call (best-effort, never raises).

    No-op unless MCP_TRACE_FILE is set. The fingerprint (sha256/size/snippet) is
    computed in-sandbox and must reproduce the host-side fingerprint_result
    contract byte-for-byte.
    """
    global _result_body_emitted_bytes
    try:
        trace_file = os.environ.get("MCP_TRACE_FILE")
        if not trace_file:
            return
        try:
            if isinstance(result, (dict, list)):
                canonical = json.dumps(
                    result, sort_keys=True, default=str, ensure_ascii=False
                )
            else:
                canonical = str(result)
        except Exception:
            try:
                canonical = str(result)
            except Exception:
                canonical = ""
        encoded = canonical.encode("utf-8")
        entry = {
            "server": server,
            "tool": tool,
            "args": args if isinstance(args, dict) else {},
            "result_sha256": hashlib.sha256(encoded).hexdigest(),
            # TRUE full byte length — independent of the body cap below, so the
            # host can derive truncation as byte_len > len(stored body).
            "result_size": len(encoded),
            # Cap interpolated from the canonical SNIPPET_MAX_CHARS in
            # agent/provenance/types.py at codegen time, so host + sandbox
            # snippets match byte-for-byte for dedup with no manual hardcode.
            "result_snippet": canonical[:500],
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        # Per-call body = first RESULT_BODY_MAX_BYTES *bytes* of the canonical
        # result (decode with errors="ignore" so a multibyte char split at the
        # cap is dropped, not mojibake). Hash-consistent: the body is a prefix of
        # the exact bytes that produced result_sha256. Skip once the aggregate
        # per-execution budget is exhausted — snippet/sha/size always survive.
        if _result_body_emitted_bytes < _RESULT_BODY_TRACE_BUDGET_BYTES:
            body = encoded[:_RESULT_BODY_MAX_BYTES].decode("utf-8", errors="ignore")
            entry["result_body"] = body
            _result_body_emitted_bytes += len(body.encode("utf-8"))
        os.makedirs(os.path.dirname(trace_file), exist_ok=True)
        with open(trace_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, default=str, ensure_ascii=False) + "\n")
            fh.flush()
    except Exception:
        # Tracing must never break the agent's code.
        pass


def _is_error_result(envelope: Any, value: Any) -> bool:
    """True when an MCP tools/call result is an error rather than data.

    Honors the MCP ``isError`` flag (any spec-compliant server, including remote
    HTTP MCP) and our servers' ``{"error": ...}`` return convention. Provenance
    records only data the agent actually received, so error results are returned
    to the caller unchanged but never traced. A falsy ``error`` field (e.g.
    ``error: null`` on a success payload) is NOT treated as an error.
    """
    if isinstance(envelope, dict) and envelope.get("isError") is True:
        return True
    return isinstance(value, dict) and bool(value.get("error"))


def _unwrap_mcp_content(envelope: Any) -> Any:
    """Prefer structuredContent (unwrapping the SDK's single-``result``-key
    convention for non-object returns); fall back to a single text content
    block parsed as JSON / text; else passthrough."""
    if isinstance(envelope, dict):
        structured = envelope.get("structuredContent")
        if isinstance(structured, dict):
            if set(structured) == {"result"}:
                return structured["result"]
            return structured

    if (isinstance(envelope, dict) and
        isinstance(envelope.get("content"), list)):

        content_blocks = envelope["content"]

        if (len(content_blocks) == 1 and
            isinstance(content_blocks[0], dict) and
            content_blocks[0].get("type") == "text"):

            unwrapped = content_blocks[0].get("text", "")

            if unwrapped.startswith(("{", "[")):
                try:
                    return json.loads(unwrapped)
                except json.JSONDecodeError:
                    return unwrapped

            return unwrapped

    return envelope


def _finalize_mcp_result(
    server_name: str, tool_name: str, arguments: dict[str, Any], envelope: Any
) -> Any:
    """Unwrap an MCP result and trace it iff it carries real data.

    Shared by both transports — the single place that sees the raw envelope (so
    the ``isError`` flag survives) and decides whether the call is recordable.
    """
    if isinstance(envelope, dict):
        result_type = envelope.get("resultType")
        if result_type not in (None, "complete"):
            # input_required (or any future resultType) needs an interactive
            # continuation this one-shot client can't provide — fail clearly,
            # never retry-loop or hang.
            msg = (
                f"MCP tool {server_name}.{tool_name} returned "
                f"resultType={result_type!r}, which this client cannot continue"
            )
            raise RuntimeError(msg)
    value = _unwrap_mcp_content(envelope)
    if not _is_error_result(envelope, value):
        _trace_mcp_call(server_name, tool_name, arguments, value)
    return value


def _settle_reply(
    reply: dict, server_name: str, tool_name: str, arguments: dict[str, Any]
) -> Any:
    """Turn a matched tools/call reply into the tool's value, or raise.

    Both transports end here, so a JSON-RPC error and a malformed reply read
    the same to the agent whichever way the server was reached.
    """
    if "error" in reply:
        error_msg = f"MCP tool call failed: {reply['error']}"
        print(f"ERROR: {error_msg}", file=sys.stderr)  # noqa: T201
        print(f"Tool: {server_name}.{tool_name}", file=sys.stderr)  # noqa: T201
        print(f"Arguments: {arguments}", file=sys.stderr)  # noqa: T201
        raise RuntimeError(error_msg)
    if "result" not in reply:
        error_msg = f"MCP reply from {server_name} has no result field"
        print(f"ERROR: {error_msg}", file=sys.stderr)  # noqa: T201
        print(f"Reply: {reply}", file=sys.stderr)  # noqa: T201
        raise RuntimeError(error_msg)
    return _finalize_mcp_result(server_name, tool_name, arguments, reply["result"])


def _log_call_failure(
    label: str, exc: Exception, server_name: str, tool_name: str, arguments: Any
) -> None:
    """Dump a failed tool call to stderr — the agent's only view of the cause."""
    import traceback

    print(f"\n{'='*60}", file=sys.stderr)  # noqa: T201
    print(f"ERROR in {label}", file=sys.stderr)  # noqa: T201
    print(f"{'='*60}", file=sys.stderr)  # noqa: T201
    print(f"Error Type: {type(exc).__name__}", file=sys.stderr)  # noqa: T201
    print(f"Error Message: {exc}", file=sys.stderr)  # noqa: T201
    print(f"Server: {server_name}", file=sys.stderr)  # noqa: T201
    print(f"Tool: {tool_name}", file=sys.stderr)  # noqa: T201
    print(f"Arguments: {arguments}", file=sys.stderr)  # noqa: T201
    print("\nFull Traceback:", file=sys.stderr)  # noqa: T201
    traceback.print_exc(file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)  # noqa: T201


# ---------------------------------------------------------------------------
# JSON-RPC framing — request builders, ids, per-server locks.
# ---------------------------------------------------------------------------


def _get_next_message_id() -> int:
    """Get next message ID for JSON-RPC requests."""
    global _message_id_counter
    with _message_id_lock:
        _message_id_counter += 1
        return _message_id_counter


def _get_server_lock(server_name: str) -> threading.RLock:
    """Per-server RLock, created race-free before any process inspection."""
    with _locks_guard:
        lock = _server_locks.get(server_name)
        if lock is None:
            lock = threading.RLock()
            _server_locks[server_name] = lock
    return lock


def _modern_request(method: str, params: dict, version: str) -> dict:
    """Build a 2026-07-28 request: _meta must carry protocolVersion AND
    clientCapabilities on EVERY request or the server rejects it."""
    params = dict(params or {})
    meta = dict(params.get("_meta") or {})
    meta["io.modelcontextprotocol/protocolVersion"] = version
    meta["io.modelcontextprotocol/clientCapabilities"] = {}
    meta["io.modelcontextprotocol/clientInfo"] = _CLIENT_INFO
    params["_meta"] = meta
    return {
        "jsonrpc": "2.0",
        "id": _get_next_message_id(),
        "method": method,
        "params": params,
    }


def _legacy_request(method: str, params: dict) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": _get_next_message_id(),
        "method": method,
        "params": params or {},
    }


def _legacy_init_request() -> dict:
    """The pre-2026 initialize both transports send — one spelling of the offer."""
    return _legacy_request("initialize", {
        "protocolVersion": _LEGACY_OFFER,
        "capabilities": {},
        "clientInfo": _CLIENT_INFO,
    })


# ---------------------------------------------------------------------------
# stdio transport — subprocess lifecycle, line framing, negotiation ladder.
# ---------------------------------------------------------------------------

# Queued by the stdout pump on a _REPLY_MAX_BYTES breach; distinct from the
# None EOF sentinel so the reader reports the cause instead of "closed
# connection".
_STDIO_OVERSIZE = object()


def _kill_server(server_name: str, proc: subprocess.Popen) -> None:
    try:
        proc.kill()
    except OSError:
        pass
    _server_processes.pop(server_name, None)
    _PROTO.pop(server_name, None)


def _send_message(server_name: str, proc: subprocess.Popen, message: dict) -> None:
    try:
        proc.stdin.write(json.dumps(message) + "\n")
        proc.stdin.flush()
    except OSError as e:
        _kill_server(server_name, proc)
        error_msg = f"Failed to send request to MCP server {server_name}: {e}"
        print(f"ERROR: {error_msg}", file=sys.stderr)  # noqa: T201
        raise RuntimeError(error_msg)


def _read_reply(server_name: str, proc: subprocess.Popen, want_id: int, timeout: float) -> dict:
    """Read the reply matching ``want_id``.

    Skips notifications and stale replies (abandoned ids from a prior timeout),
    answers server-initiated requests with -32601 (server->client requests are
    deprecated in 2026-07-28), and kills the process on timeout / EOF / oversize
    / invalid framing so the next call restarts cleanly. Lines come from the pump
    thread's queue, never select() — a burst of messages lands in Python's
    stdio buffer where select() on the fd would block forever.
    """
    deadline = time.monotonic() + timeout
    refusals = 0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            _kill_server(server_name, proc)
            error_msg = f"MCP server {server_name} timed out after {timeout:.0f}s"
            print(f"ERROR: {error_msg}", file=sys.stderr)  # noqa: T201
            raise RuntimeError(error_msg)
        try:
            line = proc.mcp_stdout_queue.get(timeout=remaining)
        except queue.Empty:
            continue
        if isinstance(line, str):
            # Consumed — retire it from the pump's outstanding-bytes budget.
            with proc.mcp_budget_lock:
                proc.mcp_outstanding[0] -= len(line)
        if line is _STDIO_OVERSIZE:
            _kill_server(server_name, proc)
            error_msg = (
                f"MCP server {server_name}: unconsumed stdout exceeded "
                f"{_REPLY_MAX_BYTES} bytes without a matching reply "
                "[reply_too_large]"
            )
            print(f"ERROR: {error_msg}", file=sys.stderr)  # noqa: T201
            raise RuntimeError(error_msg)
        if line is None:  # EOF sentinel from the pump thread
            _kill_server(server_name, proc)
            error_msg = f"MCP server {server_name} closed connection"
            stderr_tail = "\n".join(getattr(proc, "mcp_stderr_tail", ()))
            if "No module named 'mcp." in stderr_tail:
                error_msg += (
                    " — the server crashed importing an MCP SDK module its"
                    " runtime does not provide (incompatible mcp version in"
                    " its environment); launch it isolated via uvx/npx with"
                    " pinned versions"
                )
            if stderr_tail:
                error_msg += f"\nserver stderr tail:\n{stderr_tail}"
            print(f"ERROR: {error_msg}", file=sys.stderr)  # noqa: T201
            raise RuntimeError(error_msg)
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            _kill_server(server_name, proc)
            error_msg = f"Invalid JSON from MCP server {server_name}: {line[:200]!r}"
            print(f"ERROR: {error_msg}", file=sys.stderr)  # noqa: T201
            raise RuntimeError(error_msg)
        if not isinstance(message, dict) or "id" not in message:
            continue  # notification (or junk) — never the reply
        if "method" in message:
            refusals += 1
            if refusals > _REFUSAL_MAX:
                _kill_server(server_name, proc)
                error_msg = (
                    f"MCP server {server_name} sent more than {_REFUSAL_MAX} "
                    "requests while a reply was pending [request_flood]"
                )
                print(f"ERROR: {error_msg}", file=sys.stderr)  # noqa: T201
                raise RuntimeError(error_msg)
            refusal = {
                "jsonrpc": "2.0",
                "id": message["id"],
                "error": {"code": -32601, "message": "Method not found"},
            }
            try:
                proc.stdin.write(json.dumps(refusal) + "\n")
                proc.stdin.flush()
            except OSError:
                pass
            continue
        if message.get("id") != want_id:
            continue  # stale reply from an abandoned request
        return message


def _spawn_mcp_process(server_name: str, discovery: bool = False) -> subprocess.Popen:
    """Spawn the server subprocess (no handshake, no registry publication)."""
    cfg = _server_cfg(server_name)
    cmd = [cfg.command] + _resolve_cmd_args(cfg, discovery=discovery)
    proc_env = _build_proc_env(cfg, discovery=discovery)

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=proc_env,
        text=True,
        bufsize=1,  # Line buffered
    )

    # Drain stderr in background to prevent pipe buffer deadlock.
    # MCP servers log INFO to stderr; if the 64KB pipe buffer fills, the
    # server blocks on write(stderr) and can't respond on stdout. A bounded
    # tail is kept so a crash-on-spawn can report its cause.
    err_tail = collections.deque(maxlen=40)

    def _drain_stderr(p=proc, t=err_tail):
        # Capped readline, not line iteration: a stderr stream that never
        # newlines (a \r progress meter) would otherwise assemble one
        # unbounded line in memory before the tail truncation could apply.
        # The first piece of a line is exactly the head the tail keeps; the
        # rest of an overlong line is read and dropped.
        mid_line = False
        try:
            while True:
                piece = p.stderr.readline(_STDERR_LINE_MAX_BYTES)
                if not piece:
                    break
                if not mid_line:
                    t.append(piece.rstrip("\n")[:_STDERR_LINE_MAX_BYTES])
                mid_line = not piece.endswith("\n")
        except (OSError, ValueError):
            pass

    threading.Thread(target=_drain_stderr, daemon=True).start()
    proc.mcp_stderr_tail = err_tail

    # Pump stdout lines onto a queue: readers wait on the queue, not
    # select() on the fd, which goes quiet once a message burst has been
    # slurped into Python's stdio buffer.
    out_queue = queue.Queue()

    # The pump also bounds OUTSTANDING bytes — enqueued but not yet consumed by
    # a reader — because unbounded queue depth is the stdio twin of an unbounded
    # HTTP body. Readers retire what they dequeue, so a long-lived server may
    # stream any total volume; only bytes piling up unconsumed (one giant reply,
    # or junk with no reader waiting) breach the cap, kill the server, and let
    # the next call respawn it with an empty queue.
    budget_lock = threading.Lock()
    outstanding = [0]

    def _pump(p=proc, q=out_queue, lock=budget_lock, pending=outstanding):
        # Capped readline pieces, not line iteration: charging only COMPLETED
        # lines would let one no-newline flood assemble unbounded in memory
        # before the cap could bite. Pieces charge as they arrive (so the cap
        # bounds the partially-assembled line too); readline still returns the
        # moment a newline lands, so small replies keep their latency.
        buf: list[str] = []
        try:
            while True:
                piece = p.stdout.readline(_STDIO_CHUNK_CHARS)
                if not piece:
                    break
                with lock:
                    pending[0] += len(piece)
                    over = pending[0] > _REPLY_MAX_BYTES
                if over:
                    q.put(_STDIO_OVERSIZE)
                    return
                buf.append(piece)
                if piece.endswith("\n"):
                    q.put("".join(buf))
                    buf = []
        except (OSError, ValueError):
            pass
        if buf:
            q.put("".join(buf))  # final unterminated line, as iteration yielded
        q.put(None)  # EOF sentinel

    threading.Thread(target=_pump, daemon=True).start()
    proc.mcp_stdout_queue = out_queue
    proc.mcp_budget_lock = budget_lock
    proc.mcp_outstanding = outstanding

    return proc


def _server_identity(result: dict, *, modern: bool) -> dict | None:
    """What the server said it is, from either era's handshake result.

    Display-only per the spec, which is why every malformed shape reads as
    absent: a server that stamps nonsense here still serves tools, and refusing
    the connection over its business card would be the wrong trade.
    """
    if modern:
        meta = result.get("_meta")
        raw = meta.get(_SERVER_INFO_META_KEY) if isinstance(meta, dict) else None
    else:
        raw = result.get("serverInfo")
    return raw if isinstance(raw, dict) else None


def _legacy_initialize(server_name: str, proc: subprocess.Popen) -> dict:
    """Pre-2026 handshake; offer the newest legacy revision, adopt the reply's."""
    request = _legacy_init_request()
    _send_message(server_name, proc, request)
    response = _read_reply(server_name, proc, request["id"], _PROBE_TIMEOUT)
    if "error" in response:
        _kill_server(server_name, proc)
        msg = f"MCP initialization failed: {response['error']}"
        raise RuntimeError(msg)
    version = (response.get("result") or {}).get("protocolVersion") or _LEGACY_OFFER
    _send_message(server_name, proc, {
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
    })
    return {
        "mode": "legacy",
        "version": version,
        "session_id": None,
        "server_info": _server_identity(response.get("result") or {}, modern=False),
    }


def _negotiate_stdio(server_name: str, proc: subprocess.Popen, discovery: bool = False) -> tuple:
    """server/discover probe with a bounded fallback ladder.

    Mutual modern version => modern. JSON-RPC method error => legacy initialize
    on the SAME stream (a pre-2026 server answered politely; no era latch).
    Discover ok but only legacy versions advertised => legacy initialize on a
    FRESH stream (the v2 server era-latched this connection on first request).
    Timeout / EOF / invalid framing => the probe may have crashed the server:
    restart once, then legacy initialize. -32022 with a mutual version in its
    data => retry discover once with that version.
    """
    version = _MODERN_VERSIONS[0]
    error = None
    for attempt in (1, 2):
        request = _modern_request("server/discover", {}, version)
        _send_message(server_name, proc, request)
        try:
            response = _read_reply(server_name, proc, request["id"], _PROBE_TIMEOUT)
        except RuntimeError:
            proc = _spawn_mcp_process(server_name, discovery=discovery)
            return proc, _legacy_initialize(server_name, proc)
        error = response.get("error")
        if error is None:
            result = response.get("result") or {}
            supported = result.get("supportedVersions") or []
            mutual = [v for v in _MODERN_VERSIONS if v in supported]
            if mutual:
                return proc, {
                    "mode": "modern",
                    "version": mutual[0],
                    "session_id": None,
                    "server_info": _server_identity(result, modern=True),
                }
            _kill_server(server_name, proc)
            proc = _spawn_mcp_process(server_name, discovery=discovery)
            return proc, _legacy_initialize(server_name, proc)
        code = error.get("code") if isinstance(error, dict) else None
        if code == -32022 and attempt == 1:
            data = error.get("data") or {}
            supported = data.get("supportedVersions") or []
            mutual = [v for v in _MODERN_VERSIONS if v in supported]
            if mutual:
                version = mutual[0]
                continue
        return proc, _legacy_initialize(server_name, proc)
    _kill_server(server_name, proc)
    msg = f"MCP server {server_name} rejected protocol negotiation: {error}"
    raise RuntimeError(msg)


def _ensure_stdio_server(server_name: str, discovery: bool = False) -> tuple:
    """Return (proc, proto); spawn + negotiate + publish is ONE lock-guarded
    transaction, so two racing cold starts produce exactly one process."""
    with _get_server_lock(server_name):
        proc = _server_processes.get(server_name)
        proto = _PROTO.get(server_name)
        if proc is not None and proc.poll() is None and proto is not None:
            return proc, proto
        proc = _spawn_mcp_process(server_name, discovery=discovery)
        proc, proto = _negotiate_stdio(server_name, proc, discovery=discovery)
        _server_processes[server_name] = proc
        _PROTO[server_name] = proto
        return proc, proto


def _call_mcp_tool_stdio(server_name: str, tool_name: str, arguments: dict[str, Any]) -> Any:
    """Call an MCP tool via stdio transport (subprocess)."""
    try:
        # The whole exchange holds the server lock: ensure (spawn+negotiate on
        # cold start — RLock, so re-entry is fine), then write+read serialized.
        with _get_server_lock(server_name):
            proc, proto = _ensure_stdio_server(server_name)

            params = {"name": tool_name, "arguments": arguments}
            if proto["mode"] == "modern":
                request = _modern_request("tools/call", params, proto["version"])
            else:
                request = _legacy_request("tools/call", params)

            _send_message(server_name, proc, request)
            response = _read_reply(server_name, proc, request["id"], _CALL_TIMEOUT)
            return _settle_reply(response, server_name, tool_name, arguments)

    except Exception as e:  # noqa: BLE001 - Top-level error handler for MCP tool call
        _log_call_failure(
            "_call_mcp_tool_stdio", e, server_name, tool_name, arguments
        )
        raise


# ---------------------------------------------------------------------------
# HTTP transport — spec headers, bounded reply reading, negotiation.
# ---------------------------------------------------------------------------


# Protocol-owned header names a configured header map must never supply: the
# config would silently desync the wire header from the body ``_meta`` (or
# forge a session), and dict-key casing would even send both spellings.
_RESERVED_MCP_HEADERS = frozenset(
    {"mcp-protocol-version", "mcp-method", "mcp-name", "mcp-session-id"}
)


def _mcp_headers(method: str, mcp_name: str, proto: dict, extra: dict) -> dict:
    """Spec headers for one HTTP request. Modern adds Mcp-Method/Mcp-Name
    (MCP-Protocol-Version must equal the body _meta); legacy echoes the
    captured Mcp-Session-Id. Configured headers are applied last, minus the
    reserved protocol names. Tool-declared x-mcp-header params are not
    emitted — a known limitation for third-party servers that rely on them."""
    headers = {"Accept": "application/json, text/event-stream"}
    headers["MCP-Protocol-Version"] = proto["version"]
    if proto["mode"] == "modern":
        headers["Mcp-Method"] = method
        if mcp_name:
            try:
                mcp_name.encode("ascii")
            except UnicodeEncodeError:
                raise RuntimeError(
                    f"Non-ASCII MCP name {mcp_name!r}: the base64 Mcp-Name "
                    "sentinel form is not supported by this client"
                )
            headers["Mcp-Name"] = mcp_name
    elif proto.get("session_id"):
        headers["Mcp-Session-Id"] = proto["session_id"]
    for name, value in (extra or {}).items():
        if name.lower() in _RESERVED_MCP_HEADERS:
            continue
        headers[name] = value
    return headers


# A single HTTP reply (JSON body or the SSE frames up to the matching message)
# is read incrementally so _REPLY_MAX_BYTES and the deadline below can bite: a
# direct (non-relay) HTTP server is untrusted, and httpx's read timeout resets
# on every byte — so without these a flooding server OOMs the interpreter and a
# slow-drip server hangs it forever. Relay-bound traffic is already time-capped
# by the relay's own wall clock; this is the guard for everything else.


def _deadline_exceeded(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() > deadline


def _phase_deadline(
    outer: float, *, cap: float | None = None, floor: float = 0.0
) -> float:
    """Deadline for the next phase of a multi-phase exchange.

    ``cap`` bounds what a single phase may take out of the outer budget;
    ``floor`` guarantees the phase after it a usable clock, because a slow
    first phase must not silently expire the fallback that exists to rescue it.
    """
    now = time.monotonic()
    remaining = outer - now
    if cap is not None:
        remaining = min(remaining, cap)
    return now + max(remaining, floor)


def _parse_http_reply(
    response, want_id: int, server_name: str, deadline: float | None = None
) -> dict:
    """Extract the JSON-RPC reply from a JSON or SSE-framed HTTP response.

    The response must be a streamed httpx response so the body is consumed
    incrementally under the size cap and ``deadline`` (a total-exchange wall
    clock that httpx's per-read timeout can't provide). SSE parsing joins
    multiline data: fields per the eventsource spec, skips comment/priming
    frames and interleaved notifications, and returns the first id match.
    """
    ctype = (response.headers.get("content-type") or "").split(";")[0].strip().lower()
    if ctype != "text/event-stream":
        # A JSON body carries exactly one message, so unlike the SSE scan there
        # is nothing to skip past: anything but the awaited reply is a broken
        # server and gets a named refusal instead of flowing on as the result.
        body = _read_body_capped(response, server_name, deadline)
        try:
            message = json.loads(body)
        except json.JSONDecodeError:
            msg = (
                f"Invalid JSON from MCP server {server_name}: "
                f"{body[:200]!r} [invalid_reply]"
            )
            raise RuntimeError(msg)
        if not isinstance(message, dict):
            msg = (
                f"MCP server {server_name}: HTTP reply is not a JSON-RPC "
                "message object [invalid_reply]"
            )
            raise RuntimeError(msg)
        if "method" in message and "id" in message:
            msg = (
                f"MCP server {server_name}: server-initiated request "
                f"{message['method']!r} is not supported by this client "
                "[unsupported_server_request]"
            )
            raise RuntimeError(msg)
        if "method" in message or message.get("id") != want_id:
            msg = (
                f"MCP server {server_name}: HTTP reply did not answer "
                f"request id {want_id} [mismatched_reply]"
            )
            raise RuntimeError(msg)
        return message

    def _frame_reply(payload):
        try:
            message = json.loads(payload)
        except json.JSONDecodeError:
            return None
        if not isinstance(message, dict):
            return None
        if "method" in message and "id" in message:
            # Server-initiated request (sampling/elicitation): unsupported —
            # fail fast instead of scanning on until the stream times out.
            msg = (
                f"MCP server {server_name}: server-initiated request "
                f"{message['method']!r} is not supported by this client "
                "[unsupported_server_request]"
            )
            raise RuntimeError(msg)
        if message.get("id") == want_id and "method" not in message:
            return message
        return None

    data_lines = []
    line_parts: list[str] = []
    seen = 0

    def _consume_line(line):
        """One framed SSE line; returns the matching reply or None."""
        if line == "":
            if data_lines:
                reply = _frame_reply("\n".join(data_lines))
                data_lines.clear()
                return reply
            return None
        if line.startswith(":"):
            return None  # comment / keep-alive priming
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip(" "))
        # other SSE fields (event:, id:, retry:) never carry the payload
        return None

    # Chunked, never line-iterated: iter_lines() buffers an unterminated line
    # without limit inside httpx, so a newline-free flood would blow past both
    # guards below. Chunks arrive at the transport's read granularity, which
    # the server does not control.
    for chunk in response.iter_text():
        if _deadline_exceeded(deadline):
            raise RuntimeError(
                f"MCP server {server_name}: SSE read exceeded the deadline "
                "before a matching reply [stream_deadline]"
            )
        seen += len(chunk)
        if seen > _REPLY_MAX_BYTES:
            raise RuntimeError(
                f"MCP server {server_name}: SSE stream exceeded "
                f"{_REPLY_MAX_BYTES} bytes without a matching reply "
                "[reply_too_large]"
            )
        while chunk:
            newline = chunk.find("\n")
            if newline < 0:
                line_parts.append(chunk)
                break
            line_parts.append(chunk[:newline])
            chunk = chunk[newline + 1 :]
            line = "".join(line_parts).rstrip("\r")
            line_parts.clear()
            reply = _consume_line(line)
            if reply is not None:
                return reply
    if line_parts:
        _consume_line("".join(line_parts).rstrip("\r"))
    if data_lines:
        reply = _frame_reply("\n".join(data_lines))
        if reply is not None:
            return reply
    msg = f"MCP server {server_name}: SSE stream ended without a matching reply"
    raise RuntimeError(msg)


def _read_body_capped(response, server_name: str, deadline: float | None) -> bytes:
    """Read a non-SSE response body incrementally under the size cap + deadline."""
    chunks = []
    seen = 0
    for chunk in response.iter_bytes():
        if _deadline_exceeded(deadline):
            raise RuntimeError(
                f"MCP server {server_name}: HTTP read exceeded the deadline "
                "[stream_deadline]"
            )
        seen += len(chunk)
        if seen > _REPLY_MAX_BYTES:
            raise RuntimeError(
                f"MCP server {server_name}: HTTP reply exceeded "
                f"{_REPLY_MAX_BYTES} bytes [reply_too_large]"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _ensure_http_server(server_name: str, discovery: bool = False) -> dict:
    """Negotiate (once per interpreter) and return the server's proto state.

    HTTP is stateless per POST, so a failed discover probe needs no restart —
    the legacy initialize simply goes out as a fresh request, on a deadline of
    its own so a probe that hung cannot condemn it.
    """
    proto = _PROTO.get(server_name)
    if proto is not None:
        return proto

    cfg = _server_cfg(server_name)
    if not cfg.url and not cfg.relay_bound:
        msg = f"Remote MCP server {server_name} has no URL configured"
        raise ValueError(msg)

    url, _headers = _resolve_http(cfg, discovery=discovery)

    version = _MODERN_VERSIONS[0]
    probe_note = ""
    try:
        with httpx.Client(timeout=_HTTP_EXCHANGE_BUDGET) as client:
            outer = time.monotonic() + _HTTP_EXCHANGE_BUDGET
            probe = _modern_request("server/discover", {}, version)
            probe_headers = _mcp_headers(
                "server/discover",
                "",
                {"mode": "modern", "version": version, "session_id": None},
                _headers,
            )
            reply = None
            try:
                with client.stream("POST", url, json=probe, headers=probe_headers) as response:
                    if response.status_code < 400:
                        reply = _parse_http_reply(
                            response,
                            probe["id"],
                            server_name,
                            # Mirrors the stdio probe budget: a silent or
                            # drip-feeding server costs one bounded probe, never
                            # the whole handshake.
                            _phase_deadline(outer, cap=_PROBE_TIMEOUT),
                        )
            except (httpx.HTTPError, RuntimeError, ValueError) as probe_exc:
                if "stream_deadline" in str(probe_exc):
                    # Name the phase that actually stalled, or the fallback
                    # below gets blamed for the probe's failure.
                    probe_note = (
                        " (the server/discover probe stalled first"
                        " [discover_probe_timeout])"
                    )
                reply = None
            if isinstance(reply, dict) and "result" in reply:
                supported = (reply["result"] or {}).get("supportedVersions") or []
                mutual = [v for v in _MODERN_VERSIONS if v in supported]
                if mutual:
                    proto = {
                        "mode": "modern",
                        "version": mutual[0],
                        "session_id": None,
                        "server_info": _server_identity(
                            reply["result"] or {}, modern=True
                        ),
                    }
                    _PROTO[server_name] = proto
                    return proto

            init = _legacy_init_request()
            relay_auth_retried = False
            while True:
                # Fresh per-attempt clock: the probe above (and a relay-auth
                # retry below) already spent part of the outer budget, and an
                # initialize born expired is refused before its first chunk.
                init_deadline = _phase_deadline(outer, floor=_HTTP_PHASE_FLOOR)
                init_headers = {"Accept": "application/json, text/event-stream"}
                init_headers.update(_headers)
                with client.stream("POST", url, json=init, headers=init_headers) as response:
                    if (
                        response.headers.get("x-relay-error") == "relay_auth"
                        and not relay_auth_retried
                    ):
                        # Every execute_code runs in a fresh interpreter with an
                        # empty _PROTO, so the handshake — not the tool call — is
                        # where a concurrent host re-mint is usually raced. Re-read
                        # the credential file and retry once, as tools/call does.
                        relay_auth_retried = True
                        url, _headers = _resolve_http(cfg, discovery=discovery)
                        continue
                    _msg = _relay_error(response, server_name)
                    if _msg:
                        raise RuntimeError(_msg)
                    response.raise_for_status()
                    session_id = response.headers.get("mcp-session-id")
                    reply = _parse_http_reply(
                        response, init["id"], server_name, init_deadline
                    )
                break
            if "error" in reply:
                msg = f"MCP HTTP initialization failed: {reply['error']}"
                raise RuntimeError(msg)
            adopted = (reply.get("result") or {}).get("protocolVersion") or _LEGACY_OFFER
            proto = {
                "mode": "legacy",
                "version": adopted,
                "session_id": session_id,
                "server_info": _server_identity(
                    reply.get("result") or {}, modern=False
                ),
            }
            notif_headers = _mcp_headers("notifications/initialized", "", proto, _headers)
            # Streamed and never read: the reply is discarded either way, and
            # buffering it would hand an unbounded body to the interpreter.
            with client.stream(
                "POST",
                url,
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                headers=notif_headers,
            ):
                pass
        _PROTO[server_name] = proto
        return proto

    except Exception as e:  # noqa: BLE001 - Re-raising as RuntimeError with context
        msg = f"Failed to initialize remote MCP server {server_name}: {e}{probe_note}"
        raise RuntimeError(msg) from e


def _call_mcp_tool_http(server_name: str, tool_name: str, arguments: dict[str, Any]) -> Any:
    """Call an MCP tool via streamable HTTP transport."""
    try:
        # Negotiate once per interpreter, then speak the agreed era
        proto = _ensure_http_server(server_name)

        cfg = _server_cfg(server_name)
        url, _headers = _resolve_http(cfg)

        params = {"name": tool_name, "arguments": arguments}

        def _build(p: dict) -> dict:
            return (
                _modern_request("tools/call", params, p["version"])
                if p["mode"] == "modern"
                else _legacy_request("tools/call", params)
            )

        request = _build(proto)
        headers = _mcp_headers("tools/call", tool_name, proto, _headers)

        # Each attempt streams the reply under a size cap + a deadline of its
        # own: the 65s budget sits strictly above the egress relay's 55s hard
        # wall so relay budget errors stay typed, and the deadline bounds a
        # direct server that drips bytes forever (httpx's read timeout can't).
        # It is re-armed per send because the session-expiry path spends a whole
        # re-negotiation first — a retry inheriting the first attempt's clock
        # would fall back under the relay's wall and turn a typed relay error
        # into a local timeout. The two recovery paths — relay credential
        # re-mint and legacy session expiry — each fire at most once, so the
        # loop is bounded to three sends.
        with httpx.Client(timeout=_HTTP_CALL_BUDGET) as client:
            relay_auth_retried = False
            session_reinited = False
            while True:
                deadline = time.monotonic() + _HTTP_CALL_BUDGET
                with client.stream("POST", url, json=request, headers=headers) as response:
                    relay_code = response.headers.get("x-relay-error")
                    if relay_code:
                        if relay_code == "relay_auth" and not relay_auth_retried:
                            # The credential file may have been re-minted between
                            # our read and this call — re-read it and retry once.
                            relay_auth_retried = True
                            url, _headers = _resolve_http(cfg)
                            headers = _mcp_headers("tools/call", tool_name, proto, _headers)
                            continue
                        # A relay rejection also invalidates the negotiated
                        # vendor session (e.g. reconnect mints a new grant).
                        _PROTO.pop(server_name, None)
                        raise RuntimeError(_relay_error(response, server_name))
                    if (
                        response.status_code == 404
                        and proto.get("mode") == "legacy"
                        and proto.get("session_id")
                        and not session_reinited
                    ):
                        # 2025-11-25 session expiry: the server dropped our
                        # session id. Reinitialize once and retry with the fresh
                        # session.
                        session_reinited = True
                        _PROTO.pop(server_name, None)
                        proto = _ensure_http_server(server_name)
                        request = _build(proto)
                        headers = _mcp_headers("tools/call", tool_name, proto, _headers)
                        continue
                    response.raise_for_status()
                    result = _parse_http_reply(
                        response, request["id"], server_name, deadline
                    )
                    break

        return _settle_reply(result, server_name, tool_name, arguments)

    except Exception as e:  # noqa: BLE001 - Top-level error handler for MCP tool call
        _log_call_failure(
            "_call_mcp_tool_http", e, server_name, tool_name, arguments
        )
        raise


# ---------------------------------------------------------------------------
# Dispatch, discovery + CLI.
# ---------------------------------------------------------------------------


def _legacy_sse_refusal(server_name: str) -> str:
    """The single refusal text for legacy ``sse``, shared by calls and discovery.

    The old client POSTed plain JSON-RPC, which never satisfied a real
    legacy-SSE server's GET->endpoint-event->POST flow, so refusing breaks
    nothing that worked — but both lanes must refuse together, or the connector
    discovers "ok" at save time and dies on every call mid-turn.
    """
    return (
        f"MCP server {server_name} uses legacy transport 'sse', which this "
        "client does not support; change the server's transport to 'http' "
        "(streamable HTTP)."
    )


def _call_mcp_tool(server_name: str, tool_name: str, arguments: dict[str, Any]) -> Any:
    """Call an MCP tool via the appropriate transport.

    Routes on the server's configured transport: streamable HTTP or stdio.
    Legacy ``sse`` is refused (the old client never spoke the real SSE flow).
    """
    transport = _server_cfg(server_name).transport

    # Tracing happens in the transport via _finalize_mcp_result, where the raw
    # envelope (incl. the MCP isError flag) is still visible — so failed calls
    # and error payloads are returned to the agent but never recorded as sources.
    if transport == "http":
        return _call_mcp_tool_http(server_name, tool_name, arguments)
    if transport == "sse":
        raise RuntimeError(_legacy_sse_refusal(server_name))
    return _call_mcp_tool_stdio(server_name, tool_name, arguments)


def cleanup_mcp_servers():
    """Clean up all MCP server processes."""
    for server_name, proc in _server_processes.items():
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except (OSError, TimeoutError) as e:
            print(f"Error cleaning up MCP server {server_name}: {e}", file=sys.stderr)  # noqa: T201
    _server_processes.clear()
    _PROTO.clear()


def discover(server_name: str) -> dict:
    """List a server's tools without requiring the vault (file-IPC caller writes JSON).

    Returns {"server", "status", "error", "tools": [{name, description,
    input_schema}], "server_info"}. Never raises — failures are captured in
    ``status``/``error``.
    """
    cfg = _SERVER_CONFIGS.get(server_name)
    if cfg is None:
        return {"server": server_name, "status": "error",
                "error": "unknown server", "tools": []}
    try:
        if cfg.transport == "sse":
            # Refuse here too, so the connector saves as status=error with an
            # actionable reason instead of discovering "ok" over a POST the
            # call path will reject on every turn.
            raise RuntimeError(_legacy_sse_refusal(server_name))
        if cfg.transport == "http":
            raw = _discover_http(server_name)
        else:
            raw = _discover_stdio(server_name)
    except Exception as e:  # noqa: BLE001 - discovery must never crash the driver
        return {"server": server_name, "status": "error",
                "error": str(e), "tools": []}
    tools = []
    for t in raw or []:
        if not isinstance(t, dict):
            continue
        tools.append({
            "name": t.get("name", ""),
            "description": t.get("description", "") or "",
            "input_schema": t.get("inputSchema") or t.get("input_schema") or {},
        })
    return {
        "server": server_name,
        "status": "ok",
        "error": "",
        "tools": tools,
        # Read from the published proto rather than re-handshaking: the
        # negotiation above already has it and this runs once per interpreter.
        "server_info": (_PROTO.get(server_name) or {}).get("server_info"),
    }


def _discover_stdio(server_name: str) -> list:
    """Negotiate with the stdio server in discovery mode and list its tools."""
    proc, proto = _ensure_stdio_server(server_name, discovery=True)
    if proto["mode"] == "modern":
        req = _modern_request("tools/list", {}, proto["version"])
    else:
        req = _legacy_request("tools/list", {})
    _send_message(server_name, proc, req)
    resp = _read_reply(server_name, proc, req["id"], 30)
    if "error" in resp:
        raise RuntimeError(f"tools/list error: {resp['error']}")
    return (resp.get("result") or {}).get("tools", [])


def _discover_http(server_name: str) -> list:
    """Negotiate with the http server in discovery mode and list its tools."""
    proto = _ensure_http_server(server_name, discovery=True)
    url, headers = _resolve_http(_server_cfg(server_name), discovery=True)
    if proto["mode"] == "modern":
        req = _modern_request("tools/list", {}, proto["version"])
    else:
        req = _legacy_request("tools/list", {})
    hdrs = _mcp_headers("tools/list", "", proto, headers)
    with httpx.Client(timeout=_HTTP_EXCHANGE_BUDGET) as client:
        deadline = time.monotonic() + _HTTP_EXCHANGE_BUDGET
        with client.stream("POST", url, json=req, headers=hdrs) as response:
            # A relay rejection here would otherwise surface as a bare 502
            # against the relay URL; decode it like the handshake path does.
            _msg = _relay_error(response, server_name)
            if _msg:
                raise RuntimeError(_msg)
            response.raise_for_status()
            result = _parse_http_reply(response, req["id"], server_name, deadline)
    if "error" in result:
        raise RuntimeError(f"tools/list error: {result['error']}")
    return (result.get("result") or {}).get("tools", [])


def _cli_main() -> None:
    """CLI dispatch: ``mcp_client.py discover <server_name> <output_path>``.

    Invoked from the generated epilogue, never from a module-level guard here:
    this file's source precedes ``_apply_config_dict`` in the composed client,
    so a guard at this point would dispatch against the placeholder config and
    every probe would report "unknown server".
    """
    if len(sys.argv) >= 4 and sys.argv[1] == "discover":
        _server, _out = sys.argv[2], sys.argv[3]
        _result = discover(_server)
        with open(_out, "w") as _f:
            json.dump(_result, _f)
        sys.exit(0)
    print(  # noqa: T201
        "usage: mcp_client.py discover <server_name> <output_path>",
        file=sys.stderr,
    )
    sys.exit(2)
