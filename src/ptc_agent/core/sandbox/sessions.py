"""Preview servers, each run in a session of its own.

Functions take the owning ``PTCSandbox`` as their explicit first argument;
``PTCSandbox`` exposes same-name delegators, so call sites and patch
semantics are unchanged.
"""

import asyncio
import hashlib
import uuid
from typing import Any

import structlog


from ptc_agent.core.sandbox.preview_probe import app_answered
from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import (
    PreviewInfo,
    PreviewPortInUse,
    SessionCommandResult,
    SessionState,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


def _owner_tag(owner: str) -> str:
    """A fixed-length, dash-free stand-in for the owner, so it sits inside a
    session id whatever the owner's own spelling."""
    return hashlib.sha256(owner.encode("utf-8")).hexdigest()[:16]


def _preview_session_id(port: int, owner: str | None) -> str:
    """A preview session's id, naming the workspace whose app it runs.

    The machine lists the id to every worker alike and keeps it while the
    session runs, so it says whose session holds a port after the process
    that started it, and that process's own record, are gone.
    """
    tag = "" if owner is None else f"{_owner_tag(owner)}-"
    return f"preview-{port}-{tag}{uuid.uuid4().hex[:12]}"


def _session_owner_tag(session_id: str, port: int) -> str | None:
    """The owner tag in a preview session's id; None for one started with no
    owner, or before ids carried one."""
    tag, sep, _rest = session_id.removeprefix(f"preview-{port}-").partition("-")
    return tag if sep else None


async def _owned_port_sessions(
    sandbox: "PTCSandbox", port: int, owner: str | None
) -> list[SessionState]:
    """The owner's preview sessions on *port*, running or stopped, whichever
    process started them.

    The machine's list is the only one read: a process's own record goes stale
    the moment another worker replaces the session. Raises
    ``PreviewPortInUse`` while any other session runs there, one whose id
    names no owner included: what answers on the port may be that one, and
    nothing says whose it is. Stopped ones are the owner's to delete, since
    every start mints a new id and nothing else would. With no owner, none is
    provably the caller's, so none is listed.
    """
    if owner is None:
        return []
    assert sandbox.runtime is not None
    states = await sandbox._runtime_call(
        sandbox.runtime.list_sessions, retry_policy=RetryPolicy.SAFE
    )
    mine = _owner_tag(owner)
    on_port = [
        (state, _session_owner_tag(state.session_id, port))
        for state in states
        if state.session_id.startswith(f"preview-{port}-")
    ]
    if any(state.running and tag != mine for state, tag in on_port):
        raise PreviewPortInUse(port)
    return [state for state, tag in on_port if tag == mine]


async def _delete_session_quietly(sandbox: "PTCSandbox", session_id: str) -> None:
    assert sandbox.runtime is not None
    try:
        await sandbox._runtime_call(
            sandbox.runtime.delete_session,
            session_id,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception:
        logger.debug("Preview session cleanup failed", session_id=session_id)


async def get_preview_url(
    sandbox: "PTCSandbox", port: int, expires_in: int = 3600, *, wake: bool = True
) -> PreviewInfo:
    """Get a signed preview URL for a service running on the given port.

        Args:
            port: Port number (3000-9999) the service is listening on.
            expires_in: URL expiry in seconds (default: 3600 = 1 hour).
            wake: ``False`` for a caller that must not start a computer that
                stopped: one attempt and no reconnect, as ``get_preview_link``.

        Returns:
            PreviewInfo with url and token.
        """
    await sandbox._wait_ready()
    assert sandbox.runtime is not None
    return await sandbox._runtime_call(
        sandbox.runtime.get_preview_url,
        port,
        expires_in,
        retry_policy=RetryPolicy.SAFE,
        allow_reconnect=wake,
        retries=5 if wake else 1,
    )


async def get_preview_link(
    sandbox: "PTCSandbox", port: int, *, wake: bool = True
) -> PreviewInfo:
    """Get a standard preview URL with header-based auth token.

        Results are cached per-port since the standard URL doesn't change
        while the sandbox is running. Cache is cleared on sandbox restart.
        A loopback link is the exception: it is a forward this process holds
        on its connection to the machine, which a reconnect moves to another
        port, so only the runtime can say where it is now.
        ``wake=False`` is for probes: one attempt and no reconnect, because
        the reconnect that rescues a stopped sandbox would start it.
        """
    cached = sandbox._preview_link_cache.get(port)
    if cached is not None:
        return cached
    await sandbox._wait_ready()
    assert sandbox.runtime is not None
    result = await sandbox._runtime_call(
        sandbox.runtime.get_preview_link,
        port,
        retry_policy=RetryPolicy.SAFE,
        allow_reconnect=wake,
        retries=5 if wake else 1,
    )
    if not result.process_local:
        sandbox._preview_link_cache[port] = result
    return result


async def _create_session(sandbox: "PTCSandbox", session_id: str) -> None:
    assert sandbox.runtime is not None
    try:
        await sandbox._runtime_call(
            sandbox.runtime.create_session,
            session_id,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise
        # A retried create whose first attempt landed: delete and recreate,
        # so the session never inherits a command running in it.
        try:
            await sandbox._runtime_call(
                sandbox.runtime.delete_session,
                session_id,
                retry_policy=RetryPolicy.SAFE,
            )
            await sandbox._runtime_call(
                sandbox.runtime.create_session,
                session_id,
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception:
            logger.debug(
                "Stale preview session cleanup failed, reusing",
                session_id=session_id,
            )


async def start_preview_server(
    sandbox: "PTCSandbox",
    command: str,
    port: int,
    *,
    owner: str | None = None,
) -> str:
    """Start a command in a dedicated per-port session for preview URL serving.

        Each port gets its own Daytona session so blocking server commands
        (e.g. ``python -m http.server``) don't interfere with each other.
        If a session for this port already exists the old one is deleted first.

        Returns:
            The command ID from the session.
        """
    await sandbox._wait_ready()
    assert sandbox.runtime is not None

    session_id = _preview_session_id(port, owner)

    # Replace what runs on the port: the session this process started, and
    # every one of the owner's, another worker's included.
    owned = await _owned_port_sessions(sandbox, port, owner)
    known = sandbox._preview_sessions.pop(port, None)
    stale = [state.session_id for state in owned] + ([known] if known else [])
    for prior in dict.fromkeys(stale):
        await _delete_session_quietly(sandbox, prior)

    try:
        await _create_session(sandbox, session_id)
        cmd_id = await sandbox._runtime_call(
            sandbox.runtime.session_execute,
            session_id,
            command,
            retry_policy=RetryPolicy.UNSAFE,
            total_timeout=30,
        )
    except BaseException:
        # A session with no command lists as running, which would hold the
        # port against every other workspace. A cancel or a lost reply can
        # leave one made by either call.
        await asyncio.shield(_delete_session_quietly(sandbox, session_id))
        raise
    sandbox._preview_sessions[port] = session_id
    logger.info(
        "Preview server started",
        cmd_id=cmd_id,
        session_id=session_id,
        port=port,
    )
    return cmd_id


async def _is_preview_reachable(sandbox: "PTCSandbox", port: int, *, timeout: float = 3.0) -> bool:
    """Check if a preview port is reachable via the Daytona proxy.

        Uses the preview link (proxy URL + auth headers) to verify the server
        is accessible from outside the sandbox — not just locally.  A server
        binding to 127.0.0.1 passes an in-sandbox ``/dev/tcp`` check but
        returns 502 through the proxy.
        """
    import httpx

    from ptc_agent.core.sandbox.preview_probe import head_preview

    try:
        link = await sandbox.get_preview_link(port)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await head_preview(client, link.url, link.auth_headers)
            return app_answered(resp.status_code)
    except Exception:
        return False


async def start_and_get_preview_url(
    sandbox: "PTCSandbox",
    command: str,
    port: int,
    *,
    expires_in: int = 3600,
    startup_timeout: float = 10.0,
    owner: str | None = None,
) -> PreviewInfo:
    """Ports belong to the computer; callers must choose an unused one."""
    await sandbox._wait_ready()
    assert sandbox.runtime is not None

    if port not in sandbox._preview_locks:
        sandbox._preview_locks[port] = asyncio.Lock()
    async with sandbox._preview_locks[port]:
        occupied = await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"bash -c '(echo > /dev/tcp/localhost/{port}) >/dev/null 2>&1'",
            timeout=5,
            retry_policy=RetryPolicy.SAFE,
        )
        if occupied.exit_code == 0:
            owned = await _owned_port_sessions(sandbox, port, owner)
            if not any(state.running for state in owned):
                raise PreviewPortInUse(port)
            if await sandbox._is_preview_reachable(port):
                return await sandbox.get_preview_url(port, expires_in)
        await sandbox.start_preview_server(command, port, owner=owner)

        # Poll until the port is listening.
        # Uses bash built-in /dev/tcp (no external tools like nc needed) via
        # a single lightweight runtime.exec call with an internal retry loop.
        max_attempts = max(int(startup_timeout / 0.5), 1)
        try:
            result = await sandbox._runtime_call(
                sandbox.runtime.exec,
                f"bash -c 'for i in $(seq 1 {max_attempts}); do"
                    f" (echo > /dev/tcp/localhost/{port}) 2>/dev/null && echo READY && exit 0;"
                    f" sleep 0.5; done; echo TIMEOUT'",
                timeout=int(startup_timeout) + 5,
                retry_policy=RetryPolicy.SAFE,
            )
            if "READY" in result.stdout:
                logger.info("Preview server port ready", port=port)
            else:
                logger.warning(
                    "Preview server port not reachable after startup timeout",
                    port=port,
                    startup_timeout=startup_timeout,
                )
        except Exception:
            logger.warning(
                "Port readiness check failed, proceeding anyway",
                port=port,
                exc_info=True,
            )

        logs = await sandbox.get_preview_server_logs(port)
        if not logs.get("success") or logs.get("exit_code") is not None:
            raise RuntimeError(
                f"Preview command on port {port} failed. "
                + str(logs.get("stderr") or logs.get("stdout") or "Check the server command.")
            )
        return await sandbox.get_preview_url(port, expires_in=expires_in)


async def get_preview_server_logs(sandbox: "PTCSandbox", port: int) -> dict[str, Any]:
    """Get logs for the preview server running on the given port.

        A preview session runs one command, so its latest is the server.

        Returns:
            Dict with keys: success, is_running, stdout, stderr, port.
        """
    session_id = sandbox._preview_sessions.get(port)
    missing = {
        "success": False,
        "is_running": False,
        "stdout": "",
        "stderr": f"No preview session for port {port}",
        "port": port,
    }
    if session_id is None:
        return missing
    await sandbox._wait_ready()
    assert sandbox.runtime is not None
    try:
        result: SessionCommandResult | None = await sandbox._runtime_call(
            sandbox.runtime.session_logs,
            session_id,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception as e:
        return {**missing, "stderr": f"Failed to get logs: {e!s}"}
    if result is None:
        return missing
    return {
        "success": True,
        "is_running": result.exit_code is None,
        "exit_code": result.exit_code,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "port": port,
    }


async def stop_preview_server(sandbox: "PTCSandbox", port: int) -> bool:
    """Stop the preview server on the given port by deleting its session.

        Returns True if the session was found and deleted.
        """
    session_id = sandbox._preview_sessions.get(port)
    if session_id is None:
        return False
    await sandbox._wait_ready()
    assert sandbox.runtime is not None
    try:
        await sandbox._runtime_call(
            sandbox.runtime.delete_session,
            session_id,
            retry_policy=RetryPolicy.SAFE,
        )
        logger.info("Preview server stopped", port=port, session_id=session_id)
    except Exception:
        logger.debug("Failed to delete preview session", session_id=session_id)
    sandbox._preview_sessions.pop(port, None)
    return True
