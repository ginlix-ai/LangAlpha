"""Preview servers and background command sessions.

Functions take the owning ``PTCSandbox`` as their explicit first argument;
``PTCSandbox`` exposes same-name delegators, so call sites and patch
semantics are unchanged.
"""

import asyncio
import shlex
from typing import Any

import structlog


from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import (
    PreviewInfo,
    SessionCommandResult,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


async def get_preview_url(sandbox: "PTCSandbox", port: int, expires_in: int = 3600) -> PreviewInfo:
    """Get a signed preview URL for a service running on the given port.

        Args:
            port: Port number (3000-9999) the service is listening on.
            expires_in: URL expiry in seconds (default: 3600 = 1 hour).

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
    )


async def get_preview_link(sandbox: "PTCSandbox", port: int) -> PreviewInfo:
    """Get a standard preview URL with header-based auth token.

        Results are cached per-port since the standard URL doesn't change
        while the sandbox is running. Cache is cleared on sandbox restart.
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
    )
    sandbox._preview_link_cache[port] = result
    return result


async def start_preview_server(sandbox: "PTCSandbox", command: str, port: int) -> str:
    """Start a command in a dedicated per-port session for preview URL serving.

        Each port gets its own Daytona session so blocking server commands
        (e.g. ``python -m http.server``) don't interfere with each other.
        If a session for this port already exists the old one is deleted first.

        Returns:
            The command ID from the session.
        """
    await sandbox._wait_ready()
    assert sandbox.runtime is not None

    session_id = f"preview-{port}"

    # Tear down stale session for this port if one exists
    if port in sandbox._preview_sessions:
        old_sid, _old_cmd = sandbox._preview_sessions[port]
        try:
            await sandbox._runtime_call(
                sandbox.runtime.delete_session,
                old_sid,
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception:
            logger.debug("Stale preview session cleanup failed", port=port)
        del sandbox._preview_sessions[port]

    try:
        await sandbox._runtime_call(
            sandbox.runtime.create_session,
            session_id,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception as e:
        if "already exists" in str(e).lower():
            # Stale session from a previous server process — delete and
            # recreate to avoid inheriting a running command from the old
            # session (same pattern as _create_bg_session).
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
        else:
            raise

    result = await sandbox._runtime_call(
        sandbox.runtime.session_execute,
        session_id,
        command,
        run_async=True,
        retry_policy=RetryPolicy.UNSAFE,
        total_timeout=30,
    )
    sandbox._preview_sessions[port] = (session_id, result.cmd_id)
    logger.info(
        "Preview server started",
        cmd_id=result.cmd_id,
        session_id=session_id,
        port=port,
    )
    return result.cmd_id


async def _is_preview_reachable(sandbox: "PTCSandbox", port: int, *, timeout: float = 3.0) -> bool:
    """Check if a preview port is reachable via the Daytona proxy.

        Uses the preview link (proxy URL + auth headers) to verify the server
        is accessible from outside the sandbox — not just locally.  A server
        binding to 127.0.0.1 passes an in-sandbox ``/dev/tcp`` check but
        returns 502 through the proxy.
        """
    import httpx

    try:
        link = await sandbox.get_preview_link(port)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.head(
                link.url,
                headers=link.auth_headers,
                follow_redirects=True,
            )
            # 4xx means the server IS running (path not found, etc.)
            # 5xx (especially 502) means the proxy can't reach the backend
            return 200 <= resp.status_code < 500 and resp.status_code != 502
    except Exception:
        return False


async def start_and_get_preview_url(
    sandbox: "PTCSandbox",
    command: str,
    port: int,
    *,
    expires_in: int = 3600,
    startup_timeout: float = 10.0,
) -> PreviewInfo:
    """Start a server command in background and return a signed preview URL.

        Combines start_preview_server + port readiness poll + get_preview_url.
        If the port is already reachable through the Daytona proxy the server
        start is skipped entirely, making this method safe to call repeatedly.

        Polls for up to ``startup_timeout`` seconds to confirm the port is
        actually listening before generating the URL.  If the port never
        becomes reachable the URL is still returned — the frontend
        health-check polling handles dead-server detection.

        If the server command fails (e.g. port already in use), the preview
        URL is still generated — the existing server keeps serving.
        """
    await sandbox._wait_ready()
    assert sandbox.runtime is not None

    if port not in sandbox._preview_locks:
        sandbox._preview_locks[port] = asyncio.Lock()
    async with sandbox._preview_locks[port]:
        # Quick probe: is the server already reachable through the proxy?
        # This catches the common case where the server is already running
        # and avoids an unnecessary (destructive) session teardown + restart.
        # We check the proxy — not an in-sandbox /dev/tcp — because a server
        # binding to 127.0.0.1 would pass the in-sandbox check but return 502
        # through the proxy.
        if await sandbox._is_preview_reachable(port):
            logger.info("Preview already reachable via proxy, skipping server start", port=port)
            return await sandbox.get_preview_url(port, expires_in=expires_in)

        try:
            await sandbox.start_preview_server(command, port)
        except Exception as e:
            logger.warning("Failed to start preview server", command=command, error=str(e))

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

        return await sandbox.get_preview_url(port, expires_in=expires_in)


async def _evict_finished_bg_sessions(sandbox: "PTCSandbox") -> None:
    """Evict finished background sessions to stay under the cap."""
    assert sandbox.runtime is not None
    # Collect finished sessions (skip sentinel keys)
    finished: list[str] = []
    for cmd_id, sid in list(sandbox._bg_sessions.items()):
        if cmd_id.startswith("_pending:"):
            continue
        try:
            result = await sandbox._runtime_call(
                sandbox.runtime.session_command_logs,
                sid, cmd_id,
                retry_policy=RetryPolicy.SAFE,
            )
            if result.exit_code is not None:
                finished.append(cmd_id)
        except Exception:
            # Can't check status (e.g. sandbox restarted) — treat as
            # finished to avoid zombie entries that permanently block the cap.
            finished.append(cmd_id)
    # Delete finished sessions. A finished command's MCP trace is normally
    # harvested by the BashOutput that observes completion (provenance is
    # attributed to that observation). A command that finished but was never
    # observed before the cap forced eviction has no tool-call surface to
    # attribute its trace to, so it's dropped — log it so the rare provenance
    # gap is observable, not silent. (Faithful harvest-on-evict would need a
    # deferred-trace channel keyed to the original launch; out of scope here.)
    for cmd_id in finished:
        dropped_trace = sandbox._bg_trace_paths.pop(cmd_id, None)
        if dropped_trace:
            logger.info(
                "Evicting finished bg session with unharvested MCP trace",
                cmd_id=cmd_id,
            )
            # The pop above drops the last host-side reference to this trace,
            # so delete the JSONL now — otherwise it leaks on the sandbox
            # until teardown (no later path knows the filename to reap it).
            try:
                await sandbox._runtime_call(
                    sandbox.runtime.exec,
                    f"rm -f {shlex.quote(dropped_trace)}",
                    retry_policy=RetryPolicy.SAFE,
                )
            except Exception:
                logger.debug("Evict bg trace cleanup failed", path=dropped_trace)
        sid = sandbox._bg_sessions.pop(cmd_id, None)
        if sid:
            try:
                await sandbox._runtime_call(
                    sandbox.runtime.delete_session, sid,
                    retry_policy=RetryPolicy.SAFE,
                )
            except Exception:
                logger.debug("Evict bg session failed", session_id=sid)


async def _create_bg_session(sandbox: "PTCSandbox", label: str) -> str:
    """Create a dedicated session for a background command.

        Each background command gets its own Daytona session so blocking
        commands don't prevent subsequent ones from executing.
        Evicts finished sessions when the cap is reached.
        """
    await sandbox._wait_ready()
    assert sandbox.runtime is not None

    # Evict finished sessions if at or above the cap
    active_count = sum(1 for k in sandbox._bg_sessions if not k.startswith("_pending:"))
    if active_count >= sandbox._MAX_BG_SESSIONS:
        await sandbox._evict_finished_bg_sessions()

    session_id = f"bg-{label}"
    try:
        await sandbox._runtime_call(
            sandbox.runtime.create_session,
            session_id,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception as e:
        if "already exists" in str(e).lower():
            # Stale session from a previous run — delete and recreate
            # to avoid inheriting env/state from the old session
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
                logger.debug("Stale bg session cleanup failed, reusing", session_id=session_id)
        else:
            raise
    return session_id


async def get_background_command_status(sandbox: "PTCSandbox", cmd_id: str) -> dict[str, Any]:
    """Get status and logs for a background command.

        Args:
            cmd_id: Command ID returned when the background command was started.

        Returns:
            Dict with keys: success, is_running, exit_code, stdout, stderr, cmd_id.
        """
    await sandbox._wait_ready()
    assert sandbox.runtime is not None

    session_id = sandbox._bg_sessions.get(cmd_id)
    if not session_id:
        return {
            "success": False,
            "is_running": False,
            "exit_code": None,
            "stdout": "",
            "stderr": "No background session found for this command",
            "cmd_id": cmd_id,
            "mcp_trace": [],
        }

    result: SessionCommandResult = await sandbox._runtime_call(
        sandbox.runtime.session_command_logs,
        session_id,
        cmd_id,
        retry_policy=RetryPolicy.SAFE,
    )
    is_running = result.exit_code is None

    # Harvest the backgrounded command's MCP provenance trace exactly once,
    # when it finishes. This rides the same status path that returns the
    # command's output to the agent, so there's no result-bearing path that
    # skips provenance (the stop action returns no output). Best-effort.
    mcp_trace: list[dict] = []

    # Auto-clean: if the command finished (e.g. killed via pkill), tear
    # down the orphaned session so it doesn't leak on the Daytona side.
    if not is_running:
        trace_path = sandbox._bg_trace_paths.pop(cmd_id, None)
        if trace_path:
            mcp_trace = await sandbox._collect_mcp_trace(trace_path)
        sid = sandbox._bg_sessions.pop(cmd_id, None)
        if sid:
            try:
                await sandbox._runtime_call(
                    sandbox.runtime.delete_session, sid,
                    retry_policy=RetryPolicy.SAFE,
                )
            except Exception:
                logger.debug("Auto-clean bg session failed", session_id=sid)

    return {
        "success": not is_running and result.exit_code == 0,
        "is_running": is_running,
        "exit_code": result.exit_code,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "cmd_id": cmd_id,
        "mcp_trace": mcp_trace,
    }


async def stop_background_command(sandbox: "PTCSandbox", cmd_id: str) -> bool:
    """Stop a background command by deleting its session.

        Returns True if the session was found and deleted.
        """
    session_id = sandbox._bg_sessions.get(cmd_id)
    if not session_id:
        return False
    await sandbox._wait_ready()
    assert sandbox.runtime is not None
    # Drop the trace mapping (the ephemeral sandbox FS owns the file itself).
    # A stopped command yields no output, so there's nothing to attest.
    sandbox._bg_trace_paths.pop(cmd_id, None)
    try:
        await sandbox._runtime_call(
            sandbox.runtime.delete_session,
            session_id,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception:
        logger.warning("Failed to delete bg session", session_id=session_id)
        sandbox._bg_sessions.pop(cmd_id, None)
        return False
    sandbox._bg_sessions.pop(cmd_id, None)
    return True


async def get_preview_server_logs(sandbox: "PTCSandbox", port: int) -> dict[str, Any]:
    """Get logs for the preview server running on the given port.

        Returns:
            Dict with keys: success, is_running, stdout, stderr, port.
        """
    entry = sandbox._preview_sessions.get(port)
    if not entry:
        return {
            "success": False,
            "is_running": False,
            "stdout": "",
            "stderr": f"No preview session for port {port}",
            "port": port,
        }
    session_id, cmd_id = entry
    await sandbox._wait_ready()
    assert sandbox.runtime is not None
    try:
        result: SessionCommandResult = await sandbox._runtime_call(
            sandbox.runtime.session_command_logs,
            session_id,
            cmd_id,
            retry_policy=RetryPolicy.SAFE,
        )
        is_running = result.exit_code is None
        return {
            "success": True,
            "is_running": is_running,
            "exit_code": result.exit_code,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "port": port,
        }
    except Exception as e:
        return {
            "success": False,
            "is_running": False,
            "stdout": "",
            "stderr": f"Failed to get logs: {e!s}",
            "port": port,
        }


async def stop_preview_server(sandbox: "PTCSandbox", port: int) -> bool:
    """Stop the preview server on the given port by deleting its session.

        Returns True if the session was found and deleted.
        """
    entry = sandbox._preview_sessions.get(port)
    if not entry:
        return False
    session_id, _cmd_id = entry
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
