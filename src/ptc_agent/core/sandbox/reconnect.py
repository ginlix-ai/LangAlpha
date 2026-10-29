"""Reconnecting a sandbox handle to its machine, and keeping to one at a time.

The state lives on the ``PTCSandbox`` (``_reconnect_task``,
``_reconnects_running``, ``_reconnect_incomplete``, ``_kept_for_salvage``);
these functions take it as their first argument, as ``assets.py`` and
``mcp_setup.py`` do. What a caller or test reaches through the sandbox
stays there, ``reconnect`` and ``_ensure_sandbox_connected`` as delegators
and ``_reset_provider`` whole, so patching those, or that module's
``create_provider``, still takes effect here.
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

import structlog

from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import (
    SandboxFailureKind,
    SandboxGoneError,
    SandboxHostLostError,
    SandboxHostUnavailableError,
    SandboxTransientError,
)

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


async def reconnect(
    sandbox: PTCSandbox,
    sandbox_id: str,
    on_state_observed: Callable[[str], None] | None = None,
) -> None:
    """``PTCSandbox.reconnect``, which documents it."""
    # Counted so a call that succeeds inside it, before it completes, is
    # not taken for the session serving again (``call_served``), and so a
    # transient inside it starts no second one (``ensure_connected``).
    sandbox._reconnects_running += 1
    try:
        await _reconnect(sandbox, sandbox_id, on_state_observed)
    finally:
        sandbox._reconnects_running -= 1


async def _reconnect(
    sandbox: PTCSandbox,
    sandbox_id: str,
    on_state_observed: Callable[[str], None] | None,
) -> None:
    logger.debug("Reconnecting to stopped sandbox", sandbox_id=sandbox_id)

    _t0 = time.time()
    _rc_phases: dict[str, float] = {}

    def _mark_rc(name: str) -> None:
        nonlocal _t0
        now = time.time()
        _rc_phases[name] = (now - _t0) * 1000
        _t0 = now

    sandbox._reconnect_incomplete = True

    # Clear stale state: sessions and preview links don't survive stop/start
    sandbox._preview_sessions.clear()
    sandbox._preview_link_cache.clear()

    # Get the existing sandbox via provider
    try:
        sandbox.runtime = await sandbox._runtime_call(
            sandbox.provider.get,
            sandbox_id,
            retry_policy=RetryPolicy.SAFE,
            allow_reconnect=False,
        )
    except SandboxTransientError:
        # A transport blip is NOT a missing sandbox. Converting every failure
        # here into SandboxGoneError made a network hiccup trigger recovery,
        # which creates a replacement sandbox and abandons the real one, and
        # several workers can do it at once.
        raise
    except Exception as e:
        # Only a positively identified absence may authorize Gone. The caller
        # answers Gone by building a replacement and abandoning this sandbox,
        # so an undecidable failure must not reach it: UNKNOWN is where a
        # rotated or under-privileged provider key (401/403) lands, and there
        # the sandbox is fine and the credential is not. Treating that as
        # absence recreates a live sandbox on every request and leaks the
        # original each time.
        if sandbox.provider.classify_error(e) is SandboxFailureKind.SANDBOX_GONE:
            raise SandboxGoneError(sandbox_id, f"not found: {e}") from e
        raise SandboxTransientError(f"Could not reach sandbox {sandbox_id}: {e}") from e
    _mark_rc("provider_get")

    assert sandbox.runtime is not None
    await sandbox.provider.prepare_reconnect(
        sandbox.runtime, tier=sandbox.resource_tier
    )
    sandbox.sandbox_id = sandbox_id

    # Check sandbox state before attempting to start
    state = await sandbox.runtime.get_state()
    state_value = state.value
    _mark_rc("get_state")

    if on_state_observed is not None:
        try:
            # An archive in progress ends in a restore from archived, so it
            # gets the same slow-restore notice.
            on_state_observed("archived" if state_value == "archiving" else state_value)
        except Exception:
            logger.debug(
                "on_state_observed callback raised; ignoring",
                sandbox_id=sandbox_id,
            )

    if state_value != "running":
        sandbox.livefs_lost = True
        sandbox.livefs = None
    if state_value == "running":
        logger.debug("Sandbox already started, skipping start", sandbox_id=sandbox_id)
    elif state_value == "stopped":
        logger.debug(
            "Starting stopped sandbox", sandbox_id=sandbox_id, state=state_value
        )
        await _start_runtime(sandbox, sandbox_id, sandbox.runtime.start, timeout=60)
        _mark_rc("start")
    elif state_value == "starting":
        # Sandbox is already transitioning: wait for it to reach 'running'.
        logger.debug(
            "Sandbox is starting, waiting for ready",
            sandbox_id=sandbox_id,
        )
        for _ in range(40):  # Max ~20 seconds
            await asyncio.sleep(0.5)
            sandbox.runtime = await sandbox._runtime_call(
                sandbox.provider.get,
                sandbox_id,
                retry_policy=RetryPolicy.SAFE,
                allow_reconnect=False,
            )
            state = await sandbox.runtime.get_state()
            state_value = state.value
            if state_value == "running":
                break
        if state_value != "running":
            # Transient, not gone: the state was just read back, which is
            # positive evidence the sandbox exists. ``SandboxGoneError`` is
            # the authorization to replace, and its handlers in
            # ``workspace_manager`` call ``_clear_session`` first, which
            # deletes the runtime, so calling a slow boot "gone" destroys a
            # live sandbox and restores it from a DB backup. The word
            # "starting" is load-bearing: it is what renders the retry card.
            raise SandboxTransientError(
                f"Sandbox {sandbox_id} is still starting "
                f"(state '{state_value}' after ~20s)"
            )
        _mark_rc("wait_starting")
    elif state_value == "stopping":
        # Wait for sandbox to finish stopping, then start it.
        logger.info(
            "Sandbox is stopping, waiting before start",
            sandbox_id=sandbox_id,
        )
        for _ in range(20):  # Max ~10 seconds
            await asyncio.sleep(0.5)
            sandbox.runtime = await sandbox._runtime_call(
                sandbox.provider.get,
                sandbox_id,
                retry_policy=RetryPolicy.SAFE,
                allow_reconnect=False,
            )
            state = await sandbox.runtime.get_state()
            state_value = state.value
            if state_value == "stopped":
                break
        if state_value == "stopped":
            logger.info(
                "Sandbox finished stopping, starting it",
                sandbox_id=sandbox_id,
            )
            await _start_runtime(sandbox, sandbox_id, sandbox.runtime.start, timeout=60)
        else:
            # Same reasoning as the 'starting' wait above: still mid-stop is
            # a sandbox that exists, so this is a retry, not a replacement.
            raise SandboxTransientError(
                f"Sandbox {sandbox_id} has not finished stopping "
                f"(state '{state_value}' after ~10s)"
            )
        _mark_rc("wait_stopping")
    elif state_value == "archived":
        # Always restore. Daytona's ``recoverable`` flag describes an
        # errored sandbox and reads False on every healthy one, so gating
        # on it called each auto-archived sandbox gone: the recover path
        # rebuilt it from the DB backup, dropping whatever only the archive
        # held, and orphaned the archive. A restore that truly fails is
        # classified like any start, Gone only on a positive absence.
        logger.info(
            "Starting archived sandbox (restore may take longer)",
            sandbox_id=sandbox_id,
        )
        await _start_runtime(sandbox, sandbox_id, sandbox.runtime.start, timeout=300)
        _mark_rc("start_archived")
    elif state_value == "archiving":
        # Never started mid-archive: a start that lands once Daytona has
        # removed the container errors the sandbox beyond recovery, which
        # reads as gone and rebuilds it from the backup. Wait for the
        # archive to land, then restore it like any archived sandbox.
        logger.info(
            "Sandbox is being archived, waiting before restore",
            sandbox_id=sandbox_id,
        )
        # A deadline, not a poll count: each poll is also a provider read,
        # and the restore after it has its own 300s budget.
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            sandbox.runtime = await sandbox._runtime_call(
                sandbox.provider.get,
                sandbox_id,
                retry_policy=RetryPolicy.SAFE,
                allow_reconnect=False,
            )
            state = await sandbox.runtime.get_state()
            state_value = state.value
            if state_value != "archiving":
                break
        # A cancelled archive ends stopped, which starts like any other,
        # or running, when something else already started it.
        if state_value not in ("archived", "stopped", "running"):
            # Still archiving is a sandbox that exists, so a retry, not a
            # replacement; any other state is the next open's to handle.
            logger.warning(
                "Sandbox did not finish archiving",
                sandbox_id=sandbox_id,
                state=state_value,
            )
            raise SandboxTransientError(
                f"Sandbox {sandbox_id} is being archived "
                f"(state '{state_value}' after ~45s); try again in a few minutes"
            )
        if state_value != "running":
            await _start_runtime(
                sandbox, sandbox_id, sandbox.runtime.start, timeout=300
            )
        _mark_rc("start_archiving")
    elif state_value == "error":
        # Restarting an errored sandbox the provider will not recover fails
        # the same way on every open, so the runtime answers Gone and the
        # caller rebuilds from the backup. A recovery can restore from the
        # provider's own backup, so it gets the archive restore's budget.
        logger.warning(
            "Sandbox in error state, attempting recovery",
            sandbox_id=sandbox_id,
        )
        await _start_runtime(
            sandbox, sandbox_id, sandbox.runtime.recover_from_error, timeout=300
        )
        _mark_rc("start_error_recovery")
    else:
        raise SandboxGoneError(
            sandbox_id,
            f"unrecoverable state: {state_value}",
        )
    await _confirm_host_back(sandbox, sandbox_id)

    # Fetch the actual working dir from the sandbox. The config default
    # may differ from the real dir (e.g. /home/workspace vs /home/daytona)
    # when the sandbox was created under a previous config.
    sandbox._work_dir = await sandbox.runtime.fetch_working_dir()
    _mark_rc("fetch_workdir")

    total = sum(_rc_phases.values())
    phases = " ".join(f"{k}={v:.0f}ms" for k, v in _rc_phases.items())
    logger.info(
        f"[RECONNECT] sandbox_id={sandbox_id} state={state_value} "
        f"total={total:.0f}ms ({phases})"
    )

    # SKIP: _setup_workspace() - directories already exist
    # SKIP: _upload_mcp_server_files() - files already uploaded
    # SKIP: _install_tool_modules() - tool modules already installed

    # Before the MCP servers start, which read their keys from it; a daemon
    # already running holds the old env, so a change replaces it.
    env_changed = await sandbox.converge_env()

    # Initialize MCP server sessions (needed for tool execution)
    sandbox.mcp_server_sessions: dict[str, Any] = {}
    if await sandbox._start_internal_mcp_servers(replace=env_changed) is False:
        sandbox.livefs_lost = True
        sandbox.livefs = None
    sandbox._reconnect_incomplete = False

    logger.debug(
        "Sandbox started from stopped state",
        sandbox_id=sandbox.sandbox_id,
    )


async def _start_runtime(
    sandbox: PTCSandbox, sandbox_id: str, start: Callable[..., Any], *, timeout: int
) -> None:
    """Start the sandbox, telling a recovering host apart from other failures.

    Such a host refuses every start until it is back, so the policy decides
    between waiting for it (transient) and rebuilding elsewhere (gone). The
    policy failing is no answer, and waiting is the default.
    """
    try:
        # Only reconnect calls this, and a reconnect from inside it would
        # replace the provider the one running is using.
        await sandbox._runtime_call(
            start,
            timeout=timeout,
            retry_policy=RetryPolicy.SAFE,
            allow_reconnect=False,
        )
    except SandboxGoneError:
        raise
    except Exception as e:
        if not sandbox.provider.is_host_unavailable(e):
            raise
        outage_since = None
        if sandbox.host_unavailable_policy is not None:
            try:
                outage_since = await sandbox.host_unavailable_policy.rebuild_now(
                    sandbox_id
                )
            except Exception:
                logger.warning(
                    "Host-unavailable policy failed; waiting for the host",
                    sandbox_id=sandbox_id,
                    exc_info=True,
                )
        if isinstance(outage_since, datetime):
            sandbox._kept_for_salvage = True
            raise SandboxHostLostError(
                sandbox_id,
                f"host unavailable: {e}",
                outage_since=outage_since,
            ) from e
        raise SandboxHostUnavailableError(sandbox_id, str(e)) from e


async def _confirm_host_back(sandbox: PTCSandbox, sandbox_id: str) -> None:
    """End the outage clock the moment the sandbox is up, before anything runs on it.

    Later setup can fail and leave the sandbox up anyway, and a clock left
    running cuts the next outage short. A rebuild authorized meanwhile may
    already have replaced the sandbox; then it is no longer the machine's
    and must be neither used nor deleted, and is stopped so it does not
    run unowned. Still the machine's, it is deleted with the machine again,
    even after this handle once gave it up. Without the answer neither is
    known, so the reconnect fails and is retried.
    """
    if sandbox.host_unavailable_policy is None:
        return
    try:
        current = await sandbox.host_unavailable_policy.reconnected(sandbox_id)
    except Exception as e:
        raise SandboxTransientError(
            f"Could not record that the host of sandbox {sandbox_id} is back"
        ) from e
    if current is False:
        sandbox._kept_for_salvage = True
        try:
            # Nothing else stops it: no row names it any more.
            await sandbox._runtime_call(
                sandbox.runtime.stop,
                retry_policy=RetryPolicy.SAFE,
                allow_reconnect=False,
            )
        except Exception:
            logger.warning(
                "Could not stop a sandbox a rebuild replaced",
                sandbox_id=sandbox_id,
                exc_info=True,
            )
        raise SandboxGoneError(
            sandbox_id, "replaced by a rebuild while its host was unavailable"
        )
    if current is True:
        sandbox._kept_for_salvage = False


async def ensure_connected(sandbox: PTCSandbox) -> None:
    """Reconnect after a transient, joining a reconnect already in flight.

    A caller arriving while one runs takes its outcome, success or failure,
    rather than running another after it; one arriving once it has ended
    reconnects anew. Shielded, so a caller that gives up does not cancel
    the reconnect the others are waiting on.
    """
    if sandbox.sandbox_id is None:
        raise SandboxTransientError(
            "Sandbox disconnected and no sandbox_id is available"
        )
    task = sandbox._reconnect_task
    joinable = task is not None and not task.done()
    # A call inside a reconnect met a transient: joining its own would wait
    # on itself, and one a caller ran directly is no task to join, where a
    # fresh one would replace the provider it is using. The call's own
    # retries are all it gets.
    if (joinable and task is asyncio.current_task()) or (
        not joinable and sandbox._reconnects_running
    ):
        raise SandboxTransientError(
            f"Sandbox {sandbox.sandbox_id} is already reconnecting"
        )
    if not joinable:
        task = asyncio.create_task(_reconnect_fresh(sandbox, sandbox.sandbox_id))
        task.add_done_callback(functools.partial(_reconnect_settled, sandbox))
        sandbox._reconnect_task = task
    await asyncio.shield(task)


async def _reconnect_fresh(sandbox: PTCSandbox, sandbox_id: str) -> None:
    # This only runs after a transient error, so the existing client may be
    # dead or stale.
    await sandbox._reset_provider()
    try:
        await sandbox.reconnect(sandbox_id)
    except SandboxGoneError as e:
        sandbox._lost = e
        raise
    sandbox._lost = None


def _reconnect_settled(sandbox: PTCSandbox, task: asyncio.Task[None]) -> None:
    if sandbox._reconnect_task is task:
        sandbox._reconnect_task = None
    if not task.cancelled():
        # Every caller may have stopped waiting, so the outcome is
        # retrieved here too, or asyncio reports it as never retrieved.
        task.exception()


def call_served(sandbox: PTCSandbox) -> None:
    """Record a call that reached the machine outside any reconnect.

    A reconnect that failed is not the last word: a machine back from an
    outage answers the next call on a fresh connection with no reconnect,
    and a session serving again is torn down like any other, rather than
    kept on the machine after its removal.
    """
    if sandbox._reconnect_incomplete and not sandbox._reconnects_running:
        sandbox._reconnect_incomplete = False
