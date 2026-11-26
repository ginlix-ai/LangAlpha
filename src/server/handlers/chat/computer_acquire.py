"""A turn's computer, had before its run starts, and the run closed when a
stop or this process's drain ends the wait."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import HTTPException

from src.server.contracts.status import (
    RESTART_INTERRUPTED_META,
    is_restart_interrupted,
    run_end_stopper,
)
from src.server.services.runs import restart_drain
from src.server.services.runs.coordinator import protected_finalize
from src.server.services.runs.pending_input import read_pending_input

from .request_prep import logger


class StoppedAwaitingComputer(Exception):
    """The run was cancelled while its stream acquired its computer."""


def computer_unavailable(exc: Exception, shared: bool) -> Exception | None:
    """In a shared workspace, a failure to have the turn's computer worded
    for the person who sent it, or None to raise ``exc`` as it is: another
    workspace's, a refusal already worded (a host outage, a move, a stop, an
    HTTP answer), and any failure past the computer."""
    from ptc_agent.core.sandbox.runtime import SandboxHostUnavailableError
    from src.server.database.workspace import WorkspaceMovingError
    from src.server.services.computer_errors import (
        ComputerUnavailableError,
        MachineLockTimeout,
    )

    if not shared or isinstance(
        exc,
        (
            StoppedAwaitingComputer,
            HTTPException,
            SandboxHostUnavailableError,
            WorkspaceMovingError,
            ComputerUnavailableError,
        ),
    ):
        return None
    return ComputerUnavailableError(starting=isinstance(exc, MachineLockTimeout))


async def _unless_stopped(acquire: asyncio.Task, stopped: asyncio.Event):
    """``acquire``'s result, or :class:`StoppedAwaitingComputer` once
    ``stopped`` is set first. A session that arrived anyway is taken: the
    run's registration then applies the cancel, as it always has."""
    stop = asyncio.create_task(stopped.wait())
    try:
        await asyncio.wait({acquire, stop}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stop.cancel()
    if acquire.done():
        return acquire.result()
    raise StoppedAwaitingComputer


async def acquire_session(
    manager: Any,
    workspace_manager: Any,
    *,
    thread_id: str,
    run_id: str,
    workspace_id: str,
    runs_in: str,
    user_id: str,
    needs_startup: bool,
    skills_signature: Any,
    shared: bool,
) -> AsyncIterator[Any]:
    """The ``workspace_status`` frames of the wait, then the session.

    The frames name the turn's workspace (``workspace_id``), which is what the
    client shows; the session is the computer of the folder the turn runs in
    (``runs_in``), a stand-in's for a Home turn while Home is away.

    A cancel during the wait ends it here, on whichever worker the cancel
    lands, rather than once the provider answers, and so does this process's
    drain, which leaves the turn to the next process. The caller closes this
    generator when it stops early, so a dropped client still cancels the
    acquire."""
    try:
        with manager.awaiting_computer(thread_id, run_id) as stopped:
            if not needs_startup:
                acquire = asyncio.create_task(
                    workspace_manager.get_session_for_workspace(
                        runs_in,
                        user_id=user_id,
                        skills_signature=skills_signature,
                        run_id=run_id,
                        # The turn's first model call needs neither the asset
                        # sync nor the skill links; its calls on the computer
                        # wait for them at the gate (AssetGateMiddleware).
                        assets_later=True,
                    )
                )
                try:
                    session = await _unless_stopped(acquire, stopped)
                except BaseException:
                    if not acquire.done():
                        acquire.cancel()
                        with contextlib.suppress(BaseException):
                            await acquire
                    raise
            else:
                yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'starting', 'workspace_id': workspace_id})}\n\n"

                # Learn the pre-start sandbox state via a callback threaded
                # through session init → PTCSandbox.reconnect. The callback
                # fires once with the state string as soon as reconnect reads
                # it (before runtime.start() is invoked). We coordinate via
                # asyncio.Event and the acquisition race each other so a warm sibling
                # does not wait for a reconnect callback it can never emit.
                state_event = asyncio.Event()
                state_box: dict[str, str | None] = {"value": None}

                def _on_state(state: str) -> None:
                    state_box["value"] = state
                    state_event.set()

                session_task = asyncio.create_task(
                    workspace_manager.get_session_for_workspace(
                        runs_in,
                        user_id=user_id,
                        on_state_observed=_on_state,
                        skills_signature=skills_signature,
                        run_id=run_id,
                        assets_later=True,
                    )
                )

                try:

                    async def _wait_for_session():
                        return await asyncio.shield(session_task)

                    state_waiter = asyncio.create_task(state_event.wait())
                    session_waiter = asyncio.create_task(_wait_for_session())
                    stop_waiter = asyncio.create_task(stopped.wait())
                    try:
                        done, pending = await asyncio.wait(
                            {state_waiter, session_waiter, stop_waiter},
                            timeout=5.0,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    except BaseException:
                        state_waiter.cancel()
                        session_waiter.cancel()
                        stop_waiter.cancel()
                        await asyncio.gather(
                            state_waiter, session_waiter, stop_waiter,
                            return_exceptions=True,
                        )
                        raise
                    for waiter in pending:
                        waiter.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)

                    # In the message: the default log format drops extras.
                    logger.info(
                        f"[WS_STATUS] state observation workspace={workspace_id} "
                        f"sandbox_state={state_box['value']}",
                        extra={
                            "workspace_id": workspace_id,
                            "sandbox_state": state_box["value"],
                        },
                    )

                    if state_box["value"] == "archived":
                        yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'starting', 'workspace_id': workspace_id, 'sandbox_state': 'archived'})}\n\n"

                    session = (
                        await session_waiter
                        if session_waiter in done
                        else await _unless_stopped(session_task, stopped)
                    )
                    yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'ready', 'workspace_id': workspace_id})}\n\n"
                except BaseException:
                    # Client disconnect / GeneratorExit / any error during the
                    # yield or await chain above must not leak session_task.
                    # Cancel and drain to surface the outcome (or CancelledError).
                    if not session_task.done():
                        session_task.cancel()
                        with contextlib.suppress(BaseException):
                            await session_task
                    raise
    except Exception as exc:
        worded = computer_unavailable(exc, shared)
        if worded is None:
            raise
        raise worded from exc
    yield session


async def close_stopped_while_acquiring(
    scope: Any, comments: Any, manager: Any, thread_id: str, run_id: str
) -> str | None:
    """Close a run stopped while it acquired its computer; the status it
    settled as, None when it settled as nothing."""
    logger.info(
        f"[PTC_CHAT] Cancelled while acquiring its computer: "
        f"thread_id={thread_id} run_id={run_id}"
    )
    # Ended by this process's drain, unless a stop is on the row: the
    # finalize drops the restart mark for any stop stamped before it, a
    # user's or a suspension's, so such a stop still wins and is not
    # resumed (``is_restart_interrupted``).
    restart = (
        {"cancelled_by_user": False, RESTART_INTERRUPTED_META: True}
        if restart_drain.draining()
        else None
    )

    async def close_stopped() -> str | None:
        result = await scope.fail_open(
            "cancelled while acquiring its computer", metadata=restart
        )
        # A resume runs the turn's message again, the comments with it, so
        # only a turn nothing resumes gives them back. The resume declines
        # input the client alone can send again (``resume_verdict``).
        settled = result.run if result is not None else None
        pending = read_pending_input(settled)
        resumes = (
            is_restart_interrupted(settled)
            and pending is not None
            and not pending.needs_client
        )
        if comments is not None and not resumes:
            await comments.run_never_began()
        status = (result.run or {}).get("status") if result is not None else None
        if status:
            # The closing frames a registered run's cancel writes, read
            # back as its stream would be; with no settled status the
            # stream ends bare and the client reconnects to learn the
            # outcome.
            await manager.append_run_end_event(
                thread_id, run_id, status,
                stopped_by=run_end_stopper(settled),
            )
        return status

    # The close is every watcher's, not only this request's: the sender's
    # own Stop drops its read once its cancel is answered, which lands
    # while the close is still being written.
    return await protected_finalize(close_stopped(), "stopped-acquiring")
