"""
AutomationExecutor — Executes a single automation run.

Shared by all trigger types (time-based now, event-based later).
Builds a ChatRequest, invokes the appropriate agent workflow,
and drains the async generator (no HTTP client to consume SSE).
Every way a firing ends goes through ``automation_settlement``: once its turn
reached the run ledger, by the run's own finalize.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import HTTPException

from src.server.database import automation as auto_db
from src.server.database.api_keys import is_byok_active
from src.server.database.oauth_tokens import has_any_oauth_token
from src.server.database.runs import lifecycle as tl_db
from src.server.database.workspace import get_or_create_flash_workspace
from src.server.dependencies.usage_limits import enforce_credit_limit
from src.server.models.chat import ChatMessage, ChatRequest, ThreadOrigin
from src.server.services.automation_settlement import (
    Outcome,
    announce_wait,
    clean_error_text,
    fire_webhook,
    settle,
)
from src.observability.tracing import hash_id as _obs_hash_id, tracer as _otel_tracer

logger = logging.getLogger(__name__)

# How long a firing waits for its thread's running turn to end before it is
# skipped, and how often it looks.
_WAIT_CAP_SECONDS = 30 * 60
_WAIT_POLL_SECONDS = 5

# The process holding a firing touches its row this often, and the sweep
# settles one quiet for ABANDONED_AFTER_SECONDS. The margin is for event-loop
# stalls, which delay a beat without the process being gone.
_HEARTBEAT_SECONDS = 60
ABANDONED_AFTER_SECONDS = 5 * 60

# Admission 409s that mean "a turn holds this thread": get back in line.
_THREAD_BUSY_CODES = frozenset({"running", "stopping", "compacting"})

# 429s that are our capacity rather than the user's usage limit.
_OUR_429_TYPES = frozenset({"burst_limit", "service_unavailable"})

# Automations a firing may still run under after a wait. Any other status,
# unless the firing started under it (a manual run of a paused automation),
# means it was paused, disabled or finished meanwhile.
_RUNNABLE = frozenset({"active", "executing"})

# ``_look_again``'s answer while the thread is still busy.
_STILL_WAITING = object()


@dataclass
class _Firing:
    """How far a firing got, so it settles against the thread and run it
    reached whichever way it ends.

    ``automation`` is re-read after every wait. ``agent_mode`` stays the one
    the thread and workspace were resolved for.
    """

    automation: Dict[str, Any]
    agent_mode: str
    workspace_id: Optional[str] = None
    thread_id: Optional[str] = None
    run_id: Optional[str] = None
    # The run ledger holds ``run_id`` and the firing records it: the turn went
    # ahead, and the firing now ends however that run ends.
    admitted: bool = False


async def _thread_busy(thread_id: str) -> bool:
    """A turn is live on the thread, or a compact or offload is rewriting it.

    Both are read where every worker sees them: the run ledger and the
    mutation registry's advertised op.
    """
    from src.server.services.thread_mutation import ThreadMutationRunner

    if await tl_db.get_active_run(thread_id) is not None:
        return True
    return await ThreadMutationRunner.get_instance().is_mutating(thread_id)


async def _keep_alive(execution_id: str) -> None:
    while True:
        await asyncio.sleep(_HEARTBEAT_SECONDS)
        try:
            await auto_db.touch_execution(execution_id)
        except Exception as e:
            # One missed beat is harmless; the sweep waits for five.
            logger.warning(
                f"[AUTOMATION_EXEC] Heartbeat failed: "
                f"execution_id={execution_id} error={e}"
            )


async def _link_run(execution_id: str, run_id: str) -> bool:
    """Record the firing's run on its execution; False once the firing is no
    longer running.

    The one writer of the link. The run's finalize settles the firing either
    way; the link lets the live run be opened from the firing, and lets the
    sweep settle the firing by that run should its finalize job never run.
    """
    return (
        await auto_db.transition_execution(
            execution_id,
            from_statuses=("running",),
            to="running",
            conversation_response_id=run_id,
        )
        is not None
    )


def _lost_thread_race(e: BaseException) -> bool:
    """Someone else's turn took the thread between the idle check and this
    turn's admission: the busy 409, or the run slot the ledger holds for a
    live run."""
    from src.server.database.runs.lifecycle import RunSlotBusyError
    from src.server.database.runs.subagent_runs import TaskRunSlotBusyError

    if isinstance(e, (RunSlotBusyError, TaskRunSlotBusyError)):
        return True
    return (
        isinstance(e, HTTPException)
        and e.status_code == 409
        and isinstance(e.detail, dict)
        and e.detail.get("code") in _THREAD_BUSY_CODES
    )


def _limit_message(e: BaseException) -> Optional[str]:
    """The quota service's words when ``e`` is a usage limit refusing the
    firing; None for anything else.

    Relayed verbatim: which limit applies and how to say so is the
    service's, and the credit gate's own fallback stands in only when a
    denial arrives with none.
    """
    # Lazy: the middleware package imports the automation tools, which
    # import this module.
    from ptc_agent.agent.middleware.credit_gate import DEFAULT_STOP_MESSAGE

    if not (
        isinstance(e, HTTPException)
        and e.status_code == 429
        and isinstance(e.detail, dict)
        and e.detail.get("type") not in _OUR_429_TYPES
    ):
        return None
    return e.detail.get("message") or DEFAULT_STOP_MESSAGE


def _our_failure(e: BaseException) -> bool:
    """An outage of ours rather than the automation's failure.

    The credit gate's 503 is its fail-closed answer when the quota service
    gives no verdict, a burst or unavailable 429 is our capacity, and an
    unavailable writer guard is the checkpoint store. A usage limit is not
    here either: it is ``_limit_message``'s, and never a strike.
    """
    from src.server.services.writer_guard import WriterGuardUnavailable

    if isinstance(e, WriterGuardUnavailable):
        return True
    if not isinstance(e, HTTPException):
        return False
    return e.status_code == 503 or (
        e.status_code == 429
        and isinstance(e.detail, dict)
        and e.detail.get("type") in _OUR_429_TYPES
    )


async def _credentials(user_id: str) -> tuple[bool, bool]:
    """``(has_byok, has_cred)``, read at the turn that uses them.

    has_cred is what the credit gate reports: an OAuth turn pays its own
    vendor bill just as a BYOK one does. The workflow's is_byok is a
    different question, whether to attempt the BYOK ladder, so it keys off
    has_byok alone, or an OAuth-only user gets a futile BYOK prefetch.
    """
    has_byok, has_oauth = await asyncio.gather(
        is_byok_active(user_id), has_any_oauth_token(user_id)
    )
    return has_byok, has_byok or has_oauth


async def _resolve_workspace(automation: Dict[str, Any]) -> str:
    """Flash runs in the user's shared flash workspace, created on demand;
    PTC in the stored one, whose ownership was verified when the automation
    was written."""
    if automation["agent_mode"] == "flash":
        flash_ws = await get_or_create_flash_workspace(automation["user_id"])
        return str(flash_ws["workspace_id"])
    ws_id = automation.get("workspace_id")
    if not ws_id:
        raise ValueError(
            "PTC mode requires a workspace_id, but automation has none "
            "(workspace may have been deleted)"
        )
    return str(ws_id)


class AutomationExecutor:
    """Singleton that executes automation runs."""

    _instance: Optional["AutomationExecutor"] = None

    @classmethod
    def get_instance(cls) -> "AutomationExecutor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        # Set while the server shuts down: a waiting firing gives up its place
        # rather than holding shutdown for its whole wait.
        self._stopping = asyncio.Event()

    def stop_waiting(self) -> None:
        self._stopping.set()

    def resume_waiting(self) -> None:
        self._stopping.clear()

    async def _precreate_titled_thread(
        self,
        automation: Dict[str, Any],
        thread_id: str,
        workspace_id: str,
        agent_mode: str,
    ) -> None:
        """Pre-create the run's thread titled "<automation name> — <date>".

        Automation runs bypass POST /threads, so without this the thread would be
        born inside ensure_thread_exists with the raw instruction as title (then
        LLM-retitled). A stable name+date reads better for recurring runs. Never
        raises — on failure the run proceeds and the ensure path creates the row.
        """
        name = (automation.get("name") or "").strip()
        if not name:
            return  # no name to stamp — let the ensure path LLM-title it
        try:
            from src.server.database.conversation import create_thread
            from src.utils.timezone_utils import zone_or_none

            tz = zone_or_none(automation.get("timezone")) or timezone.utc
            run_date = datetime.now(tz).strftime("%Y-%m-%d")
            await create_thread(
                conversation_thread_id=thread_id,
                workspace_id=workspace_id,
                current_status="completed",
                msg_type=agent_mode,
                thread_index=None,
                title=f"{name} — {run_date}"[:255],
                metadata={
                    "origin": {
                        "type": "automation",
                        "id": str(automation["automation_id"]),
                    }
                },
            )
        except Exception as e:
            logger.warning(f"[AUTOMATION_EXEC] Thread pre-create failed: {e}")

    async def _resolve_thread(
        self, automation: Dict[str, Any], workspace_id: str
    ) -> str:
        """The pinned thread under 'continue', else a new titled one, which
        'continue' pins when it has none yet."""
        strategy = automation.get("thread_strategy", "new")
        if strategy == "continue" and automation.get("conversation_thread_id"):
            return str(automation["conversation_thread_id"])
        thread_id = str(uuid4())
        if strategy == "continue":
            await auto_db.update_automation(
                str(automation["automation_id"]), automation["user_id"],
                conversation_thread_id=thread_id,
            )
        await self._precreate_titled_thread(
            automation, thread_id, workspace_id, automation["agent_mode"]
        )
        return thread_id

    async def _wait_for_thread(
        self,
        automation: Dict[str, Any],
        execution_id: str,
        thread_id: str,
        deadline: float,
    ) -> Optional[Dict[str, Any]]:
        """Hold the firing as ``waiting`` until its thread's turn ends.

        The automation as it stands now, once the thread is free and the
        execution is running again. None when the firing ended skipped
        instead: by the reader, because an earlier firing of it is already
        waiting, because the automation was paused or removed, at
        ``deadline``, or because the server is stopping. Whoever skipped it
        settled the firing.
        """
        # ``started_at`` becomes the wait's start, which the run stamps again
        # when it starts.
        if not await auto_db.transition_execution(
            execution_id,
            from_statuses=("pending", "running"),
            to="waiting",
            conversation_thread_id=thread_id,
            started_at=datetime.now(timezone.utc),
        ):
            return None
        logger.info(
            f"[AUTOMATION_EXEC] Waiting for thread: execution_id={execution_id} "
            f"thread_id={thread_id}"
        )
        await announce_wait(automation["user_id"], thread_id, execution_id, waiting=True)
        try:
            while True:
                try:
                    step = await self._look_again(
                        automation, execution_id, thread_id, deadline
                    )
                except Exception as e:
                    # A failed look is no verdict; the next one decides.
                    logger.warning(
                        f"[AUTOMATION_EXEC] Waiting on the thread failed, still "
                        f"waiting: execution_id={execution_id} error={e}"
                    )
                else:
                    if step is not _STILL_WAITING:
                        return step
                try:
                    await asyncio.wait_for(
                        self._stopping.wait(), timeout=_WAIT_POLL_SECONDS
                    )
                except asyncio.TimeoutError:
                    continue
                await self._skip_interrupted(automation, execution_id)
                return None
        except asyncio.CancelledError:
            await self._skip_interrupted(automation, execution_id)
            raise

    async def _skip_interrupted(
        self, automation: Dict[str, Any], execution_id: str
    ) -> None:
        try:
            await settle(
                automation, execution_id, Outcome.SKIPPED, skip_reason="interrupted"
            )
        except Exception as e:
            # The sweep skips it once the heartbeat stops.
            logger.error(
                f"[AUTOMATION_EXEC] Skipping an interrupted wait failed: "
                f"execution_id={execution_id} error={e}"
            )

    async def _look_again(
        self,
        automation: Dict[str, Any],
        execution_id: str,
        thread_id: str,
        deadline: float,
    ) -> Any:
        """One look at the line: the fresh automation once this firing may
        run, None once it ended, ``_STILL_WAITING`` otherwise."""
        automation_id = str(automation["automation_id"])
        if await auto_db.get_execution_status(execution_id) != "waiting":
            return None
        # One firing in line is enough: a frequent schedule must not stack up
        # copies of the same run behind a long turn. Asked on every look, since
        # an earlier firing can join the line after this one did.
        if await auto_db.has_earlier_waiting_execution(automation_id, execution_id):
            await settle(
                automation, execution_id, Outcome.SKIPPED, skip_reason="thread_busy"
            )
            return None
        if not await _thread_busy(thread_id):
            fresh = await auto_db.get_automation(automation_id, automation["user_id"])
            if fresh is None or (
                fresh["status"] not in _RUNNABLE
                and fresh["status"] != automation["status"]
            ):
                await settle(
                    automation, execution_id, Outcome.SKIPPED, skip_reason="user"
                )
                return None
            if not await auto_db.transition_execution(
                execution_id,
                from_statuses=("waiting",),
                to="running",
                started_at=datetime.now(timezone.utc),
            ):
                return None
            await announce_wait(
                automation["user_id"], thread_id, execution_id, waiting=False
            )
            return fresh
        if time.monotonic() >= deadline:
            await settle(
                automation, execution_id, Outcome.SKIPPED, skip_reason="thread_busy"
            )
            return None
        return _STILL_WAITING

    async def _note_admission(self, execution_id: str, firing: _Firing) -> bool:
        """Once the ledger holds the turn's run, link it and announce the
        start; False while there is no run row to go by yet, or when
        recording it failed and the next event should retry.

        The turn is already running then, and a bookkeeping error must not
        fail it; the drain's end links the run if no event ever did. A run the
        admission itself cancelled was never the automation's turn.

        The start goes out only while the run is still going. The terminal
        notice leaves from the run's settle job, possibly on another worker,
        and a start landing after it would leave a channel showing a run
        that already ended. This narrows that window; it does not close it.
        """
        try:
            run = await tl_db.get_run(firing.run_id)
            if run is None:
                return False
            if run["status"] == "cancelled":
                return True
            if (
                await _link_run(execution_id, firing.run_id)
                and run["status"] == "in_progress"
            ):
                await fire_webhook(
                    "automation.started", firing.automation, execution_id,
                    firing.thread_id, firing.workspace_id, run_id=firing.run_id,
                )
            firing.admitted = True
            return True
        except Exception as e:
            logger.warning(
                f"[AUTOMATION_EXEC] Recording the admitted run failed, retrying "
                f"on the next event: execution_id={execution_id} error={e}"
            )
            return False

    async def _hand_to_run(self, execution_id: str, firing: _Firing) -> bool:
        """Leave the firing to its run; False when the turn never reached the
        ledger, or a concurrent turn superseded it at admission.

        The run outlives this task in the run executor, and its finalize
        settles the firing as the run ended, on whichever worker drains the
        outbox job (``automation_settlement``): a rejected key or a usage
        limit must not read as this task's failure. A superseded run owes
        the firing no such job.
        """
        run = await tl_db.get_run(firing.run_id) if firing.run_id else None
        if run is None or (run.get("metadata") or {}).get("superseded"):
            return False
        await _link_run(execution_id, firing.run_id)
        return True

    async def _left_to_run(
        self, execution_id: str, error: Exception, firing: _Firing
    ) -> bool:
        """A failure after START is this task's, not the run's: the run's
        finalize settles the firing. False when there is no run to go by, and
        the error then settles it here."""
        try:
            handed = await self._hand_to_run(execution_id, firing)
        except Exception as e:
            logger.error(
                f"[AUTOMATION_EXEC] Linking the run failed: "
                f"execution_id={execution_id} run_id={firing.run_id} error={e}"
            )
            # An admitted run's finalize settles the firing all the same.
            handed = firing.admitted
        if handed:
            logger.error(
                f"[AUTOMATION_EXEC] Following the run failed, leaving the "
                f"firing to it: execution_id={execution_id} "
                f"run_id={firing.run_id} "
                f"error={type(error).__name__}: {clean_error_text(str(error))}"
            )
        return handed

    async def _run_turn(
        self,
        execution_id: str,
        firing: _Firing,
        *,
        has_byok: bool,
        deadline: float,
    ) -> bool:
        """Drain the automation's turn on ``firing.thread_id``; False when the
        firing ended while back in line for the thread instead.

        Losing the thread to another turn before admission means the reader
        started one between the idle check and this one: the firing gets
        back in line behind it, then reads its automation and credentials
        again and reruns the credit gate, whose verdict a long wait would
        outlive.
        """
        # TODO(layering): sanctioned services→handlers residual: automation
        # is an alternate run driver; fixing this means moving the run
        # entrypoints into services/runs/ with handlers as thin adapters.
        from src.server.handlers.chat import (
            astream_flash_workflow,
            astream_ptc_workflow,
        )

        while True:
            automation = firing.automation
            user_id = automation["user_id"]
            instruction = automation["instruction"]
            request = ChatRequest(
                agent_mode=firing.agent_mode,
                workspace_id=firing.workspace_id,
                messages=[ChatMessage(role="user", content=instruction)],
                llm_model=automation.get("llm_model"),
                additional_context=automation.get("additional_context"),
                origin=ThreadOrigin(
                    type="automation", id=str(automation["automation_id"])
                ),
            )
            # Generate run_id locally: automations don't pass through the
            # HTTP handler that normally creates it. Per-turn keying for
            # BTM / persistence / Redis stream.
            firing.run_id = str(uuid4())
            firing.admitted = admission_known = False
            turn_args = dict(
                request=request,
                thread_id=firing.thread_id,
                run_id=firing.run_id,
                user_input=instruction,
                user_id=user_id,
                is_byok=has_byok,
                steerable=False,
                # Stamped on the run at START, so its finalize settles this
                # firing whichever worker is left to drain the job.
                run_metadata={
                    "automation_execution_id": execution_id,
                    "automation_id": str(automation["automation_id"]),
                },
            )
            if firing.agent_mode == "flash":
                turn = astream_flash_workflow(**turn_args)
            else:
                turn = astream_ptc_workflow(**turn_args, workspace_id=firing.workspace_id)

            # Drain the async generator: no HTTP client to consume SSE
            event_count = 0
            try:
                async for _event in turn:
                    event_count += 1
                    if not admission_known:
                        admission_known = await self._note_admission(
                            execution_id, firing
                        )
            except Exception as e:
                if firing.admitted or not _lost_thread_race(e):
                    raise
            else:
                logger.info(
                    f"[AUTOMATION_EXEC] Workflow complete: "
                    f"execution_id={execution_id} events={event_count}"
                )
                return True
            firing.run_id = None
            fresh = await self._wait_for_thread(
                automation, execution_id, firing.thread_id, deadline
            )
            if fresh is None:
                return False
            firing.automation = fresh
            has_byok, has_cred = await _credentials(user_id)
            await enforce_credit_limit(user_id, byok=has_cred)

    async def execute(
        self,
        automation: Dict[str, Any],
        execution_id: str,
    ) -> None:
        """Run one firing of ``automation`` until it settles or its run does.

        It starts only from ``pending``: the sweep may have settled it while
        it sat in line. Once its turn reached the run ledger, the run's
        finalize settles it. Every other exit goes through ``settle`` here,
        which refuses when a skip or the sweep ended the firing first; the
        firing then stays as they recorded it.
        """
        automation_id = str(automation["automation_id"])
        user_id = automation["user_id"]
        agent_mode = automation["agent_mode"]

        logger.info(
            f"[AUTOMATION_EXEC] Starting execution: "
            f"automation_id={automation_id} execution_id={execution_id} "
            f"mode={agent_mode}"
        )
        if not await auto_db.transition_execution(
            execution_id,
            from_statuses=("pending",),
            to="running",
            started_at=datetime.now(timezone.utc),
        ):
            logger.warning(
                f"[AUTOMATION_EXEC] Not starting: execution_id={execution_id} "
                f"was settled before it began"
            )
            return

        _trigger = automation.get("trigger_type") or "unknown"
        _exec_span = _otel_tracer.start_span(
            "automation.execution",
            attributes={
                "automation_id": _obs_hash_id(automation_id),
                "trigger": _trigger,
                "mode": agent_mode or "unknown",
            },
        )
        keep_alive = asyncio.create_task(
            _keep_alive(execution_id),
            name=f"automation_heartbeat_{execution_id[:8]}",
        )

        firing = _Firing(automation=automation, agent_mode=agent_mode)
        wait_deadline = time.monotonic() + _WAIT_CAP_SECONDS
        try:
            # ─── Wait out a running turn ──────────────────────────
            # A run on a pinned thread is a turn of its own, so it waits for
            # the reader's turn to end rather than steering into it. It waits
            # before the credit gate, whose verdict a long wait would outlive.
            pinned = (
                automation.get("conversation_thread_id")
                if automation.get("thread_strategy") == "continue"
                else None
            )
            if pinned and await _thread_busy(str(pinned)):
                fresh = await self._wait_for_thread(
                    automation, execution_id, str(pinned), wait_deadline
                )
                if fresh is None:
                    _exec_span.set_attribute("status", "skipped")
                    return
                # Nothing is resolved yet, so the run takes the automation
                # as it now stands, its mode and thread included.
                firing.automation = fresh
                firing.agent_mode = fresh["agent_mode"]

            # ─── Credential check + credit gate ───────────────────
            has_byok, has_cred = await _credentials(user_id)
            await enforce_credit_limit(user_id, byok=has_cred)

            # ─── Resolve workspace + thread ───────────────────────
            firing.workspace_id = await _resolve_workspace(firing.automation)
            firing.thread_id = await self._resolve_thread(
                firing.automation, firing.workspace_id
            )
            # The thread exists from here on, so record it on the run now: a
            # live run can then be opened and watched while it streams, not
            # only once it settles.
            if not await auto_db.transition_execution(
                execution_id,
                from_statuses=("running",),
                to="running",
                conversation_thread_id=firing.thread_id,
            ):
                logger.warning(
                    f"[AUTOMATION_EXEC] Not running the turn: "
                    f"execution_id={execution_id} was settled elsewhere"
                )
                _exec_span.set_attribute("status", "settled_elsewhere")
                return

            # ─── Run the turn ─────────────────────────────────────
            if not await self._run_turn(
                execution_id, firing, has_byok=has_byok, deadline=wait_deadline
            ):
                _exec_span.set_attribute("status", "skipped")
                return
            # A turn too quick to be seen at an event, or seen only cancelled
            # (a stop), is left to its run here.
            if not firing.admitted and not await self._hand_to_run(
                execution_id, firing
            ):
                raise RuntimeError(
                    f"workflow run finished as 'no run row' (run_id={firing.run_id})"
                )
            _exec_span.set_attribute("status", "handed_to_run")

        except Exception as e:
            _exec_span.record_exception(e)
            if await self._left_to_run(execution_id, e, firing):
                _exec_span.set_attribute("status", "handed_to_run")
                return
            limit_message = _limit_message(e)
            if limit_message is not None:
                outcome, error_msg = Outcome.LIMITED, limit_message
                logger.warning(
                    f"[AUTOMATION_EXEC] A usage limit refused the firing, no "
                    f"strike: execution_id={execution_id} error={error_msg}"
                )
            else:
                ours = _our_failure(e)
                outcome = Outcome.FAILED_OURS if ours else Outcome.FAILED
                error_msg = f"{type(e).__name__}: {clean_error_text(str(e))}"
                logger.error(
                    f"[AUTOMATION_EXEC] Execution failed: "
                    f"execution_id={execution_id} error={error_msg}"
                )
                if ours:
                    logger.warning(
                        f"[AUTOMATION_EXEC] Not counting a strike against "
                        f"automation_id={automation_id}: the failure was ours"
                    )
            settled = await settle(
                firing.automation, execution_id, outcome,
                thread_id=firing.thread_id, workspace_id=firing.workspace_id,
                error=error_msg,
            )
            _exec_span.set_attribute(
                "status",
                "settled_elsewhere" if not settled
                else "limited" if outcome is Outcome.LIMITED else "failure",
            )
        except asyncio.CancelledError:
            # The server is stopping. A cancel inside a wait already skipped
            # the firing, and this finds it settled. A turn that reached the
            # ledger ends with its run, and anything short of one was
            # interrupted here.
            try:
                if not await self._hand_to_run(execution_id, firing):
                    await settle(
                        firing.automation, execution_id, Outcome.INTERRUPTED,
                        thread_id=firing.thread_id,
                        workspace_id=firing.workspace_id,
                    )
            except Exception as e:
                # The sweep settles it once the heartbeat stops.
                logger.error(
                    f"[AUTOMATION_EXEC] Settling an interrupted run failed: "
                    f"execution_id={execution_id} error={e}"
                )
            _exec_span.set_attribute("status", "interrupted")
            raise
        finally:
            keep_alive.cancel()
            _exec_span.end()

