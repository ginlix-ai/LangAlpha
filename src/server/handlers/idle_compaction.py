"""Firing an idle compaction the sweeper claimed (``services.idle_compaction``).

The summary runs outside the thread mutation fence, on the checkpoint the
completed turn left, so a user who comes back while it runs is never held,
stopped or refused: the firing reads the run ledger every second and
gives itself up once a run starts on the thread, whose turn then reads the
cache warm. Only the install is fenced, and only while the thread still
points at that checkpoint, so it lands on exactly the state it summarized.

Nothing is fired on a thread with a step waiting on the user, whose answer a
write would leave nothing to resume, or with anything set to report back to
it, whose wake turn should find the context as the agent left it. The
computer is used only while it runs and is never kept up by the firing.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException

from ptc_agent.agent.middleware.compaction import IdleCompact, IdleSkipped
from ptc_agent.agent.middleware.compaction.compact import NoOwnSummary
from ptc_agent.agent.middleware.compaction.idle import IDLE_DEADLINE_MARGIN_S
from ptc_agent.agent.middleware.compaction.measure import measured_tokens
from ptc_agent.agent.middleware.compaction.rebuild import LAST_ANSWER_KEY, forced_under_threshold
from ptc_agent.agent.middleware.runtime_context.state import as_dict
from ptc_agent.core.sandbox.attach_only import SandboxNotRunning
from src.llms.claude_code import is_claude_code_model
from src.server.services.computer_manager import attach_only
from src.server.services.idle_compaction import log_idle

logger = logging.getLogger(__name__)

#: How often, in seconds, the summary looks for a run started on its thread:
#: one indexed read, at most two firings a worker.
POLL_S = 1.0


@dataclass(frozen=True)
class _Thread:
    meta: dict[str, Any]
    route: Any


class _Skip(Exception):
    def __init__(self, event: str, reason: str) -> None:
        super().__init__(reason)
        self.event = event
        self.reason = reason


async def fire(row: dict[str, Any]) -> None:
    """Fire one claimed row, logging how it ended: installed, or skipped
    before the model call, or discarded after it started."""
    thread_id, kind = row["thread_id"], row["kind"]
    try:
        if datetime.now(UTC) > row["deadline"]:
            raise _Skip("discard", "past_deadline")
        thread = await _precheck(row)
        with attach_only():
            async with AsyncExitStack() as held:
                compaction = await _summarize_and_install(row, thread, held)
        # A deploy since the arm may have made a summary keeping the tail a
        # rebuild, which keeps no turns.
        kind = "I2" if compaction.preserved else "I1"
    except _Skip as skip:
        log_idle(skip.event, thread_id, skip.reason, kind)
    except asyncio.CancelledError:
        # A worker stopping; the install, if it began, runs to its end.
        log_idle("discard", thread_id, "shutdown", kind)
        raise
    except IdleSkipped as skip:
        log_idle("skip", thread_id, skip.reason, kind)
    except SandboxNotRunning:
        log_idle("skip", thread_id, "computer_stopped", kind)
    except NoOwnSummary:
        log_idle("discard", thread_id, "native_failed", kind)
    except TimeoutError:
        log_idle("discard", thread_id, "timeout", kind)
    except HTTPException as e:
        log_idle("skip", thread_id, "unavailable", kind, status=e.status_code)
    except Exception:
        logger.warning("[Compaction] idle firing failed thread_id=%s", thread_id, exc_info=True)
        log_idle("discard", thread_id, "error", kind)
    else:
        log_idle("installed", thread_id, "summarized", kind)


async def _precheck(row: dict[str, Any]) -> _Thread:
    """What drops a claim before anything is built: the thread moved off the
    turn's checkpoint, a run started since the one that armed it, the thread
    archived, a step waits on the user, a rebuild that no deploy or urgent
    prompt made due (``forced_under_threshold``, on the turn's own
    ``floor_tokens``), something set to report back
    before the entry expires, a Claude Code thread, or a computer that is
    not running."""
    from src.server.database.computer import get_computer_for_workspace
    from src.server.database.conversation import get_thread_by_id, threads_read
    from src.server.services.thread_agent import thread_route
    from src.server.utils.checkpoint_helpers import build_checkpoint_config, get_checkpointer, interrupt_records
    from src.tools.thread_status.tool import pending_work

    thread_id, base = row["thread_id"], row["base_checkpoint_id"]
    if (await threads_read.get_thread_branch_read(thread_id)).checkpoint_id != base:
        raise _Skip("skip", "moved")
    if await _returned(thread_id, row["armed_by_run_id"]):
        raise _Skip("skip", "superseded")
    if (await get_thread_by_id(thread_id) or {}).get("archived_at"):
        raise _Skip("skip", "archived")
    saved = await get_checkpointer().aget_tuple(build_checkpoint_config(thread_id, base))
    if saved is None:
        raise _Skip("skip", "moved")
    if interrupt_records(saved.pending_writes):
        raise _Skip("skip", "waiting_on_user")
    values = saved.checkpoint.get("channel_values") or {}
    floor = as_dict(values.get(LAST_ANSWER_KEY)).get("floor_tokens") or 0
    if row["kind"] == "I1":
        forced, _ = forced_under_threshold(values, measured_tokens(values) or 0, floor, claude=True, cold=True)
        if not forced:
            raise _Skip("skip", "not_due")
    meta, route = await thread_route(thread_id, row["user_id"])
    if is_claude_code_model(meta.get("llm_model")):
        raise _Skip("skip", "claude_code")
    pending = await pending_work(
        thread_id, row["user_id"], analysts=route.role == "chief_of_staff", before=_expiry(row)
    )
    if pending is not None:
        raise _Skip("skip", f"pending_{pending}")
    computer = await get_computer_for_workspace(str(meta["workspace_id"]))
    if computer is None or computer.get("status") != "running":
        raise _Skip("skip", "computer_stopped")
    return _Thread(meta, route)


async def _summarize_and_install(row: dict[str, Any], thread: _Thread, held: AsyncExitStack) -> Any:
    from src.server.handlers.thread_maintenance import _recorded, _resolver, _thread_agent
    from src.server.services.llm import thread_model

    thread_id, user_id = row["thread_id"], row["user_id"]
    config = await thread_model.resolve_turn_config(
        user_id,
        thread_id,
        named=None,
        held=thread.meta.get("llm_model"),
        # On the thread's own model and credential or not at all: nobody is
        # waiting for this summary, and another model reads none of the cache.
        fall_back=False,
        resolve=await _resolver(user_id, thread_id, thread.meta, thread.route),
    )
    agent = await _thread_agent(
        thread_id,
        "idle_compact",
        config=config,
        user_id=user_id,
        held=held,
        role=thread.route.role,
        checkpoint_id=row["base_checkpoint_id"],
    )
    # Ends with ``held``, after the install.
    await held.enter_async_context(_recorded(agent, user_id, "idle_compact"))
    compaction = await _unless_returned(lambda: agent.ask(IdleCompact()), thread_id, row["armed_by_run_id"])
    # Shielded: a stop landing between the write and its pointer advance
    # would leave the summary beside the pointer, where the next turn reads it.
    await asyncio.shield(_install(agent, compaction, row, thread))
    return compaction


def _expiry(row: dict[str, Any]) -> datetime:
    """When the cache entry the firing reads expires: anything set to report
    back before then would find the context warm as the agent left it."""
    return row["deadline"] + timedelta(seconds=IDLE_DEADLINE_MARGIN_S)


async def _returned(thread_id: str, run_id: str) -> bool:
    """Whether a run started on the thread since ``run_id``, the one that armed
    the firing, live or not."""
    from src.server.database.runs import lifecycle as tl_db

    latest = await tl_db.get_latest_run_id(thread_id)
    return latest is not None and latest != run_id


async def _unless_returned(ask: Callable[[], Awaitable[Any]], thread_id: str, run_id: str) -> Any:
    """``ask``'s answer, within the compaction timeout, unless a run starts
    on the thread first: then the call is cancelled and nothing is kept. A
    run that started while the agent was built is caught before the call,
    whose first step exports the transcript of the checkpoint it summarizes
    under the thread's newest one."""
    from src.config.settings import get_compaction_timeout

    if await _returned(thread_id, run_id):
        raise _Skip("skip", "user_returned")
    task = asyncio.ensure_future(ask())
    try:
        async with asyncio.timeout(get_compaction_timeout()):
            while True:
                done, _ = await asyncio.wait({task}, timeout=POLL_S)
                if done:
                    return task.result()
                if await _returned(thread_id, run_id):
                    raise _Skip("discard", "user_returned")
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def _install(agent: Any, compaction: Any, row: dict[str, Any], thread: _Thread) -> None:
    """Install the summary under the mutation fence, which refuses while a
    run or a tail writer holds the thread, and only on the checkpoint it was
    written from with no run started since, nothing set to report back and
    nothing waiting on the user: a run that failed before its commit leaves
    its steps beside the pointer, where the next turn reads them, and a
    write onto the checkpoint would drop them. The write is manual
    /compact's, with its marker, and its pointer advance is a compare-and-set
    from that checkpoint."""
    from src.server.database.conversation import threads_read
    from src.server.handlers.thread_maintenance import _hold_thread_mutation, compaction_write
    from src.server.utils.checkpoint_helpers import build_checkpoint_config, get_checkpointer, interrupt_records
    from src.tools.thread_status.tool import pending_work

    thread_id, base = row["thread_id"], row["base_checkpoint_id"]
    values = await compaction_write(agent, compaction)
    try:
        # As manual /compact's: a send landing in the install's milliseconds
        # is held as compacting, which it is.
        async with _hold_thread_mutation(thread_id, "compact") as mutation:
            if (await threads_read.get_thread_branch_read(thread_id)).checkpoint_id != base:
                raise _Skip("discard", "moved")
            if await _returned(thread_id, row["armed_by_run_id"]):
                raise _Skip("discard", "superseded")
            saver = mutation.saver or get_checkpointer()
            saved = await saver.aget_tuple(build_checkpoint_config(thread_id, base))
            if saved is None or interrupt_records(saved.pending_writes):
                raise _Skip("discard", "waiting_on_user")
            pending = await pending_work(
                thread_id, row["user_id"], analysts=thread.route.role == "chief_of_staff", before=_expiry(row)
            )
            if pending is not None:
                raise _Skip("discard", f"pending_{pending}")
            if not await agent.write(values, "idle_compact", saver=mutation.saver):
                raise _Skip("discard", "moved")
    except HTTPException as e:
        # The fence refuses with 409 or 503; a write that fails raises others.
        raise _Skip("discard", "fence_refused" if e.status_code in (409, 503) else "write_failed") from e
