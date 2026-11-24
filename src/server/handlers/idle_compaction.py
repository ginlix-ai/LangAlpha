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

A Claude Code thread's firing (``CC``) has the CLI compact the session, as a
manual /compact does, on the turn's own credential and the computer its CLI
runs on, which is not started for it either.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException

from ptc_agent.agent.middleware.compaction import IdleCompact, IdleSkipped
from ptc_agent.agent.middleware.compaction.compact import NoOwnSummary
from ptc_agent.agent.middleware.compaction.idle import FEATURE, IDLE_DEADLINE_MARGIN_S
from ptc_agent.agent.middleware.compaction.measure import measured_tokens
from ptc_agent.agent.middleware.compaction.rebuild import LAST_ANSWER_KEY, forced_under_threshold
from ptc_agent.agent.middleware.runtime_context.state import as_dict
from ptc_agent.core.sandbox.attach_only import SandboxNotRunning
from src.llms.claude_code import SLOTS_FULL, ClaudeCodeTarget, ClaudeTurnFailed
from src.server.services.computer_manager import attach_only
from src.server.services.features import user_feature_enabled
from src.server.services.idle_compaction import Kind, log_idle

if TYPE_CHECKING:
    from src.server.handlers.thread_maintenance import SummaryWrite

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
        _unless_before_deadline(row)
        thread = await _precheck(row)
        with attach_only():
            async with AsyncExitStack() as held:
                kind = await _summarize_and_install(row, thread, held)
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
    except ClaudeTurnFailed as e:
        # Its CLI refused or failed as a turn's would: logged in, the plan,
        # the computer it runs on, or a computer too busy to start it at all.
        log_idle("skip" if e.kind in SLOTS_FULL else "discard", thread_id, f"claude_code_{e.kind}", kind)
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
    """What drops a claim before anything is built: the user turned the
    feature off since the arm, the thread moved off the turn's checkpoint, a
    run started since the one that armed it, the thread archived, a step
    waits on the user, a rebuild that no deploy or urgent prompt made due
    (``forced_under_threshold``, on the turn's own ``floor_tokens``),
    something set to report back before the entry expires, or a computer
    that is not running."""
    from src.server.database.computer import get_computer_for_workspace
    from src.server.database.conversation import get_thread_by_id, threads_read
    from src.server.services.home_placement import runs_in
    from src.server.services.ssh_computers import computer_unreachable
    from src.server.services.thread_agent import thread_route
    from src.server.utils.checkpoint_helpers import build_checkpoint_config, get_checkpointer, interrupt_records
    from src.tools.thread_status.tool import pending_work

    thread_id, base = row["thread_id"], row["base_checkpoint_id"]
    if not await user_feature_enabled(row["user_id"], FEATURE):
        raise _Skip("skip", "feature_off")
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
    pending = await pending_work(
        thread_id, row["user_id"], analysts=route.role == "chief_of_staff", before=_expiry(row)
    )
    if pending is not None:
        raise _Skip("skip", f"pending_{pending}")
    # The computer the summary would use: for Home while its machine can't
    # be reached, the stand-in already placed (none is placed for this).
    computer = await get_computer_for_workspace(
        await runs_in(row["user_id"], str(meta["workspace_id"]), make=False)
    )
    if computer is None or computer.get("status") != "running":
        raise _Skip("skip", "computer_stopped")
    if await computer_unreachable(computer):
        raise _Skip("skip", "computer_unreachable")
    return _Thread(meta, route)


async def _summarize_and_install(row: dict[str, Any], thread: _Thread, held: AsyncExitStack) -> Kind:
    """Summarize the thread and install the summary; the kind installed."""
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
    # Only Claude Code compacts its session, and only its session: a thread
    # whose model moved to or off it since the arm would summarize another
    # model's context.
    target = getattr(config, "llm_client", None)
    if isinstance(target, ClaudeCodeTarget) != (row["kind"] == "CC"):
        raise _Skip("skip", "model_changed")
    agent = await _thread_agent(
        thread_id,
        "idle_compact",
        config=config,
        user_id=user_id,
        held=held,
        route=thread.route,
        checkpoint_id=row["base_checkpoint_id"],
    )
    # Ends with ``held``, after the install.
    tracker = await held.enter_async_context(_recorded(agent, "idle_compact"))
    if isinstance(target, ClaudeCodeTarget):
        write, kind = await _session_summary(agent, row, target)
    else:
        write, kind = await _agent_summary(agent, row)
    _warn_cold_read(row, kind, tracker.get_per_call_records())
    # Shielded: a stop landing between the write and its pointer advance
    # would leave the summary beside the pointer, where the next turn reads it.
    await asyncio.shield(_install(agent, write.values, row, thread))
    await _announce(row, thread, write)
    return kind


async def _announce(row: dict[str, Any], thread: _Thread, write: SummaryWrite) -> None:
    """Tell the user's open tabs the summary landed, as a manual /compact's
    answer tells the tab that asked."""
    from src.server.services.shared_workspaces.access import shared_org_of
    from src.server.services.thread_lifecycle_feed import publish_thread_compacted

    workspace_id = str(thread.meta["workspace_id"]) if thread.meta.get("workspace_id") else None
    try:
        # A shared thread's other members may have it open too.
        shared_org = await shared_org_of(workspace_id)
    except Exception:
        shared_org = None
    await publish_thread_compacted(
        user_id=row["user_id"],
        thread_id=row["thread_id"],
        workspace_id=workspace_id,
        original_message_count=write.props["original_message_count"],
        summary_text=write.summary_text,
        shared_org=shared_org,
    )


async def _agent_summary(agent: Any, row: dict[str, Any]) -> tuple[SummaryWrite, Kind]:
    """The agent's own summary, and its kind: a deploy since the arm may have
    made a summary keeping the tail a rebuild, which keeps no turns."""
    from src.server.handlers.thread_maintenance import compaction_write

    compaction = await _unless_returned(lambda: agent.ask(IdleCompact()), row["thread_id"], row["armed_by_run_id"])
    return await compaction_write(agent, compaction), "I2" if compaction.preserved else "I1"


async def _session_summary(agent: Any, row: dict[str, Any], target: ClaudeCodeTarget) -> tuple[SummaryWrite, Kind]:
    """The CLI's compaction of the thread's Claude Code session, when its
    records, read with the agent built, still show it due by the user's
    settings now (``CompactionRules.idle_plan``): a deploy since the arm can
    take the move it was due away, and a setting turned off since, the
    compaction. Whether the turn's send kept the full context is that send's,
    which no setting changes.

    Only on the model and effort the turn ran on (``ClaudeTurn.reading``),
    which ``target`` must still name: the cache is keyed by both, and a
    choice changed since writes no checkpoint. Not with a call Claude Code
    deferred waiting on the user, which the compaction's CLI would hold for
    its whole budget, nor past the deadline, which the build and the CLI's
    start may have taken it past since the claim.
    """
    from claude_agent_sdk import CLIConnectionError

    from ptc_agent.agent.claude_code.session import RULES_KEY, awaiting_user
    from src.server.handlers.thread_maintenance import session_compaction_write

    values = agent.state.values
    rules = values.get(RULES_KEY) or {}
    if (rules.get("model"), rules.get("effort")) != (target.model, target.effort):
        raise _Skip("skip", "request_changed")
    if awaiting_user(values.get("claude_session"), agent.messages):
        raise _Skip("skip", "waiting_on_user")
    judged = replace(agent.graph.compaction, keep_full_context=bool(rules.get("keep_full_context")))
    due, reason = judged.idle_plan(values.get("claude_session") or {})
    if not due:
        raise _Skip("skip", reason)
    _unless_before_deadline(row)
    try:
        session = await _unless_returned(agent.compact_claude_code, row["thread_id"], row["armed_by_run_id"])
    except CLIConnectionError as e:
        # The CLI never connected.
        raise _Skip("skip", "cli_unavailable") from e
    if session is None:
        raise _Skip("discard", "nothing_to_compact")
    return session_compaction_write(session), "CC"


def _warn_cold_read(row: dict[str, Any], kind: Kind, calls: list[dict[str, Any]]) -> None:
    """Warn of a summary that wrote more of its prompt to the cache than it
    read: every firing is timed to read a warm entry, so that is one that
    missed, and the call paid the write it was armed to spare. The counts
    are the calls' usage (``idle_compact``'s row); ``expires_in_s`` is how
    long the entry it meant to read had left by its arm's reckoning."""
    usage = [c.get("usage") or {} for c in calls]
    read = sum(u.get("cached_tokens") or 0 for u in usage)
    written = sum((u.get("cache_5m_tokens") or 0) + (u.get("cache_1h_tokens") or 0) for u in usage)
    if read >= written:
        return
    logger.warning(
        "[Compaction] idle=cold_read kind=%s thread_id=%s cache_read=%d cache_write=%d expires_in_s=%d",
        kind,
        row["thread_id"],
        read,
        written,
        (_expiry(row) - datetime.now(UTC)).total_seconds(),
    )


def _unless_before_deadline(row: dict[str, Any]) -> None:
    """Past the deadline the entry would expire under the summary, which
    would write the history at the write rate."""
    if datetime.now(UTC) > row["deadline"]:
        raise _Skip("discard", "past_deadline")


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


async def _install(agent: Any, values: dict[str, Any], row: dict[str, Any], thread: _Thread) -> None:
    """Install the summary's ``values`` under the mutation fence, which refuses while a
    run or a tail writer holds the thread, and only on the checkpoint it was
    written from with no run started since, nothing set to report back and
    nothing waiting on the user: a run that failed before its commit leaves
    its steps beside the pointer, where the next turn reads them, and a
    write onto the checkpoint would drop them. The write is manual
    /compact's, with its marker, and its pointer advance is a compare-and-set
    from that checkpoint."""
    from src.server.database.conversation import threads_read
    from src.server.handlers.thread_maintenance import _hold_thread_mutation
    from src.server.utils.checkpoint_helpers import build_checkpoint_config, get_checkpointer, interrupt_records
    from src.tools.thread_status.tool import pending_work

    thread_id, base = row["thread_id"], row["base_checkpoint_id"]
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
