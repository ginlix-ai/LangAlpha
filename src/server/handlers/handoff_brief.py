"""Writing the brief a thread hands Claude Code when it moves there from
another model (``ptc_agent.agent.middleware.compaction.brief``).

The model the thread was on writes it where it reads the thread cheaply
(``CompactionMiddleware._reads_cheaply``), else the compaction model; when
neither writes one, Claude is handed the thread itself. It is written at the
switch, under the thread mutation fence, while the user is still writing;
else inside the turn that opens on it, for every way a turn reaches Claude
Code without a switch (a dispatch, a chat app, a report-back, an automation,
an edit). Both run the same chain within the same budget, which a send
waiting on the switch is held at least as long as. A turn handed nothing
opens on the server's digest (``ClaudeTurn.hand_off``).
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException

from ptc_agent.agent.middleware.compaction import WriteBrief
from ptc_agent.agent.middleware.compaction.brief import (
    BRIEF_KEY,
    HandOffBrief,
    brief_due,
    brief_from,
    brief_owed,
    kept,
)
from src.config.settings import get_compaction_timeout
from src.llms.claude_code import is_claude_code_model
from src.server.utils.checkpoint_helpers import build_checkpoint_config, get_checkpointer, turn_base_checkpoint_id

if TYPE_CHECKING:
    from src.server.handlers.thread_maintenance import ThreadAgent
    from src.server.services.thread_agent import AgentInputs
    from src.server.services.turn_runtime import TurnRoute

logger = logging.getLogger(__name__)

async def brief_on_switch(thread_id: str, user_id: str, reader: str | None) -> bool:
    """When a switch leaves the thread on Claude Code after another model's
    reply (on ``reader``, the model it names, None for its default), have
    the brief Claude's first turn opens on written (``_writers``), and say
    whether it is.

    True once it holds the thread, so a send from then on waits for it at
    admission. A brief that cannot be written here (a turn or background
    tasks hold the thread, its computer is not running, a step waits on the
    user, the worker dies) is written by the turn itself (``brief_for_turn``):
    picking a model never starts a computer.
    """
    from src.server.handlers.chat.detached import fire_and_forget
    from src.server.services.history.reader import CheckpointHistoryReader

    try:
        # Read first, so a switch with nothing to write never holds the thread.
        base = await turn_base_checkpoint_id(get_checkpointer(), thread_id)
        state = await CheckpointHistoryReader.get_instance().aget_state(thread_id, base)
        if not brief_owed((state.values or {}) if state else {}):
            return False
    except Exception:
        logger.warning(f"[handoff] thread {thread_id}: switch not read", exc_info=True)
        return False
    started = asyncio.get_running_loop().create_future()
    fire_and_forget(_brief_between_turns(thread_id, user_id, reader, started), name=f"brief_{thread_id[:8]}")
    return await asyncio.shield(started)


async def _brief_between_turns(
    thread_id: str, user_id: str, reader: str | None, started: asyncio.Future
) -> None:
    """Write the thread's brief under the fence and keep it in the checkpoint
    for the turn to read; ``started`` is True once the fence is held."""
    from src.server.handlers.thread_maintenance import _hold_thread_mutation
    from ptc_agent.core.sandbox.attach_only import SandboxNotRunning, attach_only

    try:
        with attach_only():
            async with _hold_thread_mutation(thread_id, "brief") as mutation, asyncio.timeout(_budget()):
                started.set_result(True)
                await _write_held(thread_id, user_id, reader, mutation)
    except SandboxNotRunning:
        logger.info(f"[handoff] thread {thread_id}: computer not running, the brief is left to the turn")
    except HTTPException as e:
        stage = "given up" if started.done() else "not started"
        logger.info(f"[handoff] thread {thread_id}: brief {stage} ({e.detail})")
    except TimeoutError:
        logger.info(f"[handoff] thread {thread_id}: brief given up past the compaction budget")
    except Exception:
        logger.warning(f"[handoff] thread {thread_id}: brief failed", exc_info=True)
    finally:
        if not started.done():
            started.set_result(False)


async def _write_held(thread_id: str, user_id: str, reader: str | None, mutation: Any) -> None:
    """Under the fence ``mutation`` holds: the brief written by the first
    writer that writes one, and kept in the checkpoint."""
    from src.server.handlers.thread_maintenance import _resolver, _thread_agent
    from src.server.services.llm.thread_model import next_turn_runs_claude_code
    from src.server.services.thread_agent import thread_route

    meta, route = await thread_route(thread_id, user_id)
    # A switch made since, back off Claude Code, leaves no turn to read it.
    if not await next_turn_runs_claude_code(user_id, str(meta["workspace_id"]), meta.get("llm_model")):
        return
    async for writer in _writers(thread_id, await _resolver(user_id, thread_id, meta, route), reader):
        async with AsyncExitStack() as held:
            agent = await _thread_agent(
                thread_id, "handoff", config=writer.config, checkpointer=mutation.saver,
                user_id=user_id, held=held, role=route.role,
            )
            values = agent.state.values
            # Read again under the fence: a turn or another switch may have
            # moved the thread since ``brief_on_switch`` read it. A write
            # would also end a step waiting on the user, whose answer would
            # then have nothing to resume.
            if not brief_owed(values) or agent.state.interrupts:
                return
            if (brief := await _write(agent, user_id, writer, values)) is not None:
                await agent.write({BRIEF_KEY: kept(brief)}, "handoff")
                return


async def brief_owed_at(graph: Any, thread_id: str, checkpoint_id: str | None) -> dict | None:
    """The values a Claude Code turn starts from at ``checkpoint_id`` when
    they are owed a brief (``brief_owed``), else None. One the thread keeps
    already reaches the turn from state, and a failed read leaves the turn
    the server's digest."""
    try:
        values = (await graph.aget_state(build_checkpoint_config(thread_id, checkpoint_id))).values or {}
    except Exception:
        logger.warning(f"[handoff] thread {thread_id}: turn start not read", exc_info=True)
        return None
    return values if brief_owed(values) else None


async def brief_kept_at_head(graph: Any, thread_id: str, values: dict) -> HandOffBrief | None:
    """The brief the thread's newest checkpoint keeps for the reply
    ``values`` end on, else None. A regenerate or an edit just after a
    hand-off starts before the turn that kept it, on the same history up to
    that reply, so it opens on that brief rather than pay for another. Read
    from the channel, not ``stored_brief``: once Claude has replied the head
    is due no brief, though the one it keeps still covers ``values``."""
    try:
        head = (await graph.aget_state(build_checkpoint_config(thread_id, None))).values or {}
    except Exception:
        logger.warning(f"[handoff] thread {thread_id}: head not read", exc_info=True)
        return None
    stored = head.get(BRIEF_KEY)
    return stored if stored and stored["through"] == brief_due(values["messages"]) else None


async def brief_for_turn(
    values: dict, session: Any, inputs: AgentInputs, *, user_id: str, route: TurnRoute, reader: str
) -> HandOffBrief | None:
    """The brief a turn on the Claude Code model ``reader`` starting from
    ``values`` (``brief_owed_at``) opens on, written now on the turn's own
    session and inputs. None when none was written, which leaves the turn the
    server's digest. The turn keeps what it is handed, so a retry of it, and
    a regenerate or an edit just after it (``brief_kept_at_head``), reads it
    rather than pay for it again."""
    from ptc_agent.agent.graph import build_ptc_graph_with_session
    from src.server.database.conversation import get_thread_auth_meta
    from src.server.handlers.thread_maintenance import ThreadAgent, _resolver

    thread_id = inputs.thread_id
    try:
        async with asyncio.timeout(_budget()):
            resolve = await _resolver(user_id, thread_id, await get_thread_auth_meta(thread_id) or {}, route)
            async for writer in _writers(thread_id, resolve, reader):
                try:
                    built = await build_ptc_graph_with_session(
                        session=session, config=writer.config, checkpointer=get_checkpointer(),
                        **inputs.build_kwargs(),
                    )
                except Exception:
                    logger.warning(f"[handoff] thread {thread_id}: {writer.model!r}'s agent not built", exc_info=True)
                    continue
                agent = ThreadAgent(
                    thread_id=thread_id, graph=built, state=None, messages=values["messages"],
                    workspace_id=inputs.workspace_id, backend=None, project=inputs.project,
                )
                if (brief := await _write(agent, user_id, writer, values)) is not None:
                    return brief
    except TimeoutError:
        logger.info(f"[handoff] thread {thread_id}: the turn's brief given up past the compaction budget")
    except Exception:
        logger.warning(f"[handoff] thread {thread_id}: no brief for the turn", exc_info=True)
    return None


def _budget() -> float:
    """How long writing a brief may take: the compaction budget the models
    share, and a little past it for the last step, which hands Claude the
    thread itself, and the write. Half the margin admission holds a send
    waiting on the switch past that budget, so the fence is released inside
    it."""
    from src.server.services.runs.admission import COMPACTION_ADMISSION_MARGIN_S

    return get_compaction_timeout() + COMPACTION_ADMISSION_MARGIN_S / 2


@dataclass(frozen=True)
class _Writer:
    """An agent the brief is written in, and what it is asked."""

    model: str
    config: Any
    ask: WriteBrief
    #: The model the thread was on, which the hand-off is from.
    from_model: str | None


async def _writers(
    thread_id: str, resolve: Callable[[str | None], Awaitable[Any]], reader: str | None
) -> AsyncIterator[_Writer]:
    """The agents a brief for ``reader`` may be written in, in the order
    tried; one that cannot run now is skipped. First the model the thread's
    newest turn off Claude Code ran, which writes it from its own request
    where it reads the thread cheaply there, and otherwise leaves it to the
    compaction model its config names; then the deployment's default, whose
    compaction model writes it. Never a Claude Code row, which no request
    reaches: Claude is handed the thread itself when no model writes it."""
    from src.server.database.conversation import recent_turn_models
    from src.server.services.llm.config import system_default_models

    held = next((m for m in await recent_turn_models(thread_id) if not is_claude_code_model(m)), None)
    default = system_default_models().get("default_model")
    room = await _reader_room(resolve, reader)
    for model in dict.fromkeys(m for m in (held, default) if m and not is_claude_code_model(m)):
        try:
            config = await resolve(model)
        except Exception as e:
            logger.info(f"[handoff] thread {thread_id}: {model!r} cannot run ({e})")
            continue
        yield _Writer(model, config, WriteBrief(by_own=model == held, room=room), held)


async def _reader_room(resolve: Callable[[str | None], Awaitable[Any]], reader: str | None) -> int:
    """How much of the thread ``reader`` is handed when no model writes its
    brief: half the context its CLI compacts at, leaving the rest to its
    prompt, its tools and the turn's own work before that compaction. A
    ``reader`` of None is the account default, which ``resolve`` names."""
    try:
        settings = (await resolve(reader)).compaction
    except Exception as e:
        logger.info(f"[handoff] {reader!r}'s compaction threshold not read ({e})")
        return WriteBrief.room
    return settings.token_threshold // 2 if settings.enabled and settings.token_threshold else WriteBrief.room


async def _write(agent: ThreadAgent, user_id: str, writer: _Writer, values: dict) -> HandOffBrief | None:
    """The brief of ``values``, through their last reply, written in
    ``agent`` (``WriteBrief``), its usage recorded as the thread's; None when
    it failed."""
    from src.server.handlers.thread_maintenance import _recorded

    began = time.monotonic()
    try:
        async with _recorded(agent, user_id, "handoff"):
            compaction = await agent.ask(writer.ask, values)
    except Exception as e:
        logger.warning(
            f"[handoff] thread {agent.thread_id}: {writer.model!r} wrote no brief "
            f"in {time.monotonic() - began:.1f}s ({type(e).__name__}: {e})"
        )
        return None
    brief = brief_from(
        compaction.event["summary_message"], through=brief_due(values["messages"]),
        from_model=writer.from_model, room=writer.ask.room,
    )
    logger.info(
        f"[handoff] thread {agent.thread_id}: brief in {writer.model!r}'s agent "
        f"in {time.monotonic() - began:.1f}s ({brief['source']}, {len(brief['text'])} chars)"
    )
    return brief
