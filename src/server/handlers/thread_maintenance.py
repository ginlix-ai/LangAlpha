"""Thread maintenance — compact, offload and the hand-off at a model switch,
under the exclusive thread guard.

Each mutation resolves the thread's graph and checkpoint state, runs the
operation with a cross-worker stop key, and writes its result, with the
``context_window`` marker replay shows for a compact or an offload. Cancel
dispatch and status reads live in services/cancel_dispatch.py and
services/thread_status.py.
"""

import asyncio
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from ptc_agent.agent.roles import AgentRole
from ptc_agent.core.project_context import ProjectContext
from src.llms.claude_code import ClaudeCodeTarget, claude_replied_last, is_claude_code_model
from src.server.handlers.cancellation import cancellation_as_http
from src.server.services.history.projector import context_window_record
from src.server.services.thread_agent import thread_route
from src.server.utils.checkpoint_helpers import (
    build_checkpoint_config,
    get_checkpointer,
    interrupt_records,
    turn_base_checkpoint_id,
    update_at_commit,
)

# Import setup module to access initialized globals
from src.server.app import setup

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ThreadAgent:
    """The thread's agent, built as its next turn would build it, and the
    state that turn would start from."""

    thread_id: str
    graph: Any
    state: Any
    messages: list
    workspace_id: str
    #: Pinned to the thread's workspace folder; None without a sandbox.
    backend: Any
    project: ProjectContext | None

    async def ask(self, ask):
        """``ask`` answered by the agent's own model chain
        (``ask_between_turns``), with the project bound as a run binds it.
        Raises ``NoCompaction`` when the agent compacts nothing (compaction
        off in its config)."""
        from ptc_agent.agent.middleware.compaction import ask_between_turns
        from ptc_agent.core.project_context import project_bound

        with project_bound(self.project):
            return await ask_between_turns(
                self.graph.agent,
                self.state.values,
                ask,
                thread_id=self.thread_id,
                store=setup.store,
            )

    async def write(self, values: dict, verb: str) -> None:
        await _update_graph_state(self.graph, self.state, values, self.thread_id, verb)


async def _thread_agent(
    thread_id: str,
    verb: str,
    config=None,
    checkpointer=None,
    user_id=None,
    *,
    held: AsyncExitStack,
    role: AgentRole = "analyst",
) -> ThreadAgent:
    """Validate thread, build graph, get state, build backend.

    ``config`` is the resolved AgentConfig; defaults to ``setup.agent_config``.
    ``checkpointer`` overrides the global pooled saver — a mutation passes its
    fence-bound saver so checkpoint writes die with the lock session (I2).
    ``user_id`` identifies the caller to the session acquire, whose MCP resolve
    is owner-scoped. ``held`` is the caller's stack, which keeps the backend's
    folder in place until the caller's block ends.

    The agent is built as the thread's next turn would build it, in ``role``,
    from the inputs the turn's build reads (``AgentInputs``): the tools and
    the prompt a model call between turns sends have to be that turn's, or
    the provider's cache misses and a provider's own compaction is refused.
    What only a turn has (its row, its run, its relay sessions) it leaves out.
    """
    from src.server.database import conversation as qr_db
    from src.server.database.workspace import get_workspace
    from src.server.database.workspace_folders import (
        WorkspaceFolderMoving,
        is_top_level,
        workspace_folder_in_use,
    )
    from src.server.utils.error_sanitization import sandbox_unreachable_detail
    from src.server.services.workspace_manager import WorkspaceManager
    from ptc_agent.agent.graph import build_ptc_graph_with_session
    from ptc_agent.agent.backends.sandbox import SandboxBackend
    from ptc_agent.core.paths import SandboxLayout

    # Validate thread + workspace
    thread_info = await qr_db.get_thread_with_summary(thread_id)
    if not thread_info:
        raise HTTPException(status_code=404, detail=f"Thread not found: {thread_id}")
    workspace_id = thread_info.get("workspace_id")
    if not workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Thread {thread_id} has no associated workspace",
        )
    # The row holds a uuid.UUID; the session and its asset sync key on the str
    # the request paths pass.
    workspace_id = str(workspace_id)

    # Session
    workspace_manager = WorkspaceManager.get_instance()
    try:
        session = await workspace_manager.get_session_for_workspace(
            workspace_id, user_id=user_id
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Backend. Pinned to the thread's workspace folder because these routes
    # run outside a turn, where nothing has bound a project: an unpinned
    # backend would file this thread's saved attachments on the machine root,
    # which a later delete of the workspace would leave behind. No run keeps a
    # settle off this folder: ``held`` keeps the folder in place, and the row,
    # and the project the agent is built on, are read under it.
    backend = None
    if hasattr(session, "sandbox") and session.sandbox is not None:
        try:
            await held.enter_async_context(workspace_folder_in_use(workspace_id))
        except WorkspaceFolderMoving as e:
            raise HTTPException(status_code=503, detail=sandbox_unreachable_detail(e)) from None
        row = await get_workspace(workspace_id) or {}
        dir_name = row.get("dir_name")
        if dir_name and not is_top_level(dir_name):
            raise HTTPException(
                status_code=503,
                detail=sandbox_unreachable_detail(WorkspaceFolderMoving(workspace_id)),
            )
        layout = SandboxLayout(session.sandbox.working_dir).for_workspace(dir_name)
        backend = SandboxBackend(session.sandbox, layout.workspace)

    # Graph
    checkpointer = checkpointer if checkpointer is not None else get_checkpointer()
    effective_config = config if config is not None else setup.agent_config
    if not effective_config:
        raise HTTPException(
            status_code=500, detail="Agent configuration not initialized"
        )
    from src.server.database.user import get_user_profile_for_prompt
    from src.server.services.thread_agent import AgentInputs

    inputs = await AgentInputs.read(
        session,
        user_id=user_id,
        workspace_id=workspace_id,
        thread_id=thread_id,
        role=role,
        user_profile=await get_user_profile_for_prompt(user_id) if user_id else None,
    )
    graph = await build_ptc_graph_with_session(
        session=session,
        config=effective_config,
        checkpointer=checkpointer,
        **inputs.build_kwargs(),
    )

    # State with timeout, read where the next turn would start, so the write
    # lands on the thread's pointer rather than on a branch a fork abandoned.
    base_checkpoint_id = await turn_base_checkpoint_id(checkpointer, thread_id)
    try:
        state = await asyncio.wait_for(
            graph.aget_state(build_checkpoint_config(thread_id, base_checkpoint_id)),
            timeout=10.0,
        )
    except asyncio.TimeoutError:
        logger.error(f"aget_state timed out for thread {thread_id} during {verb}")
        raise HTTPException(
            status_code=504,
            detail=f"Timed out retrieving state for thread: {thread_id}",
        )
    if not state or not state.values:
        raise HTTPException(
            status_code=404, detail=f"No state found for thread: {thread_id}"
        )
    messages = state.values.get("messages", [])
    if not messages:
        raise HTTPException(status_code=400, detail=f"No messages to {verb}")

    return ThreadAgent(
        thread_id=thread_id,
        graph=graph,
        state=state,
        messages=messages,
        workspace_id=workspace_id,
        backend=backend,
        project=inputs.project,
    )


async def _update_graph_state(
    graph, state, values: dict, thread_id: str, verb: str
) -> None:
    """Write ``values`` onto the checkpoint ``state`` was read at, and move
    the thread's commit pointer onto the write when it still points there
    (``update_at_commit``).

    Left beside the pointer, the write is seen by the next turn, which runs
    from the latest checkpoint, but not by anything reading the committed
    branch until that turn ends: replay, and the window's slice coverage.
    """
    built_on = ((state.config or {}).get("configurable") or {}).get("checkpoint_id")
    try:
        advanced = await update_at_commit(
            graph, thread_id, built_on, values, timeout=10.0
        )
    except asyncio.TimeoutError:
        logger.error(f"aupdate_state timed out for thread {thread_id} during {verb}")
        raise HTTPException(
            status_code=504,
            detail=f"Timed out updating state for thread: {thread_id}",
        )
    if not advanced:
        logger.info(
            f"[{verb}] thread {thread_id}: commit pointer is not at {built_on}, "
            "left in place"
        )


async def _summary_since_the_turn(thread_id: str, state):
    """The summary written since the thread's latest turn started, which a
    compact now is about to replace, or None.

    Replay reads a turn's summaries from the state its span ends on, and the
    compact's write becomes that end, so without a copy on the compact's
    marker the turn would lose the one the agent wrote during it, or the one
    an earlier compact after it installed. A summary from before the turn
    shows where it was written. A failed read keeps no copy.
    """
    from langchain_core.messages import HumanMessage

    from src.server.services.history.reader import CheckpointHistoryReader

    values = state.values or {}
    event = values.get("_summarization_event")
    message = event.get("summary_message") if isinstance(event, dict) else None
    if not isinstance(message, HumanMessage) or not message.id:
        return None
    built_on = ((state.config or {}).get("configurable") or {}).get("checkpoint_id")
    try:
        reader = CheckpointHistoryReader.get_instance()
        anchors, _ = await asyncio.wait_for(
            reader.aget_turn_anchors(thread_id, built_on), timeout=10.0
        )
        if not anchors:
            return None
        before = await asyncio.wait_for(
            reader.aget_state(thread_id, anchors[-1].input_checkpoint_id),
            timeout=10.0,
        )
    except Exception:
        logger.warning(
            f"[compact] thread {thread_id}: could not read the latest turn's "
            f"start; a summary it wrote is not kept on the marker",
            exc_info=True,
        )
        return None
    prior = ((before.values or {}) if before else {}).get("_summarization_event")
    prior_message = prior.get("summary_message") if isinstance(prior, dict) else None
    if getattr(prior_message, "id", None) == message.id:
        return None
    return message


@asynccontextmanager
async def _hold_thread_mutation(thread_id: str, verb: str):
    """Hold the exclusive-T mutation fence for a manual /compact|/offload|
    /delete, mapping the runner's refusals onto the HTTP contract (409 with a
    stable code the frontend branches on; 503 on budget exhaustion)."""
    from src.server.services.thread_mutation import (
        MutationConflict,
        MutationUnavailable,
        ThreadMutationRunner,
    )

    runner = ThreadMutationRunner.get_instance()
    try:
        async with runner.exclusive(thread_id, verb) as session:
            yield session
    except MutationConflict as e:
        raise HTTPException(status_code=409, detail=e.detail)
    except MutationUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e))


async def _resolver(user_id: str, thread_id: str, meta: dict, route):
    """``thread_resolver`` for a call between turns, which no composer
    started, so the thread's row sets its effort and fast mode."""
    from src.server.database.api_keys import is_byok_active
    from src.server.services.thread_agent import thread_resolver

    return thread_resolver(
        user_id, thread_id, route, thread=meta, is_byok=await is_byok_active(user_id)
    )


async def _summarize(agent: ThreadAgent, config):
    """The pipeline automatic compaction runs when the agent compacts nothing,
    on the user's resolved config, naming the notes when the user's flag is
    on, as the agent build does."""
    from ptc_agent.agent.middleware.compaction import compact_messages
    from ptc_agent.agent.middleware.compaction.notes import ThreadScratchpad

    logger.info(f"[compact] thread {agent.thread_id}: the agent compacts nothing")
    scratchpad = (
        ThreadScratchpad.resolve(config, agent.backend.workspace_dir, agent.thread_id)
        if agent.backend is not None and config is not None
        else None
    )
    return await compact_messages(
        agent.messages,
        agent.state.values,
        config,
        thread_id=agent.thread_id,
        backend=agent.backend,
        workspace_id=agent.workspace_id,
        notes_dir=scratchpad.notes_dir if scratchpad is not None else None,
    )


def _refuse_claude_code(on_claude_code: bool) -> None:
    """Refuse a summary or an offload on a Claude Code thread. Claude Code manages its own
    session, and the thread's messages are only the record replay and
    transcripts read, so rewriting them changes nothing Claude sees, while a
    write through the ordinary graph drops what the runtime keeps beside them.
    Called inside the mutation fence, which orders it against a model switch."""
    if on_claude_code:
        raise HTTPException(
            status_code=400,
            detail={
                "type": "claude_code_compacts_itself",
                "message": "Claude Code manages this thread's context itself.",
            },
        )


@cancellation_as_http("compact")
async def trigger_compaction(
    thread_id: str,
    *,
    user_id: str | None = None,
) -> dict:
    """Manually trigger context compaction for a thread.

    Sent as the agent's own model call (``ask_between_turns``), built and
    resolved as the thread's next turn would be, so a provider that compacts
    the conversation itself does here as it does mid-turn, from its cache, and
    the summary runs on the model and credential the thread's turns do.
    """
    try:
        from ptc_agent.agent.middleware.compaction import Compact, NoCompaction

        # The mutation fence FIRST — before any graph state reads or writes:
        # exclusive T(thread) refuses while a fenced run or tail writer is
        # live (any worker), the ledger gate refuses on an in_progress row,
        # and the op key holds concurrent message POSTs at admission. The
        # runner also owns the user-Stop path (local cancel / cross-worker
        # stop flag).
        async with (
            _hold_thread_mutation(thread_id, "compact") as mutation,
            AsyncExitStack() as held,
        ):
            agent_cfg = setup.agent_config
            role: AgentRole = "analyst"
            if user_id and agent_cfg is not None:
                meta, route = await thread_route(thread_id, user_id)
                role = route.role
                # Read off the thread too, so a resolve that falls back to
                # the base config below cannot run a Claude Code thread's.
                _refuse_claude_code(is_claude_code_model(meta.get("llm_model")))
                try:
                    from src.server.services.llm import thread_model

                    agent_cfg = await thread_model.resolve_turn_config(
                        user_id,
                        thread_id,
                        named=None,
                        held=meta.get("llm_model"),
                        # A summary changes no answer, so it runs on the
                        # default rather than waiting on a reconnect.
                        fall_back=True,
                        resolve=await _resolver(user_id, thread_id, meta, route),
                    )
                except HTTPException:
                    # 402 insufficient credits, 403 revoked key, etc. are intentional
                    # user-facing signals — don't silently downgrade to platform config.
                    raise
                except Exception as e:
                    logger.warning(
                        f"[compact] resolve_llm_config failed for user {user_id}: {e}; "
                        "falling back to base agent_config"
                    )
                    agent_cfg = setup.agent_config

            # A thread following a Claude Code default holds no model of its own.
            _refuse_claude_code(isinstance(getattr(agent_cfg, "llm_client", None), ClaudeCodeTarget))

            agent = await _thread_agent(
                thread_id, "compact", config=agent_cfg,
                checkpointer=mutation.saver, user_id=user_id, held=held, role=role,
            )
            try:
                try:
                    compaction = await agent.ask(Compact())
                except NoCompaction:
                    compaction = await _summarize(agent, agent_cfg)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

            # The view that was compacted, as automatic compaction counts it,
            # not every message the checkpoint has kept since the thread began.
            original_count = compaction.original_count
            new_message_count = len(compaction.messages)
            summary_text = compaction.summary.text
            summary_length = len(summary_text)
            summary_source = compaction.summary.source

            # The marker rides the write that installs the summary. Action
            # "summarize" is the wire protocol's. Replay reads the text for
            # the "View summary" panel from the summary the marker names.
            marker = context_window_record(
                {
                    "action": "summarize",
                    "signal": "complete",
                    "original_message_count": original_count,
                    "new_message_count": new_message_count,
                    "summary_length": summary_length,
                    "source": summary_source,
                },
                summary_message_id=compaction.event["summary_message"].id,
                replaced=await _summary_since_the_turn(thread_id, agent.state),
            )
            await agent.write(
                {**compaction.update(agent.state.values), "ui": [marker]}, "compact"
            )

            logger.info(
                f"Manual compaction completed for thread {thread_id}: "
                f"{original_count} -> {new_message_count} messages"
            )

            return {
                "success": True,
                "thread_id": thread_id,
                "original_message_count": original_count,
                "new_message_count": new_message_count,
                "summary_length": summary_length,
                "summary_text": summary_text,
                "source": summary_source,
            }

    except HTTPException:
        raise
    except Exception as e:
        # CancelledError (user Stop / client disconnect) is handled by the
        # @cancellation_as_http wrapper, which sees it after the mutation
        # fence releases.
        logger.exception(f"Error triggering compaction for thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to trigger compaction: {str(e)}"
        )


@cancellation_as_http("offload")
async def trigger_offload(thread_id: str, *, user_id: str | None = None) -> dict:
    """
    Manually trigger tool-arg offloading for a thread (Tier 1 only).

    Records large tool arguments and stale Read results in older messages as
    offloaded, an argument only once the transcript that keeps it is saved.
    No LLM summarization is performed. ``user_id`` identifies the caller to the
    session acquire, same as
    :func:`trigger_compaction`.

    Args:
        thread_id: The thread/conversation ID to offload

    Returns:
        Dict with success, thread_id, message_count, offloaded_args, offloaded_reads
    """
    try:
        from ptc_agent.agent.middleware.compaction import (
            OffloadSettings,
            offload_tool_args,
            record_offloads,
        )
        from src.server.database.conversation import get_thread_auth_meta

        # Same fence as /compact — /offload also writes checkpoint state and
        # could race a running workflow's _offloaded_tool_call_ids updates.
        # The exclusive-T lock + ledger gate are deterministic, so the old
        # fail-open/fail-closed tracker asymmetry is gone.
        async with (
            _hold_thread_mutation(thread_id, "offload") as mutation,
            AsyncExitStack() as held,
        ):
            _refuse_claude_code(is_claude_code_model((await get_thread_auth_meta(thread_id) or {}).get("llm_model")))
            agent = await _thread_agent(
                thread_id, "offload", checkpointer=mutation.saver, user_id=user_id, held=held
            )
            messages, state = agent.messages, agent.state
            # A thread following a Claude Code default holds no model of its own.
            _refuse_claude_code(claude_replied_last(messages))

            # Set: resolving the graph above refuses to run without it.
            settings = OffloadSettings.from_config(setup.agent_config.compaction)
            try:
                offloaded_arg_ids, offloaded_read_ids = await offload_tool_args(
                    messages,
                    state.values,
                    settings,
                    thread_id=thread_id,
                    backend=agent.backend,
                    workspace_id=agent.workspace_id,
                )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

            offloaded_args = len(offloaded_arg_ids)
            offloaded_reads = len(offloaded_read_ids)

            # Ids only, never messages: the checkpoint keeps every message and
            # the middleware applies the recorded ids on every call. Unlike
            # the automatic pass, this one does not wait for an idle turn: the
            # user asked for it now. The marker rides the same write.
            marker = context_window_record(
                {
                    "action": "offload",
                    "signal": "complete",
                    "offloaded_args": offloaded_args,
                    "offloaded_reads": offloaded_reads,
                }
            )
            await agent.write(
                {
                    **record_offloads(
                        state.values, offloaded_arg_ids, offloaded_read_ids
                    ),
                    "ui": [marker],
                },
                "offload",
            )

            logger.info(
                f"Manual offload completed for thread {thread_id}: "
                f"{offloaded_args} tool args, {offloaded_reads} read results"
            )

            return {
                "success": True,
                "thread_id": thread_id,
                "message_count": len(messages),
                "offloaded_args": offloaded_args,
                "offloaded_reads": offloaded_reads,
            }

    except HTTPException:
        raise
    except Exception as e:
        # CancelledError (user Stop / client disconnect) is handled by the
        # @cancellation_as_http wrapper, which sees it after the mutation
        # fence releases.
        logger.exception(f"Error triggering offload for thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to trigger offload: {str(e)}"
        )



async def _summary_to_hand_off(thread_id: str):
    """The summary the thread's next turn opens on, when only the provider
    that wrote it can read it and its model has not written it out yet; else
    None. None too while a step waits on the user: a write ends that step, and
    the answer the user gives would have nothing to resume.

    Read off the raw checkpoint, which holds both, rather than the thread's
    state: every model switch asks, and the state replays the message deltas,
    which a long thread makes slow (about 170 ms against 2 ms on one of 1,800
    messages)."""
    from langchain_core.messages import HumanMessage

    from ptc_agent.agent.middleware.compaction.utils import handed_off
    from src.llms.provider_compaction import is_opaque

    checkpointer = get_checkpointer()
    base = await turn_base_checkpoint_id(checkpointer, thread_id)
    saved = await checkpointer.aget_tuple(build_checkpoint_config(thread_id, base))
    values = (saved.checkpoint.get("channel_values") or {}) if saved else {}
    event = values.get("_summarization_event")
    message = event.get("summary_message") if isinstance(event, dict) else None
    if not isinstance(message, HumanMessage) or not is_opaque(message) or handed_off(message):
        return None
    if interrupt_records(saved.pending_writes):
        return None
    return message


async def hand_off_on_switch(
    thread_id: str, user_id: str, previous_model: str | None
) -> bool:
    """When a switch from ``previous_model`` (None for the account default)
    leaves the thread on a model that cannot read the summary its provider
    compacted it into, have that model write the summary out for the new one,
    and say whether it is.

    True only once the hand-off holds the thread, so a send from then on waits
    for it at admission. The new model reads the digest the summary carries
    when the hand-off cannot start (a turn or background tasks still hold the
    thread), fails, or dies with its worker.
    """
    from src.server.handlers.chat.detached import fire_and_forget

    try:
        # Checked again under the fence; asked first so a switch with nothing
        # to hand off never holds the thread.
        if await _hand_off_config(thread_id, user_id, previous_model) is None:
            return False
    except Exception:
        logger.warning(f"[handoff] thread {thread_id}: switch not read", exc_info=True)
        return False
    started = asyncio.get_running_loop().create_future()
    fire_and_forget(
        _hand_off(thread_id, user_id, previous_model, started),
        name=f"handoff_{thread_id[:8]}",
    )
    return await asyncio.shield(started)


async def _hand_off_config(thread_id: str, user_id: str, model: str | None):
    """The config ``model`` runs under on the thread and the role it runs in,
    when the summary is still to hand off, the thread's model cannot read it,
    and ``model`` can; else None."""
    from src.llms.provider_compaction import replays

    summary = await _summary_to_hand_off(thread_id)
    if summary is None:
        return None
    meta, route = await thread_route(thread_id, user_id)
    resolve = await _resolver(user_id, thread_id, meta, route)
    try:
        switched_to = (await resolve(meta.get("llm_model"))).get_llm_client()
    except Exception:
        # A model that cannot run now reads the text when it can.
        switched_to = None
    if switched_to is not None and replays(summary, switched_to):
        return None
    try:
        config = await resolve(model)
        readable = replays(summary, config.get_llm_client())
    except Exception as e:
        logger.info(f"[handoff] thread {thread_id}: {model!r} cannot run ({e})")
        return None
    if not readable:
        logger.info(f"[handoff] thread {thread_id}: {model!r} cannot read the summary")
        return None
    return config, route.role


async def _hand_off(
    thread_id: str, user_id: str, model: str | None, started: asyncio.Future
) -> None:
    """``model``, the one the thread was on, writes out the summary only its
    provider can read (``HandOff``), in place of the digest; ``started`` is
    True once it holds the thread and has checked it has one to write.
    Nothing is written when it cannot: the digest stays."""
    from ptc_agent.agent.middleware.compaction import HandOff, NoCompaction
    from src.config.settings import get_compaction_timeout

    try:
        async with (
            _hold_thread_mutation(thread_id, "handoff") as mutation,
            AsyncExitStack() as held,
        ):
            target = await _hand_off_config(thread_id, user_id, model)
            if target is None:
                return
            config, role = target
            started.set_result(True)
            # A send waiting on the hand-off is held at least this long, so
            # the hand-off ends, written or given up, before it is refused.
            async with asyncio.timeout(get_compaction_timeout()):
                agent = await _thread_agent(
                    thread_id, "handoff", config=config,
                    checkpointer=mutation.saver, user_id=user_id, held=held, role=role,
                )
                try:
                    written = await agent.ask(HandOff())
                except NoCompaction:
                    logger.info(f"[handoff] thread {thread_id}: the agent compacts nothing")
                    return
            if written is None:
                return
            event = agent.state.values["_summarization_event"]
            await agent.write(
                {"_summarization_event": {**event, "summary_message": written}}, "handoff"
            )
            logger.info(
                f"[handoff] thread {thread_id}: {model!r} wrote out its summary "
                f"({len(written.content)} chars)"
            )
    except HTTPException as e:
        # Before it starts, the thread is held (a turn, background tasks,
        # another mutation); after, its agent could not be built.
        stage = "given up" if started.done() else "not started"
        logger.info(f"[handoff] thread {thread_id}: {stage} ({e.detail})")
    finally:
        if not started.done():
            started.set_result(False)
