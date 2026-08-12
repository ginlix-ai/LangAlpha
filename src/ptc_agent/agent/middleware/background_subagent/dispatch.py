"""Direct background-subagent dispatch outside the Task-tool call path.

The Task tool spawns subagents by intercepting its own tool calls
(``BackgroundSubagentMiddleware``). The workflow driver has no tool call to
intercept — it dispatches children directly against the same compiled
subagent graphs, through the same canonical writer spawn pipeline, so they
are indistinguishable from Task-tool subagents for model resolution, event
capture, collection, and billing.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import structlog
from langchain_core.messages import HumanMessage
from langgraph.types import Command

from ptc_agent.agent.middleware.background_subagent.context import (
    current_background_agent_id,
    current_background_token_tracker,
    current_background_tool_call_id,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
)
from ptc_agent.agent.middleware.background_subagent.spawn import (
    NamespaceUnfenced,
    SpawnError,
    TaskRunRefused,
    spawn_task_writer,
)
from ptc_agent.agent.middleware.background_subagent.subagent import (
    arun_subagent_streaming,
    return_command_with_state_update,
)

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        BackgroundSubagentMiddleware,
    )

logger = structlog.get_logger(__name__)

# Keys that must never leak from the launching turn's config into a
# dispatched child. The Task-tool path passes its parent config through
# wholesale, and each Task call runs inside its OWN tool-node task, so its
# per-call pregel state is fresh. Direct dispatch reuses ONE captured config
# for every child, so the per-call state must be dropped to emulate that:
#
# - checkpoint_ns/checkpoint_id/checkpoint_map — the child owns its own
#   checkpoint namespace.
# - __pregel_scratchpad — the capturing tool call's scratchpad. Its shared
#   subgraph counter suffixes each successive child's checkpoint_ns with
#   ``|N`` (a fresh Task call always counts from 0 → no suffix), which
#   breaks StreamMessagesHandler's namespace filter — every messages-mode
#   token of children after the first is silently dropped — and shifts the
#   child's checkpoints out of the ``task:<id>`` namespace history readers
#   expect.
# - __pregel_stream — the launching turn's stream handle. The turn is
#   typically settled by the time children run; duplexing into its dead
#   queue serves no consumer.
#
# The REMAINING ``__pregel_*`` internals (task_id, send/read, checkpointer,
# runtime) are deliberately kept: they mark the child run as nested, which
# is what makes LangGraph preserve the ``checkpoint_ns`` override — a
# non-nested run gets its ns reset to "" and would write checkpoints into
# the thread's ROOT namespace, which the writer guard refuses once the
# launching turn's session ends.
_DROPPED_CONFIGURABLE_KEYS = frozenset(
    {
        "checkpoint_ns",
        "checkpoint_id",
        "checkpoint_map",
        "__pregel_scratchpad",
        "__pregel_stream",
    }
)

# Mirrors the main agent's ``with_config({"recursion_limit": 2000})``, which
# Task-tool subagents inherit via the parent config.
_DISPATCH_RECURSION_LIMIT = 2000


async def run_subagent_graph(
    subagent_graph: Any,
    *,
    prompt: str,
    parent_thread_id: str,
    task_id: str,
    subagent_type: str,
    registry: Any,
    tool_call_id: str,
    base_configurable: dict[str, Any] | None = None,
    task_run_id: str | None = None,
) -> Command:
    """Invoke a compiled subagent graph directly and return the result Command.

    Mirrors the Task-tool init path: ``checkpoint_ns=f"task:{task_id}"`` under
    the parent thread, token-tracker callbacks read from the background
    ContextVar, streaming forwarded to the per-task registry buffer, and the
    final message extracted into a ToolMessage Command.
    """
    configurable = {
        k: v
        for k, v in (base_configurable or {}).items()
        if k not in _DROPPED_CONFIGURABLE_KEYS
    }
    configurable["thread_id"] = parent_thread_id
    configurable["checkpoint_ns"] = f"task:{task_id}"

    metadata: dict[str, Any] = {
        "subagent_type": subagent_type,
        "description": prompt[:200],
    }
    if task_run_id:
        # Same stamp the Task path's ``_subagent_run_metadata`` writes: every
        # checkpoint this run produces names its ledger row, which is the join
        # key replay's segment claim uses to attribute a task namespace's
        # rounds. Without it a dispatched child's segments fall back to
        # positional claiming and lose their per-run status.
        metadata["task_run_id"] = task_run_id

    config: dict[str, Any] = {
        "configurable": configurable,
        "metadata": metadata,
        "recursion_limit": _DISPATCH_RECURSION_LIMIT,
    }
    tracker = current_background_token_tracker.get(None)
    if tracker is not None:
        config["callbacks"] = [tracker]

    state = {"messages": [HumanMessage(content=prompt)]}
    result = await arun_subagent_streaming(
        subagent_graph, state, config, registry=registry, tool_call_id=tool_call_id
    )
    return return_command_with_state_update(result, tool_call_id)


async def dispatch_background_subagent(
    mw: "BackgroundSubagentMiddleware",
    subagent_graphs: dict[str, Any],
    *,
    subagent_type: str,
    description: str,
    prompt: str,
    parent_thread_id: str,
    run_id: str | None,
    owner_task_id: str | None = None,
    tool_call_id: str | None = None,
    base_configurable: dict[str, Any] | None = None,
) -> BackgroundTask:
    """Register and spawn a background subagent without a Task tool call.

    ``run_id`` stamps ``spawned_run_id`` (collector fencing + record run
    stamps); ``owner_task_id`` marks the child as workflow-owned so
    agent-facing aggregation (wait-all, notifications, TaskOutput's
    all-tasks view) skips it. Raises ``ValueError`` on an unknown
    ``subagent_type`` and ``SpawnError`` when admission was rejected
    or the writer could not be spawned.
    """
    if subagent_type not in subagent_graphs:
        available = ", ".join(sorted(subagent_graphs))
        raise ValueError(
            f"Unknown subagent type '{subagent_type}'. Available: {available}"
        )

    tool_call_id = tool_call_id or f"dispatch-{uuid.uuid4().hex[:12]}"
    task = await mw.registry.register(
        tool_call_id=tool_call_id,
        description=description,
        prompt=prompt,
        subagent_type=subagent_type,
        run_id=run_id,
        owner_task_id=owner_task_id,
    )

    # The entry is returned only on success, so a raise from here on leaves one
    # nobody can name — this id was minted above rather than handed in.
    # `discard_unstarted` refuses anything that settled some other way, which
    # is what keeps a stop mid-spawn registered for its guard drain.
    try:
        # The ledger row is what routes the child's captured events onto a live
        # v2 lane and puts it under scanner recovery. parent_run_id stays None:
        # report-back jobs are only enqueued for runs with a parent, and a
        # workflow child reports to its driver, never to the launching turn's
        # transcript.
        try:
            admitted = await mw.admit_task_run(
                task,
                cause="init",
                description=description,
                launch_tool_call_id=task.tool_call_id,
                parent_run_id=None,
            )
        except NamespaceUnfenced as refusal:
            task.mark_never_started(refusal.settle_reason)
            raise SpawnError(
                f"could not fence checkpoint namespace for {task.display_id}"
            ) from refusal
        except TaskRunRefused as refusal:
            task.mark_never_started(refusal.settle_reason)
            raise SpawnError(
                f"Error: could not start {task.display_id} — {refusal.reason}."
            ) from refusal
        task.task_run_id = admitted or None

        subagent_graph = subagent_graphs[subagent_type]

        async def _dispatch_runner(_request: Any) -> Command:
            # Set inside the spawned task's context: the event-capture and
            # steering middleware attribute tool calls to the active background
            # task by reading these vars during graph execution.
            current_background_tool_call_id.set(task.tool_call_id)
            current_background_agent_id.set(task.agent_id)
            return await run_subagent_graph(
                subagent_graph,
                prompt=prompt,
                parent_thread_id=parent_thread_id,
                task_id=task.task_id,
                subagent_type=subagent_type,
                registry=mw.registry,
                tool_call_id=task.tool_call_id,
                base_configurable=base_configurable,
                task_run_id=task.task_run_id,
            )

        await spawn_task_writer(
            mw,
            task,
            _dispatch_runner,
            prompt=prompt,
            label="Dispatched subagent",
            name=f"background_subagent_dispatch_{task.display_id}",
            action="dispatch",
        )
    except BaseException:
        await mw.registry.discard_unstarted(task.tool_call_id)
        raise

    logger.info(
        "Dispatched background subagent",
        task_id=task.task_id,
        display_id=task.display_id,
        subagent_type=subagent_type,
        owner_task_id=owner_task_id,
    )
    return task


class SubagentDispatcher:
    """Thread-bound handle for dispatching background subagents directly.

    Built in ``PTCAgent.create_agent`` from the ``SubAgentMiddleware``'s
    compiled graphs so direct dispatches run the exact graph instances the
    Task tool uses, and from the ``BackgroundSubagentMiddleware`` so they
    run through the same fence/spawn pipeline.
    """

    def __init__(
        self,
        middleware: "BackgroundSubagentMiddleware",
        subagent_graphs: dict[str, Any],
        thread_id: str,
    ) -> None:
        self.middleware = middleware
        self.subagent_graphs = subagent_graphs
        self.thread_id = thread_id

    @property
    def registry(self) -> Any:
        return self.middleware.registry

    @property
    def subagent_types(self) -> list[str]:
        return sorted(self.subagent_graphs)

    async def dispatch(
        self,
        *,
        subagent_type: str,
        description: str,
        prompt: str,
        run_id: str | None,
        owner_task_id: str | None = None,
        base_configurable: dict[str, Any] | None = None,
    ) -> BackgroundTask:
        return await dispatch_background_subagent(
            self.middleware,
            self.subagent_graphs,
            subagent_type=subagent_type,
            description=description,
            prompt=prompt,
            parent_thread_id=self.thread_id,
            run_id=run_id,
            owner_task_id=owner_task_id,
            base_configurable=base_configurable,
        )
