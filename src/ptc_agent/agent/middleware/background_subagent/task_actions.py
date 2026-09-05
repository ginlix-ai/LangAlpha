"""Task-tool action routing: init / update / resume for background subagents.

``handle_task_action`` is the dispatcher behind
``BackgroundSubagentMiddleware.awrap_tool_call``; the three per-action
handlers share the writer spawn pipeline in ``_spawn_writer``.
"""

import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import structlog
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from ptc_agent.agent.middleware.background_subagent.context import (
    current_background_agent_id,
    current_background_tool_call_id,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    TaskWriterLive,
)
from ptc_agent.agent.middleware.background_subagent.task import (
    WORKFLOW_SUBAGENT_TYPE,
)
from ptc_agent.agent.middleware.background_subagent.outcomes import is_credit_stop
from ptc_agent.agent.middleware.background_subagent.spawn import (
    SpawnSetupError,
    SpawnStoppedError,
    TaskRunRefused,
    settle_never_started,
    spawn_task_writer,
)

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        BackgroundSubagentMiddleware,
    )

logger = structlog.get_logger(__name__)


def _truncate_description(description: str, max_sentences: int = 2) -> str:
    """Return the first N sentences of description (period-delimited)."""
    sentences = []
    remaining = description
    for _ in range(max_sentences):
        period_idx = remaining.find(".")
        if period_idx == -1:
            sentences.append(remaining)
            break
        sentences.append(remaining[: period_idx + 1])
        remaining = remaining[period_idx + 1 :].lstrip()
        if not remaining:
            break
    return " ".join(sentences)


async def handle_task_action(
    mw: "BackgroundSubagentMiddleware",
    request: ToolCallRequest,
    handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
) -> ToolMessage | Command:
    """Route an intercepted Task tool call by its ``action`` argument.

    1. ``action="update"`` + ``task_id`` → queue follow-up via Redis to running task
    2. ``action="resume"`` + ``task_id`` → reset a completed or stopped task and respawn it
    3. ``action="init"`` (default) → new task spawn
    """
    tool_call = request.tool_call
    tool_call_id = tool_call.get("id", "unknown")
    if not tool_call_id or tool_call_id == "unknown":
        raise RuntimeError("Tool call ID is required for background tasks")
    args = tool_call.get("args", {})
    description = args.get("description", "unknown task")
    prompt = args.get("prompt", "")
    action = args.get("action", "init")
    target_task_id = args.get("task_id")
    subagent_type = args.get("subagent_type")

    # Extract parent_thread_id for hydration fallback
    parent_thread_id = (
        (request.runtime.config.get("configurable") or {}).get("thread_id", "")
        if request.runtime
        else ""
    )

    # Extract the current turn's run_id from the LangGraph config so the
    # registry can stamp it on newly-spawned subagents without relying on
    # ``registry.current_run_id`` (which races when two concurrent turns
    # on the same thread share the per-thread registry). Config metadata
    # is the reliable channel: LangChain strips the top-level ``run_id``
    # from child configs by the time a tool call executes, while metadata
    # is inherited verbatim (build_graph_config stamps run_id into both).
    runtime_config = request.runtime.config if request.runtime else {}
    current_run_id = (runtime_config.get("metadata") or {}).get(
        "run_id"
    ) or runtime_config.get("run_id")

    if action == "update":
        return await _handle_update(
            mw,
            target_task_id=target_task_id,
            subagent_type=subagent_type,
            description=description,
            prompt=prompt,
            parent_thread_id=parent_thread_id,
            tool_call_id=tool_call_id,
        )
    elif action == "resume":
        return await _handle_resume(
            mw,
            request,
            handler,
            target_task_id=target_task_id,
            subagent_type=subagent_type,
            description=description,
            prompt=prompt,
            args=args,
            parent_thread_id=parent_thread_id,
            tool_call_id=tool_call_id,
            current_run_id=current_run_id,
        )
    return await _handle_init(
        mw,
        request,
        handler,
        subagent_type=subagent_type,
        description=description,
        prompt=prompt,
        tool_call_id=tool_call_id,
        current_run_id=current_run_id,
    )


def _settle_resume_failure(task: BackgroundTask, exc: Exception) -> None:
    """Pre-spawn settle for a resume: errored, not inert.

    The task's durable result survives in its ``task:{id}`` checkpoint, so a
    resume that never got off the ground has to leave the entry resumable —
    marking it never-started would both bar the retry and mark the result
    seen, silently dropping the turn notification.
    """
    task.terminal_status = "error"
    task.error = f"resume setup failed before spawn: {exc}"


async def _spawn_writer(
    mw: "BackgroundSubagentMiddleware",
    task: BackgroundTask,
    request: ToolCallRequest,
    handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    *,
    prompt: str,
    description: str,
    tool_call_id: str,
    action: str,
) -> ToolMessage | None:
    """The Task tool's face on the canonical spawn (``spawn_task_writer``).

    Adds only what the tool call needs: a ToolMessage per refusal instead of
    an exception, and the resume settle — a failed resume keeps its durable
    result in the ``task:{id}`` checkpoint, so the entry stays resumable
    rather than going inert. Returns None once the writer is spawned.
    """
    resume = action == "resume"
    try:
        await spawn_task_writer(
            mw,
            task,
            handler,
            prompt=prompt,
            label="Resumed background subagent" if resume else "Background subagent",
            name=(
                f"background_subagent_resume_{task.display_id}"
                if resume
                else f"background_subagent_{task.display_id}"
            ),
            action=action,
            request=request,
            description=description,
            on_setup_failure=(
                _settle_resume_failure if resume else settle_never_started
            ),
        )
    except SpawnSetupError:
        return ToolMessage(
            content=(
                f"Error: could not resume {task.display_id} — setup "
                f"failed before the subagent spawned. Try again."
                if resume
                else f"Error: could not start {task.display_id} — setup "
                f"failed before the subagent spawned. Try again."
            ),
            tool_call_id=tool_call_id,
            name="Task",
        )
    except SpawnStoppedError:
        return ToolMessage(
            content=(
                f"Background subagent {task.display_id} was stopped "
                + ("before the resume started." if resume else "before it started.")
            ),
            tool_call_id=tool_call_id,
            name="Task",
        )
    return None


async def _handle_update(
    mw: "BackgroundSubagentMiddleware",
    *,
    target_task_id: str | None,
    subagent_type: str | None,
    description: str,
    prompt: str,
    parent_thread_id: str,
    tool_call_id: str,
) -> ToolMessage:
    """UPDATE: instruct a running task via its Redis steering queue."""
    resolved = await mw._resolve_or_error(
        target_task_id,
        parent_thread_id,
        tool_call_id,
        action_name="update",
    )
    if isinstance(resolved, ToolMessage):
        return resolved
    task = resolved

    # The agent just looked at this task — bump last_checked_at.
    task.last_checked_at = time.time()

    if task.cancelled:
        return ToolMessage(
            content=f"Error: Task-{target_task_id} was cancelled and cannot be updated.",
            tool_call_id=tool_call_id,
            name="Task",
        )

    if task.terminal_status == "never_started":
        return ToolMessage(
            content=(
                f"Error: Task-{target_task_id} never started "
                f"({task.error}) — there is no running subagent to instruct. "
                f"Dispatch it again with action='init'."
            ),
            tool_call_id=tool_call_id,
            name="Task",
        )

    # Validate subagent_type if explicitly provided
    if subagent_type and subagent_type != task.subagent_type:
        return ToolMessage(
            content=f"Error: Task-{target_task_id} is a '{task.subagent_type}' agent, not '{subagent_type}'.",
            tool_call_id=tool_call_id,
            name="Task",
        )

    if not task.is_pending:
        return ToolMessage(
            content=f"Error: Task-{target_task_id} is not running. Use action='resume' to resume a completed or stopped task.",
            tool_call_id=tool_call_id,
            name="Task",
        )

    input_id = await mw._queue_followup_to_redis(task, prompt)
    if input_id:
        task.last_updated_at = time.time()
        logger.info(
            "Queued follow-up for running task",
            task_id=target_task_id,
            display_id=task.display_id,
            input_id=input_id,
        )
        return ToolMessage(
            content=f"Follow-up sent to **{task.display_id}**. The subagent will receive your instructions before its next reasoning step.",
            tool_call_id=tool_call_id,
            name="Task",
            additional_kwargs={
                "task_artifact": {
                    "task_id": task.task_id,
                    "task_run_id": task.task_run_id,
                    "action": "update",
                    "description": description,
                    "prompt": prompt,
                    "input_id": input_id,
                }
            },
        )
    else:
        # The push-then-verify recheck reclaims a follow-up that
        # landed after the run settled — report the terminal race
        # honestly instead of a generic transport error.
        meta_status = None
        try:
            from ptc_agent.agent.middleware.background_subagent.redis_stream import (
                read_task_meta,
            )

            meta = await read_task_meta(
                parent_thread_id or "", task.task_id
            )
            meta_status = (meta or {}).get("status")
        except Exception:
            pass
        if meta_status and meta_status != "running":
            return ToolMessage(
                content=(
                    f"Error: Task-{target_task_id} finished "
                    f"({meta_status}) before the follow-up could be "
                    f"delivered. Check its output with "
                    f"action='output', or use action='resume' to "
                    f"continue it with new instructions."
                ),
                tool_call_id=tool_call_id,
                name="Task",
            )
        return ToolMessage(
            content=f"Error: Could not deliver follow-up to {task.display_id} -- message queue not available.",
            tool_call_id=tool_call_id,
            name="Task",
        )


async def _handle_resume(
    mw: "BackgroundSubagentMiddleware",
    request: ToolCallRequest,
    handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    *,
    target_task_id: str | None,
    subagent_type: str | None,
    description: str,
    prompt: str,
    args: dict[str, Any],
    parent_thread_id: str,
    tool_call_id: str,
    current_run_id: str | None,
) -> ToolMessage:
    """RESUME: reset a completed task and respawn it in the background."""
    resolved = await mw._resolve_or_error(
        target_task_id,
        parent_thread_id,
        tool_call_id,
        action_name="resume",
    )
    if isinstance(resolved, ToolMessage):
        return resolved
    task = resolved

    # The agent just looked at this task — bump last_checked_at.
    task.last_checked_at = time.time()

    # A credit stop spells itself as a cancel everywhere else on purpose, but
    # it is the one kind of stop whose checkpoint is meant to be re-entered:
    # the resume reminder lists exactly these tasks and tells the model to
    # resume them.
    if task.cancelled and not is_credit_stop(task.result):
        return ToolMessage(
            content=f"Error: Task-{target_task_id} was cancelled and cannot be resumed.",
            tool_call_id=tool_call_id,
            name="Task",
        )

    # Never-started tasks are refused here rather than retried through the
    # resume pipeline: there is no checkpoint to continue from, and a resume
    # would re-admit a ledger row for work that has to go back through init
    # (a refused workflow run task would come back as an unknown subagent).
    if task.terminal_status == "never_started":
        return ToolMessage(
            content=(
                f"Error: Task-{target_task_id} never started "
                f"({task.error}) — there is nothing to resume. Dispatch it "
                f"again with action='init'."
            ),
            tool_call_id=tool_call_id,
            name="Task",
        )

    # A workflow run's task is a driver entry, not a subagent. Resuming it
    # would admit a fresh ledger row and hand it to the ordinary spawn path,
    # which answers an unknown type with prose *as a successful result* — so
    # the new run completes, becomes the task's latest, and TaskOutput starts
    # resolving against it instead of the workflow's archived result. Refused
    # before admission for the same reason `never_started` is: the damage is
    # the row, not the spawn.
    if task.subagent_type == WORKFLOW_SUBAGENT_TYPE:
        return ToolMessage(
            content=(
                f"Error: Task-{target_task_id} is a workflow run and cannot be "
                f'resumed. Use TaskOutput(task_id="{target_task_id}") for its '
                f"result, or RunWorkflow to start a new run."
            ),
            tool_call_id=tool_call_id,
            name="Task",
        )

    # Validate subagent_type if explicitly provided
    if subagent_type and subagent_type != task.subagent_type:
        return ToolMessage(
            content=f"Error: Task-{target_task_id} is a '{task.subagent_type}' agent, not '{subagent_type}'.",
            tool_call_id=tool_call_id,
            name="Task",
        )

    locally_live = (
        task.asyncio_task is not None and not task.asyncio_task.done()
    )
    if locally_live:
        return ToolMessage(
            content=f"Error: Task-{target_task_id} is still running. Use action='update' to send instructions to a running task.",
            tool_call_id=tool_call_id,
            name="Task",
        )
    # Claim the resume before the first await: two parallel resume
    # calls in one model step would otherwise both observe not-live
    # and double-spawn writers for one namespace (the session fence
    # is idempotent for this run and admits both).
    if task.task_id in mw._resume_claims:
        return ToolMessage(
            content=f"Error: Task-{target_task_id} is already being resumed. Use action='update' to send instructions to it.",
            tool_call_id=tool_call_id,
            name="Task",
        )
    mw._resume_claims.add(task.task_id)
    try:
        if mw.namespace_owner is not None:
            # Not live in THIS process: the namespace lock is the
            # cluster-wide arbiter. Contended = a writer on another
            # worker (or a prior run's tail) still owns it; free =
            # safe to resume even if a stale meta/hydration said
            # "running".
            if not await mw._acquire_task_ns(task.task_id):
                return ToolMessage(
                    content=(
                        f"Error: Task-{target_task_id} is still running "
                        f"(its writer is live elsewhere). Use "
                        f"action='update' to send instructions to it."
                    ),
                    tool_call_id=tool_call_id,
                    name="Task",
                )
        elif task.is_pending:
            # No fence available (single-writer deployment): a pending
            # task here means a registered-but-unstarted local writer.
            return ToolMessage(
                content=f"Error: Task-{target_task_id} is still running. Use action='update' to send instructions to a running task.",
                tool_call_id=tool_call_id,
                name="Task",
            )

        logger.info(
            "Resuming completed task in background",
            task_id=target_task_id,
            display_id=task.display_id,
            checkpoint_ns=task.task_id,
        )

        # A resume is a NEW run: bear its ledger row under the fence
        # BEFORE the v1 reset destroys the prior round's streams — a
        # rejected resume must leave them intact.
        resume_run_id = current_run_id or mw.registry.current_run_id
        try:
            admitted = await mw.admit_task_run(
                task,
                cause="resume",
                # Args aren't backfilled yet at admission time; a model
                # that omitted description would otherwise ledger the
                # schema default instead of the task's real one.
                description=args.get("description") or task.description or "",
                launch_tool_call_id=tool_call_id,
                parent_run_id=resume_run_id,
                # Held since the liveness arbitration above.
                acquire_fence=False,
            )
        except TaskRunRefused as refusal:
            # No settle: the task keeps the result of its last run and stays
            # resumable, so this refusal costs the model only a retry.
            return ToolMessage(
                content=f"Error: could not start {task.display_id} — {refusal.reason}.",
                tool_call_id=tool_call_id,
                name="Task",
            )
        task.task_run_id = admitted or None

        await mw._reset_task_for_resume(task)

        # This run owns the writer now: rebind run ownership so its
        # drain/stop teardown and collector account for the resumed
        # writer (the original spawner may be another, long-finalized
        # run — under whose id nothing would ever await it). Stamp
        # unconditionally: None is safe (collectors treat it as
        # claimable-by-any-run), whereas keeping the stale spawner id
        # detaches the writer from every run's teardown.
        task.spawned_run_id = resume_run_id

        # Set ContextVars for the resumed task
        current_background_tool_call_id.set(task.tool_call_id)
        current_background_agent_id.set(task.agent_id)

        # Backfill args the model may omit on resume (the task already
        # carries them): the downstream Task tool schema requires
        # `description`, so a missing one would fail validation inside
        # the spawned writer — an instant no-op resume that still
        # reports success.
        arg_fills = {}
        if subagent_type is None:
            arg_fills["subagent_type"] = task.subagent_type
        if not args.get("description"):
            arg_fills["description"] = task.description or prompt[:200]
            description = arg_fills["description"]
        if arg_fills:
            args = {**args, **arg_fills}
            tool_call = {**request.tool_call, "args": args}
            request = request.override(tool_call=tool_call)

        failure = await _spawn_writer(
            mw,
            task,
            request,
            handler,
            prompt=prompt,
            description=description,
            tool_call_id=tool_call_id,
            action="resume",
        )
        if failure is not None:
            return failure
    finally:
        # Safe to drop once the writer is spawned (locally_live now
        # gates), and must drop on refusal/error so a later resume
        # can retry.
        mw._resume_claims.discard(task.task_id)

    short_description = _truncate_description(description, max_sentences=2)
    pseudo_result = (
        f"Resumed **{task.display_id}** in background with new instructions.\n"
        f"- Type: {task.subagent_type}\n"
        f"- New task: {short_description}\n"
        f"- Status: Running (resumed with full previous context)\n\n"
        f"You can:\n"
        f"- Continue with other work\n"
        f'- Use `TaskOutput(task_id="{task.task_id}")` to get progress or result'
    )

    return ToolMessage(
        content=pseudo_result,
        tool_call_id=tool_call_id,
        name="Task",
        additional_kwargs={
            "task_artifact": {
                "task_id": task.task_id,
                "task_run_id": task.task_run_id,
                "action": "resume",
                "description": description,
                "prompt": prompt,
                "type": task.subagent_type,
            }
        },
    )


async def _handle_init(
    mw: "BackgroundSubagentMiddleware",
    request: ToolCallRequest,
    handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    *,
    subagent_type: str | None,
    description: str,
    prompt: str,
    tool_call_id: str,
    current_run_id: str | None,
) -> ToolMessage:
    """INIT (default): register, admit, and spawn a new background task."""
    if subagent_type is None:
        subagent_type = "general-purpose"

    # Register the task first to get the task_id. A checkpoint
    # re-execution of an already-spawned call raises TaskWriterLive
    # (atomically, under the registry lock): re-registering would
    # displace the live writer's routing identity (events would
    # attribute to an inert replacement without its task_run_id/v2
    # stream) and ledger admission would reject the duplicate anyway
    # — answer idempotently with the live task instead.
    try:
        task = await mw.registry.register(
            tool_call_id=tool_call_id,
            description=description,
            prompt=prompt,
            subagent_type=subagent_type,
            asyncio_task=None,  # Will be set after task creation
            run_id=current_run_id,
        )
    except TaskWriterLive as exc:
        existing = exc.task
        logger.info(
            "Task tool call re-executed while its writer is alive; "
            "returning the existing task",
            tool_call_id=tool_call_id,
            task_id=existing.task_id,
        )
        return ToolMessage(
            content=(
                f"Background subagent already running: "
                f"**{existing.display_id}**\n"
                f"- Type: {existing.subagent_type}\n"
                f"- Status: Running in background\n\n"
                f'Use `TaskOutput(task_id="{existing.task_id}")` to get '
                f"progress or result"
            ),
            tool_call_id=tool_call_id,
            name="Task",
            additional_kwargs={
                "task_artifact": {
                    "task_id": existing.task_id,
                    "task_run_id": existing.task_run_id,
                    "action": "init",
                    "description": existing.description,
                    "prompt": existing.prompt,
                    "type": existing.subagent_type,
                }
            },
        )
    logger.info(
        "Intercepting task tool call for background execution",
        tool_call_id=tool_call_id,
        task_id=task.task_id,
        display_id=task.display_id,
        subagent_type=subagent_type,
        description=description[:100],
    )

    try:
        admitted = await mw.admit_task_run(
            task,
            cause="init",
            description=description,
            launch_tool_call_id=tool_call_id,
            parent_run_id=task.spawned_run_id,
        )
    except TaskRunRefused as refusal:
        # Nothing will ever run under this entry, so leave it inert — no
        # collector claims it and no scanner retries it.
        task.mark_never_started(refusal.settle_reason)
        return ToolMessage(
            content=f"Error: could not start {task.display_id} — {refusal.reason}.",
            tool_call_id=tool_call_id,
            name="Task",
        )
    task.task_run_id = admitted or None

    current_background_tool_call_id.set(tool_call_id)
    current_background_agent_id.set(task.agent_id)

    failure = await _spawn_writer(
        mw,
        task,
        request,
        handler,
        prompt=prompt,
        description=description,
        tool_call_id=tool_call_id,
        action="init",
    )
    if failure is not None:
        return failure

    # Return immediate pseudo-result with Task-N format
    short_description = _truncate_description(description, max_sentences=2)
    pseudo_result = (
        f"Background subagent deployed: **{task.display_id}**\n"
        f"- Type: {subagent_type}\n"
        f"- Task: {short_description}\n"
        f"- Status: Running in background\n\n"
        f"You can:\n"
        f"- Continue with other work\n"
        f'- Use `TaskOutput(task_id="{task.task_id}")` to get progress or result'
    )

    return ToolMessage(
        content=pseudo_result,
        tool_call_id=tool_call_id,
        name="Task",
        additional_kwargs={
            "task_artifact": {
                "task_id": task.task_id,
                "task_run_id": task.task_run_id,
                "action": "init",
                "description": description,
                "prompt": prompt,
                "type": subagent_type,
            }
        },
    )
