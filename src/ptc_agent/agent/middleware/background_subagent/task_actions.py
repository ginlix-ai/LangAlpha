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

from ptc_agent.agent.middleware.background_subagent import run_executor
from ptc_agent.agent.middleware.background_subagent.context import (
    current_background_agent_id,
    current_background_tool_call_id,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    TaskWriterLive,
)
from src.observability.tracing import (
    create_task_with_context,
    emit_subagent_launch,
)
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

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
    2. ``action="resume"`` + ``task_id`` → reset completed task and respawn in background
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
    """The shared spawn pipeline for init and resume: launch telemetry →
    pre-spawn "running" meta → run-opener event → fenced writer publish →
    done-callback. Returns an error/stop ToolMessage when no writer spawned
    (the admitted run is settled before returning), or None on success.
    """
    resume = action == "resume"
    # Create a dedicated token tracker for this subagent
    subagent_token_tracker = PerCallTokenTracker()

    # Spawn in background. create_task_with_context propagates the current
    # OTel context (via contextvars snapshot) so spans emitted inside the
    # subagent inherit the launching chat.turn trace.
    try:
        emit_subagent_launch(
            task.subagent_type, action=action, description_len=len(description),
        )
        # Meta before spawn: a fast writer's terminal meta (written at
        # settle) must never be overwritten by a late "running".
        await mw.registry.write_task_meta(task, "running")
        await mw._append_run_opener(task, prompt)
        asyncio_task = await mw.registry.publish_writer(
            task,
            lambda: create_task_with_context(
                run_executor._run_background_task(
                    task, handler, request, subagent_token_tracker,
                    "Resumed background subagent" if resume else "Background subagent",
                    registry=mw.registry,
                    namespace_owner=mw.namespace_owner,
                ),
                name=(
                    f"background_subagent_resume_{task.display_id}"
                    if resume
                    else f"background_subagent_{task.display_id}"
                ),
            ),
        )
    except Exception as e:
        # An admitted run either spawns or terminates — never a stranded
        # in_progress row. Resume re-settles the entry so the task stays
        # resumable (its durable result survives in the task:{id}
        # checkpoint); init marks the entry inert.
        await mw._abort_admitted_run(task, e)
        if resume:
            task.completed = True
            task.error = f"resume setup failed before spawn: {e}"
            return ToolMessage(
                content=(
                    f"Error: could not resume {task.display_id} — setup "
                    f"failed before the subagent spawned. Try again."
                ),
                tool_call_id=tool_call_id,
                name="Task",
            )
        task.mark_never_started(f"setup failed before spawn: {e}")
        return ToolMessage(
            content=(
                f"Error: could not start {task.display_id} — setup "
                f"failed before the subagent spawned. Try again."
            ),
            tool_call_id=tool_call_id,
            name="Task",
        )
    if asyncio_task is None:
        # A stop stamped the (handle-less) task during a setup await — the
        # publish fence refused the writer, so the run ends here,
        # admitted-but-never-spawned.
        await mw._finalize_cancelled_before_spawn(task)
        return ToolMessage(
            content=(
                f"Background subagent {task.display_id} was stopped "
                + ("before the resume started." if resume else "before it started.")
            ),
            tool_call_id=tool_call_id,
            name="Task",
        )
    asyncio_task.add_done_callback(
        run_executor._make_task_done_callback(
            task, mw._finalize_cancelled_before_spawn
        )
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

    # Validate subagent_type if explicitly provided
    if subagent_type and subagent_type != task.subagent_type:
        return ToolMessage(
            content=f"Error: Task-{target_task_id} is a '{task.subagent_type}' agent, not '{subagent_type}'.",
            tool_call_id=tool_call_id,
            name="Task",
        )

    if not task.is_pending:
        return ToolMessage(
            content=f"Error: Task-{target_task_id} is not running. Use action='resume' to resume a completed task.",
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

    if task.cancelled:
        return ToolMessage(
            content=f"Error: Task-{target_task_id} was cancelled and cannot be resumed.",
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
        admitted = await mw._admit_task_run(
            task,
            cause="resume",
            # Args aren't backfilled yet at admission time; a model
            # that omitted description would otherwise ledger the
            # schema default instead of the task's real one.
            description=args.get("description") or task.description or "",
            launch_tool_call_id=tool_call_id,
            parent_run_id=resume_run_id,
        )
        if isinstance(admitted, ToolMessage):
            return admitted  # fence already released by _admit_task_run
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
        f'- Use `TaskOutput(task_id="{task.task_id}")` to get progress or result\n'
        f'- Use `TaskOutput(task_id="{task.task_id}", timeout=60)` to wait until complete'
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

    if mw.namespace_owner is not None and not await mw._acquire_task_ns(
        task.task_id
    ):
        # A fresh task_id is practically uncontended, so this is the
        # fence itself being unusable (guard session dead) — refuse
        # rather than spawn an unfenced writer, and leave the entry
        # inert so no collector claims it.
        task.mark_never_started("namespace fence unavailable")
        return ToolMessage(
            content=(
                f"Error: could not start {task.display_id} — its "
                f"checkpoint namespace could not be fenced. Try again."
            ),
            tool_call_id=tool_call_id,
            name="Task",
        )

    # Ledger row born under N(thread, task:id), before any spawn side
    # effect — admission-authoritative from day one.
    admitted = await mw._admit_task_run(
        task,
        cause="init",
        description=description,
        launch_tool_call_id=tool_call_id,
        parent_run_id=task.spawned_run_id,
    )
    if isinstance(admitted, ToolMessage):
        task.mark_never_started("run admission rejected")
        return admitted
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
        f'- Use `TaskOutput(task_id="{task.task_id}")` to get progress or result\n'
        f'- Use `TaskOutput(task_id="{task.task_id}", timeout=60)` to wait until complete\n'
        f"- Use `TaskOutput(timeout=60)` to wait for all background tasks"
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
