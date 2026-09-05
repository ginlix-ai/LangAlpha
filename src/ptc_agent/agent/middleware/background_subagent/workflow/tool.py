"""RunWorkflow tool factory for server-side JavaScript orchestration."""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Annotated, Any

import structlog
from langchain_core.messages import ToolMessage
from langchain_core.tools import BaseTool, InjectedToolCallId, tool
from langgraph.config import get_config

from ptc_agent.agent.backends.workflows import (
    workflow_key,
    workflow_namespace,
    workflow_script_from_value,
)
from ptc_agent.agent.middleware.background_subagent.dispatch import (
    SubagentDispatcher,
)
from ptc_agent.agent.middleware.background_subagent.registry import TaskWriterLive
from ptc_agent.agent.middleware.background_subagent.task import (
    WORKFLOW_SUBAGENT_TYPE,
)
from ptc_agent.agent.middleware.background_subagent.spawn import (
    SpawnError,
    TaskRunRefused,
    spawn_task_writer,
)
from ptc_agent.agent.middleware.background_subagent.workflow.driver import (
    WorkflowDriver,
    WorkflowRunSpec,
    run_dir,
)
from ptc_agent.agent.middleware.background_subagent.workflow.engine import (
    MAX_DESCRIPTION_CHARS,
    WorkflowScriptError,
    acompile_check,
)
from ptc_agent.agent.middleware.background_subagent.workflow.prebuilt import (
    PrebuiltWorkflowRegistry,
)
from ptc_agent.core.paths import (
    HIDDEN_DIR_NAMES,
    MEMO_USER_DIR,
    MEMORY_USER_DIR,
    MEMORY_WORKSPACE_DIR,
)

logger = structlog.get_logger(__name__)

# Derived from the canonical path vocabulary rather than re-spelled here: a
# script under one of these lives in the store or in agent infrastructure, not
# on the workspace filesystem the backend reads from.
_UNREADABLE_SCRIPT_PREFIXES: tuple[str, ...] = (
    f"{MEMORY_USER_DIR}/",
    f"{MEMORY_WORKSPACE_DIR}/",
    f"{MEMO_USER_DIR}/",
)


# The budget every other store reader on the agent side carries. Resolution
# runs inline in the tool body, before the run exists, so `run_timeout` is not
# standing behind this read — nothing is.
_STORE_OP_TIMEOUT_S = 2.0


class _Refused(Exception):
    """Carries the reply a refused call answers with, raised by the phase
    helpers so a resolved script has exactly one shape at the call site.

    The failure prefix is stamped here rather than written at each raise. A
    refusal starts no run, so no task artifact binds the launch card and no
    channel will ever close it — this text is the only signal the card has to
    settle on, and the one wording that omitted the prefix left the card
    spinning "Running" for the life of the thread.
    """

    def __init__(self, reason: str) -> None:
        reply = reason if reason[:5].lower() == "error" else f"Error: {reason}"
        super().__init__(reply)
        self.reply = reply


def _script_path_forms(backend: Any, path: str) -> set[str]:
    """Every spelling of ``path`` the guard has to answer for.

    The router picks a mount from the *normalized* path, so a guard reading
    only the caller's text refuses `.agents/user/memo/x.js` while admitting
    the absolute spelling of that same file. Checking both forms rather than
    replacing one with the other keeps canonicalization able to add refusals
    and never to remove them, so a backend without these helpers degrades to
    the raw check instead of opening a hole.
    """
    forms = {path}
    try:
        forms.add(backend.virtualize_path(backend.normalize_path(path)))
    except Exception:  # noqa: BLE001 - a backend that cannot canonicalize
        logger.debug("script_path canonicalization unavailable", path=path)
    return forms


def _script_path_is_unreadable(path: str) -> bool:
    # Whole segments, not substrings: both vocabularies name directories, and
    # matching them anywhere in the text refuses ordinary files whose name
    # merely contains one (`workflows/company_internal.js`).
    normalized = path.strip("/")
    segments = [segment for segment in normalized.split("/") if segment not in ("", ".")]
    if any(segment in HIDDEN_DIR_NAMES for segment in segments):
        return True
    return any(
        normalized.startswith(prefix) for prefix in _UNREADABLE_SCRIPT_PREFIXES
    )


def _reply_with_artifact(
    run_task: Any,
    result_text: str,
    *,
    workflow_name: str,
    description: str,
    tool_call_id: str,
) -> str | ToolMessage:
    """Carry the artifact that binds the frontend card to the run's task lane.

    Every reply naming a live run goes through here — a checkpoint
    re-execution reattaches to a run the card still has to find, so answering
    it with bare text would leave the recovered run uncarded.
    """
    if not tool_call_id:
        return result_text
    # ``action: "workflow"`` — deliberately NOT "init": the history
    # TaskLaneProjector claims checkpoint namespaces for init/resume
    # launches, and a workflow run task has none (its writer is the
    # driver coroutine). The artifact still flows through the live SSE
    # producer, the projector, and replay status stamping, which is what
    # binds the frontend card to the run's task lane.
    return ToolMessage(
        content=result_text,
        tool_call_id=tool_call_id,
        name="RunWorkflow",
        additional_kwargs={
            "task_artifact": {
                "task_id": run_task.task_id,
                "task_run_id": run_task.task_run_id,
                "action": "workflow",
                "description": description,
                "prompt": description,
                "type": "workflow",
                "workflow": workflow_name,
            }
        },
    )


def _run_started_reply(
    run_task: Any,
    *,
    workflow_name: str,
    description: str,
    base_rel: str,
    tool_call_id: str,
) -> str | ToolMessage:
    """The launch reply, plus the artifact binding the card to the run."""
    result_text = (
        f"Workflow run started: **{run_task.display_id}**\n"
        f"- Workflow: {workflow_name}\n"
        "- Status: Running in background\n"
        f"- Run files: {base_rel}/\n\n"
        "You can:\n"
        "- Continue with other work\n"
        f'- Use `TaskOutput(task_id="{run_task.task_id}")` for progress '
        "or the final result\n"
        f'- Use `TaskOutput(task_id="{run_task.task_id}", timeout=120)` '
        "to wait for completion\n"
        f"- Read per-child records from {base_rel}/children/"
    )
    return _reply_with_artifact(
        run_task,
        result_text,
        workflow_name=workflow_name,
        description=description,
        tool_call_id=tool_call_id,
    )


def create_run_workflow_tool(
    *,
    dispatcher: SubagentDispatcher,
    backend: Any,
    thread_id: str,
    short_thread_id: str,
    store: Any | None = None,
    user_id: str | None = None,
    prebuilt_workflows: PrebuiltWorkflowRegistry | None = None,
) -> BaseTool:
    mw = dispatcher.middleware
    registry = dispatcher.registry
    bound_user_id = user_id
    # Counting open runs and landing this one's ledger row have to be one
    # critical section: ToolNode gathers a message's tool calls, so sibling
    # RunWorkflow calls run concurrently here, both the count and the insert
    # suspend, and unlike the per-task slot this cap has no DB constraint
    # behind it. A process-local lock suffices because an admission cannot
    # complete without a live root-guard session — `_acquire_task_ns` fails
    # closed, so a worker whose guard died reaches the count but never the
    # insert. That stops holding if RunWorkflow ceases to be main-agent-only,
    # or the admit ceases to be namespace-fenced; either would need an advisory
    # lock spanning count and insert.
    admission_lock = asyncio.Lock()

    async def _load_named_workflow(name: str) -> tuple[str, str] | None:
        """Resolve a workflow name to ``(script, source)`` — user-saved shadows builtin.

        A failing, slow, or unreadable store read propagates: swallowing any of
        them would resolve the user's saved workflow to the shipped builtin of
        the same name and run a different script under that name.
        """
        if store is not None and bound_user_id:
            item = await asyncio.wait_for(
                store.aget(workflow_namespace(bound_user_id), workflow_key(name)),
                timeout=_STORE_OP_TIMEOUT_S,
            )
            if item is not None:
                return workflow_script_from_value(item.value), "saved"
        if prebuilt_workflows is not None:
            content = prebuilt_workflows.get(name)
            if content is not None:
                return content, "builtin"
        return None

    def _unknown_workflow_error(name: str) -> str:
        names = prebuilt_workflows.names() if prebuilt_workflows is not None else []
        available = (
            f" Available pre-built workflows: {', '.join(names)}." if names else ""
        )
        return (
            f"Unknown workflow '{name}'.{available} "
            "Workflows live in .agents/workflows/<name>.js."
        )

    async def _resolve_script(
        script: str | None, script_path: str | None, workflow: str | None
    ) -> tuple[str, str]:
        """Resolve whichever of the three source modes was given to
        ``(script, source)``, refusing rather than returning on failure."""
        if script_path is not None:
            if any(
                _script_path_is_unreadable(form)
                for form in _script_path_forms(backend, script_path)
            ):
                raise _Refused(
                    f"script_path '{script_path}' is not on the "
                    "workspace filesystem — memory, memo and _internal paths "
                    "are managed elsewhere. Pass the script inline with "
                    "'script', or save it as .agents/workflows/<name>.js and "
                    "run it by name with 'workflow'."
                )
            try:
                from_file = await backend.aread_text(script_path)
            except Exception as error:
                # The reason stays in the log: a backend failure here is a
                # store or transport error whose text can carry hosts and
                # query fragments, and this reply is handed to the model and
                # persisted with the turn. Retryability is all the caller can
                # act on anyway.
                logger.warning(
                    "Workflow script_path read failed",
                    script_path=script_path,
                    exc_info=True,
                )
                raise _Refused(
                    f"could not read script_path '{script_path}'. "
                    "Try again, or pass the script inline."
                ) from error
            if not from_file:
                raise _Refused(
                    f"script_path '{script_path}' is missing or empty."
                )
            return from_file, "file"

        if workflow is not None:
            try:
                resolved = await _load_named_workflow(workflow)
            except Exception as error:
                logger.warning(
                    "Workflow store read failed",
                    workflow=workflow,
                    user_id=bound_user_id,
                    exc_info=True,
                )
                raise _Refused(
                    f"could not read workflow '{workflow}' from the "
                    "workflow store. Try again, or pass the script inline."
                ) from error
            if resolved is None:
                raise _Refused(_unknown_workflow_error(workflow))
            return resolved

        return script or "", "inline"

    async def _admit_run(
        run_task: Any, caps: Any, *, description: str, launch_tool_call_id: str
    ) -> None:
        """Take the per-thread run slot and bear the ledger row under one lock,
        stamping ``task_run_id``; refuses without leaving a claimable entry."""
        async with admission_lock:
            active_ids: list[str]
            run_ledger = getattr(registry, "run_ledger", None)
            if run_ledger is None:
                # No ledger to consult (CLI): one process holds every run, so
                # its registry is the whole truth rather than a slice of it.
                active_ids = [
                    task.task_id
                    for task in await registry.get_all_tasks()
                    if task.subagent_type == "workflow"
                    and task.is_pending
                    and task.task_id != run_task.task_id
                ]
            else:
                try:
                    rows = await run_ledger.list_open_workflow_runs()
                except Exception as error:
                    # A ledgered deployment is multi-worker, so the local
                    # registry is a fraction of the truth, not a degraded
                    # copy of it — counting it here would hand every worker a
                    # fresh quota. Refuse: this is admission, where a re-read
                    # is the caller's next message, not a stuck state.
                    logger.warning(
                        "Workflow cap ledger read failed; refusing admission",
                        thread_id=thread_id,
                        exc_info=True,
                    )
                    run_task.mark_never_started("workflow cap ledger unreadable")
                    raise _Refused(
                        "could not read the workflow run ledger to "
                        "check the per-thread cap. Try again shortly."
                    ) from error
                active_ids = [
                    str(row["task_id"])
                    for row in rows
                    if str(row["task_id"]) != run_task.task_id
                ]
            if len(active_ids) >= caps.max_runs_per_thread:
                running = ", ".join(active_ids)
                run_task.mark_never_started("per-thread workflow cap reached")
                raise _Refused(
                    f"{len(active_ids)} workflow run(s) already active "
                    f"({running}); the per-thread cap is "
                    f"{caps.max_runs_per_thread}. "
                    "Wait for one to finish or cancel it first."
                )

            # A workflow run task is a turn-lifecycle run like any Task spawn,
            # so it is admitted the same way — inside the cap's critical
            # section, since the ledger row is what the count reads.
            try:
                admitted = await mw.admit_task_run(
                    run_task,
                    cause="init",
                    description=description,
                    launch_tool_call_id=launch_tool_call_id,
                    parent_run_id=run_task.spawned_run_id,
                )
            except TaskRunRefused as refusal:
                run_task.mark_never_started(refusal.settle_reason)
                raise _Refused(
                    f"could not start {run_task.display_id} — "
                    f"{refusal.reason}."
                ) from refusal
            run_task.task_run_id = admitted or None

    async def _launch(
        script: str | None,
        script_path: str | None,
        workflow: str | None,
        params: Any,
        description: str | None,
        tool_call_id: str,
    ) -> str | ToolMessage:
        """Every path that starts a run, raising ``_Refused`` for those that
        do not — so the caller turns a refusal into a reply exactly once."""
        sources = (script, script_path, workflow)
        if sum(source is not None for source in sources) != 1:
            raise _Refused(
                "Provide exactly one of 'script', 'script_path', "
                "or 'workflow'."
            )

        # A ledgered run answers TaskOutput from its archived result, which
        # only a checkpointer can hold. Refusing here beats admitting a run
        # whose result would be unreadable the moment its turn ends.
        if getattr(registry, "run_ledger", None) is not None and mw.checkpointer is None:
            raise _Refused(
                "workflow runs are unavailable — no checkpointer is "
                "configured for this thread."
            )

        script, script_source = await _resolve_script(
            script, script_path, workflow
        )

        from src.config.settings import get_workflow_orchestration_config

        caps = get_workflow_orchestration_config()
        if len(script.encode()) > caps.max_script_bytes:
            raise _Refused(
                f"Invalid workflow script — script exceeds "
                f"{caps.max_script_bytes // 1024}KB"
            )
        try:
            meta = await acompile_check(script)
        except WorkflowScriptError as error:
            raise _Refused(f"Invalid workflow script — {error}") from error
        # A name-resolved run is announced, framed and carded under meta.name,
        # so a script saved as one name and declaring another runs as the
        # other everywhere — while the REST surface lists it invalid.
        if workflow is not None and meta.name != workflow:
            raise _Refused(
                f"workflow '{workflow}' declares meta.name "
                f"'{meta.name}'. Rename one so they agree, then run it again."
            )

        # One bounded launch description for every sink. `meta.description`
        # is capped at extraction precisely because it is copied into the
        # registry, the run ledger, the launch stream and the task artifact —
        # the tool argument overrides it and reaches all four, so it answers
        # to the same cap rather than to the model's restraint.
        launch_description = (description or meta.description)[:MAX_DESCRIPTION_CHARS]

        # Capture turn attribution at call time: the run stamp children
        # inherit and the configurable they run under. The turn's run_id
        # must come from metadata — langchain's patch_config strips the
        # top-level run_id from tool-call configs (the handler stamps both).
        config = get_config() or {}
        configurable = dict(config.get("configurable") or {})
        metadata = config.get("metadata") or {}
        launch_run_id = metadata.get("run_id") or config.get("run_id")

        # A checkpoint re-execution of an already-spawned call raises
        # TaskWriterLive — answer idempotently with the live run.
        launch_tool_call_id = tool_call_id or f"wfrun-{uuid.uuid4().hex[:12]}"
        try:
            run_task = await registry.register(
                tool_call_id=launch_tool_call_id,
                description=launch_description,
                prompt=script[:500],
                subagent_type=WORKFLOW_SUBAGENT_TYPE,
                run_id=str(launch_run_id) if launch_run_id else None,
            )
        except TaskWriterLive as exc:
            existing = exc.task
            return _reply_with_artifact(
                existing,
                f"Workflow run already active: **{existing.display_id}** "
                f"({meta.name}). Use "
                f'`TaskOutput(task_id="{existing.task_id}")` for progress '
                "or the final result.",
                workflow_name=meta.name,
                description=launch_description,
                tool_call_id=tool_call_id,
            )

        # Admission runs AFTER registration so a checkpoint re-execution of a
        # live call reattaches idempotently above instead of tripping over its
        # own run in the count.
        # A refused admission leaves the id nowhere — and a never-started entry
        # has no writer, capture or usage, which is exactly what stops a
        # collector claiming and evicting it. Same asymmetry the direct-dispatch
        # path handles, and the same guard: `discard_unstarted` refuses anything
        # that settled some other way, so a stop mid-admission keeps its entry.
        try:
            await _admit_run(
                run_task,
                caps,
                description=launch_description,
                launch_tool_call_id=launch_tool_call_id,
            )
        except BaseException:
            await registry.discard_unstarted(launch_tool_call_id)
            raise

        base_rel = run_dir(short_thread_id, run_task.task_id)
        artifacts = {
            "workflow.js": script,
            "args.json": json.dumps(params, ensure_ascii=False, default=str),
        }
        for filename, content in artifacts.items():
            try:
                await backend.awrite_text(f"{base_rel}/{filename}", content)
            except Exception:
                logger.warning(
                    "Failed to snapshot workflow artifact",
                    path=f"{base_rel}/{filename}",
                    exc_info=True,
                )

        spec = WorkflowRunSpec(
            run_task=run_task,
            registry=registry,
            dispatcher=dispatcher,
            backend=backend,
            checkpointer=mw.checkpointer,
            thread_id=thread_id,
            short_thread_id=short_thread_id,
            script=script,
            script_args=params,
            meta=meta,
            source=script_source,
            base_configurable=configurable,
            caps=caps,
        )
        driver = WorkflowDriver(spec)
        try:
            await spawn_task_writer(
                mw,
                run_task,
                driver.run,
                prompt=launch_description,
                label="Workflow run",
                name=f"workflow_run_{run_task.display_id}",
                action="init",
            )
        except SpawnError as error:
            raise _Refused(
                f"could not start {run_task.display_id} — {error}."
            ) from error

        logger.info(
            "Workflow run started",
            wf_task_id=run_task.task_id,
            workflow=meta.name,
            thread_id=thread_id,
        )
        return _run_started_reply(
            run_task,
            workflow_name=meta.name,
            description=launch_description,
            base_rel=base_rel,
            tool_call_id=tool_call_id,
        )

    @tool("RunWorkflow")
    async def run_workflow(
        script: str | None = None,
        script_path: str | None = None,
        workflow: str | None = None,
        params: Any = None,
        description: str | None = None,
        tool_call_id: Annotated[str, InjectedToolCallId] = "",
    ) -> str | ToolMessage:
        """Run a JavaScript workflow script server-side to orchestrate subagents.

        Use for multi-subagent pipelines instead of issuing many Task calls.
        Provide a script with a ``meta`` literal that uses ``agent()``,
        ``parallel()``, ``pipeline()``, ``phase()`` and ``log()``; or run a
        saved or pre-built workflow by name. ``params`` is exposed to the
        script as its ``args`` global.
        """
        # The one place a refusal becomes a reply. A launch that is turned away
        # never starts a run, so nothing binds its card and no channel closes —
        # the reply text is the card's only settle signal, and routing every
        # refusal through `_Refused` is what keeps that text one shape.
        try:
            return await _launch(
                script, script_path, workflow, params, description, tool_call_id
            )
        except _Refused as refused:
            return refused.reply

    return run_workflow
