"""Server-side driver and host bridge for QuickJS workflow runs."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any

import structlog

from ptc_agent.agent.middleware.background_subagent.dispatch import SubagentDispatcher
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.tools import extract_result_content
from ptc_agent.agent.middleware.background_subagent.workflow.emitter import (
    WORKFLOW_CHILD_STATUSES,
    WorkflowChildStatus,
    WorkflowEmitter,
    WorkflowRunStatus,
    truncate_to_bytes,
)
from ptc_agent.agent.middleware.background_subagent.workflow.engine import (
    WorkflowHostError,
    WorkflowUsageError,
    WorkflowLimits,
    WorkflowMeta,
    WorkflowOutcome,
    run_workflow_script,
)
from ptc_agent.agent.middleware.background_subagent.workflow.ui_snapshot import (
    persist_task_result,
)
from ptc_agent.agent.middleware.background_subagent.workflow.validation import (
    DispatchValidationError,
    check_prompt_cap,
    compose_child_prompt,
    parse_schema_result,
    validate_dispatch,
)
from src.config.models import WorkflowOrchestrationConfig
from src.server.utils.error_sanitization import sanitize_error_text

logger = structlog.get_logger(__name__)

# The per-child record is a workspace file rather than a UI frame, so it keeps
# more of the failure than the 500-byte SSE clip — bounded all the same, since
# the text is an upstream exception sized by nothing we control.
_RECORD_ERROR_BYTES = 4000
# How long a timed-out child's writer gets to finish unwinding before this run
# reads its usage. Generous for an already-cancelled task, and the cost of
# overrunning it is a token count, never a stuck run.
_CANCEL_SETTLE_TIMEOUT = 5.0


def run_dir(short_thread_id: str, run_task_id: str) -> str:
    """A run's artifact directory — the launch-time snapshot and the driver's
    own writes have to land in the same place."""
    return f".agents/threads/{short_thread_id}/workflows/{run_task_id}"


@dataclass(kw_only=True)
class WorkflowRunSpec:
    """Everything a driver needs, captured at RunWorkflow call time."""

    run_task: BackgroundTask
    registry: BackgroundTaskRegistry
    dispatcher: SubagentDispatcher
    backend: Any
    # The run's graph checkpointer (the writer-guard's session-bound saver on
    # fenced deployments) — the terminal ui snapshot writes through it so the
    # namespace fence applies to the snapshot too. None = no persistence.
    checkpointer: Any = None
    thread_id: str
    short_thread_id: str
    script: str
    meta: WorkflowMeta
    script_args: Any = None
    # Where the script came from: 'inline' | 'file' | 'saved' | 'builtin'.
    source: str = "inline"
    base_configurable: dict[str, Any] = field(default_factory=dict)
    caps: WorkflowOrchestrationConfig = field(
        default_factory=WorkflowOrchestrationConfig
    )


class WorkflowRunError(RuntimeError):
    """Terminal workflow failure; raised from ``run()`` so the writer
    wrapper (``_run_background_task``) records the run as an error."""


@dataclass
class _Terminal:
    """A finished script, normalized: ``summary`` on success, ``error`` on
    failure. Every arm of ``run()`` produces one, so the settle tail below
    them runs once and reads the same way for every exit."""

    summary: str = ""
    result_preview: str | None = None
    error: str | None = None


@dataclass(kw_only=True)
class _ChildRun:
    """One dispatched child, from admission to record.

    The single description of a child: every arm below mutates this in place,
    so the script's ``agent()`` return and the on-disk record are two views of
    one object rather than two shapes assembled from the same locals.
    """

    seq: int
    label: str
    phase: str | None
    subagent_type: str
    started_at: float
    # One clock read serves the frame and the record, which otherwise disagree
    # about how long the child took by however long the frame's emit blocked.
    ended_at: float = 0.0
    task: BackgroundTask | None = None
    status: WorkflowChildStatus = "error"
    content: str | None = None
    error: str | None = None
    parsed: Any = None
    schema_valid: bool | None = None
    truncated: bool = False
    tokens_used: int = 0

    @property
    def duration_s(self) -> float:
        return round(self.ended_at - self.started_at, 1)

    def to_record(self) -> dict[str, Any]:
        task_id = self.task.task_id if self.task else None
        # The error travels the same sanitize-then-clip path the UI frame takes
        # (clipping first could split a credential and leave a fragment the
        # pattern misses). This record is a file in the agent's workspace, so
        # an upstream exception carrying a token must not land verbatim.
        error = self.error
        if error:
            error = truncate_to_bytes(sanitize_error_text(error), _RECORD_ERROR_BYTES)[0]
        return {
            "seq": self.seq,
            "task_id": task_id,
            "label": self.label,
            "subagent_type": self.subagent_type,
            "phase": self.phase,
            "status": self.status,
            "result": self.content,
            "result_json": self.parsed,
            "schema_valid": self.schema_valid,
            "error": error,
            "truncated": self.truncated,
            "full_result_ref": {"task_id": task_id} if task_id else None,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_s": self.duration_s,
        }


def _stop_targets_this_run() -> bool:
    """Whether the cancellation being handled is aimed at the run itself.

    A child's task is awaited through a shield, so a stop aimed at that child
    alone surfaces as ``CancelledError`` without this coroutine ever being
    cancelled — which is what separates the two cases.
    """
    current = asyncio.current_task()
    return current is not None and bool(current.cancelling())


def _child_status(task: BackgroundTask | None) -> WorkflowChildStatus:
    """The child's outcome as the run card spells it.

    The task is the single authority; only the name differs — the child
    vocabulary says ``ok`` where a task says ``completed``. A task that never
    reached a terminal status failed on the way there, and so does any status
    the vocabulary doesn't declare: widening ``TerminalStatus`` must not widen
    the wire without someone deciding it should.
    """
    status = task.terminal_status if task is not None else None
    if status == "completed":
        return "ok"
    return status if status in WORKFLOW_CHILD_STATUSES else "error"


class WorkflowDriver:
    """Run one workflow and implement its server-side host operations."""

    def __init__(self, spec: WorkflowRunSpec) -> None:
        self.spec = spec
        self.wf_task_id = spec.run_task.task_id
        self.base_rel = run_dir(spec.short_thread_id, self.wf_task_id)
        self._emitter = WorkflowEmitter(
            run_task=spec.run_task,
            registry=spec.registry,
            checkpointer=spec.checkpointer,
            thread_id=spec.thread_id,
            max_summary_bytes=spec.caps.max_summary_bytes,
        )
        self._sem: asyncio.Semaphore | None = None
        self._dispatched = 0
        self._child_seq = 0
        self._tokens_spent = 0
        # Per-child usage already folded into the total, keyed by child task id.
        self._accrued: dict[str, int] = {}
        self._started_at: float | None = None
        # Children that have announced themselves but not yet reported a
        # terminal frame, keyed by seq. A stop unwinds their coroutines only
        # after this run's own terminal frame is persisted, so the cancel path
        # reports them from here instead of waiting on them.
        self._inflight: dict[int, dict[str, Any]] = {}
        # Set once the run starts tearing down, so a child unwinding afterwards
        # is read as part of that teardown rather than as a child-alone stop.
        self._stopping = False

    async def run(self, _request: Any = None) -> str:
        """Drive the run to a terminal state.

        Returns the summary string the launching turn shows on success;
        raises ``WorkflowRunError`` on failure/timeout (the writer wrapper
        turns that into the task's error result and ledger status).
        """
        caps = self.spec.caps
        self._emitter.bind_loop(asyncio.get_running_loop())
        self._sem = asyncio.Semaphore(caps.max_concurrent_children)
        self._started_at = time.time()
        try:
            await self._emitter.emit(
                "run_started",
                name=self.spec.meta.name,
                description=self.spec.meta.description,
                source=self.spec.source,
            )
            async with asyncio.timeout(caps.run_timeout):
                outcome = await run_workflow_script(
                    self.spec.script,
                    self.spec.script_args,
                    host=self,
                    limits=WorkflowLimits(
                        memory_limit_mb=caps.memory_limit_mb,
                        cpu_budget_s=caps.cpu_budget_s,
                    ),
                )
            # Terminal bookkeeping runs outside the deadline — a run that
            # beat the timeout must not have its finalize killed by it.
            terminal = await self._finish_outcome(outcome)
        except asyncio.CancelledError:
            await asyncio.shield(self._on_cancelled())
            raise
        except TimeoutError:
            terminal = _Terminal(error=f"Run timed out after {caps.run_timeout}s")
        except Exception as error:
            logger.error(
                "Workflow driver crashed",
                wf_task_id=self.wf_task_id,
                error=str(error),
                exc_info=True,
            )
            terminal = _Terminal(error=str(error))

        if terminal.error is None:
            await self._finalize("completed", result_preview=terminal.result_preview)
            return terminal.summary
        # Scrubbed and clipped once, here, because every delivery below shares
        # this string: the ledger row, the task result TaskOutput reads back,
        # and the reason the children are told. Terminal bookkeeping reaches
        # this branch as a raw exception — a checkpointer or database failure
        # carries whatever the driver happened to be holding — and unlike the
        # child record and the lifecycle frame, neither delivery scrubs it.
        # Sanitize before clipping: clipping first can split a credential and
        # leave a fragment no pattern matches.
        error = truncate_to_bytes(
            sanitize_error_text(terminal.error), _RECORD_ERROR_BYTES
        )[0]
        await self._cancel_children(reason=error)
        await self._finalize("failed", error=error)
        raise WorkflowRunError(error)

    async def _finish_outcome(self, outcome: WorkflowOutcome) -> _Terminal:
        if outcome.status != "completed":
            await self._write_json(
                "error.json",
                {
                    "status": outcome.status,
                    "error": outcome.error,
                    "error_stack": outcome.error_stack,
                },
            )
            # The failure mode rides the message the run card shows: a CPU
            # timeout and a script error are different problems, and "failed"
            # alone cannot tell the agent which one it hit.
            return _Terminal(error=f"Workflow {outcome.status}: {outcome.error}")

        result_json = json.dumps(outcome.result, ensure_ascii=False, default=str)
        wrote_full = await self._write_json("result.json", outcome.result)
        display_result, truncated = truncate_to_bytes(
            result_json, self.spec.caps.max_summary_bytes
        )
        if truncated and not wrote_full:
            # The clipped copy is all that is left and the file holding the
            # rest never landed. Reporting success here would hand the agent a
            # result_ref to nothing and silently drop the omitted portion, so
            # the run fails with the reason instead.
            return _Terminal(
                error=(
                    f"Result is {len(result_json)} bytes and was truncated for "
                    f"display, but {self.base_rel}/result.json could not be "
                    f"written — the full result would be unrecoverable"
                )
            )
        # Named only where it is load bearing, and only because it is known to
        # exist: past the guard above, a truncated display always has its full
        # copy on disk, so this never points the agent at a missing file.
        remainder = (
            f"Result truncated for display — the full value is "
            f"{self.base_rel}/result.json\n"
            if truncated
            else ""
        )
        summary = (
            f"Workflow '{self.spec.meta.name}' completed: "
            f"{self._dispatched} subagent dispatch(es).\n"
            f"Result:\n{display_result}\n"
            f"{remainder}\n"
            f"Run files: {self.base_rel}/ "
            f"(per-child records under children/)"
        )
        await self._archive_result(summary, truncated=truncated, has_full=wrote_full)
        return _Terminal(
            summary=summary,
            result_preview=self._emitter.clip_result_preview(result_json),
        )

    async def _archive_result(
        self, text: str, *, truncated: bool, has_full: bool
    ) -> None:
        """Persist the run's TaskOutput text before anything reports success.

        Not best-effort, and ahead of the ``run_completed`` emit rather than
        merely the ledger CAS: a run that announced completion and then failed
        this write would leave the card settled on a status the ledger never
        adopted. Raising here routes the run down the failure arm instead,
        which is what keeps "completed" and "result readable" the same claim.
        """
        task_run_id = self.spec.run_task.task_run_id
        if not task_run_id:
            # No ledger row, so no ledger-authoritative read to satisfy —
            # TaskOutput answers such runs from the registry entry itself.
            return
        if self.spec.checkpointer is None:
            raise WorkflowRunError(
                "run result cannot be archived: the run is ledgered but has "
                "no checkpointer"
            )
        await persist_task_result(
            self.spec.checkpointer,
            self.spec.thread_id,
            self.wf_task_id,
            task_run_id=task_run_id,
            text=text,
            truncated=truncated,
            # Only advertise the file when it landed — a ref to a missing path
            # reads as "the rest is over there" and sends the agent nowhere.
            result_ref=f"{self.base_rel}/result.json" if has_full else None,
        )

    def phase(self, title: str) -> None:
        self._emitter.phase(title)

    def log(self, message: str) -> None:
        self._emitter.log(message)

    async def agent(self, prompt: str, opts: dict[str, Any]) -> Any:
        """Dispatch one subagent for the JavaScript ``agent()`` helper."""
        # Ahead of validation so a run at its cap says so, whatever else is
        # wrong with the call.
        if self._cap_reached():
            raise WorkflowHostError(
                "Run dispatch cap max_dispatches_per_run="
                f"{self.spec.caps.max_dispatches_per_run} has been reached"
            )
        try:
            rec = validate_dispatch(
                prompt=prompt,
                opts=opts,
                known_subagent_types=self.spec.dispatcher.subagent_types,
                default_subagent_type="general-purpose",
                caps=self.spec.caps,
            )
        except DispatchValidationError as error:
            # Misuse, not misfortune: this call fails identically on every
            # retry, so the prelude gets to treat it as a script bug. The cap
            # above and the retry-prompt cap below stay plain host errors —
            # both depend on how the run unfolded, not on the call's shape.
            raise WorkflowUsageError(str(error)) from error

        self._dispatched += 1
        seq = self._next_seq()
        phase = rec["phase"] or self._emitter.current_phase
        label = rec["label"] or prompt[:60]
        schema = rec["schema"]

        child = await self._dispatch_child(
            seq=seq,
            rec=rec,
            prompt=rec["prompt"],
            label=label,
            phase=phase,
            schema=schema,
        )
        if schema is None:
            return child.content if child.status == "ok" else None
        if child.schema_valid:
            return child.parsed
        if child.status != "invalid_schema":
            return None

        if self._cap_reached():
            return None
        retry_prompt = compose_child_prompt(
            prompt, schema, validation_error=child.error
        )
        try:
            # The retry is a dispatch like any other, so it clears the same cap
            # — the first one passing says nothing about a prompt that has since
            # grown a validation error.
            check_prompt_cap(retry_prompt, self.spec.caps)
        except DispatchValidationError as error:
            raise WorkflowHostError(str(error)) from error
        self._dispatched += 1
        retry = await self._dispatch_child(
            seq=self._next_seq(),
            rec=rec,
            prompt=retry_prompt,
            label=label,
            phase=phase,
            schema=schema,
        )
        return retry.parsed if retry.schema_valid else None

    async def _dispatch_child(
        self,
        *,
        seq: int,
        rec: dict[str, Any],
        prompt: str,
        label: str,
        phase: str | None,
        schema: dict[str, Any] | None,
    ) -> _ChildRun:
        sem = self._sem
        if sem is None:
            raise RuntimeError("workflow driver has not started")
        run = _ChildRun(
            seq=seq,
            label=label,
            phase=phase,
            subagent_type=rec["subagent_type"],
            started_at=time.time(),
        )

        async with sem:
            try:
                # Children carry the LAUNCHING turn's run stamp — the same
                # stamp a Task-tool subagent of that turn would get — so the
                # standard turn collector claims, drains, and bills them.
                run.task = await self.spec.dispatcher.dispatch(
                    subagent_type=rec["subagent_type"],
                    description=label,
                    prompt=prompt,
                    run_id=self.spec.run_task.spawned_run_id,
                    owner_task_id=self.wf_task_id,
                    base_configurable=self.spec.base_configurable,
                )
                await self._emitter.emit(
                    "child_started",
                    seq=seq,
                    label=label,
                    subagent_type=rec["subagent_type"],
                    workflow_phase=phase,
                    child_task_id=run.task.task_id,
                )
                self._inflight[seq] = {
                    "task": run.task,
                    "phase": phase,
                    "started_at": run.started_at,
                }
                await self._await_child(run)
                # Read-only accrual at child completion: the turn collector
                # later snapshots-and-clears per_call_records when it bills,
                # so this must run first (and must never clear them itself).
                run.tokens_used = self._accrue_tokens(run.task)
            except asyncio.CancelledError:
                raise
            except Exception as dispatch_error:
                run.error = str(dispatch_error)

        run.status = _child_status(run.task)
        if schema is not None and run.status == "ok" and run.content is not None:
            # Off the loop: this walks agent-supplied schema against model
            # output, and both are sized by the turn rather than by us.
            run.schema_valid, run.parsed, schema_error = await asyncio.to_thread(
                parse_schema_result, run.content, schema
            )
            if not run.schema_valid:
                run.status = "invalid_schema"
                run.error = schema_error

        run.ended_at = time.time()
        if run.content is not None:
            run.content, run.truncated = truncate_to_bytes(
                run.content, self.spec.caps.max_result_bytes
            )
        self._inflight.pop(seq, None)
        await self._emitter.emit(
            "child_done",
            seq=seq,
            status=run.status,
            duration_s=run.duration_s,
            workflow_phase=phase,
            child_task_id=run.task.task_id if run.task else None,
            error=run.error,
            tokens_used=run.tokens_used,
            tokens_spent=self._tokens_spent,
        )
        await self._write_json(f"children/{seq:03d}.json", run.to_record())
        return run

    async def _await_child(self, run: _ChildRun) -> None:
        """Await one dispatched child and settle how it ended.

        Every arm leaves the task at a terminal status and ``run`` holding
        either content or an error, never both.
        """
        task = run.task
        try:
            result = await asyncio.wait_for(
                asyncio.shield(task.asyncio_task),
                timeout=self.spec.caps.child_timeout,
            )
            task.adopt_writer_outcome(task.asyncio_task)
            ok, run.content = extract_result_content(result)
            if not ok:
                run.error, run.content = run.content, None
        except asyncio.TimeoutError:
            run.error = f"Timed out after {self.spec.caps.child_timeout}s"
            # Durable intent before signalling the writer (registry cancel-path
            # invariant): a worker dying mid-unwind must recover this child as
            # cancelled, not worker_lost. A timeout is this run's own verdict on
            # one child, not a scope teardown, so it settles the task directly
            # rather than through the registry's owner-scoped cancel.
            await self.spec.registry.stamp_cancel_intent([task])
            task.force_cancel(run.error, status="timeout")
            # `force_cancel` only *schedules* the cancellation, and the writer
            # merges its token tracker in its own settle `finally`. Accrual
            # runs the moment this returns and caches what it reads, so
            # without this wait a timed-out child reports zero tokens for the
            # life of the run. Bounded, and already cancelled: this waits on
            # an unwind, not on the work.
            if task.asyncio_task is not None:
                await asyncio.wait(
                    [task.asyncio_task], timeout=_CANCEL_SETTLE_TIMEOUT
                )
        except asyncio.CancelledError:
            # Either signal alone misses an ordering: the sweep sets _stopping
            # before children unwind, while a stop delivered straight to this
            # coroutine arrives before the sweep runs.
            if self._stopping or _stop_targets_this_run():
                # The run is unwinding and nothing downstream will run, so
                # report the child here, shielded. The cancel sweep usually got
                # there first, making this a no-op.
                await asyncio.shield(self._emit_child_cancelled(run.seq))
                raise
            # Only this child was stopped; the run is live. agent() resolves to
            # null for any child that does not succeed, so fall through and
            # report this one the same way — unwinding here would strand the
            # script's promise and kill the run.
            run.error = "Cancelled"
            task.mark_cancelled()
        except Exception as child_error:
            run.error = str(child_error)
            task.mark_error(run.error)

    def _cap_reached(self) -> bool:
        """Whether the run has spent its dispatch budget.

        The counter is the driver's, so the cap is the driver's: both the
        script's ``agent()`` and the schema retry (which never reaches
        ``validate_dispatch``) admit themselves here. Every caller must
        increment with no await in between — concurrent ``agent()`` calls are
        separate tasks, and a suspension point there would let each of them
        admit itself against the same stale count.
        """
        return self._dispatched >= self.spec.caps.max_dispatches_per_run

    def _next_seq(self) -> int:
        seq = self._child_seq
        self._child_seq += 1
        return seq

    def _accrue_tokens(self, task: BackgroundTask) -> int:
        """Fold one child's usage into the run total, at most once per child.

        A child settles inside ``_await_child`` but stays in ``_inflight``
        until its own frame is emitted, so a stop landing in between makes the
        cancel path report a child the normal path also accrues. Both callers
        get the same per-child number; only the first moves the run total.
        """
        cached = self._accrued.get(task.task_id)
        if cached is not None:
            return cached
        child_total = 0
        for record in task.per_call_records or []:
            usage = record.get("usage") or {}
            total = usage.get("total_tokens")
            if total is None:
                total = (usage.get("input_tokens") or 0) + (
                    usage.get("output_tokens") or 0
                )
            if isinstance(total, (int, float)) and not isinstance(total, bool):
                child_total += int(total)
        self._accrued[task.task_id] = child_total
        self._tokens_spent += child_total
        return child_total

    async def _emit_child_cancelled(self, seq: int) -> None:
        """Report an in-flight child as stopped, once."""
        rec = self._inflight.pop(seq, None)
        if rec is None:
            return
        task = rec["task"]
        await self._emitter.emit(
            "child_done",
            terminal=True,
            seq=seq,
            status="cancelled",
            duration_s=round(time.time() - rec["started_at"], 1),
            workflow_phase=rec["phase"],
            child_task_id=task.task_id,
            error=None,
            tokens_used=self._accrue_tokens(task),
            tokens_spent=self._tokens_spent,
        )

    async def _cancel_children(self, *, reason: str) -> int:
        self._stopping = True
        cancelled = await self.spec.registry.cancel_owner_children(
            self.wf_task_id, reason=reason
        )
        # Their own coroutines unwind after this run's terminal frame is already
        # snapshotted, so the last word on each child has to be said here.
        for seq in sorted(self._inflight):
            await self._emit_child_cancelled(seq)
        return cancelled

    async def _on_cancelled(self) -> None:
        try:
            await self._cancel_children(reason="Workflow cancelled")
            await self._finalize("cancelled", error="Workflow cancelled")
        except Exception:
            logger.warning(
                "Workflow cancel cleanup failed",
                wf_task_id=self.wf_task_id,
                exc_info=True,
            )

    async def _finalize(
        self,
        status: WorkflowRunStatus,
        *,
        error: str | None = None,
        result_preview: str | None = None,
    ) -> None:
        """Emit completion and write terminal status.

        The emit goes first: on thread-cancel the registry entry may be torn
        down while the (slow) sandbox status write is in flight, and a late
        emit against a cleared registry is a silent no-op.
        """
        await self._emitter.emit(
            "run_completed",
            terminal=True,
            status=status,
            error=error,
            result_preview=result_preview,
            children_total=self._child_seq,
            tokens_spent=self._tokens_spent,
            duration_s=(
                round(time.time() - self._started_at, 1)
                if self._started_at is not None
                else None
            ),
        )
        payload = {
            "run_id": self.wf_task_id,
            "status": status,
            "name": self.spec.meta.name,
            "dispatched_total": self._dispatched,
            "error": error,
            "updated_at": time.time(),
        }
        await self._write_json("status.json", payload)
        await self._emitter.persist_snapshot()

    async def _write_json(self, path: str, value: Any) -> bool:
        """Write one run artifact; False if it did not land.

        Most artifacts are best-effort and ignore the result — ``result.json``
        does not, because it is the only full copy of a truncated result.
        """
        full_path = f"{self.base_rel}/{path}"
        try:
            return bool(
                await self.spec.backend.awrite_text(
                    full_path,
                    json.dumps(value, ensure_ascii=False, default=str),
                )
            )
        except Exception:
            logger.debug(
                "Failed to write workflow run artifact",
                path=full_path,
                exc_info=True,
            )
            return False
