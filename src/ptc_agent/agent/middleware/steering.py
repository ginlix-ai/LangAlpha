"""
Steering Middleware.

Before each LLM call, takes in what reached the thread while the agent works:
messages steered into it (by the user or the Chief of Staff), the
reports owed to the thread by background work that finished meanwhile, the
alerts its market-watch targets raised, and the wakeups the agent set that
fired. All of it waits in the server's outbox, which hands it to the next model call
rather than to a turn of its own. Messages sent to a background task whose
run ended before reading them resume that task instead.
"""

import logging
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, TypedDict

from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    ToolMessage,
)
from langchain_core.runnables import RunnableConfig
from langgraph.runtime import Runtime
from langgraph.types import Command

from langchain.agents.middleware.types import (
    AgentMiddleware,
    AgentState,
    ModelRequest,
    ModelResponse,
    hook_config,
)
from langchain.tools.tool_node import ToolCallRequest
from ptc_agent.agent.middleware.background_subagent.context import (
    current_steer_resume,
)
from ptc_agent.agent.middleware.background_subagent.utils import config_own_run_id
from ptc_agent.agent.transcript.steer_text import STEERS_AT, sender_runs, steer_text

logger = logging.getLogger(__name__)

# The steering message's record of the outbox rows its run holds, which the
# model call that reads them confirms. Messages checkpointed before steers
# moved to the outbox carry the older name.
_HELD_JOBS = "held_jobs"
_LEGACY_HELD = "held_reports"

# The resume call's record of the task steers its run holds, keyed by tool
# call id, which the call hands to the run it resumes or gives back.
_STEER_RESUMES = "steer_resumes"

ALL_KINDS: tuple[str, ...] = ("report", "steer")

#: The one block the contents of a group's reports, taken in together, read
#: as: ``render_group(kind, contents)``.
GroupRenderer = Callable[[str, list[str]], str]


@dataclass(frozen=True)
class SteeredReport:
    """One outbox row a live turn takes in: a report owed to the thread, a
    market-watch target's alert or a wakeup's notice (reports the reader is
    shown as rows of their own), or a steer (``kind="steer"``) sent into it.

    A report of any other kind than ``report`` belongs to a group, whose
    reports one call takes in read as a single block the inbox renders
    (``ReportInbox.render_group``): a wakeup's ``content`` is its one line,
    and the notices share one ``<wakeup>`` block. ``shown`` is the reader's
    view of a report, the one key its ``steering_delivered`` entry carries
    (``report_source``, ``market_alert`` or ``wakeup``)."""

    job_id: str
    content: str                    # what the model reads
    kind: str = "report"
    shown: Mapping[str, Any] = field(default_factory=dict)
    sender: str | None = None
    platform: str | None = None
    request_key: str | None = None
    origin: dict[str, Any] | None = None
    timestamp: float | None = None
    created_at: Any = None
    user_id: str | None = None


@dataclass(frozen=True)
class TaskResume:
    """Messages for a background task whose run ended before reading them,
    which this turn resumes the task with."""

    task_id: str
    job_ids: list[str]
    entries: list[dict[str, Any]]   # [{input_id, content, sender?}]
    description: str = ""

    @property
    def prompt(self) -> str:
        """What the resumed run reads: each sender's messages in the blocks a
        running task reads them in, so who sent each one is not lost."""
        return steer_text(
            sender_runs((e.get("sender") or "user", e.get("content") or "") for e in self.entries)
        )

    @property
    def words(self) -> str:
        """The messages as sent, for replay to show."""
        return "\n".join(e.get("content") or "" for e in self.entries)


class ReportInbox(Protocol):
    async def take(
        self, thread_id: str, run_id: str, *, kinds: Sequence[str] = ALL_KINDS
    ) -> list[SteeredReport]: ...
    def render_group(self, kind: str, contents: list[str]) -> str: ...
    async def confirm(self, run_id: str, job_ids: list[str]) -> None: ...
    async def take_resumes(self, thread_id: str, run_id: str) -> list[TaskResume]: ...
    async def hand_off_resumes(
        self,
        thread_id: str,
        run_id: str,
        task_id: str,
        task_run_id: str,
        job_ids: list[str],
    ) -> list[str]: ...
    async def confirm_resumes(self, run_id: str, job_ids: list[str]) -> None: ...
    async def release_resumes(
        self, thread_id: str, run_id: str, task_id: str, job_ids: list[str]
    ) -> None: ...


class _HeldJobs(TypedDict):
    run_id: str
    job_ids: list[str]


def checkpointed_holds(messages: Sequence[Any], owner_id: str) -> set[str]:
    """The outbox rows ``owner_id`` (a run, or a task run) holds that a
    steering message in ``messages`` carries. Read from a run's checkpoint at
    its ending: a row there is in the thread's history, so it was delivered
    even if no model call got to confirm it. Every message counts, not only
    those since the last model response, so a confirm that failed earlier
    still closes once."""
    found: set[str] = set()
    for message in messages or []:
        kwargs = getattr(message, "additional_kwargs", None) or {}
        held = kwargs.get(_HELD_JOBS) or kwargs.get(_LEGACY_HELD)
        if not isinstance(held, dict):
            continue
        if owner_id in (held.get("run_id"), held.get("task_run_id")):
            found.update(str(j) for j in held.get("job_ids") or [])
    return found


def since_model_response(
    messages: Sequence[Any], *, stop_at_resume: bool = False
) -> list[Any]:
    """The messages after the last model response, oldest first.

    A ``Task(action="resume")`` call this middleware made (``_resume_call``)
    is not a model response: what was injected before it is still unread.
    ``stop_at_resume`` ends the window at one anyway."""
    window: list[Any] = []
    for message in reversed(messages or []):
        if isinstance(message, AIMessage) and (
            stop_at_resume
            or (message.additional_kwargs or {}).get("lc_source") != "steering"
        ):
            break
        window.append(message)
    window.reverse()
    return window


class SteeringMiddleware(AgentMiddleware):
    """Takes in what reached the thread while the agent works, before each LLM call.

    ``report_inbox.take`` holds each row it hands over for this run, and the
    model call that reads them closes them (``confirm``), since the graph
    checkpoints the injected message before that step starts. A row the run
    never takes, or never confirms, settles when the run ends: a steer
    becomes the next turn's input, a report its own turn. Everything reaches
    the model as one HumanMessage, reports first so the senders' words come
    last. Steers sent before a fresh turn started are not taken here: the
    server opens the turn with them ahead of its input
    (``steers_ahead_message``), so the turn's input checkpoint, where an edit
    or a regenerate starts over, holds them; its first call shows them.

    With ``"resume"`` in ``kinds``, messages for a task whose run ended
    unread resume it: a ``Task(action="resume")`` call is appended as if the
    model made it and the step jumps to the tool node, so the resume runs
    through the one Task interceptor and shows as any resume does. The
    resume hands the rows to the task run it admits, whose first model call
    confirms them as it confirms any steer it takes; a refused resume gives
    them back.

    Placement: main_only_middleware (subagents don't consume steering messages).
    """

    def __init__(
        self,
        report_inbox: ReportInbox | None = None,
        kinds: Sequence[str] = ALL_KINDS,
    ) -> None:
        super().__init__()
        self.report_inbox = report_inbox
        self.kinds = tuple(kinds)

    @hook_config(can_jump_to=["tools"])
    async def abefore_model(
        self, state: AgentState, runtime: Runtime, *, config: RunnableConfig
    ) -> dict[str, Any] | None:
        """Inject held steers and owed reports before the model call."""
        configurable = config.get("configurable") or {}
        thread_id = configurable.get("thread_id")
        if not thread_id:
            logger.debug("[Steering] No thread_id in config, skipping")
            return None
        run_id = config_own_run_id(config)

        for delivered in _opened_with(
            state, configurable.get("steer_input_message_id"), run_id
        ):
            _announce(runtime, thread_id, delivered)
        taken = await self._take(thread_id, run_id)
        resumes = await self._take_resumes(thread_id, run_id)
        if not taken and not resumes:
            return None
        update = self._inject(runtime, thread_id, run_id, taken)
        if resumes:
            update["messages"].append(_resume_call(resumes, run_id))
            update["jump_to"] = "tools"
            logger.info(
                f"[Steering] Resuming {len(resumes)} task(s) with messages their "
                f"runs never read, for thread_id={thread_id}"
            )
        return update

    def _inject(
        self,
        runtime: Runtime,
        thread_id: str,
        run_id: str | None,
        taken: list[SteeredReport],
    ) -> dict[str, Any]:
        if not taken:
            return {"messages": []}
        message, delivered = _steering_message(
            taken, run_id, time.time(), self.report_inbox.render_group
        )
        logger.info(
            f"[Steering] Injecting {sum(t.kind == 'steer' for t in taken)} "
            f"steer(s) and {sum(t.kind != 'steer' for t in taken)} report(s) "
            f"for thread_id={thread_id}"
        )
        _announce(runtime, thread_id, delivered)
        return {"messages": [message]}

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        await self._confirm(request.state)
        return await handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        """Settle a resume this middleware called once its result is in.

        The rows ride the tool call into the resumed run's opener (the
        context variable), so the task's stream shows them as delivered, and
        the resume hands them to that run (``hand_off``) before its writer
        starts. Confirming them here, before the run checkpoints its opener,
        would lose them with a worker that died in between."""
        held = _held_resume(request)
        if held is None or self.report_inbox is None:
            return await handler(request)
        inbox = self.report_inbox
        resume: dict[str, Any] = {
            "task_id": held["task_id"],
            "delivered": held["delivered"],
        }

        async def _hand_off(task_run_id: str) -> bool:
            moved = await inbox.hand_off_resumes(
                held["thread_id"],
                held["run_id"],
                held["task_id"],
                task_run_id,
                list(held["job_ids"]),
            )
            if moved:
                resume["held_jobs"] = {"task_run_id": task_run_id, "job_ids": moved}
            return bool(moved)

        resume["hand_off"] = _hand_off
        token = current_steer_resume.set(resume)
        result: Any = None
        try:
            result = await handler(request)
        finally:
            current_steer_resume.reset(token)
            if "held_jobs" not in resume:
                await self._settle_resume(held, result)
        return result

    async def _settle_resume(self, held: dict[str, Any], result: Any) -> None:
        """Give the rows back on a refused resume (one that raised too: the
        run did not start). A resume that started without handing them to a
        run of its own (no run ledger) confirms them here instead."""
        artifact = (
            (getattr(result, "additional_kwargs", None) or {}).get("task_artifact")
            if isinstance(result, ToolMessage)
            else None
        ) or {}
        resumed = (
            isinstance(result, ToolMessage)
            and result.status != "error"
            and artifact.get("action") == "resume"
        )
        try:
            if resumed:
                await self.report_inbox.confirm_resumes(
                    held["run_id"], list(held["job_ids"])
                )
            else:
                await self.report_inbox.release_resumes(
                    held["thread_id"],
                    held["run_id"],
                    held["task_id"],
                    list(held["job_ids"]),
                )
        except Exception as e:
            logger.warning(f"[Steering] Settling resume of {held['task_id']} failed: {e}")

    async def _take_resumes(
        self, thread_id: str, run_id: str | None
    ) -> list[TaskResume]:
        if "resume" not in self.kinds or self.report_inbox is None or not run_id:
            return []
        take = getattr(self.report_inbox, "take_resumes", None)
        if take is None:
            return []
        try:
            return await take(thread_id, run_id)
        except Exception as e:
            logger.warning(f"[Steering] Taking task resumes failed: {e}")
            return []

    async def _confirm(self, state: Any) -> None:
        """Close the rows injected since the last model response.

        The step that injected them is checkpointed before this one starts,
        so they can no longer be lost with the turn. Retries of the same
        call confirm again, which closes nothing twice."""
        if self.report_inbox is None:
            return
        # A resume call in the window holds no rows of its own.
        for message in since_model_response((state or {}).get("messages") or []):
            kwargs = getattr(message, "additional_kwargs", None) or {}
            held: _HeldJobs | None = kwargs.get(_HELD_JOBS) or kwargs.get(_LEGACY_HELD)
            if held and held.get("run_id") and held.get("job_ids"):
                try:
                    await self.report_inbox.confirm(
                        held["run_id"], list(held["job_ids"])
                    )
                except Exception as e:
                    logger.warning(f"[Steering] Confirm failed: {e}")

    async def _take(self, thread_id: str, run_id: str | None) -> list[SteeredReport]:
        """The rows the server hands this run, held until the model call that
        reads them confirms them. Holding needs a run to hold them for."""
        if self.report_inbox is None or not run_id:
            return []
        try:
            return await self.report_inbox.take(thread_id, run_id, kinds=self.kinds)
        except Exception as e:
            logger.warning(f"[Steering] Inbox take failed: {e}")
            return []


def _resume_call(resumes: list[TaskResume], run_id: str | None) -> AIMessage:
    """The ``Task(action="resume")`` calls that hand each task its messages,
    as an AIMessage the tool node runs. It records the rows each call holds,
    for that call's result to settle."""
    calls: list[dict[str, Any]] = []
    held: dict[str, dict[str, Any]] = {}
    for resume in resumes:
        call_id = f"call_{uuid.uuid4().hex}"
        args: dict[str, Any] = {
            "action": "resume",
            "task_id": resume.task_id,
            "prompt": resume.prompt,
        }
        if resume.description:
            args["description"] = resume.description
        calls.append({"name": "Task", "args": args, "id": call_id, "type": "tool_call"})
        held[call_id] = {
            "run_id": run_id,
            "task_id": resume.task_id,
            "job_ids": list(resume.job_ids),
            "delivered": _resume_delivered(resume),
        }
    return AIMessage(
        content="",
        id=f"steering-resume-{uuid.uuid4().hex}",
        tool_calls=calls,
        additional_kwargs={"lc_source": "steering", _STEER_RESUMES: held},
    )


def _resume_delivered(resume: TaskResume) -> dict[str, Any]:
    """The task-side ``steering_delivered`` payload, as the task's own
    steering middleware would have written it."""
    return {
        "content": resume.words,
        "count": len(resume.entries),
        "input_ids": [e["input_id"] for e in resume.entries if e.get("input_id")],
        "entries": list(resume.entries),
    }


def _held_resume(request: ToolCallRequest) -> dict[str, Any] | None:
    """The rows a resume call this middleware made holds, read off the
    AIMessage that carries the call."""
    call_id = (request.tool_call or {}).get("id")
    if not call_id or request.tool_call.get("name") != "Task":
        return None
    for message in reversed((request.state or {}).get("messages") or []):
        if not isinstance(message, AIMessage):
            continue
        kwargs = message.additional_kwargs or {}
        held = (kwargs.get(_STEER_RESUMES) or {}).get(call_id)
        if held is None:
            return None
        thread_id = (
            (request.runtime.config.get("configurable") or {}).get("thread_id")
            if request.runtime
            else None
        )
        return {**held, "thread_id": thread_id}
    return None


def _opened_with(
    state: Any, input_id: str | None, run_id: str | None
) -> list[dict[str, Any]]:
    """The ``steering_delivered`` payloads of the steers this run's turn
    opened with ahead of its input, while the model has not answered it."""
    if not input_id or not run_id:
        return []
    # A resume call after the input ends the window too: the call ahead of
    # it already showed them.
    window = since_model_response(
        (state or {}).get("messages") or [], stop_at_resume=True
    )
    ids = [getattr(m, "id", None) for m in window]
    if input_id not in ids:
        return []
    opened: list[dict[str, Any]] = []
    for message in window[: ids.index(input_id)]:
        kwargs = getattr(message, "additional_kwargs", None) or {}
        held = kwargs.get(_HELD_JOBS) or {}
        delivered = kwargs.get("steering_delivered")
        if held.get("run_id") == run_id and isinstance(delivered, dict):
            opened.append(delivered)
    return opened


def _announce(runtime: Runtime, thread_id: str, delivered: dict[str, Any]) -> None:
    try:
        runtime.stream_writer(
            {"type": "steering_delivered", "thread_id": thread_id, **delivered}
        )
    except Exception:
        logger.debug("[Steering] stream_writer unavailable, skipping SSE event")


def steers_ahead_message(steers: list[SteeredReport], run_id: str) -> HumanMessage:
    """The message a fresh turn opens with ahead of its input: the steers
    sent before it started, held by ``run_id`` until its first model call
    reads them. Steers only, so no group to render."""
    message, _ = _steering_message(steers, run_id, time.time())
    return message


def _delivered_origin(origin: dict[str, Any] | None) -> dict[str, Any] | None:
    """What a reader is shown of who sent a steer: the sender, never the Home
    thread or dispatch it came from, which a shared thread must not carry."""
    if not origin:
        return None
    shown = {k: origin[k] for k in ("type", "agent") if origin.get(k)}
    return shown or None


def _report_entry(report: SteeredReport, now: float) -> dict[str, Any]:
    """A taken report's ``steering_delivered`` entry: no text of its own, as
    the reader is shown an alert's row or what a report reports on."""
    return {"content": "", **report.shown, "timestamp": now}


def _steer_entry(steer: SteeredReport) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "content": steer.content,
        "user_id": steer.user_id,
        "timestamp": steer.timestamp,
    }
    if steer.platform:
        entry["platform"] = steer.platform
    if steer.sender and steer.sender != "user":
        entry["sender"] = steer.sender
    if steer.request_key:
        entry["request_key"] = steer.request_key
    origin = _delivered_origin(steer.origin)
    if origin:
        entry["origin"] = origin
    return entry


def _steering_message(
    taken: list[SteeredReport],
    run_id: str | None,
    now: float,
    render_group: GroupRenderer | None = None,
) -> tuple[HumanMessage, dict[str, Any]]:
    """The message the model takes everything in through, and its
    ``steering_delivered`` payload.

    The payload rides both the live SSE event and the message's
    additional_kwargs, so checkpoint-sourced replay can re-emit
    steering_delivered without the stored SSE stream. A report's text is
    model-facing: the reader is shown what it reports on, as a report turn's
    hidden message is. Each group's reports read as one block, after the
    other reports and before the steers.
    """
    reports = [t for t in taken if t.kind == "report"]
    groups: dict[str, list[SteeredReport]] = {}
    for t in taken:
        if t.kind not in ("report", "steer"):
            groups.setdefault(t.kind, []).append(t)
    steers = [t for t in taken if t.kind == "steer"]
    said = "\n\n".join(report.content for report in reports)
    if groups and render_group is None:
        raise ValueError(f"no block to read {', '.join(groups)} reports in")
    blocks = [
        render_group(kind, [g.content for g in group])
        for kind, group in groups.items()
    ]
    sent = (
        steer_text(sender_runs((s.sender or "user", s.content) for s in steers))
        if steers
        else ""
    )
    content = "\n\n".join(part for part in (said, *blocks, sent) if part)

    delivered = {
        # The user's messages only: a chat gateway retires this many of the
        # messages it queued, oldest first, and it never queued another
        # sender's.
        "count": sum((s.sender or "user") == "user" for s in steers),
        "messages": [
            _report_entry(report, now)
            for report in reports + [g for group in groups.values() for g in group]
        ]
        + [_steer_entry(steer) for steer in steers],
        "timestamp": now,
    }
    additional_kwargs: dict[str, Any] = {
        "lc_source": "steering",
        "steering_delivered": delivered,
        STEERS_AT: len(content) - len(sent),
        _HELD_JOBS: _HeldJobs(run_id=run_id, job_ids=[t.job_id for t in taken]),
    }
    # A fresh id per injection: the reducer replaces a message whose id it
    # already holds.
    message = HumanMessage(
        content=content,
        id=f"steering-{uuid.uuid4().hex}",
        additional_kwargs=additional_kwargs,
    )
    return message, delivered
