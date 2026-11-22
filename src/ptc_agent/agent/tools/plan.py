"""UpdatePlan: the plan the reader follows while the agent works.

The plan is for the person reading the chat, not a memory aid for the agent.
An update rides on a call the agent was already making, and the server
resolves it against the plan in thread state, so the view never shows two
running steps or a list the model forgot to resend.
"""

from __future__ import annotations

import inspect
import logging
import unicodedata
from datetime import datetime, timezone
from typing import Annotated, Any, Dict, List, Literal, Optional, Sequence, TypedDict

from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import BaseTool, InjectedToolCallId
from langgraph.prebuilt import InjectedState
from langgraph.types import Command
from pydantic import BaseModel, Field, ValidationError, field_validator

logger = logging.getLogger(__name__)

PLAN_TOOL_NAME = "UpdatePlan"
PLAN_STATE_KEY = "plan"
PLAN_ARTIFACT_TYPE = "plan_update"
MAX_STEPS = 12

StepStatus = Literal["pending", "in_progress", "completed"]

# Model-facing wording. Every line here is prompt surface: change it only with
# the user's sign-off on the exact text.
UPDATE_PLAN_DOC = """\
Show the user your plan for this request and which step you are on.

Use it when a request takes three or more distinct steps. Send it in the same
message as the tool call that starts the work. Pass `steps` to set or change
the plan, and `advance` as each step finishes, in the same message as the call
that starts the next one. When steps finish out of order or several at
once, pass `done` and `start` instead.
"""

STEPS_DOC = (
    "The whole plan, in order, replacing any earlier one. Each step is a part "
    "of the user's request; your own upkeep, such as notes or setup, stays out "
    "of it. Mark the step you are starting `in_progress`, later ones `pending` "
    "and any already done `completed`."
)
TITLE_DOC = "A short imperative phrase in the user's terms, about seven words at most."
ADVANCE_DOC = (
    "True to complete the step in progress and start the next one. Use it "
    "instead of resending `steps` when the plan itself has not changed. When "
    "the last step is done, send it before your final answer, so the plan "
    "shows finished."
)
DONE_DOC = (
    "Numbers (from 1) of the steps you finished. Use it with `start` when "
    "steps finish out of order or several at once, instead of resending `steps`."
)
START_DOC = (
    "Number (from 1) of the step you are starting now. Left out, the step in "
    "progress keeps running, and once `done` finishes it, the first pending "
    "step starts."
)

RESULTS = {
    "running": "Step {n} of {total} is in progress: {title}",
    "shown": "Plan shown ({total} steps). ",
    "finished": "Plan finished.",
    "neither": "Send `steps` to set the plan, or `advance` to move to the next step.",
    "no_plan": "There is no plan in progress to advance. Send `steps` to set one.",
    "bad_step": "The plan has no step {n}; its steps are numbered 1 to {total}.",
    "too_many": f"A plan holds at most {MAX_STEPS} steps. Merge steps and send it again.",
    "superseded": (
        "Only the last `UpdatePlan` call in this message was applied; send one `UpdatePlan` call per message."
    ),
}


class PlanStepDict(TypedDict):
    """A step as the plan in state holds it."""

    title: str
    status: StepStatus


class PlanStep(BaseModel):
    title: str = Field(min_length=1, max_length=120, description=TITLE_DOC)
    status: StepStatus = "pending"

    @field_validator("title", mode="before")
    @classmethod
    def _one_line(cls, value: Any) -> Any:
        """One line, since a title is quoted in the harness's rows and the
        summary's plan, where a newline or a control character would break
        the line it sits in."""
        if not isinstance(value, str):
            return value
        return " ".join("".join(" " if unicodedata.category(c) == "Cc" else c for c in value).split())

    @field_validator("status", mode="before")
    @classmethod
    def _lower(cls, value: Any) -> Any:
        return value.strip().lower() if isinstance(value, str) else value


class _Args(BaseModel):
    steps: Optional[List[PlanStep]] = Field(default=None, description=STEPS_DOC)
    advance: bool = Field(default=False, description=ADVANCE_DOC)
    done: Optional[List[int]] = Field(default=None, description=DONE_DOC)
    start: Optional[int] = Field(default=None, description=START_DOC)


class _ToolInput(_Args):
    """The model's arguments plus what the tool node injects, which
    ``tool_call_schema`` leaves out of what the model sees."""

    state: Annotated[dict, InjectedState]
    tool_call_id: Annotated[str, InjectedToolCallId]


def settled(out: List[PlanStepDict]) -> List[PlanStepDict]:
    """``out``, in place, with exactly one running step while any is
    unfinished: a later duplicate goes back to pending, and with none running
    the first pending one starts. The reader is shown where the agent is, so
    the plan can never say two places or none."""
    running = [i for i, s in enumerate(out) if s["status"] == "in_progress"]
    for i in running[1:]:
        out[i]["status"] = "pending"
    if not running:
        nxt = next((i for i, s in enumerate(out) if s["status"] == "pending"), None)
        if nxt is not None:
            out[nxt]["status"] = "in_progress"
    return out


def current_index(steps: Sequence[PlanStepDict]) -> Optional[int]:
    return next((i for i, s in enumerate(steps) if s["status"] == "in_progress"), None)


def moved(
    steps: Sequence[PlanStepDict], done: Sequence[int], start: Optional[int]
) -> List[PlanStepDict]:
    """A copy of ``steps`` with the numbered ones completed and ``start``
    running. A step started puts any other running one back to pending; with
    none named, the running step keeps running, or the first pending one
    starts."""
    out = [PlanStepDict(title=s["title"], status=s["status"]) for s in steps]
    for n in done:
        out[n - 1]["status"] = "completed"
    if start is not None:
        for step in out:
            if step["status"] == "in_progress":
                step["status"] = "pending"
        out[start - 1]["status"] = "in_progress"
    return settled(out)


def plan_payload(steps: Sequence[PlanStepDict]) -> Dict[str, Any]:
    """What the chat draws, live and on replay: the whole resolved list."""
    return {
        "steps": [dict(s) for s in steps],
        "total": len(steps),
        "completed": sum(1 for s in steps if s["status"] == "completed"),
        "current": current_index(steps),
    }


def _receipt(steps: Sequence[PlanStepDict], *, new: bool) -> str:
    cur = current_index(steps)
    lead = RESULTS["shown"].format(total=len(steps)) if new else ""
    if cur is None:
        return lead + RESULTS["finished"]
    return lead + RESULTS["running"].format(
        n=cur + 1, total=len(steps), title=steps[cur]["title"]
    )


def _superseded(state: dict, tool_call_id: str) -> bool:
    """Whether a later ``UpdatePlan`` call in the same message replaces this
    one. Each accepted call writes the whole plan, and every call in the
    message resolves against the plan from before it, so applying more than
    one would apply an ``advance`` twice from the same start, or let an
    earlier call's list overwrite a later one's. Only a later call that would
    apply replaces it: one refused leaves nothing in its place."""
    messages = state.get("messages") or ()
    message = next((m for m in reversed(messages) if isinstance(m, AIMessage)), None)
    if message is None:
        return False
    calls = [call for call in message.tool_calls if call.get("name") == PLAN_TOOL_NAME]
    at = next((i for i, call in enumerate(calls) if call.get("id") == tool_call_id), None)
    if at is None:
        return False
    previous = state.get(PLAN_STATE_KEY) or []
    return any(_applies(call, previous) for call in calls[at + 1 :])


def _applies(call: Dict[str, Any], previous: Sequence[PlanStepDict]) -> bool:
    """Whether ``call`` would change the plan: its arguments validate, as the
    tool node coerces them, and ``resolve`` accepts them."""
    # Imported here: the middleware package reaches back into the tools.
    from ptc_agent.agent.middleware.tool.argument_parsing import parse_tool_args

    try:
        args = _Args.model_validate(parse_tool_args(call.get("args") or {}, PLAN_TOOL_NAME))
    except ValidationError:
        return False
    plan, _ = resolve(previous, args.steps, args.advance, args.done, args.start)
    return plan is not None


def _emit(tool_call_id: str, payload: Dict[str, Any]) -> None:
    try:
        from langgraph.config import get_stream_writer

        writer = get_stream_writer()
    except Exception:  # noqa: BLE001 - no stream outside a run; replay still has the result
        return
    try:
        writer(
            {
                "artifact_type": PLAN_ARTIFACT_TYPE,
                "artifact_id": tool_call_id,
                "agent": "ptc",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "completed",
                "payload": payload,
            }
        )
    except Exception:  # noqa: BLE001 - the plan is in state either way
        logger.warning("[Plan] could not emit the plan event", exc_info=True)


def resolve(
    previous: Sequence[PlanStepDict],
    steps: Optional[Sequence[PlanStep]],
    advance: bool,
    done: Optional[Sequence[int]] = None,
    start: Optional[int] = None,
) -> tuple[Optional[List[PlanStepDict]], str]:
    """The plan after this call and the text the model gets back. A refused
    call returns no plan, and the plan in state stays as it was.

    ``steps`` states every status, so it wins when ``advance`` comes with it:
    refusing the pair left a plan the model believed it had moved standing
    still for the rest of the turn. ``done`` and ``start`` win over
    ``advance`` for the same reason: they say more. ``advance`` alone is
    ``done`` naming the running step."""
    if steps is not None:
        if not steps:
            return None, RESULTS["neither"]
        if len(steps) > MAX_STEPS:
            return None, RESULTS["too_many"]
        plan = settled([PlanStepDict(title=s.title, status=s.status) for s in steps])
        return plan, _receipt(plan, new=True)
    if advance and not done and start is None:
        cur = current_index(previous)
        if cur is None:
            return None, RESULTS["no_plan"]
        done = [cur + 1]
    if not done and start is None:
        return None, RESULTS["neither"]
    if not previous:
        return None, RESULTS["no_plan"]
    named = [*(done or ()), *(() if start is None else (start,))]
    bad = next((n for n in named if not 1 <= n <= len(previous)), None)
    if bad is not None:
        return None, RESULTS["bad_step"].format(n=bad, total=len(previous))
    plan = moved(previous, done or (), start)
    return plan, _receipt(plan, new=False)


class UpdatePlanTool(BaseTool):
    name: str = PLAN_TOOL_NAME
    description: str = inspect.cleandoc(UPDATE_PLAN_DOC)
    args_schema: type[BaseModel] = _ToolInput

    def _run(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("UpdatePlan is async only")

    async def _arun(
        self,
        steps: Optional[List[PlanStep]] = None,
        advance: bool = False,
        done: Optional[List[int]] = None,
        start: Optional[int] = None,
        *,
        state: dict,
        tool_call_id: str,
    ) -> Any:
        if _superseded(state, tool_call_id):
            return RESULTS["superseded"]
        previous = state.get(PLAN_STATE_KEY) or []
        plan, text = resolve(previous, steps, advance, done, start)
        if plan is None:
            return text
        payload = plan_payload(plan)
        _emit(tool_call_id, payload)
        return Command(
            update={
                PLAN_STATE_KEY: plan,
                "messages": [
                    ToolMessage(
                        content=text,
                        tool_call_id=tool_call_id,
                        name=PLAN_TOOL_NAME,
                        artifact=payload,
                    )
                ],
            }
        )


def build_update_plan_tool() -> UpdatePlanTool:
    return UpdatePlanTool()
