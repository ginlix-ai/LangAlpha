"""Cross-cutting ContextVars that identify the running background subagent."""

import contextvars

from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

# This ContextVar propagates tool_call_id to subagent tool calls, used by
# the capture middleware in event_capture.py to track which background task a
# tool call belongs to.
current_background_tool_call_id: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar("current_background_tool_call_id", default=None)
)

# This ContextVar propagates the unified agent identity (e.g., "research:uuid4")
# to subagent tool calls, for internal tool tracking.
current_background_agent_id: contextvars.ContextVar[str | None] = (
    contextvars.ContextVar("current_background_agent_id", default=None)
)

# This ContextVar propagates a dedicated PerCallTokenTracker to the subagent
# so its LLM calls are tracked separately from the parent agent's tracker.
current_background_token_tracker: contextvars.ContextVar[PerCallTokenTracker | None] = (
    contextvars.ContextVar("current_background_token_tracker", default=None)
)

# Set by SteeringMiddleware around a Task resume it calls for messages a
# task's ended run never read: ``{"task_id", "delivered", "hand_off"}``. The
# resumed run opens with them as delivered steers rather than as a fresh
# instruction, so the sender's pending messages settle by input id. The
# resume hands the rows to the run it admitted (``hand_off``), which records
# ``held_jobs`` here for that run's opener to carry, as a steer it took
# itself carries them, so its first model call confirms them.
current_steer_resume: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "current_steer_resume", default=None
)


def _resume_of(task_id: str) -> dict | None:
    held = current_steer_resume.get()
    return held if held and held.get("task_id") == task_id else None


def steer_resume_for(task_id: str) -> dict | None:
    """The delivered steers a resume of ``task_id`` opens with, if this
    resume is one SteeringMiddleware called for them."""
    held = _resume_of(task_id)
    return held.get("delivered") if held else None


def steer_resume_held(task_id: str) -> dict | None:
    """``{"task_run_id", "job_ids"}``: the rows a resume of ``task_id`` handed
    to the run it admitted, for that run's opener to confirm."""
    held = _resume_of(task_id)
    return held.get("held_jobs") if held else None


async def hand_off_steer_resume(task_id: str, task_run_id: str) -> bool:
    """Hand the steers a resume of ``task_id`` opens with to ``task_run_id``,
    the run it admitted. False when they could not be handed over, which
    refuses the resume; True when they were, or there are none."""
    held = _resume_of(task_id)
    if held is None or held.get("hand_off") is None:
        return True
    return bool(await held["hand_off"](task_run_id))
