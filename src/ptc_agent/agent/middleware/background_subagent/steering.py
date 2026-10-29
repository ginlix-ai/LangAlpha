"""
Subagent Steering Middleware.

Takes in the messages sent into a running background task (the user's, and
the main agent's ``Task(action="update")``) before the subagent's next LLM
call. They wait in the server's steer outbox on the task's chain, accepted
only for the run they were sent to; this run holds them and the model call
that reads them confirms them. What the run never reads is settled when it
ends, in the same transaction (``report_back.task_steer``).
"""

import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.runtime import Runtime

from langchain.agents.middleware.types import (
    AgentMiddleware,
    AgentState,
    ModelRequest,
    ModelResponse,
)

from ptc_agent.agent.middleware.background_subagent.middleware import current_background_tool_call_id
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.transcript.steer_text import sender_runs, steer_text

logger = logging.getLogger(__name__)

# The steering message's record of the rows its task run holds, which the
# model call that reads them confirms.
_HELD_JOBS = "held_jobs"


class SubagentSteeringMiddleware(AgentMiddleware):
    """Takes in the messages sent into this task's run, before each LLM call.

    Placement: first item in ``subagent_middleware`` list so the follow-up
    is visible before any other middleware runs.
    """

    def __init__(self, registry: BackgroundTaskRegistry | None = None) -> None:
        super().__init__()
        self.registry = registry

    async def abefore_model(
        self, state: AgentState, runtime: Runtime
    ) -> dict[str, Any] | None:
        """Hold the run's waiting messages and inject them before the model call."""
        try:
            tool_call_id = current_background_tool_call_id.get()
            if not tool_call_id or self.registry is None:
                return None
            task = self.registry._tasks.get(tool_call_id)
            task_id = getattr(task, "task_id", None)
            task_run_id = getattr(task, "task_run_id", None)
            take = getattr(self.registry.report_inbox, "take_task", None)
            if not task_id or not task_run_id or take is None:
                return None

            taken = await take(self.registry.thread_id or "", task_id, task_run_id)
            if not taken:
                return None

            contents = [t["content"] for t in taken]
            content = "\n".join(contents) if len(contents) > 1 else contents[0]
            # The user's instructions and the main agent's follow-ups join into
            # one text, so the delivery names each entry: a client settles its
            # own instruction by id, and labels each by who sent it.
            delivered = {
                "content": content,
                "count": len(taken),
                "input_ids": [t["input_id"] for t in taken if t.get("input_id")],
                "entries": [_delivered_entry(t) for t in taken],
            }
            # Stamp the delivered payload so checkpoint-sourced replay can
            # re-emit steering_delivered without the captured SSE stream.
            human_msg = HumanMessage(
                content=steer_text(
                    sender_runs((t.get("sender") or "user", t["content"]) for t in taken)
                ),
                additional_kwargs={
                    "lc_source": "steering",
                    "steering_delivered": delivered,
                    _HELD_JOBS: {
                        "task_run_id": task_run_id,
                        "job_ids": [t["job_id"] for t in taken],
                    },
                },
            )

            logger.info(
                f"[SubagentSteering] Injecting {len(taken)} follow-up message(s) "
                f"for task {task_id}"
            )

            # Captured for the task's stream, which replay of the subagent
            # conversation reads.
            try:
                await self.registry.append_captured_event(
                    tool_call_id,
                    {
                        "event": "steering_delivered",
                        "data": {"agent": f"task:{task_id}", **delivered},
                        "ts": time.time(),
                    },
                )
            except Exception:
                pass

            return {"messages": [human_msg]}

        except Exception as e:
            logger.error(f"[SubagentSteering] Error taking steering messages: {e}")
            return None

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        await self._confirm(request.state)
        return await handler(request)

    async def _confirm(self, state: Any) -> None:
        """Close the rows injected since the last model response: the step
        that injected them is checkpointed before this one starts. That
        includes the rows a resume handed this run, on the message it opens
        with. A retry confirms again, which closes nothing twice."""
        confirm = getattr(
            getattr(self.registry, "report_inbox", None), "confirm_task", None
        )
        if confirm is None:
            return
        for message in reversed((state or {}).get("messages") or []):
            if isinstance(message, AIMessage):
                return
            held = (getattr(message, "additional_kwargs", None) or {}).get(_HELD_JOBS)
            if held and held.get("task_run_id") and held.get("job_ids"):
                try:
                    await confirm(held["task_run_id"], list(held["job_ids"]))
                except Exception as e:
                    logger.warning(f"[SubagentSteering] Confirm failed: {e}")


def _delivered_entry(taken: dict[str, Any]) -> dict[str, Any]:
    entry = {"input_id": taken.get("input_id"), "content": taken["content"]}
    if taken.get("sender"):
        entry["sender"] = taken["sender"]
    return entry
