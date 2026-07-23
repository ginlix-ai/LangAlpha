"""
Subagent Steering Middleware.

Checks Redis for follow-up steering messages sent by the orchestrator to running
subagents. Injected into subagent middleware stacks so that the main agent
can send additional instructions to a running subagent via
``Task(task_id="...", description="...")``.

Modeled on the main ``SteeringMiddleware`` but uses the per-run steering queue
(``steering_queue_key``): messages are fenced to the execution they were
accepted for, so a later resume of the same task can never consume them. The
legacy task-lifetime key is still drained for pre-ledger producers.
"""

import logging
import time
from typing import Any

from langchain_core.messages import HumanMessage
from langgraph.runtime import Runtime

from langchain.agents.middleware.types import AgentMiddleware, AgentState

from ptc_agent.agent.middleware.background_subagent.middleware import current_background_tool_call_id
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.redis_stream import (
    parse_steering_payload,
    steering_queue_key,
)

logger = logging.getLogger(__name__)


class SubagentSteeringMiddleware(AgentMiddleware):
    """Checks Redis for follow-up steering messages for a running subagent.

    When the main agent calls ``Task(task_id="...", description="...")`` on a
    running subagent, the ``BackgroundSubagentMiddleware`` pushes the message
    to Redis.  This middleware picks it up before the subagent's next LLM call
    and injects it as a ``HumanMessage``.

    Placement: first item in ``subagent_middleware`` list so the follow-up
    is visible before any other middleware runs.
    """

    def __init__(self, registry: BackgroundTaskRegistry | None = None) -> None:
        super().__init__()
        self.registry = registry

    async def abefore_model(
        self, state: AgentState, runtime: Runtime
    ) -> dict[str, Any] | None:
        """Check Redis for pending follow-up steering and inject before model call."""
        try:
            tool_call_id = current_background_tool_call_id.get()
            if not tool_call_id:
                return None

            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if not cache.enabled or not cache.client:
                return None

            # Delivery-time identity: the registry task IS this writer's
            # execution — its task_run_id names the run these messages may
            # be delivered to.
            task = (
                self.registry._tasks.get(tool_call_id) if self.registry else None
            )
            own_run_id = getattr(task, "task_run_id", None)

            # Drain the run-scoped queue plus the legacy task-lifetime key
            # (pre-ledger producers), atomically per key.
            keys = [steering_queue_key(tool_call_id)]
            if own_run_id:
                keys.insert(0, steering_queue_key(tool_call_id, own_run_id))
            pipe = cache.client.pipeline()
            for key in keys:
                pipe.lrange(key, 0, -1)
                pipe.delete(key)
            results = await pipe.execute()
            raw_messages = [
                raw for i in range(0, len(results), 2) for raw in results[i] or []
            ]
            if not raw_messages:
                return None

            # Fence check: a payload stamped for a different run was accepted
            # against an execution that no longer exists — return it rather
            # than delivering instructions to a run that never agreed to them.
            parsed: list[dict[str, Any]] = []
            returned: list[dict[str, Any]] = []
            for raw in raw_messages:
                payload = parse_steering_payload(raw)
                if payload is None:
                    logger.warning(
                        "[SubagentSteering] Failed to parse steering message"
                    )
                    continue
                expected = payload["expected_task_run_id"]
                if expected and own_run_id and expected != own_run_id:
                    returned.append(payload)
                else:
                    parsed.append(payload)

            task_id = getattr(task, "task_id", None)
            agent_id = f"task:{task_id}" if task_id else f"subagent:{tool_call_id}"
            ts = time.time()
            if returned and self.registry:
                for payload in returned:
                    await self._capture(
                        tool_call_id,
                        {
                            "event": "steering_returned",
                            "data": {
                                "agent": agent_id,
                                "content": payload["content"],
                                "input_id": payload["input_id"],
                                "reason": "run_mismatch",
                            },
                            "ts": ts,
                        },
                    )

            if not parsed:
                return None

            contents = [p["content"] for p in parsed]
            input_ids = [p["input_id"] for p in parsed if p["input_id"]]
            content = "\n".join(contents) if len(contents) > 1 else contents[0]
            # Stamp the delivered payload so checkpoint-sourced replay can
            # re-emit steering_delivered without the captured SSE stream.
            human_msg = HumanMessage(
                content=f"[Follow-up Instructions from Orchestrator]\n{content}",
                additional_kwargs={
                    "lc_source": "steering",
                    "steering_delivered": {
                        "content": content,
                        "count": len(parsed),
                        "input_ids": input_ids,
                    },
                },
            )

            logger.info(
                f"[SubagentSteering] Injecting {len(parsed)} follow-up message(s) "
                f"for tool_call_id={tool_call_id}"
            )

            # Capture for history replay so it appears when loading
            # subagent conversation from stored events
            if self.registry:
                await self._capture(
                    tool_call_id,
                    {
                        "event": "steering_delivered",
                        "data": {
                            "agent": agent_id,
                            "content": content,
                            "count": len(parsed),
                            "input_ids": input_ids,
                        },
                        "ts": ts,
                    },
                )

            return {"messages": [human_msg]}

        except Exception as e:
            logger.error(f"[SubagentSteering] Error checking steering queue: {e}")
            return None

    async def _capture(self, tool_call_id: str, event: dict[str, Any]) -> None:
        try:
            await self.registry.append_captured_event(tool_call_id, event)
        except Exception:
            pass
