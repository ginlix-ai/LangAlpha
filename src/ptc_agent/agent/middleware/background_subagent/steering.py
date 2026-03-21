"""
Subagent Steering Middleware.

Checks Redis for follow-up steering messages sent by the orchestrator to running
subagents. Injected into subagent middleware stacks so that the main agent
can send additional instructions to a running subagent via
``Task(task_id="...", description="...")``.

Modeled on the main ``SteeringMiddleware`` but uses a per-task Redis key
(``subagent:steering:{tool_call_id}``) instead of the per-thread key.
"""

import json
import logging
import time
from typing import Any

from langchain_core.messages import HumanMessage
from langgraph.runtime import Runtime

from langchain.agents.middleware.types import AgentMiddleware, AgentState

from ptc_agent.agent.middleware.background_subagent.middleware import current_background_tool_call_id
from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTaskRegistry

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

            key = f"subagent:steering:{tool_call_id}"

            # Atomically read all steering messages and delete the key
            pipe = cache.client.pipeline()
            pipe.lrange(key, 0, -1)
            pipe.delete(key)
            results = await pipe.execute()

            raw_messages = results[0]
            if not raw_messages:
                return None

            # Parse steering messages
            parsed: list[str] = []
            for raw in raw_messages:
                try:
                    data = json.loads(
                        raw.decode("utf-8") if isinstance(raw, bytes) else raw
                    )
                    parsed.append(
                        data
                        if isinstance(data, str)
                        else data.get("content", str(data))
                    )
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning(
                        f"[SubagentSteering] Failed to parse steering message: {e}"
                    )

            if not parsed:
                return None

            content = "\n".join(parsed) if len(parsed) > 1 else parsed[0]
            human_msg = HumanMessage(
                content=f"[Follow-up Instructions from Orchestrator]\n{content}"
            )

            logger.info(
                f"[SubagentSteering] Injecting {len(parsed)} follow-up message(s) "
                f"for tool_call_id={tool_call_id}"
            )

            # Emit SSE custom event so frontend can render the follow-up
            # in the subagent view as a user message
            ts = time.time()

            # Capture for history replay so it appears when loading
            # subagent conversation from stored events
            if self.registry:
                try:
                    task = self.registry._tasks.get(tool_call_id)
                    agent_id = f"task:{task.task_id}" if task else f"subagent:{tool_call_id}"
                    await self.registry.append_captured_event(
                        tool_call_id,
                        {
                            "event": "steering_delivered",
                            "data": {
                                "agent": agent_id,
                                "content": content,
                                "count": len(parsed),
                            },
                            "ts": ts,
                        },
                    )
                except Exception:
                    pass

            return {"messages": [human_msg]}

        except Exception as e:
            logger.error(f"[SubagentSteering] Error checking steering queue: {e}")
            return None
