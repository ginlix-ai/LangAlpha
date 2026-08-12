"""
Steering Middleware.

Checks Redis for steering messages from the user before each LLM call and injects
them into the conversation state. This enables users to send messages while the
agent is processing, which get picked up before the next model invocation.
"""

import json
import logging
import time
from typing import Any

from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.runtime import Runtime

from langchain.agents.middleware.types import AgentMiddleware, AgentState

logger = logging.getLogger(__name__)


class SteeringMiddleware(AgentMiddleware):
    """Checks Redis for steering messages from the user before each LLM call.

    When a user sends a message while the agent is already processing,
    the server queues it in Redis. This middleware picks up those steering
    messages before the next LLM invocation and injects them as a
    HumanMessage into the conversation state.

    Placement: main_only_middleware (subagents don't consume steering messages).
    """

    async def abefore_model(
        self, state: AgentState, runtime: Runtime, *, config: RunnableConfig
    ) -> dict[str, Any] | None:
        """Check Redis for steering messages and inject them before model call."""
        try:
            thread_id = config.get("configurable", {}).get("thread_id")
            if not thread_id:
                logger.debug("[Steering] No thread_id in config, skipping")
                return None

            # Import here to avoid circular imports
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
            if not cache.enabled or not cache.client:
                return None

            key = f"workflow:steering:{thread_id}"

            # Peek, partition, then LREM exactly what we consume (v4 2.4c
            # review F3). Foreign-stamped payloads (a run that died without
            # draining) are never touched — this run's end-of-run drain
            # returns them to the user. A pop-and-requeue cycle would open
            # a crash window per model call in which the only copy of an
            # accepted user message vanishes.
            raw_messages = await cache.client.lrange(key, 0, -1)
            if not raw_messages:
                return None

            own_run_id = (config.get("metadata") or {}).get("run_id")

            # Parse steering messages (handle both str and dict payloads)
            parsed: list[dict] = []
            consumed: list[Any] = []
            for raw in raw_messages:
                try:
                    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                    data = json.loads(text)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning(f"[Steering] Failed to parse steering message: {e}")
                    consumed.append(raw)  # drop garbage from the queue
                    continue
                if (
                    isinstance(data, dict)
                    and own_run_id
                    and data.get("run_id") not in (None, own_run_id)
                ):
                    continue
                consumed.append(raw)
                if isinstance(data, str):
                    parsed.append({"content": data})
                elif isinstance(data, dict):
                    parsed.append(data)
                else:
                    parsed.append({"content": str(data)})

            if consumed:
                # LREM by exact value: identical duplicates are covered
                # because one LREM count=1 is issued per read instance.
                pipe = cache.client.pipeline()
                for raw in consumed:
                    pipe.lrem(key, 1, raw)
                await pipe.execute()

            if not parsed:
                return None

            # Build combined message content
            if len(parsed) == 1:
                content = parsed[0].get("content", str(parsed[0]))
            else:
                lines = [
                    f"{i + 1}. {msg.get('content', str(msg))}"
                    for i, msg in enumerate(parsed)
                ]
                content = "\n".join(lines)

            # Delivered payload rides both the live SSE event and the injected
            # message's additional_kwargs, so checkpoint-sourced replay can
            # re-emit steering_delivered without the stored SSE stream.
            delivered = {
                "count": len(parsed),
                "messages": [
                    {
                        "content": q.get("content", ""),
                        "user_id": q.get("user_id"),
                        "timestamp": q.get("timestamp"),
                    }
                    for q in parsed
                ],
                "timestamp": time.time(),
            }
            human_msg = HumanMessage(
                content=f"[Steering from User]\n{content}",
                additional_kwargs={
                    "lc_source": "steering",
                    "steering_delivered": delivered,
                },
            )

            logger.info(
                f"[Steering] Injecting {len(parsed)} steering message(s) "
                f"for thread_id={thread_id}"
            )

            # Emit SSE custom event so frontend knows the message was delivered
            try:
                runtime.stream_writer(
                    {
                        "type": "steering_delivered",
                        "thread_id": thread_id,
                        **delivered,
                    }
                )
            except Exception:
                # Stream writer may not be available in all contexts
                logger.debug("[Steering] stream_writer unavailable, skipping SSE event")

            return {"messages": [human_msg]}

        except Exception as e:
            logger.error(f"[Steering] Error checking steering queue: {e}")
            return None
