"""Cross-cutting ContextVars that identify the running background subagent."""

import contextvars

from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

# This ContextVar propagates tool_call_id to subagent tool calls, used by
# SubagentEventCaptureMiddleware to track which background task a tool call
# belongs to.
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
