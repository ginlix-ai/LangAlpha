"""Flash Agent graph builder."""

import logging
from typing import Any

from ptc_agent.agent.flash.agent import FlashAgent
from ptc_agent.config import AgentConfig

logger = logging.getLogger(__name__)


def build_flash_graph(
    config: AgentConfig,
    checkpointer: Any | None = None,
    user_profile: dict | None = None,
    store: Any | None = None,
    response_format: Any | None = None,
    direct_mcp: Any | None = None,
    order_ledger: Any | None = None,
) -> Any:
    """Build flash agent graph without sandbox.

    Unlike build_ptc_graph_with_session, this does not require
    workspace, session, or MCP registry - it's stateless and fast.

    Args:
        config: AgentConfig with LLM and flash settings
        checkpointer: Optional LangGraph checkpointer for state persistence
        user_profile: Optional user profile dict with name, timezone, locale
        response_format: Optional structured output schema (Pydantic model or dict).
            When set, the agent is forced to return structured data matching this schema.

    Returns:
        Compiled LangGraph agent
    """
    logger.info("Building Flash agent graph (no sandbox)")

    flash_agent = FlashAgent(config)
    return flash_agent.create_agent(
        checkpointer=checkpointer,
        user_profile=user_profile,
        store=store,
        response_format=response_format,
        direct_mcp=direct_mcp,
        order_ledger=order_ledger,
    )
