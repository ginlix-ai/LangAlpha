"""Research sub-agent definition for deepagent.

This sub-agent specializes in web research using the configured search engine
(Tavily, Bocha, or Serper based on agent_config.yaml) and strategic thinking
for comprehensive information gathering.
"""

from typing import Any

from ptc_agent.agent.prompts import get_loader
from ptc_agent.agent.tools import think_tool
from src.tools.search import get_web_search_tool


def get_research_subagent_config(
    max_researcher_iterations: int = 3,
    mcp_tools: list[Any] | None = None,
) -> dict[str, Any]:
    """Get configuration for the research sub-agent.

    Args:
        max_researcher_iterations: Maximum search iterations
        mcp_tools: Additional MCP tools to include (per-subagent config)

    Returns:
        Sub-agent configuration dictionary for deepagent
    """
    # Render researcher instructions using template loader (date auto-injected from session)
    loader = get_loader()
    instructions = loader.get_subagent_prompt("researcher")

    # Get configured search tool (Tavily, Bocha, or Serper from agent_config.yaml)
    web_search_tool = get_web_search_tool(
        max_search_results=10,
        time_range=None,
        verbose=False,
    )

    # Base tools for research
    tools = [web_search_tool, think_tool]

    # Add any MCP tools configured for this sub-agent
    if mcp_tools:
        tools.extend(mcp_tools)

    return {
        "name": "research",
        "description": (
            "Delegate research to the sub-agent researcher. "
            "Give this researcher one specific topic or question at a time. "
            "The researcher will search the web and provide findings with citations."
        ),
        "system_prompt": instructions,
        "tools": tools,
    }


def create_research_subagent(
    max_researcher_iterations: int = 3,
    mcp_tools: list[Any] | None = None,
) -> dict[str, Any]:
    """Create a research sub-agent for deepagent.

    This is a convenience wrapper around get_research_subagent_config.

    Args:
        max_researcher_iterations: Maximum search iterations
        mcp_tools: Additional MCP tools for this sub-agent

    Returns:
        Sub-agent configuration dictionary
    """
    return get_research_subagent_config(
        max_researcher_iterations=max_researcher_iterations,
        mcp_tools=mcp_tools,
    )
