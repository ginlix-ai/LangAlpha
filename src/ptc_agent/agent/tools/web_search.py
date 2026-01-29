"""Configurable web search tool for research subagent.

Uses the configured search engine from agent_config.yaml (Tavily, Bocha, or Serper).
"""

from typing import Annotated, Literal

import structlog
from langchain_core.tools import InjectedToolArg, tool

from src.tools.search import get_web_search_tool

logger = structlog.get_logger(__name__)


@tool(parse_docstring=True)
async def web_search(
    query: str,
    max_results: Annotated[int, InjectedToolArg] = 5,
    time_range: Annotated[str | None, InjectedToolArg] = None,
) -> str:
    """Search the web for information using the configured search engine.

    Uses the search engine configured in agent_config.yaml (Tavily, Bocha, or Serper).

    Args:
        query: Search query to execute
        max_results: Maximum number of results to return (default: 5)
        time_range: Time range filter - 'd' (day), 'w' (week), 'm' (month), 'y' (year)

    Returns:
        Formatted search results with titles, URLs, and content snippets
    """
    try:
        # Get the configured search tool
        search_tool = get_web_search_tool(
            max_search_results=max_results,
            time_range=time_range,
            verbose=True
        )

        # Execute search asynchronously
        result = await search_tool._arun(query=query)

        # Parse results
        content, metadata = result

        # Format results for LLM
        result_texts = []
        for i, item in enumerate(content, 1):
            title = item.get("title", "Untitled")
            url = item.get("url", "")
            snippet = item.get("content", "")

            result_text = f"""## {i}. {title}
**URL:** {url}

{snippet}

---
"""
            result_texts.append(result_text)

        # Build response
        engine = metadata.get("search_engine", "unknown")
        total = metadata.get("total_results", 0)

        response = f"""Search Results (via {engine}):
Found {total} result(s) for '{query}':

{chr(10).join(result_texts)}"""

        # Add knowledge graph if available
        if metadata.get("knowledge_graph"):
            kg = metadata["knowledge_graph"]
            kg_text = f"""

📚 Knowledge Graph:
Title: {kg.get('title', 'N/A')}
Type: {kg.get('type', 'N/A')}
"""
            if kg.get('description'):
                kg_text += f"Description: {kg['description']}\n"
            response += kg_text

        return response

    except Exception as e:
        logger.exception("Web search failed", query=query)
        return f"ERROR: Web search failed - {e}"
