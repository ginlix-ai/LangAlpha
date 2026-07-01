import logging
import json
from typing import Dict, List, Set, Any
from langchain_core.messages import SystemMessage, HumanMessage

logger = logging.getLogger(__name__)

class QueryToolMatcher:
    """
    Analyzes the user's query text to identify target categories/servers.
    Uses LLM classification (v2) for accurate and semantic classification.
    """
    def __init__(self, category_mappings: Dict[str, List[str]] = None):
        self.category_mappings = category_mappings or {
            "price_analysis": ["price_data", "yf_price"],
            "fundamental_analysis": ["fundamentals", "yf_fundamentals", "yf_analysis"],
            "market_overview": ["macro", "yf_market"],
            "options_analysis": ["options"],
            "social_sentiment": ["x_api"],
            "web_research": ["scrapling"]
        }

    async def match_query(self, query: str, mcp_registry: Any = None, llm_client: Any = None) -> Set[str]:
        """
        Analyzes query text using LLM and returns a set of matching server names.
        If no matches are found, returns empty set (which will trigger fallback).
        """
        if not query:
            return set()

        # If no LLM client is provided, or registry is missing, return empty set (safe fallback)
        if llm_client is None or mcp_registry is None:
            logger.warning("ACE: Classification LLM or MCP Registry is missing, using fallback.")
            return set()

        # 1. Build list of active servers and descriptions
        servers_desc = []
        active_servers = set(mcp_registry.get_all_tools().keys())
        
        if hasattr(mcp_registry, "config") and hasattr(mcp_registry.config, "mcp"):
            for s in mcp_registry.config.mcp.servers:
                if s.enabled and s.name in active_servers:
                    desc = getattr(s, "description", "") or getattr(s, "instruction", "")
                    servers_desc.append(f"- {s.name}: {desc}")

        if not servers_desc:
            return set()

        # 2. Build system prompt (strictly under 800 tokens)
        system_prompt = (
            "You are a tool router. Your task is to identify which tool servers are relevant to answering the user's query.\n"
            "Respond ONLY with a JSON list of matched server names (e.g. [\"price_data\", \"fundamentals\"]).\n"
            "Do not output any markdown formatting, backticks, or extra explanation.\n\n"
            "Available tool servers:\n" + "\n".join(servers_desc) + "\n\n"
            "Examples:\n"
            "Query: AAPL의 10년치 재무제표 가져와줘\n"
            "Response: [\"fundamentals\", \"yf_fundamentals\"]\n\n"
            "Query: 테슬라 주가 차트랑 최근 옵션 정보 조회해줘\n"
            "Response: [\"price_data\", \"yf_price\", \"options\"]\n\n"
            "Query: 어제 뉴스 기사랑 트위터 여론 요약해줘\n"
            "Response: [\"x_api\", \"scrapling\"]\n"
        )

        try:
            logger.info(f"ACE: Classifying query: '{query}'")
            # 3. Invoke LLM (async preferred)
            if hasattr(llm_client, "ainvoke"):
                res = await llm_client.ainvoke([
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=f"Query: {query}")
                ])
                response_text = getattr(res, "content", str(res))
            else:
                res = llm_client.invoke([
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=f"Query: {query}")
                ])
                response_text = getattr(res, "content", str(res))

            # 4. Clean up response formatting
            text = response_text.strip()
            if text.startswith("```"):
                lines = text.splitlines()
                if len(lines) >= 2:
                    if lines[0].startswith("```"):
                        lines = lines[1:]
                    if lines[-1].startswith("```"):
                        lines = lines[:-1]
                    text = "\n".join(lines).strip()

            # 5. Parse and validate JSON list of servers
            server_names = json.loads(text)
            if isinstance(server_names, list):
                matched = {s for s in server_names if isinstance(s, str) and s in active_servers}
                logger.info(f"ACE: Classified query to servers: {matched}")
                return matched

        except Exception as e:
            logger.error(f"ACE: Failed to classify query using LLM: {e}", exc_info=True)

        return set()
