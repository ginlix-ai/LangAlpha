import logging
import re
from typing import Dict, List, Set

logger = logging.getLogger(__name__)

class QueryToolMatcher:
    """
    Analyzes the user's query text to identify target categories/servers.
    Uses regex and keyword matching for fast, O(1) matching.
    """
    def __init__(self, category_mappings: Dict[str, List[str]] = None):
        # Default category mapping
        # Maps user-facing category to lists of MCP server names
        self.category_mappings = category_mappings or {
            "price_analysis": ["price_data", "yf_price"],
            "fundamental_analysis": ["fundamentals", "yf_fundamentals", "yf_analysis"],
            "market_overview": ["macro", "yf_market"],
            "options_analysis": ["options"],
            "social_sentiment": ["x_api"],
            "web_research": ["scrapling"]
        }

        # Keyword patterns corresponding to categories
        self.patterns = {
            "price_analysis": [
                r"\bprice(s)?\b", r"\bchart(s)?\b", r"\b주가\b", r"\b차트\b", r"\b캔들\b",
                r"\bohlc(v)?\b", r"\btechnical\b", r"\b기술적\b", r"\b이동평균\b", r"\bmacd\b", r"\brsi\b"
            ],
            "fundamental_analysis": [
                r"\bfundamental(s)?\b", r"\bfinancial(s)?\b", r"\bdcf\b", r"\b재무\b", r"\b손익\b",
                r"\b대차대조표\b", r"\b현금흐름\b", r"\b밸류에이션\b", r"\bvaluation\b", r"\bratio\b",
                r"\bpe\b", r"\bpb\b", r"\beps\b", r"\bper\b", r"\bpbr\b", r"\broe\b"
            ],
            "market_overview": [
                r"\bmacro\b", r"\b시장\b", r"\b매크로\b", r"\bgdp\b", r"\binflation\b", r"\b금리\b",
                r"\b인플레이션\b", r"\b국채\b", r"\b달러\b", r"\b환율\b", r"\bfed\b", r"\b연준\b"
            ],
            "options_analysis": [
                r"\boption(s)?\b", r"\bput(s)?\b", r"\bcall(s)?\b", r"\bstrike(s)?\b", r"\b옵션\b",
                r"\b풋옵션\b", r"\b콜옵션\b", r"\b만기\b"
            ],
            "social_sentiment": [
                r"\btwitter\b", r"\bx\b", r"\btweet(s)?\b", r"\bsocial\b", r"\b소셜\b", r"\b트위터\b",
                r"\b여론\b", r"\b감성\b", r"\bsentiment\b"
            ],
            "web_research": [
                r"\bscrape\b", r"\bcrawl\b", r"\bweb\b", r"\burl\b", r"\b뉴스\b", r"\b검색\b",
                r"\b기사\b", r"\b웹\b"
            ]
        }

    def match_query(self, query: str) -> Set[str]:
        """
        Analyzes query text and returns a set of matching server names.
        If no matches are found, returns empty set (which will trigger fallback).
        """
        if not query:
            return set()

        matched_servers: Set[str] = set()
        query_lower = query.lower()

        for category, patterns in self.patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    servers = self.category_mappings.get(category, [])
                    for s in servers:
                        matched_servers.add(s)
                    break # Match for this category found, move to next

        logger.debug(f"Query '{query}' matched servers: {matched_servers}")
        return matched_servers
