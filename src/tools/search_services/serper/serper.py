"""Serper API client for Google Search results.

Serper.dev provides a simple API to access Google Search results.
Official docs: https://serper.dev/docs
"""

import os
from typing import Any, Dict, List, Optional, Tuple

import httpx


class SerperAPI:
    """Serper.dev API client for Google Search."""

    def __init__(self, api_key: Optional[str] = None):
        """Initialize Serper API client.

        Args:
            api_key: Serper API key. If not provided, reads from SERPER_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("SERPER_API_KEY")
        if not self.api_key:
            raise ValueError("SERPER_API_KEY not found in environment variables")

        self.base_url = "https://google.serper.dev"
        self.search_endpoint = f"{self.base_url}/search"

    async def _make_request(
        self,
        query: str,
        num: int = 10,
        time_range: Optional[str] = None,
        gl: str = "us",  # Geographic location
        hl: str = "en",  # Language
    ) -> dict:
        """Make HTTP request to Serper API.

        Args:
            query: Search query
            num: Number of results (max 100)
            time_range: Time range filter (d, w, m, y)
            gl: Geographic location code (e.g., 'us', 'cn')
            hl: Language code (e.g., 'en', 'zh-cn')

        Returns:
            Raw API response as dict

        Raises:
            httpx.HTTPError: If API request fails
        """
        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
        }

        payload = {
            "q": query,
            "num": min(num, 100),  # Serper max is 100
            "gl": gl,
            "hl": hl,
        }

        # Add time range if specified
        if time_range:
            # Serper uses 'tbs' parameter for time filtering
            time_map = {
                "d": "qdr:d",  # Past day
                "w": "qdr:w",  # Past week
                "m": "qdr:m",  # Past month
                "y": "qdr:y",  # Past year
            }
            if time_range.lower() in time_map:
                payload["tbs"] = time_map[time_range.lower()]

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                self.search_endpoint,
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            return response.json()

    async def web_search(
        self,
        query: str,
        num: int = 10,
        time_range: Optional[str] = None,
        gl: str = "us",
        hl: str = "en",
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Execute web search and return formatted results.

        Args:
            query: Search query
            num: Number of results
            time_range: Time range filter (d, w, m, y)
            gl: Geographic location code
            hl: Language code

        Returns:
            Tuple of (detailed_results, metadata):
                - detailed_results: List of search result dicts for LLM
                - metadata: Search metadata for UI/frontend
        """
        raw_response = await self._make_request(
            query=query,
            num=num,
            time_range=time_range,
            gl=gl,
            hl=hl,
        )

        # Extract organic results
        organic_results = raw_response.get("organic", [])

        # Format detailed results for LLM
        detailed_results = []
        for result in organic_results:
            detailed_results.append({
                "type": "page",
                "title": result.get("title", ""),
                "url": result.get("link", ""),
                "content": result.get("snippet", ""),
                "position": result.get("position", 0),
            })

        # Build metadata for UI
        metadata = {
            "query": query,
            "search_engine": "serper",
            "total_results": len(detailed_results),
            "answer_box": raw_response.get("answerBox"),
            "knowledge_graph": raw_response.get("knowledgeGraph"),
            "results": [
                {
                    "title": r.get("title", ""),
                    "url": r.get("link", ""),
                    "snippet": r.get("snippet", ""),
                }
                for r in organic_results
            ],
        }

        return detailed_results, metadata
