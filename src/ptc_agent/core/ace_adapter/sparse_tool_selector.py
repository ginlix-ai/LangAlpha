import logging
from collections import deque
from typing import List, Set, Dict, Any
from src.ptc_agent.core.mcp_registry import MCPRegistry, MCPToolInfo
from src.ptc_agent.core.ace_adapter.async_cache_manager import AsyncCacheManager
from src.ptc_agent.core.ace_adapter.mcp_schema_indexer import MCPSchemaIndexer
from src.ptc_agent.core.ace_adapter.query_tool_matcher import QueryToolMatcher

logger = logging.getLogger(__name__)

class SparseToolSelector:
    """
    Orchestrates indexing and matching to select a sparse set of tools for a query.
    """
    def __init__(
        self,
        cache_manager: AsyncCacheManager = None,
        indexer: MCPSchemaIndexer = None,
        matcher: QueryToolMatcher = None,
        max_depth: int = 1,
        fallback_to_all: bool = True
    ):
        self.cache = cache_manager or AsyncCacheManager()
        self.indexer = indexer or MCPSchemaIndexer()
        self.matcher = matcher or QueryToolMatcher()
        self.max_depth = max_depth
        self.fallback_to_all = fallback_to_all

    async def get_or_build_graph(self, all_tools: List[MCPToolInfo]) -> Dict[str, List[str]]:
        """
        Retrieves the dependency graph from cache, or builds and caches it if missing.
        """
        graph = await self.cache.get_graph()
        if not graph and all_tools:
            logger.info("ACE: Dependency graph not found in cache. Building...")
            graph = self.indexer.build_graph(all_tools)
            await self.cache.save_graph(graph)
            
            # Save individual nodes too
            for node, deps in graph.items():
                await self.cache.update_node(node, deps)
        return graph

    async def select_relevant_tools(self, query: str, mcp_registry: MCPRegistry, llm_client: Any = None) -> List[MCPToolInfo]:
        """
        Given a user query, selects the sparse subset of tools.
        """
        # Flatten all tools from registry
        all_tools_dict = mcp_registry.get_all_tools()
        all_tools: List[MCPToolInfo] = []
        for tools_list in all_tools_dict.values():
            all_tools.extend(tools_list)

        if not all_tools:
            return []

        # 1. Match query to servers using LLM matcher
        matched_servers = await self.matcher.match_query(query, mcp_registry=mcp_registry, llm_client=llm_client)
        if not matched_servers:
            if self.fallback_to_all:
                logger.info("ACE: No servers matched query. Falling back to all tools.")
                return all_tools
            else:
                return []

        # 2. Get/build dependency graph
        graph = await self.get_or_build_graph(all_tools)
        if not graph:
            # Fallback to matched servers only if graph is empty/unavailable
            return [t for t in all_tools if t.server_name in matched_servers]

        # 3. Find starting nodes (entrypoint tools)
        entrypoints: Set[str] = set()
        for t in all_tools:
            if t.server_name in matched_servers:
                entrypoints.add(self.indexer._get_tool_id(t))

        # 4. Trace dependencies via BFS
        visited: Set[str] = set()
        queue = deque([(ep, 0) for ep in entrypoints if ep in graph])

        while queue:
            node, depth = queue.popleft()
            if node in visited:
                continue
            visited.add(node)

            if depth < self.max_depth:
                deps = graph.get(node, [])
                for dep in deps:
                    if dep not in visited:
                        queue.append((dep, depth + 1))

        # 5. Map back visited node IDs to MCPToolInfo
        tool_id_to_info = {self.indexer._get_tool_id(t): t for t in all_tools}
        selected_tools = [tool_id_to_info[tid] for tid in visited if tid in tool_id_to_info]

        logger.info(f"ACE: Selected {len(selected_tools)} tools out of {len(all_tools)} for query.")
        return selected_tools
