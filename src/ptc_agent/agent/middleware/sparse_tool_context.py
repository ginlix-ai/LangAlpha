import logging
from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import SystemMessage, HumanMessage

logger = logging.getLogger(__name__)

def _append_content_block(system_message: SystemMessage | None, text: str) -> SystemMessage:
    """Append a text content block to a system message."""
    new_content: list[dict[str, str]] = (
        list(system_message.content_blocks) if system_message else []
    )
    prefix = "\n\n" if new_content else ""
    new_content.append({"type": "text", "text": f"{prefix}{text}"})
    return SystemMessage(content_blocks=new_content)

class SparseToolContextMiddleware(AgentMiddleware):
    """
    Dynamically selects and injects a sparse tool summary based on user query.
    Appends the selected tool guidelines after the cache breakpoint to avoid cache invalidation.
    """
    def __init__(self, *, mcp_registry: Any, ace_config: Any) -> None:
        self.mcp_registry = mcp_registry
        self.ace_config = ace_config
        
        from src.ptc_agent.core.ace_adapter import (
            SparseToolSelector,
            AsyncCacheManager,
            MCPSchemaIndexer,
            QueryToolMatcher,
        )
        
        # Initialize ACE components
        prefix = ace_config.redis_key_prefix if hasattr(ace_config, "redis_key_prefix") else "ace:"
        cache_manager = AsyncCacheManager(prefix=prefix)
        indexer = MCPSchemaIndexer()
        
        mappings = None
        fallback = True
        if hasattr(ace_config, "sparse_selection"):
            mappings = getattr(ace_config.sparse_selection, "category_mappings", None)
            fallback = getattr(ace_config.sparse_selection, "fallback_to_all", True)
            
        matcher = QueryToolMatcher(category_mappings=mappings)
        self.selector = SparseToolSelector(
            cache_manager=cache_manager,
            indexer=indexer,
            matcher=matcher,
            max_depth=1,
            fallback_to_all=fallback
        )

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        """Inject sparse MCP tool summary based on query analysis."""
        enabled = getattr(self.ace_config, "enabled", True)
        selection_enabled = True
        if hasattr(self.ace_config, "sparse_selection"):
            selection_enabled = getattr(self.ace_config.sparse_selection, "enabled", True)
            
        if not enabled or not selection_enabled or not self.mcp_registry:
            return await handler(request)

        # 1. Extract query from request messages
        query = ""
        for msg in reversed(request.messages):
            if isinstance(msg, HumanMessage):
                query = msg.content
                break

        # Handle structured content
        if isinstance(query, list):
            text_parts = []
            for block in query:
                if isinstance(block, dict) and block.get("type") == "text":
                    text_parts.append(block.get("text", ""))
                elif isinstance(block, str):
                    text_parts.append(block)
            query = " ".join(text_parts)
        elif not isinstance(query, str):
            query = str(query)

        # 2. Select matching tools via ACE
        try:
            selected_tools = await self.selector.select_relevant_tools(query, self.mcp_registry)
        except Exception as e:
            logger.error(f"ACE: Error selecting tools, falling back to all: {e}", exc_info=True)
            return await handler(request)

        # 3. Format tool summary
        from ptc_agent.agent.prompts.formatter import build_tool_summary_from_registry
        
        class FilteredMCPRegistry:
            def __init__(self, selected_tools_list, original_registry):
                self.selected_tools_list = selected_tools_list
                self.config = getattr(original_registry, "config", None)
                
            def get_all_tools(self):
                tools_by_server = {}
                for tool in self.selected_tools_list:
                    tools_by_server.setdefault(tool.server_name, []).append(tool)
                return tools_by_server

        filtered_registry = FilteredMCPRegistry(selected_tools, self.mcp_registry)
        
        mode = "summary"
        if hasattr(self.mcp_registry, "config") and hasattr(self.mcp_registry.config, "mcp"):
            mode = getattr(self.mcp_registry.config.mcp, "tool_exposure_mode", "summary")
            
        sparse_summary = build_tool_summary_from_registry(filtered_registry, mode=mode)

        # 4. Inject into system message
        if sparse_summary:
            context_block = (
                "<sparse_mcp_tools>\n"
                "### Matched MCP Tools (via Python)\n"
                "Only the following MCP tools are loaded for this turn. Do NOT attempt to use tools not listed here.\n\n"
                f"{sparse_summary}\n"
                "</sparse_mcp_tools>"
            )
            logger.info(f"ACE: Injected sparse tool summary ({len(selected_tools)} tools).")
            new_system_message = _append_content_block(request.system_message, context_block)
            modified_request = request.override(system_message=new_system_message)
            return await handler(modified_request)

        return await handler(request)
