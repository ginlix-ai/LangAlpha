import pytest
from unittest.mock import AsyncMock, MagicMock
from src.ptc_agent.core.mcp_registry import MCPToolInfo
from src.ptc_agent.core.ace_adapter.query_tool_matcher import QueryToolMatcher
from src.ptc_agent.core.ace_adapter.mcp_schema_indexer import MCPSchemaIndexer
from src.ptc_agent.core.ace_adapter.sparse_tool_selector import SparseToolSelector

def test_query_tool_matcher():
    matcher = QueryToolMatcher()
    
    # Test matching categories
    price_servers = matcher.match_query("show me AAPL stock price chart")
    assert "price_data" in price_servers
    assert "yf_price" in price_servers
    
    macro_servers = matcher.match_query("what is the inflation rate in US?")
    assert "macro" in macro_servers
    
    options_servers = matcher.match_query("fetch options chain for TSLA")
    assert "options" in options_servers
    
    # Test no match
    empty_servers = matcher.match_query("hello world")
    assert len(empty_servers) == 0

def test_mcp_schema_indexer():
    indexer = MCPSchemaIndexer()
    
    # Mock some tools
    tools = [
        MCPToolInfo(
            name="get_financial_statements",
            description="get income statement and balance sheet",
            input_schema={"properties": {"symbol": {"type": "string"}, "period": {"type": "string"}}},
            server_name="fundamentals"
        ),
        MCPToolInfo(
            name="get_historical_valuation",
            description="get DCF model valuation",
            input_schema={"properties": {"symbol": {"type": "string"}}},
            server_name="fundamentals"
        ),
        MCPToolInfo(
            name="get_stock_daily_prices",
            description="get stock price chart",
            input_schema={"properties": {"symbol": {"type": "string"}, "period": {"type": "string"}}},
            server_name="price_data"
        )
    ]
    
    graph = indexer.build_graph(tools)
    
    # Tools on same server should be connected
    assert "fundamentals:get_historical_valuation" in graph["fundamentals:get_financial_statements"]
    assert "fundamentals:get_financial_statements" in graph["fundamentals:get_historical_valuation"]
    
    # Tools sharing parameters should be connected
    assert "price_data:get_stock_daily_prices" in graph["fundamentals:get_financial_statements"]
    
    # DCF -> financial_statements connection via workflow_connections
    assert "fundamentals:get_financial_statements" in graph["fundamentals:get_historical_valuation"]

@pytest.mark.asyncio
async def test_sparse_tool_selector():
    # Mock dependencies
    cache_mock = AsyncMock()
    cache_mock.get_graph.return_value = {} # Force indexing
    
    tools = [
        MCPToolInfo(
            name="get_stock_daily_prices",
            description="get stock price",
            input_schema={"properties": {"symbol": {"type": "string"}}},
            server_name="price_data"
        ),
        MCPToolInfo(
            name="get_financial_statements",
            description="get financial statements",
            input_schema={"properties": {"symbol": {"type": "string"}}},
            server_name="fundamentals"
        )
    ]
    
    registry_mock = MagicMock()
    registry_mock.get_all_tools.return_value = {
        "price_data": [tools[0]],
        "fundamentals": [tools[1]]
    }
    
    selector = SparseToolSelector(
        cache_manager=cache_mock,
        fallback_to_all=True
    )
    
    # Query matching price
    selected = await selector.select_relevant_tools("AAPL stock price", registry_mock)
    selected_names = [t.name for t in selected]
    assert "get_stock_daily_prices" in selected_names
    # BFS might pull get_financial_statements as well due to shared parameter 'symbol'
    
    # Query with no match -> should fallback to all
    selected_fallback = await selector.select_relevant_tools("something completely unrelated", registry_mock)
    assert len(selected_fallback) == 2
