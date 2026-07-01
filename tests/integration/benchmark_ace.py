import pytest
import json
from unittest.mock import MagicMock, AsyncMock
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.messages import SystemMessage, HumanMessage
from src.ptc_agent.core.mcp_registry import MCPToolInfo
from src.ptc_agent.agent.middleware.sparse_tool_context import SparseToolContextMiddleware

# A simple mock config mimicking AgentConfig
class MockAceSparseSelectionConfig:
    enabled = True
    fallback_to_all = True
    max_servers_per_query = 4
    classification_llm = "test-flash"
    category_mappings = {
        "price_analysis": ["price_data", "yf_price"],
        "fundamental_analysis": ["fundamentals", "yf_fundamentals"],
        "market_overview": ["macro", "yf_market"],
        "options_analysis": ["options"]
    }

class MockAceConfig:
    enabled = True
    redis_key_prefix = "test_ace:"
    sparse_selection = MockAceSparseSelectionConfig()

@pytest.mark.asyncio
async def test_ace_token_savings_benchmark():
    # 1. Setup a set of 15 dummy tools across 4 servers
    tools = []
    
    # 5 price tools
    for i in range(5):
        tools.append(MCPToolInfo(
            name=f"get_price_metric_{i}",
            description=f"detailed description of price metric {i} to increase token usage",
            input_schema={"properties": {"symbol": {"type": "string"}, "interval": {"type": "string"}}},
            server_name="price_data"
        ))
        
    # 5 fundamental tools
    for i in range(5):
        tools.append(MCPToolInfo(
            name=f"get_fundamental_ratio_{i}",
            description=f"detailed description of fundamental ratio {i} to increase token usage",
            input_schema={"properties": {"symbol": {"type": "string"}, "period": {"type": "string"}}},
            server_name="fundamentals"
        ))
        
    # 3 macro tools
    for i in range(3):
        tools.append(MCPToolInfo(
            name=f"get_macro_indicator_{i}",
            description=f"detailed description of macro indicator {i} to increase token usage",
            input_schema={"properties": {"country": {"type": "string"}}},
            server_name="macro"
        ))
        
    # 2 options tools
    for i in range(2):
        tools.append(MCPToolInfo(
            name=f"get_options_contracts_{i}",
            description=f"detailed description of option contracts {i} to increase token usage",
            input_schema={"properties": {"symbol": {"type": "string"}, "strike": {"type": "number"}}},
            server_name="options"
        ))

    # Mock registry
    mcp_registry = MagicMock()
    mcp_registry.get_all_tools.return_value = {
        "price_data": [t for t in tools if t.server_name == "price_data"],
        "fundamentals": [t for t in tools if t.server_name == "fundamentals"],
        "macro": [t for t in tools if t.server_name == "macro"],
        "options": [t for t in tools if t.server_name == "options"]
    }
    mcp_registry.config.mcp.tool_exposure_mode = "detailed"
    
    class MockServer:
        def __init__(self, name, description):
            self.name = name
            self.enabled = True
            self.description = description
            self.instruction = ""
            self.tool_exposure_mode = "detailed"
            self.source = "builtin"
            
    mcp_registry.config.mcp.servers = [
        MockServer("price_data", "stock prices"),
        MockServer("fundamentals", "financial statements"),
        MockServer("macro", "macro indicators"),
        MockServer("options", "option contracts")
    ]

    # Mock LLM client for classifier
    llm_mock = AsyncMock()

    # Initialize middleware with mock LLM client
    middleware = SparseToolContextMiddleware(
        mcp_registry=mcp_registry, 
        ace_config=MockAceConfig(),
        classification_llm=llm_mock
    )
    middleware.selector.cache = AsyncMock()
    middleware.selector.cache.get_graph.return_value = {} # Trigger dynamic index build

    # Estimate token count (chars / 4)
    def estimate_tokens(text: str) -> int:
        return len(text) // 4

    # Baseline tool summary (All tools)
    from ptc_agent.agent.prompts.formatter import build_tool_summary_from_registry
    full_summary = build_tool_summary_from_registry(mcp_registry, mode="detailed")
    full_tokens = estimate_tokens(full_summary)
    
    print("\n" + "="*50)
    print(f"BASELINE (All Tools): {len(tools)} tools, approx {full_tokens} tokens")
    print("="*50)

    # Scenarios to test
    scenarios = [
        {
            "name": "Price Analysis Query",
            "query": "Show me TSLA price chart and daily moving average",
            "expected_servers": ["price_data"]
        },
        {
            "name": "Fundamentals Query",
            "query": "I want to calculate DCF for Apple using historical financials",
            "expected_servers": ["fundamentals"]
        },
        {
            "name": "Unrelated Query (Fallback)",
            "query": "Translate this sentence to French",
            "expected_servers": [] # Will fallback to all
        }
    ]

    for scenario in scenarios:
        # Configure LLM mock response dynamically based on scenario
        if scenario["expected_servers"]:
            llm_mock.ainvoke.return_value.content = json.dumps(scenario["expected_servers"])
        else:
            llm_mock.ainvoke.return_value.content = "[]"

        # Create request
        system_msg = SystemMessage(content_blocks=[{"type": "text", "text": "You are a helpful assistant."}])
        messages = [
            HumanMessage(content=scenario["query"])
        ]
        request = ModelRequest(
            messages=messages,
            system_message=system_msg,
            model="test-model"
        )
        
        # Define a mock handler that captures the modified request
        captured_request = None
        async def mock_handler(req: ModelRequest):
            nonlocal captured_request
            captured_request = req
            return MagicMock()

        # Execute middleware
        await middleware.awrap_model_call(request, mock_handler)
        
        # Analyze results
        assert captured_request is not None
        modified_system_msg = captured_request.system_message
        
        # Get sparse block
        sparse_block = ""
        for block in modified_system_msg.content_blocks:
            text = block.get("text", "")
            if "<sparse_mcp_tools>" in text:
                sparse_block = text
                break
                
        assert sparse_block != ""
        sparse_tokens = estimate_tokens(sparse_block)
        savings = full_tokens - sparse_tokens
        savings_pct = (savings / full_tokens) * 100 if full_tokens > 0 else 0
        
        print(f"\nScenario: {scenario['name']}")
        print(f"Query   : '{scenario['query']}'")
        print(f"Tokens  : {sparse_tokens} (vs {full_tokens} baseline)")
        print(f"Savings : {savings} tokens ({savings_pct:.1f}% reduction)")
        
        if scenario["expected_servers"]:
            # Verify we reduced token usage
            assert sparse_tokens < full_tokens
            assert savings_pct > 30.0 # Expecting at least 30% savings with subsetting
        else:
            # Fallback should return all tools (similar to full summary size)
            assert sparse_tokens >= full_tokens * 0.9
