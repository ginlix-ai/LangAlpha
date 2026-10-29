"""The research graph behind market insights.

A brief needs news, prices and filings, and nothing that reaches into a user's
account or workspace, so the graph binds the web, market-data and SEC tools
alone. Each brief is one run that nobody resumes or steers: no checkpointer,
no store, no runtime context.
"""

from typing import Any

from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware
from langchain.agents import create_agent
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware

from ptc_agent.agent.limits import UNBOUNDED_RECURSION_LIMIT
from ptc_agent.agent.middleware import (
    CompactionMiddleware,
    EmptyToolCallRetryMiddleware,
    ReasoningCompatibilityMiddleware,
    RepeatCallGuardMiddleware,
    ToolArgumentParsingMiddleware,
    ToolErrorHandlingMiddleware,
    ToolResultNormalizationMiddleware,
)
from ptc_agent.agent.middleware.system_prompt_caching import (
    SystemPromptCacheMiddleware,
)
from ptc_agent.agent.state import DeltaAgentState
from ptc_agent.agent.turn import build_model_resilience_middleware, turn_model
from ptc_agent.config import AgentConfig
from src.tools.market_data.tool import (
    get_company_overview,
    get_daily_prices,
    get_market_overview,
    get_options_chain,
    get_quote,
    screen_stocks,
)
from src.tools.sec.tool import get_sec_filing
from src.tools.web.fetch import web_fetch_tool
from src.tools.web.search import get_web_search_tool

INSIGHT_RESEARCH_PROMPT = (
    "You research financial markets for a market brief. Use WebSearch for news "
    "and current events, WebFetch to read a page in full, and the market-data "
    "and SEC tools for prices, company data and filings. Report only what your "
    "sources support."
)


def insight_tools(config: AgentConfig) -> list[Any]:
    return [
        get_web_search_tool(
            max_search_results=10,
            time_range=None,
            verbose=False,
            provider=config.search_api,
            depth=config.search_depth,
        ),
        web_fetch_tool,
        get_sec_filing,
        get_quote,
        get_daily_prices,
        get_company_overview,
        get_market_overview,
        get_options_chain,
        screen_stocks,
    ]


def background_client(config: AgentConfig) -> Any:
    """The Background model's client, or the main model's when none is named.

    A client ``resolve_llm_config`` already built (BYOK, OAuth) is the one this
    user's credentials reach, so it wins over building from the name.
    """
    if config.llm_client is not None or config.llm is None or not config.llm.background:
        return config.get_llm_client()
    from src.llms import create_llm
    from src.llms.llm import ensure_model_in_manifest

    # A name missing from models.json is a custom model with no resolvable BYOK
    # key, or a typo: raise the neutral error, not the factory's generic one.
    ensure_model_in_manifest(config.llm.background)
    return create_llm(config.llm.background, cache_key=config.cache_key)


def build_insight_graph(config: AgentConfig) -> Any:
    turn = turn_model(config, None, background_client(config), background=True)

    middleware = [
        m
        for m in [
            # Ahead of the tool wrappers, as in the chat stacks. With no step cap,
            # it ends a loop before the generation timeout discards the brief.
            RepeatCallGuardMiddleware(),
            ToolArgumentParsingMiddleware(),
            ToolErrorHandlingMiddleware(),
            ToolResultNormalizationMiddleware(),
            # A brief can fetch enough pages to fill the window.
            CompactionMiddleware.for_agent(config),
            build_model_resilience_middleware(config, turn),
            AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
            SystemPromptCacheMiddleware(),
            EmptyToolCallRetryMiddleware(),
            PatchToolCallsMiddleware(),
            # Inside model resilience, so it sanitizes against the model a
            # fallback actually calls.
            ReasoningCompatibilityMiddleware(),
        ]
        if m is not None
    ]

    return create_agent(
        turn.client,
        system_prompt=INSIGHT_RESEARCH_PROMPT,
        tools=insight_tools(config),
        middleware=middleware,
        state_schema=DeltaAgentState,
    ).with_config({"recursion_limit": UNBOUNDED_RECURSION_LIMIT})
