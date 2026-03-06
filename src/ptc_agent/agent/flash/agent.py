"""Flash Agent - Minimal agent without sandbox dependencies.

Optimized for fast responses using external tools only (web search, market data,
SEC filings). No code execution, no sandbox, no MCP tools.
"""

from datetime import UTC, datetime
from typing import Any

import structlog
from langchain.agents import create_agent
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware

from ptc_agent.agent.middleware import (
    EmptyToolCallRetryMiddleware,
    ToolArgumentParsingMiddleware,
    ToolErrorHandlingMiddleware,
    ToolResultNormalizationMiddleware,
    SummarizationMiddleware,
    SkillsMiddleware,
    AskUserMiddleware,
)
from ptc_agent.agent.prompts import format_current_time, get_loader
from ptc_agent.config import AgentConfig

# Import model resilience middleware
try:
    from langchain.agents.middleware import (
        ModelRetryMiddleware,
        ModelFallbackMiddleware,
    )
except ImportError:
    ModelRetryMiddleware = None  # type: ignore[misc,assignment]
    ModelFallbackMiddleware = None  # type: ignore[misc,assignment]

# External tools only (no sandbox, no MCP)
from src.tools.search import get_web_search_tool
from src.tools.fetch import web_fetch_tool
from src.tools.sec.tool import get_sec_filing
from src.tools.market_data.tool import (
    get_stock_daily_prices,
    get_company_overview,
    get_market_indices,
    get_sector_performance,
)

logger = structlog.get_logger(__name__)


class FlashAgent:
    """Lightweight agent for fast responses without sandbox.

    Features:
    - No sandbox startup latency (~0.5s vs ~8-10s)
    - Minimal system prompt (~300 tokens vs ~2000 tokens)
    - External tools only (web search, market data, SEC filings)
    - No code execution capabilities
    - No MCP tool access

    Use cases:
    - Quick market data lookups
    - News and web searches
    - SEC filing queries
    - Simple Q&A that doesn't require code execution
    """

    def __init__(self, config: AgentConfig) -> None:
        """Initialize Flash agent.

        Args:
            config: Agent configuration (uses flash settings and LLM config)
        """
        self.config = config

        # Use flash-specific LLM if configured, otherwise fall back to main LLM
        if config.llm.flash:
            # If an llm_client was pre-created (e.g. OAuth/BYOK), use it directly
            if config.llm_client is not None:
                self.llm: Any = config.llm_client
            else:
                from src.llms import create_llm

                self.llm = create_llm(config.llm.flash)
            model = config.llm.flash
            provider = "llm_config"
        else:
            self.llm = config.get_llm_client()
            # Get provider/model info for logging
            if config.llm_definition is not None:
                provider = config.llm_definition.provider
                model = config.llm_definition.model_id
            else:
                provider = getattr(self.llm, "_llm_type", "unknown")
                model = getattr(
                    self.llm, "model", getattr(self.llm, "model_name", "unknown")
                )

        logger.info(
            "Initialized FlashAgent",
            provider=provider,
            model=model,
        )

    def _build_tools(self) -> list[Any]:
        """Build the tool list for Flash agent.

        Returns:
            List of external tools (no sandbox/MCP tools)
        """
        tools: list[Any] = []

        # Web search tool (uses configured search engine)
        web_search_tool = get_web_search_tool(
            max_search_results=10,
            time_range=None,
            verbose=False,
        )
        tools.append(web_search_tool)
        tools.append(web_fetch_tool)

        # Finance tools
        tools.extend(
            [
                get_sec_filing,
                get_stock_daily_prices,
                get_company_overview,
                get_market_indices,
                get_sector_performance,
            ]
        )

        return tools

    def _build_system_prompt(
        self,
        tools: list[Any],
        user_profile: dict | None = None,
        current_time: str | None = None,
    ) -> str:
        """Build minimal system prompt for Flash agent.

        Args:
            tools: List of available tools
            user_profile: Optional user profile dict with name, timezone, locale
            current_time: Pre-formatted current time string for time awareness

        Returns:
            Minimal system prompt string
        """
        loader = get_loader()
        return loader.render(
            "flash_system.md.j2",
            tools=tools,
            user_profile=user_profile,
            current_time=current_time,
        )

    def create_agent(
        self,
        checkpointer: Any | None = None,
        llm: Any | None = None,
        user_profile: dict | None = None,
        store: Any | None = None,
    ) -> Any:
        """Create a Flash agent with minimal middleware stack.

        Note: No MCP registry, no sandbox - MCP tools require sandbox.

        Args:
            checkpointer: Optional LangGraph checkpointer for state persistence
            llm: Optional LLM override
            user_profile: Optional user profile dict with name, timezone, locale

        Returns:
            Configured LangGraph agent
        """
        model = llm if llm is not None else self.llm

        # Freeze current time for this request (refreshes on each new query)
        request_time = datetime.now(tz=UTC)
        timezone_str = (user_profile or {}).get("timezone")
        current_time = format_current_time(request_time, timezone_str)

        # Build tools
        tools = self._build_tools()

        # Build system prompt
        system_prompt = self._build_system_prompt(
            tools, user_profile=user_profile, current_time=current_time
        )

        # Minimal shared middleware stack
        shared_middleware: list[Any] = [
            ToolArgumentParsingMiddleware(),
            ToolErrorHandlingMiddleware(),
            ToolResultNormalizationMiddleware(),
        ]

        # Add dynamic skill loader middleware (Flash mode: inline SKILL.md)
        skill_loader_middleware = SkillsMiddleware(
            mode="flash",
        )
        shared_middleware.append(skill_loader_middleware)
        tools.extend(skill_loader_middleware.tools)  # LoadSkill tool
        tools.extend(
            skill_loader_middleware.get_all_skill_tools()
        )  # Pre-register skill tools
        logger.info(
            "Flash skill loader enabled",
            skill_count=len(skill_loader_middleware.skill_registry),
            skill_tool_count=len(skill_loader_middleware.get_all_skill_tools()),
        )

        # Main middleware stack (minimal)
        main_middleware: list[Any] = []

        # Message queue middleware (allows injecting queued user messages)
        from src.ptc_agent.agent.middleware.message_queue import MessageQueueMiddleware

        main_middleware.append(MessageQueueMiddleware())

        # AskUserQuestion middleware (needed for onboarding and preference updates)
        ask_user_middleware = AskUserMiddleware()
        main_middleware.append(ask_user_middleware)
        tools.extend(ask_user_middleware.tools)
        logger.info("AskUserQuestion tool enabled for Flash agent")

        # Optional summarization (shares config with main agent)
        from src.config.settings import get_summarization_config
        summ_config = None
        if self.config.llm.summarization:
            summ_config = get_summarization_config()
            summ_config["llm"] = self.config.llm.summarization
        summarization = SummarizationMiddleware.from_config(config=summ_config, backend=None)
        if summarization is not None:
            main_middleware.append(summarization)
            summarization_config = summ_config or get_summarization_config()
            logger.info(
                "Summarization enabled",
                threshold=summarization_config.get("token_threshold", 120000),
            )

        # Model resilience middleware (retry + fallback)
        if ModelFallbackMiddleware is not None and self.config.llm.fallback:
            from src.llms import get_llm_by_type

            fallback_instances = [
                get_llm_by_type(name) for name in self.config.llm.fallback
            ]
            main_middleware.append(ModelFallbackMiddleware(*fallback_instances))
            logger.info(
                "Flash model fallback enabled",
                fallback_models=self.config.llm.fallback,
            )

        if ModelRetryMiddleware is not None:
            main_middleware.append(
                ModelRetryMiddleware(
                    max_retries=3,
                    on_failure="error",
                    backoff_factor=2.0,
                    initial_delay=1.0,
                    max_delay=60.0,
                    jitter=True,
                )
            )
            logger.info("Flash model retry enabled", max_retries=3)

        # Prompt caching, empty tool call retry, and tool call patching
        main_middleware.extend(
            [
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                EmptyToolCallRetryMiddleware(),
                PatchToolCallsMiddleware(),
            ]
        )

        # Build final middleware stack
        middleware = [*shared_middleware, *main_middleware]

        logger.info(
            "Creating Flash agent",
            tool_count=len(tools),
            middleware_count=len(middleware),
        )

        # Create agent
        agent = create_agent(
            model,
            system_prompt=system_prompt,
            tools=tools,
            middleware=middleware,
            checkpointer=checkpointer,
            store=store,
        ).with_config({"recursion_limit": 100})

        return agent
