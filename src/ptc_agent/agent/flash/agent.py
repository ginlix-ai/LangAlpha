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
    CompactionMiddleware,
    SkillsMiddleware,
    AskUserMiddleware,
)
from ptc_agent.agent.middleware.runtime_context import RuntimeContextMiddleware
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
    get_options_chain,
    get_sector_performance,
    screen_stocks,
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
                get_options_chain,
                get_sector_performance,
                screen_stocks,
            ]
        )

        # Secretary tools (workspace management, PTC dispatch, output monitoring)
        from src.tools.secretary import SECRETARY_TOOLS

        tools.extend(SECRETARY_TOOLS)

        return tools

    def _build_system_prompt(
        self,
        tools: list[Any],
    ) -> str:
        """Build the static system prompt (excludes time/profile for cacheability).

        Args:
            tools: List of available tools

        Returns:
            Rendered system prompt string
        """
        loader = get_loader()
        return loader.render(
            "flash_system.md.j2",
            tools=tools,
        )

    def create_agent(
        self,
        checkpointer: Any | None = None,
        llm: Any | None = None,
        user_profile: dict | None = None,
        store: Any | None = None,
        response_format: Any | None = None,
    ) -> Any:
        """Create a Flash agent with minimal middleware stack.

        Note: No MCP registry, no sandbox - MCP tools require sandbox.

        Args:
            checkpointer: Optional LangGraph checkpointer for state persistence
            llm: Optional LLM override
            user_profile: Optional user profile dict with name, timezone, locale
            response_format: Optional structured output schema (Pydantic model or dict).
                When set, the agent is forced to return structured data matching this schema.

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

        # Build system prompt (time + profile injected by RuntimeContextMiddleware)
        system_prompt = self._build_system_prompt(tools)

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

        # Steering middleware (allows injecting steering messages from user)
        from ptc_agent.agent.middleware.steering import SteeringMiddleware

        main_middleware.append(SteeringMiddleware())

        # AskUserQuestion middleware (needed for onboarding and preference updates)
        ask_user_middleware = AskUserMiddleware()
        main_middleware.append(ask_user_middleware)
        tools.extend(ask_user_middleware.tools)
        logger.info("AskUserQuestion tool enabled for Flash agent")

        # Optional compaction (shares config with main agent)
        compaction_config = None
        if self.config.llm.compaction:
            compaction_config = self.config.compaction.model_dump()
            compaction_config["llm"] = self.config.llm.compaction
            compaction_client = self.config.subsidiary_llm_clients.get("compaction")
            if compaction_client:
                compaction_config["_llm_client"] = compaction_client
            elif self.config.llm_client:
                # Deep-copy so CompactionMiddleware.from_config() can set
                # streaming=False without mutating the main agent's model.
                compaction_config["_llm_client"] = self.config.llm_client.model_copy()
        compaction = CompactionMiddleware.from_config(config=compaction_config, backend=None)
        if compaction is not None:
            main_middleware.append(compaction)
            logger.info(
                "Compaction enabled",
                threshold=self.config.compaction.token_threshold,
            )

        # Model resilience middleware (retry + fallback)
        if ModelFallbackMiddleware is not None and self.config.llm.fallback:
            # Use pre-resolved clients (OAuth/BYOK-aware) when available
            if self.config.fallback_llm_clients:
                fallback_instances = self.config.fallback_llm_clients
            else:
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

        # Runtime context middleware (time + user profile — after cache breakpoint)
        runtime_context_middleware = RuntimeContextMiddleware(
            current_time=current_time,
            user_profile=user_profile,
        )

        # Build final middleware stack
        # RuntimeContextMiddleware is last (innermost) so it appends after
        # the cache breakpoint, keeping the static prompt cacheable.
        middleware = [*shared_middleware, *main_middleware, runtime_context_middleware]

        logger.info(
            "Creating Flash agent",
            tool_count=len(tools),
            middleware_count=len(middleware),
        )

        # Create agent
        create_kwargs: dict[str, Any] = dict(
            system_prompt=system_prompt,
            tools=tools,
            middleware=middleware,
            checkpointer=checkpointer,
            store=store,
        )
        if response_format is not None:
            create_kwargs["response_format"] = response_format

        agent = create_agent(
            model,
            **create_kwargs,
        ).with_config({"recursion_limit": 500})

        return agent
