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
    CreditGateMiddleware,
    EmptyToolCallRetryMiddleware,
    ToolArgumentParsingMiddleware,
    ToolErrorHandlingMiddleware,
    ToolResultNormalizationMiddleware,
    CompactionMiddleware,
    SkillsMiddleware,
    AskUserMiddleware,
    LeakDetectionMiddleware,
    MultimodalStripMiddleware,
    ProvenanceMiddleware,
    ReasoningCompatibilityMiddleware,
)
from ptc_agent.agent.middleware.openai_prompt_caching import (
    OpenAIPromptCachingMiddleware,
)
from ptc_agent.agent.middleware.direct_mcp import (
    DirectToolSet,
    direct_tool_middleware,
    direct_tool_summary,
)
from ptc_agent.agent.middleware.order_governance import OrderLedger
from ptc_agent.agent.middleware.skills.registry import (
    build_effective_skill_registry,
)
from ptc_agent.agent.context_stack import build_context_middleware
from ptc_agent.agent.middleware.runtime_context import (
    BaselineSources,
    MemoryTierSource,
    TurnContext,
)
from ptc_agent.agent.state import DeltaAgentState
from ptc_agent.agent.prompts import (
    get_loader,
    guidance_template_vars,
)
from ptc_agent.config import AgentConfig
from ptc_agent.core.paths import MEMORY_INDEX_FILENAME, MEMORY_USER_DIR

from ptc_agent.agent.turn import build_model_resilience_middleware, turn_model

# External tools only (no sandbox, no MCP)
from src.tools.web.search import get_web_search_tool
from src.tools.web.fetch import web_fetch_tool
from src.tools.sec.tool import get_sec_filing
from src.tools.market_data.tool import (
    get_daily_prices,
    get_company_overview,
    get_market_overview,
    get_options_chain,
    get_quote,
    screen_stocks,
)
from src.tools.chart_annotation import CHART_ANNOTATION_TOOLS

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
                from src.llms.llm import ensure_model_in_manifest

                # Models not in models.json reach here either because the user
                # picked a custom model without a resolvable BYOK key, or
                # because the name is a typo. Raise a neutral error instead of
                # the generic factory one.
                ensure_model_in_manifest(config.llm.flash)
                self.llm = create_llm(config.llm.flash, cache_key=config.cache_key)
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

    def _build_tools(self, chart_annotation: bool = True) -> list[Any]:
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
            provider=self.config.search_api,
            depth=self.config.search_depth,
        )
        tools.append(web_search_tool)
        tools.append(web_fetch_tool)

        # Finance tools
        tools.extend(
            [
                get_sec_filing,
                get_quote,
                get_daily_prices,
                get_company_overview,
                get_market_overview,
                get_options_chain,
                screen_stocks,
            ]
        )

        # Secretary tools (workspace management, PTC dispatch, output monitoring)
        from src.tools.secretary import SECRETARY_TOOLS

        tools.extend(SECRETARY_TOOLS)

        # Not gated on loading the chart-annotation skill (that is only the
        # drawing guide), but off with it.
        if chart_annotation:
            tools.extend(CHART_ANNOTATION_TOOLS)

        return tools

    def _build_system_prompt(
        self,
        tools: list[Any],
        guidance: str,
        direct_tool_summary: str = "",
        chart_annotation_enabled: bool = True,
    ) -> str:
        """Build the static system prompt (excludes time/profile for cacheability).

        ``guidance`` is resolved for the flash model, not the main one: a
        deployment running Haiku on Flash and Opus on PTC sizes each prompt
        for the model that renders it.
        """
        loader = get_loader()
        return loader.render(
            "flash_system.md.j2",
            tools=tools,
            direct_tool_summary=direct_tool_summary,
            ask_user_enabled=True,
            chart_annotation_enabled=chart_annotation_enabled,
            **guidance_template_vars(guidance),
        )

    def create_agent(
        self,
        checkpointer: Any | None = None,
        llm: Any | None = None,
        user_profile: dict | None = None,
        user_data_counts: dict | None = None,
        store: Any | None = None,
        user_id: str | None = None,
        response_format: Any | None = None,
        direct_mcp: DirectToolSet | None = None,
        order_ledger: OrderLedger | None = None,
        turn_context: TurnContext | None = None,
        chart_annotation: bool = True,
    ) -> Any:
        """Create a Flash agent with minimal middleware stack.

        No MCP registry and no sandbox. ``direct_mcp`` is the one MCP surface
        Flash has: tools bound to the model as JSON tools through the relay,
        checked per call against the connection's current status and consent.
        An order is stopped against the same durable attempt the PTC path
        writes, so the user confirms before the vendor sees it.

        Args:
            checkpointer: Optional LangGraph checkpointer for state persistence
            llm: Optional LLM override
            user_profile: Optional user profile dict with name, timezone, locale
            user_id: Owner of the user-tier memory namespace. None disables the
                memory tier of the runtime-context baseline.
            response_format: Optional structured output schema (Pydantic model or dict).
                When set, the agent is forced to return structured data matching this schema.
            turn_context: What this turn knows about itself (when the previous
                one ran, the surface it arrived on and that surface's delivery
                rules, the zone its clock is stamped in), for the turn anchor
                row. None for a context-free build.
            chart_annotation: False for a build with no workspace to draw in.

        Returns:
            Configured LangGraph agent
        """
        turn = turn_model(self.config, llm, self.llm, flash=True)

        # Freeze current time for this request (refreshes on each new query)
        request_time = datetime.now(tz=UTC)

        # Same per-user registry assembly as the PTC build: feature gates,
        # builtin disables, user skills. Flash previously took the bare
        # system-gated registry, so a per-user feature opt-out that hid a
        # skill in PTC left it visible here. Built before the tools because
        # the chart-annotation skill's switch also turns its tools off.
        skill_registry = build_effective_skill_registry(
            "flash",
            feature_resolver=self.config.feature_enabled,
            disabled_skills=self.config.disabled_skills,
            user_skills=self.config.user_skills,
            user_skill_dir=self.config.user_skill_dir,
            workspace_skill_dir=self.config.workspace_skill_dir,
        )
        chart_annotation = chart_annotation and "chart-annotation" in skill_registry
        if not chart_annotation:
            # No tools to guide: keep the guide out of the manifest and LoadSkill.
            skill_registry.pop("chart-annotation", None)

        # Build tools
        tools = self._build_tools(chart_annotation=chart_annotation)
        direct_tools = list(direct_mcp.tools) if direct_mcp is not None else []

        # Build system prompt (the volatile stamp rides the tail envelope)
        system_prompt = self._build_system_prompt(
            tools,
            turn.guidance,
            direct_tool_summary=direct_tool_summary(direct_tools),
            chart_annotation_enabled=chart_annotation,
        )

        # Leak detector wired into provenance so web/market/SEC snippets are
        # scrubbed before they're emitted/persisted, mirroring the main agent.
        # Flash has no MCP/vault secret surface today, so this is effectively a
        # no-op now; wiring it keeps the two modes consistent and active the
        # moment Flash ever gains a secret source.
        leak_detection = LeakDetectionMiddleware(
            mcp_servers=None,
            vault_secrets=None,
        )

        # Minimal shared middleware stack
        shared_middleware: list[Any] = [
            # Inert unless the server installed a gate state for this run.
            CreditGateMiddleware(),
            ToolArgumentParsingMiddleware(),
            ToolErrorHandlingMiddleware(),
            leak_detection,
            ToolResultNormalizationMiddleware(),
            # Traces web/market/SEC reads (filesystem/MCP extractors no-op here).
            ProvenanceMiddleware(redactor=leak_detection.redact),
        ]

        # Add dynamic skill loader middleware (Flash mode: inline SKILL.md).
        skill_loader_middleware = SkillsMiddleware(
            skill_registry=skill_registry,
            mode="flash",
            skill_dirs=[
                d for d, _ in self.config.skills.local_skill_dirs_with_sandbox()
            ],
            # The baseline states the manifest, so it is frozen for the epoch
            # instead of sitting in front of the history and moving under it.
            inject_manifest=False,
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

        # Consent is re-read per call, and an order is stopped against its own
        # durable attempt before the vendor sees it.
        main_middleware.extend(direct_tool_middleware(direct_mcp, order_ledger))
        tools.extend(direct_tools)

        # AskUserQuestion middleware (needed for onboarding and preference updates)
        ask_user_middleware = AskUserMiddleware()
        main_middleware.append(ask_user_middleware)
        tools.extend(ask_user_middleware.tools)
        logger.info("AskUserQuestion tool enabled for Flash agent")

        from src.tools.secretary.approvals import StandingApprovalMiddleware

        main_middleware.append(StandingApprovalMiddleware(user_id))

        # Optional compaction (shares config with main agent)
        compaction = (
            CompactionMiddleware.for_agent(self.config)
            if self.config.llm.compaction_name
            else None
        )
        if compaction is not None:
            main_middleware.append(compaction)
            logger.info(
                "Compaction enabled",
                threshold=self.config.compaction.token_threshold,
            )

        # Model resilience middleware (retry + fallback + progress events)
        model_resilience = build_model_resilience_middleware(self.config, turn)
        main_middleware.append(model_resilience)
        logger.info(
            "Flash model resilience enabled",
            max_retries=3,
            fallback_models=[name for name, _ in model_resilience.fallbacks],
        )

        # Only the strip half is wired here: Flash exposes no filesystem tool at
        # all, so the injection half has nothing to intercept. Without the strip,
        # a mid-thread switch to a text-only model replays an earlier turn's
        # image/PDF blocks and strict providers reject the request outright.
        # Inside model resilience so it strips against the post-fallback model.
        # ``can_extract=False`` for the same reason: there is no workspace to
        # tell it to pull the file apart in.
        main_middleware.append(
            MultimodalStripMiddleware(
                model_name=self.config.llm.flash_name,
                custom_modalities=self.config.input_modalities,
                can_extract=False,
            )
        )

        # Prompt caching (per-provider breakpoints), empty tool call retry,
        # and tool call patching
        main_middleware.extend(
            [
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                OpenAIPromptCachingMiddleware(),
                EmptyToolCallRetryMiddleware(),
                PatchToolCallsMiddleware(),
            ]
        )

        # The turn row, the per-thread baseline and the tail envelope. Flash
        # has no sandbox, so the baseline carries no agent.md tier: what it has
        # is the user's identity plus the steering the profile component holds,
        # the user memory index when identity is known, and the skills manifest.
        # No MCP roster: Flash reaches MCP only through directly bound tools.
        context = build_context_middleware(
            now=request_time,
            guidance=turn.guidance,
            model_name=turn.name or None,
            turn_context=turn_context,
            user_profile=user_profile,
            user_data_counts=user_data_counts,
            sandbox_enabled=False,
            sources=BaselineSources(
                store=store if user_id else None,
                memory=(
                    {
                        "user": MemoryTierSource(
                            namespace_factory=lambda: (user_id, "memory"),
                            display_path=f"{MEMORY_USER_DIR}/{MEMORY_INDEX_FILENAME}",
                        )
                    }
                    if store is not None and user_id
                    else {}
                ),
            ),
            blocks={
                "skills": lambda state: skill_loader_middleware.build_manifest(state)
                or ""
            },
        )

        # Build final middleware stack. Where each of the three context
        # middlewares has to sit is on ContextMiddleware;
        # ReasoningCompatibilityMiddleware sits inside model resilience so it
        # sanitizes against the post-fallback model, not the requested one.
        middleware = [
            *shared_middleware,
            *main_middleware,
            context.turn,
            context.baseline,
            context.tail,
            ReasoningCompatibilityMiddleware(),
        ]

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
            state_schema=DeltaAgentState,
        )
        if response_format is not None:
            create_kwargs["response_format"] = response_format

        agent = create_agent(
            turn.client,
            **create_kwargs,
        ).with_config({"recursion_limit": 500})

        return agent
