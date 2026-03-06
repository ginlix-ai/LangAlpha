"""PTC Agent - Main agent using create_agent with Programmatic Tool Calling pattern.

This module creates a PTC agent that:
- Uses langchain's create_agent with custom middleware stack
- Integrates Daytona sandbox via DaytonaBackend
- Provides MCP tools through execute_code
- Supports sub-agent delegation for specialized tasks
"""

from datetime import UTC, datetime
from typing import Any

import structlog
from langchain.agents import create_agent

from ptc_agent.agent.backends import DaytonaBackend
from ptc_agent.agent.middleware import SubAgentMiddleware
from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware

from ptc_agent.agent.middleware import (
    AskUserMiddleware,
    BackgroundSubagentMiddleware,
    BackgroundSubagentOrchestrator,
    PlanModeMiddleware,
    ToolCallCounterMiddleware,
    MultimodalMiddleware,
    create_plan_mode_interrupt_config,
    # Tool middleware
    EmptyToolCallRetryMiddleware,
    ToolArgumentParsingMiddleware,
    ToolErrorHandlingMiddleware,
    ToolResultNormalizationMiddleware,
    # File operations SSE middleware
    FileOperationMiddleware,
    # Todo operations SSE middleware
    TodoWriteMiddleware,
    # Skills middleware
    SkillsMiddleware,
    # Summarization middleware
    SummarizationMiddleware,
    # Large result eviction middleware
    LargeResultEvictionMiddleware,
    # Message queue middleware
    MessageQueueMiddleware,
    # Subagent message queue middleware
    SubagentMessageQueueMiddleware,
    # Workspace context middleware
    WorkspaceContextMiddleware,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.skills.discovery import SkillMetadata
from ptc_agent.agent.prompts import (
    build_tool_summary_from_registry,
    format_current_time,
    format_subagent_summary,
    get_loader,
)
from ptc_agent.agent.subagents import (
    SubagentCompiler,
    SubagentRegistry,
    create_subagents,
)
from ptc_agent.agent.tools import (
    create_execute_bash_tool,
    create_execute_code_tool,
    create_filesystem_tools,
    create_glob_tool,
    create_grep_tool,
    TodoWrite,
)
from src.tools.search import get_web_search_tool
from src.tools.fetch import web_fetch_tool
from src.tools.sec.tool import get_sec_filing
from src.tools.market_data.tool import (
    get_stock_daily_prices,
    get_company_overview,
    get_market_indices,
    get_sector_performance,
    screen_stocks,
)
from ptc_agent.config import AgentConfig
from ptc_agent.core.mcp_registry import MCPRegistry
from ptc_agent.core.sandbox import PTCSandbox
from ptc_agent.utils.storage.storage_uploader import is_storage_enabled

# Import HITL middleware for plan mode
try:
    from langchain.agents.middleware import HumanInTheLoopMiddleware
except ImportError:
    HumanInTheLoopMiddleware = None  # type: ignore[misc,assignment]

# Import model resilience middleware
try:
    from langchain.agents.middleware import (
        ModelRetryMiddleware,
        ModelFallbackMiddleware,
    )
except ImportError:
    ModelRetryMiddleware = None  # type: ignore[misc,assignment]
    ModelFallbackMiddleware = None  # type: ignore[misc,assignment]

# Import Checkpointer type for type hints
try:
    from langgraph.types import Checkpointer
except ImportError:
    Checkpointer = None  # type: ignore[misc,assignment]

logger = structlog.get_logger(__name__)


# Default limits for sub-agent coordination
DEFAULT_MAX_CONCURRENT_TASK_UNITS = 3
DEFAULT_MAX_TASK_ITERATIONS = 3
DEFAULT_MAX_GENERAL_ITERATIONS = 10


class PTCAgent:
    """Agent that uses Programmatic Tool Calling (PTC) pattern for MCP tool execution.

    This agent:
    - Uses langchain's create_agent with custom middleware stack
    - Integrates Daytona sandbox via DaytonaBackend
    - Provides execute_code tool for MCP tool invocation
    - Supports sub-agent delegation for specialized tasks
    """

    def __init__(self, config: AgentConfig) -> None:
        """Initialize PTC agent.

        Args:
            config: Agent configuration
        """
        self.config = config
        self.llm: Any = config.get_llm_client()
        self.subagents: dict[
            str, Any
        ] = {}  # Populated in create_agent() for introspection

        # Get provider/model info for logging
        if config.llm_definition is not None:
            provider = config.llm_definition.provider
            model = config.llm_definition.model_id
        else:
            # LLM client was passed directly via AgentConfig.create()
            # Try to extract info from the LLM instance
            provider = getattr(self.llm, "_llm_type", "unknown")
            model = getattr(
                self.llm, "model", getattr(self.llm, "model_name", "unknown")
            )

        logger.info(
            "Initialized PTCAgent with deepagent",
            provider=provider,
            model=model,
        )

    def _build_system_prompt(
        self,
        tool_summary: str,
        subagent_summary: str,
        user_profile: dict | None = None,
        plan_mode: bool = False,
        current_time: str | None = None,
        thread_id: str | None = None,
    ) -> str:
        """Build the system prompt for the agent.

        Args:
            tool_summary: Formatted MCP tool summary
            subagent_summary: Formatted subagent summary
            user_profile: Optional user profile dict with name, timezone, locale
            plan_mode: If True, includes plan mode workflow instructions
            current_time: Pre-formatted current time string for time awareness
            thread_id: Optional thread ID (first 8 chars) for thread-scoped directories

        Returns:
            Complete system prompt
        """
        loader = get_loader()

        # Render the main system prompt with all variables
        return loader.get_system_prompt(
            tool_summary=tool_summary,
            subagent_summary=subagent_summary,
            user_profile=user_profile,
            max_concurrent_task_units=DEFAULT_MAX_CONCURRENT_TASK_UNITS,
            max_task_iterations=DEFAULT_MAX_TASK_ITERATIONS,
            storage_enabled=is_storage_enabled(),
            ask_user_enabled=True,
            plan_mode=plan_mode,
            include_examples=True,
            include_anti_patterns=True,
            current_time=current_time,
            thread_id=thread_id or "",
        )

    def _build_model_resilience_middleware(self) -> list[Any]:
        """Build model retry and fallback middleware.

        Returns a list of middleware in append order (fallback first, then retry).
        Middleware execution: Fallback → Retry → Model
        Error propagation: Model fails → Retry catches (3x) → Fallback catches (switch model)
        """
        middleware: list[Any] = []

        # Fallback middleware (outermost — catches errors after retry exhausted)
        if ModelFallbackMiddleware is not None and self.config.llm.fallback:
            from src.llms import get_llm_by_type

            fallback_instances = [
                get_llm_by_type(name) for name in self.config.llm.fallback
            ]
            middleware.append(ModelFallbackMiddleware(*fallback_instances))
            logger.info(
                "Model fallback middleware enabled",
                fallback_models=self.config.llm.fallback,
            )

        # Retry middleware (innermost — retries same model before fallback)
        if ModelRetryMiddleware is not None:
            middleware.append(
                ModelRetryMiddleware(
                    max_retries=3,
                    on_failure="error",
                    backoff_factor=2.0,
                    initial_delay=1.0,
                    max_delay=60.0,
                    jitter=True,
                )
            )

        return middleware

    def _get_tool_summary(self, mcp_registry: MCPRegistry) -> str:
        """Get formatted tool summary for prompts."""
        return build_tool_summary_from_registry(
            mcp_registry, mode=self.config.mcp.tool_exposure_mode
        )

    def create_agent(
        self,
        sandbox: PTCSandbox,
        mcp_registry: MCPRegistry,
        subagent_names: list[str] | None = None,
        additional_subagents: list[dict[str, Any]] | None = None,
        background_timeout: float = 300.0,
        checkpointer: Any | None = None,
        session: Any | None = None,
        llm: Any | None = None,
        operation_callback: Any | None = None,
        background_registry: BackgroundTaskRegistry | None = None,
        user_profile: dict | None = None,
        plan_mode: bool = False,
        thread_id: str | None = None,
        on_agent_md_write: Any | None = None,
        store: Any | None = None,
    ) -> Any:
        """Create a deepagent with PTC pattern capabilities.

        Args:
            sandbox: PTCSandbox instance for code execution
            mcp_registry: MCPRegistry with available MCP tools
            subagent_names: List of subagent names to include from registry
                (default: config.subagents.enabled)
            additional_subagents: Custom subagent dicts that bypass the registry
            background_timeout: Timeout for waiting on background tasks (seconds)
            checkpointer: Optional LangGraph checkpointer for state persistence.
                Required for submit_plan interrupt/resume workflow.
            session: Optional Session object. When provided, WorkspaceContextMiddleware
                dynamically injects agent.md into the system prompt on every model call.
            llm: Optional LLM override. If provided, uses this instead of self.llm.
                Useful for model switching without recreating PTCAgent instance.
            operation_callback: Optional callback for file operation logging.
                Receives dict with operation details (operation, file_path, timestamp, etc.).
            background_registry: Optional shared background task registry for subagents.
            user_profile: Optional user profile dict with name, timezone, locale for
                injection into the system prompt.
            plan_mode: If True, adds submit_plan tool for plan review workflow.
                HITL middleware is always added for future interrupt features.
            thread_id: Optional thread ID for thread-scoped workspace directories.
                First 8 chars used as thread directory name in .agent/threads/{id}/.
            on_agent_md_write: Optional callback invoked when agent.md is written/edited.
                Used to invalidate Session's agent.md cache.

        Returns:
            Configured BackgroundSubagentOrchestrator wrapping the deepagent
        """
        # Use provided LLM or fall back to instance LLM
        model = llm if llm is not None else self.llm

        # Freeze current time for this request (refreshes on each new query)
        request_time = datetime.now(tz=UTC)
        timezone_str = (user_profile or {}).get("timezone")
        current_time = format_current_time(request_time, timezone_str)

        # Compute short thread ID for thread-scoped storage
        short_thread_id = thread_id[:8] if thread_id else ""

        # Create the execute_code tool for MCP invocation
        execute_code_tool = create_execute_code_tool(
            sandbox, mcp_registry, thread_id=short_thread_id
        )

        # Create the Bash tool for shell command execution
        bash_tool = create_execute_bash_tool(sandbox, thread_id=short_thread_id)

        # Start with base tools
        tools: list[Any] = [execute_code_tool, bash_tool, TodoWrite]

        # Create backend for SkillsMiddleware and LargeResultEvictionMiddleware
        backend = DaytonaBackend(sandbox, operation_callback=operation_callback)

        # Create custom filesystem tools (override deepagents middleware tools)
        read_file, write_file, edit_file = create_filesystem_tools(
            sandbox,
            operation_callback=operation_callback,
        )
        filesystem_tools = [
            read_file,  # overrides middleware read_file
            write_file,  # overrides middleware write_file
            edit_file,  # overrides middleware edit_file
            create_glob_tool(sandbox),  # overrides middleware glob
            create_grep_tool(sandbox),  # overrides middleware grep
        ]
        tools.extend(filesystem_tools)

        # Add web search tool (uses configured search engine from agent_config.yaml)
        web_search_tool = get_web_search_tool(
            max_search_results=10,
            time_range=None,
            verbose=False,
        )
        tools.append(web_search_tool)
        tools.append(web_fetch_tool)

        # Add finance tools
        finance_tools = [
            get_sec_filing,  # SEC filing extraction (10-K, 10-Q, 8-K)
            get_stock_daily_prices,  # Stock OHLCV price data
            get_company_overview,  # Company investment analysis (includes real-time quote)
            get_market_indices,  # Market indices data
            get_sector_performance,  # Sector performance metrics
            screen_stocks,  # Stock screener with filters
        ]
        tools.extend(finance_tools)

        # Default to subagents from config if none specified
        if subagent_names is None:
            subagent_names = self.config.subagents.enabled

        # --- Build shared middleware (for both main agent and subagents) ---
        shared_middleware: list[Any] = []

        # Tool middleware - handles argument parsing, error handling, and result normalization
        # These run in order: parse args -> execute -> handle errors -> normalize results
        shared_middleware.extend(
            [
                ToolArgumentParsingMiddleware(),
                ToolErrorHandlingMiddleware(),
                ToolResultNormalizationMiddleware(),
            ]
        )

        # File operation SSE middleware - emits events for write_file/edit_file
        shared_middleware.append(
            FileOperationMiddleware(on_agent_md_write=on_agent_md_write)
        )

        # Todo operation SSE middleware - emits events for TodoWrite
        shared_middleware.append(TodoWriteMiddleware())

        # Add multimodal middleware for read_file image/PDF support (when enabled)
        if self.config.enable_view_image:
            shared_middleware.append(MultimodalMiddleware(sandbox=sandbox))

        # Add dynamic skill loader middleware for user onboarding etc.
        # Includes filesystem scanning for user-installed skills when enabled
        skill_sources = (
            [f"{self.config.skills.sandbox_skills_base}/"]
            if self.config.skills.enabled
            else []
        )

        # Extract pre-parsed skill metadata from the sandbox's cached manifest
        # so the middleware can skip re-downloading SKILL.md files it already knows about.
        known_skills: dict[str, Any] = {}
        if sandbox.skills_manifest and sandbox.skills_manifest.get("skills"):
            known_skills = {
                name: SkillMetadata(**meta)
                for name, meta in sandbox.skills_manifest["skills"].items()
            }

        skill_loader_middleware = SkillsMiddleware(
            mode="ptc",
            backend=backend,
            sources=skill_sources,
            known_skills=known_skills,
        )
        shared_middleware.append(skill_loader_middleware)
        tools.extend(skill_loader_middleware.tools)
        tools.extend(skill_loader_middleware.get_all_skill_tools())

        # --- Build main-only middleware (NOT passed to subagents) ---
        main_only_middleware: list[Any] = []

        # Message queue middleware - checks Redis for queued user messages
        # before each LLM call (must be first so queued context is visible
        # before any other middleware runs)
        main_only_middleware.append(MessageQueueMiddleware())

        # Create counter middleware for tracking subagent tool calls
        # (Created early so it can be passed to BackgroundSubagentMiddleware)
        _bg_registry = background_registry or BackgroundTaskRegistry()
        counter_middleware = ToolCallCounterMiddleware(registry=_bg_registry)

        # Create background subagent middleware (must be created before subagents)
        background_middleware = BackgroundSubagentMiddleware(
            timeout=background_timeout,
            enabled=True,
            registry=_bg_registry,
            counter_middleware=counter_middleware,
            checkpointer=checkpointer,
        )
        main_only_middleware.append(background_middleware)
        tools.extend(background_middleware.tools)

        # Add HITL middleware (always available for future interrupt features)
        if HumanInTheLoopMiddleware is not None:
            # Add HITL interrupt config for submit_plan
            interrupt_config: Any = create_plan_mode_interrupt_config()
            hitl_middleware = HumanInTheLoopMiddleware(interrupt_on=interrupt_config)
            main_only_middleware.append(hitl_middleware)

            # Only add submit_plan tool when plan_mode is enabled
            if plan_mode:
                plan_middleware = PlanModeMiddleware()
                main_only_middleware.append(plan_middleware)
                tools.extend(plan_middleware.tools)

        # Ask user question middleware (always available for main agent)
        ask_user_middleware = AskUserMiddleware()
        main_only_middleware.append(ask_user_middleware)
        tools.extend(ask_user_middleware.tools)

        # Build subagent registry and compiler
        # Note: Subagents get vision capability through VisionMiddleware in shared_middleware
        from ptc_agent.agent.tools import think_tool

        subagent_registry = SubagentRegistry(
            user_definitions=(
                self.config.subagents.definitions
                if self.config.subagents.definitions
                else None
            ),
        )
        subagent_tool_sets: dict[str, list[Any]] = {
            "execute_code": [execute_code_tool],
            "bash": [bash_tool],
            "filesystem": list(filesystem_tools) if filesystem_tools else [],
            "web_search": [web_search_tool, web_fetch_tool],
            "finance": finance_tools,
            "think": [think_tool],
            "todo": [TodoWrite],
        }
        subagent_compiler = SubagentCompiler(
            sandbox=sandbox,
            mcp_registry=mcp_registry,
            tool_sets=subagent_tool_sets,
            user_profile=user_profile,
            current_time=current_time,
            thread_id=short_thread_id,
        )
        subagents = create_subagents(
            registry=subagent_registry,
            enabled_names=subagent_names,
            compiler=subagent_compiler,
            counter_middleware=counter_middleware,
        )

        if additional_subagents:
            subagents.extend(additional_subagents)

        # Get tool summary for system prompt
        tool_summary = self._get_tool_summary(mcp_registry)

        # Build subagent summary for system prompt
        subagent_summary = format_subagent_summary(subagents)

        # Build system prompt and eviction dir (short_thread_id computed earlier)
        eviction_dir = (
            f".agent/threads/{short_thread_id}/large_tool_results"
            if short_thread_id
            else ".agent/large_tool_results"
        )
        system_prompt = self._build_system_prompt(
            tool_summary,
            subagent_summary,
            user_profile,
            plan_mode=plan_mode,
            current_time=current_time,
            thread_id=short_thread_id,
        )

        # Store subagent info for introspection (used by print_agent_config)
        self.subagents = {}
        for subagent in subagents:
            name = subagent.get("name", "unknown")
            subagent_tools = subagent.get("tools", [])
            tool_names = [
                t.name if hasattr(t, "name") else str(t) for t in subagent_tools
            ]
            self.subagents[name] = {
                "description": subagent.get("description", ""),
                "tools": tool_names,
            }

        # Store native tools info for introspection (used by print_agent_config)
        self.native_tools = [t.name if hasattr(t, "name") else str(t) for t in tools]

        logger.info(
            "Creating agent with custom middleware stack",
            tool_count=len(tools),
            subagent_count=len(subagents),
            skills_enabled=self.config.skills.enabled,
        )

        # --- Build final middleware stacks ---

        # Custom SSE-enabled summarization emits 'summarization_signal' events
        # Pass backend for offloading conversation history to sandbox
        summ_config = None
        if self.config.llm.summarization:
            from src.config.settings import get_summarization_config
            summ_config = get_summarization_config()
            summ_config["llm"] = self.config.llm.summarization
        summarization = SummarizationMiddleware.from_config(config=summ_config, backend=backend)

        # Build model resilience middleware (retry + fallback)
        model_resilience = self._build_model_resilience_middleware()

        # Subagent middleware (shared only, no SubAgentMiddleware/BackgroundSubagentMiddleware/HITL)
        # SubagentMessageQueueMiddleware is first so follow-up messages are
        # visible before any other middleware runs.
        subagent_middleware = [
            m
            for m in [
                SubagentMessageQueueMiddleware(registry=background_middleware.registry),
                LargeResultEvictionMiddleware(
                    backend=backend, eviction_dir=eviction_dir
                ),
                *shared_middleware,
                summarization,
                *model_resilience,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                EmptyToolCallRetryMiddleware(),
                PatchToolCallsMiddleware(),
            ]
            if m is not None
        ]

        # Workspace context middleware (agent.md injection — main agent only)
        workspace_context_middleware: list[Any] = []
        if session is not None:
            workspace_context_middleware = [WorkspaceContextMiddleware(session=session)]

        # Main agent middleware (includes SubAgentMiddleware + main_only)
        deepagent_middleware = [
            m
            for m in [
                LargeResultEvictionMiddleware(
                    backend=backend, eviction_dir=eviction_dir
                ),
                SubAgentMiddleware(
                    default_model=model,
                    default_tools=tools,
                    subagents=subagents if subagents else [],
                    default_middleware=subagent_middleware,
                    registry=background_middleware.registry,
                    checkpointer=checkpointer,
                ),
                *shared_middleware,
                *main_only_middleware,
                summarization,
                *model_resilience,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                EmptyToolCallRetryMiddleware(),
                PatchToolCallsMiddleware(),
                *workspace_context_middleware,
            ]
            if m is not None
        ]

        # Create agent with middleware stack
        agent: Any = create_agent(
            model,
            system_prompt=system_prompt,
            tools=tools,
            middleware=deepagent_middleware,
            checkpointer=checkpointer,
            store=store,
        ).with_config({"recursion_limit": 1000})

        # Wrap with orchestrator for background execution support
        return BackgroundSubagentOrchestrator(
            agent=agent,
            middleware=background_middleware,
            auto_wait=self.config.background_auto_wait,
        )
