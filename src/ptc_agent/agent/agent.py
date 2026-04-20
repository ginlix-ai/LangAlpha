"""PTC Agent - Main agent using create_agent with Programmatic Tool Calling pattern.

This module creates a PTC agent that:
- Uses langchain's create_agent with custom middleware stack
- Integrates sandbox via SandboxBackend
- Provides MCP tools through execute_code
- Supports sub-agent delegation for specialized tasks
"""

from datetime import UTC, datetime
from typing import Any

import structlog
from langchain.agents import create_agent

from ptc_agent.agent.backends import SandboxBackend
from ptc_agent.agent.middleware import SubAgentMiddleware
from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware

from ptc_agent.agent.middleware import (
    AskUserMiddleware,
    BackgroundSubagentMiddleware,
    BackgroundSubagentOrchestrator,
    PlanModeMiddleware,
    SubagentEventCaptureMiddleware,
    MultimodalMiddleware,
    create_plan_mode_interrupt_config,
    # Tool middleware
    CodeValidationMiddleware,
    EmptyToolCallRetryMiddleware,
    LeakDetectionMiddleware,
    ProtectedPathMiddleware,
    ToolArgumentParsingMiddleware,
    ToolErrorHandlingMiddleware,
    ToolResultNormalizationMiddleware,
    # File operations SSE middleware
    FileOperationMiddleware,
    # Todo operations SSE middleware
    TodoWriteMiddleware,
    # Skills middleware
    SkillsMiddleware,
    # Compaction middleware
    CompactionMiddleware,
    # Large result eviction middleware
    LargeResultEvictionMiddleware,
    # Steering middleware
    SteeringMiddleware,
    # Subagent steering middleware
    SubagentSteeringMiddleware,
    # Workspace context middleware
    WorkspaceContextMiddleware,
)
from ptc_agent.agent.middleware.runtime_context import RuntimeContextMiddleware
from ptc_agent.agent.middleware.anthropic_thinking_sanitizer import (
    AnthropicThinkingSanitizerMiddleware,
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
    create_bash_output_tool,
    create_execute_bash_tool,
    create_execute_code_tool,
    create_filesystem_tools,
    create_glob_tool,
    create_grep_tool,
    create_preview_url_tool,
    create_show_widget_tool,
    TodoWrite,
)
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
from ptc_agent.config import AgentConfig
from ptc_agent.core.mcp_registry import MCPRegistry
from ptc_agent.core.sandbox import PTCSandbox

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
    - Integrates sandbox via SandboxBackend
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

    def _build_system_prompt(
        self,
        tool_summary: str,
        subagent_summary: str,
        plan_mode: bool = False,
        thread_id: str | None = None,
    ) -> str:
        """Build the static system prompt (excludes time/profile for cacheability).

        Args:
            tool_summary: Formatted MCP tool summary
            subagent_summary: Formatted subagent summary
            plan_mode: If True, includes plan mode workflow instructions
            thread_id: Optional thread ID (first 8 chars) for thread-scoped directories

        Returns:
            Complete system prompt
        """
        loader = get_loader()

        return loader.get_system_prompt(
            tool_summary=tool_summary,
            subagent_summary=subagent_summary,
            max_concurrent_task_units=DEFAULT_MAX_CONCURRENT_TASK_UNITS,
            max_task_iterations=DEFAULT_MAX_TASK_ITERATIONS,
            ask_user_enabled=True,
            plan_mode=plan_mode,
            include_examples=True,
            include_anti_patterns=True,
            thread_id=thread_id or "",
            working_directory=self.config.filesystem.working_directory,
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
            # Use pre-resolved clients (OAuth/BYOK-aware) when available
            if self.config.fallback_llm_clients:
                fallback_instances = self.config.fallback_llm_clients
            else:
                from src.llms import get_llm_by_type
                fallback_instances = [
                    get_llm_by_type(name) for name in self.config.llm.fallback
                ]
            middleware.append(ModelFallbackMiddleware(*fallback_instances))
            logger.debug(
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
        on_signed_url: Any | None = None,
        vault_secrets: dict[str, str] | None = None,
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
                First 8 chars used as thread directory name in .agents/threads/{id}/.
            on_agent_md_write: Optional callback invoked when agent.md is written/edited.
                Used to invalidate Session's agent.md cache.
            on_signed_url: Optional async callback(sandbox_id, port, url) to cache
                signed preview URLs. Injected from the server layer.

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
        bash_output_tool = create_bash_output_tool(sandbox)

        # Create the preview URL tool for sandbox service previews
        workspace_id = getattr(session, "conversation_id", "") if session else ""
        preview_url_tool = create_preview_url_tool(sandbox, workspace_id=workspace_id, on_signed_url=on_signed_url)

        # Create the show widget tool for inline HTML visualizations
        show_widget_tool = create_show_widget_tool(sandbox)

        # Start with base tools
        tools: list[Any] = [execute_code_tool, bash_tool, bash_output_tool, preview_url_tool, show_widget_tool, TodoWrite]

        # Create backend for SkillsMiddleware and LargeResultEvictionMiddleware
        backend = SandboxBackend(sandbox, operation_callback=operation_callback)

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
            get_options_chain,  # Options contracts chain with snapshot pricing
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
                ProtectedPathMiddleware(
                    denied_directories=self.config.filesystem.denied_directories,
                ),
                CodeValidationMiddleware(),
                ToolErrorHandlingMiddleware(),
                LeakDetectionMiddleware(
                    mcp_servers=self.config.mcp.servers,
                    vault_secrets=vault_secrets,
                ),
                ToolResultNormalizationMiddleware(),
            ]
        )

        # File operation SSE middleware - emits events for write_file/edit_file
        shared_middleware.append(
            FileOperationMiddleware(
                on_agent_md_write=on_agent_md_write,
                work_dir=self.config.filesystem.working_directory,
            )
        )

        # Todo operation SSE middleware - emits events for TodoWrite
        shared_middleware.append(TodoWriteMiddleware())

        # Add multimodal middleware for read_file image/PDF support (when enabled)
        if self.config.enable_view_image and self.config.llm:
            shared_middleware.append(MultimodalMiddleware(
                sandbox=sandbox,
                model_name=self.config.llm.name,
                custom_modalities=self.config.input_modalities,
            ))

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

        # Steering middleware - checks Redis for steering messages from the user
        # before each LLM call (must be first so steering context is visible
        # before any other middleware runs)
        main_only_middleware.append(SteeringMiddleware())

        # Create event capture middleware for subagent streaming and metrics
        # (Created early so it can be passed to BackgroundSubagentMiddleware)
        _bg_registry = background_registry or BackgroundTaskRegistry()
        event_capture_middleware = SubagentEventCaptureMiddleware(registry=_bg_registry)

        # Create background subagent middleware (must be created before subagents)
        background_middleware = BackgroundSubagentMiddleware(
            timeout=background_timeout,
            enabled=True,
            registry=_bg_registry,
            event_capture_middleware=event_capture_middleware,
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
            event_capture_middleware=event_capture_middleware,
        )

        if additional_subagents:
            subagents.extend(additional_subagents)

        # Get tool summary for system prompt
        tool_summary = self._get_tool_summary(mcp_registry)

        # Build subagent summary for system prompt
        subagent_summary = format_subagent_summary(subagents)

        # Build system prompt and eviction dir (short_thread_id computed earlier)
        eviction_dir = (
            f".agents/threads/{short_thread_id}/large_tool_results"
            if short_thread_id
            else ".agents/large_tool_results"
        )
        system_prompt = self._build_system_prompt(
            tool_summary,
            subagent_summary,
            plan_mode=plan_mode,
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

        logger.debug(
            "Creating agent with custom middleware stack",
            tool_count=len(tools),
            subagent_count=len(subagents),
            skills_enabled=self.config.skills.enabled,
        )

        # --- Build final middleware stacks ---

        # Custom SSE-enabled compaction emits 'context_window' events
        # Pass backend for offloading conversation history to sandbox
        compaction_config = self.config.compaction.model_dump()
        if self.config.llm and self.config.llm.compaction:
            compaction_config["llm"] = self.config.llm.compaction
            compaction_client = self.config.subsidiary_llm_clients.get("compaction")
            if compaction_client:
                compaction_config["_llm_client"] = compaction_client
            elif self.config.llm_client:
                # Deep-copy so CompactionMiddleware.from_config() can set
                # streaming=False without mutating the main agent's model.
                compaction_config["_llm_client"] = self.config.llm_client.model_copy()
        elif self.config.llm_client:
            # No dedicated compaction model; fall back to main agent LLM
            compaction_config["_llm_client"] = self.config.llm_client.model_copy()
        compaction = CompactionMiddleware.from_config(config=compaction_config, backend=backend)

        # Build model resilience middleware (retry + fallback)
        model_resilience = self._build_model_resilience_middleware()

        # Subagent middleware (shared only, no SubAgentMiddleware/BackgroundSubagentMiddleware/HITL)
        # SubagentSteeringMiddleware is first so follow-up messages are
        # visible before any other middleware runs.
        subagent_middleware = [
            m
            for m in [
                SubagentSteeringMiddleware(registry=background_middleware.registry),
                LargeResultEvictionMiddleware(
                    backend=backend, eviction_dir=eviction_dir
                ),
                *shared_middleware,
                compaction,
                *model_resilience,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                EmptyToolCallRetryMiddleware(),
                PatchToolCallsMiddleware(),
                AnthropicThinkingSanitizerMiddleware(),
            ]
            if m is not None
        ]

        # Workspace context middleware (agent.md injection — main agent only)
        workspace_context_middleware: list[Any] = []
        if session is not None:
            workspace_context_middleware = [WorkspaceContextMiddleware(session=session)]

        # Runtime context middleware (time + user profile — after cache breakpoint)
        runtime_context_middleware: list[Any] = [
            RuntimeContextMiddleware(
                current_time=current_time,
                user_profile=user_profile,
            )
        ]

        # Main agent middleware (includes SubAgentMiddleware + main_only)
        # Ordering matters for prompt caching:
        #   - AnthropicPromptCachingMiddleware places cache_control breakpoint on
        #     the last system message block it sees (the static prompt + skills).
        #   - WorkspaceContextMiddleware (agent.md) and RuntimeContextMiddleware
        #     (time + profile) are innermost — they append AFTER the breakpoint,
        #     so dynamic content doesn't invalidate the cached prefix.
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
                compaction,
                *model_resilience,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                EmptyToolCallRetryMiddleware(),
                PatchToolCallsMiddleware(),
                *workspace_context_middleware,
                *runtime_context_middleware,
                AnthropicThinkingSanitizerMiddleware(),
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
        ).with_config({"recursion_limit": 2000})

        # Wrap with orchestrator for background execution support
        return BackgroundSubagentOrchestrator(
            agent=agent,
            middleware=background_middleware,
            auto_wait=self.config.background_auto_wait,
        )
