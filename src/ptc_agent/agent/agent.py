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

from ptc_agent.agent.backends import (
    CompositeFilesystemBackend,
    NamespaceFactory,
    RequestScopedStoreCache,
    SandboxBackend,
    StoreBackend,
)
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
    CodeValidationMiddleware,
    EmptyToolCallRetryMiddleware,
    LeakDetectionMiddleware,
    ProtectedPathMiddleware,
    ToolArgumentParsingMiddleware,
    ToolErrorHandlingMiddleware,
    ToolResultNormalizationMiddleware,
    FileOperationMiddleware,
    TodoWriteMiddleware,
    SkillsMiddleware,
    CompactionMiddleware,
    LargeResultEvictionMiddleware,
    SteeringMiddleware,
    SubagentSteeringMiddleware,
    WorkspaceContextMiddleware,
    # memory.md injection from the LangGraph store
    MemoryContextMiddleware,
    # injects <memo-index count=N path=.../>
    MemoAwarenessMiddleware,
)
from ptc_agent.core.paths import (
    MEMO_INDEX_FILENAME,
    MEMO_USER_DIR,
    MEMORY_INDEX_FILENAME,
    MEMORY_USER_DIR,
    MEMORY_WORKSPACE_DIR,
)
from ptc_agent.agent.middleware.runtime_context import RuntimeContextMiddleware
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
        memory_enabled: bool = True,
        memo_enabled: bool = True,
    ) -> str:
        """Build the static system prompt (excludes time/profile for cacheability)."""
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
            memory_enabled=memory_enabled,
            memo_enabled=memo_enabled,
        )

    def _build_model_resilience_middleware(self) -> list[Any]:
        """Append order is outermost-first: Fallback → Retry → Model. Errors propagate inward."""
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
        user_id: str | None = None,
    ) -> Any:
        """Create a deepagent with PTC pattern capabilities.

        Key non-obvious parameters:
            checkpointer: Required for submit_plan interrupt/resume workflow.
            thread_id: First 8 chars used as thread directory name under
                ``.agents/threads/{id}/``.
            user_id: First component of memory-namespace tuples. When ``None``,
                memory is disabled entirely rather than falling back to a shared
                namespace that would cross-pollinate unauthenticated sessions.
            on_agent_md_write: Invalidates the Session's agent.md cache on write.

        Returns:
            Configured BackgroundSubagentOrchestrator wrapping the deepagent.
        """
        model = llm if llm is not None else self.llm

        # Freeze current time for this request (refreshes on each new query)
        request_time = datetime.now(tz=UTC)
        timezone_str = (user_profile or {}).get("timezone")
        current_time = format_current_time(request_time, timezone_str)

        # Compute short thread ID for thread-scoped storage
        short_thread_id = thread_id[:8] if thread_id else ""

        backend = SandboxBackend(sandbox, operation_callback=operation_callback)

        # Memory is opt-in: disabled entirely when identity is missing rather
        # than falling back to a shared namespace that would cross-pollinate
        # unauthenticated sessions.
        workspace_id_for_memory = (
            getattr(session, "conversation_id", None) if session else None
        )
        user_memory_enabled = store is not None and bool(user_id)
        workspace_memory_enabled = (
            store is not None and bool(user_id) and bool(workspace_id_for_memory)
        )
        memory_enabled = user_memory_enabled or workspace_memory_enabled

        if store is not None and not memory_enabled:
            logger.warning(
                "memory disabled due to missing identity",
                user_id_present=bool(user_id),
                workspace_id_present=bool(workspace_id_for_memory),
            )

        # Memo (user-managed documents) mirrors user-tier memory: enabled
        # whenever the store is wired and we have a user identity.
        memo_enabled = store is not None and bool(user_id)

        filesystem_backend: Any = backend
        # Holds both MemoryContextMiddleware (memory.md injection) and
        # MemoAwarenessMiddleware (memo count block). Both append content
        # after the prompt-cache breakpoint, hence "dynamic context".
        dynamic_context_middleware: list[Any] = []
        # One cache per agent (≈ per request). Shared by every memory/memo
        # backend route + the two read-side middlewares so that across the
        # K model calls in a turn we pay 1 set of store reads, not K.
        # Agent-side writes invalidate the affected key so reads in later
        # rounds within the same turn see the fresh value.
        store_cache: RequestScopedStoreCache | None = (
            RequestScopedStoreCache() if (memory_enabled or memo_enabled) else None
        )
        if memory_enabled or memo_enabled:
            sandbox_root = backend.root_dir.rstrip("/")

            # INVARIANT: these closures capture identity at agent-creation
            # time. Safe only because one PTCAgent is built per request — if an
            # orchestrator ever reuses agent instances across requests, memory
            # will cross-pollinate between users. Resolve identity at call time
            # (e.g. via `langgraph.runtime.get_runtime()`) before introducing
            # reuse.
            routes: list[StoreBackend] = []
            user_namespace_factory: NamespaceFactory | None = None
            workspace_namespace_factory: NamespaceFactory | None = None
            memo_namespace_factory: NamespaceFactory | None = None

            if user_memory_enabled:
                captured_user_id = user_id
                def _user_namespace() -> tuple[str, ...]:
                    return (captured_user_id, "memory")

                user_namespace_factory = _user_namespace
                routes.append(
                    StoreBackend(
                        store=store,
                        namespace_factory=_user_namespace,
                        root_prefix=f"{sandbox_root}/{MEMORY_USER_DIR}/",
                        sandbox_backend=backend,
                        cache=store_cache,
                    )
                )

            if workspace_memory_enabled:
                captured_user_id = user_id
                captured_workspace_id = workspace_id_for_memory
                def _workspace_namespace() -> tuple[str, ...]:
                    return (
                        captured_user_id,
                        "workspaces",
                        captured_workspace_id,
                        "memory",
                    )

                workspace_namespace_factory = _workspace_namespace
                routes.append(
                    StoreBackend(
                        store=store,
                        namespace_factory=_workspace_namespace,
                        root_prefix=f"{sandbox_root}/{MEMORY_WORKSPACE_DIR}/",
                        sandbox_backend=backend,
                        cache=store_cache,
                    )
                )

            if memo_enabled:
                captured_user_id = user_id
                def _memo_namespace() -> tuple[str, ...]:
                    # Plural: avoid string-prefix collision with the
                    # ``(user_id, "memory")`` tier in AsyncPostgresStore,
                    # whose asearch is ``LIKE 'user_id.memo%'``.
                    return (captured_user_id, "memos")

                memo_namespace_factory = _memo_namespace
                routes.append(
                    StoreBackend(
                        store=store,
                        namespace_factory=_memo_namespace,
                        root_prefix=f"{sandbox_root}/{MEMO_USER_DIR}/",
                        sandbox_backend=backend,
                        read_only=True,
                        read_only_error=(
                            "Memo is user-managed. Ask the user to edit or "
                            "upload via the memo panel."
                        ),
                        cache=store_cache,
                    )
                )

            if routes:
                filesystem_backend = CompositeFilesystemBackend(
                    sandbox=backend,
                    routes=routes,
                )
                if memory_enabled:
                    dynamic_context_middleware = [
                        MemoryContextMiddleware(
                            store=store,
                            user_namespace_factory=user_namespace_factory,
                            workspace_namespace_factory=workspace_namespace_factory,
                            user_display_path=f"{MEMORY_USER_DIR}/{MEMORY_INDEX_FILENAME}",
                            workspace_display_path=f"{MEMORY_WORKSPACE_DIR}/{MEMORY_INDEX_FILENAME}",
                            index_key=MEMORY_INDEX_FILENAME,
                            cache=store_cache,
                        )
                    ]
                if memo_enabled and memo_namespace_factory is not None:
                    # Memo's count block injects after the cache breakpoint
                    # alongside memory.md, hence the shared list.
                    dynamic_context_middleware.append(
                        MemoAwarenessMiddleware(
                            store=store,
                            user_namespace_factory=memo_namespace_factory,
                            display_path=f"{MEMO_USER_DIR}/",
                            index_key=MEMO_INDEX_FILENAME,
                            cache=store_cache,
                        )
                    )

        # Create the execute_code tool for MCP invocation
        execute_code_tool = create_execute_code_tool(
            backend, mcp_registry, thread_id=short_thread_id
        )

        # Create the Bash tool for shell command execution
        bash_tool = create_execute_bash_tool(backend, thread_id=short_thread_id)
        bash_output_tool = create_bash_output_tool(backend)

        # Create the preview URL tool for sandbox service previews
        workspace_id = getattr(session, "conversation_id", "") if session else ""
        preview_url_tool = create_preview_url_tool(backend, workspace_id=workspace_id, on_signed_url=on_signed_url)

        # Create the show widget tool for inline HTML visualizations
        show_widget_tool = create_show_widget_tool(backend)

        # Start with base tools
        tools: list[Any] = [execute_code_tool, bash_tool, bash_output_tool, preview_url_tool, show_widget_tool, TodoWrite]

        # Create custom filesystem tools (override deepagents middleware tools).
        # `filesystem_backend` is the composite when a store is wired; otherwise
        # it's the plain sandbox backend. Tools see a uniform rich-method
        # surface either way.
        read_file, write_file, edit_file = create_filesystem_tools(
            filesystem_backend,
            operation_callback=operation_callback,
        )
        filesystem_tools = [
            read_file,  # overrides middleware read_file
            write_file,  # overrides middleware write_file
            edit_file,  # overrides middleware edit_file
            create_glob_tool(filesystem_backend),  # overrides middleware glob
            create_grep_tool(filesystem_backend),  # overrides middleware grep
        ]
        tools.extend(filesystem_tools)

        web_search_tool = get_web_search_tool(
            max_search_results=10,
            time_range=None,
            verbose=False,
        )
        tools.append(web_search_tool)
        tools.append(web_fetch_tool)

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

        if subagent_names is None:
            subagent_names = self.config.subagents.enabled

        # --- Build shared middleware (for both main agent and subagents) ---
        shared_middleware: list[Any] = []

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

        shared_middleware.append(
            FileOperationMiddleware(
                on_agent_md_write=on_agent_md_write,
                work_dir=self.config.filesystem.working_directory,
            )
        )
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
        if backend.skills_manifest and backend.skills_manifest.get("skills"):
            known_skills = {
                name: SkillMetadata(**meta)
                for name, meta in backend.skills_manifest["skills"].items()
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

        ask_user_middleware = AskUserMiddleware()
        main_only_middleware.append(ask_user_middleware)
        tools.extend(ask_user_middleware.tools)

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

        tool_summary = self._get_tool_summary(mcp_registry)
        subagent_summary = format_subagent_summary(subagents)

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
            memory_enabled=memory_enabled,
            memo_enabled=memo_enabled,
        )

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
            ]
            if m is not None
        ]

        # Workspace context middleware (agent.md injection — main agent only)
        workspace_context_middleware: list[Any] = []
        if session is not None:
            workspace_context_middleware = [WorkspaceContextMiddleware(session=session)]

        # Memory context middleware (memory.md injection from LangGraph store).
        # Built alongside the composite backend above; empty list when store is
        # None so dev/MemorySaver runs skip the middleware entirely.
        #
        # Positioned next to workspace_context_middleware — both run innermost
        # (after the prompt-cache breakpoint) so dynamic content doesn't
        # invalidate the cached prefix.

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
                *dynamic_context_middleware,
                *runtime_context_middleware,
            ]
            if m is not None
        ]

        agent: Any = create_agent(
            model,
            system_prompt=system_prompt,
            tools=tools,
            middleware=deepagent_middleware,
            checkpointer=checkpointer,
            store=store,
        ).with_config({"recursion_limit": 2000})

        return BackgroundSubagentOrchestrator(
            agent=agent,
            middleware=background_middleware,
            auto_wait=self.config.background_auto_wait,
        )
