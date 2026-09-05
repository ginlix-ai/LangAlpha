"""PTC Agent - Main agent using create_agent with Programmatic Tool Calling pattern.

This module creates a PTC agent that:
- Uses langchain's create_agent with custom middleware stack
- Integrates sandbox via SandboxBackend
- Provides MCP tools through execute_code
- Supports sub-agent delegation for specialized tasks
"""

from dataclasses import dataclass
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
    WorkflowsBackend,
    prebuilt_workflow_backend,
    workflow_namespace,
)
from ptc_agent.agent.middleware import SubAgentMiddleware
from ptc_agent.agent.state import DeltaAgentState
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
    CreditGateMiddleware,
    EmptyToolCallRetryMiddleware,
    LeakDetectionMiddleware,
    ProtectedPathMiddleware,
    ToolArgumentParsingMiddleware,
    ToolErrorHandlingMiddleware,
    ToolResultNormalizationMiddleware,
    FileOperationMiddleware,
    TodoWriteMiddleware,
    ProvenanceMiddleware,
    SkillsMiddleware,
    CompactionMiddleware,
    resolve_compaction_client,
    LargeResultEvictionMiddleware,
    MarketWatchMiddleware,
    SteeringMiddleware,
    SubagentSteeringMiddleware,
    WorkspaceContextMiddleware,
    # memory.md injection from the LangGraph store
    MemoryContextMiddleware,
    # injects <memo-index count=N path=.../>
    MemoAwarenessMiddleware,
    ReasoningCompatibilityMiddleware,
)
from ptc_agent.core.paths import (
    MEMO_INDEX_FILENAME,
    MEMO_USER_DIR,
    MEMORY_INDEX_FILENAME,
    MEMORY_USER_DIR,
    MEMORY_WORKSPACE_DIR,
    USER_PROFILE_DATA_DIR,
    WORKFLOW_DIR,
)
from ptc_agent.agent.middleware.background_subagent.workflow.prebuilt import (
    get_prebuilt_workflows,
)
from ptc_agent.agent.backends.user_data import UserDataBackend
from ptc_agent.agent.middleware.image_capture import ImageCaptureMiddleware
from ptc_agent.agent.middleware.openai_prompt_caching import OpenAIPromptCachingMiddleware
from ptc_agent.agent.middleware.runtime_context import RuntimeContextMiddleware
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.skills.discovery import SkillMetadata
from ptc_agent.agent.middleware.skills.registry import (
    build_effective_skill_registry,
)
from ptc_agent.agent.prompts import (
    build_tool_summary_from_registry,
    format_current_time,
    format_subagent_summary,
    get_loader,
    guidance_template_vars,
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
from src.tools.web.search import get_web_search_tool
from src.tools.web.fetch import web_fetch_tool
from src.tools.web.crawl import create_crawl_tools
from src.tools.sec.tool import get_sec_filing
from src.tools.market_data.tool import (
    get_daily_prices,
    get_company_overview,
    get_market_overview,
    get_options_chain,
    get_quote,
    screen_stocks,
)
from src.tools.market_watch import watch_market
from ptc_agent.config import AgentConfig
from ptc_agent.core.mcp_registry import MCPRegistry
from ptc_agent.core.sandbox import PTCSandbox

try:
    from langchain.agents.middleware import HumanInTheLoopMiddleware
except ImportError:
    HumanInTheLoopMiddleware = None  # type: ignore[misc,assignment]

from ptc_agent.agent.turn import build_model_resilience_middleware, turn_model

try:
    from langgraph.types import Checkpointer
except ImportError:
    Checkpointer = None  # type: ignore[misc,assignment]

logger = structlog.get_logger(__name__)


DEFAULT_MAX_CONCURRENT_TASK_UNITS = 3


@dataclass(frozen=True)
class _IdentityGates:
    """Which identity-derived surfaces a build gets.

    Memory, memo and the workflow store are opt-in on identity: without a user
    id they are disabled entirely rather than falling back to a shared
    namespace that would cross-pollinate unauthenticated sessions.
    """

    user_memory: bool
    workspace_memory: bool
    memo: bool
    user_data: bool
    workflow: bool
    workflow_fs: bool
    workflow_tool: bool

    @property
    def memory(self) -> bool:
        return self.user_memory or self.workspace_memory


def _resolve_identity_gates(
    *,
    store: Any | None,
    user_id: str | None,
    workspace_id: str | None,
    disable_subagents: bool,
) -> _IdentityGates:
    from src.config.settings import get_workflow_orchestration_config

    workflow = get_workflow_orchestration_config().enabled
    identified = store is not None and bool(user_id)
    return _IdentityGates(
        user_memory=identified,
        workspace_memory=identified and bool(workspace_id),
        memo=identified,
        # Independent of `store`: the user-profile data backend (portfolio +
        # watchlist + preferences) talks to the application DB tables, not the
        # LangGraph store.
        user_data=bool(user_id),
        workflow=workflow,
        workflow_fs=workflow and identified,
        # RunWorkflow dispatches subagents, so it drops with the recursion
        # gate. The skill that advertises it is gated on the same flag —
        # advertising a skill whose tool this build never registers strands
        # the agent.
        workflow_tool=workflow and not disable_subagents,
    )


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

    def _build_system_prompt(
        self,
        tool_summary: str,
        subagent_summary: str,
        guidance: str,
        plan_mode: bool = False,
        thread_id: str | None = None,
        memory_enabled: bool = True,
        memo_enabled: bool = True,
        crawl_enabled: bool = False,
    ) -> str:
        """Build the static system prompt (excludes time/profile for cacheability).

        ``guidance`` shapes the cached prefix, so the prefix varies by (model,
        guidance) rather than (model), which only splits when a user pins the
        level themselves.
        """
        loader = get_loader()
        return loader.get_system_prompt(
            **guidance_template_vars(guidance),
            tool_summary=tool_summary,
            subagent_summary=subagent_summary,
            max_concurrent_task_units=DEFAULT_MAX_CONCURRENT_TASK_UNITS,
            ask_user_enabled=True,
            plan_mode=plan_mode,
            include_examples=True,
            include_anti_patterns=True,
            thread_id=thread_id or "",
            working_directory=self.config.filesystem.working_directory,
            memory_enabled=memory_enabled,
            memo_enabled=memo_enabled,
            market_watch_enabled=self.config.feature_enabled("market_watch"),
            crawl_enabled=crawl_enabled,
        )

    def _get_tool_summary(self, mcp_registry: MCPRegistry) -> str:
        return build_tool_summary_from_registry(
            mcp_registry, mode=self.config.mcp.tool_exposure_mode
        )

    def _build_filesystem_backend(
        self,
        *,
        backend: Any,
        gates: _IdentityGates,
        store: Any | None,
        user_id: str | None,
        workspace_id: str | None,
    ) -> tuple[Any, list[Any]]:
        """Mount the store-backed routes over the sandbox filesystem.

        Returns the backend the filesystem tools should see, and the middleware
        that injects those routes' content after the prompt-cache breakpoint
        (memory.md, the memo count block) — one list because they share that
        position.
        """
        if not (gates.memory or gates.memo or gates.user_data or gates.workflow):
            return backend, []

        # One cache per agent (≈ per request). Shared by every memory/memo
        # backend route + the two read-side middlewares so that across the
        # K model calls in a turn we pay 1 set of store reads, not K.
        # Agent-side writes invalidate the affected key so reads in later
        # rounds within the same turn see the fresh value.
        store_cache: RequestScopedStoreCache | None = (
            RequestScopedStoreCache()
            if (gates.memory or gates.memo or gates.workflow_fs)
            else None
        )
        sandbox_root = backend.root_dir.rstrip("/")

        # INVARIANT: these closures capture identity at agent-creation time
        # (``user_id`` is bound once per call). Safe only because one PTCAgent
        # is built per request — if an orchestrator ever reuses agent instances
        # across requests, memory will cross-pollinate between users. Resolve
        # identity at call time (e.g. via `langgraph.runtime.get_runtime()`)
        # before introducing reuse.
        routes: list[Any] = []
        user_namespace_factory: NamespaceFactory | None = None
        workspace_namespace_factory: NamespaceFactory | None = None
        memo_namespace_factory: NamespaceFactory | None = None

        if gates.user_memory:

            def _user_namespace() -> tuple[str, ...]:
                return (user_id, "memory")

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

        if gates.workspace_memory:

            def _workspace_namespace() -> tuple[str, ...]:
                return (user_id, "workspaces", workspace_id, "memory")

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

        if gates.memo:

            def _memo_namespace() -> tuple[str, ...]:
                # Plural: avoid string-prefix collision with the
                # ``(user_id, "memory")`` tier in AsyncPostgresStore,
                # whose asearch is ``LIKE 'user_id.memo%'``.
                return (user_id, "memos")

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

        if gates.workflow:
            workflow_root = f"{sandbox_root}/{WORKFLOW_DIR}/"
            prebuilt_route = prebuilt_workflow_backend(
                files=get_prebuilt_workflows().files(),
                root_prefix=workflow_root,
                sandbox_backend=backend,
            )
            if gates.workflow_fs:

                def _workflow_namespace() -> tuple[str, ...]:
                    return workflow_namespace(user_id)

                routes.append(
                    WorkflowsBackend(
                        store_backend=StoreBackend(
                            store=store,
                            namespace_factory=_workflow_namespace,
                            root_prefix=workflow_root,
                            sandbox_backend=backend,
                            cache=store_cache,
                        ),
                        prebuilt_backend=prebuilt_route,
                    )
                )
            else:
                routes.append(prebuilt_route)

        if gates.user_data:
            routes.append(
                UserDataBackend(
                    user_id=user_id,
                    sandbox_backend=backend,
                    root_prefix=f"{sandbox_root}/{USER_PROFILE_DATA_DIR}/",
                )
            )

        if not routes:
            return backend, []

        dynamic_context_middleware: list[Any] = []
        if gates.memory:
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
        if gates.memo and memo_namespace_factory is not None:
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

        return (
            CompositeFilesystemBackend(sandbox=backend, routes=routes),
            dynamic_context_middleware,
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
        namespace_owner: Any | None = None,
        user_profile: dict | None = None,
        plan_mode: bool = False,
        thread_id: str | None = None,
        workspace_name: str = "",
        workspace_description: str = "",
        on_agent_md_write: Any | None = None,
        store: Any | None = None,
        on_signed_url: Any | None = None,
        vault_secrets: dict[str, str] | None = None,
        user_id: str | None = None,
        user_data_counts: dict[str, Any] | None = None,
        tool_summary: str | None = None,
        disable_subagents: bool = False,
    ) -> Any:
        """Create a deepagent with PTC pattern capabilities.

        Key non-obvious parameters:
            checkpointer: Required for submit_plan interrupt/resume workflow.
            disable_subagents: Build the agent WITHOUT the subagent machinery
                (no Task/TaskOutput tools, no SubAgentMiddleware) — the
                structural recursion gate for synthetic notification turns.
            namespace_owner: Writer fence for background-subagent checkpoint
                namespaces (acquire_task_ns/release_task_ns, e.g. the run's
                WriterGuard). None = no fence (single-writer deployment).
            thread_id: First 8 chars used as thread directory name under
                ``.agents/threads/{id}/``.
            user_id: First component of memory-namespace tuples. When ``None``,
                memory is disabled entirely rather than falling back to a shared
                namespace that would cross-pollinate unauthenticated sessions.
            on_agent_md_write: Invalidates the Session's agent.md cache on write.

        Returns:
            Configured BackgroundSubagentOrchestrator wrapping the deepagent.
        """
        turn = turn_model(self.config, llm, self.llm, flash=False)

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
        gates = _resolve_identity_gates(
            store=store,
            user_id=user_id,
            workspace_id=workspace_id_for_memory,
            disable_subagents=disable_subagents,
        )
        if store is not None and not gates.memory:
            logger.warning(
                "memory disabled due to missing identity",
                user_id_present=bool(user_id),
                workspace_id_present=bool(workspace_id_for_memory),
            )

        filesystem_backend, dynamic_context_middleware = (
            self._build_filesystem_backend(
                backend=backend,
                gates=gates,
                store=store,
                user_id=user_id,
                workspace_id=workspace_id_for_memory,
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
            provider=self.config.search_api,
            depth=self.config.search_depth,
        )
        tools.append(web_search_tool)
        tools.append(web_fetch_tool)

        # Site-crawl tools (PTC-only): experimental opt-in feature, further
        # tier-gated at resolve time. The factory returns [] when the crawl
        # provider's API key is unset.
        crawl_tools: list[Any] = []
        if self.config.feature_enabled("site_crawl"):
            crawl_tools = create_crawl_tools(filesystem_backend)
            tools.extend(crawl_tools)

        finance_tools = [
            get_sec_filing,  # SEC filing extraction (10-K, 10-Q, 8-K)
            get_quote,  # Real-time quotes (cheap — price freshness)
            get_daily_prices,  # Stock OHLCV price data
            get_company_overview,  # Company investment analysis (includes real-time quote)
            get_market_overview,  # Single-day market snapshot (indices + US sectors)
            get_options_chain,  # Options contracts chain with snapshot pricing
            screen_stocks,  # Stock screener with filters
        ]
        if self.config.feature_enabled("market_watch"):
            finance_tools.append(watch_market)  # Market watch start/stop (live price injection)
        tools.extend(finance_tools)

        if subagent_names is None:
            subagent_names = self.config.subagents.enabled

        # --- Build shared middleware (for both main agent and subagents) ---
        shared_middleware: list[Any] = []

        leak_detection = LeakDetectionMiddleware(
            mcp_servers=self.config.mcp.servers,
            vault_secrets=vault_secrets,
        )
        shared_middleware.extend(
            [
                # First so a stop fires before any per-boundary work below;
                # shared placement gives subagent lanes the same gate. Inert
                # unless the server installed a gate state for the lane.
                CreditGateMiddleware(),
                ToolArgumentParsingMiddleware(),
                ProtectedPathMiddleware(
                    denied_directories=self.config.filesystem.denied_directories,
                ),
                CodeValidationMiddleware(),
                ToolErrorHandlingMiddleware(),
                leak_detection,
                ToolResultNormalizationMiddleware(),
            ]
        )

        shared_middleware.append(
            FileOperationMiddleware(
                on_agent_md_write=on_agent_md_write,
                work_dir=self.config.filesystem.working_directory,
            )
        )
        # Shared placement gives subagents provenance coverage too. The leak
        # detector's redactor scrubs secrets from snippets the content-only scan
        # never sees (provenance fingerprints the raw result/artifact).
        shared_middleware.append(ProvenanceMiddleware(redactor=leak_detection.redact))
        shared_middleware.append(TodoWriteMiddleware())

        skill_sources = (
            [f"{self.config.skills.sandbox_skills_base}/"]
            if self.config.skills.enabled
            else []
        )

        known_skills: dict[str, Any] = {}
        if backend.skills_manifest and backend.skills_manifest.get("skills"):
            known_skills = {
                name: SkillMetadata(**meta)
                for name, meta in backend.skills_manifest["skills"].items()
            }

        # Per-user registry: this build's feature gate (the registry default
        # only applies the system gate), builtin disables, and user skills.
        skill_registry = build_effective_skill_registry(
            "ptc",
            feature_resolver=self.config.feature_enabled,
            disabled_skills=self.config.disabled_skills,
            user_skills=self.config.user_skills,
            user_skill_dir=self.config.user_skill_dir,
            workspace_skill_dir=self.config.workspace_skill_dir,
        )
        # RunWorkflow is skill-gated: the run-workflow skill hides the tool from
        # model requests until the agent reads its SKILL.md. Drop the skill on
        # any build that registers no tool for it to gate.
        if not gates.workflow_tool:
            skill_registry.pop("run-workflow", None)

        skill_loader_middleware = SkillsMiddleware(
            skill_registry=skill_registry,
            mode="ptc",
            backend=backend,
            sources=skill_sources,
            known_skills=known_skills,
            skill_dirs=[
                d for d, _ in self.config.skills.local_skill_dirs_with_sandbox()
            ],
            disabled_skills=self.config.disabled_skills,
        )
        shared_middleware.append(skill_loader_middleware)
        tools.extend(skill_loader_middleware.tools)
        tools.extend(skill_loader_middleware.get_all_skill_tools())

        # --- Build main-only middleware (NOT passed to subagents) ---
        main_only_middleware: list[Any] = []

        # Must be first: steering context must be visible before any other middleware.
        main_only_middleware.append(SteeringMiddleware())

        _bg_registry = background_registry or BackgroundTaskRegistry()
        event_capture_middleware = SubagentEventCaptureMiddleware(registry=_bg_registry)

        background_middleware = BackgroundSubagentMiddleware(
            timeout=background_timeout,
            enabled=not disable_subagents,
            registry=_bg_registry,
            checkpointer=checkpointer,
            namespace_owner=namespace_owner,
        )
        main_only_middleware.append(background_middleware)
        if not disable_subagents:
            tools.extend(background_middleware.tools)

        if HumanInTheLoopMiddleware is not None:
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
        # The compiler gets its own registry: same per-user gates, but
        # mode-unfiltered — subagent definitions may be flash-mode and preload
        # flash-only skills the ptc-filtered registry above excludes.
        subagent_compiler = SubagentCompiler(
            sandbox=sandbox,
            mcp_registry=mcp_registry,
            tool_sets=subagent_tool_sets,
            user_profile=user_profile,
            current_time=current_time,
            thread_id=short_thread_id,
            config=self.config,
            skill_registry=build_effective_skill_registry(
                None,
                feature_resolver=self.config.feature_enabled,
                disabled_skills=self.config.disabled_skills,
                user_skills=self.config.user_skills,
                user_skill_dir=self.config.user_skill_dir,
                workspace_skill_dir=self.config.workspace_skill_dir,
            ),
            skill_dirs=[
                d for d, _ in self.config.skills.local_skill_dirs_with_sandbox()
            ],
        )
        if disable_subagents:
            # Recursion gate: no subagents compiled, none advertised in the
            # prompt ("No sub-agents configured."), no Task tool below.
            subagents = []
        else:
            subagents = create_subagents(
                registry=subagent_registry,
                enabled_names=subagent_names,
                compiler=subagent_compiler,
                event_capture_middleware=event_capture_middleware,
            )
            if additional_subagents:
                subagents.extend(additional_subagents)

        # Prefer the session-cached summary (precomputed once per session in the
        # WorkspaceManager) so the hot path never recomputes it — that's what
        # keeps the prompt-cache prefix byte-stable per turn. Fall back to
        # computing from the registry for callers without a cached summary
        # (tests, the SessionProvider path).
        if tool_summary is None:
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
            turn.guidance,
            plan_mode=plan_mode,
            thread_id=short_thread_id,
            memory_enabled=gates.memory,
            memo_enabled=gates.memo,
            crawl_enabled=bool(crawl_tools),
        )

        logger.debug(
            "Creating agent with custom middleware stack",
            tool_count=len(tools),
            subagent_count=len(subagents),
            skills_enabled=self.config.skills.enabled,
        )

        # --- Build final middleware stacks ---
        compaction_config = self.config.compaction.model_dump()
        if self.config.llm and self.config.llm.compaction:
            compaction_config["llm"] = self.config.llm.compaction
        client = resolve_compaction_client(self.config)
        if client is not None:
            compaction_config["_llm_client"] = client
        compaction = CompactionMiddleware.from_config(config=compaction_config, backend=backend)

        model_resilience = [build_model_resilience_middleware(self.config, turn)]

        # Inside model_resilience so it strips against the post-fallback model:
        # a vision primary falling back to a text-only candidate would otherwise
        # replay image/PDF blocks and earn the 400 the fallback exists to avoid.
        # In both stacks because a subagent on its own model needs the same
        # protection; it reads that model off each request, so the two stacks
        # can share one instance rather than needing one apiece.
        multimodal = (
            MultimodalMiddleware(
                sandbox=sandbox,
                model_name=self.config.llm.name,
                custom_modalities=self.config.input_modalities,
            )
            if self.config.llm
            else None
        )

        # Placed before (outer to) model_resilience so sandbox images are
        # captured once, on the final response only — not per retry attempt.
        image_capture = (
            ImageCaptureMiddleware(session=session) if session is not None else None
        )

        # SubagentSteeringMiddleware must be first so follow-up messages are visible before other middleware.
        subagent_middleware = [
            m
            for m in [
                SubagentSteeringMiddleware(registry=background_middleware.registry),
                LargeResultEvictionMiddleware(
                    backend=backend, eviction_dir=eviction_dir
                ),
                *shared_middleware,
                image_capture,
                compaction,
                *model_resilience,
                multimodal,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                OpenAIPromptCachingMiddleware(),
                EmptyToolCallRetryMiddleware(),
                PatchToolCallsMiddleware(),
                ReasoningCompatibilityMiddleware(),
            ]
            if m is not None
        ]

        # Workspace context middleware (agent.md injection — main agent only)
        workspace_context_middleware: list[Any] = []
        if session is not None:
            workspace_context_middleware = [
                WorkspaceContextMiddleware(
                    session=session,
                    name=workspace_name,
                    description=workspace_description,
                )
            ]

        # Positioned after the prompt-cache breakpoint (innermost) so dynamic
        # content doesn't invalidate the cached prefix.
        runtime_context_middleware: list[Any] = [
            RuntimeContextMiddleware(
                current_time=current_time,
                user_profile=user_profile,
                user_data_counts=user_data_counts,
                sandbox_enabled=True,
            )
        ]

        # Compiled subagent graphs live on this middleware; the RunWorkflow
        # dispatcher below shares the same instances so direct dispatches get
        # identical model resolution and middleware wiring. default_tools is
        # snapshotted so appending RunWorkflow to the main tools afterwards
        # can never leak it into subagents. Absent entirely under the
        # recursion gate: this middleware is the sole provider of the Task
        # tool, so skipping it (plus the TaskOutput extend above) makes a
        # notification turn structurally unable to spawn subagents —
        # RunWorkflow drops with it below.
        subagent_task_middleware = (
            SubAgentMiddleware(
                default_model=turn.client,
                default_tools=list(tools),
                subagents=subagents if subagents else [],
                default_middleware=subagent_middleware,
                registry=background_middleware.registry,
                checkpointer=checkpointer,
            )
            if not disable_subagents
            else None
        )

        # RunWorkflow: programmatic subagent orchestration (main agent only —
        # deliberately absent from subagent tool sets, so children can't nest
        # workflows).
        if gates.workflow_tool and subagent_task_middleware is not None:
            from ptc_agent.agent.middleware.background_subagent.dispatch import (
                SubagentDispatcher,
            )
            from ptc_agent.agent.middleware.background_subagent.workflow import (
                create_run_workflow_tool,
            )

            subagent_dispatcher = SubagentDispatcher(
                background_middleware,
                subagent_task_middleware.subagent_graphs,
                thread_id or "",
            )
            run_workflow_tool = create_run_workflow_tool(
                dispatcher=subagent_dispatcher,
                backend=filesystem_backend,
                thread_id=thread_id or "",
                short_thread_id=short_thread_id,
                store=store,
                user_id=user_id,
                prebuilt_workflows=get_prebuilt_workflows(),
            )
            tools.append(run_workflow_tool)

        # Main agent middleware (includes SubAgentMiddleware + main_only)
        # Ordering matters for prompt caching:
        #   - AnthropicPromptCachingMiddleware (cache_control) and
        #     OpenAIPromptCachingMiddleware (prompt_cache_breakpoint) each place
        #     their provider's breakpoint on the last system message block they
        #     see (the static prompt + skills); each no-ops for other providers.
        #   - WorkspaceContextMiddleware (agent.md) and RuntimeContextMiddleware
        #     (time + profile) are innermost — they append AFTER the breakpoint,
        #     so dynamic content doesn't invalidate the cached prefix.
        deepagent_middleware = [
            m
            for m in [
                LargeResultEvictionMiddleware(
                    backend=backend, eviction_dir=eviction_dir
                ),
                subagent_task_middleware,
                *shared_middleware,
                *main_only_middleware,
                image_capture,
                compaction,
                *model_resilience,
                multimodal,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                OpenAIPromptCachingMiddleware(),
                # Market watch (main agent only): appends the ephemeral
                # <market-watch> price stamp. Inside model_resilience so it
                # sees the post-fallback model — its provider-specific cache
                # breakpoints (Anthropic cache_control / OpenAI
                # prompt_cache_breakpoint) must never reach another provider.
                *(
                    [MarketWatchMiddleware()]
                    if self.config.feature_enabled("market_watch")
                    else []
                ),
                EmptyToolCallRetryMiddleware(),
                PatchToolCallsMiddleware(),
                *workspace_context_middleware,
                *dynamic_context_middleware,
                *runtime_context_middleware,
                ReasoningCompatibilityMiddleware(),
            ]
            if m is not None
        ]

        agent: Any = create_agent(
            turn.client,
            system_prompt=system_prompt,
            tools=tools,
            middleware=deepagent_middleware,
            checkpointer=checkpointer,
            store=store,
            state_schema=DeltaAgentState,
        ).with_config({"recursion_limit": 2000})

        return BackgroundSubagentOrchestrator(
            agent=agent,
            middleware=background_middleware,
            auto_wait=self.config.background_auto_wait,
        )
