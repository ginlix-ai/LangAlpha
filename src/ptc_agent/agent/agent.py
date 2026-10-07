"""PTC Agent - Main agent using create_agent with Programmatic Tool Calling pattern.

This module creates a PTC agent that:
- Uses langchain's create_agent with custom middleware stack
- Integrates sandbox via SandboxBackend
- Provides MCP tools through execute_code
- Supports sub-agent delegation for specialized tasks
"""

from collections.abc import Mapping
from datetime import UTC, datetime
from functools import partial
from typing import Any

import structlog
from langchain.agents import create_agent

from ptc_agent.agent.backends import SandboxBackend
from ptc_agent.core.paths import SandboxLayout, WorkspaceLayout
from ptc_agent.core.project_context import ProjectContext
from ptc_agent.agent.middleware import SubAgentMiddleware
from ptc_agent.agent.main_state import MainAgentState
from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware

from ptc_agent.agent.middleware import (
    AskUserMiddleware,
    BackgroundSubagentMiddleware,
    BackgroundSubagentOrchestrator,
    SubagentEventCaptureMiddleware,
    MultimodalMiddleware,
    MultimodalStripMiddleware,
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
    LargeResultEvictionMiddleware,
    MarketWatchMiddleware,
    SteeringMiddleware,
    SubagentSteeringMiddleware,
    ReasoningCompatibilityMiddleware,
)
from ptc_agent.agent.middleware.compaction.notes import (
    NotesDueMiddleware,
    NotesOffMiddleware,
    ThreadScratchpad,
    notes_window_carry,
)
from ptc_agent.agent.middleware.compaction.window import WindowCoverage
from ptc_agent.agent.middleware.direct_mcp import (
    DirectToolSet,
    direct_tool_middleware,
    direct_tool_summary,
)
from ptc_agent.agent.middleware.order_governance import OrderLedger
from ptc_agent.agent.middleware.subagent_switch import (
    SubagentSwitchMiddleware,
    SubagentSwitchReader,
    subagents_window_carry,
)
from ptc_agent.agent.context_stack import build_context_middleware
from ptc_agent.agent.roles import AgentRole
from ptc_agent.agent.filesystem_routes import (
    build_filesystem_backend,
    resolve_identity_gates,
)
from ptc_agent.agent.middleware.background_subagent.workflow.prebuilt import (
    get_prebuilt_workflows,
)
from ptc_agent.agent.middleware.image_capture import ImageCaptureMiddleware
from ptc_agent.agent.middleware.openai_prompt_caching import OpenAIPromptCachingMiddleware
from ptc_agent.agent.middleware.runtime_context import (
    FrozenPromptMiddleware,
    TailEnvelopeMiddleware,
    TurnContext,
    TurnContextMiddleware,
)
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
    workspace_path_vars,
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
from src.tools.chart_annotation import CHART_ANNOTATION_TOOLS
from src.tools.messaging import build_messaging_tools, messaging_enabled
from ptc_agent.config import AgentConfig
from ptc_agent.core.mcp_registry import MCPRegistry
from ptc_agent.core.sandbox import PTCSandbox
from ptc_agent.core.sandbox.livefs_mount import CallContext

from ptc_agent.agent.turn import build_model_resilience_middleware, turn_model

try:
    from langgraph.types import Checkpointer
except ImportError:
    Checkpointer = None  # type: ignore[misc,assignment]

logger = structlog.get_logger(__name__)


DEFAULT_MAX_CONCURRENT_TASK_UNITS = 3


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
        subagent_summary: str,
        guidance: str,
        thread_id: str | None = None,
        memory_enabled: bool = True,
        memo_enabled: bool = True,
        todo_enabled: bool = False,
        crawl_enabled: bool = False,
        scratchpad_enabled: bool = False,
        direct_tool_summary: str = "",
        workspace: WorkspaceLayout | None = None,
        legacy_layout: bool = False,
        files_mounted: bool = False,
        chart_annotation_enabled: bool = True,
        role: AgentRole = "analyst",
    ) -> str:
        """Build the static system prompt (excludes time/profile for cacheability).

        ``guidance`` shapes the cached prefix, so the prefix varies by (model,
        guidance) rather than (model), which only splits when a user pins the
        level themselves. The workspace folder varies it too, but a thread
        lives in one workspace, so a thread still reuses its own prefix.
        """
        loader = get_loader()
        return loader.get_system_prompt(
            **guidance_template_vars(guidance),
            **workspace_path_vars(
                workspace,
                root=self.config.filesystem.working_directory,
                legacy_layout=legacy_layout,
            ),
            subagent_summary=subagent_summary,
            max_concurrent_task_units=DEFAULT_MAX_CONCURRENT_TASK_UNITS,
            ask_user_enabled=True,
            include_examples=True,
            include_anti_patterns=True,
            thread_id=thread_id or "",
            memory_enabled=memory_enabled,
            memo_enabled=memo_enabled,
            market_watch_enabled=self.config.feature_enabled("market_watch"),
            todo_enabled=todo_enabled,
            crawl_enabled=crawl_enabled,
            scratchpad_enabled=scratchpad_enabled,
            direct_tool_summary=direct_tool_summary,
            files_mounted=files_mounted,
            chart_annotation_enabled=chart_annotation_enabled,
            role=role,
        )

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
        namespace_owner: Any | None = None,
        user_profile: dict | None = None,
        thread_id: str | None = None,
        workspace_name: str | None = None,
        workspace_description: str | None = None,
        on_agent_md_write: Any | None = None,
        store: Any | None = None,
        on_signed_url: Any | None = None,
        vault_secrets: dict[str, str] | None = None,
        user_id: str | None = None,
        user_data_counts: dict[str, Any] | None = None,
        tool_summary: str | None = None,
        disable_subagents: bool = False,
        direct_mcp: DirectToolSet | None = None,
        order_ledger: OrderLedger | None = None,
        turn_context: TurnContext | None = None,
        project: ProjectContext | None = None,
        role: AgentRole = "analyst",
        harness_blocks: Mapping[str, str | None] | None = None,
        subagent_switch: SubagentSwitchReader | None = None,
        window_coverage: WindowCoverage | None = None,
    ) -> Any:
        """Create a deepagent with PTC pattern capabilities.

        Key non-obvious parameters:
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
            on_agent_md_write: Invalidates the Session's agent.md cache on write
                and records the write's last-writer stamp.
            turn_context: What this turn knows about itself (when the previous
                one ran, the surface it arrived on and that surface's delivery
                rules, the zone its clock is stamped in), for the turn anchor
                row. None for a context-free build such as thread maintenance.
            project: The workspace folder this turn runs in. Passed rather
                than read from the ambient context because the build happens
                before the run's own task binds it.
            role: The Chief of Staff gets its role section and its
                coordination tools, which subagents never see, and only the
                subagents listed for its role (``SubagentDefinition.roles``).
            harness_blocks: The baseline blocks this build's role adds, keyed
                by kind (see ``HARNESS_BLOCKS``), each the text its read
                returned before the build, or None when that read did not
                answer, which the baseline takes as a hole to read again.
            subagent_switch: Reads the user's subagent switch for this
                thread, fresh on every call. None for a build with no thread
                row to read (thread maintenance, a synthetic turn), which
                leaves subagents as built. It never changes the tools or the
                prompt: it adds a history row and refuses launches instead.
            window_coverage: Asks the server whether turn slices hold the
                runs the main agent's window would trim from its checkpoint
                (see ``compaction.window``). None for a build with no server
                behind it, which never trims.

        Returns:
            Configured BackgroundSubagentOrchestrator wrapping the deepagent.
        """
        turn = turn_model(self.config, llm, self.llm, flash=False)

        # Freeze current time for this request (refreshes on each new query)
        request_time = datetime.now(tz=UTC)
        timezone_str = turn_context.timezone if turn_context else None
        current_time = format_current_time(request_time, timezone_str)

        # Compute short thread ID for thread-scoped storage
        short_thread_id = thread_id[:8] if thread_id else ""

        preview_owner = project.workspace_id if project else None
        backend = SandboxBackend(
            sandbox,
            operation_callback=operation_callback,
            preview_owner=preview_owner,
        )
        # The one workspace root this build uses, read off the live computer
        # root plus the turn's folder rather than the config default.
        workspace_layout = sandbox.workspace(project)
        # The scratchpad is named after its thread, so a build with no thread
        # gets neither the prompt section nor the folder.
        scratchpad = ThreadScratchpad.resolve(
            self.config, workspace_layout.workspace, thread_id
        )

        # Memory is opt-in: disabled entirely when identity is missing rather
        # than falling back to a shared namespace that would cross-pollinate
        # unauthenticated sessions. The workspace comes from the project: the
        # session is per computer and several workspaces share it, so its own
        # id names whichever one acquired it first.
        workspace_id_for_memory = project.workspace_id if project else None
        gates = resolve_identity_gates(
            store=store,
            user_id=user_id,
            workspace_id=workspace_id_for_memory,
            disable_subagents=disable_subagents,
            channels=messaging_enabled(),
        )
        if store is not None and not gates.memory:
            logger.warning(
                "memory disabled due to missing identity",
                user_id_present=bool(user_id),
                workspace_id_present=bool(workspace_id_for_memory),
            )

        # Who this turn writes for: a new automation's defaults, for the
        # file tools and for a save through the file mount alike.
        call_context = CallContext(
            workspace_id=workspace_id_for_memory,
            thread_id=thread_id,
            timezone=turn_context.tool_timezone if turn_context else None,
        )
        filesystem_backend, baseline_store_sources = build_filesystem_backend(
            backend=backend,
            gates=gates,
            store=store,
            user_id=user_id,
            workspace_id=workspace_id_for_memory,
            layout=workspace_layout,
            call=call_context,
        )

        # The folders only the file tools reach, which Bash and code refuse.
        file_tools_only = (SandboxLayout.CHANNELS_DIR,) if gates.channels else ()

        # Create the execute_code tool for MCP invocation
        execute_code_tool = create_execute_code_tool(
            backend,
            mcp_registry,
            thread_id=short_thread_id,
            session=session,
            call_context=call_context,
            file_tools_only=file_tools_only,
        )

        # Create the Bash tool for shell command execution
        bash_tool = create_execute_bash_tool(
            backend,
            thread_id=short_thread_id,
            call_context=call_context,
            file_tools_only=file_tools_only,
        )
        bash_output_tool = create_bash_output_tool(backend, call_context=call_context)

        # Create the preview URL tool for sandbox service previews
        workspace_id = project.workspace_id if project else ""
        preview_url_tool = create_preview_url_tool(backend, workspace_id=workspace_id, on_signed_url=on_signed_url)

        # Create the show widget tool for inline HTML visualizations
        show_widget_tool = create_show_widget_tool(backend)

        # Start with base tools
        tools: list[Any] = [execute_code_tool, bash_tool, bash_output_tool, preview_url_tool, show_widget_tool]

        # Opt-in while the tool is on its way out: a default build binds no
        # todo tool, so neither the main agent nor a subagent that lists the
        # "todo" set is given one.
        todo_tools: list[Any] = (
            [TodoWrite] if self.config.feature_enabled("todo_write") else []
        )
        tools.extend(todo_tools)

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
                # The workspace root: the hook fires on ``agent.md``, which
                # lives in the folder, and a path measured from the computer
                # root keeps the folder name in front of it.
                work_dir=workspace_layout.workspace,
                thread_id=thread_id,
            )
        )
        # Shared placement gives subagents provenance coverage too. The leak
        # detector's redactor scrubs secrets from snippets the content-only scan
        # never sees (provenance fingerprints the raw result/artifact).
        shared_middleware.append(ProvenanceMiddleware(redactor=leak_detection.redact))
        if todo_tools:
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
        # The chart-annotation skill no longer gates its tools, but its switch
        # still does: a user who turned it off, or the plugin that ships it,
        # gets neither the tools nor the prompt line naming them.
        chart_annotation = "chart-annotation" in skill_registry
        # Subagents never bind the annotation tools (see where they are
        # appended), so their manifest must not offer the guide to them.
        subagent_skill_registry = {
            name: skill
            for name, skill in skill_registry.items()
            if name != "chart-annotation"
        }

        # One per stack rather than one shared instance, because the two stacks
        # differ in where the manifest goes, and in the one skill above. The
        # main agent has a baseline to freeze it into; a subagent does not, so
        # its copy keeps appending the manifest per call.
        skills_middleware = partial(
            SkillsMiddleware,
            mode="ptc",
            backend=backend,
            sources=skill_sources,
            known_skills=known_skills,
            skill_dirs=[
                d for d, _ in self.config.skills.local_skill_dirs_with_sandbox()
            ],
            disabled_skills=self.config.disabled_skills,
        )
        skill_loader_middleware = skills_middleware(
            skill_registry=skill_registry, inject_manifest=False
        )
        subagent_skill_middleware = skills_middleware(
            skill_registry=subagent_skill_registry, inject_manifest=True
        )
        tools.extend(skill_loader_middleware.tools)
        tools.extend(skill_loader_middleware.get_all_skill_tools())

        # --- Build main-only middleware (NOT passed to subagents) ---
        main_only_middleware: list[Any] = []

        # Must be first: steering context must be visible before any other middleware.
        main_only_middleware.append(SteeringMiddleware())

        # Right after steering, so a steer and a flip sent together reach the
        # model in that order, with the operator-role row last. Its tool hook
        # wraps BackgroundSubagentMiddleware (the one Task interceptor) and the
        # tool node (RunWorkflow), so a refused launch starts nothing; the
        # outer wrappers only parse, normalize and record, as for any result.
        if subagent_switch is not None and not disable_subagents:
            main_only_middleware.append(SubagentSwitchMiddleware(subagent_switch))

        # Consent is re-read per call and an order is put to the user against
        # a durable attempt, which is what execution then reads.
        direct_tools = list(direct_mcp.tools) if direct_mcp is not None else []
        main_only_middleware.extend(direct_tool_middleware(direct_mcp, order_ledger))

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

        ask_user_middleware = AskUserMiddleware()
        main_only_middleware.append(ask_user_middleware)
        tools.extend(ask_user_middleware.tools)

        if role == "chief_of_staff":
            from src.tools.secretary.approvals import StandingApprovalMiddleware

            main_only_middleware.append(StandingApprovalMiddleware(user_id))

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
            "todo": todo_tools,
        }
        # The compiler gets its own registry: same per-user gates, but
        # mode-unfiltered — subagent definitions may be flash-mode and preload
        # flash-only skills the ptc-filtered registry above excludes. It drops
        # the chart guide for the same reason the subagent manifest does.
        compiler_skill_registry = build_effective_skill_registry(
            None,
            feature_resolver=self.config.feature_enabled,
            disabled_skills=self.config.disabled_skills,
            user_skills=self.config.user_skills,
            user_skill_dir=self.config.user_skill_dir,
            workspace_skill_dir=self.config.workspace_skill_dir,
        )
        compiler_skill_registry.pop("chart-annotation", None)
        subagent_compiler = SubagentCompiler(
            sandbox=sandbox,
            mcp_registry=mcp_registry,
            tool_sets=subagent_tool_sets,
            default_model=turn.client,
            user_profile=user_profile,
            current_time=current_time,
            thread_id=short_thread_id,
            config=self.config,
            skill_registry=compiler_skill_registry,
            skill_dirs=[
                d for d, _ in self.config.skills.local_skill_dirs_with_sandbox()
            ],
            project=project,
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
                role=role,
            )
            if additional_subagents:
                subagents.extend(additional_subagents)

        # The roster the baseline freezes into <mcp-servers>. Prefer the
        # session-cached summary (precomputed once per session in the
        # WorkspaceManager) so the hot path never recomputes it; fall back to
        # computing from the registry for callers without a cached summary
        # (tests, the SessionProvider path). Resolved to a string here rather
        # than read live at the turn boundary: a source that answers None
        # marks the epoch incomplete, and a build with no session cache would
        # then rebuild the block on every turn.
        if tool_summary is None:
            tool_summary = self._get_tool_summary(mcp_registry)
        subagent_summary = format_subagent_summary(subagents)

        eviction_dir = (
            WorkspaceLayout.large_results_subdir(short_thread_id)
            if short_thread_id
            else WorkspaceLayout.LARGE_TOOL_RESULTS_DIR
        )

        render_prompt = partial(
            self._build_system_prompt,
            subagent_summary,
            turn.guidance,
            thread_id=short_thread_id,
            memory_enabled=gates.memory,
            memo_enabled=gates.memo,
            todo_enabled=bool(todo_tools),
            crawl_enabled=bool(crawl_tools),
            scratchpad_enabled=scratchpad is not None,
            direct_tool_summary=direct_tool_summary(direct_tools),
            workspace=workspace_layout,
            legacy_layout=bool(project is not None and project.layout_origin == 3),
            chart_annotation_enabled=chart_annotation,
            role=role,
        )
        # Read once: the baseline freezes this value per epoch, and the
        # prompt is sent with the frozen one (FrozenPromptMiddleware).
        files_mounted = sandbox.livefs is not None
        system_prompt = render_prompt(files_mounted=files_mounted)

        logger.debug(
            "Creating agent with custom middleware stack",
            tool_count=len(tools),
            subagent_count=len(subagents),
            skills_enabled=self.config.skills.enabled,
        )

        # --- Build final middleware stacks ---
        compaction = CompactionMiddleware.for_agent(
            self.config, backend=backend, workspace_id=workspace_id_for_memory
        )
        # The main agent is asked to update its scratchpad notes before each
        # summary, and its summaries name them; a subagent's do not. Without
        # the scratchpad, rows asked while it was on stay out of the calls.
        main_compaction, notes_rows = compaction, None
        if scratchpad is None:
            notes_rows = NotesOffMiddleware()
        elif compaction is not None:
            main_compaction = compaction.with_scratchpad_notes(scratchpad.notes_dir)
            notes_rows = NotesDueMiddleware(scratchpad, main_compaction)
        # Each carry writes a field ``MainAgentState`` declares, so a build
        # without the carry's reader (a notification turn has no switch)
        # trims too.
        if main_compaction and window_coverage and thread_id:
            main_compaction = main_compaction.with_window(
                window_coverage, [notes_window_carry(thread_id), subagents_window_carry]
            )

        model_resilience = [build_model_resilience_middleware(self.config, turn)]

        # The strip goes inside model_resilience so it judges the post-fallback
        # model: a vision primary falling back to a text-only candidate would
        # otherwise replay image/PDF blocks and earn the 400 the fallback exists
        # to avoid. Both halves go in both stacks because a subagent on its own
        # model needs the same treatment; the strip reads that model off each
        # request, so the two stacks share one instance rather than one apiece.
        #
        # Unconditional, unlike the read half's old combined form, because the
        # two halves are no longer switched on together. ``model_name`` only
        # decides whether the user's per-model override applies; the target is
        # read off each request, and an unresolvable one is judged text-only. So
        # a config with no llm gets a strip that removes everything rather than
        # no strip at all, which is the safe direction: without it the read half
        # would keep attaching blocks that nothing removes.
        multimodal_strip = MultimodalStripMiddleware(
            model_name=self.config.llm.name if self.config.llm else None,
            custom_modalities=self.config.input_modalities,
            can_extract=True,
        )
        multimodal_read = MultimodalMiddleware(sandbox=sandbox)

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
                # The turn row a subagent can act on: when its turn opened,
                # and that it is a child rather than the main agent. The user's
                # context stays out, a subagent works from its brief. It writes
                # into history at the turn boundary, so its position among the
                # middlewares below does not matter. No clock is injected: this
                # stack is built once per parent turn and run for every
                # subagent, so the row reads the clock when its own turn opens.
                TurnContextMiddleware(
                    timezone=timezone_str or "UTC",
                    is_subagent=True,
                ),
                LargeResultEvictionMiddleware(
                    backend=backend, eviction_dir=eviction_dir
                ),
                *shared_middleware,
                subagent_skill_middleware,
                image_capture,
                compaction,
                *model_resilience,
                multimodal_strip,
                multimodal_read,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                OpenAIPromptCachingMiddleware(),
                EmptyToolCallRetryMiddleware(),
                PatchToolCallsMiddleware(),
                # Innermost: carries the turn's rows in the shape this model
                # accepts and pins the tail breakpoint on the last block.
                TailEnvelopeMiddleware(
                    now=request_time,
                    guidance=turn.guidance,
                    model_name=turn.name or None,
                ),
                ReasoningCompatibilityMiddleware(),
            ]
            if m is not None
        ]

        # The turn row, the per-thread baseline and the tail envelope, wired
        # for the main agent: the baseline is agent.md, the memory indices, the
        # memo pointer and the user's identity + steering, which a subagent
        # never gets because it works from its brief, not from the user's
        # workspace. Where each of the three has to sit is on ContextMiddleware.
        context = build_context_middleware(
            now=request_time,
            guidance=turn.guidance,
            model_name=turn.name or None,
            turn_context=turn_context,
            user_profile=user_profile,
            sandbox_enabled=True,
            session=session,
            workspace_name=workspace_name,
            workspace_description=workspace_description,
            sources=baseline_store_sources,
            files_mounted=files_mounted,
            channels_enabled=gates.channels,
            blocks={
                "mcp_servers": lambda _state: tool_summary,
                "skills": lambda state: skill_loader_middleware.build_manifest(state)
                or "",
                **{
                    kind: (lambda _state, text=text: text)
                    for kind, text in (harness_blocks or {}).items()
                },
                # Its path names the thread, which the static prompt must not.
                **(
                    {"scratchpad": lambda _state: scratchpad.folder}
                    if scratchpad is not None
                    else {}
                ),
            },
            user_data_counts=user_data_counts,
            role=role,
        )

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

        # Chart annotations, main agent only: appended after the subagent
        # snapshot above, which the default general-purpose subagent is built
        # from. A drawing is part of the reply the user sees, and a subagent
        # answers the main agent, not the user.
        if chart_annotation:
            tools.extend(CHART_ANNOTATION_TOOLS)

        # Messaging the user (send_message, list_message_targets), present only
        # when a channel gateway is configured. Main agent only, added after the
        # subagent snapshot: a subagent reports to its parent, never to a person.
        tools.extend(build_messaging_tools(role))

        # Main agent middleware (includes SubAgentMiddleware + main_only)
        # Ordering matters for prompt caching:
        #   - AnthropicPromptCachingMiddleware (cache_control) and
        #     OpenAIPromptCachingMiddleware (prompt_cache_breakpoint) each place
        #     their provider's breakpoint on the last system message block they
        #     see, which is the static prompt's own: the skills manifest and the
        #     MCP roster ride in the baseline now. Each no-ops for other
        #     providers.
        #   - TurnContextMiddleware runs before BaselineContextMiddleware so
        #     the turn row is written into history ahead of the change rows.
        #   - BaselineContextMiddleware is innermost on the system message: it
        #     appends the frozen per-thread baseline AFTER their breakpoint as
        #     one block and pins breakpoint 3 on it, so the static prefix stays
        #     shareable while the baseline caches per thread.
        #   - NotesDueMiddleware (NotesOffMiddleware without the scratchpad)
        #     sits after the turn and baseline rows, so a reminder written on
        #     a resumed turn follows its anchor, and inside compaction, so it
        #     sees the summary each call is sent with and drops the reminders
        #     that summary answered.
        #   - TailEnvelopeMiddleware is innermost overall. It carries this
        #     turn's rows, appends an envelope only when the call has a
        #     market_watch stamp, and pins the tail cache breakpoint, so
        #     nothing may be appended after it.
        # On the wire that is: tools (bp1), static system (bp2), baseline block
        # (bp3), messages ending in the turn's rows (bp4), the four Anthropic
        # allows.
        deepagent_middleware = [
            m
            for m in [
                # Outermost, since it replaces the prompt every middleware
                # below appends to.
                FrozenPromptMiddleware(
                    files_mounted=files_mounted,
                    render=lambda mounted: render_prompt(files_mounted=mounted),
                ),
                LargeResultEvictionMiddleware(
                    backend=backend, eviction_dir=eviction_dir
                ),
                subagent_task_middleware,
                *shared_middleware,
                skill_loader_middleware,
                *main_only_middleware,
                image_capture,
                main_compaction,
                *model_resilience,
                multimodal_strip,
                multimodal_read,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                OpenAIPromptCachingMiddleware(),
                # Market watch (main agent only): contributes the
                # <market-watch> price stamp as a per-call row on the request,
                # which the tail envelope renders. Must stay OUTSIDE
                # TailEnvelopeMiddleware, which reads that key.
                *(
                    [MarketWatchMiddleware()]
                    if self.config.feature_enabled("market_watch")
                    else []
                ),
                EmptyToolCallRetryMiddleware(),
                PatchToolCallsMiddleware(),
                context.turn,
                context.baseline,
                notes_rows,
                context.tail,
                ReasoningCompatibilityMiddleware(),
            ]
            if m is not None
        ]

        # Main agent only, added after the subagent snapshot was taken above:
        # directly bound MCP tools are the ones a policy has to see every
        # call, and a subagent runs no main-only middleware.
        if role == "chief_of_staff":
            from src.tools.secretary.chief_of_staff import CHIEF_OF_STAFF_TOOLS

            tools = [*tools, *CHIEF_OF_STAFF_TOOLS]
        if direct_tools:
            tools = [*tools, *direct_tools]

        agent: Any = create_agent(
            turn.client,
            system_prompt=system_prompt,
            tools=tools,
            middleware=deepagent_middleware,
            checkpointer=checkpointer,
            store=store,
            state_schema=MainAgentState,
        ).with_config({"recursion_limit": 2000})

        return BackgroundSubagentOrchestrator(
            agent=agent,
            middleware=background_middleware,
            auto_wait=self.config.background_auto_wait,
        )
