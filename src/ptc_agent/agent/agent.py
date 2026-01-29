"""PTC Agent - Main agent using create_agent with Programmatic Tool Calling pattern.

This module creates a PTC agent that:
- Uses langchain's create_agent with custom middleware stack
- Integrates Daytona sandbox via DaytonaBackend
- Provides MCP tools through execute_code
- Supports sub-agent delegation for specialized tasks
"""

from typing import Any

import structlog
from langchain.agents import create_agent

from ptc_agent.agent.backends import DaytonaBackend
from deepagents.middleware import FilesystemMiddleware, SkillsMiddleware, SubAgentMiddleware
from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware

from ptc_agent.agent.middleware import (
    BackgroundSubagentMiddleware,
    BackgroundSubagentOrchestrator,
    PlanModeMiddleware,
    ToolCallCounterMiddleware,
    ViewImageMiddleware,
    create_plan_mode_interrupt_config,
    create_view_image_tool,
    # Tool middleware
    ToolArgumentParsingMiddleware,
    ToolErrorHandlingMiddleware,
    ToolResultNormalizationMiddleware,
    # File operations SSE middleware
    FileOperationMiddleware,
    # Todo operations SSE middleware
    TodoWriteMiddleware,
    # Dynamic skill loader middleware
    DynamicSkillLoaderMiddleware,
    # Summarization middleware
    SummarizationMiddleware,
)
from ptc_agent.agent.skills import SKILL_REGISTRY
from ptc_agent.agent.middleware.background.registry import BackgroundTaskRegistry
from ptc_agent.agent.prompts import format_subagent_summary, format_tool_summary, get_loader
from ptc_agent.agent.subagents import create_subagents_from_names
from ptc_agent.agent.tools import (
    create_execute_bash_tool,
    create_execute_code_tool,
    create_filesystem_tools,
    create_glob_tool,
    create_grep_tool,
    TodoWrite,
)
from src.tools.search import get_web_search_tool
from ptc_agent.config import AgentConfig
from ptc_agent.core.mcp_registry import MCPRegistry
from ptc_agent.core.sandbox import ExecutionResult, PTCSandbox
from ptc_agent.utils.storage.storage_uploader import is_storage_enabled

# Import HITL middleware for plan mode
try:
    from langchain.agents.middleware import HumanInTheLoopMiddleware
except ImportError:
    HumanInTheLoopMiddleware = None  # type: ignore[misc,assignment]

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

# Description for the SubAgentMiddleware Task tool
SUBAGENT_MIDDLEWARE_DESCRIPTION = """Launch a subagent for complex, multi-step tasks.

Args:
    description: Detailed task instructions for the subagent
    subagent_type: Agent type to use

Usage:
- Use for: Complex tasks, isolated research, context-heavy operations
- NOT for: Simple 1-2 tool operations (do directly)
- Parallel: Launch multiple agents in single message for concurrent tasks
- Results: Subagent returns final report only (intermediate steps hidden)

The subagent works autonomously. Provide clear, complete instructions."""


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
        self.subagents: dict[str, Any] = {}  # Populated in create_agent() for introspection

        # Get provider/model info for logging
        if config.llm_definition is not None:
            provider = config.llm_definition.provider
            model = config.llm_definition.model_id
        else:
            # LLM client was passed directly via AgentConfig.create()
            # Try to extract info from the LLM instance
            provider = getattr(self.llm, "_llm_type", "unknown")
            model = getattr(self.llm, "model", getattr(self.llm, "model_name", "unknown"))

        logger.info(
            "Initialized PTCAgent with deepagent",
            provider=provider,
            model=model,
        )

    def _get_subagent_summary(self, mcp_registry: MCPRegistry | None = None) -> str:
        """Get formatted subagent summary for prompts.

        Returns a summary of configured subagents. If called after create_agent(),
        returns the actual subagents that were created. If called before, returns
        a summary based on configured subagent names.

        Args:
            mcp_registry: Optional MCP registry (unused, kept for API consistency)

        Returns:
            Formatted subagent summary string
        """
        if self.subagents:
            # Format from stored subagent info (after create_agent was called)
            lines = []
            for name, info in self.subagents.items():
                description = info.get("description", "")
                tools = info.get("tools", [])
                lines.append(f"- **{name}**: {description}")
                if tools:
                    lines.append(f"  Tools: {', '.join(tools)}")
            return "\n".join(lines) if lines else "No sub-agents configured."
        # Before create_agent, show configured subagent names
        if self.config.subagents_enabled:
            return f"Configured subagents: {', '.join(self.config.subagents_enabled)}"
        return "No sub-agents configured."

    def _build_system_prompt(
        self,
        tool_summary: str,
        subagent_summary: str,
        user_profile: dict | None = None,
    ) -> str:
        """Build the system prompt for the agent.

        Args:
            tool_summary: Formatted MCP tool summary
            subagent_summary: Formatted subagent summary
            user_profile: Optional user profile dict with name, timezone, locale

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
            include_examples=True,
            include_anti_patterns=True,
            for_task_workflow=True,
        )

    def _get_tool_summary(self, mcp_registry: MCPRegistry) -> str:
        """Get formatted tool summary for prompts.

        Args:
            mcp_registry: MCP registry

        Returns:
            Formatted tool summary string
        """
        tools_by_server = mcp_registry.get_all_tools()

        # Convert to format expected by formatter
        tools_dict = {}
        for server_name, tools in tools_by_server.items():
            tools_dict[server_name] = [tool.to_dict() for tool in tools]

        # Build server configs dict for formatter (only enabled servers)
        server_configs = {s.name: s for s in self.config.mcp.servers if s.enabled}

        # Get tool exposure mode from config
        mode = self.config.mcp.tool_exposure_mode

        return format_tool_summary(tools_dict, mode=mode, server_configs=server_configs)

    def create_agent(
        self,
        sandbox: PTCSandbox,
        mcp_registry: MCPRegistry,
        subagent_names: list[str] | None = None,
        additional_subagents: list[dict[str, Any]] | None = None,
        background_timeout: float = 300.0,
        checkpointer: Any | None = None,
        system_prompt_suffix: str | None = None,
        llm: Any | None = None,
        operation_callback: Any | None = None,
        background_registry: BackgroundTaskRegistry | None = None,
        user_profile: dict | None = None,
        plan_mode: bool = False,
    ) -> Any:
        """Create a deepagent with PTC pattern capabilities.

        Args:
            sandbox: PTCSandbox instance for code execution
            mcp_registry: MCPRegistry with available MCP tools
            subagent_names: List of subagent names to include from SUBAGENT_REGISTRY
                (default: config.subagents_enabled)
            additional_subagents: Custom subagent dicts that bypass the registry
            background_timeout: Timeout for waiting on background tasks (seconds)
            checkpointer: Optional LangGraph checkpointer for state persistence.
                Required for submit_plan interrupt/resume workflow.
            system_prompt_suffix: Optional string to append to the system prompt.
                Useful for adding user/project-specific instructions (e.g., agent.md content).
            llm: Optional LLM override. If provided, uses this instead of self.llm.
                Useful for model switching without recreating PTCAgent instance.
            operation_callback: Optional callback for file operation logging.
                Receives dict with operation details (operation, file_path, timestamp, etc.).
            background_registry: Optional shared background task registry for subagents.
            user_profile: Optional user profile dict with name, timezone, locale for
                injection into the system prompt.
            plan_mode: If True, adds submit_plan tool for plan review workflow.
                HITL middleware is always added for future interrupt features.

        Returns:
            Configured BackgroundSubagentOrchestrator wrapping the deepagent
        """
        # Use provided LLM or fall back to instance LLM
        model = llm if llm is not None else self.llm
        # Create the execute_code tool for MCP invocation
        execute_code_tool = create_execute_code_tool(sandbox, mcp_registry)

        # Create the Bash tool for shell command execution
        bash_tool = create_execute_bash_tool(sandbox)

        # Start with base tools
        tools: list[Any] = [execute_code_tool, bash_tool, TodoWrite]

        # Always create backend for FilesystemMiddleware
        # (it handles ls, and provides fallback for other operations)
        backend = DaytonaBackend(sandbox, operation_callback=operation_callback)

        # Create custom filesystem tools (override deepagents middleware tools)
        read_file, write_file, edit_file = create_filesystem_tools(
            sandbox,
            operation_callback=operation_callback,
        )
        filesystem_tools = [
            read_file,                        # overrides middleware read_file
            write_file,                       # overrides middleware write_file
            edit_file,                        # overrides middleware edit_file
            create_glob_tool(sandbox),        # overrides middleware glob
            create_grep_tool(sandbox),        # overrides middleware grep
        ]
        tools.extend(filesystem_tools)
        logger.info(
            "Using custom filesystem tools",
            tools=["read_file", "write_file", "edit_file", "glob", "grep"],
        )

        # Add web search tool (uses configured search engine from agent_config.yaml)
        web_search_tool = get_web_search_tool(
            max_search_results=10,
            time_range=None,
            verbose=False,
        )
        tools.append(web_search_tool)
        logger.info("Web search tool enabled", tool="web_search")

        # Add view_image tool if enabled (with sandbox for reading local images)
        view_image_tool = None
        if self.config.enable_view_image:
            view_image_tool = create_view_image_tool(sandbox=sandbox)
            tools.append(view_image_tool)
            logger.info("Vision tool enabled", tool="view_image")

        # Default to subagents from config if none specified
        if subagent_names is None:
            subagent_names = self.config.subagents_enabled

        # --- Build shared middleware (for both main agent and subagents) ---
        shared_middleware: list[Any] = []

        # Tool middleware - handles argument parsing, error handling, and result normalization
        # These run in order: parse args -> execute -> handle errors -> normalize results
        shared_middleware.extend([
            ToolArgumentParsingMiddleware(),   # Parse JSON string args to Python types
            ToolErrorHandlingMiddleware(),     # Catch tool errors, return simplified messages
            ToolResultNormalizationMiddleware(),  # Ensure all results are strings for LLM
        ])
        logger.info("Tool middleware enabled: argument parsing, error handling, result normalization")

        # File operation SSE middleware - emits events for write_file/edit_file
        shared_middleware.append(FileOperationMiddleware())
        logger.info("FileOperationMiddleware enabled for SSE events")

        # Todo operation SSE middleware - emits events for TodoWrite
        shared_middleware.append(TodoWriteMiddleware())
        logger.info("TodoWriteMiddleware enabled for SSE events")

        # Add view image middleware (always added when tool is enabled, for image injection)
        if self.config.enable_view_image:
            view_image_middleware = ViewImageMiddleware(
                validate_urls=True,
                strict_validation=True,
                sandbox=sandbox,
            )
            shared_middleware.append(view_image_middleware)
            logger.info("ViewImageMiddleware enabled with strict validation and sandbox support")

        # Add dynamic skill loader middleware for user onboarding etc.
        skill_loader_middleware = DynamicSkillLoaderMiddleware(
            skill_registry=SKILL_REGISTRY
        )
        shared_middleware.append(skill_loader_middleware)
        # Add load_skill tool
        tools.extend(skill_loader_middleware.tools)
        # Pre-register all skill tools (they're available but discovered via load_skill)
        tools.extend(skill_loader_middleware.get_all_skill_tools())
        logger.info(
            "Dynamic skill loader enabled",
            skill_count=len(SKILL_REGISTRY),
            skill_tool_count=len(skill_loader_middleware.get_all_skill_tools()),
        )

        # --- Build main-only middleware (NOT passed to subagents) ---
        main_only_middleware: list[Any] = []

        # Create background subagent middleware (must be created before subagents)
        background_middleware = BackgroundSubagentMiddleware(
            timeout=background_timeout,
            enabled=True,
            registry=background_registry,
        )
        main_only_middleware.append(background_middleware)
        # Add background management tools (wait, task_progress)
        tools.extend(background_middleware.tools)
        # Create counter middleware for tracking subagent tool calls
        counter_middleware = ToolCallCounterMiddleware(
            registry=background_middleware.registry
        )
        logger.info(
            "Background subagent execution enabled",
            timeout=background_timeout,
            background_tools=[t.name for t in background_middleware.tools],
        )

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
                logger.info(
                    "Plan tools enabled",
                    plan_tools=[getattr(t, "name", str(t)) for t in plan_middleware.tools],
                )

        # Create subagents from names using the registry
        # Pass vision tools to subagents if enabled
        vision_tools = [view_image_tool] if view_image_tool else None
        subagents = create_subagents_from_names(
            names=subagent_names,
            sandbox=sandbox,
            mcp_registry=mcp_registry,
            counter_middleware=counter_middleware,
            max_researcher_iterations=DEFAULT_MAX_TASK_ITERATIONS,
            max_iterations=DEFAULT_MAX_GENERAL_ITERATIONS,
            filesystem_tools=filesystem_tools,  # Pass custom tools to subagents
            vision_tools=vision_tools,  # Pass vision tools to subagents
        )

        if additional_subagents:
            subagents.extend(additional_subagents)

        # Get tool summary for system prompt
        tool_summary = self._get_tool_summary(mcp_registry)

        # Build subagent summary for system prompt
        subagent_summary = format_subagent_summary(subagents)

        # Build system prompt
        system_prompt = self._build_system_prompt(tool_summary, subagent_summary, user_profile)

        # Append suffix if provided (e.g., agent.md content)
        if system_prompt_suffix:
            system_prompt = f"{system_prompt}\n\n{system_prompt_suffix}"

        # Store subagent info for introspection (used by print_agent_config)
        self.subagents = {}
        for subagent in subagents:
            name = subagent.get("name", "unknown")
            subagent_tools = subagent.get("tools", [])
            tool_names = [t.name if hasattr(t, "name") else str(t) for t in subagent_tools]
            self.subagents[name] = {
                "description": subagent.get("description", ""),
                "tools": tool_names,
            }

        # Store native tools info for introspection (used by print_agent_config)
        self.native_tools = [t.name if hasattr(t, "name") else str(t) for t in tools]

        # Build skill sources from config (sandbox paths where skills were uploaded)
        skill_sources: list[str] | None = None
        if self.config.skills.enabled:
            # Single skills directory - both user and project skills uploaded here
            skill_sources = [f"{self.config.skills.sandbox_skills_base}/"]

        logger.info(
            "Creating agent with custom middleware stack",
            tool_count=len(tools),
            subagent_count=len(subagents),
            skills_enabled=self.config.skills.enabled,
        )

        # --- Build final middleware stacks ---
        # Skills middleware (optional, based on config)
        skills_middleware: list[Any] = []
        if skill_sources:
            skills_middleware = [SkillsMiddleware(backend=backend, sources=skill_sources)]

        # Custom SSE-enabled summarization emits 'summarization_signal' events
        summarization = SummarizationMiddleware()

        # Subagent middleware (shared only, no SubAgentMiddleware/BackgroundSubagentMiddleware/HITL)
        subagent_middleware = [
            m for m in [
                *skills_middleware,
                FilesystemMiddleware(backend=backend),
                *shared_middleware,
                summarization,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                PatchToolCallsMiddleware(),
            ] if m is not None
        ]

        # Main agent middleware (includes SubAgentMiddleware + main_only)
        deepagent_middleware = [
            m for m in [
                *skills_middleware,
                FilesystemMiddleware(backend=backend),
                SubAgentMiddleware(
                    default_model=model,
                    default_tools=tools,
                    subagents=subagents if subagents else [],
                    task_description=SUBAGENT_MIDDLEWARE_DESCRIPTION,
                    system_prompt=None,  # Disable verbose TASK_SYSTEM_PROMPT injection
                    default_middleware=subagent_middleware,
                    general_purpose_agent=True,
                ),
                *shared_middleware,
                *main_only_middleware,
                summarization,
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                PatchToolCallsMiddleware(),
            ] if m is not None
        ]

        # Create agent with middleware stack
        agent: Any = create_agent(
            model,
            system_prompt=system_prompt,
            tools=tools,
            middleware=deepagent_middleware,
            checkpointer=checkpointer,
        ).with_config({"recursion_limit": 1000})

        # Wrap with orchestrator for background execution support
        return BackgroundSubagentOrchestrator(
            agent=agent,
            middleware=background_middleware,
            auto_wait=self.config.background_auto_wait,
        )


class PTCExecutor:
    """Executor that combines agent and sandbox for complete task execution."""

    def __init__(self, agent: PTCAgent, mcp_registry: MCPRegistry) -> None:
        """Initialize executor.

        Args:
            agent: PTC agent for task execution
            mcp_registry: MCP registry with available tools
        """
        self.agent = agent
        self.mcp_registry = mcp_registry

        logger.info("Initialized PTCExecutor")

    async def execute_task(
        self,
        task: str,
        sandbox: PTCSandbox,
        max_retries: int = 3,
    ) -> ExecutionResult:
        """Execute a task using deepagent with automatic error recovery.

        Args:
            task: User's task description
            sandbox: PTCSandbox instance
            max_retries: Maximum retry attempts

        Returns:
            Final execution result.
        """
        logger.info("Executing task with deepagent", task=task[:100])

        # Create the agent with injected dependencies
        agent = self.agent.create_agent(
            sandbox,
            self.mcp_registry,
        )

        try:
            # Configure recursion limit
            recursion_limit = max(max_retries * 5, 15)

            # Execute task via deepagent
            agent_result = await agent.ainvoke(
                {"messages": [("user", task)]},
                config={"recursion_limit": recursion_limit},
            )

            return await self._parse_agent_result(agent_result, sandbox)

        except Exception as e:
            logger.exception("Agent execution failed")

            return ExecutionResult(
                success=False,
                stdout="",
                stderr=f"Agent execution error: {e!s}",
                duration=0,
                files_created=[],
                files_modified=[],
                execution_id="agent_error",
                code_hash="",
            )

    async def _parse_agent_result(
        self,
        agent_result: dict,
        sandbox: PTCSandbox,
    ) -> ExecutionResult:
        """Parse deepagent result into ExecutionResult.

        Args:
            agent_result: Result from agent.ainvoke()
            sandbox: Sandbox instance to query for files

        Returns:
            ExecutionResult with execution details
        """
        messages = agent_result.get("messages", [])

        if not messages:
            return ExecutionResult(
                success=False,
                stdout="",
                stderr="Agent returned no messages",
                duration=0,
                files_created=[],
                files_modified=[],
                execution_id="no_messages",
                code_hash="",
            )

        # Find tool messages
        tool_messages = [
            msg for msg in messages if hasattr(msg, "type") and msg.type == "tool"
        ]

        if not tool_messages:
            # Extract final AI message
            ai_messages = [
                msg for msg in messages if hasattr(msg, "type") and msg.type == "ai"
            ]
            final_message = ai_messages[-1].content if ai_messages else "No execution"

            return ExecutionResult(
                success=True,  # Agent completed without code execution
                stdout=final_message,
                stderr="",
                duration=0,
                files_created=[],
                files_modified=[],
                execution_id="no_tool_calls",
                code_hash="",
            )

        # Get last tool message
        last_tool_msg = tool_messages[-1]
        observation = (
            last_tool_msg.content
            if hasattr(last_tool_msg, "content")
            else str(last_tool_msg)
        )

        # Check success
        success = "SUCCESS" in observation or "ERROR" not in observation

        # Extract stdout/stderr
        if success:
            stdout = observation.replace("SUCCESS", "").strip()
            stderr = ""
        else:
            stdout = ""
            stderr = observation.replace("ERROR", "").strip()

        # Get files from sandbox (optional - failure doesn't affect result)
        files_created = []
        try:
            if hasattr(sandbox, "_list_result_files"):
                result_files = await sandbox._list_result_files()
                files_created = [f for f in result_files if f]
        except Exception as e:
            # Graceful degradation: file listing is optional, log for debugging
            logger.debug("Failed to list result files (non-critical)", error=str(e))

        return ExecutionResult(
            success=success,
            stdout=stdout,
            stderr=stderr,
            duration=0.0,
            files_created=files_created,
            files_modified=[],
            execution_id=f"agent_step_{len(tool_messages)}",
            code_hash="",
        )


# For LangGraph deployment compatibility
async def create_ptc_agent(config: AgentConfig | None = None) -> PTCAgent:
    """Create a PTCAgent instance.

    Factory function for LangGraph deployment.

    Args:
        config: Optional agent configuration. If None, loads from default config files.

    Returns:
        Configured PTCAgent
    """
    if config is None:
        from ptc_agent.config import load_from_files
        config = await load_from_files()
        config.validate_api_keys()

    return PTCAgent(config)
