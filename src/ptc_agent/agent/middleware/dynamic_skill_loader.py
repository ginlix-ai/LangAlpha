"""Dynamic skill loader middleware.

This middleware provides a `load_skill` tool that dynamically makes skill tools
available to the agent. Skills are pre-registered at agent creation, but the
`wrap_model_call` hook filters tools before each model turn.

Key insight: Middleware's `wrap_model_call` hook can dynamically modify
`ModelRequest.tools` before each turn. Tools must be pre-registered at agent
creation for ToolNode execution, but visibility is controlled per-turn.

Architecture:
- Tools from all skills are pre-registered with ToolNode at agent creation
- The `load_skill` tool is intercepted by middleware to update state["loaded_skills"]
- Before each model call, `awrap_model_call` filters tools based on loaded skills
- Result: Model only sees tools from loaded skills (+ load_skill itself)

Usage:
    from ptc_agent.agent.middleware.dynamic_skill_loader import DynamicSkillLoaderMiddleware
    from ptc_agent.agent.skills import SKILL_REGISTRY

    middleware = DynamicSkillLoaderMiddleware(skill_registry=SKILL_REGISTRY)
    # middleware.tools contains [load_skill]
    # middleware.get_all_skill_tools() returns all skill tools for ToolNode
"""

from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from langchain.agents.middleware import AgentMiddleware, AgentState
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from langgraph.types import Command
from typing_extensions import NotRequired

from ptc_agent.agent.skills import SkillDefinition, SKILL_REGISTRY, get_skill, list_skills

logger = structlog.get_logger(__name__)

# State key for tracking loaded skills
LOADED_SKILLS_KEY = "loaded_skills"


class LoadedSkillsState(AgentState):
    """State schema for tracking loaded skills."""
    loaded_skills: NotRequired[list[str]]


class DynamicSkillLoaderMiddleware(AgentMiddleware):
    """Middleware that provides dynamic skill loading with tool filtering.

    This middleware:
    1. Provides a `load_skill` tool for the agent to request skill capabilities
    2. Intercepts `load_skill` calls to update state["loaded_skills"] via Command
    3. Filters tools in `awrap_model_call` based on state["loaded_skills"]
    4. Pre-registers all skill tools for ToolNode availability (but hidden until loaded)

    The middleware works with LangGraph state to track loaded skills:
    - state["loaded_skills"]: List of skill names that have been loaded

    Attributes:
        skill_registry: Mapping of skill names to SkillDefinition objects
        tools: List containing the load_skill tool
    """

    # Tool name to intercept
    TOOL_NAME = "load_skill"

    # State schema for LangGraph
    state_schema = LoadedSkillsState

    def __init__(
        self,
        skill_registry: dict[str, SkillDefinition] | None = None,
    ) -> None:
        """Initialize the middleware.

        Args:
            skill_registry: Optional custom skill registry. Defaults to SKILL_REGISTRY.
        """
        super().__init__()
        self.skill_registry = skill_registry or SKILL_REGISTRY

        # Build mapping of tool names to their skill
        self._tool_to_skill: dict[str, str] = {}
        for skill_name, skill in self.skill_registry.items():
            for t in skill.tools:
                tool_name = getattr(t, "name", str(t))
                self._tool_to_skill[tool_name] = skill_name

        # Create the load_skill tool
        self.tools = [self._create_load_skill_tool()]

        logger.info(
            "DynamicSkillLoaderMiddleware initialized",
            skill_count=len(self.skill_registry),
            skills=list(self.skill_registry.keys()),
            skill_tools=len(self._tool_to_skill),
        )

    def _create_load_skill_tool(self) -> Any:
        """Create the load_skill tool.

        The actual state update happens in awrap_tool_call, not in the tool itself.
        The tool just returns a simple acknowledgment.

        Returns:
            A LangChain tool for loading skills
        """

        @tool
        def load_skill(skill_name: str) -> str:
            """Load special tools from a skill.
            This is designed to access some specialized tools that is currently hidden
            but will be available to you after loading the skill. You should call them
            as tool calls instead of using execute_code tool.

            Args:
                skill_name: Name of the skill to load

            Returns:
                The tool will be available for you to call *directly*
            """
            # This is a placeholder - the middleware intercepts this
            # and handles state updates + returns proper instructions
            return f"Loading skill: {skill_name}"

        return load_skill

    def _build_skill_result(self, skill: SkillDefinition) -> str:
        """Build the result message for a loaded skill.

        Args:
            skill: The skill definition

        Returns:
            Formatted instructions string
        """
        # Build tool descriptions
        tool_descriptions = []
        for t in skill.tools:
            name = getattr(t, "name", str(t))
            desc = getattr(t, "description", "No description")
            # Truncate long descriptions
            if len(desc) > 200:
                desc = desc[:200] + "..."
            tool_descriptions.append(f"  - **{name}**: {desc}")

        tools_text = "\n".join(tool_descriptions)

        # Build instruction to read SKILL.md
        skill_md_instruction = ""
        if skill.skill_md_path:
            skill_md_instruction = (
                f"\n\n**IMPORTANT**: Read the skill documentation for detailed usage examples:\n"
                f"  Path: `{skill.skill_md_path}`\n"
                f"  Use the file read tool to read this file before using the skill tools."
            )

        return (
            f"# Skill Loaded: {skill.name}\n\n"
            f"{skill.description}\n\n"
            f"**Available tools:**\n{tools_text}"
            f"{skill_md_instruction}\n\n"
            f"You can now use these tools to help the user."
        )

    def wrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Any],
    ) -> Any:
        """Sync wrapper - pass through to handler."""
        tool_call = request.tool_call
        tool_name = tool_call.get("name")

        if tool_name != self.TOOL_NAME:
            return handler(request)

        # For sync, just run the tool normally (no state update)
        logger.warning(
            "[SKILL_LOADER] Sync execution detected. State update may not work. "
            "Use async execution for full functionality."
        )
        return handler(request)

    async def awrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        """Intercept load_skill calls to update state via Command.

        Args:
            request: Tool call request
            handler: Next handler in chain

        Returns:
            Command with state update and ToolMessage, or pass through for other tools
        """
        tool_call = request.tool_call
        tool_name = tool_call.get("name")

        # Pass through non-target tools
        if tool_name != self.TOOL_NAME:
            return await handler(request)

        tool_call_id = tool_call.get("id", "unknown")
        tool_args = tool_call.get("args", {})
        skill_name = tool_args.get("skill_name", "")

        logger.debug(
            "Intercepting load_skill call",
            tool_call_id=tool_call_id,
            skill_name=skill_name,
        )

        # Look up the skill
        skill = get_skill(skill_name)

        if not skill:
            available = list_skills()
            skill_names = [s["name"] for s in available]
            error_msg = (
                f"Error: Skill '{skill_name}' not found.\n\n"
                f"Available skills: {', '.join(skill_names)}\n\n"
                f"Use one of the available skill names to load it."
            )
            return ToolMessage(
                content=error_msg,
                tool_call_id=tool_call_id,
                name=self.TOOL_NAME,
            )

        # Build the result message
        result_message = self._build_skill_result(skill)

        logger.info(
            "Skill loaded via middleware",
            skill_name=skill_name,
            tool_count=len(skill.tools),
        )

        # Return Command to update state with loaded skill
        # Use a list since sets aren't JSON serializable
        return Command(
            update={
                LOADED_SKILLS_KEY: [skill_name],  # Will be merged/appended
                "messages": [
                    ToolMessage(
                        content=result_message,
                        tool_call_id=tool_call_id,
                        name=self.TOOL_NAME,
                    )
                ],
            },
        )

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        """Sync wrapper - filters tools based on loaded skills."""
        filtered_request = self._filter_tools(request)
        return handler(filtered_request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        """Filter tools before each model call based on loaded skills.

        This is where the magic happens: before the model sees the tools,
        we filter out tools from skills that haven't been loaded yet.

        Args:
            request: ModelRequest containing tools list and state
            handler: Next handler in middleware chain

        Returns:
            ModelResponse from the filtered request
        """
        filtered_request = self._filter_tools(request)
        return await handler(filtered_request)

    def _filter_tools(self, request: ModelRequest) -> ModelRequest:
        """Filter tools based on which skills are loaded.

        Args:
            request: Original ModelRequest

        Returns:
            ModelRequest with filtered tools list
        """
        # Get loaded skills from state
        state = request.state
        loaded_skills: set[str] = set()

        # Debug: log state structure
        logger.debug(
            "Filtering tools - checking state",
            state_type=type(state).__name__,
            state_keys=list(state.keys()) if hasattr(state, "keys") else "N/A",
        )

        # Try to get loaded_skills from state
        raw_skills = None
        if state and hasattr(state, "get"):
            raw_skills = state.get(LOADED_SKILLS_KEY)
        elif state and isinstance(state, dict):
            raw_skills = state.get(LOADED_SKILLS_KEY)

        if raw_skills:
            if isinstance(raw_skills, (list, tuple, set, frozenset)):
                loaded_skills = set(raw_skills)
            else:
                loaded_skills = {raw_skills}

            logger.info(
                "Found loaded skills in state",
                loaded_skills=list(loaded_skills),
            )

        # Filter tools: keep non-skill tools + tools from loaded skills
        original_tools = request.tools or []
        filtered_tools = []

        for t in original_tools:
            tool_name = getattr(t, "name", None) or (t.get("name") if isinstance(t, dict) else str(t))

            # Check if this tool belongs to a skill
            skill_name = self._tool_to_skill.get(tool_name)

            if skill_name is None:
                # Not a skill tool - always include (e.g., load_skill, execute_code, etc.)
                filtered_tools.append(t)
            elif skill_name in loaded_skills:
                # Skill tool and skill is loaded - include
                filtered_tools.append(t)
            # else: skill tool but skill not loaded - exclude

        hidden_count = len(original_tools) - len(filtered_tools)
        if hidden_count > 0:
            logger.info(
                "Filtered skill tools from model request",
                original_count=len(original_tools),
                filtered_count=len(filtered_tools),
                hidden_count=hidden_count,
                loaded_skills=list(loaded_skills),
            )

        # Return modified request with filtered tools
        return request.override(tools=filtered_tools)

    def get_all_skill_tools(self) -> list[Any]:
        """Get all tools from all registered skills.

        Use this to pre-register all skill tools with ToolNode at agent creation.
        This ensures tools are available when needed, even though the agent
        learns about them only when loading the skill.

        Returns:
            Flat list of all tools from all skills
        """
        all_tools = []
        for skill in self.skill_registry.values():
            all_tools.extend(skill.tools)
        return all_tools

    def get_skill_tool_names(self) -> set[str]:
        """Get names of all tools from all registered skills.

        Returns:
            Set of tool names
        """
        return set(self._tool_to_skill.keys())
