"""Dynamic skill loader middleware — unified skill source of truth.

This middleware handles two skill sources:
- **Registry skills** (``SKILL_REGISTRY``) — our provided skills with tool gating
- **Discovered skills** (filesystem scan) — user-installed skills without tools

Behavior differs by agent mode:

- **PTC mode**: No ``LoadSkill`` tool. Auto-detects when the agent reads a
  skill's SKILL.md via the ``Read`` tool and silently loads the skill's tools.
  Optionally scans sandbox filesystem for user-installed skills via ``before_agent``.
- **Flash mode**: Exposes ``LoadSkill`` tool. Embeds SKILL.md content inline
  (Flash has no filesystem). No filesystem scanning.

Both modes inject a combined skill manifest into the system message listing
all available skills (registry + discovered).

Architecture:
- Tools from all skills are pre-registered with ToolNode at agent creation
- Before each model call, ``awrap_model_call`` filters tools based on loaded skills
- PTC: Reading a SKILL.md triggers auto-load via Command state update
- Flash: ``LoadSkill`` tool call triggers load + inline SKILL.md content

Usage:
    from ptc_agent.agent.middleware.skills import SkillsMiddleware

    middleware = SkillsMiddleware(mode="ptc", backend=backend, sources=[...])
    middleware = SkillsMiddleware(mode="flash")  # LoadSkill + manifest
"""

import asyncio
import operator
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

import structlog
from langchain.agents.middleware import AgentMiddleware, AgentState
from langchain.agents.middleware.types import (
    ModelRequest,
    ModelResponse,
    PrivateStateAttr,
)
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from langgraph.types import Command
from typing_extensions import NotRequired

from ptc_agent.agent.middleware._utils import append_to_system_message
from ptc_agent.agent.middleware.skills.content import load_skill_content
from ptc_agent.agent.middleware.skills.discovery import (
    SkillMetadata,
    adiscover_skills,
)
from ptc_agent.agent.middleware.skills.registry import (
    SkillDefinition,
    SkillMode,
    get_skill,
    get_skill_registry,
    list_skills,
)

logger = structlog.get_logger(__name__)

# State key for tracking loaded skills
LOADED_SKILLS_KEY = "loaded_skills"


class LoadedSkillsState(AgentState):
    """State schema for tracking loaded skills."""

    loaded_skills: NotRequired[Annotated[list[str], operator.add]]
    discovered_skills: NotRequired[Annotated[list[SkillMetadata], PrivateStateAttr]]


class SkillsMiddleware(AgentMiddleware):
    """Middleware that provides dynamic skill loading with tool filtering.

    Behavior varies by mode:
    - PTC: Auto-loads skills when agent reads a SKILL.md file. No LoadSkill tool.
    - Flash: Exposes LoadSkill tool with inline SKILL.md. Injects skill manifest
      into system message so the agent knows what skills are available.

    Attributes:
        skill_registry: Mapping of skill names to SkillDefinition objects
        tools: List containing the LoadSkill tool (Flash) or empty (PTC)
    """

    # Tool name to intercept (Flash mode only)
    TOOL_NAME = "LoadSkill"

    # Tool name and filename used for PTC auto-load detection
    _READ_TOOL_NAME = "Read"
    _SKILL_MD_FILENAME = "SKILL.md"

    # State schema for LangGraph
    state_schema = LoadedSkillsState

    def __init__(
        self,
        skill_registry: dict[str, SkillDefinition] | None = None,
        mode: SkillMode | None = None,
        backend: Any | None = None,
        sources: list[str] | None = None,
        known_skills: dict[str, SkillMetadata] | None = None,
    ) -> None:
        """Initialize the middleware.

        Args:
            skill_registry: Optional custom skill registry. Defaults to SKILL_REGISTRY
                filtered by mode.
            mode: Agent mode. Determines behavior:
                - "ptc": No LoadSkill tool, auto-load on Read of SKILL.md
                - "flash": LoadSkill tool exposed, skill manifest in system message
            backend: Optional SandboxBackend for filesystem scanning of user-installed
                skills (PTC mode only).
            sources: Optional list of sandbox paths to scan for SKILL.md files.
            known_skills: Pre-parsed skill metadata from the upload manifest, keyed
                by skill directory name. Skills present here skip re-downloading
                during filesystem scanning.
        """
        super().__init__()
        self._mode = mode
        self._backend = backend
        self._sources = sources or []
        self._known_skills = known_skills or {}
        self.skill_registry = skill_registry or get_skill_registry(mode)

        # Build mapping of tool names → skill names (a tool can belong to multiple skills)
        self._tool_to_skills: dict[str, set[str]] = {}
        for skill_name, skill_def in self.skill_registry.items():
            for t in skill_def.tools:
                tool_name = getattr(t, "name", str(t))
                self._tool_to_skills.setdefault(tool_name, set()).add(skill_name)

        # Build mapping of SKILL.md paths → skill names (for PTC auto-load)
        self._skill_md_to_name: dict[str, str] = {}
        for skill_name, skill_def in self.skill_registry.items():
            if skill_def.skill_md_path:
                self._skill_md_to_name[skill_def.skill_md_path] = skill_name

        # PTC mode: no LoadSkill tool (auto-loads on Read)
        # Flash mode: expose LoadSkill tool
        if mode == "ptc":
            self.tools: list[Any] = []
        else:
            self.tools = [self._create_load_skill_tool()]

        logger.debug(
            "SkillsMiddleware initialized",
            mode=mode,
            skill_count=len(self.skill_registry),
            skills=list(self.skill_registry.keys()),
            skill_tools=len(self._tool_to_skills),
            has_load_skill_tool=len(self.tools) > 0,
            has_filesystem_scan=bool(self._backend and self._sources),
            known_skills_count=len(self._known_skills),
        )

    def _create_load_skill_tool(self) -> Any:
        """Create the LoadSkill tool (Flash mode only).

        The actual state update happens in awrap_tool_call, not in the tool itself.

        Returns:
            A LangChain tool for loading skills
        """

        @tool("LoadSkill")
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
            # Placeholder - middleware intercepts and handles state updates
            return f"Loading skill: {skill_name}"

        return load_skill

    async def abefore_agent(self, state: Any, runtime: Any) -> dict | None:
        """Scan filesystem for user-installed skills (async, PTC mode only).

        Uses optimized discovery: skills already present in ``_known_skills``
        (from the upload manifest) are returned without re-downloading.
        Only truly new skills trigger a download.

        Runs on every user message so newly deployed skills are picked up
        mid-thread. The known_skills cache keeps re-scans cheap (~100ms
        with 0 downloads when all skills are already known).
        """
        if not self._backend or not self._sources:
            return {"discovered_skills": []}

        all_skills: dict[str, SkillMetadata] = {}
        for source_path in self._sources:
            for skill in await adiscover_skills(
                self._backend, source_path, self._known_skills
            ):
                all_skills[skill["name"]] = skill

        # Filter out registry skills — registry is source of truth for those
        unregistered = [
            s for name, s in all_skills.items() if name not in self.skill_registry
        ]
        return {"discovered_skills": unregistered}

    def _build_combined_manifest(self, state: Any) -> str | None:
        """Build a combined skill manifest for the system message.

        Merges registry skills and filesystem-discovered skills into a single
        manifest. Works for both PTC and Flash modes.

        Args:
            state: Agent state containing discovered_skills

        Returns:
            Formatted manifest string, or None if no skills to list
        """
        lines = ["## Available Skills", ""]

        if self._mode == "ptc":
            lines.append(
                "Skills provide specialized capabilities. "
                "To activate, read `.agents/skills/{name}/SKILL.md`."
            )
        else:
            lines.append("Call `LoadSkill` with the skill name to activate its tools.")
        lines.append("")

        has_skills = False

        # 1. Registry skills (excluding hidden)
        for skill_def in self.skill_registry.values():
            if skill_def.exposure == "hidden":
                continue
            has_skills = True
            entry = f"- **{skill_def.name}**: {skill_def.description}"
            tool_names = skill_def.get_tool_names()
            if tool_names:
                entry += f" (tools: {', '.join(tool_names)})"
            lines.append(entry)

        # 2. Discovered (unregistered) skills from filesystem
        discovered = state.get("discovered_skills", []) if state else []
        for skill in discovered:
            has_skills = True
            if skill.get("confirmed", True):
                entry = f"- **{skill['name']}**: {skill['description']}"
                if skill.get("allowed_tools"):
                    entry += f" (tools: {', '.join(skill['allowed_tools'])})"
            else:
                entry = f"- **{skill['name']}** *(unconfirmed)*"
            lines.append(entry)

        return "\n".join(lines) if has_skills else None

    def _match_skill_from_read(self, tool_name: str, tool_args: dict) -> str | None:
        """Check if a Read tool call targets a registered skill's SKILL.md.

        Uses a two-stage filter for efficiency:
        1. O(1) fast-path reject if not reading any SKILL.md
        2. O(n) match against registered skill paths (n = skill count, typically < 10)

        Args:
            tool_name: Name of the tool being called
            tool_args: Arguments passed to the tool

        Returns:
            Skill name if matched, None otherwise
        """
        if tool_name != self._READ_TOOL_NAME:
            return None
        file_path: str = tool_args.get("file_path", "")
        if not file_path.endswith(self._SKILL_MD_FILENAME):
            return None  # Fast path: not reading any SKILL.md
        # Only reach here when reading *some* SKILL.md — check if it's registered
        for md_path, skill_name in self._skill_md_to_name.items():
            if file_path == md_path or file_path.endswith("/" + md_path):
                return skill_name
        return None

    async def _build_skill_result(self, skill: SkillDefinition) -> str:
        """Build the result message for a loaded skill.

        Args:
            skill: The skill definition

        Returns:
            Formatted instructions string
        """
        tools_text = skill.format_tool_descriptions()

        # Build SKILL.md section based on mode
        skill_md_section = ""
        if skill.skill_md_path:
            if self._mode != "ptc":
                # Flash mode: embed content directly (no filesystem access)
                content = await asyncio.to_thread(
                    load_skill_content, skill.name, mode=self._mode
                )
                if content:
                    skill_md_section = f"\n\n**Skill Documentation:**\n{content}"
            else:
                # PTC mode: point to sandbox path (agent has filesystem)
                skill_md_section = (
                    f"\n\n**IMPORTANT**: Read the skill documentation for detailed usage examples:\n"
                    f"  Path: `{skill.skill_md_path}`\n"
                    f"  Use the file read tool to read this file before using the skill tools."
                )

        return (
            f"# Skill Loaded: {skill.name}\n\n"
            f"{skill.description}\n\n"
            f"**Available tools:**\n{tools_text}"
            f"{skill_md_section}\n\n"
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
        """Intercept tool calls for skill loading.

        - PTC mode: Auto-load skill when agent reads a SKILL.md
        - Flash mode: Handle LoadSkill tool calls

        Args:
            request: Tool call request
            handler: Next handler in chain

        Returns:
            Command with state update and ToolMessage, or pass through
        """
        tool_call = request.tool_call
        tool_name = tool_call.get("name")

        # Pass through non-target tools
        if tool_name != self.TOOL_NAME:
            result = await handler(request)

            # PTC mode: auto-load skill when agent reads a SKILL.md
            if self._mode == "ptc" and self._skill_md_to_name:
                tool_args = tool_call.get("args", {})
                matched_skill = self._match_skill_from_read(tool_name, tool_args)
                if matched_skill and isinstance(result, ToolMessage):
                    logger.info(
                        "Auto-loading skill from SKILL.md read",
                        skill_name=matched_skill,
                    )
                    return Command(
                        update={
                            LOADED_SKILLS_KEY: [matched_skill],
                            "messages": [result],  # preserve original Read result
                        },
                    )

            return result

        # --- Flash mode: handle LoadSkill tool call ---
        tool_call_id = tool_call.get("id", "unknown")
        tool_args = tool_call.get("args", {})
        skill_name = tool_args.get("skill_name", "")

        logger.debug(
            "Intercepting load_skill call",
            tool_call_id=tool_call_id,
            skill_name=skill_name,
        )

        # Look up the skill (filtered by mode if set)
        skill = get_skill(skill_name, mode=self._mode)

        # Block hidden skills from being loaded via LoadSkill
        # (they can only be activated via additionalContext)
        if skill and skill.exposure == "hidden":
            skill = None

        if not skill:
            available = list_skills(mode=self._mode)
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
        result_message = await self._build_skill_result(skill)

        logger.info(
            "Skill loaded via middleware",
            skill_name=skill_name,
            tool_count=len(skill.tools),
        )

        # Return Command to update state with loaded skill
        return Command(
            update={
                LOADED_SKILLS_KEY: [skill_name],
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

        Also injects the skill manifest into the system message (Flash mode).

        Args:
            request: Original ModelRequest

        Returns:
            ModelRequest with filtered tools list (and possibly updated system message)
        """
        # Get loaded skills from state
        state = request.state
        loaded_skills: set[str] = set()

        logger.debug(
            "Filtering tools - checking state",
            state_type=type(state).__name__,
            state_keys=list(state.keys()) if hasattr(state, "keys") else "N/A",
        )

        # Try to get loaded_skills from state
        raw_skills = None
        if state and hasattr(state, "get"):
            raw_skills = state.get(LOADED_SKILLS_KEY)

        if raw_skills:
            if isinstance(raw_skills, (list, tuple, set, frozenset)):
                loaded_skills = set(raw_skills)
            else:
                loaded_skills = {raw_skills}

            logger.debug(
                "Found loaded skills in state",
                loaded_skills=list(loaded_skills),
            )

        # Filter tools: keep non-skill tools + tools from loaded skills
        original_tools = request.tools or []
        filtered_tools = []

        for t in original_tools:
            tool_name = getattr(t, "name", None) or (
                t.get("name") if isinstance(t, dict) else str(t)
            )

            # Check if this tool belongs to any skill(s)
            skill_names = self._tool_to_skills.get(tool_name)

            if skill_names is None:
                # Not a skill tool - always include
                filtered_tools.append(t)
            elif skill_names & loaded_skills:
                # Skill tool and at least one owning skill is loaded - include
                filtered_tools.append(t)
            # else: skill tool but no owning skill loaded - exclude

        hidden_count = len(original_tools) - len(filtered_tools)
        if hidden_count > 0:
            logger.debug(
                "Skill tools hidden from model request",
                hidden_count=hidden_count,
            )

        # Build filtered request
        filtered = request.override(tools=filtered_tools)

        # Inject combined skill manifest into system message (both PTC and Flash)
        manifest = self._build_combined_manifest(request.state)
        if manifest:
            new_sys = append_to_system_message(filtered.system_message, manifest)
            filtered = filtered.override(system_message=new_sys)

        return filtered

    def get_all_skill_tools(self) -> list[Any]:
        """Get all tools from all registered skills.

        Use this to pre-register all skill tools with ToolNode at agent creation.

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
        return set(self._tool_to_skills.keys())
