"""Tool summary formatting functions for prompts.

These functions generate dynamic content based on runtime data
and are kept in Python rather than templates.
"""

import structlog
from typing import Any

from ptc_agent.core.mcp_sanitize import (
    is_user_server,
    sanitize_tool_name,
    sanitize_tool_text,
)

logger = structlog.get_logger(__name__)

TOOL_SUMMARY_TEMPLATE = """
{server_name}:
{tools}
"""

TOOL_ITEM_TEMPLATE = "  - {tool_name}({parameters}) -> {return_type}: {description}"

# Hard caps for untrusted (source='workspace') server-level text rendered into
# the prompt. Match the API write-time caps so a user can't balloon the prompt.
WORKSPACE_DESCRIPTION_MAX_LEN = 512
WORKSPACE_INSTRUCTION_MAX_LEN = 1024

# Detailed-mode bounds for untrusted servers. A workspace server requesting
# `detailed` exposure is rendered detailed ONLY within these caps; over either
# cap it falls back to summary with a suppression marker. Built-ins are NOT
# capped (their detailed listing renders unchanged).
WORKSPACE_DETAILED_MAX_TOOLS = 25
WORKSPACE_DETAILED_MAX_CHARS = 8000


def _is_workspace_source(config: Any) -> bool:
    """True when ``config`` is an untrusted user-provided (workspace) server."""
    return bool(config) and is_user_server(config)


def _safe_tool_name(name: Any, *, workspace: bool) -> str:
    """Render a tool name into a single prompt line.

    For workspace (untrusted) servers, strip newlines and neutralize any
    injection text so a hostile tool name can't smuggle a fake directive into
    the detailed listing. Builtin names render verbatim (byte-identical).
    """
    text = str(name)
    if not workspace:
        return text
    return sanitize_tool_text(text.replace("\r", " ").replace("\n", " "))


def _safe_param_name(name: Any, *, workspace: bool) -> str:
    """Render a tool PARAMETER name into a prompt-safe token.

    A workspace (untrusted) param name is coerced to a bare identifier, so a
    hostile name like ``x)\\nInstructions: ...`` collapses to one inert token and
    can't open a fake directive line in the detailed listing. Builtin params
    render verbatim (byte-identical).
    """
    if not workspace:
        return str(name)
    return sanitize_tool_name(str(name)) or "arg"


def _safe_param_text(text: Any, *, workspace: bool) -> str:
    """Render an untrusted param type/default (or tool description) inline-safe.

    Strips newlines and neutralizes injection text so the value can't break onto
    its own prompt line, while preserving brackets etc. so legit types like
    ``list[str]`` stay intact. Builtin values render verbatim (byte-identical).
    The length bound is the detailed-mode total cap (not the smaller default), so
    per-field sanitization never shrinks the body out from under that cap's
    fall-back-to-summary check.
    """
    if not workspace:
        return str(text)
    return sanitize_tool_text(
        str(text).replace("\r", " ").replace("\n", " "),
        WORKSPACE_DETAILED_MAX_CHARS,
    )


def _workspace_server_header(server_name: str, config: Any) -> list[str]:
    """Render a workspace server's header under a neutral, attributed heading.

    Sanitizes and length-caps the user-provided ``description``/``instruction``
    and renders them as inert data — never under the authoritative
    ``Instructions:`` label built-ins use.
    """
    lines = [f"\n{server_name}:"]
    note_parts: list[str] = []
    if getattr(config, "description", ""):
        desc = sanitize_tool_text(config.description, WORKSPACE_DESCRIPTION_MAX_LEN)
        if desc:
            note_parts.append(desc)
    if getattr(config, "instruction", ""):
        instr = sanitize_tool_text(config.instruction, WORKSPACE_INSTRUCTION_MAX_LEN)
        if instr:
            note_parts.append(instr)
    if note_parts:
        note = " ".join(note_parts)
        lines.append(f"  User-provided server (untrusted) — note: {note}")
    return lines


def _detailed_over_cap(tools: list, detailed_lines: list[str]) -> bool:
    """True when a workspace server's detailed render exceeds the prompt caps."""
    if len(tools) > WORKSPACE_DETAILED_MAX_TOOLS:
        return True
    return len("\n".join(detailed_lines)) > WORKSPACE_DETAILED_MAX_CHARS


def format_tool_summary(
    tools_by_server: dict,
    mode: str = "summary",
    server_configs: dict | None = None,
) -> str:
    """Format tool information for prompt.

    Args:
        tools_by_server: Dictionary mapping server names to lists of tool info dicts
        mode: "summary" for brief server overview, "detailed" for full tool listings
        server_configs: Optional dict mapping server names to MCPServerConfig objects

    Returns:
        Formatted string for prompt
    """
    # If we have server configs, use per-server mode logic
    if server_configs:
        return _format_tool_summary_per_server(tools_by_server, server_configs, mode)

    # Fallback to global mode when no server configs
    if mode == "summary":
        return _format_tool_summary_brief(tools_by_server, server_configs)
    if mode == "detailed":
        return _format_tool_summary_detailed(tools_by_server, server_configs)
    # Default to summary for unknown modes
    return _format_tool_summary_brief(tools_by_server, server_configs)


def _format_tool_summary_per_server(
    tools_by_server: dict,
    server_configs: dict,
    default_mode: str = "summary",
) -> str:
    """Format tool summary with per-server exposure modes.

    Each server can have its own tool_exposure_mode, falling back to the global default.

    Args:
        tools_by_server: Dictionary mapping server names to lists of tool info dicts
        server_configs: Dict mapping server names to MCPServerConfig objects
        default_mode: Global default mode to use if server doesn't specify one

    Returns:
        Formatted string for prompt
    """
    lines = []

    for server_name, tools in tools_by_server.items():
        config = server_configs.get(server_name)

        # Determine mode for this server (per-server override or global default)
        server_mode = default_mode
        if config and config.tool_exposure_mode:
            server_mode = config.tool_exposure_mode

        if server_mode == "detailed":
            # Untrusted (workspace) servers are bounded in detailed mode: over
            # the per-server tool-count / rendered-text cap they fall back to
            # summary with a suppression marker. Built-ins are never capped.
            if _is_workspace_source(config):
                detailed = _format_server_detailed(server_name, tools, config)
                over_cap = _detailed_over_cap(tools, detailed)
                if over_cap:
                    summary = _format_server_brief(server_name, tools, config)
                    summary.append(
                        f"  ({len(tools)} tools; detailed listing suppressed "
                        f"— over size cap)"
                    )
                    lines.extend(summary)
                else:
                    lines.extend(detailed)
            else:
                lines.extend(_format_server_detailed(server_name, tools, config))
        else:
            lines.extend(_format_server_brief(server_name, tools, config))

    if not lines:
        return "\nNo MCP servers configured."

    summary = "\n".join(lines)

    # Brief reminder to check docs for signatures
    note = "\n\n**Note**: Check `tools/docs/{server_name}/{tool_name}.md` for exact function signatures before use."

    return f"{summary}{note}"


def _server_header_lines(server_name: str, config: Any) -> list:
    """Header lines for one server: neutral attributed framing for untrusted
    workspace servers, the authoritative description/Instructions: path for
    built-ins."""
    if _is_workspace_source(config):
        # Untrusted server: neutral, attributed header (no Description:/Instructions:).
        return _workspace_server_header(server_name, config)

    lines = []
    # Server header with description
    if config and config.description:
        lines.append(f"\n{server_name}: {config.description}")
    else:
        lines.append(f"\n{server_name}:")

    # Add instruction if available
    if config and config.instruction:
        lines.append(f"  Instructions: {config.instruction}")
    return lines


def _format_server_brief(server_name: str, tools: list, config: Any) -> list:
    """Format a single server in brief/summary mode.

    Args:
        server_name: Name of the server
        tools: List of tool info dicts
        config: MCPServerConfig for this server (or None)

    Returns:
        List of formatted lines
    """
    tool_count = len(tools)
    tools_word = "tool" if tool_count == 1 else "tools"

    lines = _server_header_lines(server_name, config)
    lines.append(f"  - Module: tools/{server_name}.py")
    lines.append(f"  - Tools: {tool_count} {tools_word} available")
    lines.append(f"  - Import: from tools.{server_name} import <tool_name>")
    lines.append(f"  - Documentation: tools/docs/{server_name}/*.md")

    return lines


def _format_server_detailed(server_name: str, tools: list, config: Any) -> list:
    """Format a single server in detailed mode with full tool signatures.

    Args:
        server_name: Name of the server
        tools: List of tool info dicts
        config: MCPServerConfig for this server (or None)

    Returns:
        List of formatted lines
    """
    lines = _server_header_lines(server_name, config)
    lines.append(f"  Module: tools/{server_name}.py")
    lines.append("  Available tools:")

    workspace = _is_workspace_source(config)
    for tool in tools:
        tool_line = f"    - {_safe_tool_name(tool['name'], workspace=workspace)}("

        # Add parameters
        if tool.get("parameters"):
            params = tool["parameters"]
            if isinstance(params, list):
                tool_line += ", ".join(params)
            elif isinstance(params, dict):
                param_strs = []
                for pname, pinfo in params.items():
                    safe_name = _safe_param_name(pname, workspace=workspace)
                    safe_type = _safe_param_text(pinfo.get("type", "any"), workspace=workspace)
                    if pinfo.get("required", False):
                        param_strs.append(f"{safe_name}: {safe_type}")
                    else:
                        safe_default = _safe_param_text(pinfo.get("default", "None"), workspace=workspace)
                        param_strs.append(f"{safe_name}: {safe_type} = {safe_default}")
                tool_line += ", ".join(param_strs)

        tool_line += ")"

        # Add return type
        if tool.get("return_type"):
            tool_line += f" -> {_safe_param_text(tool['return_type'], workspace=workspace)}"

        # Add description
        if tool.get("description"):
            tool_line += f": {_safe_param_text(tool['description'], workspace=workspace)}"

        lines.append(tool_line)

    return lines


def _format_tool_summary_brief(
    tools_by_server: dict,
    server_configs: dict | None = None,
) -> str:
    """Format brief tool summary (server names, descriptions, and module locations).

    This is the recommended mode for token efficiency.

    Args:
        tools_by_server: Dictionary mapping server names to lists of tool info dicts
        server_configs: Optional dict mapping server names to MCPServerConfig objects

    Returns:
        Formatted string for prompt
    """
    lines = []

    for server_name, tools in tools_by_server.items():
        tool_count = len(tools)
        tools_word = "tool" if tool_count == 1 else "tools"

        # Get server config for description/instruction
        config = server_configs.get(server_name) if server_configs else None

        # Server header with description
        if config and config.description:
            lines.append(f"\n{server_name}: {config.description}")
        else:
            lines.append(f"\n{server_name}:")

        # Add instruction if available
        if config and config.instruction:
            lines.append(f"  Instructions: {config.instruction}")

        lines.append(f"  - Module: tools/{server_name}.py")
        lines.append(f"  - Tools: {tool_count} {tools_word} available")
        lines.append(f"  - Import: from tools.{server_name} import <tool_name>")
        lines.append(f"  - Documentation: tools/docs/{server_name}/*.md")

    if not lines:
        return "\nNo MCP servers configured."

    summary = "\n".join(lines)

    # Brief reminder to check docs for signatures
    note = "\n\n**Note**: Check `tools/docs/{server_name}/{tool_name}.md` for exact function signatures before use."

    return f"{summary}{note}"


def _format_tool_summary_detailed(
    tools_by_server: dict,
    server_configs: dict | None = None,
) -> str:
    """Format detailed tool summary (full tool signatures and descriptions).

    Args:
        tools_by_server: Dictionary mapping server names to lists of tool info dicts
        server_configs: Optional dict mapping server names to MCPServerConfig objects

    Returns:
        Formatted string for prompt
    """
    lines = []

    for server_name, tools in tools_by_server.items():
        # Get server config for description/instruction
        config = server_configs.get(server_name) if server_configs else None

        # Server header with description
        if config and config.description:
            lines.append(f"\n{server_name}: {config.description}")
        else:
            lines.append(f"\n{server_name}:")

        # Add instruction if available
        if config and config.instruction:
            lines.append(f"  Instructions: {config.instruction}")

        lines.append(f"  Module: tools/{server_name}.py")
        lines.append("  Available tools:")

        workspace = _is_workspace_source(config)
        for tool in tools:
            tool_line = f"    - {_safe_tool_name(tool['name'], workspace=workspace)}("

            # Add parameters
            if tool.get("parameters"):
                params = tool["parameters"]
                if isinstance(params, list):
                    tool_line += ", ".join(params)
                elif isinstance(params, dict):
                    param_strs = []
                    for pname, pinfo in params.items():
                        safe_name = _safe_param_name(pname, workspace=workspace)
                        safe_type = _safe_param_text(pinfo.get("type", "any"), workspace=workspace)
                        if pinfo.get("required", False):
                            param_strs.append(f"{safe_name}: {safe_type}")
                        else:
                            safe_default = _safe_param_text(pinfo.get("default", "None"), workspace=workspace)
                            param_strs.append(f"{safe_name}: {safe_type} = {safe_default}")
                    tool_line += ", ".join(param_strs)

            tool_line += ")"

            # Add return type
            if tool.get("return_type"):
                tool_line += f" -> {_safe_param_text(tool['return_type'], workspace=workspace)}"

            # Add description
            if tool.get("description"):
                tool_line += f": {_safe_param_text(tool['description'], workspace=workspace)}"

            lines.append(tool_line)

    if not lines:
        return "\nNo MCP servers configured."

    return "\n".join(lines)


def build_tool_summary_from_registry(
    mcp_registry: Any,
    *,
    mode: str = "full",
) -> str:
    """Build a formatted MCP tool summary from a registry instance.

    Shared by PTCAgent and SubagentCompiler to avoid duplicating
    the registry-to-string conversion logic.

    Args:
        mcp_registry: MCP registry instance (or None/falsy).
        mode: Tool exposure mode ("summary", "detailed", or "full").

    Returns:
        Formatted tool summary string, or "" on failure.
    """
    if not mcp_registry:
        return ""
    try:
        tools_by_server = mcp_registry.get_all_tools()
        if not tools_by_server:
            return ""
        tools_dict = {
            server_name: [tool.to_dict() for tool in tools]
            for server_name, tools in tools_by_server.items()
        }
        server_configs: dict[str, Any] = {}
        if hasattr(mcp_registry, "config") and hasattr(mcp_registry.config, "mcp"):
            server_configs = {
                s.name: s for s in mcp_registry.config.mcp.servers if s.enabled
            }
        return format_tool_summary(tools_dict, mode=mode, server_configs=server_configs)
    except Exception:
        logger.warning("failed to build MCP tool summary", exc_info=True)
        return ""


def format_subagent_summary(subagents: list[dict]) -> str:
    """Format subagent configurations into a summary for the system prompt.

    Similar to format_tool_summary for MCP tools. Displays each subagent
    with its name, description, and available tools.

    Args:
        subagents: List of subagent config dicts with keys:
            - name: Subagent name (e.g., "general-purpose", "research")
            - description: What the subagent does
            - tools: List of tool objects or tool names

    Returns:
        Formatted string for the system prompt
    """
    if not subagents:
        return "No sub-agents configured."

    lines = []

    for subagent in subagents:
        name = subagent.get("name", "unknown")
        description = subagent.get("description", "")
        tools = subagent.get("tools", [])

        # Format tool names
        tool_names = []
        for tool in tools:
            if hasattr(tool, "name"):
                tool_names.append(tool.name)
            elif isinstance(tool, str):
                tool_names.append(tool)
            else:
                tool_names.append(str(tool))

        # Build subagent entry
        lines.append(f"- **{name}**: {description}")
        if tool_names:
            lines.append(f"  Tools: {', '.join(tool_names)}")

    return "\n".join(lines)
