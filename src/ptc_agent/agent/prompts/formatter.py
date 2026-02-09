"""Tool summary formatting functions for prompts.

These functions generate dynamic content based on runtime data
and are kept in Python rather than templates.
"""

from typing import Any

TOOL_SUMMARY_TEMPLATE = """
{server_name}:
{tools}
"""

TOOL_ITEM_TEMPLATE = "  - {tool_name}({parameters}) -> {return_type}: {description}"


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
            lines.extend(_format_server_detailed(server_name, tools, config))
        else:
            lines.extend(_format_server_brief(server_name, tools, config))

    if not lines:
        return "\nNo MCP servers configured."

    summary = "\n".join(lines)

    # Brief reminder to check docs for signatures
    note = "\n\n**Note**: Check `tools/docs/{server_name}/{tool_name}.md` for exact function signatures before use."

    return f"{summary}{note}"


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
    lines = []

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
    lines = []

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

    for tool in tools:
        tool_line = f"    - {tool['name']}("

        # Add parameters
        if tool.get("parameters"):
            params = tool["parameters"]
            if isinstance(params, list):
                tool_line += ", ".join(params)
            elif isinstance(params, dict):
                param_strs = []
                for pname, pinfo in params.items():
                    ptype = pinfo.get("type", "any")
                    required = pinfo.get("required", False)
                    if required:
                        param_strs.append(f"{pname}: {ptype}")
                    else:
                        default = pinfo.get("default", "None")
                        param_strs.append(f"{pname}: {ptype} = {default}")
                tool_line += ", ".join(param_strs)

        tool_line += ")"

        # Add return type
        if tool.get("return_type"):
            tool_line += f" -> {tool['return_type']}"

        # Add description
        if tool.get("description"):
            tool_line += f": {tool['description']}"

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

        for tool in tools:
            tool_line = f"    - {tool['name']}("

            # Add parameters
            if tool.get("parameters"):
                params = tool["parameters"]
                if isinstance(params, list):
                    tool_line += ", ".join(params)
                elif isinstance(params, dict):
                    param_strs = []
                    for pname, pinfo in params.items():
                        ptype = pinfo.get("type", "any")
                        required = pinfo.get("required", False)
                        if required:
                            param_strs.append(f"{pname}: {ptype}")
                        else:
                            default = pinfo.get("default", "None")
                            param_strs.append(f"{pname}: {ptype} = {default}")
                    tool_line += ", ".join(param_strs)

            tool_line += ")"

            # Add return type
            if tool.get("return_type"):
                tool_line += f" -> {tool['return_type']}"

            # Add description
            if tool.get("description"):
                tool_line += f": {tool['description']}"

            lines.append(tool_line)

    if not lines:
        return "\nNo MCP servers configured."

    return "\n".join(lines)


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
