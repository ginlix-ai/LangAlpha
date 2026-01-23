"""Tool Function Generator - Convert MCP tool schemas to Python functions."""

from pathlib import Path
from typing import Any

import structlog

from ptc_agent.config.core import MCPServerConfig

from .mcp_registry import MCPToolInfo

logger = structlog.get_logger(__name__)


class ToolFunctionGenerator:
    """Generates Python function code from MCP tool schemas."""

    def generate_tool_module(
        self, server_name: str, tools: list[MCPToolInfo]
    ) -> str:
        """Generate a complete Python module for a server's tools.

        Args:
            server_name: Name of the MCP server
            tools: List of tools from this server

        Returns:
            Complete Python module code as string
        """
        logger.info(
            "Generating tool module",
            server=server_name,
            tool_count=len(tools),
        )

        code = f'''"""
Auto-generated tool functions for MCP server: {server_name}

This module provides Python functions that call tools on the {server_name} MCP server.
Functions are automatically generated from the MCP tool schemas.
"""

from typing import Any, List, Dict
import json

# Import MCP client
try:
    from .mcp_client import _call_mcp_tool
except ImportError:
    # Fallback for when mcp_client is not available
    def _call_mcp_tool(server_name: str, tool_name: str, arguments: dict[str, Any]) -> Any:
        raise NotImplementedError(
            "MCP client not initialized. "
            "This module must be used within a PTC sandbox with mcp_client.py installed."
        )


'''

        # Generate functions for each tool
        for tool in tools:
            code += self._generate_function(tool, server_name)
            code += "\n\n"

        return code

    def _generate_function(self, tool: MCPToolInfo, server_name: str) -> str:
        """Generate Python function for a single tool.

        Args:
            tool: Tool information from MCP server
            server_name: Name of the MCP server this tool belongs to

        Returns:
            Python function code
        """
        # Generate function signature
        func_name = tool.name.replace("-", "_").replace(".", "_")
        params = tool.get_parameters()

        # Build parameter list - required parameters must come before optional
        param_list = []

        # First add required parameters
        for param_name, param_info in params.items():
            if param_info["required"]:
                param_type = self._map_json_type_to_python(param_info["type"])
                param_list.append(f"{param_name}: {param_type}")

        # Then add optional parameters
        for param_name, param_info in params.items():
            if not param_info["required"]:
                param_type = self._map_json_type_to_python(param_info["type"])
                default = param_info.get("default")
                if default is None:
                    param_list.append(f"{param_name}: {param_type} | None = None")
                else:
                    default_repr = repr(default)
                    param_list.append(f"{param_name}: {param_type} = {default_repr}")

        param_str = ", ".join(param_list)

        # Generate docstring
        docstring = self._generate_docstring(tool, params)

        # Generate function body
        arg_dict_entries = [f'        "{param_name}": {param_name},' for param_name in params]

        args_dict = "\n".join(arg_dict_entries)

        # Extract return type from description for better type hints
        return_type, _ = self._extract_return_info(tool.description)

        return f'''def {func_name}({param_str}) -> {return_type}:
    """{docstring}"""
    arguments = {{
{args_dict}
    }}

    # Remove None values
    arguments = {{k: v for k, v in arguments.items() if v is not None}}

    return _call_mcp_tool("{server_name}", "{tool.name}", arguments)'''


    def _generate_docstring(
        self, tool: MCPToolInfo, params: dict[str, Any]
    ) -> str:
        """Generate docstring for a tool function.

        Args:
            tool: Tool information
            params: Parameter information

        Returns:
            Formatted docstring
        """
        lines = []

        # Add description
        if tool.description:
            # Escape backslashes to avoid syntax warnings in docstrings
            escaped_desc = tool.description.replace("\\", "\\\\")
            lines.append(escaped_desc)
            lines.append("")

        # Add parameters
        if params:
            lines.append("Args:")
            for param_name, param_info in params.items():
                param_desc = param_info.get("description", "")
                # Escape backslashes to avoid syntax warnings in docstrings
                escaped_desc = param_desc.replace("\\", "\\\\")
                param_type = param_info["type"]
                required = " (required)" if param_info["required"] else ""
                lines.append(f"    {param_name} ({param_type}){required}: {escaped_desc}")
            lines.append("")

        # Add returns - extract from description if available
        return_type, return_desc = self._extract_return_info(tool.description)
        lines.append("Returns:")
        # Format multiline return descriptions properly
        return_lines = return_desc.split("\n")
        first_line = return_lines[0].strip()
        if return_type != "Any":
            lines.append(f"    {return_type}: {first_line}")
        else:
            lines.append(f"    {first_line}")
        # Add remaining lines with proper indentation
        for line in return_lines[1:]:
            stripped = line.strip()
            if stripped:
                lines.append(f"    {stripped}")
        lines.append("")

        # Add example
        example_args = []
        for param_name, param_info in params.items():
            if param_info["required"]:
                example_val = self._generate_example_value(param_info["type"])
                example_args.append(f"{param_name}={example_val}")

        if example_args:
            func_name = tool.name.replace("-", "_").replace(".", "_")
            example_call = f"{func_name}({', '.join(example_args[:2])})"  # Limit to 2 args
            lines.append("Example:")
            lines.append(f"    result = {example_call}")

        return "\n    ".join(lines)

    def _map_json_type_to_python(self, json_type: str) -> str:
        """Map JSON schema type to Python type hint.

        Args:
            json_type: JSON schema type

        Returns:
            Python type hint string
        """
        type_map = {
            "string": "str",
            "number": "float",
            "integer": "int",
            "boolean": "bool",
            "array": "List",
            "object": "Dict",
            "null": "None",
        }

        return type_map.get(json_type, "Any")

    def _generate_example_value(self, param_type: str) -> str:
        """Generate example value for a parameter type.

        Args:
            param_type: Parameter type

        Returns:
            Example value as string
        """
        examples = {
            "string": '"example"',
            "number": "42.0",
            "integer": "42",
            "boolean": "True",
            "array": "[]",
            "object": "{}",
        }

        return examples.get(param_type, '""')

    def _extract_return_info(self, description: str) -> tuple[str, str]:
        """Extract return type info from tool description's Returns: section.

        Parses the description to find a Returns: section and extracts:
        - return_type: A type hint string (e.g., "dict", "list[dict]")
        - return_description: The description of what's returned

        Args:
            description: Tool description that may contain Returns: section

        Returns:
            Tuple of (return_type, return_description)
            Returns ("Any", "Tool execution result") if no Returns: section found
        """
        import re

        if not description:
            return ("Any", "Tool execution result")

        # Look for "Returns:" section in description
        # Pattern matches "Returns:" followed by content until next section or end
        returns_pattern = r"Returns?:\s*\n?\s*(.*?)(?:\n\s*(?:Args?:|Example|Note|Raises?:|HIGH PTC|VERY HIGH|MEDIUM PTC|$)|\Z)"
        match = re.search(returns_pattern, description, re.IGNORECASE | re.DOTALL)

        if not match:
            return ("Any", "Tool execution result")

        returns_text = match.group(1).strip()

        # If returns_text is empty, return default
        if not returns_text:
            return ("Any", "Tool execution result")

        # Try to extract type hint from common patterns:
        # "dict: {...}" or "dict with..." or "Dictionary containing..."
        # "list[dict]" or "List of dicts"
        type_hint = "Any"

        type_patterns = [
            (r"^(dict|Dict)\s*[:{]", "dict"),
            (r"^(list|List)\s*\[?\s*(dict|Dict)", "list[dict]"),
            (r"^(list|List)\b", "list"),
            (r"^(str|string)\b", "str"),
            (r"^(int|integer)\b", "int"),
            (r"^(float|number)\b", "float"),
            (r"^(bool|boolean)\b", "bool"),
            (r"[Dd]ictionary\s+(?:with|containing)", "dict"),
            (r"[Ll]ist\s+of\s+(?:dict|record)", "list[dict]"),
        ]

        for pattern, hint in type_patterns:
            if re.search(pattern, returns_text, re.IGNORECASE):
                type_hint = hint
                break

        return (type_hint, returns_text)

    def generate_tool_documentation(self, tool: MCPToolInfo) -> str:
        """Generate markdown documentation for a tool.

        Args:
            tool: Tool information

        Returns:
            Markdown documentation string
        """
        func_name = tool.name.replace("-", "_").replace(".", "_")
        params = tool.get_parameters()

        # Build signature
        param_list = []
        for param_name, param_info in params.items():
            param_type = self._map_json_type_to_python(param_info["type"])
            if param_info["required"]:
                param_list.append(f"{param_name}: {param_type}")
            else:
                default = param_info.get("default", "None")
                param_list.append(f"{param_name}: {param_type} = {default}")

        signature = f"{func_name}({', '.join(param_list)})"

        # Build documentation
        doc = f"# {signature}\n\n"

        if tool.description:
            doc += f"{tool.description}\n\n"

        doc += "## Parameters\n\n"
        if params:
            for param_name, param_info in params.items():
                required_marker = "**Required**" if param_info["required"] else "Optional"
                param_type = param_info["type"]
                param_desc = param_info.get("description", "")
                doc += f"- `{param_name}` ({param_type}) - {required_marker}\n"
                if param_desc:
                    doc += f"  {param_desc}\n"
                doc += "\n"
        else:
            doc += "No parameters\n\n"

        doc += "## Returns\n\n"
        return_type, return_desc = self._extract_return_info(tool.description)
        doc += f"**Type:** `{return_type}`\n\n"
        doc += f"{return_desc}\n\n"

        doc += "## Example\n\n"
        doc += "```python\n"
        doc += f"from tools.{tool.server_name} import {func_name}\n\n"

        # Generate example call
        example_args = []
        for param_name, param_info in params.items():
            if param_info["required"]:
                example_val = self._generate_example_value(param_info["type"])
                example_args.append(f"{param_name}={example_val}")

        if example_args:
            doc += f"result = {func_name}({', '.join(example_args)})\n"
        else:
            doc += f"result = {func_name}()\n"

        doc += "print(result)  # noqa: T201\n"
        doc += "```\n"

        return doc

    def generate_mcp_client_code(
        self, server_configs: list[MCPServerConfig]
    ) -> str:
        """Generate standalone MCP client code for sandbox.

        This generates a complete MCP client that can run inside the sandbox,
        start MCP server processes, and communicate with them via JSON-RPC over stdio.

        Args:
            server_configs: List of MCP server configurations

        Returns:
            Python code for complete MCP client
        """
        # Build server configuration dict for code generation.
        # IMPORTANT: do NOT resolve `${VAR}` placeholders here.
        # This code runs on the host and generates a file uploaded into the sandbox.
        # Resolving here would inline secrets into sandbox-readable files.

        servers_dict = "{\n"
        for server in server_configs:
            if server.transport == "sse":
                # SSE transport - use URL
                url = server.url or ""
                servers_dict += f"""    \"{server.name}\": {{
        \"transport\": \"sse\",
        \"url\": {url!r},
    }},
"""
            elif server.transport == "http":
                # HTTP transport - use URL
                url = server.url or ""
                servers_dict += f"""    \"{server.name}\": {{
        \"transport\": \"http\",
        \"url\": {url!r},
    }},
"""
            else:
                # Stdio transport - use command
                env_dict = "{}"
                if hasattr(server, "env") and server.env:
                    safe_env = {k: str(v) for k, v in server.env.items()}
                    env_dict = repr(safe_env)


                # Transform Python MCP servers for sandbox execution
                # uv run python mcp_servers/xxx.py -> uv run python /home/daytona/mcp_servers/xxx.py
                command = server.command
                args = list(server.args)

                if command == "uv" and len(args) >= 3 and args[0] == "run" and args[1] == "python":
                    # Extract the Python file path (e.g., "mcp_servers/yfinance_mcp_server.py")
                    local_path = args[2]
                    filename = Path(local_path).name
                    # Keep uv run, just fix the path to sandbox
                    command = "uv"
                    args = ["run", "python", f"/home/daytona/mcp_servers/{filename}"]
                    logger.info(
                        "Transformed MCP server command for sandbox",
                        server=server.name,
                        original_command=server.command,
                        original_args=server.args,
                        sandbox_command=command,
                        sandbox_args=args
                    )

                args_list = ", ".join([repr(str(arg)) for arg in args])
                servers_dict += f"""    "{server.name}": {{
        "transport": "stdio",
        "command": "{command}",
        "args": [{args_list}],
        "env": {env_dict},
    }},
"""
        servers_dict += "}"

        return f'''"""
MCP Client for sandbox environment.

This module manages MCP server processes and provides tool calling functionality.
It supports both stdio (subprocess) and SSE (HTTP) transports.
"""

import json
import os
import subprocess
import sys
import threading
from typing import Any
import time
import httpx

# Global registry of MCP server processes (for stdio)
_server_processes: dict[str, subprocess.Popen] = {{}}
_server_locks: dict[str, threading.Lock] = {{}}
_message_id_counter = 0
_message_id_lock = threading.Lock()

# Global registry for SSE sessions
_sse_sessions: dict[str, bool] = {{}}  # server_name -> initialized

# MCP server configurations
_SERVER_CONFIGS = {servers_dict}


def _get_next_message_id() -> int:
    """Get next message ID for JSON-RPC requests."""
    global _message_id_counter
    with _message_id_lock:
        _message_id_counter += 1
        return _message_id_counter


def _start_mcp_server(server_name: str) -> subprocess.Popen:
    """Start an MCP server process if not already running.

    Args:
        server_name: Name of the MCP server

    Returns:
        Popen process object
    """
    if server_name in _server_processes:
        proc = _server_processes[server_name]
        if proc.poll() is None:  # Process still running
            return proc

    # Get server config
    config = _SERVER_CONFIGS.get(server_name)
    if not config:
        msg = f"Unknown MCP server: {{server_name}}"
        raise ValueError(msg)

    # Build command
    cmd = [config["command"]] + config["args"]

    # Start process with stdio pipes
    # Merge server env with current environment
    proc_env = os.environ.copy()

    # Ensure sandbox-internal packages are importable by Python MCP servers.
    # We upload them under /home/daytona/_internal and add that to PYTHONPATH.
    internal_root = "/home/daytona/_internal"
    existing_pythonpath = proc_env.get("PYTHONPATH", "")
    extra_paths = ["/home/daytona", internal_root]
    proc_env["PYTHONPATH"] = ":".join([p for p in [existing_pythonpath, *extra_paths] if p])

    server_env = config.get("env", {{}})
    if server_env:
        # Resolve placeholders like "${{VAR_NAME}}" in environment variables
        for key, value in server_env.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                # Extract variable name and resolve from os.environ
                var_name = value[2:-1]
                proc_env[key] = proc_env.get(var_name, value)
            else:
                proc_env[key] = value

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=proc_env,
        text=True,
        bufsize=1,  # Line buffered
    )

    # Store process
    if server_name not in _server_locks:
        _server_locks[server_name] = threading.Lock()
    _server_processes[server_name] = proc

    # Send initialize request
    init_request = {{
        "jsonrpc": "2.0",
        "id": _get_next_message_id(),
        "method": "initialize",
        "params": {{
            "protocolVersion": "2024-11-05",
            "capabilities": {{}},
            "clientInfo": {{
                "name": "open-ptc-client",
                "version": "1.0.0"
            }}
        }}
    }}

    proc.stdin.write(json.dumps(init_request) + "\\n")
    proc.stdin.flush()

    # Read initialize response
    response_line = proc.stdout.readline()
    if response_line:
        response = json.loads(response_line)
        if "error" in response:
            msg = f"MCP initialization failed: {{response['error']}}"
            raise RuntimeError(msg)

    # Send initialized notification
    initialized_notif = {{
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    }}
    proc.stdin.write(json.dumps(initialized_notif) + "\\n")
    proc.stdin.flush()

    return proc


def _initialize_sse_server(server_name: str) -> None:
    """Initialize an SSE MCP server connection.

    Args:
        server_name: Name of the MCP server
    """
    if server_name in _sse_sessions and _sse_sessions[server_name]:
        return  # Already initialized

    config = _SERVER_CONFIGS.get(server_name)
    if not config:
        msg = f"Unknown MCP server: {{server_name}}"
        raise ValueError(msg)

    url = config.get("url")
    if not url:
        msg = f"SSE server {{server_name}} has no URL configured"
        raise ValueError(msg)

    # Resolve environment variables in URL
    import re
    def resolve_env(match):
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))

    url = re.sub(r'\\$\\{{([^}}]+)\\}}', resolve_env, url)

    # Send initialize request
    init_request = {{
        "jsonrpc": "2.0",
        "id": _get_next_message_id(),
        "method": "initialize",
        "params": {{
            "protocolVersion": "2024-11-05",
            "capabilities": {{}},
            "clientInfo": {{
                "name": "open-ptc-client",
                "version": "1.0.0"
            }}
        }}
    }}

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(url, json=init_request)
            response.raise_for_status()
            result = response.json()

            if "error" in result:
                msg = f"MCP SSE initialization failed: {{result['error']}}"
                raise RuntimeError(msg)

            # Send initialized notification
            initialized_notif = {{
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }}
            client.post(url, json=initialized_notif)

        _sse_sessions[server_name] = True

    except Exception as e:  # noqa: BLE001 - Re-raising as RuntimeError with context
        msg = f"Failed to initialize SSE server {{server_name}}: {{e}}"
        raise RuntimeError(msg) from e


def _call_mcp_tool_sse(server_name: str, tool_name: str, arguments: dict[str, Any]) -> Any:
    """Call an MCP tool via SSE/HTTP transport.

    Args:
        server_name: Name of the MCP server
        tool_name: Name of the tool
        arguments: Tool arguments

    Returns:
        Tool result
    """
    import traceback
    import re

    try:
        # Ensure server is initialized
        _initialize_sse_server(server_name)

        config = _SERVER_CONFIGS.get(server_name)
        url = config.get("url", "")

        # Resolve environment variables in URL
        def resolve_env(match):
            var_name = match.group(1)
            return os.environ.get(var_name, match.group(0))

        url = re.sub(r'\\$\\{{([^}}]+)\\}}', resolve_env, url)

        # Build JSON-RPC request
        request = {{
            "jsonrpc": "2.0",
            "id": _get_next_message_id(),
            "method": "tools/call",
            "params": {{
                "name": tool_name,
                "arguments": arguments
            }}
        }}

        # Send request via HTTP POST
        with httpx.Client(timeout=60.0) as client:
            response = client.post(url, json=request)
            response.raise_for_status()
            result = response.json()

        # Check for errors
        if "error" in result:
            error = result["error"]
            error_msg = f"MCP SSE tool call failed: {{error}}"
            print(f"ERROR: {{error_msg}}", file=sys.stderr)  # noqa: T201
            raise RuntimeError(error_msg)

        # Return result
        if "result" in result:
            result_data = result["result"]

            # Unwrap MCP content format
            if (isinstance(result_data, dict) and
                "content" in result_data and
                isinstance(result_data.get("content"), list)):

                content_blocks = result_data["content"]

                if (len(content_blocks) == 1 and
                    isinstance(content_blocks[0], dict) and
                    content_blocks[0].get("type") == "text"):

                    unwrapped = content_blocks[0].get("text", "")

                    if unwrapped.startswith(("{{", "[")):
                        try:
                            return json.loads(unwrapped)
                        except json.JSONDecodeError:
                            return unwrapped

                    return unwrapped

            return result_data
        else:
            raise RuntimeError("MCP SSE response missing result field")

    except Exception as e:  # noqa: BLE001 - Top-level error handler for MCP tool call
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"\\n{{'='*60}}", file=sys.stderr)  # noqa: T201
        print(f"ERROR in _call_mcp_tool_sse", file=sys.stderr)  # noqa: T201
        print(f"{{'='*60}}", file=sys.stderr)  # noqa: T201
        print(f"Error Type: {{error_type}}", file=sys.stderr)  # noqa: T201
        print(f"Error Message: {{error_msg}}", file=sys.stderr)  # noqa: T201
        print(f"Server: {{server_name}}", file=sys.stderr)  # noqa: T201
        print(f"Tool: {{tool_name}}", file=sys.stderr)  # noqa: T201
        print(f"Arguments: {{arguments}}", file=sys.stderr)  # noqa: T201
        print(f"\\nFull Traceback:", file=sys.stderr)  # noqa: T201
        traceback.print_exc(file=sys.stderr)
        print(f"{{'='*60}}\\n", file=sys.stderr)  # noqa: T201
        raise


def _call_mcp_tool_stdio(server_name: str, tool_name: str, arguments: dict[str, Any]) -> Any:
    """Call an MCP tool via stdio transport (subprocess).

    Args:
        server_name: Name of the MCP server
        tool_name: Name of the tool
        arguments: Tool arguments

    Returns:
        Tool result
    """
    import traceback

    try:
        # Ensure server is running
        proc = _start_mcp_server(server_name)

        # Use lock to ensure thread-safe communication
        lock = _server_locks[server_name]
        with lock:
            # Build JSON-RPC request
            request = {{
                "jsonrpc": "2.0",
                "id": _get_next_message_id(),
                "method": "tools/call",
                "params": {{
                    "name": tool_name,
                    "arguments": arguments
                }}
            }}

            # Send request
            request_json = json.dumps(request) + "\\n"
            try:
                proc.stdin.write(request_json)
                proc.stdin.flush()
            except (OSError, IOError) as e:
                error_msg = f"Failed to send request to MCP server {{server_name}}: {{e}}"
                print(f"ERROR: {{error_msg}}", file=sys.stderr)  # noqa: T201
                raise RuntimeError(error_msg)

            # Read response
            try:
                response_line = proc.stdout.readline()
                if not response_line:
                    error_msg = f"MCP server {{server_name}} closed connection"
                    print(f"ERROR: {{error_msg}}", file=sys.stderr)  # noqa: T201
                    raise RuntimeError(error_msg)

                response = json.loads(response_line)
            except json.JSONDecodeError as e:
                error_msg = f"Invalid JSON response from MCP server {{server_name}}: {{e}}"
                print(f"ERROR: {{error_msg}}", file=sys.stderr)  # noqa: T201
                print(f"Response line: {{response_line}}", file=sys.stderr)  # noqa: T201
                raise RuntimeError(error_msg)

            # Check for errors
            if "error" in response:
                error = response["error"]
                error_msg = f"MCP tool call failed: {{error}}"
                print(f"ERROR: {{error_msg}}", file=sys.stderr)  # noqa: T201
                print(f"Tool: {{server_name}}.{{tool_name}}", file=sys.stderr)  # noqa: T201
                print(f"Arguments: {{arguments}}", file=sys.stderr)  # noqa: T201
                raise RuntimeError(error_msg)

            # Return result
            if "result" in response:
                result = response["result"]

                # Unwrap MCP content format for easier agent consumption
                if (isinstance(result, dict) and
                    "content" in result and
                    isinstance(result.get("content"), list)):

                    content_blocks = result["content"]

                    if (len(content_blocks) == 1 and
                        isinstance(content_blocks[0], dict) and
                        content_blocks[0].get("type") == "text"):

                        unwrapped = content_blocks[0].get("text", "")

                        if unwrapped.startswith(("{{", "[")):
                            try:
                                return json.loads(unwrapped)
                            except json.JSONDecodeError:
                                return unwrapped

                        return unwrapped

                return result
            else:
                error_msg = "MCP response missing result field"
                print(f"ERROR: {{error_msg}}", file=sys.stderr)  # noqa: T201
                print(f"Response: {{response}}", file=sys.stderr)  # noqa: T201
                raise RuntimeError(error_msg)

    except Exception as e:  # noqa: BLE001 - Top-level error handler for MCP tool call
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"\\n{{'='*60}}", file=sys.stderr)  # noqa: T201
        print(f"ERROR in _call_mcp_tool_stdio", file=sys.stderr)  # noqa: T201
        print(f"{{'='*60}}", file=sys.stderr)  # noqa: T201
        print(f"Error Type: {{error_type}}", file=sys.stderr)  # noqa: T201
        print(f"Error Message: {{error_msg}}", file=sys.stderr)  # noqa: T201
        print(f"Server: {{server_name}}", file=sys.stderr)  # noqa: T201
        print(f"Tool: {{tool_name}}", file=sys.stderr)  # noqa: T201
        print(f"Arguments: {{arguments}}", file=sys.stderr)  # noqa: T201
        print(f"\\nFull Traceback:", file=sys.stderr)  # noqa: T201
        traceback.print_exc(file=sys.stderr)
        print(f"{{'='*60}}\\n", file=sys.stderr)  # noqa: T201
        raise


def _call_mcp_tool(server_name: str, tool_name: str, arguments: dict[str, Any]) -> Any:
    """Call an MCP tool via the appropriate transport.

    Routes to SSE or stdio transport based on server configuration.

    Args:
        server_name: Name of the MCP server
        tool_name: Name of the tool
        arguments: Tool arguments

    Returns:
        Tool result (unwraps MCP content format for easier use)
    """
    config = _SERVER_CONFIGS.get(server_name)
    if not config:
        msg = f"Unknown MCP server: {{server_name}}"
        raise ValueError(msg)

    transport = config.get("transport", "stdio")

    if transport in ("sse", "http"):
        return _call_mcp_tool_sse(server_name, tool_name, arguments)
    else:
        return _call_mcp_tool_stdio(server_name, tool_name, arguments)


def cleanup_mcp_servers():
    """Clean up all MCP server processes."""
    for server_name, proc in _server_processes.items():
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except (OSError, TimeoutError) as e:
            print(f"Error cleaning up MCP server {{server_name}}: {{e}}", file=sys.stderr)  # noqa: T201
    _server_processes.clear()
'''

