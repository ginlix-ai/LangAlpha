"""Tool Function Generator - Convert MCP tool schemas to Python functions."""

from pathlib import Path
from typing import Any

import structlog

from ptc_agent.agent.provenance import SNIPPET_MAX_CHARS
from ptc_agent.config.core import MCPServerConfig

from .mcp_registry import MCPToolInfo
from .mcp_sanitize import (
    discovery_should_use_secrets,
    is_user_server,
    sanitize_tool_name,
    sanitize_tool_set,
    sanitize_tool_text,
)

logger = structlog.get_logger(__name__)


# Version of the generated mcp_client.py output. Bump whenever the generated
# client *code* changes (e.g. the _trace_mcp_call template) in a way that should
# reach existing sandboxes. The sandbox manifest hashes generation *inputs* (MCP
# server files, tool schemas, user config) — not this generator's source — so a
# pure codegen change is otherwise invisible to sync_sandbox_assets and never
# re-uploaded to a reused sandbox. Folding this constant into the tool_modules
# version makes a bump force the regenerated client onto every workspace on its
# next sync. See ptc_sandbox._compute_sandbox_manifest.
MCP_CLIENT_CODEGEN_VERSION = "1"


def _safe_func_name(name: str) -> str:
    """Map an MCP tool name to a wrapper function name.

    Uses the shared identifier sanitizer; falls back to a stable placeholder so
    codegen never emits an illegal ``def`` (collision detection happens upstream
    in :func:`mcp_sanitize.sanitize_tool_set`).
    """
    return sanitize_tool_name(name) or "_invalid_tool"


# ---------------------------------------------------------------------------
# Discovery source — substituted into the generated mcp_client.py f-string when
# any workspace server is present. The constant is inserted VERBATIM via the
# {discover_block} substitution (not f-string-interpolated), so braces are
# single. Discovery runs tools/list over the server's own transport WITHOUT the
# vault (refs resolve to inert placeholders) and writes its result to a file:
# {"server": name, "status": "ok"|"error", "error": str,
#  "tools": [{name, description, input_schema}]}.
# ---------------------------------------------------------------------------
_DISCOVER_SOURCE = '''

def discover(server_name: str) -> dict:
    """List a server's tools without requiring the vault (file-IPC caller writes JSON).

    Returns {"server", "status", "error", "tools": [{name, description,
    input_schema}]}. Never raises — failures are captured in ``status``/``error``.
    """
    config = _SERVER_CONFIGS.get(server_name)
    if not config:
        return {"server": server_name, "status": "error",
                "error": "unknown server", "tools": []}
    transport = config.get("transport", "stdio")
    try:
        if transport in ("sse", "http"):
            raw = _discover_sse(server_name)
        else:
            raw = _discover_stdio(server_name)
    except Exception as e:  # noqa: BLE001 - discovery must never crash the driver
        return {"server": server_name, "status": "error",
                "error": str(e), "tools": []}
    tools = []
    for t in raw or []:
        if not isinstance(t, dict):
            continue
        tools.append({
            "name": t.get("name", ""),
            "description": t.get("description", "") or "",
            "input_schema": t.get("inputSchema") or t.get("input_schema") or {},
        })
    return {"server": server_name, "status": "ok", "error": "", "tools": tools}


def _discover_stdio(server_name: str) -> list:
    """Start the stdio server in discovery mode and return its tools/list."""
    proc = _start_mcp_server(server_name, discovery=True)
    req = {"jsonrpc": "2.0", "id": _get_next_message_id(),
           "method": "tools/list", "params": {}}
    proc.stdin.write(json.dumps(req) + "\\n")
    proc.stdin.flush()
    ready, _, _ = select.select([proc.stdout], [], [], 30)
    if not ready:
        proc.kill()
        _server_processes.pop(server_name, None)
        raise RuntimeError(f"discovery timed out for {server_name} (30s)")
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError(f"{server_name} closed connection during discovery")
    resp = json.loads(line)
    if "error" in resp:
        raise RuntimeError(f"tools/list error: {resp['error']}")
    return (resp.get("result") or {}).get("tools", [])


def _discover_sse(server_name: str) -> list:
    """Initialize the sse/http server in discovery mode and return its tools/list."""
    _initialize_sse_server(server_name, discovery=True)
    config = _SERVER_CONFIGS.get(server_name)
    url, headers = _resolve_sse(config, server_name, discovery=True)
    req = {"jsonrpc": "2.0", "id": _get_next_message_id(),
           "method": "tools/list", "params": {}}
    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, json=req, headers=headers)
        response.raise_for_status()
        result = response.json()
    if "error" in result:
        raise RuntimeError(f"tools/list error: {result['error']}")
    return (result.get("result") or {}).get("tools", [])
'''


# CLI: ``python mcp_client.py discover <server_name> <output_path>``. Inserted
# verbatim (single braces). Writes the discover() dict to <output_path> as JSON.
# Always exits 0 (errors go in the file) so the host driver reads structured
# results, not exit codes.
_MAIN_SOURCE = '''

if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "discover":
        _server, _out = sys.argv[2], sys.argv[3]
        _result = discover(_server)
        with open(_out, "w") as _f:
            json.dump(_result, _f)
        sys.exit(0)
    print("usage: mcp_client.py discover <server_name> <output_path>", file=sys.stderr)  # noqa: T201
    sys.exit(2)
'''


class ToolFunctionGenerator:
    """Generates Python function code from MCP tool schemas."""

    def generate_tool_module(
        self, server_name: str, tools: list[MCPToolInfo], source: str = "builtin"
    ) -> str:
        """Generate a complete Python module for a server's tools.

        Args:
            server_name: Name of the MCP server
            tools: List of tools from this server
            source: 'builtin' or 'workspace'. For workspace (untrusted) servers,
                tool names are validated/de-collided and descriptions sanitized.

        Returns:
            Complete Python module code as string
        """
        logger.debug(
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

        # For untrusted workspace servers, validate + de-collide tool names so
        # one hostile/duplicate name can't break the module (builtins keep their
        # historical behavior; they are trusted and already collision-free).
        if source == "workspace":
            sanitized = sanitize_tool_set(tools)
            if sanitized.skipped:
                logger.warning(
                    "Skipped invalid tools for workspace MCP server",
                    server=server_name,
                    skipped=sanitized.skipped,
                )
            tools = sanitized.kept

        # Generate functions for each tool
        for tool in tools:
            code += self._generate_function(tool, server_name, source)
            code += "\n\n"

        return code

    def _generate_function(
        self, tool: MCPToolInfo, server_name: str, source: str = "builtin"
    ) -> str:
        """Generate Python function for a single tool.

        Args:
            tool: Tool information from MCP server
            server_name: Name of the MCP server this tool belongs to
            source: 'builtin' or 'workspace' (untrusted text is sanitized for
                workspace servers; builtin output is unchanged)

        Returns:
            Python function code
        """
        # Generate function signature
        func_name = _safe_func_name(tool.name)
        params = tool.get_parameters()

        # For untrusted workspace servers, coerce each param NAME into a legal
        # identifier (a hostile schema key could otherwise inject code or break
        # the module); skip names that can't be salvaged. Builtins keep the raw
        # key verbatim so their generated code stays byte-identical.
        if source == "workspace":
            usable: dict[str, dict[str, Any]] = {}
            for param_name, param_info in params.items():
                safe_param = sanitize_tool_name(param_name)
                if safe_param is None or safe_param in usable:
                    logger.warning(
                        "Skipped invalid/colliding param for workspace MCP tool",
                        server=server_name,
                        tool=tool.name,
                        param=param_name,
                    )
                    continue
                usable[safe_param] = param_info
            params = usable

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
        docstring = self._generate_docstring(tool, params, source)

        # Generate function body. For workspace servers the arg-dict KEY is
        # emitted via repr (the param name is untrusted text); builtins keep the
        # historical double-quoted literal so their output is byte-identical.
        if source == "workspace":
            arg_dict_entries = [
                f"        {param_name!r}: {param_name}," for param_name in params
            ]
        else:
            arg_dict_entries = [
                f'        "{param_name}": {param_name},' for param_name in params
            ]

        args_dict = "\n".join(arg_dict_entries)

        # Extract return type from description for better type hints
        return_type, _ = self._extract_return_info(tool.description)

        # Workspace tool names are untrusted — emit server/tool via repr so a
        # hostile name can't escape the string literal and inject code. Builtins
        # keep the historical double-quoted literal (byte-identical).
        if source == "workspace":
            call_line = (
                f"    return _call_mcp_tool({server_name!r}, {tool.name!r}, arguments)"
            )
        else:
            call_line = (
                f'    return _call_mcp_tool("{server_name}", "{tool.name}", arguments)'
            )

        return f'''def {func_name}({param_str}) -> {return_type}:
    """{docstring}"""
    arguments = {{
{args_dict}
    }}

    # Remove None values
    arguments = {{k: v for k, v in arguments.items() if v is not None}}

{call_line}'''

    def _generate_docstring(
        self, tool: MCPToolInfo, params: dict[str, Any], source: str = "builtin"
    ) -> str:
        """Generate docstring for a tool function.

        Args:
            tool: Tool information
            params: Parameter information
            source: 'builtin' (escape backslashes only, byte-stable) or
                'workspace' (full untrusted-text sanitization)

        Returns:
            Formatted docstring
        """

        def _escape(text: str) -> str:
            # Workspace (untrusted) text is fully sanitized — triple-quote
            # breakouts, control chars, length cap. Builtins keep the historical
            # backslash-only escape so their generated code stays byte-identical.
            if source == "workspace":
                return sanitize_tool_text(text)
            return text.replace("\\", "\\\\")

        lines = []

        # Add description
        if tool.description:
            lines.append(_escape(tool.description))
            lines.append("")

        # Add parameters
        if params:
            lines.append("Args:")
            for param_name, param_info in params.items():
                param_desc = param_info.get("description", "")
                escaped_desc = _escape(param_desc)
                # The schema `type` field is untrusted text like the
                # description — escape it (and coerce non-str values) so it
                # can't terminate the docstring.
                param_type = _escape(str(param_info["type"]))
                required = " (required)" if param_info["required"] else ""
                lines.append(
                    f"    {param_name} ({param_type}){required}: {escaped_desc}"
                )
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
            func_name = _safe_func_name(tool.name)
            example_call = (
                f"{func_name}({', '.join(example_args[:2])})"  # Limit to 2 args
            )
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

        # A hostile schema may carry a non-str (unhashable) `type`.
        if not isinstance(json_type, str):
            return "Any"
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

        # A hostile schema may carry a non-str (unhashable) `type`.
        if not isinstance(param_type, str):
            return '""'
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

    def generate_tool_documentation(
        self, tool: MCPToolInfo, source: str = "builtin"
    ) -> str:
        """Generate markdown documentation for a tool.

        Args:
            tool: Tool information
            source: 'builtin' or 'workspace' (untrusted description text is
                sanitized for workspace servers; builtin output is unchanged)

        Returns:
            Markdown documentation string
        """
        func_name = _safe_func_name(tool.name)
        params = tool.get_parameters()
        description = (
            sanitize_tool_text(tool.description)
            if source == "workspace"
            else tool.description
        )

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

        if description:
            doc += f"{description}\n\n"

        doc += "## Parameters\n\n"
        if params:
            for param_name, param_info in params.items():
                required_marker = (
                    "**Required**" if param_info["required"] else "Optional"
                )
                param_type = str(param_info["type"])
                param_desc = param_info.get("description", "")
                if source == "workspace":
                    param_type = sanitize_tool_text(param_type)
                    param_desc = sanitize_tool_text(param_desc)
                doc += f"- `{param_name}` ({param_type}) - {required_marker}\n"
                if param_desc:
                    doc += f"  {param_desc}\n"
                doc += "\n"
        else:
            doc += "No parameters\n\n"

        doc += "## Returns\n\n"
        return_type, return_desc = self._extract_return_info(tool.description)
        if source == "workspace":
            return_desc = sanitize_tool_text(return_desc)
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

    def _vault_runtime_block(self, working_dir: str) -> str:
        """Runtime helpers for vault-only secret resolution (workspace servers).

        Emitted into the generated client ONLY when at least one workspace
        server is present. ``${vault:NAME}`` references resolve exclusively from
        ``{working_dir}/_internal/.vault_secrets.json`` — never from host
        os.environ — so a user-named platform env var resolves to nothing. The
        regex MUST mirror ``mcp_sanitize.VAULT_REF_RE``. In discovery mode the
        vault file is absent, so refs resolve to an inert placeholder.
        """
        # NOTE: keep this pattern byte-identical to mcp_sanitize.VAULT_REF_RE.
        return f'''
import re as _re

# Matches ${{vault:NAME}} — mirrors mcp_sanitize.VAULT_REF_RE. Only this exact
# form resolves; a bare ${{VAR}} is intentionally NOT a vault reference.
_VAULT_REF_RE = _re.compile(r"\\$\\{{vault:([A-Za-z_][A-Za-z0-9_]{{0,127}})\\}}")
_WORK_DIR = "{working_dir}"
_INTERNAL_ROOT = "{working_dir}/_internal"
_VAULT_SECRETS_FILE = "{working_dir}/_internal/.vault_secrets.json"


def _load_vault() -> dict:
    """Load the workspace vault. Returns {{}} when the file is absent (discovery)."""
    try:
        with open(_VAULT_SECRETS_FILE) as _f:
            return json.load(_f)
    except (FileNotFoundError, ValueError, OSError):
        return {{}}


def _resolve_vault_refs(value, vault, *, missing, discovery=False):
    """Substitute ${{vault:NAME}} refs in ``value`` against ``vault`` only.

    Unresolvable refs are recorded in ``missing`` (by NAME, never value). In
    discovery mode they become an inert empty string so tools/list still runs.
    There is NO fallback to os.environ — that is the whole point.
    """
    def _sub(match):
        name = match.group(1)
        if name in vault:
            return vault[name]
        missing.append(name)
        return "" if discovery else match.group(0)

    return _VAULT_REF_RE.sub(_sub, value)


def _build_proc_env(config, server_name="?", *, discovery=False):
    """Build the stdio subprocess env.

    Builtin servers inherit os.environ. Workspace (untrusted) servers get a
    MINIMAL scoped env (PATH/HOME plus only their own declared env values), with
    ${{vault:NAME}} refs resolved vault-only — never the sandbox's full
    os.environ, never a host-env fallback.
    """
    if config.get("source") != "workspace":
        proc_env = os.environ.copy()
        for key in config.get("env_keys", []):
            if key in os.environ:
                proc_env[key] = os.environ[key]
    else:
        proc_env = {{}}
        for _k in ("PATH", "HOME", "LANG", "LC_ALL"):
            if _k in os.environ:
                proc_env[_k] = os.environ[_k]
        # Secret-less discovery (default): every ${{vault:NAME}} ref hits the
        # inert path. Opt in per server via discovery_uses_secrets for servers
        # that need auth even to list tools. Normal calls always resolve.
        vault = _load_vault() if (not discovery or config.get("discovery_uses_secrets")) else {{}}
        missing = []
        for _name, _val in (config.get("env") or {{}}).items():
            proc_env[_name] = _resolve_vault_refs(
                str(_val), vault, missing=missing, discovery=discovery
            )
        if missing and not discovery:
            raise RuntimeError(
                "Missing vault secret(s) for server "
                + repr(server_name) + ": "
                + ", ".join(sorted(set(missing)))
            )

    internal_root = _INTERNAL_ROOT
    existing_pythonpath = proc_env.get("PYTHONPATH", "")
    extra_paths = [_WORK_DIR, internal_root + "/src", internal_root]
    proc_env["PYTHONPATH"] = ":".join(
        [p for p in [existing_pythonpath, *extra_paths] if p]
    )
    return proc_env


def _resolve_cmd_args(config, server_name, *, discovery=False):
    """Resolve ${{vault:NAME}} refs in a stdio server's args, vault-only.

    Builtin servers pass args through unchanged. Workspace (untrusted) servers
    resolve refs the same way env/headers do — so a credential moved into args
    by import resolves at spawn instead of leaking as a literal — with no host
    os.environ fallback. Missing refs raise (named, never valued) unless in
    discovery, where they become inert placeholders.
    """
    args = list(config.get("args") or [])
    if config.get("source") != "workspace":
        return args
    vault = _load_vault() if (not discovery or config.get("discovery_uses_secrets")) else {{}}
    missing = []
    resolved = [
        _resolve_vault_refs(str(_a), vault, missing=missing, discovery=discovery)
        for _a in args
    ]
    if missing and not discovery:
        raise RuntimeError(
            "Missing vault secret(s) for server "
            + repr(server_name) + ": " + ", ".join(sorted(set(missing)))
        )
    return resolved


def _resolve_sse(config, server_name, *, discovery=False):
    """Return (url, headers) for an sse/http request.

    Builtin servers keep the legacy ${{VAR}}-from-os.environ URL resolution and
    send no extra headers. Workspace (untrusted) servers resolve ${{vault:NAME}}
    refs in BOTH the URL and headers vault-only (no host-env fallback) and send
    the resolved headers. Missing refs raise (named, never valued) unless in
    discovery, where they become inert placeholders.
    """
    url = config.get("url", "") or ""
    if config.get("source") != "workspace":
        def _env_sub(match):
            return os.environ.get(match.group(1), match.group(0))

        return _re.sub(r"\\$\\{{([^}}]+)\\}}", _env_sub, url), {{}}

    # Secret-less discovery (default): refs resolve inert. Opt in per server via
    # discovery_uses_secrets. Normal calls (discovery=False) always resolve.
    vault = _load_vault() if (not discovery or config.get("discovery_uses_secrets")) else {{}}
    missing = []
    url = _resolve_vault_refs(url, vault, missing=missing, discovery=discovery)
    headers = {{}}
    for _hname, _hval in (config.get("headers") or {{}}).items():
        headers[_hname] = _resolve_vault_refs(
            str(_hval), vault, missing=missing, discovery=discovery
        )
    if missing and not discovery:
        raise RuntimeError(
            "Missing vault secret(s) for server "
            + repr(server_name) + ": " + ", ".join(sorted(set(missing)))
        )
    return url, headers
'''

    def generate_mcp_client_code(
        self,
        server_configs: list[MCPServerConfig],
        working_dir: str = "/home/workspace",
    ) -> str:
        """Generate standalone MCP client code for sandbox.

        This generates a complete MCP client that can run inside the sandbox,
        start MCP server processes, and communicate with them via JSON-RPC over stdio.

        Args:
            server_configs: List of MCP server configurations
            working_dir: Sandbox working directory for path resolution

        Returns:
            Python code for complete MCP client
        """
        # Build server configuration dict for code generation.
        #
        # Builtin servers (source == "builtin"): only env key NAMES are
        # embedded — never values. The sandbox already has the resolved values
        # in os.environ (injected by _build_sandbox_env_vars at creation time),
        # so the generated code resolves them from os.environ at runtime.
        #
        # Workspace servers (source == "workspace", untrusted): env/header
        # values may hold ``${vault:NAME}`` references. Those resolve ONLY from
        # _internal/.vault_secrets.json — never from host os.environ — and a
        # stdio server's subprocess gets a minimal scoped env (PATH/HOME plus
        # its own declared values). The vault machinery is emitted only when at
        # least one workspace server is present, so a builtin-only config yields
        # the byte-identical module it always has (no `vault` references appear).
        has_workspace = any(is_user_server(s) for s in server_configs)

        servers_dict = "{\n"
        for server in server_configs:
            is_workspace = is_user_server(server)
            if server.transport in ("sse", "http"):
                url = server.url or ""
                if is_workspace:
                    headers_repr = repr(dict(getattr(server, "headers", {}) or {}))
                    dus = discovery_should_use_secrets(server)
                    servers_dict += f"""    "{server.name}": {{
        "transport": "{server.transport}",
        "url": {url!r},
        "source": "workspace",
        "headers": {headers_repr},
        "discovery_uses_secrets": {dus!r},
    }},
"""
                else:
                    servers_dict += f"""    \"{server.name}\": {{
        \"transport\": \"{server.transport}\",
        \"url\": {url!r},
    }},
"""
            else:
                # Stdio transport.
                command = server.command
                args = list(server.args)

                if (
                    command == "uv"
                    and len(args) >= 3
                    and args[0] == "run"
                    and args[1] == "python"
                ):
                    # Extract the Python file path (e.g., "mcp_servers/yfinance_mcp_server.py")
                    local_path = args[2]
                    filename = Path(local_path).name
                    # Keep uv run, just fix the path to sandbox
                    command = "uv"
                    args = ["run", "python", f"{working_dir}/mcp_servers/{filename}"]
                    logger.debug(
                        "Transformed MCP server command for sandbox",
                        server=server.name,
                        original_command=server.command,
                        original_args=server.args,
                        sandbox_command=command,
                        sandbox_args=args,
                    )

                args_list = ", ".join([repr(str(arg)) for arg in args])
                if is_workspace:
                    # Embed the full env mapping (name -> literal | "${vault:NAME}").
                    # Values are NOT secrets: vault refs are placeholders resolved
                    # in-sandbox; literals are user-supplied non-secret config.
                    env_repr = repr(dict(getattr(server, "env", {}) or {}))
                    dus = discovery_should_use_secrets(server)
                    servers_dict += f"""    "{server.name}": {{
        "transport": "stdio",
        "command": "{command}",
        "args": [{args_list}],
        "source": "workspace",
        "env": {env_repr},
        "discovery_uses_secrets": {dus!r},
    }},
"""
                else:
                    # Builtin: store only env key names, NOT values. The sandbox
                    # already has the resolved values in os.environ.
                    env_keys_repr = "[]"
                    if hasattr(server, "env") and server.env:
                        env_keys_repr = repr(list(server.env.keys()))
                    servers_dict += f"""    "{server.name}": {{
        "transport": "stdio",
        "command": "{command}",
        "args": [{args_list}],
        "env_keys": {env_keys_repr},
    }},
"""
        servers_dict += "}"

        vault_block = self._vault_runtime_block(working_dir) if has_workspace else ""

        # The stdio env-setup section. For builtin-only configs it is the
        # historical inline block (byte-identical). When any workspace server is
        # present it delegates to ``_build_proc_env`` (emitted in vault_block),
        # which scopes the subprocess env and does vault-only resolution.
        if has_workspace:
            proc_env_setup = (
                "\n    proc_env = _build_proc_env("
                "config, server_name, discovery=discovery)\n"
            )
        else:
            proc_env_setup = (
                "\n    # Start process with stdio pipes\n"
                "    # Merge server env with current environment\n"
                "    proc_env = os.environ.copy()\n"
                "\n"
                "    # Ensure sandbox-internal packages are importable by Python MCP servers.\n"
                f"    # We upload them under {working_dir}/_internal/src and add paths to PYTHONPATH.\n"
                "    # - _internal/src: allows `from data_client.fmp import ...` (bare package name)\n"
                "    # - _internal:     allows `from src.data_client.fmp import ...` (qualified)\n"
                f'    internal_root = "{working_dir}/_internal"\n'
                '    existing_pythonpath = proc_env.get("PYTHONPATH", "")\n'
                f'    extra_paths = ["{working_dir}", f"{{internal_root}}/src", internal_root]\n'
                '    proc_env["PYTHONPATH"] = ":".join([p for p in [existing_pythonpath, *extra_paths] if p])\n'
                "\n"
                "    # Resolve env vars by key name from os.environ (values are injected\n"
                "    # at sandbox creation time, never hardcoded in this file).\n"
                '    for key in config.get("env_keys", []):\n'
                "        if key in os.environ:\n"
                "            proc_env[key] = os.environ[key]\n"
            )

        # stdio command args. Builtin-only stays byte-identical (raw args). With
        # a workspace server present, resolve ${vault:NAME} refs in args
        # vault-only (mirrors env/header resolution) so an imported secret can
        # live in args without leaking as a literal at rest.
        if has_workspace:
            cmd_args_expr = "_resolve_cmd_args(config, server_name, discovery=discovery)"
        else:
            cmd_args_expr = 'config["args"]'

        # SSE/HTTP URL+header resolution. Builtin-only configs keep the legacy
        # inline os.environ URL substitution with no extra headers (byte-stable).
        # With any workspace server present, both paths route through
        # ``_resolve_sse`` (vault-only for workspace, os.environ for builtins) and
        # send resolved headers.
        if has_workspace:
            sse_init_resolve = (
                "\n    url, _headers = _resolve_sse("
                "config, server_name, discovery=discovery)\n"
            )
            sse_call_resolve = (
                "\n        url, _headers = _resolve_sse(config, server_name)\n"
            )
            sse_post_kwargs = ", headers=_headers"
        else:
            sse_init_resolve = (
                "\n    # Resolve environment variables in URL\n"
                "    import re\n"
                "    def resolve_env(match):\n"
                "        var_name = match.group(1)\n"
                "        return os.environ.get(var_name, match.group(0))\n"
                "\n"
                "    url = re.sub(r'\\$\\{([^}]+)\\}', resolve_env, url)\n"
            )
            sse_call_resolve = (
                "\n        # Resolve environment variables in URL\n"
                "        def resolve_env(match):\n"
                "            var_name = match.group(1)\n"
                "            return os.environ.get(var_name, match.group(0))\n"
                "\n"
                "        url = re.sub(r'\\$\\{([^}]+)\\}', resolve_env, url)\n"
            )
            sse_post_kwargs = ""

        # Discovery entrypoint + CLI. Emitted only when a workspace server is
        # present so builtin-only clients stay byte-identical. Discovery lists a
        # server's tools WITHOUT requiring the vault file (refs -> placeholders)
        # and writes its JSON result to a caller-specified file (stdout is
        # polluted by npx/MCP server logs, so file IPC is the contract).
        if has_workspace:
            discover_block = _DISCOVER_SOURCE
            main_block = _MAIN_SOURCE
            discovery_param = ", discovery: bool = False"
            discovery_doc = (
                "\n        discovery: When True, unresolved secret refs in a "
                "workspace server's env\n            resolve to an inert "
                "placeholder (secrets may be absent in discovery)."
            )
            discovery_doc_sse = (
                "\n        discovery: tolerate missing secret refs "
                "(resolve to placeholder)"
            )
        else:
            discover_block = ""
            main_block = ""
            discovery_param = ""
            discovery_doc = ""
            discovery_doc_sse = ""

        return f'''"""
MCP Client for sandbox environment.

This module manages MCP server processes and provides tool calling functionality.
It supports both stdio (subprocess) and SSE (HTTP) transports.
"""

import datetime
import hashlib
import json
import os
import select
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
{vault_block}

def _trace_mcp_call(server: str, tool: str, args: Any, result: Any) -> None:
    """Append one JSONL provenance line for an MCP call (best-effort, never raises).

    No-op unless MCP_TRACE_FILE is set. The fingerprint (sha256/size/snippet) is
    computed in-sandbox and must reproduce the host-side fingerprint_result
    contract byte-for-byte.
    """
    try:
        trace_file = os.environ.get("MCP_TRACE_FILE")
        if not trace_file:
            return
        try:
            if isinstance(result, (dict, list)):
                canonical = json.dumps(
                    result, sort_keys=True, default=str, ensure_ascii=False
                )
            else:
                canonical = str(result)
        except Exception:
            try:
                canonical = str(result)
            except Exception:
                canonical = ""
        encoded = canonical.encode("utf-8")
        entry = {{
            "server": server,
            "tool": tool,
            "args": args if isinstance(args, dict) else {{}},
            "result_sha256": hashlib.sha256(encoded).hexdigest(),
            "result_size": len(encoded),
            # Cap interpolated from the canonical SNIPPET_MAX_CHARS in
            # agent/provenance/types.py at codegen time, so host + sandbox
            # snippets match byte-for-byte for dedup with no manual hardcode.
            "result_snippet": canonical[:{SNIPPET_MAX_CHARS}],
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }}
        os.makedirs(os.path.dirname(trace_file), exist_ok=True)
        with open(trace_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, default=str, ensure_ascii=False) + "\\n")
            fh.flush()
    except Exception:
        # Tracing must never break the agent's code.
        pass


def _is_error_result(envelope: Any, value: Any) -> bool:
    """True when an MCP tools/call result is an error rather than data.

    Honors the MCP ``isError`` flag (any spec-compliant server, including remote
    HTTP MCP) and our servers' ``{{"error": ...}}`` return convention. Provenance
    records only data the agent actually received, so error results are returned
    to the caller unchanged but never traced. A falsy ``error`` field (e.g.
    ``error: null`` on a success payload) is NOT treated as an error.
    """
    if isinstance(envelope, dict) and envelope.get("isError") is True:
        return True
    return isinstance(value, dict) and bool(value.get("error"))


def _unwrap_mcp_content(envelope: Any) -> Any:
    """Unwrap a single text content block to parsed JSON / text; else passthrough."""
    if (isinstance(envelope, dict) and
        isinstance(envelope.get("content"), list)):

        content_blocks = envelope["content"]

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

    return envelope


def _finalize_mcp_result(
    server_name: str, tool_name: str, arguments: dict[str, Any], envelope: Any
) -> Any:
    """Unwrap an MCP result and trace it iff it carries real data.

    Shared by both transports — the single place that sees the raw envelope (so
    the ``isError`` flag survives) and decides whether the call is recordable.
    """
    value = _unwrap_mcp_content(envelope)
    if not _is_error_result(envelope, value):
        _trace_mcp_call(server_name, tool_name, arguments, value)
    return value


def _get_next_message_id() -> int:
    """Get next message ID for JSON-RPC requests."""
    global _message_id_counter
    with _message_id_lock:
        _message_id_counter += 1
        return _message_id_counter


def _start_mcp_server(server_name: str{discovery_param}) -> subprocess.Popen:
    """Start an MCP server process if not already running.

    Args:
        server_name: Name of the MCP server{discovery_doc}

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
    cmd = [config["command"]] + {cmd_args_expr}
{proc_env_setup}
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=proc_env,
        text=True,
        bufsize=1,  # Line buffered
    )

    # Drain stderr in background to prevent pipe buffer deadlock.
    # FastMCP logs INFO to stderr via RichHandler; if the 64KB pipe buffer
    # fills, the server blocks on write(stderr) and can't respond on stdout.
    threading.Thread(target=lambda: proc.stderr.read(), daemon=True).start()

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

    # Read initialize response (with timeout to avoid hanging on broken servers)
    ready, _, _ = select.select([proc.stdout], [], [], 30)
    if not ready:
        proc.kill()
        _server_processes.pop(server_name, None)
        raise RuntimeError(f"MCP server {{server_name}} timed out during initialization (30s)")
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


def _initialize_sse_server(server_name: str{discovery_param}) -> None:
    """Initialize an SSE MCP server connection.

    Args:
        server_name: Name of the MCP server{discovery_doc_sse}
    """
    if server_name in _sse_sessions and _sse_sessions[server_name]:
        return  # Already initialized

    config = _SERVER_CONFIGS.get(server_name)
    if not config:
        msg = f"Unknown MCP server: {{server_name}}"
        raise ValueError(msg)

    url = config.get("url")
    if not url:
        msg = f"Remote MCP server {{server_name}} has no URL configured"
        raise ValueError(msg)
{sse_init_resolve}
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
            response = client.post(url, json=init_request{sse_post_kwargs})
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
            client.post(url, json=initialized_notif{sse_post_kwargs})

        _sse_sessions[server_name] = True

    except Exception as e:  # noqa: BLE001 - Re-raising as RuntimeError with context
        msg = f"Failed to initialize remote MCP server {{server_name}}: {{e}}"
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
{sse_call_resolve}
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
            response = client.post(url, json=request{sse_post_kwargs})
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
            return _finalize_mcp_result(
                server_name, tool_name, arguments, result["result"]
            )
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
        # Ensure server is running (initial start outside lock to avoid holding
        # the lock during slow server startup)
        _start_mcp_server(server_name)

        # Use lock to ensure thread-safe communication
        lock = _server_locks[server_name]
        with lock:
            # Re-fetch proc inside lock to avoid TOCTOU race: another thread
            # may have killed the process while we were waiting for the lock.
            proc = _server_processes.get(server_name)
            if proc is None or proc.poll() is not None:
                proc = _start_mcp_server(server_name)
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

            # Read response (with timeout to detect stalled servers)
            try:
                ready, _, _ = select.select([proc.stdout], [], [], 120)
                if not ready:
                    error_msg = f"MCP server {{server_name}} timed out after 120s on tool {{tool_name}}"
                    print(f"ERROR: {{error_msg}}", file=sys.stderr)  # noqa: T201
                    proc.kill()
                    _server_processes.pop(server_name, None)
                    raise RuntimeError(error_msg)
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
                return _finalize_mcp_result(
                    server_name, tool_name, arguments, response["result"]
                )
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

    # Tracing happens in the transport via _finalize_mcp_result, where the raw
    # envelope (incl. the MCP isError flag) is still visible — so failed calls
    # and error payloads are returned to the agent but never recorded as sources.
    if transport in ("sse", "http"):
        return _call_mcp_tool_sse(server_name, tool_name, arguments)
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
{discover_block}{main_block}'''
