"""Tool Function Generator - MCP tool schemas to sandbox wrapper modules.

The sandbox MCP client itself is NOT generated: it ships verbatim from
``sandbox/mcp_client_runtime.py`` (a static, lintable, directly-testable
module). ``generate_mcp_client_code`` composes that source with a JSON
config epilogue carrying all per-workspace variance; this module otherwise
generates only the per-server wrapper functions and their docs.
"""

import hashlib
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import structlog

from ptc_agent.agent.provenance.types import RESULT_BODY_MAX_BYTES
from ptc_agent.config.core import MCPServerConfig

from .mcp_registry import MCPToolInfo
from .mcp_sanitize import (
    discovery_should_use_secrets,
    is_untrusted_server,
    sanitize_tool_name,
    sanitize_tool_set,
    sanitize_tool_text,
)

logger = structlog.get_logger(__name__)


_RUNTIME_FILE = Path(__file__).parent / "sandbox" / "mcp_client_runtime.py"


@lru_cache(maxsize=1)
def client_runtime_source() -> str:
    """The static sandbox client runtime, uploaded verbatim (plus epilogue)."""
    return _RUNTIME_FILE.read_text(encoding="utf-8")


# Version of the uploaded mcp_client.py + generated wrapper modules. The
# manifest hashes generation *inputs* (MCP server files, tool schemas, user
# config) — not this package's source — so a pure client/codegen change is
# otherwise invisible to sync_sandbox_assets and never reaches a reused
# sandbox. Folding this value into the tool_modules version forces the
# regenerated client onto every workspace on its next sync (see
# sandbox/assets.py:_compute_sandbox_manifest).
#
# The hash suffix tracks mcp_client_runtime.py AND a deterministic sample of
# this module's wrapper/doc emission (see MCP_CLIENT_CODEGEN_VERSION at the
# bottom of this file), so neither a runtime edit nor an emission change needs
# a manual bump. The major marks deliberate architecture shifts only.
# "4": MCP 2026-07-28 client. "5": client extracted to the static runtime
# module; per-workspace config moved to a JSON epilogue.
_WRAPPER_CODEGEN_MAJOR = "5"

# Aggregate per-execution ceiling on result_body bytes emitted BY THE SANDBOX
# CLIENT, shipped to it via the config epilogue. This keeps a cooperative run's
# trace small (per call still capped at RESULT_BODY_MAX_BYTES; this bounds
# their sum to ~4 MiB). It is NOT a host-memory security bound: MCP_TRACE_FILE
# is visible to agent code, which can append to the JSONL directly and bypass
# this counter. The hard host-side bound on what _collect_mcp_trace reads lives
# in ptc_sandbox (it sizes the file and skips one far past any legit trace).
RESULT_BODY_TRACE_BUDGET_BYTES = 4 * 1024 * 1024


def _safe_func_name(name: str) -> str:
    """Map an MCP tool name to a wrapper function name.

    Uses the shared identifier sanitizer; falls back to a stable placeholder so
    codegen never emits an illegal ``def`` (collision detection happens upstream
    in :func:`mcp_sanitize.sanitize_tool_set`).
    """
    return sanitize_tool_name(name) or "_invalid_tool"


@dataclass(frozen=True)
class _ParamBinding:
    """One parameter's two names plus its resolved schema facts.

    ``wire`` is the schema key — the only name the server accepts. ``py`` is
    the wrapper-signature identifier the agent types. They differ exactly when
    sanitization renamed a keyword/illegal name (``type`` -> ``type_``); the
    arg dict must always emit ``wire``, everything agent-facing shows ``py``.
    """

    wire: str
    py: str
    info: dict[str, Any]


_JSON_TO_PY = {
    "string": "str",
    "number": "float",
    "integer": "int",
    "boolean": "bool",
    "array": "list",
    "object": "dict",
    "null": "None",
}

# Enums render as Literal[...] only while they stay readable as a signature;
# a hostile or bloated enum falls back to the base type (values still appear,
# capped, in the docstring).
_MAX_LITERAL_VALUES = 8
_MAX_LITERAL_CHARS = 200
_MAX_DOC_ENUM_VALUES = 12


class ToolFunctionGenerator:
    """Generates Python function code from MCP tool schemas."""

    def generate_tool_module(
        self, server_name: str, tools: list[MCPToolInfo], *, untrusted: bool = False
    ) -> str:
        """Generate a complete Python module for a server's tools.

        Args:
            server_name: Name of the MCP server
            tools: List of tools from this server
            untrusted: True for user-configured servers (any non-builtin
                source): tool names are validated/de-collided and descriptions
                sanitized. Trusted builtins keep their text verbatim.

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

from typing import Any, Literal  # noqa: F401 - Literal used only by enum params
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

        # For untrusted servers, validate + de-collide tool names so one
        # hostile/duplicate name can't break the module (builtins keep their
        # historical behavior; they are trusted and already collision-free).
        if untrusted:
            sanitized = sanitize_tool_set(tools)
            if sanitized.skipped:
                logger.warning(
                    "Skipped invalid tools for untrusted MCP server",
                    server=server_name,
                    skipped=sanitized.skipped,
                )
            tools = sanitized.kept

        # Generate functions for each tool
        for tool in tools:
            function_code = self._generate_function(tool, server_name, untrusted)
            if not function_code:
                continue  # unshippable schema — see _bind_params
            code += function_code
            code += "\n\n"

        return code

    def _generate_function(
        self, tool: MCPToolInfo, server_name: str, untrusted: bool = False
    ) -> str:
        """Generate Python function for a single tool.

        Args:
            tool: Tool information from MCP server
            server_name: Name of the MCP server this tool belongs to
            untrusted: sanitize names/text for user-configured servers;
                trusted builtin output is unchanged

        Returns:
            Python function code, or "" when the tool cannot ship at all
        """
        # Generate function signature
        func_name = _safe_func_name(tool.name)
        bindings = self._bind_params(tool, server_name)
        if bindings is None:
            return ""
        param_str = self._render_signature_params(bindings)

        # Generate docstring
        docstring = self._generate_docstring(tool, bindings, untrusted)

        # Generate function body: the arg-dict KEY is always the WIRE name —
        # the server rejects a sanitized rename like ``type_``. For untrusted
        # servers the key is emitted via repr (the name is untrusted text);
        # builtins keep the historical double-quoted literal. Required and
        # optional params part ways on None: an optional param's None means
        # "not provided" and its key is dropped, while a required param is
        # sent unconditionally — its None is an explicit JSON null, and
        # omitting the key would fail the server's required-field check.
        def _entries(bs: list[_ParamBinding]) -> str:
            if untrusted:
                return "\n".join(f"        {b.wire!r}: {b.py}," for b in bs)
            return "\n".join(f'        "{b.wire}": {b.py},' for b in bs)

        required = [b for b in bindings if b.info["required"]]
        optional = [b for b in bindings if not b.info["required"]]

        # Extract return type from description for better type hints
        return_type, _ = self._extract_return_info(tool.description)

        # Untrusted tool names are hostile-capable text — emit server/tool via
        # repr so a hostile name can't escape the string literal and inject
        # code. Builtins keep the historical double-quoted literal.
        if untrusted:
            call_line = (
                f"    return _call_mcp_tool({server_name!r}, {tool.name!r}, arguments)"
            )
        else:
            call_line = (
                f'    return _call_mcp_tool("{server_name}", "{tool.name}", arguments)'
            )

        if required:
            body = "    arguments = {\n" + _entries(required) + "\n    }\n"
        else:
            body = "    arguments = {}\n"
        if optional:
            body += (
                "\n"
                "    # Optional params: None means \"not provided\" — drop the key.\n"
                "    # (Required params above are always sent; their None is a JSON null.)\n"
                "    optional = {\n" + _entries(optional) + "\n    }\n"
                "    arguments.update({k: v for k, v in optional.items() if v is not None})\n"
            )

        return f'''def {func_name}({param_str}) -> {return_type}:
    """{docstring}"""
{body}
{call_line}'''

    def _bind_params(
        self, tool: MCPToolInfo, server_name: str
    ) -> list[_ParamBinding] | None:
        """Resolve each schema param to a (wire, py) name pair, schema order.

        Names are sanitized for EVERY server: a Python name must be a legal
        non-keyword identifier or the module is a SyntaxError, and the
        sanitizer is the identity on names that already are. Trust gates
        untrusted *text* (descriptions, enum values), never identifiers.

        Two params can sanitize to one identifier (``type`` and ``type_`` both
        -> ``type_``); the loser is renamed, not dropped, because the wire key
        it sends is unaffected and dropping a REQUIRED param ships a wrapper
        that can never satisfy the server. Returns ``None`` when a required
        param has no salvageable identifier at all — then the tool itself must
        not ship.
        """
        bindings: list[_ParamBinding] = []
        seen: set[str] = set()
        for wire, info in tool.get_parameters().items():
            py = sanitize_tool_name(wire)
            if py is None:
                if info["required"]:
                    logger.warning(
                        "Dropped MCP tool: required param has no valid Python name",
                        server=server_name,
                        tool=tool.name,
                        param=wire,
                    )
                    return None
                logger.warning(
                    "Skipped invalid MCP tool param",
                    server=server_name,
                    tool=tool.name,
                    param=wire,
                )
                continue
            while py in seen:
                py += "_"
            seen.add(py)
            bindings.append(_ParamBinding(wire, py, info))
        return bindings

    def _render_signature_params(self, bindings: list[_ParamBinding]) -> str:
        """Render the wrapper parameter list, required params first."""
        rendered = []
        ordered = [b for b in bindings if b.info["required"]] + [
            b for b in bindings if not b.info["required"]
        ]
        for b in ordered:
            annotation = self._annotation(b.info)
            if b.info["required"]:
                rendered.append(f"{b.py}: {annotation}")
            elif b.info.get("has_default") and b.info.get("default") is not None:
                rendered.append(f"{b.py}: {annotation} = {b.info['default']!r}")
            else:
                if not annotation.endswith("| None"):
                    annotation = f"{annotation} | None"
                rendered.append(f"{b.py}: {annotation} = None")
        return ", ".join(rendered)

    def _annotation(self, info: dict[str, Any]) -> str:
        """Python type annotation for a resolved param schema.

        Enums become ``Literal[...]`` (repr'd values — safe literals by
        construction) while they stay small enough to read as a signature;
        arrays pick up their item type; ``nullable`` appends ``| None``.
        """
        annotation = self._base_annotation(info)
        if info.get("nullable") and annotation != "Any" and not annotation.endswith("| None"):
            annotation = f"{annotation} | None"
        return annotation

    def _base_annotation(self, info: dict[str, Any]) -> str:
        enum = info.get("enum")
        if (
            enum
            and len(enum) <= _MAX_LITERAL_VALUES
            and all(isinstance(v, (str, int, bool)) for v in enum)
        ):
            literal = "Literal[{}]".format(", ".join(repr(v) for v in enum))
            if len(literal) <= _MAX_LITERAL_CHARS:
                return literal
        json_type = info.get("type")
        if json_type == "array":
            item = _JSON_TO_PY.get(info.get("items_type") or "")
            return f"list[{item}]" if item else "list"
        if not isinstance(json_type, str):
            return "Any"
        return _JSON_TO_PY.get(json_type, "Any")

    def _generate_docstring(
        self, tool: MCPToolInfo, bindings: list[_ParamBinding], untrusted: bool = False
    ) -> str:
        """Generate docstring for a tool function.

        Args:
            tool: Tool information
            bindings: Parameter bindings (py names are what the agent types)
            untrusted: full untrusted-text sanitization for user-configured
                servers; trusted builtins keep the backslash-only escape

        Returns:
            Formatted docstring
        """

        def _escape(text: str) -> str:
            # Untrusted text is fully sanitized — triple-quote breakouts,
            # control chars, length cap. Builtins keep a lighter escape
            # (sanitization could truncate legitimate long builtin docstrings)
            # but still neutralize the ``\"\"\"`` delimiter: trust labels
            # servers, not strings, and a mislabeled server must not get a
            # docstring breakout for free.
            if untrusted:
                return sanitize_tool_text(text)
            return text.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')

        lines = []

        # Add description
        if tool.description:
            lines.append(_escape(tool.description))
            lines.append("")

        # Add parameters
        if bindings:
            lines.append("Args:")
            for b in bindings:
                escaped_desc = _escape(b.info.get("description", ""))
                # The schema `type` field is untrusted text like the
                # description — escape it (and coerce non-str values) so it
                # can't terminate the docstring.
                param_type = _escape(str(b.info["type"]))
                required = " (required)" if b.info["required"] else ""
                suffix = _escape(self._facts_suffix(b.info))
                lines.append(
                    f"    {b.py} ({param_type}){required}: {escaped_desc}{suffix}"
                )
            lines.append("")

        # Add returns - extract from description if available. The description
        # is untrusted, so the extracted text gets the same escape as every
        # other embedded field; return_type is a constant from the pattern
        # map, never raw input.
        return_type, return_desc = self._extract_return_info(tool.description)
        return_desc = _escape(return_desc)
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
        example_call = self._render_example_call(tool, bindings, untrusted, limit=2)
        if example_call:
            lines.append("Example:")
            lines.append(f"    result = {example_call}")

        return "\n    ".join(lines)

    def _facts_suffix(self, info: dict[str, Any]) -> str:
        """Schema facts worth surfacing beside a param description.

        Allowed values and real defaults are what agents otherwise guess at
        (and guess wrong — enum-typed params fail with opaque server errors).
        """
        parts = []
        enum = info.get("enum")
        if enum:
            shown = ", ".join(repr(v) for v in enum[:_MAX_DOC_ENUM_VALUES])
            if len(enum) > _MAX_DOC_ENUM_VALUES:
                shown += ", ..."
            parts.append(f"[allowed: {shown}]")
        if info.get("has_default") and info.get("default") is not None:
            parts.append(f"[default: {info['default']!r}]")
        return (" " + " ".join(parts)) if parts else ""

    def _render_example_call(
        self,
        tool: MCPToolInfo,
        bindings: list[_ParamBinding],
        untrusted: bool,
        limit: int | None = None,
    ) -> str:
        """One-line example call with schema-true values where the schema
        offers them (default, then first enum value) — a placeholder example
        like ``mode="example"`` fails on every enum-typed param."""
        example_args = []
        for b in bindings:
            if not b.info["required"]:
                continue
            value = self._example_value(b.info)
            # Schema-derived values are untrusted text; neutralize docstring/
            # fence breakouts before embedding in agent-facing docs.
            if untrusted:
                value = sanitize_tool_text(value)
            example_args.append(f"{b.py}={value}")
        if not example_args:
            return ""
        func_name = _safe_func_name(tool.name)
        return f"{func_name}({', '.join(example_args[:limit])})"

    def _example_value(self, info: dict[str, Any]) -> str:
        if info.get("has_default") and info.get("default") is not None:
            return repr(info["default"])
        enum = info.get("enum")
        if enum:
            return repr(enum[0])
        placeholders = {
            "string": '"example"',
            "number": "42.0",
            "integer": "42",
            "boolean": "True",
            "array": "[]",
            "object": "{}",
        }
        json_type = info.get("type")
        if not isinstance(json_type, str):
            return '""'
        return placeholders.get(json_type, '""')

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
        self, tool: MCPToolInfo, *, untrusted: bool = False
    ) -> str:
        """Generate markdown documentation for a tool.

        Args:
            tool: Tool information
            untrusted: sanitize description text for user-configured servers;
                trusted builtin output is unchanged

        Returns:
            Markdown documentation string
        """
        func_name = _safe_func_name(tool.name)
        # Same bindings as the generated wrapper: the documented signature is
        # the exact callable — sanitized names included. Docs that show the
        # wire name (`type`) for a wrapper whose param is `type_` send the
        # agent straight into a TypeError.
        bindings = self._bind_params(tool, tool.server_name)
        if bindings is None:
            # The wrapper module drops this tool, so the docs must not advertise
            # it as callable. No schema text is echoed — the reason is structural.
            return (
                f"# {func_name} (unavailable)\n\n"
                "This tool is not callable from the sandbox: a required "
                "parameter of its schema has no valid Python name.\n"
            )
        description = (
            sanitize_tool_text(tool.description) if untrusted else tool.description
        )

        signature = f"{func_name}({self._render_signature_params(bindings)})"
        if untrusted:
            # The rendered signature embeds repr'd schema values (enum
            # literals, defaults) — untrusted text like every other field on
            # this page. Identity for honest schemas, so parity with the
            # wrapper signature holds except under active hostility.
            signature = sanitize_tool_text(signature)

        # Build documentation
        doc = f"# {signature}\n\n"

        if description:
            doc += f"{description}\n\n"

        doc += "## Parameters\n\n"
        if bindings:
            for b in bindings:
                required_marker = (
                    "**Required**" if b.info["required"] else "Optional"
                )
                param_type = str(b.info["type"])
                param_desc = b.info.get("description", "")
                facts = self._facts_suffix(b.info)
                if untrusted:
                    param_type = sanitize_tool_text(param_type)
                    param_desc = sanitize_tool_text(param_desc)
                    facts = sanitize_tool_text(facts)
                doc += f"- `{b.py}` ({param_type}) - {required_marker}{facts}\n"
                if param_desc:
                    doc += f"  {param_desc}\n"
                doc += "\n"
        else:
            doc += "No parameters\n\n"

        doc += "## Returns\n\n"
        return_type, return_desc = self._extract_return_info(tool.description)
        if untrusted:
            return_desc = sanitize_tool_text(return_desc)
        doc += f"**Type:** `{return_type}`\n\n"
        doc += f"{return_desc}\n\n"

        doc += "## Example\n\n"
        doc += "```python\n"
        doc += f"from tools.{tool.server_name} import {func_name}\n\n"

        example_call = self._render_example_call(tool, bindings, untrusted)
        doc += f"result = {example_call or f'{func_name}()'}\n"
        doc += "print(result)  # noqa: T201\n"
        doc += "```\n"

        return doc

    def generate_client_config(
        self,
        server_configs: list[MCPServerConfig],
        working_dir: str = "/home/workspace",
    ) -> dict[str, Any]:
        """Build the per-workspace config dict the client runtime consumes.

        Builtin servers embed only env key NAMES — never values (the sandbox
        already has the resolved values in os.environ, injected at creation
        time). Untrusted servers embed their full env/header mappings, whose
        values are either non-secret literals or ``${vault:NAME}`` placeholders
        resolved in-sandbox against the vault file only. OAuth-connected
        servers are only marked relay-bound: the runtime dials the egress relay
        and reads the grant id from the sandbox's credential file, so the
        vendor URL, the grant and every token stay host-side.

        Trust ships as the ``untrusted`` bool computed here — never as a raw
        ``source`` tag the runtime would have to re-interpret.
        """
        servers: dict[str, dict[str, Any]] = {}
        for server in server_configs:
            untrusted = is_untrusted_server(server)
            if server.oauth_connection_id is not None:
                servers[server.name] = {
                    "transport": "http",
                    "untrusted": untrusted,
                    "relay_bound": True,
                }
                continue
            if server.transport in ("sse", "http"):
                entry: dict[str, Any] = {
                    "transport": server.transport,
                    "untrusted": untrusted,
                    "url": server.url or "",
                }
                if untrusted:
                    entry["headers"] = dict(server.headers or {})
                    entry["discovery_uses_secrets"] = discovery_should_use_secrets(
                        server
                    )
                servers[server.name] = entry
                continue

            # Stdio transport. `uv run python mcp_servers/<file>.py` paths are
            # rewritten to the sandbox's copy of the server file.
            command = server.command
            args = [str(a) for a in server.args]
            if (
                command == "uv"
                and len(args) >= 3
                and args[0] == "run"
                and args[1] == "python"
            ):
                filename = Path(args[2]).name
                args = ["run", "python", f"{working_dir}/mcp_servers/{filename}"]
                logger.debug(
                    "Transformed MCP server command for sandbox",
                    server=server.name,
                    original_args=server.args,
                    sandbox_args=args,
                )
            entry = {
                "transport": "stdio",
                "untrusted": untrusted,
                "command": command,
                "args": args,
            }
            if untrusted:
                entry["env"] = dict(server.env or {})
                entry["discovery_uses_secrets"] = discovery_should_use_secrets(server)
            else:
                entry["env_keys"] = list(server.env.keys()) if server.env else []
            servers[server.name] = entry

        return {
            "working_dir": working_dir,
            "servers": servers,
            "result_body_max_bytes": RESULT_BODY_MAX_BYTES,
            "result_body_trace_budget_bytes": RESULT_BODY_TRACE_BUDGET_BYTES,
        }

    def generate_mcp_client_code(
        self,
        server_configs: list[MCPServerConfig],
        working_dir: str = "/home/workspace",
    ) -> str:
        """Compose the uploadable mcp_client.py: static runtime + config epilogue.

        The double json.dumps makes the embedded config injection-safe by
        construction: the inner call serializes the config, the outer one turns
        that JSON text into a valid Python string literal with all quoting and
        control characters escaped — hostile server names can never terminate
        the literal. The CLI dispatch lives in the epilogue too, after the
        config apply — the runtime source must never self-dispatch, or CLI-mode
        discovery would run against the placeholder config.
        """
        config = self.generate_client_config(server_configs, working_dir=working_dir)
        config_json = json.dumps(config, sort_keys=True)
        return (
            client_runtime_source()
            + "\n\n# --- Per-workspace configuration (generated epilogue). ---\n"
            + f"_apply_config_dict(json.loads({json.dumps(config_json)}))\n"
            + '\nif __name__ == "__main__":\n    _cli_main()\n'
        )


# Covers every emission shape that matters to deployed sandboxes: a renamed
# soft-keyword param (wire-key mapping), a hard-keyword param, an enum, a
# typed array, a nullable anyOf, and a default.
_EMISSION_PROBE_TOOL = MCPToolInfo(
    name="probe_tool",
    description="Emission probe.\n\nReturns:\n    dict: probe result",
    input_schema={
        "type": "object",
        "properties": {
            "type": {"type": "string", "enum": ["market", "limit"]},
            "class": {"type": "string"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "note": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "default": "hi",
            },
        },
        "required": ["type", "class"],
    },
    server_name="probe",
)

# Second probe: the knobs the first can't reach — numeric/boolean/object
# placeholder examples, an enum past the doc cap, a boolean default, a
# multiline Returns section, and hostile quoting/control chars that must be
# neutralized on BOTH trust branches. Hash-relevant only; never uploaded.
_EMISSION_PROBE_TOOL_2 = MCPToolInfo(
    name="probe_tool_2",
    description=(
        'Second probe with a breakout attempt: """ and a backslash \\.\n'
        "\n"
        "Returns:\n"
        "    list[dict]: first line of a multiline return\n"
        '    second line with \\ and """ and \'\'\' inside'
    ),
    input_schema={
        "type": "object",
        "properties": {
            "count": {
                "type": "integer",
                "description": "Count.\x07 Ends with \\ and ''' fence",
            },
            "ratio": {"type": "number"},
            "flag": {"type": "boolean", "default": False},
            "payload": {"type": "object"},
            "mode": {
                "type": "string",
                "enum": [f"m{i}" for i in range(_MAX_DOC_ENUM_VALUES + 2)],
            },
        },
        "required": ["count", "ratio", "payload"],
    },
    server_name="probe",
)

# Third probe axis: the config/epilogue emission. One server per config
# branch — builtin uv-stdio (path rewrite + env_keys), untrusted workspace
# http (headers + secret-mode hint), OAuth relay-bound. Hash-relevant only;
# never uploaded.
_EMISSION_PROBE_SERVERS = [
    MCPServerConfig(
        name="probe_builtin",
        transport="stdio",
        command="uv",
        args=["run", "python", "mcp_servers/probe_server.py"],
        env={"PROBE_API_KEY": "${PROBE_API_KEY}"},
    ),
    MCPServerConfig(
        name="probe_workspace",
        transport="http",
        url="https://probe.example/mcp",
        headers={"Authorization": "${vault:PROBE_TOKEN}"},
        source="workspace",
    ),
    MCPServerConfig(
        name="probe_oauth",
        transport="http",
        url="https://probe.example/oauth",
        source="workspace",
        oauth_connection_id="probe-conn",
    ),
]


def _emission_probe_text() -> str:
    """Deterministic sample of this module's wrapper/doc/config emission.

    Folded into ``MCP_CLIENT_CODEGEN_VERSION`` so an emission change reaches
    warm sandboxes automatically — without this, only runtime-file edits would
    invalidate their cached wrapper modules. The epilogue slice covers
    ``generate_client_config`` and the epilogue template: trust flags, path
    rewrites and relay binding are computed at generation time, so a
    logic-only change there would otherwise never move the hash. Pinned by the
    committed golden in ``tests/unit/core/emission_probe_golden.txt`` so
    emission drift is a visible diff, never a silent hash move.
    """
    gen = ToolFunctionGenerator()
    probes = [_EMISSION_PROBE_TOOL, _EMISSION_PROBE_TOOL_2]
    return (
        gen.generate_tool_module("probe", probes)
        + gen.generate_tool_module("probe", probes, untrusted=True)
        + "".join(
            gen.generate_tool_documentation(tool, untrusted=untrusted)
            for tool in probes
            for untrusted in (False, True)
        )
        + gen.generate_mcp_client_code(_EMISSION_PROBE_SERVERS, working_dir="/probe")[
            len(client_runtime_source()) :
        ]
    )


MCP_CLIENT_CODEGEN_VERSION = "{}.{}".format(
    _WRAPPER_CODEGEN_MAJOR,
    hashlib.sha256(
        (client_runtime_source() + _emission_probe_text()).encode("utf-8")
    ).hexdigest()[:12],
)
