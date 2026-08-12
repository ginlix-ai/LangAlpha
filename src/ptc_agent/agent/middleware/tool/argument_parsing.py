"""Tool argument parsing middleware.

Bridges two LLM-side serialization quirks before tool args hit Pydantic:

1. JSON-encoded string → Python object, for tool args declared as list/dict
   (some providers return ``'["a","b"]'`` where the schema wants ``["a","b"]``).
2. Python object → JSON-encoded string, for tool args declared as string-typed
   JSON payloads (Write/Edit on ``.json`` files in particular). Models often
   pass the parsed dict directly, which would otherwise trip a Pydantic
   ``string_type`` error before our backend ever runs.
"""
import json
import logging

from langchain.agents.middleware import AgentMiddleware

logger = logging.getLogger(__name__)


# Tool args that must reach the handler as strings. Without this, an LLM that
# stringifies the payload would have it JSON-decoded back into a dict here, and
# an LLM that passes a dict would be rejected by Pydantic — exactly the two
# failure modes this middleware exists to prevent.
_STRING_VALUED_ARGS: dict[str, frozenset[str]] = {
    "Write": frozenset({"content"}),
    "Edit": frozenset({"old_string", "new_string"}),
}


def parse_tool_args(args, tool_name: str | None = None):
    """Coerce a tool-call args dict to match the tool's Pydantic schema.

    Canonical for both the live middleware and checkpoint replay: string-valued
    JSON args (Write/Edit payloads) are JSON-encoded, and JSON-looking strings on
    other args are decoded. Non-dict input passes through unchanged.
    """
    if not isinstance(args, dict):
        return args

    string_args = _STRING_VALUED_ARGS.get(tool_name or "", frozenset())

    parsed_args = {}
    for key, value in args.items():
        if key in string_args:
            # Field is string-typed: coerce dict/list to JSON; leave any
            # string alone (it IS the payload — do not JSON-decode it).
            if isinstance(value, (dict, list)):
                parsed_args[key] = json.dumps(value, ensure_ascii=False)
                logger.debug(
                    "Encoded %s arg %r from %s to JSON string",
                    tool_name, key, type(value).__name__,
                )
            else:
                parsed_args[key] = value
            continue

        if isinstance(value, str) and (
            (value.startswith("[") and value.endswith("]"))
            or (value.startswith("{") and value.endswith("}"))
        ):
            try:
                parsed_args[key] = json.loads(value)
                logger.debug(
                    "Parsed JSON string argument %r: %s... -> %s",
                    key, value[:50], type(parsed_args[key]).__name__,
                )
            except json.JSONDecodeError:
                parsed_args[key] = value
        else:
            parsed_args[key] = value

    return parsed_args


class ToolArgumentParsingMiddleware(AgentMiddleware):
    """Reconcile LLM arg serialization with each tool's Pydantic schema.

    See module docstring for the two failure modes this handles.
    """

    def _parse_args(self, args, tool_name: str | None = None):
        return parse_tool_args(args, tool_name)

    def wrap_tool_call(self, request, handler):
        tool_call = request.tool_call
        if "args" in tool_call:
            tool_call["args"] = self._parse_args(
                tool_call["args"], tool_name=tool_call.get("name")
            )
        return handler(request)

    async def awrap_tool_call(self, request, handler):
        tool_call = request.tool_call
        if "args" in tool_call:
            tool_call["args"] = self._parse_args(
                tool_call["args"], tool_name=tool_call.get("name")
            )
        return await handler(request)
