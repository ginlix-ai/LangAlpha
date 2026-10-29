"""Translation of an MCP server's advertised schemas into wrapper facts.

Transport-free and read-only: the registry, codegen and the prompt formatter
all read a server's schema through this one module, so they cannot drift into
disagreeing about what a tool's parameters or return type are.
"""

import re
from typing import Any, NamedTuple


def client_identity(client: object) -> dict[str, Any] | None:
    """The handshake card a connected client is holding, as the wire spelled it.

    Never raises, and that is the whole point. This is decoration on paths
    whose real job is tools: a card the SDK keeps somewhere else, or a server
    that stamps nonsense, must not fail a connection or turn a good discovery
    into an error row. The spec says the field is display-only; this makes the
    code agree.

    Dumped by alias so a server discovered in the sandbox and one connected
    here hand the UI the same spelling.
    """
    try:
        info = getattr(client, "server_info", None)
        if info is None:
            return None
        return info.model_dump(mode="json", by_alias=True, exclude_none=True)
    except Exception:  # noqa: BLE001 — a business card is never worth a failure
        return None


class ResolvedType(NamedTuple):
    type: str
    nullable: bool
    enum: list[Any] | None
    items_type: str | None


def resolve_schema_type(prop: dict[str, Any]) -> ResolvedType:
    """Resolve a property schema to a base JSON type + the facts wrappers need.

    Handles the two shapes real servers actually emit for optionality —
    pydantic's ``anyOf [T, null]`` and the ``type: [T, "null"]`` list form —
    so a nullable string surfaces as ``string`` + nullable instead of
    degrading to ``any``. Anything more exotic still falls back to ``any``.
    """
    t = prop.get("type")
    node = prop
    nullable = False
    if t is None:
        variants = prop.get("anyOf") or prop.get("oneOf")
        if isinstance(variants, list):
            typed = [
                v for v in variants
                if isinstance(v, dict) and v.get("type") != "null"
            ]
            nullable = len(typed) != len(variants)
            if len(typed) == 1:
                node = typed[0]
                t = node.get("type")
    if isinstance(t, list):
        non_null = [x for x in t if x != "null"]
        nullable = nullable or len(non_null) != len(t)
        t = non_null[0] if len(non_null) == 1 else None
    if not isinstance(t, str):
        t = "any"
    enum = node.get("enum")
    if not (isinstance(enum, list) and enum):
        enum = None
    items_type = None
    if t == "array":
        items = node.get("items")
        if isinstance(items, dict) and isinstance(items.get("type"), str):
            items_type = items["type"]
    return ResolvedType(t, nullable, enum, items_type)


_RETURNS_TYPE_RE = re.compile(
    r"Returns?:\s*\n?\s*(\w+(?:\[[\w,\s]+\])?)", re.IGNORECASE
)

_RETURN_TYPE_NAMES = {
    "dict": "dict",
    "dictionary": "dict",
    "list": "list",
    "array": "list",
    "str": "str",
    "string": "str",
    "int": "int",
    "integer": "int",
    "float": "float",
    "number": "float",
    "bool": "bool",
    "boolean": "bool",
}


def extract_return_type(description: str) -> str:
    """Best-effort return type from a tool description's prose ``Returns:`` line.

    A tool's advertised schema describes only its input, so the docstring is
    the sole signal for what comes back; anything unrecognized stays ``Any``.
    """
    if not description:
        return "Any"
    match = _RETURNS_TYPE_RE.search(description)
    if not match:
        return "Any"
    return _RETURN_TYPE_NAMES.get(match.group(1).lower(), "Any")


class MCPToolInfo:
    """Snapshot of a single tool's schema as reported by its MCP server."""

    def __init__(
        self,
        name: str,
        description: str,
        input_schema: dict[str, Any],
        server_name: str,
    ) -> None:
        self.name = name
        self.description = description
        self.input_schema = input_schema
        self.server_name = server_name

    def get_parameters(self) -> dict[str, Any]:
        """Return ``{param_name: {type, description, required, default, ...}}``.

        Beyond the historical keys, each entry carries ``has_default`` (a
        stored ``default: null`` is not the same as no default), ``nullable``,
        ``enum`` and ``items_type`` — resolved by :func:`resolve_schema_type`
        so wrappers and docs can show real types and allowed values.

        Total by construction: a schema is whatever a server (or a cache
        written by an older one) says it is, and this runs inside workspace
        asset sync — one ``"properties": []`` must degrade to "no parameters",
        never raise and wedge every other server's sync.
        """
        params: dict[str, Any] = {}

        schema = self.input_schema if isinstance(self.input_schema, dict) else {}
        properties = schema.get("properties")
        if not isinstance(properties, dict):
            return params

        required_raw = schema.get("required")
        required_params = required_raw if isinstance(required_raw, list) else []

        for param_name, param_info in properties.items():
            if not isinstance(param_info, dict):
                param_info = {}
            resolved = resolve_schema_type(param_info)
            params[param_name] = {
                "type": resolved.type,
                "description": param_info.get("description", ""),
                "required": param_name in required_params,
                "default": param_info.get("default"),
                "has_default": "default" in param_info,
                "nullable": resolved.nullable,
                "enum": resolved.enum,
                "items_type": resolved.items_type,
            }

        return params

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.get_parameters(),
            "server_name": self.server_name,
            "return_type": extract_return_type(self.description),
        }
