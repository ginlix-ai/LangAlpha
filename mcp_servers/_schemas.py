"""Published output schemas for the builtin MCP servers.

Each tool's return annotation is an ``output_model()`` subclass of
``RootModel[dict]``: any mapping validates (both ``make_response`` and
``make_error`` envelopes — the error contract can never become an ``isError``
exception via schema validation), the returned dict round-trips into
``structuredContent`` unpadded, and the *published* schema is the precise
envelope union injected via ``json_schema_extra``. The published root must be
``type: "object"`` with a sibling ``anyOf`` of required-key sets — never a root
``anyOf`` (breaks pre-2026 ``tools/list`` parsers) and never a Python union
annotation (the SDK wraps unions under ``result``).

Requires pydantic only (ships with the ``mcp`` SDK on every sandbox image);
schema derivation is identical under mcp 1.x and 2.x.
"""

from __future__ import annotations

from typing import Any

from pydantic import RootModel

# ── data-shape descriptors (the `data` property) ──────────────────────────────

#: List of record objects (rows).
RECORDS: dict[str, Any] = {"type": "array", "items": {"type": "object"}}

#: Map keyed by symbol/date/etc. → list of record objects.
RECORDS_BY_KEY: dict[str, Any] = {
    "type": "object",
    "additionalProperties": {"type": "array", "items": {"type": "object"}},
}

#: Single record object.
OBJECT: dict[str, Any] = {"type": "object"}

#: Map keyed by symbol/section/etc. → record object.
OBJECTS_BY_KEY: dict[str, Any] = {
    "type": "object",
    "additionalProperties": {"type": "object"},
}

#: Shape branches on an argument — documented in the tool docstring instead.
ANY: dict[str, Any] = {}

# ── property vocabulary (echo and tool-specific keys) ─────────────────────────

STR: dict[str, Any] = {"type": "string"}
INT: dict[str, Any] = {"type": "integer"}
BOOL: dict[str, Any] = {"type": "boolean"}

#: Echoed verbatim, so null when the caller omitted the value.
NULLABLE_STR: dict[str, Any] = {"type": ["string", "null"]}

STR_LIST: dict[str, Any] = {"type": "array", "items": STR}


def described(prop: dict[str, Any], description: str) -> dict[str, Any]:
    """A vocabulary entry under a call-site-specific description."""
    return {**prop, "description": description}


#: Per-symbol error envelopes carried alongside a partial-success payload.
#: Two wordings are published today (``described(ERRORS, ...)`` keeps each
#: server's bytes stable); unify them the next time the schemas may move.
ERRORS: dict[str, Any] = described(RECORDS, "Error envelopes for symbols that failed.")

#: The error arm's properties. Servers off the market-data envelope override
#: ``detail`` — its wording and type drifted before the contract settled.
ERROR_PROPS: dict[str, dict[str, Any]] = {
    "error": described(STR, "Machine-readable error code (error responses only)."),
    "detail": described(STR, "Human-readable error detail (error responses only)."),
}

# Standard envelope echo keys (make_response keyword arguments).
_FRAME_PROPS: dict[str, dict[str, Any]] = {
    "symbol": described(STR, "Echoed ticker or identifier."),
    "interval": described(STR, "Canonical interval echoed back."),
    "currency": described(STR, "ISO 4217 currency of price fields."),
    "timezone": described(STR, "IANA timezone of timestamps."),
}


def union_schema(
    properties: dict[str, Any],
    success_required: tuple[str, ...],
    *,
    error_props: dict[str, dict[str, Any]] = ERROR_PROPS,
    error_required: tuple[str, ...] = ("error", "detail"),
) -> dict[str, Any]:
    """The published frame: root object plus a sibling ``anyOf`` of required-key sets.

    ``additionalProperties`` stays true so open-ended echo keys and vendor
    drift never hard-fail a tool. The error properties always land last.
    """
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": {**properties, **error_props},
        "anyOf": [
            {"required": list(success_required)},
            {"required": list(error_required)},
        ],
    }


def envelope_schema(
    data_shape: dict[str, Any],
    *,
    frame: tuple[str, ...] = (),
    echo: dict[str, dict[str, Any]] | None = None,
    data_description: str | None = None,
) -> dict[str, Any]:
    """Published schema for the success∪error envelope of one market-data tool.

    ``frame`` selects the standard echo keys (symbol/interval/currency/
    timezone); ``echo`` adds tool-specific properties.
    """
    props: dict[str, Any] = {key: _FRAME_PROPS[key] for key in frame}
    data = dict(data_shape)
    if data_description:
        data["description"] = data_description
    props["count"] = described(INT, "Number of records in data.")
    props["data"] = data
    props["source"] = described(STR, "Upstream data source.")
    if echo:
        props.update(echo)
    return union_schema(props, ("count", "data", "source"))


class _EnvelopeBase(RootModel[dict[str, Any]]):
    """Any mapping validates; subclasses only replace the published schema."""


def output_model(name: str, schema: dict[str, Any]) -> type[_EnvelopeBase]:
    """RootModel[dict] subclass publishing ``schema`` as the tool's outputSchema.

    ``name`` is passed rather than derived because it lands on the wire as the
    published schema's ``title`` — renaming a model is a contract change.
    """
    return type(name, (_EnvelopeBase,), {"model_config": {"json_schema_extra": schema}})
