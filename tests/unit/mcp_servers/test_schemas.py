"""Published-output-schema contract: builder invariants + SDK wire behavior.

The wire tests drive an in-process MCPServer through the real v2 Client so the
SDK's own result conversion and client-side auto-validation are exercised —
the two layers that would silently turn an envelope drift into an ``isError``.
"""

import asyncio

from mcp.client import Client

from mcp_servers._bootstrap import MCPServer
from mcp_servers._envelope import make_error, make_response
from mcp_servers._schemas import (
    ANY,
    INT,
    OBJECT,
    RECORDS,
    envelope_schema,
    output_model,
    union_schema,
)

# ---------------------------------------------------------------------------
# Builder invariants
# ---------------------------------------------------------------------------


def test_envelope_schema_shape():
    schema = envelope_schema(RECORDS)
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is True
    # Sibling anyOf at the root: exactly the success/error required-key sets.
    assert schema["anyOf"] == [
        {"required": ["count", "data", "source"]},
        {"required": ["error", "detail"]},
    ]
    props = schema["properties"]
    for key in ("count", "data", "source", "error", "detail"):
        assert key in props
    # The SDK's union-wrapping convention must never appear in our schemas.
    assert "result" not in props
    assert props["data"]["type"] == "array"


def test_envelope_schema_frame_and_echo():
    schema = envelope_schema(
        OBJECT,
        frame=("symbol", "interval", "currency", "timezone"),
        echo={"period": {"type": "string"}},
        data_description="Quote fields keyed by name.",
    )
    props = schema["properties"]
    for key in ("symbol", "interval", "currency", "timezone", "period"):
        assert key in props
    assert props["data"]["description"] == "Quote fields keyed by name."
    # ANY leaves data unconstrained apart from the description.
    assert envelope_schema(ANY)["properties"]["data"] == {}


def test_union_schema_error_override():
    """The off-contract servers (scrape, x_mcp) reach the same frame through
    ``error_props``/``error_required`` — including x_mcp's one-key error arm."""
    detail = {"description": "String or structured payload."}
    schema = union_schema(
        {"posts": RECORDS, "result_count": INT},
        ("posts", "result_count"),
        error_props={"error": {"type": "string"}, "detail": detail},
        error_required=("error",),
    )
    assert schema["type"] == "object" and schema["additionalProperties"] is True
    # Error properties land last, whatever the success shape.
    assert list(schema["properties"]) == ["posts", "result_count", "error", "detail"]
    assert schema["properties"]["detail"] == detail
    assert schema["anyOf"] == [
        {"required": ["posts", "result_count"]},
        {"required": ["error"]},
    ]


def test_output_model_validates_both_arms_and_dumps_unpadded():
    _Out = output_model("BothArmsOut", envelope_schema(RECORDS))
    success = {"symbol": "A", "count": 1, "data": [{}], "source": "s", "echo": True}
    assert _Out.model_validate(success).model_dump() == success
    error = make_error("not_found", "nope", symbol="Z")
    assert _Out.model_validate(error).model_dump() == error


# ---------------------------------------------------------------------------
# Wire behavior through the real SDK
# ---------------------------------------------------------------------------

_WIRE_SCHEMA = envelope_schema(RECORDS, frame=("symbol",), echo={"note": {"type": "string"}})
_WireOut = output_model("WireOut", _WIRE_SCHEMA)

_wire_mcp = MCPServer("SchemasWireTest")
_last_return: dict = {}


@_wire_mcp.tool()
def probe(fail: bool = False) -> _WireOut:
    if fail:
        result = make_error("not_found", "nope", symbol="Z")
    else:
        result = make_response([{"a": 1}], source="test", symbol="A", note="echo")
    _last_return.clear()
    _last_return.update(result)
    return result


def test_published_schema_and_structured_roundtrip():
    async def run():
        async with Client(_wire_mcp) as client:
            tools = await client.list_tools()
            published = {
                k: v for k, v in tools.tools[0].output_schema.items() if k != "title"
            }
            assert published == _WIRE_SCHEMA

            result = await client.call_tool("probe", {})
            assert not result.is_error
            # Byte-equal round-trip, open-ended echo key included; never
            # nested under a `result` wrapper.
            assert result.structured_content == _last_return
            assert result.structured_content["note"] == "echo"

    asyncio.run(run())


def test_error_envelope_passes_validation_not_is_error():
    async def run():
        async with Client(_wire_mcp) as client:
            result = await client.call_tool("probe", {"fail": True})
            assert not result.is_error
            assert result.structured_content == {
                "error": "not_found",
                "detail": "nope",
                "symbol": "Z",
            }

    asyncio.run(run())
