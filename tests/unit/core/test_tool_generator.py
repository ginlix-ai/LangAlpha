"""Tests for ptc_agent.core.tool_generator module.

Covers ToolFunctionGenerator: type mapping, function generation,
docstring creation, and return type extraction.
"""

import ast

from ptc_agent.core.mcp_registry import MCPToolInfo
from ptc_agent.core.tool_generator import ToolFunctionGenerator


def _make_tool(
    name: str = "get-data",
    description: str = "Fetch data",
    input_schema: dict | None = None,
    server_name: str = "test_server",
) -> MCPToolInfo:
    schema = input_schema or {
        "type": "object",
        "properties": {
            "symbol": {
                "type": "string",
                "description": "Ticker symbol",
            },
        },
        "required": ["symbol"],
    }
    return MCPToolInfo(
        name=name,
        description=description,
        input_schema=schema,
        server_name=server_name,
    )


def _info(**overrides) -> dict:
    """Resolved-param info dict in the shape MCPToolInfo.get_parameters emits."""
    info = {
        "type": "string",
        "description": "",
        "required": True,
        "default": None,
        "has_default": False,
        "nullable": False,
        "enum": None,
        "items_type": None,
    }
    info.update(overrides)
    return info


class TestAnnotation:
    """Tests for _annotation / _base_annotation (resolved schema → Python type)."""

    def test_scalar_types(self):
        gen = ToolFunctionGenerator()
        assert gen._annotation(_info(type="string")) == "str"
        assert gen._annotation(_info(type="number")) == "float"
        assert gen._annotation(_info(type="integer")) == "int"
        assert gen._annotation(_info(type="boolean")) == "bool"
        assert gen._annotation(_info(type="object")) == "dict"

    def test_unknown_or_missing_type_returns_any(self):
        gen = ToolFunctionGenerator()
        assert gen._annotation(_info(type="custom_type")) == "Any"
        assert gen._annotation(_info(type=None)) == "Any"

    def test_array_items_type(self):
        gen = ToolFunctionGenerator()
        assert gen._annotation(_info(type="array", items_type="string")) == "list[str]"
        assert gen._annotation(_info(type="array")) == "list"

    def test_enum_becomes_literal(self):
        gen = ToolFunctionGenerator()
        assert (
            gen._annotation(_info(enum=["market", "limit"]))
            == "Literal['market', 'limit']"
        )

    def test_enum_over_value_cap_falls_back_to_base_type(self):
        gen = ToolFunctionGenerator()
        enum = [f"v{i}" for i in range(9)]  # _MAX_LITERAL_VALUES is 8
        assert gen._annotation(_info(enum=enum)) == "str"

    def test_enum_with_non_literal_values_falls_back(self):
        gen = ToolFunctionGenerator()
        assert gen._annotation(_info(type="number", enum=[1.5, 2.5])) == "float"

    def test_nullable_appends_none(self):
        gen = ToolFunctionGenerator()
        assert gen._annotation(_info(type="string", nullable=True)) == "str | None"
        # Any already admits None — no suffix.
        assert gen._annotation(_info(type="custom", nullable=True)) == "Any"


class TestExampleValue:
    """Tests for _example_value (schema-true example precedence)."""

    def test_type_placeholders(self):
        gen = ToolFunctionGenerator()
        assert gen._example_value(_info(type="string")) == '"example"'
        assert gen._example_value(_info(type="number")) == "42.0"
        assert gen._example_value(_info(type="integer")) == "42"
        assert gen._example_value(_info(type="boolean")) == "True"
        assert gen._example_value(_info(type="array")) == "[]"
        assert gen._example_value(_info(type="object")) == "{}"

    def test_unknown_type_returns_empty_string(self):
        gen = ToolFunctionGenerator()
        assert gen._example_value(_info(type="foo")) == '""'
        assert gen._example_value(_info(type=None)) == '""'

    def test_default_wins_over_enum(self):
        gen = ToolFunctionGenerator()
        info = _info(enum=["a", "b"], has_default=True, default="b")
        assert gen._example_value(info) == "'b'"

    def test_enum_first_value_when_no_default(self):
        gen = ToolFunctionGenerator()
        assert gen._example_value(_info(enum=["market", "limit"])) == "'market'"


class TestExtractReturnInfo:
    """Tests for _extract_return_info."""

    def test_no_returns_section(self):
        gen = ToolFunctionGenerator()
        rtype, rdesc = gen._extract_return_info("Just a description")
        assert rtype == "Any"
        assert rdesc == "Tool execution result"

    def test_empty_description(self):
        gen = ToolFunctionGenerator()
        rtype, rdesc = gen._extract_return_info("")
        assert rtype == "Any"

    def test_none_description(self):
        gen = ToolFunctionGenerator()
        rtype, rdesc = gen._extract_return_info(None)
        assert rtype == "Any"

    def test_dict_return_type(self):
        gen = ToolFunctionGenerator()
        desc = "Does something.\n\nReturns:\n    dict: A mapping of values"
        rtype, rdesc = gen._extract_return_info(desc)
        assert rtype == "dict"

    def test_list_of_dict_return_type(self):
        gen = ToolFunctionGenerator()
        desc = "Fetches data.\n\nReturns:\n    list[dict] with results"
        rtype, rdesc = gen._extract_return_info(desc)
        assert rtype == "list[dict]"


class TestGenerateFunction:
    """Tests for _generate_function."""

    def test_function_name_sanitization(self):
        gen = ToolFunctionGenerator()
        tool = _make_tool(name="get-stock.data")
        code = gen._generate_function(tool, "server")
        assert "def get_stock_data(" in code

    def test_required_params_before_optional(self):
        gen = ToolFunctionGenerator()
        tool = _make_tool(
            input_schema={
                "type": "object",
                "properties": {
                    "optional_param": {"type": "string", "description": "opt"},
                    "required_param": {"type": "integer", "description": "req"},
                },
                "required": ["required_param"],
            }
        )
        code = gen._generate_function(tool, "server")
        # required_param (int) should appear before optional_param
        req_pos = code.find("required_param: int")
        opt_pos = code.find("optional_param: str | None = None")
        assert req_pos < opt_pos


class TestGenerateToolModule:
    """Tests for generate_tool_module."""

    def test_module_contains_header_and_functions(self):
        gen = ToolFunctionGenerator()
        tools = [
            _make_tool(name="tool-a", description="Tool A"),
            _make_tool(name="tool-b", description="Tool B"),
        ]
        code = gen.generate_tool_module("my_server", tools)
        assert "my_server" in code
        assert "def tool_a(" in code
        assert "def tool_b(" in code
        assert "from typing import Any" in code


class TestGenerateToolDocumentation:
    """Tests for generate_tool_documentation."""

    def test_documentation_contains_sections(self):
        gen = ToolFunctionGenerator()
        tool = _make_tool(name="fetch-prices", description="Fetches prices.", server_name="market")
        doc = gen.generate_tool_documentation(tool)
        assert "# fetch_prices(" in doc
        assert "## Parameters" in doc
        assert "## Returns" in doc
        assert "## Example" in doc
        assert "from tools.market import fetch_prices" in doc


class TestGetParametersSchemaResolution:
    """get_parameters resolves pydantic-style schemas to flat param facts."""

    def _params(self, properties, required=()):
        tool = _make_tool(
            input_schema={
                "type": "object",
                "properties": properties,
                "required": list(required),
            }
        )
        return tool.get_parameters()

    def test_anyof_null_resolves_to_nullable_base_type(self):
        params = self._params(
            {"note": {"anyOf": [{"type": "string"}, {"type": "null"}]}}
        )
        assert params["note"]["type"] == "string"
        assert params["note"]["nullable"] is True

    def test_type_list_with_null_resolves_to_nullable_base_type(self):
        params = self._params({"limit": {"type": ["integer", "null"]}})
        assert params["limit"]["type"] == "integer"
        assert params["limit"]["nullable"] is True

    def test_enum_items_and_default_surface(self):
        params = self._params(
            {
                "side": {"type": "string", "enum": ["buy", "sell"]},
                "tags": {"type": "array", "items": {"type": "string"}},
                "interval": {"type": "string", "default": "1d"},
            }
        )
        assert params["side"]["enum"] == ["buy", "sell"]
        assert params["tags"]["items_type"] == "string"
        assert params["interval"]["has_default"] is True
        assert params["interval"]["default"] == "1d"

    def test_non_dict_param_info_degrades_to_any(self):
        params = self._params({"weird": "not-a-dict"})
        assert params["weird"]["type"] == "any"

    def test_a_malformed_properties_container_degrades_to_no_params(self):
        # A discovery cache written before the schema sanitizer can hold
        # ``"properties": []``; raising here wedges the whole workspace sync.
        for properties in ([], "nope", None, 7):
            tool = MCPToolInfo(
                "t", "d", {"type": "object", "properties": properties}, "srv"
            )
            assert tool.get_parameters() == {}

    def test_a_non_dict_input_schema_degrades_to_no_params(self):
        for schema in ([], "nope", None):
            assert MCPToolInfo("t", "d", schema, "srv").get_parameters() == {}

    def test_a_non_list_required_marks_nothing_required(self):
        # A bare ``"required": "symbol"`` would otherwise make every param whose
        # name is a substring of it required.
        tool = MCPToolInfo(
            "t",
            "d",
            {"properties": {"sym": {"type": "string"}}, "required": "symbol"},
            "srv",
        )
        assert tool.get_parameters()["sym"]["required"] is False


class TestWireKeyRoundTrip:
    """Sanitized Python names must never leak onto the wire.

    Regression for the Robinhood `type_` bug: the wrapper renamed the MCP
    param `type` → `type_` for the signature but then emitted `type_` as the
    argument key, so the server rejected every call with "unexpected
    additional properties".
    """

    def _keyword_tool(self):
        return _make_tool(
            name="place-order",
            input_schema={
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["market", "limit"]},
                    "class": {"type": "string"},
                    "symbol": {"type": "string"},
                },
                "required": ["type", "class", "symbol"],
            },
            server_name="user_srv",
        )

    def test_untrusted_arg_dict_emits_wire_keys(self):
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module(
            "user_srv", [self._keyword_tool()], untrusted=True
        )
        ast.parse(module)
        # The signature shows the sanitized identifiers the agent types…
        assert "type_: Literal['market', 'limit']" in module
        assert "class_: str" in module
        # …but the wire payload keeps the schema keys the server accepts.
        assert "'type': type_," in module
        assert "'class': class_," in module
        assert "'type_'" not in module
        assert "'class_'" not in module

    def test_trusted_builtin_keeps_raw_names_verbatim(self):
        gen = ToolFunctionGenerator()
        tool = _make_tool(
            input_schema={
                "type": "object",
                "properties": {"symbol": {"type": "string"}},
                "required": ["symbol"],
            }
        )
        module = gen.generate_tool_module("yf_price", [tool])
        assert '"symbol": symbol,' in module

    def _collision_tool(self):
        # `type` sanitizes to `type_`, colliding with a literal `type_` key.
        return _make_tool(
            name="probe",
            input_schema={
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "type_": {"type": "string"},
                },
                "required": ["type", "type_"],
            },
            server_name="user_srv",
        )

    def test_collision_after_sanitize_dedupes_instead_of_dropping(self):
        # Dropping the loser shipped a wrapper that could never send a required
        # field. Renaming it costs nothing: the wire key is what the server sees.
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module(
            "user_srv", [self._collision_tool()], untrusted=True
        )
        ast.parse(module)
        assert "def probe(type_: str, type__: str)" in module
        assert "'type': type_," in module
        assert "'type_': type__," in module

    def test_deduped_wrapper_sends_both_wire_keys(self):
        # The payload is the contract — pin the dict the wrapper actually builds.
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module(
            "user_srv", [self._collision_tool()], untrusted=True
        )
        # No parent package, so the module's relative mcp_client import falls
        # back to its stub — which the capture below replaces.
        ns: dict = {"__name__": "gen_tools"}
        exec(compile(module, "gen_tools", "exec"), ns)  # noqa: S102 - generated code
        sent = {}
        ns["_call_mcp_tool"] = lambda srv, tool, args: sent.update(args)
        ns["probe"]("market", "equity")
        assert sent == {"type": "market", "type_": "equity"}


class TestNoneSemantics:
    """None parts ways by requiredness: an optional param's None means "not
    provided" (key dropped), while a required param is always sent — its None
    is an explicit JSON null, and omitting the key would fail the server's
    required-field check."""

    def _sent(self, **call_kwargs) -> dict:
        tool = _make_tool(
            name="probe",
            input_schema={
                "type": "object",
                "properties": {
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer"},
                },
                "required": ["cursor"],
            },
            server_name="user_srv",
        )
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module("user_srv", [tool], untrusted=True)
        ns: dict = {"__name__": "gen_tools"}
        exec(compile(module, "gen_tools", "exec"), ns)  # noqa: S102 - generated code
        calls: list[dict] = []
        ns["_call_mcp_tool"] = lambda srv, tool_name, args: calls.append(args)
        ns["probe"](**call_kwargs)
        assert len(calls) == 1
        return calls[0]

    def test_required_nullable_none_is_sent_as_an_explicit_null(self):
        assert self._sent(cursor=None) == {"cursor": None}

    def test_optional_none_drops_the_wire_key(self):
        assert self._sent(cursor="abc") == {"cursor": "abc"}

    def test_provided_optional_rides_along(self):
        assert self._sent(cursor=None, limit=5) == {"cursor": None, "limit": 5}


class TestUnusableParamNames:
    """A param whose name cannot become an identifier decides the tool's fate.

    Required: the wrapper could never send that field, so shipping it hands the
    agent a permanently uncallable function — the whole tool is dropped.
    Optional: the rest of the tool still works, so only the param is skipped.
    """

    def _tool(self, *, required: list[str]):
        return _make_tool(
            name="probe",
            input_schema={
                "type": "object",
                "properties": {
                    "!!!": {"type": "string"},
                    "symbol": {"type": "string"},
                },
                "required": required,
            },
            server_name="user_srv",
        )

    def test_required_unusable_name_drops_the_whole_tool(self):
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module(
            "user_srv", [self._tool(required=["!!!", "symbol"])], untrusted=True
        )
        ast.parse(module)
        assert "def probe(" not in module

    def test_optional_unusable_name_keeps_the_tool(self):
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module(
            "user_srv", [self._tool(required=["symbol"])], untrusted=True
        )
        ast.parse(module)
        assert "def probe(symbol: str)" in module
        assert "'symbol': symbol," in module
        assert "!!!" not in module

    def test_dropped_tool_is_not_documented_as_callable(self):
        # Docs and the wrapper module must agree, or the agent calls a name
        # that does not exist.
        gen = ToolFunctionGenerator()
        doc = gen.generate_tool_documentation(
            self._tool(required=["!!!"]), untrusted=True
        )
        assert doc.startswith("# probe (unavailable)")


class TestDocsWrapperParity:
    """The documented signature is the exact wrapper signature."""

    def _signature_params(self, text: str, marker: str) -> str:
        line = next(ln for ln in text.splitlines() if ln.startswith(marker))
        return line[line.index("(") + 1 : line.rindex(")")]

    def test_doc_signature_matches_wrapper_signature(self):
        gen = ToolFunctionGenerator()
        tool = _make_tool(
            name="place-order",
            input_schema={
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["market", "limit"]},
                    "note": {
                        "anyOf": [{"type": "string"}, {"type": "null"}],
                        "default": "hi",
                    },
                },
                "required": ["type"],
            },
            server_name="user_srv",
        )
        module = gen.generate_tool_module("user_srv", [tool], untrusted=True)
        doc = gen.generate_tool_documentation(tool, untrusted=True)
        wrapper = self._signature_params(module, "def place_order(")
        documented = self._signature_params(doc, "# place_order(")
        assert wrapper == documented
        # And the shared signature carries the schema-true default repr.
        assert "note: str | None = 'hi'" in wrapper

    def test_doc_example_uses_schema_true_value(self):
        gen = ToolFunctionGenerator()
        tool = _make_tool(
            name="probe",
            input_schema={
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": ["fast", "slow"]},
                },
                "required": ["mode"],
            },
            server_name="user_srv",
        )
        doc = gen.generate_tool_documentation(tool, untrusted=True)
        assert "result = probe(mode='fast')" in doc


class TestDocstringSchemaFacts:
    """Enum/default facts surface beside the param description."""

    def test_allowed_and_default_suffixes(self):
        gen = ToolFunctionGenerator()
        tool = _make_tool(
            name="probe",
            input_schema={
                "type": "object",
                "properties": {
                    "side": {
                        "type": "string",
                        "description": "Order side",
                        "enum": ["buy", "sell"],
                    },
                    "interval": {"type": "string", "default": "1d"},
                },
                "required": ["side"],
            },
        )
        module = gen.generate_tool_module("srv", [tool])
        assert "[allowed: 'buy', 'sell']" in module
        assert "[default: '1d']" in module

    def test_enum_doc_cap_truncates_with_ellipsis(self):
        gen = ToolFunctionGenerator()
        values = [f"v{i:02d}" for i in range(15)]  # _MAX_DOC_ENUM_VALUES is 12
        tool = _make_tool(
            name="probe",
            input_schema={
                "type": "object",
                "properties": {"code": {"type": "string", "enum": values}},
                "required": ["code"],
            },
        )
        module = gen.generate_tool_module("srv", [tool])
        assert "'v11', ...]" in module
        assert "'v12'" not in module
