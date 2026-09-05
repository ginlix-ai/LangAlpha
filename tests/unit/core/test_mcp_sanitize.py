"""Tests for ptc_agent.core.mcp_sanitize.

Covers the vault-reference regex, identifier sanitization + collision
detection, and untrusted-text neutralization for user MCP servers.
"""

import ast

import pytest

from ptc_agent.core.mcp_sanitize import (
    VAULT_REF_RE,
    discovery_should_use_secrets,
    is_untrusted_server,
    iter_arg_credentials,
    iter_arg_flag_pairs,
    sanitize_tool_name,
    sanitize_tool_set,
    sanitize_tool_text,
    unsalvageable_required_params,
    vault_refs,
)

from ptc_agent.config.core import MCPServerConfig


class _Tool:
    """Minimal stand-in for MCPToolInfo (only ``.name`` is read)."""

    def __init__(self, name: str) -> None:
        self.name = name


class TestIsUserServer:
    """The trust-boundary predicate: everything user-configured is untrusted."""

    def test_workspace_source_is_untrusted(self):
        srv = MCPServerConfig(name="s", transport="stdio", command="npx", source="workspace")
        assert is_untrusted_server(srv) is True

    def test_user_source_is_untrusted(self):
        # Inherited (Connectors) servers must sanitize exactly like
        # workspace-local ones — user-supplied either way.
        srv = MCPServerConfig(name="s", transport="stdio", command="npx", source="user")
        assert is_untrusted_server(srv) is True

    def test_builtin_source_is_trusted(self):
        srv = MCPServerConfig(name="s", transport="stdio", command="npx", source="builtin")
        assert is_untrusted_server(srv) is False

    def test_missing_source_attr_defaults_to_trusted(self):
        class Bare:
            pass

        assert is_untrusted_server(Bare()) is False


class TestDiscoveryShouldUseSecrets:
    """Effective discovery-secret gating (auth'd remote servers self-enable)."""

    def test_explicit_flag_wins(self):
        srv = MCPServerConfig(
            name="s", transport="stdio", command="npx", source="workspace",
            discovery_uses_secrets=True,
        )
        assert discovery_should_use_secrets(srv) is True

    def test_remote_vault_header_auto_enables(self):
        srv = MCPServerConfig(
            name="s", transport="http", url="https://api.example.com/m",
            headers={"Authorization": "${vault:K}"}, source="workspace",
        )
        assert discovery_should_use_secrets(srv) is True

    def test_remote_without_vault_header_stays_off(self):
        srv = MCPServerConfig(
            name="s", transport="http", url="https://api.example.com/m",
            headers={"X-Trace": "literal"}, source="workspace",
        )
        assert discovery_should_use_secrets(srv) is False

    def test_stdio_with_vault_env_does_not_auto_enable(self):
        # Stdio runs untrusted code — the flag must stay opt-in there.
        srv = MCPServerConfig(
            name="s", transport="stdio", command="npx",
            env={"TOK": "${vault:K}"}, source="workspace",
        )
        assert discovery_should_use_secrets(srv) is False

    def test_builtin_remote_never_auto_enables(self):
        srv = MCPServerConfig(
            name="s", transport="http", url="https://api.example.com/m",
            headers={"Authorization": "${vault:K}"}, source="builtin",
        )
        assert discovery_should_use_secrets(srv) is False


class TestVaultRefRegex:
    """Tests for VAULT_REF_RE / vault_refs."""

    def test_matches_vault_form_only(self):
        assert vault_refs("${vault:ALPHA} and ${vault:BETA}") == ["ALPHA", "BETA"]

    def test_bare_var_is_not_a_vault_ref(self):
        # A plain ${VAR} must NOT be a vault reference — this is what stops a
        # user from naming a platform env var and having it resolve.
        assert vault_refs("${PLATFORM_TOKEN}") == []
        assert VAULT_REF_RE.findall("${PLATFORM_TOKEN}") == []

    def test_empty_and_none(self):
        assert vault_refs("") == []
        assert vault_refs(None) == []

    def test_rejects_illegal_secret_name_chars(self):
        assert vault_refs("${vault:bad-name}") == []
        assert vault_refs("${vault:ok_name}") == ["ok_name"]


class TestIterArgCredentials:
    """Key-signal only: a flag must NAME the value a credential to collect it."""

    def test_collects_both_flag_forms(self):
        args = ["--token=tok_equals_form", "--api-key", "tok_two_token_form"]
        assert iter_arg_credentials(args) == [
            ("token", "tok_equals_form"),
            ("api-key", "tok_two_token_form"),
        ]

    def test_paths_and_urls_are_never_collected(self):
        """The false-positive class the key-signal restriction exists for:
        long opaque-looking positionals (paths, URLs) must stay servable."""
        args = [
            "/workspace/output/analysis_results_2026.csv",
            "https://api.example.com/v1/some/endpoint",
            "--config=/etc/app/settings_production.yaml",
        ]
        assert iter_arg_credentials(args) == []

    def test_pairs_report_the_index_of_the_value(self):
        """What the rewriting lanes need and the redaction lanes don't: vault
        extraction replaces the element and the export scrub empties it, so
        both need the position, not just the pair."""
        args = ["-y", "srv", "--token", "tok_two_token_form", "--port", "8080"]
        assert iter_arg_flag_pairs(args) == [(3, "token", "tok_two_token_form")]

    def test_an_existing_vault_ref_is_not_a_pair_to_collect(self):
        """Already-vaulted is already handled: re-collecting it would have the
        extractor allocate a second secret holding the literal ``${vault:X}``."""
        assert iter_arg_flag_pairs(["--token", "${vault:SVC_KEY}"]) == []

    def test_vault_refs_are_skipped(self):
        args = ["--token=${vault:MY_TOKEN}", "--secret", "${vault:OTHER}"]
        assert iter_arg_credentials(args) == []

    def test_a_flag_is_not_a_flags_value(self):
        # "--token -x" is a dangling flag followed by another flag, not a pair.
        assert iter_arg_credentials(["--token", "-x"]) == []

    def test_non_string_entries_break_the_pair(self):
        assert iter_arg_credentials(["--token", 42, "loose_value_123"]) == []

    def test_empty_and_none(self):
        assert iter_arg_credentials([]) == []
        assert iter_arg_credentials(None) == []


class TestSanitizeToolName:
    """Tests for sanitize_tool_name."""

    def test_dash_and_dot_collapse_to_underscore(self):
        assert sanitize_tool_name("foo-bar") == "foo_bar"
        assert sanitize_tool_name("foo.bar") == "foo_bar"

    def test_leading_digit_prefixed(self):
        assert sanitize_tool_name("2cool") == "_2cool"

    def test_keyword_suffixed(self):
        assert sanitize_tool_name("class") == "class_"
        assert sanitize_tool_name("for") == "for_"

    def test_unsalvageable_returns_none(self):
        assert sanitize_tool_name("") is None
        assert sanitize_tool_name("!!!") is None
        assert sanitize_tool_name("---") is None

    def test_result_is_valid_identifier(self):
        for raw in ("a/b", "weird name", "tool@1"):
            out = sanitize_tool_name(raw)
            assert out is not None
            assert out.isidentifier()


class TestSanitizeToolSet:
    """Tests for sanitize_tool_set collision detection."""

    def test_collision_first_wins_and_records_reason(self):
        result = sanitize_tool_set([_Tool("foo-bar"), _Tool("foo.bar")])
        assert [t.name for t in result.kept] == ["foo-bar"]
        assert len(result.skipped) == 1
        skipped_name, reason = result.skipped[0]
        assert skipped_name == "foo.bar"
        assert "collides" in reason

    def test_illegal_name_skipped_with_reason(self):
        result = sanitize_tool_set([_Tool("ok"), _Tool("!!!")])
        assert [t.name for t in result.kept] == ["ok"]
        assert result.skipped[0][0] == "!!!"
        assert "identifier" in result.skipped[0][1]


class TestUnsalvageableRequiredParams:
    """Only params that codegen can never emit count — the tool is uncallable
    without them, so discovery drops it."""

    def test_required_illegal_name_reported(self):
        schema = {"properties": {"名前": {}}, "required": ["名前"]}
        assert unsalvageable_required_params(schema) == ["名前"]

    def test_optional_illegal_name_ignored(self):
        schema = {"properties": {"ok": {}, "名前": {}}, "required": ["ok"]}
        assert unsalvageable_required_params(schema) == []

    def test_collisions_are_salvageable(self):
        # Both sanitize to 'foo_bar'; codegen de-duplicates rather than drops.
        schema = {
            "properties": {"foo-bar": {}, "foo.bar": {}},
            "required": ["foo-bar", "foo.bar"],
        }
        assert unsalvageable_required_params(schema) == []

    def test_keyword_and_dashed_names_are_salvageable(self):
        schema = {
            "properties": {"class": {}, "start-date": {}, "2nd": {}},
            "required": ["class", "start-date", "2nd"],
        }
        assert unsalvageable_required_params(schema) == []

    def test_required_name_absent_from_properties_ignored(self):
        # Codegen binds params from 'properties' only, so a required key with
        # no property was never going to be emitted either way.
        assert unsalvageable_required_params({"properties": {}, "required": ["!!!"]}) == []

    @pytest.mark.parametrize(
        "schema",
        [
            None,
            [],
            "oops",
            {},
            {"properties": {"名前": {}}},  # no required list
            {"properties": [], "required": ["名前"]},  # malformed container
            {"properties": {"名前": {}}, "required": "名前"},  # required not a list
        ],
    )
    def test_malformed_inputs_return_empty(self, schema):
        assert unsalvageable_required_params(schema) == []


class TestSanitizeToolText:
    """Tests for sanitize_tool_text."""

    def test_triple_quote_breakout_rendered_inert(self):
        evil = 'safe """ \nimport os; os.system("x") """ tail'
        cleaned = sanitize_tool_text(evil)
        # Embedding the cleaned text in a docstring must still compile — the
        # injected triple-quotes cannot terminate the docstring early.
        module = f'def f():\n    """{cleaned}"""\n    pass\n'
        ast.parse(module)

    def test_strips_control_chars(self):
        assert "\x00" not in sanitize_tool_text("a\x00b\x07c")
        # tab/newline survive
        assert sanitize_tool_text("a\tb\nc") == "a\tb\nc"

    def test_length_cap(self):
        capped = sanitize_tool_text("x" * 5000, max_len=100)
        assert capped.startswith("x" * 100)
        assert "truncated" in capped

    def test_empty_and_none(self):
        assert sanitize_tool_text("") == ""
        assert sanitize_tool_text(None) == ""

    def test_non_string_input_collapses_to_empty(self):
        # A hostile schema can put anything where a description belongs, and
        # get_parameters forwards it raw; the sanitizer is the totality
        # boundary — raising here would wedge wrapper generation for the
        # whole workspace on one bad connector.
        for value in (123, 1.5, True, ["x"], {"a": "b"}, b"bytes"):
            assert sanitize_tool_text(value) == ""

    @pytest.mark.parametrize(
        "text",
        [
            'desc \\" tail',  # pre-existing backslash-quote
            "desc \\' tail",
            'a\\"b"""c',  # backslash-quote AND a triple-quote run
            "trailing backslash \\",
            'plain "quoted" words',
            "lone \\ backslash",
        ],
    )
    def test_escaping_round_trips_content(self, text):
        """Sanitized text embedded in a docstring must EVALUATE back to the
        original content — the escapes are source-level only. The old escape
        order silently un-doubled pre-existing ``\\"`` sequences, mutating the
        evaluated text."""
        cleaned = sanitize_tool_text(text)
        module = f'def f():\n    """{cleaned}"""\n'
        tree = ast.parse(module)
        assert ast.get_docstring(tree.body[0], clean=False) == text
