"""Validation + masking unit tests for the MCP server Pydantic models.

Covers the API security boundary: name regex, transport coherence, command
allowlist (no bash), URL policy (incl. metadata IP / userinfo / private ranges),
vault-ref vs bare host-env values, length caps, forbidden keys, and the
env/header masking that keeps literal secret values out of all responses.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.server.models.mcp_server import (
    ALLOWED_COMMANDS,
    McpServerInput,
    catalog_row_to_response,
    coerce_mcp_name,
    collect_vault_refs,
    normalize_transport,
    parse_mcp_servers_payload,
    validate_remote_url,
)


def _stdio(**overrides) -> dict:
    base = {"name": "neutral_server", "transport": "stdio", "command": "npx"}
    base.update(overrides)
    return base


def _http(**overrides) -> dict:
    base = {
        "name": "remote_server",
        "transport": "http",
        "url": "https://api.example.com/mcp",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Name regex
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["ok", "_ok", "Ok_1", "a" * 64])
def test_name_accepts_valid(name):
    assert McpServerInput(**_stdio(name=name)).name == name


@pytest.mark.parametrize(
    "name", ["1bad", "has-dash", "has.dot", "", "a" * 65, "has space"]
)
def test_name_rejects_invalid(name):
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(name=name))


# ---------------------------------------------------------------------------
# Transport coherence
# ---------------------------------------------------------------------------


def test_stdio_requires_command():
    with pytest.raises(ValidationError):
        McpServerInput(name="x", transport="stdio")


def test_stdio_forbids_url_and_headers():
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(url="https://x.example.com/m"))
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(headers={"Authorization": "literal"}))


def test_http_requires_url():
    with pytest.raises(ValidationError):
        McpServerInput(name="x", transport="http")


def test_http_forbids_command_args_env():
    with pytest.raises(ValidationError):
        McpServerInput(**_http(command="npx"))
    with pytest.raises(ValidationError):
        McpServerInput(**_http(args=["-y"]))
    with pytest.raises(ValidationError):
        McpServerInput(**_http(env={"MODE": "prod"}))


def test_sse_requires_url():
    with pytest.raises(ValidationError):
        McpServerInput(name="x", transport="sse")
    srv = McpServerInput(name="x", transport="sse", url="https://api.example.com/sse")
    assert srv.transport == "sse"


# ---------------------------------------------------------------------------
# Command allowlist — no bash / no shells
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cmd", sorted(ALLOWED_COMMANDS))
def test_command_allowlist_accepts(cmd):
    assert McpServerInput(**_stdio(command=cmd)).command == cmd


@pytest.mark.parametrize("cmd", ["bash", "sh", "zsh", "/bin/bash", "curl", "rm"])
def test_command_allowlist_rejects(cmd):
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(command=cmd))


# ---------------------------------------------------------------------------
# URL policy
# ---------------------------------------------------------------------------


def test_url_accepts_public_https():
    assert validate_remote_url("https://api.example.com/mcp")


@pytest.mark.parametrize(
    "url",
    [
        "http://api.example.com/mcp",  # not https
        "https://user:pw@api.example.com/mcp",  # userinfo
        "https://10.0.0.5/mcp",  # private 10/8
        "https://172.16.0.1/mcp",  # private 172.16/12
        "https://192.168.1.1/mcp",  # private 192.168/16
        "https://127.0.0.1/mcp",  # loopback
        "https://169.254.169.254/latest/meta-data",  # link-local metadata
        "https://100.64.0.1/mcp",  # CGNAT 100.64/10 (not is_global)
        "https://2130706433/mcp",  # decimal-int 127.0.0.1 (inet_aton form)
        "https://0x7f000001/mcp",  # hex 127.0.0.1
        "https://0177.0.0.1/mcp",  # octal-octet 127.0.0.1
        "https://127.1/mcp",  # short-dotted 127.0.0.1
        "https://2852039166/mcp",  # decimal-int 169.254.169.254 metadata
        "https://[::ffff:169.254.169.254]/mcp",  # ipv4-mapped metadata
        "https://[::1]/mcp",  # ipv6 loopback
        "https://localhost/mcp",  # localhost
        "https://svc.local/mcp",  # *.local
        "https://svc.internal/mcp",  # *.internal
        "https://svc.localhost/mcp",  # *.localhost
        "https://api.example.com/${vault:TOK}",  # secret in url
        "https://api.example.com/${VAR}/mcp",  # brace env placeholder
        "https://api.example.com/${vault:TOK",  # unclosed brace form
    ],
)
def test_url_policy_rejects(url):
    with pytest.raises(ValueError):
        validate_remote_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://api.example.com/$batch",  # OData batch endpoint
        "https://api.example.com/odata?$filter=status%20eq%20'active'",
        "https://api.example.com/odata?$select=name&$top=100",
    ],
)
def test_url_policy_accepts_bare_dollar_path_segments(url):
    """Bare ``$word`` is a legitimate URL convention (OData) and is inert
    downstream — only brace placeholder forms are rejected."""
    validate_remote_url(url)


def test_metadata_ip_rejected_via_model():
    with pytest.raises(ValidationError):
        McpServerInput(**_http(url="https://169.254.169.254/latest/meta-data"))


# ---------------------------------------------------------------------------
# env / header values
# ---------------------------------------------------------------------------


def test_env_accepts_vault_ref_and_literal():
    srv = McpServerInput(**_stdio(env={"TOK": "${vault:MY_TOKEN}", "MODE": "prod"}))
    assert srv.env["TOK"] == "${vault:MY_TOKEN}"


@pytest.mark.parametrize(
    "value",
    ["${INTERNAL_SERVICE_TOKEN}", "$HOME", "prefix-${VAR}", "${vault:bad name}"],
)
def test_env_rejects_bare_host_env_values(value):
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(env={"TOK": value}))


def test_header_accepts_vault_ref():
    srv = McpServerInput(**_http(headers={"Authorization": "${vault:API_KEY}"}))
    assert srv.headers["Authorization"] == "${vault:API_KEY}"


def test_header_rejects_bare_host_env_value():
    with pytest.raises(ValidationError):
        McpServerInput(**_http(headers={"Authorization": "${SECRET}"}))


@pytest.mark.parametrize("key", ["1bad", "has space", "a" * 129])
def test_env_key_rejected(key):
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(env={key: "literal"}))


@pytest.mark.parametrize(
    "arg",
    ["--flag=${vault:TOKEN}", "${vault:TOKEN}", "--verbose", "package-name", "$100"],
)
def test_args_accept_vault_refs_and_literals(arg):
    srv = McpServerInput(**_stdio(args=[arg]))
    assert srv.args == [arg]


@pytest.mark.parametrize(
    "arg",
    ["${HOME}", "$HOME", "--dir=${HOME}/x", "--x=${vault:bad name}", "--x=${vault:"],
)
def test_args_reject_host_env_and_malformed_vault(arg):
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(args=[arg]))


# ---------------------------------------------------------------------------
# Length caps
# ---------------------------------------------------------------------------


def test_description_cap():
    McpServerInput(**_stdio(description="x" * 512))
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(description="x" * 513))


def test_instruction_cap():
    McpServerInput(**_stdio(instruction="x" * 1024))
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(instruction="x" * 1025))


def test_tool_exposure_mode_enum():
    assert McpServerInput(**_stdio(tool_exposure_mode="detailed")).tool_exposure_mode == "detailed"
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(tool_exposure_mode="verbose"))


def test_discovery_uses_secrets_defaults_off_and_round_trips():
    # Default is the safe secret-less posture.
    assert McpServerInput(**_stdio()).discovery_uses_secrets is False
    assert McpServerInput(**_stdio()).to_config_blob()["discovery_uses_secrets"] is False
    # Explicit opt-in round-trips into the persisted config blob.
    srv = McpServerInput(**_stdio(discovery_uses_secrets=True))
    assert srv.discovery_uses_secrets is True
    assert srv.to_config_blob()["discovery_uses_secrets"] is True


def test_catalog_row_discovery_uses_secrets_in_response():
    row = {
        "name": "remote_server",
        "transport": "http",
        "command": None,
        "args": [],
        "url": "https://api.example.com/mcp",
        "env": {},
        "headers": {},
        "description": "",
        "instruction": "",
        "tool_exposure_mode": "summary",
        "discovery_uses_secrets": True,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    assert catalog_row_to_response(row).discovery_uses_secrets is True
    # A row missing the field defaults to off.
    del row["discovery_uses_secrets"]
    assert catalog_row_to_response(row).discovery_uses_secrets is False


# ---------------------------------------------------------------------------
# Forbidden keys — reject, don't strip
# ---------------------------------------------------------------------------


def test_vault_blueprints_rejected():
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(vault_blueprints=[]))


def test_source_key_rejected():
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(source="workspace"))


def test_unknown_extra_key_rejected():
    with pytest.raises(ValidationError):
        McpServerInput(**_stdio(unexpected="x"))


# ---------------------------------------------------------------------------
# Masking — vault refs surfaced, literal values never echoed
# ---------------------------------------------------------------------------


def test_collect_vault_refs_dedupes_and_sorts():
    refs = collect_vault_refs({"A": "${vault:Z}", "B": "${vault:A}", "C": "literal"})
    assert refs == ["A", "Z"]


def test_catalog_row_response_masks_literals():
    row = {
        "name": "remote_server",
        "transport": "http",
        "command": None,
        "args": [],
        "url": "https://api.example.com/mcp",
        "env": {},
        "headers": {"Authorization": "${vault:API_KEY}", "X-Trace": "literal-value"},
        "description": "d",
        "instruction": "i",
        "tool_exposure_mode": "summary",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    resp = catalog_row_to_response(row)
    dumped = resp.model_dump_json()
    assert "literal-value" not in dumped
    assert resp.header_refs == ["API_KEY"]


# ---------------------------------------------------------------------------
# Standard `mcpServers` JSON parser
# ---------------------------------------------------------------------------


def test_coerce_mcp_name_underscores_illegal_chars():
    name, renamed = coerce_mcp_name("my-stock-mcp.v2")
    assert name == "my_stock_mcp_v2" and renamed is True


def test_coerce_mcp_name_prefixes_leading_digit():
    name, renamed = coerce_mcp_name("3rd-party")
    assert name == "_3rd_party" and renamed is True


def test_coerce_mcp_name_passthrough_when_already_legal():
    name, renamed = coerce_mcp_name("already_ok")
    assert name == "already_ok" and renamed is False


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("streamablehttp", "http"),
        ("streamable-http", "http"),
        ("streamable_http", "http"),
        ("streamableHttp", "http"),
        ("http", "http"),
        ("sse", "sse"),
        ("stdio", "stdio"),
    ],
)
def test_normalize_transport_aliases(raw, expected):
    assert normalize_transport(raw, has_command=False, has_url=True) == expected


def test_normalize_transport_infers_from_fields():
    assert normalize_transport(None, has_command=True, has_url=False) == "stdio"
    assert normalize_transport(None, has_command=False, has_url=True) == "http"
    assert normalize_transport(None, has_command=False, has_url=False) is None
    assert normalize_transport("nonsense", has_command=False, has_url=True) is None


def test_parse_unwraps_mcp_servers_and_maps_remote():
    blob = {
        "mcpServers": {
            "my-stock-mcp": {
                "type": "streamablehttp",
                "url": "https://api.example.com/ds/stock",
                "headers": {"Authorization": "EXAMPLE-OPAQUE-TOKEN"},
            }
        }
    }
    [entry] = parse_mcp_servers_payload(blob)
    assert entry.error is None
    assert entry.original_name == "my-stock-mcp"
    assert entry.name == "my_stock_mcp" and entry.renamed is True
    assert entry.config["transport"] == "http"
    assert entry.config["url"] == "https://api.example.com/ds/stock"
    # Literal secret stays inline for the endpoint to extract.
    assert entry.config["headers"] == {"Authorization": "EXAMPLE-OPAQUE-TOKEN"}
    # The original config feeds a valid McpServerInput once the literal is vaulted.
    server = McpServerInput(
        **{**entry.config, "headers": {"Authorization": "${vault:TOK}"}}
    )
    assert server.transport == "http"


def test_parse_infers_stdio_from_command_and_drops_unknown_keys():
    blob = {"mcpServers": {"local_time": {"command": "uvx", "args": ["pkg"], "disabled": True}}}
    [entry] = parse_mcp_servers_payload(blob)
    assert entry.config["transport"] == "stdio"
    assert entry.config["command"] == "uvx"
    assert "disabled" not in entry.config  # unknown keys dropped on purpose


def test_parse_bare_map_without_wrapper():
    blob = {"srv_a": {"command": "npx"}, "srv_b": {"url": "https://api.example.com/m"}}
    entries = {e.name: e for e in parse_mcp_servers_payload(blob)}
    assert entries["srv_a"].config["transport"] == "stdio"
    assert entries["srv_b"].config["transport"] == "http"


def test_parse_single_self_naming_object():
    blob = {"name": "solo", "command": "node", "args": ["x"]}
    [entry] = parse_mcp_servers_payload(blob)
    assert entry.name == "solo" and entry.config["transport"] == "stdio"


def test_parse_undetermined_transport_marks_error():
    blob = {"mcpServers": {"weird": {"foo": "bar"}}}
    [entry] = parse_mcp_servers_payload(blob)
    assert entry.error is not None and not entry.config


def test_parse_non_dict_payload_is_empty():
    assert parse_mcp_servers_payload("not a dict") == []
    assert parse_mcp_servers_payload(None) == []
