"""Pydantic request/response models + validation for MCP server config.

The validators here are the API's security boundary for user-configured MCP
servers (plan §6 / Security). They reject hostile input early:

- name shape; transport↔field coherence
- command allowlist WITHOUT ``bash`` (running user commands = arbitrary code)
- URL policy: https-only, no userinfo, no private/loopback/link-local/metadata
  IPs or ``localhost``/``*.local``/``*.internal``/``*.localhost`` hosts, no
  ``${vault:...}`` smuggled into the URL (secrets belong in headers)
- env/header values are ``${vault:NAME}`` refs or literals — bare ``${VAR}``
  host-env-style values are rejected (they would never resolve)
- ``vault_blueprints`` / ``source`` keys are rejected (built-in-only fields)

Response models echo env/headers exactly as stored — ``${vault:NAME}`` refs or
owner-supplied literals, never resolved secrets — so the owner's edit form can
round-trip them; ``env_refs``/``header_refs`` carry just the vault names.
"""

from __future__ import annotations

import ipaddress
import re
import socket
from dataclasses import asdict, dataclass, field as dataclass_field
from typing import Any, Literal, Optional
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, ValidationError, model_validator

from ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from src.server.database.mcp_oauth import ConnectionStatus
from src.server.services.brokerages import Brokerage
from src.server.services.mcp_config import Origin


def _format_validation_error(exc: ValidationError) -> str:
    """Flatten a Pydantic ValidationError into a JSON-safe detail string."""
    parts = []
    for err in exc.errors(include_url=False):
        loc = ".".join(str(p) for p in err.get("loc", ())) or "body"
        parts.append(f"{loc}: {err.get('msg', 'invalid')}")
    return "; ".join(parts) or "validation error"

# ---------------------------------------------------------------------------
# Shared constants — single source of truth for validators (also mirrored
# in the frontend Zod schema; keep the two in sync).
# ---------------------------------------------------------------------------

NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")

# Allowed stdio commands — deliberately WITHOUT `bash` (and any shell). Running
# a user-chosen command is arbitrary code execution; this is the allowlist that
# bounds it (plan §Security #4).
# Commands that resolve dependencies from the shared sandbox environment rather
# than an isolated per-server venv. Allowed, but nudged: the platform image pins
# their runtime (including the mcp SDK), so an SDK-major bump can kill a server
# born outside it — the uvx/npx form is immune.
SHARED_ENV_COMMANDS = frozenset({"uv", "python", "python3", "node"})


def isolation_warnings(server: "McpServerInput") -> list[str]:
    """Non-blocking policy nudges for a validated server definition."""
    if server.transport == "stdio" and server.command in SHARED_ENV_COMMANDS:
        return [
            f"command {server.command!r} runs from the shared sandbox "
            "environment, whose dependency versions (including the mcp SDK) "
            "are pinned by the platform image and may change under it. For "
            "third-party servers prefer an isolated launch: uvx --from "
            "'<package>==<version>' <entrypoint> (or npx <package>@<version>)."
        ]
    # A warning, not a rejection: imports normalize legacy configs and must
    # keep landing — but the sandbox client refuses 'sse' outright, so without
    # this the server saves looking healthy and every tool call fails.
    if server.transport == "sse":
        return [
            "transport 'sse' is the legacy remote MCP transport and the "
            "sandbox client cannot execute its tools; change the server's "
            "transport to 'http' (streamable HTTP)."
        ]
    return []

DESCRIPTION_MAX = 512
INSTRUCTION_MAX = 1024

# Reject keys the user must never set on an MCP server payload.
_FORBIDDEN_KEYS = ("vault_blueprints", "source")

# A bare host-env placeholder like ``${VAR}`` or ``$VAR`` — never resolves for
# workspace servers (only ``${vault:NAME}`` does), so fail fast at the API.
_BARE_ENV_RE = re.compile(r"\$\{?[A-Za-z_][A-Za-z0-9_]*\}?")


# ---------------------------------------------------------------------------
# Value-level validators (shared by env and headers)
# ---------------------------------------------------------------------------


def _validate_secret_map(
    mapping: dict[str, str], *, kind: str, key_re: re.Pattern[str]
) -> dict[str, str]:
    """Validate an env/header map: legal keys, and values that are either a
    full ``${vault:NAME}`` reference or a plain literal (no host-env refs)."""
    if not isinstance(mapping, dict):
        raise ValueError(f"{kind} must be an object of string→string")
    for key, value in mapping.items():
        if not isinstance(key, str) or not key_re.match(key):
            raise ValueError(
                f"{kind} name {key!r} is invalid: must match {key_re.pattern}"
            )
        if not isinstance(value, str):
            raise ValueError(f"{kind} value for {key!r} must be a string")
        _validate_secret_value(value, kind=kind, key=key)
    return mapping


def _validate_secret_value(value: str, *, kind: str, key: str) -> None:
    """A value may EMBED ``${vault:NAME}`` refs; what it may not carry is a
    malformed one or a host-env placeholder.

    Embedding matters because ``Authorization: Bearer ${vault:TOKEN}`` is the
    shape an auth header takes almost everywhere, and requiring the whole value
    to be the reference meant the scheme word had to be stored inside the
    secret. The sandbox has always substituted refs in place rather than
    replacing the field (``_resolve_vault_refs``), and ``_validate_args``
    already accepts the embedded form — this is the same rule, applied to the
    other two maps.
    """
    remainder = VAULT_REF_RE.sub("", value)
    # Whatever is left after the well-formed refs come out: a surviving
    # ``${vault:`` is a typo in one, and a ``${...}``/``$VAR`` token is a
    # host-env-style placeholder that will never resolve for these servers.
    if "${vault:" in remainder:
        raise ValueError(
            f"{kind} value for {key!r} contains a malformed vault reference; "
            "use the exact form ${vault:NAME}"
        )
    if _BARE_ENV_RE.search(remainder):
        raise ValueError(
            f"{kind} value for {key!r} looks like a host-env placeholder; "
            "use ${vault:NAME} for secrets or a plain literal value"
        )


def _validate_args(args: list[str]) -> None:
    """Args may EMBED ``${vault:NAME}`` refs (import writes ``--flag=${vault:NAME}``)
    but, like env/headers, must not carry host-env placeholders — they would
    reach the subprocess as unresolved literals."""
    for i, arg in enumerate(args):
        remainder = VAULT_REF_RE.sub("", arg)
        if "${vault:" in remainder:
            raise ValueError(
                f"args[{i}] contains a malformed vault reference; "
                "use the exact form ${vault:NAME}"
            )
        if _BARE_ENV_RE.search(remainder):
            raise ValueError(
                f"args[{i}] looks like a host-env placeholder; "
                "use ${vault:NAME} for secrets or a plain literal value"
            )


# ---------------------------------------------------------------------------
# URL policy
# ---------------------------------------------------------------------------


def validate_remote_url(url: str) -> str:
    """Enforce the SSRF-hardening URL policy for sse/http servers (plan §6)."""
    if not isinstance(url, str) or not url:
        raise ValueError("url is required for sse/http transports")
    # Brace forms only (`${vault:NAME}`, `${VAR}`, unclosed `${`): bare `$word`
    # is a legitimate URL convention (OData `/$batch`, `?$filter=`) and is inert
    # downstream — workspace URLs resolve `${vault:...}` refs exclusively, never
    # host env vars.
    if "${" in url:
        raise ValueError("url must not contain secrets or placeholders; put credentials in headers")

    parts = urlsplit(url)
    if parts.scheme != "https":
        raise ValueError("url must use https://")
    if parts.username or parts.password or "@" in (parts.netloc or ""):
        raise ValueError("url must not contain userinfo credentials")
    try:
        parts.port
    except ValueError:
        raise ValueError("url port must be a number between 1 and 65535")

    host = parts.hostname
    if not host:
        raise ValueError("url must include a host")
    host_l = host.lower().rstrip(".")

    # Hostname blocklist (loopback / internal naming conventions).
    if host_l == "localhost" or host_l.endswith(
        (".local", ".internal", ".localhost")
    ):
        raise ValueError(f"url host {host!r} is not allowed")

    # Literal IP blocklist: anything not globally routable. ``is_global`` covers
    # private/loopback/link-local/reserved/multicast/unspecified AND CGNAT
    # (100.64.0.0/10), which the explicit-category checks missed.
    candidate = host_l.strip("[]")
    try:
        ip = ipaddress.ip_address(candidate)
    except ValueError:
        # Non-canonical numeric IPv4 forms that the sandbox resolver
        # (getaddrinfo / curl) would still treat as an address — decimal-int
        # (``2130706433``), hex (``0x7f000001``), octal (``0177.0.0.1``), or
        # short-dotted (``127.1``), all == 127.0.0.1. ``inet_aton`` canonicalizes
        # exactly those forms; a real hostname raises OSError and falls through
        # (DNS-rebinding to a private IP is the documented, accepted residual).
        try:
            ip = ipaddress.ip_address(socket.inet_aton(candidate))
        except (OSError, ValueError, UnicodeError):
            ip = None
    if ip is not None and not ip.is_global:
        raise ValueError(f"url host {host!r} resolves to a disallowed IP range")
    return url


# ---------------------------------------------------------------------------
# Core server-definition payload (shared by catalog + workspace writes)
# ---------------------------------------------------------------------------


class McpServerInput(BaseModel):
    """A full user-supplied MCP server definition (request body)."""

    name: str
    transport: Literal["stdio", "sse", "http"] = "stdio"
    command: Optional[str] = None
    args: list[str] = Field(default_factory=list)
    url: Optional[str] = None
    env: dict[str, str] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    description: str = Field("", max_length=DESCRIPTION_MAX)
    instruction: str = Field("", max_length=INSTRUCTION_MAX)
    tool_exposure_mode: Literal["summary", "detailed"] = "summary"
    # Off (default) = tool discovery runs secret-less. On = resolve real vault
    # secrets during discovery (for servers that need auth even to list tools).
    discovery_uses_secrets: bool = False

    model_config = {"extra": "forbid"}

    @model_validator(mode="before")
    @classmethod
    def _reject_forbidden_keys(cls, data: Any) -> Any:
        """Explicitly 422 on built-in-only keys rather than silently dropping."""
        if isinstance(data, dict):
            for key in _FORBIDDEN_KEYS:
                if key in data:
                    raise ValueError(
                        f"{key!r} is not allowed on a user MCP server "
                        "(built-in servers only)"
                    )
        return data

    @model_validator(mode="after")
    def _validate_all(self) -> "McpServerInput":
        if not NAME_RE.match(self.name):
            raise ValueError(
                "name must be 1-64 chars: letter/underscore then "
                "letters/digits/underscores"
            )

        # Transport ↔ field coherence.
        if self.transport == "stdio":
            if not self.command:
                raise ValueError("stdio transport requires a command")
            if self.url:
                raise ValueError("stdio transport must not set url")
            if self.headers:
                raise ValueError("stdio transport must not set headers (env only)")
            # The command is not filtered. It is launched with an argv list and
            # no shell, in the same sandbox where the agent already runs
            # arbitrary commands on the user's behalf, so an allowlist here
            # bounds nothing it does not already bound — it only decides which
            # published MCP servers the user is able to install at all, and
            # the ones distributed as a `docker run` or a `deno` invocation are
            # not unusual.
            _validate_secret_map(self.env, kind="env", key_re=ENV_KEY_RE)
            _validate_args(self.args)
        else:  # sse / http
            if not self.url:
                raise ValueError(f"{self.transport} transport requires a url")
            if self.command:
                raise ValueError(f"{self.transport} transport must not set command")
            if self.args:
                raise ValueError(f"{self.transport} transport must not set args")
            if self.env:
                raise ValueError(
                    f"{self.transport} transport must not set env (headers only)"
                )
            validate_remote_url(self.url)
            _validate_secret_map(self.headers, kind="header", key_re=ENV_KEY_RE)
        return self

    def to_config_blob(self) -> dict[str, Any]:
        """Serialize to the JSON blob persisted in ``workspace_mcp_servers.config``
        / the catalog columns. Reference strings only — never resolved secrets."""
        return {
            "name": self.name,
            "transport": self.transport,
            "command": self.command,
            "args": list(self.args),
            "url": self.url,
            "env": dict(self.env),
            "headers": dict(self.headers),
            "description": self.description,
            "instruction": self.instruction,
            "tool_exposure_mode": self.tool_exposure_mode,
            "discovery_uses_secrets": self.discovery_uses_secrets,
        }

    def to_catalog_fields(self) -> dict[str, Any]:
        """Serialize to the ``user_mcp_servers`` column set (the catalog tier).

        Same content as ``to_config_blob`` minus ``name``, which the catalog
        addresses rows by rather than storing in a blob.
        """
        fields = self.to_config_blob()
        fields.pop("name")
        return fields


class EnabledInput(BaseModel):
    """PATCH body for the enabled toggle."""

    enabled: bool

    model_config = {"extra": "forbid"}


class PromoteInput(BaseModel):
    """POST body for promoting a workspace server into the user template catalog.

    ``overwrite`` replaces an existing template of the same name; without it a
    name clash is a 409 so the UI can confirm before clobbering. ``remove_source``
    turns the copy into a move: the workspace row is deleted after the catalog
    write, so it does not shadow the template it just created.
    """

    overwrite: bool = False
    remove_source: bool = False

    model_config = {"extra": "forbid"}


# ---------------------------------------------------------------------------
# Standard `mcpServers` JSON parser
# ---------------------------------------------------------------------------
#
# Users typically have an MCP server config in the de-facto-standard shape used
# by Claude Desktop / Cursor / etc.:
#
#   {"mcpServers": {"<name>": {"command"|"url", "type"|"transport", ...}}}
#
# These helpers normalize that blob into canonical :class:`McpServerInput`
# kwargs so it can be imported as-is: transport aliases are mapped, server keys
# are coerced into our ``NAME_RE`` shape, and only the fields we persist are
# carried through (unknown keys like ``disabled`` are dropped). The parser is
# pure — literal secret values stay inline; the import endpoint extracts them
# to the vault before validation.

# Transport aliases seen in standard configs. Compared after lowercasing and
# stripping non-letters, so ``streamable-http`` / ``streamable_http`` /
# ``streamableHttp`` all collapse to ``streamablehttp``.
_TRANSPORT_ALIASES = {
    "stdio": "stdio",
    "http": "http",
    "streamablehttp": "http",
    "streamable": "http",
    "sse": "sse",
}


@dataclass
class ParsedMcpServer:
    """One entry from a parsed ``mcpServers``-style blob.

    ``config`` holds canonical :class:`McpServerInput` kwargs with literal
    secret values STILL INLINE — the import endpoint extracts them to the vault
    before validation. ``error`` is set when the entry can't be normalized
    (uncoercible name, undetermined transport); such entries skip insert.
    """

    original_name: str
    name: str
    renamed: bool
    config: dict[str, Any] = dataclass_field(default_factory=dict)
    error: Optional[str] = None


def coerce_mcp_name(raw: Any) -> tuple[Optional[str], bool]:
    """Coerce an arbitrary server key into a legal MCP name (``NAME_RE``).

    Illegal characters become ``_`` and a leading digit is prefixed, so
    ``hexin-ifind-ds-stock-mcp`` → ``hexin_ifind_ds_stock_mcp``. Returns
    ``(name, renamed)``, or ``(None, False)`` when nothing salvageable remains.
    """
    if not isinstance(raw, str) or not raw:
        return None, False
    cand = re.sub(r"[^0-9A-Za-z_]", "_", raw)
    if cand and cand[0].isdigit():
        cand = f"_{cand}"
    cand = cand[:64]
    if not cand or not NAME_RE.match(cand):
        return None, False
    return cand, cand != raw


def normalize_transport(
    raw: Any, *, has_command: bool, has_url: bool
) -> Optional[str]:
    """Map a standard-config ``type``/``transport`` to our transport enum.

    Falls back to inference when the type is absent: a ``command`` ⇒ stdio, a
    ``url`` ⇒ http. Returns ``None`` when unrecognized and inference is
    ambiguous.
    """
    if isinstance(raw, str) and raw.strip():
        key = re.sub(r"[^a-z]", "", raw.lower())
        return _TRANSPORT_ALIASES.get(key)
    if has_command and not has_url:
        return "stdio"
    if has_url and not has_command:
        return "http"
    return None


def _normalize_server_entry(raw_name: Any, body: Any) -> ParsedMcpServer:
    raw_label = raw_name if isinstance(raw_name, str) else str(raw_name)
    name, renamed = coerce_mcp_name(raw_name)
    if name is None:
        return ParsedMcpServer(
            raw_label, raw_label, False,
            error="name could not be normalized to a valid identifier",
        )
    if not isinstance(body, dict):
        return ParsedMcpServer(
            raw_label, name, renamed,
            error="server definition must be a JSON object",
        )

    raw_type = body.get("type") or body.get("transport") or body.get("transportType")
    transport = normalize_transport(
        raw_type,
        has_command=bool(body.get("command")),
        has_url=bool(body.get("url")),
    )
    if transport is None:
        hint = f" (type={raw_type!r})" if raw_type else ""
        return ParsedMcpServer(
            raw_label, name, renamed,
            error=f"could not determine transport{hint}",
        )

    config: dict[str, Any] = {"name": name, "transport": transport}
    # Carry only the canonical fields for the resolved transport; the validator
    # rejects cross-transport fields, and unknown keys are dropped on purpose.
    if transport == "stdio":
        for key in ("command", "args", "env"):
            if body.get(key) is not None:
                config[key] = body[key]
    else:
        for key in ("url", "headers"):
            if body.get(key) is not None:
                config[key] = body[key]
    for key in ("description", "instruction", "tool_exposure_mode"):
        if body.get(key) is not None:
            config[key] = body[key]
    return ParsedMcpServer(raw_label, name, renamed, config)


def _unwrap_servers_map(payload: Any) -> dict[str, Any]:
    """Find the ``{name: def}`` map inside a parsed config blob."""
    if not isinstance(payload, dict):
        return {}
    for key in ("mcpServers", "mcp_servers", "servers"):
        inner = payload.get(key)
        if isinstance(inner, dict):
            return inner
    # A single, self-naming server object (``{"name": ..., "url"|"command": ...}``).
    if isinstance(payload.get("name"), str) and any(
        k in payload for k in ("command", "url", "type", "transport", "args", "headers", "env")
    ):
        return {payload["name"]: payload}
    # Otherwise assume the dict itself is the ``{name: def}`` map.
    return payload


def parse_mcp_servers_payload(payload: Any) -> list[ParsedMcpServer]:
    """Parse a standard ``mcpServers`` blob into normalized server entries.

    Accepts ``{"mcpServers": {name: def}}`` (the common shape), a bare
    ``{name: def}`` map, or a single self-naming server object. Never raises on
    a malformed entry — the bad entry carries an ``error`` and the rest parse.
    """
    return [
        _normalize_server_entry(k, v) for k, v in _unwrap_servers_map(payload).items()
    ]


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

# Status values surfaced on the effective list (plan "Effective-server response").
McpStatus = Literal[
    "connected", "error", "needs_secret", "disabled", "pending", "unknown"
]


class ToolSummary(BaseModel):
    """A single discovered tool (sanitized snapshot)."""

    name: str
    description: str = ""
    input_schema: dict[str, Any] = Field(default_factory=dict)


class EffectiveServer(BaseModel):
    """One row in the effective per-workspace MCP list.

    ``env``/``headers`` echo the stored reference maps for workspace-origin
    servers (``${vault:NAME}`` ref strings or owner-supplied literals — never
    resolved secrets) so the edit form can round-trip them; built-ins keep them
    empty. ``env_refs``/``header_refs`` carry just the vault names for display.
    """

    name: str
    origin: Origin
    transport: str
    enabled: bool
    editable: bool
    deletable: bool
    status: McpStatus
    error: str = ""
    tool_count: int = 0
    tools: list[ToolSummary] = Field(default_factory=list)
    missing_secrets: list[str] = Field(default_factory=list)
    env_refs: list[str] = Field(default_factory=list)
    header_refs: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    description: str = ""
    instruction: str = ""
    tool_exposure_mode: str = "summary"
    discovery_uses_secrets: bool = False
    command: Optional[str] = None
    args: list[str] = Field(default_factory=list)
    url: Optional[str] = None
    config_version: int = 0
    # True on a workspace-local row that shadows an inherited user server of
    # the same name (the local-fork affordance) — deleting the local row
    # reveals the inherited one again.
    shadows_inherited: bool = False
    # Inherited (origin='user') rows only: the owner's OAuth connection status
    # for this server, INCLUDING 'revoked' — so the UI can say "Disconnected,
    # reconnect in Plugins" instead of waiting on a discovery that can
    # never run. None = the server has no OAuth connection at all.
    oauth_status: Optional[ConnectionStatus] = None
    # DISABLED built-ins only: whether the disable is this workspace's marker
    # row or the account-wide user disable — the latter renders read-only here
    # ("disabled for your account", managed in Plugins).
    disabled_scope: Optional[Literal["workspace", "user"]] = None
    # Inherited rows installed by a plugin: the owning plugin's name, display
    # only. Deliberately never on MCPServerConfig — provenance must not enter
    # the config blob round-trip.
    plugin_name: Optional[str] = None


class EffectiveServerList(BaseModel):
    """GET /{id}/mcp/servers payload."""

    servers: list[EffectiveServer]
    sandbox_running: bool
    max_servers: int
    config_version: int
    # The version the running session has actually applied (loaded into the live
    # agent), or None when no warm session exists. The frontend derives the
    # version-accurate "synced" state from applied >= config_version.
    applied_config_version: Optional[int] = None
    # True while the sandbox is transitioning *up* toward running (a proactive
    # MCP apply, or workspace entry, just kicked a warm). Lets the UI keep
    # polling — and show "Starting workspace…" — through the stopped→running
    # gap instead of resting on a stale "stopped".
    sandbox_warming: bool = False


class CatalogServer(BaseModel):
    """A user-level server row, returned only to its owner.

    ``enabled`` rows are live: inherited into every one of the user's
    workspaces by ``resolve_mcp_config``. Disabled rows are inert templates
    (the legacy catalog behavior). ``oauth_status`` reflects the user's OAuth
    connection for this server name (None when the server has none).
    """

    name: str
    transport: str
    enabled: bool = False
    oauth_status: Optional[ConnectionStatus] = None
    # The capability groups this connection was actually granted, in the order
    # they were stored. None means no connection, or one for a server we curate
    # no groups for -- distinct from ``[]``, which is a brokerage the user
    # granted nothing. The consent is enforced per call at the relay, so a
    # surface that cannot read it back can only guess what a connection does.
    granted_capabilities: Optional[list[str]] = None
    # The same keys, but answering "what did the user last choose" rather than
    # "what is in force". They part company the moment a connection stops being
    # servable: the grant is gone, so the badges must not draw one, while the
    # choice behind it is still the user's and is what a reconnect has to open
    # on. Seeding a repair from product defaults instead re-proposed every group
    # the user had declined, on a flow they entered to fix an expiry rather than
    # to change their mind.
    remembered_capabilities: Optional[list[str]] = None
    # Host-side discovered tool count for the server's CURRENT config (OAuth
    # servers only today — that's the only user-level discovery path). None =
    # no current snapshot; the UI omits the count rather than showing 0.
    tool_count: Optional[int] = None
    # Set only when the server's handshake named a mark we can reach. A path on
    # this origin, never the server's own URL: resolving it here means one fetch
    # for everyone instead of every settings-page render telling a third party
    # who is looking, which is the same reason the brokerage marks are proxied.
    icon_url: Optional[str] = None
    command: Optional[str] = None
    args: list[str] = Field(default_factory=list)
    url: Optional[str] = None
    env_refs: list[str] = Field(default_factory=list)
    header_refs: list[str] = Field(default_factory=list)
    # Echo the stored reference maps (``${vault:NAME}`` ref strings or the
    # owner's own literals — never resolved secrets) so the edit form can
    # round-trip them, exactly as ``EffectiveServer`` does for workspace-origin
    # rows. A PUT replaces the whole row, so a response that dropped them would
    # make every unrelated edit a silent wipe.
    env: dict[str, str] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    description: str = ""
    instruction: str = ""
    tool_exposure_mode: str = "summary"
    discovery_uses_secrets: bool = False
    # Non-blocking policy nudges (isolation etc.) — populated on create/update
    # responses only, never stored.
    warnings: Optional[list[str]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    # Workspaces holding a tombstone for this name (deny-list) — populated in
    # the all-scopes view only, for the "active in" checklist.
    disabled_workspace_ids: list[str] = Field(default_factory=list)
    # Plugin provenance (display + row-policy only): the owning plugin's name
    # and its enable state, both None on a hand-made or detached row. The
    # workspace list has no plugins query to join against, so the row carries
    # what the UI needs to badge and to explain a suppressed server.
    plugin_name: Optional[str] = None
    plugin_enabled: Optional[bool] = None


class WorkspaceScopedServer(BaseModel):
    """A workspace-local server row surfaced in the all-scopes catalog view.

    A summary, not an editable config: editing stays on the workspace
    endpoints. ``shadows_inherited`` marks a name that also exists in the
    catalog (the local fork hides the inherited copy in its workspace).
    """

    name: str
    workspace_id: str
    transport: str = "stdio"
    enabled: bool = True
    description: str = ""
    shadows_inherited: bool = False


class CatalogServerList(BaseModel):
    """GET /api/v1/mcp/servers payload."""

    servers: list[CatalogServer]
    max_servers: int
    # all_scopes=true only: workspace-local servers across the user's workspaces.
    workspace_servers: list[WorkspaceScopedServer] = Field(default_factory=list)


class BuiltinServer(BaseModel):
    """One process-global builtin, with this user's account-wide toggle."""

    name: str
    description: str = ""
    transport: str = "stdio"
    enabled: bool
    # As on ``CatalogServer``: a path on this origin, present only when the
    # server's handshake named a mark. Ours draw their bundle's mark instead,
    # so in practice this fills in for a self-hoster's own additions.
    icon_url: Optional[str] = None
    # The bundle that ships this server, and whether that bundle is switched
    # on — the same provenance pair a catalog row carries for its plugin, so
    # the list groups and explains both kinds the same way. Only a server
    # declared outside ``plugins/`` (an operator's own YAML entry) has none.
    plugin_name: Optional[str] = None
    plugin_enabled: Optional[bool] = None
    # Workspaces with a disable-marker for this builtin — all-scopes view only.
    disabled_workspace_ids: list[str] = Field(default_factory=list)


class BuiltinServerList(BaseModel):
    """GET /api/v1/mcp/builtin-servers payload."""

    servers: list[BuiltinServer]


class CapabilityGroupOption(BaseModel):
    """One consent toggle offered when connecting a brokerage.

    ``key`` is the fact and also the translation key; ``tone`` is how loudly to
    draw the row. No label or description, for the reason the flags above carry
    no prose: the words are the client's.
    """

    key: str
    tone: str
    # One of the steps between reading and placing an order, which is the thing
    # a row is asked first. False for the reading groups.
    rung: bool = False


class BrokerageOption(BaseModel):
    """One shipped brokerage connector, as offered on the Plugins page.

    A catalog row does not exist for it until the user turns it on, so this
    carries no per-user state at all: the page joins it to the catalog by
    ``name``. The two behavioural flags travel as booleans rather than prose
    because the sentence that explains each one is translated client-side.
    """

    name: str
    label: str
    url: str
    # The broker's own website, not the endpoint's host. The detail view links
    # it, which is the one thing a user reliably wants that we cannot answer:
    # where their actual account lives.
    site: str = ""
    description: str = ""
    native_callback_only: bool = False
    exclusive_connection: bool = False
    # List order is display order. Empty would mean a brokerage we curate no
    # groups for, which the client reads as "nothing to choose".
    capabilities: list[CapabilityGroupOption] = []


class BrokerageList(BaseModel):
    """GET /api/v1/mcp/brokerages payload."""

    brokerages: list[BrokerageOption]


def brokerage_to_response(brokerage: Brokerage) -> BrokerageOption:
    """Shape a shipped brokerage definition for the API.

    A wire model of its own rather than the registry entry itself, because the
    two are allowed to diverge: a field the registry needs is not automatically
    one the API should carry. Extra keys are ignored on the way through, so
    adding one to :class:`Brokerage` keeps it off the wire until it is named
    above — and nobody has to maintain a copy to keep that true.

    The exception is ``capabilities``, which is derived rather than stored: the
    curation map is the source for which groups a vendor has, and copying them
    onto the registry entry would be a second place for that to be wrong.
    """
    from src.server.services.brokerage_capabilities import groups_for

    return BrokerageOption.model_validate(
        asdict(brokerage)
        | {
            "capabilities": [
                {"key": g.key, "tone": g.tone, "rung": g.rung}
                for g in groups_for(brokerage.name)
            ]
        }
    )


# ---------------------------------------------------------------------------
# Masking helpers — turn a stored config blob / catalog row into refs only.
# ---------------------------------------------------------------------------


def collect_vault_refs(mapping: dict[str, str] | None) -> list[str]:
    """Return the sorted, de-duplicated vault names referenced by a value map."""
    names: set[str] = set()
    for value in (mapping or {}).values():
        for match in VAULT_REF_RE.findall(value or ""):
            names.add(match)
    return sorted(names)


def catalog_row_to_response(
    row: dict[str, Any],
    *,
    oauth_status: ConnectionStatus | None = None,
    granted_capabilities: list[str] | None = None,
    remembered_capabilities: list[str] | None = None,
    tool_count: int | None = None,
    icon_url: str | None = None,
) -> CatalogServer:
    """Shape a DB catalog row for the owner-scoped API.

    ``env``/``headers`` are echoed verbatim (refs and literals alike — the row
    stores no resolved secret) so an edit round-trips; ``env_refs``/
    ``header_refs`` stay the display-only projection of the vault names.
    """
    return CatalogServer(
        name=row["name"],
        transport=row["transport"],
        enabled=bool(row.get("enabled", False)),
        oauth_status=oauth_status,
        granted_capabilities=granted_capabilities,
        remembered_capabilities=remembered_capabilities,
        tool_count=tool_count,
        icon_url=icon_url,
        command=row.get("command"),
        args=row.get("args") or [],
        url=row.get("url"),
        env_refs=collect_vault_refs(row.get("env")),
        header_refs=collect_vault_refs(row.get("headers")),
        env=dict(row.get("env") or {}),
        headers=dict(row.get("headers") or {}),
        description=row.get("description") or "",
        instruction=row.get("instruction") or "",
        tool_exposure_mode=row.get("tool_exposure_mode") or "summary",
        discovery_uses_secrets=bool(row.get("discovery_uses_secrets", False)),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
        # Indexed, not .get(): the plugin LEFT JOIN is part of every catalog
        # SELECT, so a missing key is a projection bug and should say so here
        # rather than silently reading as an unowned row. Matches the skills
        # projection, which makes the same argument.
        plugin_name=row["plugin_name"],
        plugin_enabled=(
            bool(row["plugin_enabled"])
            if row["plugin_enabled"] is not None
            else None
        ),
    )
