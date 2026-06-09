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

Response models NEVER echo env/header literal values for any row — only the
vault reference names are surfaced (``env_refs`` / ``header_refs``); literals
are masked.
"""

from __future__ import annotations

import ipaddress
import re
from typing import Any, Literal, Optional
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, model_validator

from src.ptc_agent.core.mcp_sanitize import VAULT_REF_RE

# ---------------------------------------------------------------------------
# Shared constants — single source of truth for validators (also mirrored
# in the frontend Zod schema; keep the two in sync).
# ---------------------------------------------------------------------------

NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")

# Allowed stdio commands — deliberately WITHOUT `bash` (and any shell). Running
# a user-chosen command is arbitrary code execution; this is the allowlist that
# bounds it (plan §Security #4).
ALLOWED_COMMANDS = frozenset({"npx", "uvx", "uv", "python", "python3", "node"})

DESCRIPTION_MAX = 512
INSTRUCTION_MAX = 1024

# Reject keys the user must never set on an MCP server payload.
_FORBIDDEN_KEYS = ("vault_blueprints", "source")

# A bare host-env placeholder like ``${VAR}`` or ``$VAR`` — never resolves for
# workspace servers (only ``${vault:NAME}`` does), so fail fast at the API.
_BARE_ENV_RE = re.compile(r"\$\{?[A-Za-z_][A-Za-z0-9_]*\}?")

_TRANSPORTS = ("stdio", "sse", "http")
_EXPOSURE_MODES = ("summary", "detailed")


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
    """A value is OK iff it is a single full ``${vault:NAME}`` ref or a literal
    with no ``${...}``-style placeholders at all."""
    if VAULT_REF_RE.fullmatch(value):
        return
    # Any remaining ``${...}`` / ``$VAR`` token is a host-env-style placeholder
    # that will never resolve for a workspace server — reject it.
    if "${vault:" in value:
        raise ValueError(
            f"{kind} value for {key!r} contains a malformed vault reference; "
            "use the exact form ${vault:NAME}"
        )
    if _BARE_ENV_RE.search(value):
        raise ValueError(
            f"{kind} value for {key!r} looks like a host-env placeholder; "
            "use ${vault:NAME} for secrets or a plain literal value"
        )


# ---------------------------------------------------------------------------
# URL policy
# ---------------------------------------------------------------------------


def validate_remote_url(url: str) -> str:
    """Enforce the SSRF-hardening URL policy for sse/http servers (plan §6)."""
    if not isinstance(url, str) or not url:
        raise ValueError("url is required for sse/http transports")
    if "${vault:" in url or _BARE_ENV_RE.search(url):
        raise ValueError("url must not contain secrets or placeholders; put credentials in headers")

    parts = urlsplit(url)
    if parts.scheme != "https":
        raise ValueError("url must use https://")
    if parts.username or parts.password or "@" in (parts.netloc or ""):
        raise ValueError("url must not contain userinfo credentials")

    host = parts.hostname
    if not host:
        raise ValueError("url must include a host")
    host_l = host.lower().rstrip(".")

    # Hostname blocklist (loopback / internal naming conventions).
    if host_l == "localhost" or host_l.endswith(
        (".local", ".internal", ".localhost")
    ):
        raise ValueError(f"url host {host!r} is not allowed")

    # Literal IP blocklist: loopback / private / link-local / metadata / ULA.
    try:
        ip = ipaddress.ip_address(host_l.strip("[]"))
    except ValueError:
        ip = None
    if ip is not None and (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    ):
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
            if self.command not in ALLOWED_COMMANDS:
                raise ValueError(
                    f"command {self.command!r} is not allowed; choose one of "
                    f"{sorted(ALLOWED_COMMANDS)}"
                )
            _validate_secret_map(self.env, kind="env", key_re=ENV_KEY_RE)
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
        }


class FromTemplateInput(BaseModel):
    """POST body variant that copies a user catalog template into a workspace."""

    from_template: str = Field(..., min_length=1, max_length=64)

    model_config = {"extra": "forbid"}


class EnabledInput(BaseModel):
    """PATCH body for the enabled toggle."""

    enabled: bool

    model_config = {"extra": "forbid"}


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

    ``env_refs`` / ``header_refs`` carry ONLY the vault names referenced by the
    config — literal env/header values are never echoed.
    """

    name: str
    origin: Literal["builtin", "workspace"]
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
    description: str = ""
    instruction: str = ""
    tool_exposure_mode: Optional[str] = None
    command: Optional[str] = None
    args: list[str] = Field(default_factory=list)
    url: Optional[str] = None
    config_version: int = 0


class EffectiveServerList(BaseModel):
    """GET /{id}/mcp/servers payload."""

    servers: list[EffectiveServer]
    sandbox_running: bool
    max_servers: int
    config_version: int


class CatalogServer(BaseModel):
    """A user catalog template row (masked — only vault refs surfaced)."""

    name: str
    transport: str
    command: Optional[str] = None
    args: list[str] = Field(default_factory=list)
    url: Optional[str] = None
    env_refs: list[str] = Field(default_factory=list)
    header_refs: list[str] = Field(default_factory=list)
    description: str = ""
    instruction: str = ""
    tool_exposure_mode: str = "summary"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


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


def catalog_row_to_response(row: dict[str, Any]) -> CatalogServer:
    """Mask a DB catalog row: drop env/header literals, expose vault refs only."""
    return CatalogServer(
        name=row["name"],
        transport=row["transport"],
        command=row.get("command"),
        args=row.get("args") or [],
        url=row.get("url"),
        env_refs=collect_vault_refs(row.get("env")),
        header_refs=collect_vault_refs(row.get("headers")),
        description=row.get("description") or "",
        instruction=row.get("instruction") or "",
        tool_exposure_mode=row.get("tool_exposure_mode") or "summary",
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )
