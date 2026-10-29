"""Core configuration classes for Open PTC Agent infrastructure.

This module defines pure data classes for core configuration:
- Sandbox settings (Daytona, Docker, E2B, SSH)
- MCP server configurations
- Filesystem access settings
- Security settings
- Logging settings
"""

import ipaddress
import logging
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

# Default security lists — used by SecurityConfig defaults and create_default_security_config()
DEFAULT_ALLOWED_IMPORTS = [
    "os", "sys", "json", "yaml", "requests", "datetime",
    "pathlib", "typing", "re", "math", "random", "time",
    "collections", "itertools", "functools", "subprocess", "shutil",
]

DEFAULT_BLOCKED_PATTERNS: list[str] = []


class ResourceTier(BaseModel):
    """A named sandbox resource preset (vCPU cores, memory GiB, disk GiB)."""

    cpu: int = Field(ge=1)
    memory: int = Field(ge=1)
    disk: int = Field(ge=1)


# Daytona's largest sandbox; a tier above it is refused at create.
DAYTONA_TIER_CEILING = ResourceTier(cpu=4, memory=8, disk=10)
# E2B Pro's largest template. Its disk is free space on top of the image, and a
# 20 GiB build is accepted (2026-10: 25 GiB root, 21 GiB free).
E2B_TIER_CEILING = ResourceTier(cpu=8, memory=8, disk=20)


def _default_resource_tiers() -> dict[str, ResourceTier]:
    return {
        "standard": ResourceTier(cpu=1, memory=2, disk=5),
        "performance": ResourceTier(cpu=2, memory=4, disk=10),
        "max": ResourceTier(cpu=2, memory=8, disk=10),
    }


def _e2b_resource_tiers() -> dict[str, ResourceTier]:
    # xlarge exceeds Daytona's ceiling, so only E2B offers it.
    return {**_default_resource_tiers(), "xlarge": ResourceTier(cpu=4, memory=8, disk=20)}


class ResourceTieredConfig(BaseModel):
    """Named cpu/memory/disk presets, declared by every provider that sizes sandboxes.

    A tier name is persisted on the computer row and outlives any config edit,
    so every provider that offers a name resolves it to the same size. A
    provider may offer fewer names than another: a tier above its ceiling is
    left out rather than shrunk to fit.
    """

    # The largest sandbox the provider builds; None when it sets no limit.
    TIER_CEILING: ClassVar[ResourceTier | None] = None

    default_tier: str = "standard"
    resource_tiers: dict[str, ResourceTier] = Field(
        default_factory=_default_resource_tiers, validate_default=True
    )

    @field_validator("resource_tiers")
    @classmethod
    def _tiers_fit_the_ceiling(
        cls, v: dict[str, ResourceTier]
    ) -> dict[str, ResourceTier]:
        """Refuse a tier the provider cannot build.

        Clamping one sold a size the machine never had, and the provider
        refuses it at create anyway, so the error belongs at load.
        """
        ceiling = cls.TIER_CEILING
        if ceiling is None:
            return v
        for name, tier in v.items():
            over = [
                f"{resource}={getattr(tier, resource)} (max {getattr(ceiling, resource)})"
                for resource in ("cpu", "memory", "disk")
                if getattr(tier, resource) > getattr(ceiling, resource)
            ]
            if over:
                raise ValueError(
                    f"resource_tiers[{name!r}] exceeds what {cls.__name__} "
                    f"can build: {', '.join(over)}"
                )
        return v

    @model_validator(mode="after")
    def _default_tier_must_exist(self) -> "ResourceTieredConfig":
        """The default tier must resolve to a real preset — the create path
        relies on it to size base sandboxes, so a missing default is a config bug."""
        if self.default_tier not in self.resource_tiers:
            raise ValueError(
                f"default_tier {self.default_tier!r} is not defined in "
                f"resource_tiers (available: {sorted(self.resource_tiers)})"
            )
        return self


class CredentialedSandboxConfig(ResourceTieredConfig):
    """Settings of a provider reached with an API key.

    Its credentials and endpoints come from the environment and the rest from
    the YAML section, so a key never has to be written into a config file.
    """

    # Field to the environment variable that alone sets it.
    ENV_FIELDS: ClassVar[dict[str, str]] = {}
    # The line naming the missing key in the error a deployment without it gets.
    API_KEY_HELP: ClassVar[str] = ""

    api_key: str = ""
    # A deployment-unique prefix for its platform Secrets, so two deployments
    # sharing one provider account never reconcile one Secret.
    secret_namespace: str = ""

    @classmethod
    def from_section(cls, data: Mapping[str, Any] | None = None) -> Self:
        """Build from a YAML section with the environment laid over it.

        An ``ENV_FIELDS`` value in the YAML is never read, and an unknown key
        is ignored. ``None`` is an absent section: every tunable keeps its
        model default.
        """
        tunables = {
            key: value
            for key, value in (data or {}).items()
            if key in cls.model_fields and key not in cls.ENV_FIELDS
        }
        env = {field: os.getenv(var, "") for field, var in cls.ENV_FIELDS.items()}
        return cls(**tunables, **env)

    def require_api_key(self) -> None:
        """Raise ValueError when the API key is missing."""
        if not self.api_key:
            raise ValueError(
                "Missing required credentials in .env file:\n"
                f"  - {self.API_KEY_HELP}\n"
                "Please add these credentials to your .env file."
            )


class DaytonaConfig(CredentialedSandboxConfig):
    """Daytona sandbox configuration.

    All fields have sensible defaults. Only api_key needs to be set
    (via DAYTONA_API_KEY environment variable).
    """

    ENV_FIELDS: ClassVar[dict[str, str]] = {
        "api_key": "DAYTONA_API_KEY",
        "secret_namespace": "DAYTONA_SECRET_NAMESPACE",
    }
    API_KEY_HELP: ClassVar[str] = "DAYTONA_API_KEY"
    TIER_CEILING: ClassVar[ResourceTier | None] = DAYTONA_TIER_CEILING

    base_url: str = "https://app.daytona.io/api"
    auto_stop_interval: int = 3600  # 1 hour
    auto_archive_interval: int = 604800  # 7 days, keep stopped (fast restart) before cold storage
    auto_delete_interval: int = 7776000  # 90 days, total dormant lifetime
    # How long a computer's starts may be refused by a recovering host before
    # it is rebuilt from its backup instead; the old sandbox is kept. 0 waits.
    host_unavailable_rebuild_after_minutes: int = Field(default=60, ge=0)
    python_version: str = "3.12"

    # Snapshot configuration for faster sandbox initialization
    snapshot_enabled: bool = True
    snapshot_name: str | None = None
    snapshot_auto_create: bool = True

    @classmethod
    def from_section(cls, data: Mapping[str, Any] | None = None) -> Self:
        """A section that is present must state the intervals and image it runs."""
        if data is not None:
            from ptc_agent.config.utils import (
                DAYTONA_REQUIRED_FIELDS,
                validate_section_fields,
            )

            validate_section_fields(data, DAYTONA_REQUIRED_FIELDS, "daytona")
        return super().from_section(data)


class E2BConfig(CredentialedSandboxConfig):
    """E2B sandbox configuration.

    Only ``api_key`` needs to be set (E2B_API_KEY). The URLs stay empty for E2B
    Cloud; a self-hosted E2B sets ``domain``, or ``api_url`` and ``sandbox_url``.
    """

    ENV_FIELDS: ClassVar[dict[str, str]] = {
        "api_key": "E2B_API_KEY",
        "domain": "E2B_DOMAIN",
        "api_url": "E2B_API_URL",
        "sandbox_url": "E2B_SANDBOX_URL",
        "secret_namespace": "E2B_SECRET_NAMESPACE",
    }
    API_KEY_HELP: ClassVar[str] = (
        "E2B_API_KEY (create one at https://e2b.dev/dashboard?tab=keys)"
    )
    TIER_CEILING: ClassVar[ResourceTier | None] = E2B_TIER_CEILING

    resource_tiers: dict[str, ResourceTier] = Field(
        default_factory=_e2b_resource_tiers, validate_default=True
    )

    domain: str = ""
    api_url: str = ""
    sandbox_url: str = ""
    # Seconds without activity before a sandbox pauses. E2B keeps a paused
    # sandbox (memory included) indefinitely and bills nothing for it.
    auto_stop_interval: int = Field(default=3600, ge=60)
    # The plan's longest continuous run (3600 on Hobby, 86400 on Pro). It caps
    # every timeout we ask for.
    max_timeout: int = Field(default=3600, ge=300, le=86400)
    # Prefix of the per-tier template names; None uses "langalpha".
    template_name: str | None = None
    template_auto_create: bool = True
    # Whether a sandbox's ports answer anyone who has its URL. False makes each
    # port require E2B's traffic token, which only the preview proxy holds, so
    # the deployment must set PREVIEW_PROXY_BASE_URL. Fixed when a sandbox is
    # created: changing it reaches new computers only.
    public_traffic: bool = True

class SecurityConfig(BaseModel):
    """Security configuration for code execution.

    All fields have sensible defaults for safe code execution.
    """

    max_execution_time: int = 300  # 5 minutes
    max_code_length: int = 10000
    max_file_size: int = 10485760  # 10MB
    enable_code_validation: bool = True
    allowed_imports: list[str] = Field(
        default_factory=lambda: list(DEFAULT_ALLOWED_IMPORTS)
    )
    blocked_patterns: list[str] = Field(
        default_factory=lambda: list(DEFAULT_BLOCKED_PATTERNS)
    )


class VaultBlueprint(BaseModel):
    """Credential an MCP server expects users to set in their vault.

    Pure metadata — never touches actual values. Surfaced via
    GET /api/v1/mcp/vault/blueprints as a 'recommended but not set' list on
    the Plugins page's Secrets tab, so users don't have to read docs to learn
    the exact secret name for each integration.
    """

    # Same regex as CreateSecretRequest in src/server/models/vault.py.
    # Blueprint name becomes a vault key, so it must satisfy the same rule.
    # The regex's {0,63} already caps length at 64 — no separate max_length needed.
    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
    label: str = Field(..., min_length=1, max_length=80)
    # max_length matches CreateSecretRequest.description in src/server/models/vault.py
    # so that pre-filling the add-secret form from a blueprint never produces a
    # description that the create endpoint would reject with a 422.
    description: str = Field("", max_length=256)
    docs_url: str | None = None
    # Optional client-side hint for the secret *value*. Stored as a string (not
    # compiled at runtime on the value) because it ships to the frontend for
    # validation. The backend never runs it against values — keeps secrets opaque.
    regex: str | None = None

    @field_validator("regex")
    @classmethod
    def _validate_regex_compiles(cls, v: str | None) -> str | None:
        """Fail-fast at config-load time if an operator typos the pattern."""
        if v is None:
            return v
        try:
            re.compile(v)
        except re.error as exc:
            raise ValueError(f"vault_blueprint regex is not a valid pattern: {exc}")
        return v

    @field_validator("docs_url")
    @classmethod
    def _validate_docs_url_scheme(cls, v: str | None) -> str | None:
        # docs_url renders directly into an <a href>; rel=noopener noreferrer
        # does not block javascript:/data: schemes, so reject them at load time.
        if v is None:
            return v
        if not (v.startswith("https://") or v.startswith("http://")):
            raise ValueError("vault_blueprint docs_url must start with https:// or http://")
        return v


class MCPServerConfig(BaseModel):
    """Configuration for a single MCP server."""

    name: str
    enabled: bool = True  # Whether this server is enabled (default: True)
    description: str = ""  # What the MCP server does
    instruction: str = ""  # When/how to use this server
    transport: Literal["stdio", "sse", "http"] = "stdio"
    command: str | None = None
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    url: str | None = None  # For SSE/HTTP transports
    headers: dict[str, str] = Field(default_factory=dict)  # For SSE/HTTP transports
    tool_exposure_mode: Literal["summary", "detailed"] | None = None  # Per-server override
    vault_blueprints: list[VaultBlueprint] = Field(default_factory=list)
    # 'builtin' = from agent_config.yaml; 'user' = user-level server inherited
    # by every workspace of the user unless that workspace switched it off.
    # User servers are untrusted: vault-only secret resolution, neutral prompt
    # framing, sanitized discovery.
    source: Literal["builtin", "user"] = "builtin"
    discovery_uses_secrets: bool = False  # user servers: resolve vault secrets during discovery (default off = secret-less probe)
    # Set at resolve time when the user has a non-revoked OAuth connection for
    # this server — the server is then bound through the egress relay and its
    # sandbox config carries a grant reference instead of the vendor URL.
    oauth_connection_id: str | None = None
    # The credential the sandbox client presents to a built-in http server.
    # ``data_token`` is the computer's own data token, read from the token file
    # the server keeps current, so the server holds no key of its own. The
    # client resends such a server's requests on a refused token, a rate limit
    # or a gateway error, so it must be one whose tools only read.
    auth: Literal["data_token"] | None = None

    @model_validator(mode="after")
    def _auth_is_builtin_http_only(self) -> Self:
        # The token authorizes the whole computer's data access, so only a
        # server the operator configured may receive it, and only over the
        # one transport whose requests the client stamps.
        if self.auth is None:
            return self
        if self.source != "builtin":
            raise ValueError(f"MCP server {self.name!r}: auth is for built-in servers only")
        if self.transport != "http":
            raise ValueError(
                f"MCP server {self.name!r}: auth {self.auth!r} requires transport 'http'"
            )
        return self


class MCPConfig(BaseModel):
    """MCP server configurations.

    By default, no MCP servers are configured. Add servers to enable
    additional tools for the agent.
    """

    servers: list[MCPServerConfig] = Field(default_factory=list)
    tool_discovery_enabled: bool = True
    lazy_load: bool = True
    cache_duration: int | None = None
    tool_exposure_mode: Literal["summary", "detailed"] = "summary"


class LoggingConfig(BaseModel):
    """Logging configuration with sensible defaults."""

    level: str = "INFO"
    file: str = "logs/ptc.log"


class FilesystemConfig(BaseModel):
    """Filesystem access configuration for first-class filesystem tools.

    ``allowed_directories`` and ``denied_directories`` are derived from
    ``working_directory`` by default so you only need to set one value.

    Note: this validation is enforced for first-class filesystem tools only.
    """

    # SandboxLayout.default().root. Spelled out because ptc_agent.core imports
    # this module, so importing the layout at module scope would cycle; the
    # layout test pins this literal to it.
    working_directory: str = "/home/workspace"
    allowed_directories: list[str] | None = None
    denied_directories: list[str] | None = None
    enable_path_validation: bool = True

    def model_post_init(self, __context: Any) -> None:
        """Derive allowed/denied directories from working_directory when not set."""
        # Imported here, not at module scope: ptc_agent.core.__init__ imports
        # this module, so the layout can only be reached once both exist.
        from ptc_agent.core.paths import SandboxLayout

        layout = SandboxLayout(self.working_directory)
        if self.allowed_directories is None:
            object.__setattr__(self, "allowed_directories", layout.allowed_directories)
            self.__pydantic_fields_set__.discard("allowed_directories")
        if self.denied_directories is None:
            object.__setattr__(self, "denied_directories", layout.denied_directories)
            self.__pydantic_fields_set__.discard("denied_directories")


class DockerConfig(ResourceTieredConfig):
    """Docker sandbox provider configuration.

    Mount options (combined freely):

    * **dev_mode + host_work_dir** — bind-mount a host directory as the
      sandbox working directory.  Files appear on both sides instantly.
    * **volumes** — arbitrary extra mounts in Docker bind format
      (``"host_path:container_path[:ro]"``).  Useful for datasets, models,
      or credentials that should be available inside the sandbox.

    Examples::

        # Dev mode — edit files on host, see changes in sandbox
        docker:
          dev_mode: true
          host_work_dir: "/Users/me/project/sandbox-work"

        # Extra read-only data mount
        docker:
          volumes:
            - "/data/datasets:/mnt/datasets:ro"

        # Both
        docker:
          dev_mode: true
          host_work_dir: "/Users/me/work"
          volumes:
            - "/data/models:/mnt/models:ro"
            - "/secrets/keys:/run/secrets:ro"
    """

    # No TIER_CEILING: the host is the limit, and Docker refuses a container
    # above it at create.
    image: str = "langalpha-sandbox:latest"
    working_dir: str = "/home/workspace"  # fallback; filesystem.working_directory is authoritative
    # Docker never stops a container on idle, so the server's idle reaper
    # stops a docker computer by this interval. Daytona's default, which is
    # what a docker computer was reaped by before it had one of its own.
    auto_stop_interval: int = 3600
    # Floor for every tier: a single-host deployment sized its containers here
    # before tiers existed, and a named tier may only raise that, never lower it.
    memory_limit: str = "4g"
    cpu_count: float = 2.0
    # A tier's disk only reaches the container on a storage driver that supports
    # per-container quotas (overlay2 on xfs with pquota, btrfs, zfs, devicemapper);
    # every other driver rejects the option outright, so it is opt-in.
    storage_quota_enabled: bool = False
    dev_mode: bool = False
    host_work_dir: str | None = None
    volumes: list[str] = Field(default_factory=list)
    network_mode: str = "bridge"
    preview_proxy_ports: str = "13000-13009"
    preview_base_url: str | None = None


def _single_tier() -> dict[str, ResourceTier]:
    return {"standard": ResourceTier(cpu=1, memory=1, disk=1)}


class SSHConfig(ResourceTieredConfig):
    """A machine the user brings, reached over SSH.

    The deployment's section holds only the tunables; the target (host, user,
    pinned host key, whose client key) is each computer's own
    ``provider_config``, set by the server when the computer is added, never
    by the YAML. The machine's size is its own, so the kind has one tier and
    the tier is never shown.
    """

    host: str = ""
    port: int = Field(default=22, ge=1, le=65535)
    username: str = ""
    # The machine's public host key as an OpenSSH line ("ssh-ed25519 AAAA..."),
    # pinned when the computer is added. A connection presenting any other key
    # is refused.
    host_key: str = ""
    # The user whose client key authenticates, which only the server sets.
    key_owner: str = ""
    # The tunnel of a machine that connects out to the deployment's gateway,
    # which only the server sets. With one, ``host`` is the machine's name and
    # ``port`` its sshd's port on the machine itself; nothing dials either.
    tunnel: str = ""
    # LangAlpha's helper answers for the machine rather than an sshd, which
    # only the server sets, from the probe. The helper keeps the key in its
    # own folder, so nothing of ours is in the login's ~/.ssh.
    helper: bool = False
    # The machine's system as ``uname -s`` names it ("Darwin", "Linux"), which
    # only the server sets, from the probe. Nothing connects by it: it tells
    # the agent a Mac's command line tools are not GNU's.
    os: str = ""
    # The machine never stops on idle, so the server's idle reaper stops a
    # quiet one by this interval. Stopping only ends this computer's own
    # processes there; the machine keeps running.
    auto_stop_interval: int = Field(default=3600, ge=60)
    # The server dials each machine itself, so by default it connects only to
    # public addresses: a user's machine is never a way into the network the
    # server sits on. A deployment whose users' machines are on its own LAN or
    # VPN turns this on.
    allow_private_addresses: bool = False
    # Dials per user per hour to addresses the user names (a probe, a pairing
    # report, a create), counted by host and port tried. Starting a computer
    # that exists is not counted. 0 is no limit.
    dials_per_hour: int = Field(default=60, ge=0)
    # The machine belongs to whoever added it, so by default nothing this
    # deployment holds reaches it (``SandboxProvider.trusted_host``). A
    # deployment whose every user may hold its keys, one person self-hosting
    # most often, turns this on, and its machines get the env any computer
    # does, the built-in servers' keys included. Each machine's env follows
    # this at its next open, so turning it off takes those keys off.
    trusted_host: bool = False
    # The addresses this deployment's SSH connections reach machines from, as
    # the machines see them (IPs or CIDR networks). Named, the key's line on
    # every machine admits logins only from these and from the machine itself,
    # so a copy of the key is no use elsewhere. Empty, it admits any address,
    # since where a self-hosted server's connections come from is not known.
    login_from: list[str] = Field(default_factory=list)
    connect_timeout: int = Field(default=10, ge=1)
    keepalive_interval: int = Field(default=15, ge=1)
    keepalive_count_max: int = Field(default=3, ge=1)
    # The first start installs the Python runtime, which downloads about a
    # gigabyte of packages on the machine's own network.
    bootstrap_timeout: int = Field(default=1800, ge=60)
    # SSH computers one user may have, 0 for no limit. The plan's computer
    # cap leaves them out, the machines being their owners', but each still
    # holds a connection, a tunnel and its sweeps on this server.
    max_computers_per_user: int = Field(default=20, ge=0)
    resource_tiers: dict[str, ResourceTier] = Field(
        default_factory=_single_tier, validate_default=True
    )

    # Each computer's own, in its ``provider_config``.
    TARGET_FIELDS: ClassVar[tuple[str, ...]] = (
        "host",
        "port",
        "username",
        "host_key",
        "key_owner",
        "tunnel",
        "helper",
        "os",
    )

    @field_validator("login_from")
    @classmethod
    def _login_sources(cls, entries: list[str]) -> list[str]:
        # As sshd's from= reads them. A host name is refused: sshd matches one
        # only where it resolves clients by name, which it does not by default.
        sources = []
        for entry in entries:
            try:
                network = ipaddress.ip_network(str(entry).strip(), strict=False)
            except ValueError:
                raise ValueError(
                    f"sandbox.ssh.login_from: {entry!r} is not an IP address or network"
                ) from None
            sources.append(
                str(network.network_address)
                if network.num_addresses == 1
                else str(network)
            )
        return list(dict.fromkeys(sources))

    @classmethod
    def from_section(cls, data: Mapping[str, Any] | None = None) -> Self:
        """The deployment's section, which may not name any computer's target.

        A computer's ``provider_config`` is laid over these settings, so a
        target set here would reach every computer that lacks the key, and a
        tunnel would route them all through one machine without a word.
        """
        named = [key for key in cls.TARGET_FIELDS if key in (data or {})]
        if named:
            raise ValueError(
                f"sandbox.ssh cannot set {', '.join(named)}: each computer has "
                "its own, which the server sets when the computer is added"
            )
        return cls(**(data or {}))


class PlatformSecretDefinition(BaseModel):
    """One backend-owned platform credential, declared in agent_config.yaml.

    The set of entries is the deployment's platform-secret catalog: convergence,
    certification, and the fleet generation all operate on every entry at once,
    and any set change registers as an identity change that re-pends the fleet.
    Declared in deployment config (not code) so the OSS repo reveals the
    mechanism without the hosted vendor list.

    ``header`` is the request header that carries the credential. Daytona
    substitutes the placeholder wherever the sandbox puts it and ignores this;
    E2B sets this one header, and only it, on requests to ``hosts``.
    """

    model_config = ConfigDict(frozen=True)

    source_env_var: str
    sandbox_env_var: str
    name_suffix: str
    description: str
    hosts: tuple[str, ...]
    header: str | None = None

    @field_validator("source_env_var", "sandbox_env_var")
    @classmethod
    def _validate_env_var(cls, v: str) -> str:
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", v):
            raise ValueError(f"invalid environment variable name: {v!r}")
        return v

    @field_validator("name_suffix")
    @classmethod
    def _validate_name_suffix(cls, v: str) -> str:
        # Length-bounded so the derived Secret name (namespace + '-' + suffix)
        # stays under the provider/DB limit enforced in resolve_platform_secrets.
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,62}", v):
            raise ValueError(f"invalid Secret name suffix: {v!r}")
        return v

    @field_validator("hosts")
    @classmethod
    def _validate_hosts(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        if not v:
            raise ValueError("hosts must name at least one egress target")
        for host in v:
            if not re.fullmatch(r"[a-z0-9]([a-z0-9.-]*[a-z0-9])?", host):
                raise ValueError(f"invalid egress host: {host!r}")
        return v

    @field_validator("header")
    @classmethod
    def _validate_header(cls, v: str | None) -> str | None:
        if v is not None and not re.fullmatch(r"[A-Za-z0-9!#$%&'*+.^_`|~-]+", v):
            raise ValueError(f"invalid header name: {v!r}")
        return v


class SandboxConfig(BaseModel):
    """Provider-agnostic sandbox configuration wrapper."""

    provider: Literal["daytona", "docker", "e2b", "memory"] = "daytona"
    # Kinds a new computer may be created on besides ``provider``, which stays
    # the default. Only this list offers a kind: holding its credentials does
    # not, since one environment often carries keys for several providers.
    # ``ssh`` is only ever offered: a computer on it needs a machine its owner
    # names, so it cannot be the default.
    offered_providers: tuple[Literal["daytona", "docker", "e2b", "ssh"], ...] = ()
    daytona: DaytonaConfig = Field(default_factory=DaytonaConfig)
    e2b: E2BConfig = Field(default_factory=E2BConfig)
    docker: DockerConfig = Field(default_factory=DockerConfig)
    ssh: SSHConfig = Field(default_factory=SSHConfig)
    platform_secrets: tuple[PlatformSecretDefinition, ...] = ()

    def creatable_providers(self) -> tuple[str, ...]:
        """The kinds a new computer may be created on, the default first.

        Deduplicated because ``SANDBOX_PROVIDER`` can move the deployment onto
        a kind the config also offers.
        """
        return tuple(dict.fromkeys((self.provider, *self.offered_providers)))

    def settings_for(
        self, kind: str
    ) -> DaytonaConfig | DockerConfig | E2BConfig | SSHConfig:
        """The settings computers of ``kind`` are built from.

        Each kind's settings sit under a field of the kind's own name, so a
        kind is known exactly when such a field holds provider settings.

        Raises:
            ValueError: If ``kind`` names no provider with settings here.
        """
        settings = (
            getattr(self, kind)
            if isinstance(kind, str) and kind in type(self).model_fields
            else None
        )
        if not isinstance(settings, ResourceTieredConfig):
            raise ValueError(f"Unknown sandbox provider: {kind!r}")
        return settings  # type: ignore[return-value]

    @model_validator(mode="after")
    def _validate_unique_platform_secrets(self) -> "SandboxConfig":
        # Both keys must be unique across the catalog: a duplicate
        # sandbox_env_var collapses the binding map (and collides the rollout
        # row keyed on it), and a duplicate name_suffix resolves two entries to
        # one provider Secret — either silently mounts the wrong credential.
        for field in ("sandbox_env_var", "name_suffix"):
            values = [getattr(d, field) for d in self.platform_secrets]
            duplicates = sorted({v for v in values if values.count(v) > 1})
            if duplicates:
                raise ValueError(
                    f"duplicate {field} in platform_secrets: {duplicates}"
                )
        return self


def detect_provider_kind(daytona_api_key: str = "") -> str:
    """The provider a deployment that names none runs on, read from its keys.

    Daytona wins when both keys are set, and a deployment with neither runs
    sandboxes on the local Docker daemon.
    """
    if daytona_api_key or os.getenv("DAYTONA_API_KEY"):
        return "daytona"
    if os.getenv("E2B_API_KEY"):
        return "e2b"
    return "docker"


def validate_sandbox_api_keys(sandbox: SandboxConfig) -> None:
    """Require the credentials of every kind a new computer may be created on.

    An offered kind without them would accept computers that can never start.

    Raises:
        ValueError: If a creatable kind's API key is missing
    """
    for kind in sandbox.creatable_providers():
        try:
            settings = sandbox.settings_for(kind)
        except ValueError:
            # The in-memory test provider has no settings and no key.
            continue
        if isinstance(settings, CredentialedSandboxConfig):
            settings.require_api_key()


class CoreConfig(BaseModel):
    """Core infrastructure configuration.

    Contains settings for sandbox, MCP servers, filesystem, security, and logging.
    LLM configuration is handled separately in src/config/agent.py.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Sub-configurations
    sandbox: SandboxConfig
    security: SecurityConfig
    mcp: MCPConfig
    logging: LoggingConfig
    filesystem: FilesystemConfig
    config_file_dir: Path | None = Field(default=None, exclude=True)

    @property
    def daytona(self) -> DaytonaConfig:
        """Backward-compat shim: config.daytona -> config.sandbox.daytona."""
        return self.sandbox.daytona

    def validate_api_keys(self) -> None:
        """Validate that required API keys are present.

        Raises:
            ValueError: If required API keys are missing
        """
        validate_sandbox_api_keys(self.sandbox)


def create_default_security_config() -> SecurityConfig:
    """Create SecurityConfig with sensible defaults for Daytona sandbox execution."""
    return SecurityConfig()
