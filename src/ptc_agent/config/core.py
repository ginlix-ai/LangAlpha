"""Core configuration classes for Open PTC Agent infrastructure.

This module defines pure data classes for core configuration:
- Daytona sandbox settings
- MCP server configurations
- Filesystem access settings
- Security settings
- Logging settings
"""

import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Default security lists — used by SecurityConfig defaults and create_default_security_config()
DEFAULT_ALLOWED_IMPORTS = [
    "os", "sys", "json", "yaml", "requests", "datetime",
    "pathlib", "typing", "re", "math", "random", "time",
    "collections", "itertools", "functools", "subprocess", "shutil",
]

DEFAULT_BLOCKED_PATTERNS: list[str] = []


class DaytonaConfig(BaseModel):
    """Daytona sandbox configuration.

    All fields have sensible defaults. Only api_key needs to be set
    (via DAYTONA_API_KEY environment variable).
    """

    api_key: str = ""  # Set via DAYTONA_API_KEY env var, validated later
    base_url: str = "https://app.daytona.io/api"
    auto_stop_interval: int = 3600  # 1 hour
    auto_archive_interval: int = 604800  # 7 days — keep stopped (fast restart) before cold storage
    auto_delete_interval: int = 7776000  # 90 days — total dormant lifetime
    python_version: str = "3.12"

    # Snapshot configuration for faster sandbox initialization
    snapshot_enabled: bool = True
    snapshot_name: str | None = None
    snapshot_auto_create: bool = True


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
    """Credential an MCP server expects users to set in the workspace vault.

    Pure metadata — never touches actual values. Surfaced via
    GET /api/v1/workspaces/{id}/vault/blueprints as a 'recommended but not set'
    list in the UI's Vault tab, so users don't have to read docs to learn the
    exact secret name for each integration.
    """

    # Same regex as CreateSecretRequest in src/server/app/vault.py:37.
    # Blueprint name becomes a vault key, so it must satisfy the same rule.
    # The regex's {0,63} already caps length at 64 — no separate max_length needed.
    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
    label: str = Field(..., min_length=1, max_length=80)
    # max_length matches CreateSecretRequest.description in src/server/app/vault.py
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
    tool_exposure_mode: Literal["summary", "detailed"] | None = None  # Per-server override
    vault_blueprints: list[VaultBlueprint] = Field(default_factory=list)


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

    working_directory: str = "/home/workspace"
    allowed_directories: list[str] | None = None
    denied_directories: list[str] | None = None
    enable_path_validation: bool = True

    def model_post_init(self, __context: Any) -> None:
        """Derive allowed/denied directories from working_directory when not set."""
        if self.allowed_directories is None:
            self.allowed_directories = [self.working_directory, "/tmp"]
        if self.denied_directories is None:
            self.denied_directories = [f"{self.working_directory}/_internal"]


def validate_daytona_api_key(daytona: DaytonaConfig) -> None:
    """Validate that the Daytona API key is present.

    Raises:
        ValueError: If the API key is missing
    """
    if not daytona.api_key:
        raise ValueError(
            "Missing required credentials in .env file:\n"
            "  - DAYTONA_API_KEY\n"
            "Please add these credentials to your .env file."
        )


class DockerConfig(BaseModel):
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

    image: str = "langalpha-sandbox:latest"
    working_dir: str = "/home/workspace"  # fallback; filesystem.working_directory is authoritative
    memory_limit: str = "4g"
    cpu_count: float = 2.0
    dev_mode: bool = False
    host_work_dir: str | None = None
    volumes: list[str] = Field(default_factory=list)
    network_mode: str = "bridge"
    preview_proxy_ports: str = "13000-13009"
    preview_base_url: str | None = None


class SandboxConfig(BaseModel):
    """Provider-agnostic sandbox configuration wrapper."""

    provider: Literal["daytona", "docker", "memory"] = "daytona"
    daytona: DaytonaConfig = Field(default_factory=DaytonaConfig)
    docker: DockerConfig = Field(default_factory=DockerConfig)


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
        if self.sandbox.provider == "daytona":
            validate_daytona_api_key(self.sandbox.daytona)


def create_default_security_config() -> SecurityConfig:
    """Create SecurityConfig with sensible defaults for Daytona sandbox execution."""
    return SecurityConfig()
