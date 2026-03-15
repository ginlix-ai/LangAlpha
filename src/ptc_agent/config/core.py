"""Core configuration classes for Open PTC Agent infrastructure.

This module defines pure data classes for core configuration:
- Daytona sandbox settings
- MCP server configurations
- Filesystem access settings
- Security settings
- Logging settings
"""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# Default security lists — used by SecurityConfig defaults and create_default_security_config()
DEFAULT_ALLOWED_IMPORTS = [
    "os", "sys", "json", "yaml", "requests", "datetime",
    "pathlib", "typing", "re", "math", "random", "time",
    "collections", "itertools", "functools", "subprocess", "shutil",
]

DEFAULT_BLOCKED_PATTERNS = [
    "eval(", "exec(", "__import__", "compile(", "globals(", "locals(",
]


class DaytonaConfig(BaseModel):
    """Daytona sandbox configuration.

    All fields have sensible defaults. Only api_key needs to be set
    (via DAYTONA_API_KEY environment variable).
    """

    api_key: str = ""  # Set via DAYTONA_API_KEY env var, validated later
    base_url: str = "https://app.daytona.io/api"
    auto_stop_interval: int = 3600  # 1 hour
    auto_archive_interval: int = 86400  # 1 day
    auto_delete_interval: int = 604800  # 7 days
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

    Defaults to the standard Daytona sandbox directories.

    Note: this validation is enforced for first-class filesystem tools only.
    """

    working_directory: str = "/home/daytona"
    allowed_directories: list[str] = Field(default_factory=lambda: ["/home/daytona", "/tmp"])
    denied_directories: list[str] = Field(default_factory=list)
    enable_path_validation: bool = True


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


class CoreConfig(BaseModel):
    """Core infrastructure configuration.

    Contains settings for sandbox, MCP servers, filesystem, security, and logging.
    LLM configuration is handled separately in src/config/agent.py.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Sub-configurations
    daytona: DaytonaConfig
    security: SecurityConfig
    mcp: MCPConfig
    logging: LoggingConfig
    filesystem: FilesystemConfig
    config_file_dir: Path | None = Field(default=None, exclude=True)

    def validate_api_keys(self) -> None:
        """Validate that required API keys are present.

        Raises:
            ValueError: If required API keys are missing
        """
        validate_daytona_api_key(self.daytona)


def create_default_security_config() -> SecurityConfig:
    """Create SecurityConfig with sensible defaults for Daytona sandbox execution."""
    return SecurityConfig()
