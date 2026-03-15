"""Agent configuration management.

This module contains pure data classes for agent-specific configuration
that builds on top of the core configuration (sandbox, MCP).

Use src.config.loaders for file-based loading.
"""

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    MCPServerConfig,
    SecurityConfig,
    create_default_security_config,
    validate_daytona_api_key,
)

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel


class SummarizationConfig(BaseModel):
    """Conversation summarization settings."""

    enabled: bool = True
    token_threshold: int = 120000
    keep_messages: int = 5
    truncate_args_trigger_messages: int | None = None
    truncate_args_keep_messages: int = 20
    truncate_args_max_length: int = 2000


class FlashConfig(BaseModel):
    """Flash agent configuration.

    Flash agent is a lightweight agent optimized for speed
    """

    enabled: bool = True


class SkillsConfig(BaseModel):
    """Skills configuration for agent capabilities.

    Skills are markdown-based instruction files that extend agent capabilities.
    Each skill is a directory containing a SKILL.md file with YAML frontmatter.

    Resolution and precedence:
    - Skills are sourced from both user and project directories.
    - Project skills override user skills when names conflict.
    """

    enabled: bool = True
    user_skills_dir: str = "~/.ptc-agent/skills"
    project_skills_dir: str = (
        "skills"  # Project skills directory (relative to project root)
    )
    sandbox_skills_base: str = "/home/daytona/skills"  # Where skills live in sandbox

    def local_skill_dirs_with_sandbox(
        self, *, cwd: Path | None = None
    ) -> list[tuple[str, str]]:
        """Return ordered (local_dir, sandbox_dir) sources.

        Precedence is last-wins (later sources override earlier ones).
        Order: user skills < project skills (project wins on conflict).
        """
        base = cwd or Path.cwd()

        user_dir = str(Path(self.user_skills_dir).expanduser())
        project_dir = str((base / self.project_skills_dir).resolve())

        sources: list[tuple[str, str]] = [
            (user_dir, self.sandbox_skills_base),
            (project_dir, self.sandbox_skills_base),
        ]
        return sources


class SubagentConfig(BaseModel):
    """Configuration for a single subagent definition (built-in override or user-defined)."""

    description: str
    mode: Literal["ptc", "flash"] = "ptc"
    model: str | None = None
    role_prompt: str = ""
    role_prompt_template: str | None = None
    custom_prompt_template: str | None = None
    custom_prompt: str | None = None
    tools: list[str] = Field(default_factory=lambda: ["execute_code", "filesystem"])
    skills: list[str] = Field(default_factory=list)
    preload_skills: list[str] = Field(default_factory=list)
    max_iterations: int = 15
    sections: dict[str, bool] = Field(default_factory=dict)


class SubagentsConfig(BaseModel):
    """Subagents configuration block.

    ``enabled`` lists which subagents are active.
    ``definitions`` holds user-defined (or overridden) subagent configs.
    """

    enabled: list[str] = Field(default_factory=lambda: ["general-purpose"])
    definitions: dict[str, SubagentConfig] = Field(default_factory=dict)


class LLMDefinition(BaseModel):
    """Definition of an LLM for inline configuration in agent_config.yaml.

    This is used when an inline LLM definition is provided instead of
    referencing models.json by name. Primarily for advanced SDK usage.
    """

    model_id: str
    provider: str
    sdk: str  # e.g., "langchain_anthropic.ChatAnthropic"
    api_key_env: str  # Name of environment variable containing API key
    base_url: str | None = None
    output_version: str | None = None
    use_previous_response_id: bool | None = (
        False  # Use only for OpenAI responses api endpoint
    )
    parameters: dict[str, Any] = Field(default_factory=dict)


class LLMConfig(BaseModel):
    """LLM configuration - references an LLM from models.json."""

    name: str  # Name/alias from src/llms/manifest/models.json
    flash: str | None = None  # LLM for flash agent, defaults to main llm if None
    summarization: str | None = None  # LLM for conversation summarization
    fetch: str | None = None  # LLM for web content extraction (fetch tool)
    fallback: list[str] | None = None  # Fallback model names for retry exhaustion


class AgentConfig(BaseModel):
    """Agent-specific configuration.

    This config contains agent-related settings (LLM, security, logging)
    while using the core config for sandbox and MCP settings.
    """

    # Agent-specific configurations
    llm: LLMConfig
    security: SecurityConfig
    logging: LoggingConfig

    # Reference to core config (sandbox, MCP, filesystem)
    daytona: DaytonaConfig
    mcp: MCPConfig
    filesystem: FilesystemConfig

    # Skills configuration
    skills: SkillsConfig = Field(default_factory=SkillsConfig)

    # Flash agent configuration
    flash: FlashConfig = Field(default_factory=FlashConfig)

    # Vision tool configuration
    # If True, enable view_image tool for viewing images (requires vision-capable model)
    enable_view_image: bool = True

    # Subagent configuration
    subagents: SubagentsConfig = Field(default_factory=SubagentsConfig)

    # Summarization middleware configuration
    summarization: SummarizationConfig = Field(default_factory=SummarizationConfig)

    # Search API provider (tavily, bocha, serper)
    search_api: str = "tavily"

    # Background task configuration
    # If True, wait for background tasks to complete before returning to CLI
    # If False (default), return immediately and show status of running tasks
    background_auto_wait: bool = False

    # Note: deep-agent automatically enables middlewares (TodoList, Summarization, etc.)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Runtime data (not from config files)
    llm_definition: LLMDefinition | None = Field(default=None, exclude=True)
    llm_client: Any | None = Field(default=None, exclude=True)  # BaseChatModel instance
    config_file_dir: Path | None = Field(
        default=None, exclude=True
    )  # For path resolution

    @classmethod
    def create(
        cls,
        llm: "BaseChatModel",
        daytona_api_key: str | None = None,
        daytona_base_url: str = "https://app.daytona.io/api",
        mcp_servers: list[MCPServerConfig] | None = None,
        allowed_directories: list[str] | None = None,
        **kwargs: Any,
    ) -> "AgentConfig":
        """Create an AgentConfig with sensible defaults.

        Required:
            llm: A LangChain chat model instance (e.g., ChatAnthropic, ChatOpenAI)

        Required Environment Variables:
            DAYTONA_API_KEY: Your Daytona API key (get from https://app.daytona.io)
                            Or pass daytona_api_key directly.

        Optional - Daytona:
            daytona_api_key: Override DAYTONA_API_KEY env var
            daytona_base_url: API URL (default: "https://app.daytona.io/api")
            python_version: Python version in sandbox (default: "3.12")
            auto_stop_interval: Seconds before auto-stop (default: 3600)

        Optional - MCP:
            mcp_servers: List[MCPServerConfig] for additional tools (default: [])

        Optional - Security:
            max_execution_time: Max execution seconds (default: 300)
            max_code_length: Max code characters (default: 10000)
            allowed_imports: List of allowed Python modules
            blocked_patterns: List of blocked code patterns

        Optional - Other:
            log_level: Logging level (default: "INFO")
            allowed_directories: Sandbox paths (default: ["/home/daytona", "/tmp"])
            subagents: SubagentsConfig or use subagents_enabled for backward compat
            enable_view_image: Enable image viewing (default: True)
            background_auto_wait: Wait for background tasks (default: False)

        Returns:
            Configured AgentConfig instance

        Example (minimal):
            from langchain_anthropic import ChatAnthropic

            llm = ChatAnthropic(model="claude-sonnet-4-20250514")
            config = AgentConfig.create(llm=llm)

        Example (with MCP servers):
            from langchain_anthropic import ChatAnthropic
            from ptc_agent.config import MCPServerConfig

            llm = ChatAnthropic(model="claude-sonnet-4-20250514")
            config = AgentConfig.create(
                llm=llm,
                mcp_servers=[
                    MCPServerConfig(
                        name="tavily",
                        command="npx",
                        args=["-y", "tavily-mcp@latest"],
                        env={"TAVILY_API_KEY": os.getenv("TAVILY_API_KEY", "")},
                    ),
                ],
            )
        """
        # Create LLM config (placeholder for file-based loading compatibility)
        llm_config = LLMConfig(name="custom")

        # Create Daytona config with defaults
        api_key = daytona_api_key or os.getenv("DAYTONA_API_KEY", "")
        if not api_key:
            raise ValueError("DAYTONA_API_KEY must be provided or set in environment")
        daytona_config = DaytonaConfig(
            api_key=api_key,
            base_url=daytona_base_url,
            auto_stop_interval=kwargs.pop("auto_stop_interval", 3600),
            auto_archive_interval=kwargs.pop("auto_archive_interval", 86400),
            auto_delete_interval=kwargs.pop("auto_delete_interval", 604800),
            python_version=kwargs.pop("python_version", "3.12"),
            snapshot_enabled=kwargs.pop("snapshot_enabled", True),
            snapshot_name=kwargs.pop("snapshot_name", None),
            snapshot_auto_create=kwargs.pop("snapshot_auto_create", True),
        )

        # Create Security config with defaults
        security_defaults = create_default_security_config()
        security_config = SecurityConfig(
            max_execution_time=kwargs.pop(
                "max_execution_time", security_defaults.max_execution_time
            ),
            max_code_length=kwargs.pop(
                "max_code_length", security_defaults.max_code_length
            ),
            max_file_size=kwargs.pop("max_file_size", security_defaults.max_file_size),
            enable_code_validation=kwargs.pop(
                "enable_code_validation", security_defaults.enable_code_validation
            ),
            allowed_imports=kwargs.pop(
                "allowed_imports", list(security_defaults.allowed_imports)
            ),
            blocked_patterns=kwargs.pop(
                "blocked_patterns", list(security_defaults.blocked_patterns)
            ),
        )

        # Create MCP config
        mcp_config = MCPConfig(
            servers=mcp_servers or [],
            tool_discovery_enabled=kwargs.pop("tool_discovery_enabled", True),
            lazy_load=kwargs.pop("lazy_load", True),
            tool_exposure_mode=kwargs.pop("tool_exposure_mode", "summary"),
        )

        # Create Logging config
        logging_config = LoggingConfig(
            level=kwargs.pop("log_level", "INFO"),
            file=kwargs.pop("log_file", "logs/ptc.log"),
        )

        # Create Filesystem config
        filesystem_config = FilesystemConfig(
            working_directory=kwargs.pop("working_directory", "/home/daytona"),
            allowed_directories=allowed_directories or ["/home/daytona", "/tmp"],
            enable_path_validation=kwargs.pop("enable_path_validation", True),
        )

        # Create Skills config
        skills_config = SkillsConfig(
            enabled=kwargs.pop("skills_enabled", True),
            user_skills_dir=kwargs.pop("user_skills_dir", "~/.ptc-agent/skills"),
            project_skills_dir=kwargs.pop("project_skills_dir", "skills"),
            sandbox_skills_base=kwargs.pop(
                "sandbox_skills_base", "/home/daytona/skills"
            ),
        )

        # Create the config
        config = cls(
            llm=llm_config,
            daytona=daytona_config,
            security=security_config,
            mcp=mcp_config,
            logging=logging_config,
            filesystem=filesystem_config,
            skills=skills_config,
            enable_view_image=kwargs.pop("enable_view_image", True),
            subagents=SubagentsConfig(
                enabled=kwargs.pop("subagents_enabled", ["general-purpose"]),
                definitions=kwargs.pop("subagents_definitions", {}),
            ),
            background_auto_wait=kwargs.pop("background_auto_wait", False),
        )

        # Set runtime data - store the LLM client directly
        config.llm_client = llm

        return config

    def validate_api_keys(self) -> None:
        """Validate that required API keys are present.

        For configs created via create(), only checks DAYTONA_API_KEY since
        the LLM client is passed directly with its own API key.

        For configs created via load_from_files(), LLM API key validation
        happens in the src/llms factory when get_llm_client() is called.

        Raises:
            ValueError: If required API keys are missing
        """
        validate_daytona_api_key(self.daytona)

    def get_llm_client(self) -> "BaseChatModel":
        """Return the LLM client instance.

        For configs created via create(), returns the stored llm_client.
        For configs created via load_from_files(), uses src/llms factory.

        Returns:
            LangChain LLM client instance

        Raises:
            ValueError: If LLM name is not configured or not found in models.json
        """
        # If LLM client was passed directly (via create()), return it
        if self.llm_client is not None:
            return self.llm_client

        # Use src/llms factory for file-based loading
        from src.llms import create_llm

        return create_llm(self.llm.name)

    def to_core_config(self) -> CoreConfig:
        """Convert to CoreConfig for use with SessionManager.

        Returns:
            CoreConfig instance with sandbox/MCP settings
        """
        core_config = CoreConfig(
            daytona=self.daytona,
            security=self.security,
            mcp=self.mcp,
            logging=self.logging,
            filesystem=self.filesystem,
        )
        core_config.config_file_dir = self.config_file_dir
        return core_config
