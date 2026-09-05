"""Agent configuration management.

This module contains pure data classes for agent-specific configuration
that builds on top of the core configuration (sandbox, MCP).

Use src.config.loaders for file-based loading.
"""

import os
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from ptc_agent.config.plugins import bundled_skill_dirs
from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    MCPServerConfig,
    SandboxConfig,
    SecurityConfig,
    create_default_security_config,
    validate_daytona_api_key,
)

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel


class CredentialSource(StrEnum):
    """Which credential actually produced an LLM client.

    Distinct from ``ModelSource`` (server-side, classifies the model as
    SYSTEM/CUSTOM/UNKNOWN). Set by ``resolve_llm_config`` to record the
    credential that built ``AgentConfig.llm_client``.
    """

    OAUTH = "oauth"
    BYOK = "byok"
    PLATFORM = "platform"
    NONE = "none"


class CompactionConfig(BaseModel):
    """Context compaction settings.

    Controls the two-tier context window lifecycle: token-threshold-based LLM
    summarization (Tier 2) and message-count-based tool-arg truncation (Tier 1).
    """

    enabled: bool = True
    token_threshold: int = 120000
    keep_messages: int = 5
    truncate_args_trigger_messages: int | None = None
    truncate_args_keep_messages: int = 20
    truncate_args_max_length: int = 2000

    #: Which named preset the numbers above came from, stamped by
    #: ``resolve_llm_config``. None means they are the deployment's own YAML
    #: values, which is the honest answer for a model that declares no context
    #: window. Reported, never read: the knobs are the behavior.
    profile: str | None = None


# Named presets that bundle the three user-facing compaction knobs
# (token_threshold, truncate_args_trigger_messages, keep_messages). Applied at
# request time in ``resolve_llm_config`` when the user selects a profile.
#
# Thresholds are chosen to leave healthy headroom under common model context
# windows: 100k (<=200k models), 130k (200k models), 200k (400k/1M models),
# 300k (1M models). The other two knobs scale with the threshold so that
# relaxed profiles also keep more recent history and truncate tool args later.
COMPACTION_PROFILES: dict[str, dict[str, int]] = {
    "aggressive": {
        "token_threshold": 100000,
        "truncate_args_trigger_messages": 30,
        "keep_messages": 5,
    },
    "moderate": {
        "token_threshold": 130000,
        "truncate_args_trigger_messages": 40,
        "keep_messages": 8,
    },
    "extended": {
        "token_threshold": 200000,
        "truncate_args_trigger_messages": 60,
        "keep_messages": 10,
    },
    "relaxed": {
        "token_threshold": 300000,
        "truncate_args_trigger_messages": 70,
        "keep_messages": 15,
    },
}


class FlashConfig(BaseModel):
    """Flash agent configuration.

    Flash agent is a lightweight agent optimized for speed
    """

    enabled: bool = True


#: The operator's own drop-in directory, and the one place to override a
#: shipped skill. Absolute rather than relative to the working directory, so it
#: means the same thing whether the server was started from the repo or from
#: anywhere else.
DEFAULT_USER_SKILLS_DIR = "~/.ptc-agent/skills"


def host_skill_dirs(user_skills_dir: str = DEFAULT_USER_SKILLS_DIR) -> list[Path]:
    """Every host-side skill source, in the order that resolves them.

    Last wins, so the operator's directory comes after the bundles. Two
    readers that never build a config -- skill-name reservation and the
    content route -- have to name the same sources in the same order as
    delivery does, because both answer questions about the file the agent will
    actually load.
    """
    return [*bundled_skill_dirs(), Path(user_skills_dir).expanduser()]


class SkillsConfig(BaseModel):
    """Skills configuration for agent capabilities.

    Skills are markdown-based instruction files that extend agent capabilities.
    Each skill is a directory containing a SKILL.md file with YAML frontmatter.

    Resolution and precedence:
    - Skills are sourced from the shipped bundles, then the user directory.
    - A later source overrides an earlier one when names conflict, so an
      operator can replace a shipped skill by dropping one of the same name
      into ``user_skills_dir``.
    """

    enabled: bool = True
    #: See ``DEFAULT_USER_SKILLS_DIR``.
    user_skills_dir: str = DEFAULT_USER_SKILLS_DIR
    sandbox_skills_base: str = "/home/workspace/.agents/skills"  # Where skills live in sandbox

    def local_skill_dirs_with_sandbox(self) -> list[tuple[str, str]]:
        """Return ordered (local_dir, sandbox_dir) sources.

        Precedence is last-wins (later sources override earlier ones).
        Order: bundled skills < user skills.

        Nothing here depends on the working directory. Both sources resolve
        from somewhere fixed — the bundles from the installed source, the
        operator's from their home — so a server started from anywhere finds
        the same skills, which a directory relative to ``cwd`` could not
        promise once the shipped skills moved into the bundles that declare
        them.
        """
        return [
            (str(d), self.sandbox_skills_base)
            for d in host_skill_dirs(self.user_skills_dir)
        ]


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
    compaction: str | None = None  # LLM for context compaction (summarization step)
    fetch: str | None = None  # LLM for web content extraction (fetch tool)
    fallback: list[str] | None = None  # Fallback model names for retry exhaustion

    @property
    def flash_name(self) -> str:
        """The model a flash turn actually runs on, resolving the ``name`` fallback.

        Callers outside the flash agent (metadata, tuning lookups) have to agree
        with it on which model that is, so the rule lives here rather than being
        spelled out at each site.
        """
        return self.flash or self.name


class AgentConfig(BaseModel):
    """Agent-specific configuration.

    This config contains agent-related settings (LLM, security, logging)
    while using the core config for sandbox and MCP settings.
    """

    # Agent-specific configurations
    llm: LLMConfig | None = None
    security: SecurityConfig
    logging: LoggingConfig

    # Reference to core config (sandbox, MCP, filesystem)
    sandbox: SandboxConfig
    mcp: MCPConfig
    filesystem: FilesystemConfig

    # Skills configuration
    skills: SkillsConfig = Field(default_factory=SkillsConfig)

    # Flash agent configuration
    flash: FlashConfig = Field(default_factory=FlashConfig)

    # Custom model input modalities override (set by resolve_llm_config for custom models)
    input_modalities: list[str] | None = None

    # Subagent configuration
    subagents: SubagentsConfig = Field(default_factory=SubagentsConfig)

    # Compaction middleware configuration
    compaction: CompactionConfig = Field(default_factory=CompactionConfig)

    #: How much prompt scaffolding the main model gets, resolved per model and
    #: stamped by ``resolve_llm_config``, the twin of ``CompactionConfig
    #: .profile``. None means nobody resolved it, and the agents fall back to
    #: what the deployment and the manifest declare.
    prompt_guidance: str | None = None

    # Search API provider (tavily, serper, bocha, exa, parallel)
    search_api: str = "tavily"

    # Search depth level name from the provider's manifest entry
    # (src/tools/manifest/web_providers.json). Unknown levels fall back to
    # the provider's default level.
    search_depth: str = "standard"

    # Background task configuration
    # If True, wait for background tasks to complete before returning to CLI
    # If False (default), return immediately and show status of running tasks
    background_auto_wait: bool = False

    # Note: deep-agent automatically enables middlewares (TodoList, Compaction, etc.)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def daytona(self) -> DaytonaConfig:
        """Backward-compat shim: config.daytona -> config.sandbox.daytona."""
        return self.sandbox.daytona

    def feature_enabled(self, key: str) -> bool:
        """Effective feature flag for this build: the per-user value resolved
        by ``resolve_llm_config`` when present, else the no-user default
        (opt_in and platform plan gates resolve False)."""
        if self.features is not None and key in self.features:
            return self.features[key]
        from src.config.features import default_feature_enabled

        return default_feature_enabled(key)

    # Runtime data (not from config files)
    llm_definition: LLMDefinition | None = Field(default=None, exclude=True)
    llm_client: Any | None = Field(default=None, exclude=True)  # BaseChatModel instance
    # Which credential produced ``llm_client``; set by ``resolve_llm_config``.
    credential_source: CredentialSource = Field(
        default=CredentialSource.NONE, exclude=True
    )
    subsidiary_llm_clients: dict[str, Any] = Field(default_factory=dict, exclude=True)
    # Scaffolding level per role key, for the model that role actually runs.
    # Written alongside the clients above, because a role without a client of
    # its own still runs a model: by name, or by inheriting the main one.
    role_prompt_guidance: dict[str, str] = Field(default_factory=dict, exclude=True)
    fallback_llm_clients: list[Any] | None = Field(default=None, exclude=True)  # Pre-resolved fallback instances
    # Display names aligned index-for-index with ``fallback_llm_clients``
    # (skipped fallbacks drop from both lists).
    fallback_llm_names: list[str] | None = Field(default=None, exclude=True)
    # Forwarded by ``get_llm_client()`` to ``create_llm(cache_key=...)`` for
    # the lazy factory path.
    cache_key: str | None = Field(default=None, exclude=True)
    # Per-user resolved feature flags, set by ``resolve_llm_config``. None
    # (entry points that skip resolution) falls back to system defaults.
    features: dict[str, bool] | None = Field(default=None, exclude=True)
    # Per-user skill tier, set by ``resolve_llm_config``: uploaded skill specs
    # (duck-typed name/description/command), the host dir materializing their
    # bodies, and builtin skill names the user disabled. Not to be confused
    # with ``SkillsConfig.user_skills_dir``, a config-file path.
    user_skills: list[Any] = Field(default_factory=list, exclude=True)
    disabled_skills: frozenset[str] = Field(default_factory=frozenset, exclude=True)
    user_skill_dir: str | None = Field(default=None, exclude=True)
    # Workspace-tier bodies: a second host dir, because the sandbox delivery
    # path uploads ``user_skill_dir`` wholesale and must not touch these.
    workspace_skill_dir: str | None = Field(default=None, exclude=True)
    # Platform-skill command renames (skill name → alias); the alias replaces
    # the registry command as the slash trigger for this user.
    skill_command_overrides: dict[str, str] = Field(
        default_factory=dict, exclude=True
    )
    config_file_dir: Path | None = Field(
        default=None, exclude=True
    )  # For path resolution

    @classmethod
    def create(
        cls,
        llm: "BaseChatModel",
        provider: str | None = None,
        daytona_api_key: str | None = None,
        daytona_base_url: str = "https://app.daytona.io/api",
        mcp_servers: list[MCPServerConfig] | None = None,
        allowed_directories: list[str] | None = None,
        **kwargs: Any,
    ) -> "AgentConfig":
        """Create an AgentConfig with sensible defaults.

        Required:
            llm: A LangChain chat model instance (e.g., ChatAnthropic, ChatOpenAI)

        Required Environment Variables (Daytona provider only):
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
            allowed_directories: Sandbox paths (default: ["/home/workspace", "/tmp"])
            subagents: SubagentsConfig or use subagents_enabled for backward compat
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

        # Resolve provider: explicit > env var > auto-detect from API key presence
        resolved_provider = provider or os.getenv("SANDBOX_PROVIDER", "")
        if not resolved_provider:
            resolved_provider = (
                "daytona"
                if (daytona_api_key or os.getenv("DAYTONA_API_KEY"))
                else "docker"
            )

        # Create Daytona config (required for daytona provider, defaults for others).
        # Defaults live on DaytonaConfig (src/ptc_agent/config/core.py) — the
        # single source of truth for Python-side values. agent_config.yaml is
        # the runtime SSoT and overrides these.
        if resolved_provider == "daytona":
            api_key = daytona_api_key or os.getenv("DAYTONA_API_KEY", "")
            if not api_key:
                raise ValueError("DAYTONA_API_KEY must be provided or set in environment")
            _daytona_defaults = DaytonaConfig()
            daytona_config = DaytonaConfig(
                api_key=api_key,
                secret_namespace=os.getenv("DAYTONA_SECRET_NAMESPACE", ""),
                base_url=daytona_base_url,
                auto_stop_interval=kwargs.pop(
                    "auto_stop_interval", _daytona_defaults.auto_stop_interval
                ),
                auto_archive_interval=kwargs.pop(
                    "auto_archive_interval", _daytona_defaults.auto_archive_interval
                ),
                auto_delete_interval=kwargs.pop(
                    "auto_delete_interval", _daytona_defaults.auto_delete_interval
                ),
                python_version=kwargs.pop(
                    "python_version", _daytona_defaults.python_version
                ),
                snapshot_enabled=kwargs.pop(
                    "snapshot_enabled", _daytona_defaults.snapshot_enabled
                ),
                snapshot_name=kwargs.pop(
                    "snapshot_name", _daytona_defaults.snapshot_name
                ),
                snapshot_auto_create=kwargs.pop(
                    "snapshot_auto_create", _daytona_defaults.snapshot_auto_create
                ),
            )
        else:
            # Non-Daytona providers don't need Daytona config; use defaults
            daytona_config = DaytonaConfig()

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

        # Create Filesystem config — allowed/denied dirs derive from working_directory
        _fs_defaults = FilesystemConfig()
        filesystem_config = FilesystemConfig(
            working_directory=kwargs.pop("working_directory", _fs_defaults.working_directory),
            allowed_directories=allowed_directories or None,  # None → derived from working_directory
            enable_path_validation=kwargs.pop("enable_path_validation", True),
        )

        # Create Skills config (derive sandbox_skills_base from filesystem working_directory)
        skills_config = SkillsConfig(
            enabled=kwargs.pop("skills_enabled", True),
            user_skills_dir=kwargs.pop("user_skills_dir", "~/.ptc-agent/skills"),
            sandbox_skills_base=kwargs.pop(
                "sandbox_skills_base",
                f"{filesystem_config.working_directory}/.agents/skills",
            ),
        )

        # Wrap in SandboxConfig with resolved provider
        sandbox_config = SandboxConfig(
            provider=resolved_provider,
            daytona=daytona_config,
        )

        # Create the config
        config = cls(
            llm=llm_config,
            sandbox=sandbox_config,
            security=security_config,
            mcp=mcp_config,
            logging=logging_config,
            filesystem=filesystem_config,
            skills=skills_config,
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
        if self.sandbox.provider == "daytona":
            validate_daytona_api_key(self.sandbox.daytona)

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

        if self.llm is None:
            raise ValueError(
                "No LLM configured. Set llm in agent_config.yaml or configure a model in the setup wizard."
            )

        # Use src/llms factory for file-based loading. A name not in
        # models.json reaches this guard either because the user picked a
        # custom model without a resolvable BYOK key, or because the name
        # is a typo. Raise a neutral error instead of the generic factory one.
        from src.llms import create_llm
        from src.llms.llm import ensure_model_in_manifest

        ensure_model_in_manifest(self.llm.name)
        return create_llm(self.llm.name, cache_key=self.cache_key)

    def client_for_role(self, role: str, *, fallback_to_main: bool = False):
        """Return the pre-resolved client for a role, or None.

        Roles: "compaction", "fetch", "subagent:<name>". Returns a
        ``.model_copy()`` so role-local mutation (e.g. compaction setting
        ``streaming=False``) never touches the shared main client. With
        ``fallback_to_main=True`` and no role client, returns a copy of the
        main client when one exists.
        """
        c = self.subsidiary_llm_clients.get(role)
        if c is not None:
            return c.model_copy()
        if not fallback_to_main:
            return None
        main = self.llm_client
        return main.model_copy() if main is not None else None

    def prompt_guidance_for_role(self, role: str) -> str | None:
        """Scaffolding level for the model this role actually runs.

        Pinned roles run their own model, so sizing their prompt for the main
        one is the drift ``resolve_llm_config`` records this to prevent.
        """
        return self.role_prompt_guidance.get(role) or self.prompt_guidance

    def to_core_config(self) -> CoreConfig:
        """Convert to CoreConfig for use with SessionManager.

        Returns:
            CoreConfig instance with sandbox/MCP settings
        """
        core_config = CoreConfig(
            sandbox=self.sandbox,
            security=self.security,
            # Deep-copy the MCP config so each CoreConfig (hence each workspace
            # sandbox) owns its MCPConfig. Sharing it by reference made every
            # workspace's effective server set the same object — Phase 2 swaps
            # in per-workspace servers, which must not bleed across workspaces.
            mcp=self.mcp.model_copy(deep=True),
            logging=self.logging,
            filesystem=self.filesystem,
        )
        core_config.config_file_dir = self.config_file_dir
        return core_config
