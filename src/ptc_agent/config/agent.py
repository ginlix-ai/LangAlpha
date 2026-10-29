"""Agent configuration management.

This module contains pure data classes for agent-specific configuration
that builds on top of the core configuration (sandbox, MCP).

Use src.config.loaders for file-based loading.
"""

import os
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypedDict

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ptc_agent.config.plugins import bundled_skill_dirs
from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    E2BConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    MCPServerConfig,
    SandboxConfig,
    SecurityConfig,
    create_default_security_config,
    detect_provider_kind,
    validate_sandbox_api_keys,
)

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel


class SubagentModel(TypedDict):
    """The model a subagent runs when it is not its launcher's: the catalog
    name and the provider serving it."""

    id: str
    provider: str


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
    summarization (Tier 2), and trimming of large tool args and stale Read
    results (Tier 1) at the start of a turn after a pause of
    ``truncate_args_idle_minutes``. The pause is meant to find the provider's
    prompt cache expired, so trimming the prefix costs no cache hit, where
    trimming mid-turn would throw a warm one away. Tier 1 is off by default
    (None): what it saves is negligible, and on Claude it invalidates the
    preserved thinking after what it hides.
    """

    enabled: bool = True
    token_threshold: int = 120000
    #: How much of the newest history a summary keeps word for word, in
    #: tokens (see ``keep_cutoff``). The same for every preset: a larger tail
    #: only brings the next summary sooner.
    keep_tokens: int = 20_000
    #: The same for a Claude model. Its tokenizer counts about 1.6 times
    #: the others' for the same text, in every language measured, so this
    #: keeps about the work ``keep_tokens`` keeps elsewhere.
    claude_keep_tokens: int = 32_000
    truncate_args_idle_minutes: int | None = None
    truncate_args_keep_messages: int = 20
    truncate_args_max_length: int = 2000

    #: Which named preset the numbers above came from, stamped by
    #: ``resolve_llm_config``. None means they are the deployment's own YAML
    #: values, which is the honest answer for a model that declares no context
    #: window. Reported, never read: the knobs are the behavior.
    profile: str | None = None

    #: Whether a model whose provider can compact the conversation itself does
    #: (see ``src.llms.provider_compaction``): the user's ``native_compaction``
    #: setting, set per turn by ``apply_compaction``. The ``provider_compaction``
    #: feature gates it when the middleware is built.
    native: bool = True

    #: Whether a model whose provider does not compact the conversation
    #: writes its own summary, from its cached request, with the transcript
    #: summarizer as the fallback (see ``CompactionMiddleware._writes_own``).
    self_summary: bool = True


# Named presets for when a thread compacts (token_threshold). Applied at
# request time in ``resolve_llm_config`` when the user selects a profile, and
# listed here in ascending order, which the settings pages draw them in.
#
# Thresholds leave headroom under common context windows: 100k-130k for 200k
# models, 200k for 400k models, 300k-750k for 1M models. ``economy`` stops
# short of 272k, past which OpenAI bills a whole request at long-context
# rates.
COMPACTION_PROFILES: dict[str, dict[str, int]] = {
    "aggressive": {"token_threshold": 100000},
    "moderate": {"token_threshold": 130000},
    "extended": {"token_threshold": 200000},
    "economy": {"token_threshold": 250000},
    "relaxed": {"token_threshold": 300000},
    "spacious": {"token_threshold": 400000},
    "expansive": {"token_threshold": 500000},
    "maximal": {"token_threshold": 750000},
}


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

    def local_skill_dirs(self) -> list[str]:
        """Return the ordered host-side skill sources.

        Precedence is last-wins (later sources override earlier ones).
        Order: bundled skills < user skills.

        Nothing here depends on the working directory. Both sources resolve
        from somewhere fixed — the bundles from the installed source, the
        operator's from their home — so a server started from anywhere finds
        the same skills, which a directory relative to ``cwd`` could not
        promise once the shipped skills moved into the bundles that declare
        them. Where they land is the sandbox's to say: the folder sits under
        each computer's own root, which no deployment-wide setting knows.
        """
        return [str(d) for d in host_skill_dirs(self.user_skills_dir)]


class SubagentConfig(BaseModel):
    """One subagent definition in the deployment's YAML.

    Named after a built-in, it changes only the fields it sets, as an overlay
    does, so the defaults below apply only to a subagent of the deployment's
    own, which also needs a ``description``.
    """

    description: str | None = None
    model: str | None = None
    role_prompt: str = ""
    role_prompt_template: str | None = None
    custom_prompt_template: str | None = None
    custom_prompt: str | None = None
    tools: list[str] = Field(default_factory=lambda: ["execute_code", "filesystem"])
    preload_skills: list[str] = Field(default_factory=list)
    sections: dict[str, bool] = Field(default_factory=dict)
    reasoning_effort: str | None = None
    search_provider: str | None = None
    search_depth: str | None = None
    connectors: bool = True
    brokerages: Literal["off", "on"] = "on"
    preferred_connectors: list[str] = Field(default_factory=list)
    # Unset is on for a subagent of the deployment's own.
    memory: bool | None = None

    @field_validator("reasoning_effort")
    @classmethod
    def _effort_is_a_level(cls, value: str | None) -> str | None:
        # A word outside the vocabulary would reach no model: the client falls
        # back to the model's default while the pane shows the word.
        from src.llms.reasoning import REASONING_LEVELS

        if value is not None and value not in REASONING_LEVELS:
            raise ValueError(f"must be one of {', '.join(REASONING_LEVELS)}")
        return value


class SubagentsConfig(BaseModel):
    """Subagents configuration block.

    ``enabled`` lists which subagents are active.
    ``definitions`` holds user-defined (or overridden) subagent configs.
    """

    enabled: list[str] = Field(default_factory=lambda: ["general-purpose"])
    definitions: dict[str, SubagentConfig] = Field(default_factory=dict)
    #: This user's merged roster (a ``ResolvedSubagents``), written on the
    #: per-turn copy by ``resolve_llm_config`` for a PTC build. None on the
    #: shared config and on builds that never resolved a user, which compile
    #: the deployment's own roster (``merge.turn_roster``).
    resolved: Any | None = Field(default=None, exclude=True)

    @field_validator("definitions")
    @classmethod
    def _new_subagents_have_descriptions(
        cls, value: dict[str, SubagentConfig]
    ) -> dict[str, SubagentConfig]:
        # The parent delegates by the description, and a built-in brings its own.
        from ptc_agent.agent.subagents.builtins import BUILTIN_SUBAGENTS

        for name, defn in value.items():
            if name not in BUILTIN_SUBAGENTS and not defn.description:
                raise ValueError(
                    f"Invalid subagent definition '{name}': a subagent that is "
                    "not built in needs a description"
                )
        return value


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
    background: str | None = None  # Background model, defaults to main llm if None
    compaction: str | None = None  # LLM for context compaction (summarization step)
    fetch: str | None = None  # LLM for web content extraction (fetch tool)
    fallback: list[str] | None = None  # Fallback model names for retry exhaustion

    @property
    def background_name(self) -> str:
        """The Background model, resolving the ``name`` fallback.

        Callers (metadata, tuning lookups) have to agree on which model that
        is, so the rule lives here rather than being spelled out at each site.
        """
        return self.background or self.name

    # Blank fetch means "whatever the Background model is for this turn".
    # Resolved on read, not at load: a user's Background preference replaces
    # ``background`` per turn, and a value baked in at load would keep the
    # deployment's instead. Blank compaction means the model the turn runs,
    # which a server turn writes in (``select_model``); read here it is the
    # primary model.
    @property
    def compaction_name(self) -> str | None:
        return self.compaction or self.name

    @property
    def fetch_name(self) -> str | None:
        return self.fetch or self.background

    def fallbacks_for(self, model: str | None) -> list[str]:
        """``fallback`` as a chain behind ``model``: without ``model`` itself
        and without repeats, in order.

        Either one sends the request that just failed back to the model that
        refused it, which costs a full attempt and can only fail the same way.
        """
        return list(dict.fromkeys(m for m in self.fallback or () if m and m != model))


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
    # Each subagent whose clients above run a model other than the turn's.
    # Admission records it on the run, because nothing downstream of the
    # build can still tell which model a run used.
    subagent_models: dict[str, SubagentModel] = Field(
        default_factory=dict, exclude=True
    )
    # None permits standalone name resolution; [] means resolution rejected all candidates.
    fallback_llm_clients: list[Any] | None = Field(default=None, exclude=True)
    # Display names aligned index-for-index with ``fallback_llm_clients``
    # (skipped fallbacks drop from both lists).
    fallback_llm_names: list[str] | None = Field(default=None, exclude=True)
    # Whether the fallbacks are a list the user saved rather than the
    # deployment's. Only the user's may move a call their own key or
    # subscription pays for onto the platform's key.
    fallback_users_choice: bool = Field(default=False, exclude=True)
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

        Optional - E2B (used when no Daytona key is set):
            E2B_API_KEY: Your E2B API key (https://e2b.dev/dashboard?tab=keys)
            E2B_DOMAIN / E2B_API_URL / E2B_SANDBOX_URL: self-hosted E2B only

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
        resolved_provider = (
            provider
            or os.getenv("SANDBOX_PROVIDER", "")
            or detect_provider_kind(daytona_api_key or "")
        )

        # Create Daytona config (required for daytona provider, defaults for others).
        # Defaults live on DaytonaConfig (src/ptc_agent/config/core.py) — the
        # single source of truth for Python-side values. agent_config.yaml is
        # the runtime SSoT and overrides these.
        if resolved_provider == "daytona":
            _daytona_defaults = DaytonaConfig.from_section(None)
            api_key = daytona_api_key or _daytona_defaults.api_key
            if not api_key:
                raise ValueError("DAYTONA_API_KEY must be provided or set in environment")
            daytona_config = DaytonaConfig(
                api_key=api_key,
                secret_namespace=_daytona_defaults.secret_namespace,
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

        e2b_config = E2BConfig.from_section(None)
        if resolved_provider == "e2b":
            e2b_config.require_api_key()

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

        skills_config = SkillsConfig(
            enabled=kwargs.pop("skills_enabled", True),
            user_skills_dir=kwargs.pop("user_skills_dir", "~/.ptc-agent/skills"),
        )

        # Wrap in SandboxConfig with resolved provider
        sandbox_config = SandboxConfig(
            provider=resolved_provider,
            daytona=daytona_config,
            e2b=e2b_config,
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

        For configs created via create(), only checks the sandbox provider key since
        the LLM client is passed directly with its own API key.

        For configs created via load_from_files(), LLM API key validation
        happens in the src/llms factory when get_llm_client() is called.

        Raises:
            ValueError: If required API keys are missing
        """
        validate_sandbox_api_keys(self.sandbox)

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
        ``.model_copy()`` so role-local mutation never touches the shared
        main client. With ``fallback_to_main=True`` and no role client,
        returns a copy of the main client when one exists.
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

        Every section is deep-copied, so a CoreConfig shares no mutable state
        with the AgentConfig it came from or with any sibling CoreConfig. One
        CoreConfig is one workspace's sandbox: sharing these by reference made
        a per-workspace change to the effective MCP server set, the resource
        tier, the working directory or the platform-secret version land on
        every other workspace in the process at the same time.
        """
        core_config = CoreConfig(
            sandbox=self.sandbox.model_copy(deep=True),
            security=self.security.model_copy(deep=True),
            mcp=self.mcp.model_copy(deep=True),
            logging=self.logging.model_copy(deep=True),
            filesystem=self.filesystem.model_copy(deep=True),
        )
        core_config.config_file_dir = self.config_file_dir
        return core_config
