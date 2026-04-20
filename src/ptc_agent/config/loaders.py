"""Configuration loaders for file-based config.

This module provides functions to load AgentConfig and CoreConfig from files.

Usage:
    # File-based loading (CLI, LangGraph)
    from ptc_agent.config import load_from_files
    config = await load_from_files()

Config Search Paths:
    When no explicit path is provided, files are searched in order:
    1. Current working directory
    2. Project root (git repository root)
    3. ~/.ptc-agent/ (user config directory)

    Environment variable overrides:
    - PTC_CONFIG_FILE: explicit path to agent_config.yaml

LLM Configuration:
    LLM models are configured by name in agent_config.yaml and resolved
    at runtime via src/llms/create_llm() using models.json.
"""

import asyncio
from pathlib import Path
from typing import Any

from ptc_agent.config.agent import (
    AgentConfig,
    CompactionConfig,
    FlashConfig,
    LLMConfig,
    SkillsConfig,
    SubagentConfig,
    SubagentsConfig,
)
from ptc_agent.config.core import CoreConfig, create_default_security_config
from ptc_agent.config.utils import (
    configure_structlog,
    create_filesystem_config,
    create_logging_config,
    create_mcp_config,
    create_sandbox_config,
    load_dotenv_async,
    validate_required_sections,
)
from ptc_agent.config.file_utils import (
    AGENT_CONFIG_FILE,
    ConfigContext,
    find_config_file,
    get_config_search_paths,
    get_default_config_dir,
    load_yaml_config,
)


async def _resolve_config_file(
    config_file: Path | None,
    *,
    search_paths: bool,
    context: ConfigContext,
    auto_generate: bool = False,
) -> tuple[Path, dict[str, Any]]:
    """Find and load agent_config.yaml, returning the path and parsed data.

    Raises:
        FileNotFoundError: If agent_config.yaml is not found
    """
    cwd = await asyncio.to_thread(Path.cwd)

    if config_file is None:
        if search_paths:
            config_file = await asyncio.to_thread(
                find_config_file,
                AGENT_CONFIG_FILE,
                None,
                "PTC_CONFIG_FILE",
                context=context,
            )
        else:
            config_file = cwd / AGENT_CONFIG_FILE

    # Auto-generate if missing and requested
    if (config_file is None or not config_file.exists()) and auto_generate:
        generated = generate_config_template(
            get_default_config_dir(), include_llms=False
        )
        config_file = generated["agent_config.yaml"]

    if config_file is None or not config_file.exists():
        searched = (
            await asyncio.to_thread(get_config_search_paths, None, context=context)
            if search_paths
            else [cwd]
        )
        raise FileNotFoundError(
            f"agent_config.yaml not found in search paths:\n"
            f"  {chr(10).join(str(p) for p in searched)}\n"
            f"Create one or set PTC_CONFIG_FILE environment variable."
        )

    config_data = await asyncio.to_thread(load_yaml_config, str(config_file))
    return config_file, config_data


async def load_from_files(
    config_file: Path | None = None,
    env_file: Path | None = None,
    *,
    search_paths: bool = True,
    context: ConfigContext = ConfigContext.SDK,
    auto_generate: bool = False,
) -> AgentConfig:
    """Load AgentConfig from config files (agent_config.yaml, .env).

    Search order depends on context:
    - SDK: CWD → git root → ~/.ptc-agent/
    - CLI: ~/.ptc-agent/ → CWD (home first)

    Environment variable overrides:
    - PTC_CONFIG_FILE: explicit path to agent_config.yaml

    LLM models are specified by name in agent_config.yaml and resolved
    at runtime via src/llms/create_llm() using models.json.

    Args:
        config_file: Optional path to agent_config.yaml file
        env_file: Optional path to .env file
        search_paths: If True, search multiple paths for config files
        context: Loading context (SDK or CLI)
        auto_generate: If True, generate config at ~/.ptc-agent/ when not found

    Returns:
        Configured AgentConfig instance

    Raises:
        FileNotFoundError: If agent_config.yaml is not found
        ValueError: If required configuration is missing or invalid
        KeyError: If required fields are missing from config files
    """
    await load_dotenv_async(env_file)
    config_file, config_data = await _resolve_config_file(
        config_file,
        search_paths=search_paths,
        context=context,
        auto_generate=auto_generate,
    )

    config = load_from_dict(config_data)
    config.config_file_dir = config_file.parent if config_file else None
    return config


async def load_core_from_files(
    config_file: Path | None = None,
    env_file: Path | None = None,
    *,
    search_paths: bool = True,
    context: ConfigContext = ConfigContext.SDK,
) -> CoreConfig:
    """Load CoreConfig from config files (agent_config.yaml, .env).

    Search order depends on context:
    - SDK: CWD → git root → ~/.ptc-agent/
    - CLI: ~/.ptc-agent/ → CWD (home first)

    Args:
        config_file: Optional path to agent_config.yaml file
        env_file: Optional path to .env file
        search_paths: If True, search multiple paths for config files
        context: Loading context (SDK or CLI)

    Returns:
        Configured CoreConfig instance

    Raises:
        FileNotFoundError: If agent_config.yaml is not found
        ValueError: If required configuration is missing or invalid
        KeyError: If required fields are missing from config files
    """
    await load_dotenv_async(env_file)
    config_file, config_data = await _resolve_config_file(
        config_file,
        search_paths=search_paths,
        context=context,
    )

    # Accept either "sandbox" or "daytona" as the sandbox config key
    has_sandbox_key = "sandbox" in config_data or "daytona" in config_data
    required_sections = ["mcp", "logging", "filesystem"]
    validate_required_sections(config_data, required_sections)
    if not has_sandbox_key:
        raise ValueError(
            "Missing required sections in agent_config.yaml: sandbox (or daytona)\n"
            "Please add these sections to your agent_config.yaml file."
        )

    sandbox_config = create_sandbox_config(config_data)
    security_config = create_default_security_config()
    mcp_config = create_mcp_config(config_data["mcp"])
    logging_config = create_logging_config(config_data["logging"])

    filesystem_config = create_filesystem_config(config_data["filesystem"])

    core_config = CoreConfig(
        sandbox=sandbox_config,
        security=security_config,
        mcp=mcp_config,
        logging=logging_config,
        filesystem=filesystem_config,
    )
    core_config.config_file_dir = config_file.parent if config_file else None
    return core_config


def load_from_dict(
    config_data: dict[str, Any],
) -> AgentConfig:
    """Create AgentConfig from a dictionary (e.g., parsed YAML).

    This allows creating config from any dict source, not just files.

    LLM is specified by name in the config and resolved at runtime via
    src/llms/create_llm() using models.json.

    Args:
        config_data: Configuration dictionary (same structure as agent_config.yaml)

    Returns:
        Configured AgentConfig instance

    Raises:
        ValueError: If required configuration is missing or invalid
    """
    # Validate that all required sections exist
    # Accept either "sandbox" or "daytona" as the sandbox config key
    has_sandbox_key = "sandbox" in config_data or "daytona" in config_data
    required_sections = ["mcp", "logging", "filesystem"]
    validate_required_sections(config_data, required_sections)
    if not has_sandbox_key:
        raise ValueError(
            "Missing required sections in agent_config.yaml: sandbox (or daytona)\n"
            "Please add these sections to your agent_config.yaml file."
        )

    # Load LLM configuration - extract name and flash LLM
    # llm: null (or omitted) is allowed — users configure models via the UI instead.
    llm_data = config_data.get("llm")

    llm_config: LLMConfig | None = None
    if isinstance(llm_data, str):
        # Simple string format: "claude-sonnet-4-5"
        llm_config = LLMConfig(name=llm_data)
    elif isinstance(llm_data, dict):
        llm_name = llm_data.get("name", "")
        if not llm_name:
            raise ValueError("llm.name is required in agent_config.yaml when llm is a dict")
        llm_config = LLMConfig(
            name=llm_name,
            flash=llm_data.get("flash"),
            compaction=llm_data.get("compaction"),
            fetch=llm_data.get("fetch"),
            fallback=llm_data.get("fallback"),
        )
    elif llm_data is not None:
        raise ValueError(
            "llm section must be null, a string (LLM name), or dict with 'name' key"
        )

    # Load configurations using shared factory functions
    sandbox_config = create_sandbox_config(config_data)
    security_config = create_default_security_config()
    mcp_config = create_mcp_config(config_data["mcp"])
    logging_config = create_logging_config(config_data["logging"])

    filesystem_config = create_filesystem_config(config_data["filesystem"])

    # Configure structlog to respect the log level from config
    configure_structlog(logging_config.level)

    # Load Agent configuration (optional section)
    # Note: YAML sections with only comments parse as None, not {}
    agent_data = config_data.get("agent") or {}
    enable_view_image = agent_data.get("enable_view_image", True)
    background_auto_wait = agent_data.get("background_auto_wait", False)

    # Load Subagent configuration (optional section)
    subagents_data = config_data.get("subagents") or {}
    subagents_enabled = subagents_data.get("enabled", ["general-purpose"])
    subagent_definitions: dict[str, SubagentConfig] = {}
    for name, defn_data in (subagents_data.get("definitions") or {}).items():
        try:
            subagent_definitions[name] = SubagentConfig(**defn_data)
        except Exception as e:
            raise ValueError(f"Invalid subagent definition '{name}': {e}") from e

    # Validate enabled subagent names exist in builtins or user definitions
    from ptc_agent.agent.subagents.builtins import BUILTIN_SUBAGENTS

    known_names = set(BUILTIN_SUBAGENTS) | set(subagent_definitions)
    for name in subagents_enabled:
        if name not in known_names:
            available = ", ".join(sorted(known_names))
            raise ValueError(
                f"Subagent '{name}' in 'enabled' list but not defined. "
                f"Available: [{available}]"
            )

    subagents_config = SubagentsConfig(
        enabled=subagents_enabled,
        definitions=subagent_definitions,
    )

    # Load Skills configuration (optional section)
    skills_data = config_data.get("skills") or {}
    skills_config = SkillsConfig(
        enabled=skills_data.get("enabled", True),
        user_skills_dir=skills_data.get("user_skills_dir", "~/.ptc-agent/skills"),
        project_skills_dir=skills_data.get("project_skills_dir", "skills"),
        sandbox_skills_base=skills_data.get(
            "sandbox_skills_base", f"{filesystem_config.working_directory}/.agents/skills"
        ),
    )

    # Load Flash configuration (optional section)
    # Flash LLM is now in llm.flash; compaction uses main compaction config
    flash_data = config_data.get("flash") or {}
    flash_config = FlashConfig(
        enabled=flash_data.get("enabled", True),
    )

    # Load Compaction configuration (optional section)
    compaction_data = config_data.get("compaction") or {}
    compaction_config = CompactionConfig(**compaction_data) if compaction_data else CompactionConfig()

    # Search API provider
    search_api = config_data.get("search_api", "tavily")

    # Create config object
    config = AgentConfig(
        llm=llm_config,
        security=security_config,
        logging=logging_config,
        sandbox=sandbox_config,
        mcp=mcp_config,
        filesystem=filesystem_config,
        skills=skills_config,
        flash=flash_config,
        enable_view_image=enable_view_image,
        subagents=subagents_config,
        compaction=compaction_config,
        search_api=search_api,
        background_auto_wait=background_auto_wait,
    )

    return config


# =============================================================================
# Config Template Generation
# =============================================================================


CONFIG_TEMPLATE = """# PTC Agent Configuration
# Place this file in ~/.ptc-agent/agent_config.yaml or your project root

# CLI Configuration (for ptc-cli)
# --------------------------------
cli:
  theme: "auto"  # auto, dark, light
  # palette: "nord"  # emerald, cyan, amber, teal, nord, gruvbox, catppuccin, tokyo_night

# LLM Configuration
# -----------------
# Model name from src/llms/manifest/models.json
llm:
  name: "your-model-name"       # Required: primary model from models.json
  # flash: "your-flash-model"   # Optional: model for flash agent
  # compaction: "your-model"    # Optional: model for context compaction
  # fetch: "your-model"         # Optional: model for web content extraction
  # fallback:                   # Optional: fallback models when primary fails
  #   - "fallback-model"

# Daytona Sandbox
# ---------------
daytona:
  base_url: "https://app.daytona.io/api"
  # api_key: set DAYTONA_API_KEY in environment or .env file
  python_version: "3.12"
  auto_stop_interval: 3600  # 1 hour
  auto_archive_interval: 86400  # 24 hours
  auto_delete_interval: 604800  # 7 days

# MCP Servers (optional)
# ----------------------
mcp:
  servers: []
  # Example:
  # - name: "tavily"
  #   description: "Web search capabilities"
  #   command: "npx"
  #   args: ["-y", "tavily-mcp@latest"]
  #   env:
  #     TAVILY_API_KEY: "${TAVILY_API_KEY}"
  tool_discovery_enabled: true
  lazy_load: true

# Logging
# -------
logging:
  level: "INFO"
  file: "logs/ptc.log"

# Filesystem
# ----------
filesystem:
  # Filesystem access configuration for first-class filesystem tools
  # These tools provide direct file and directory operations without code generation

  # Working directory for the sandbox - used as the root for virtual path normalization
  # Agent sees virtual paths like /results/file.txt which map to {working_directory}/results/file.txt
  working_directory: "/home/workspace"

  # allowed_directories and denied_directories are auto-derived from working_directory:
  #   allowed: [working_directory, "/tmp"]
  #   denied:  [working_directory/_internal]
  # Override only if you need custom values.

  enable_path_validation: true

# Agent Settings (optional)
# -------------------------
agent:
  enable_view_image: true
  background_auto_wait: false  # true to wait for background tasks before returning to CLI

# Subagents (optional)
# --------------------
subagents:
  enabled:
    - "general-purpose"
    # - "research"
"""


def generate_config_template(
    output_dir: Path,
    *,
    include_llms: bool = False,  # Deprecated, kept for backward compatibility
    overwrite: bool = False,
) -> dict[str, Path]:
    """Generate agent_config.yaml template.

    This is useful for CLI 'config init' commands or first-run setup.

    Args:
        output_dir: Directory to write config files
        include_llms: Deprecated, no longer used (llms.json replaced by models.json)
        overwrite: Whether to overwrite existing files

    Returns:
        Dict mapping filename to path of created file

    Raises:
        FileExistsError: If file exists and overwrite is False
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    created = {}

    # Write agent_config.yaml
    config_path = output_dir / AGENT_CONFIG_FILE
    if config_path.exists() and not overwrite:
        msg = f"Config file already exists: {config_path}"
        raise FileExistsError(msg)
    config_path.write_text(CONFIG_TEMPLATE)
    created[AGENT_CONFIG_FILE] = config_path

    return created
