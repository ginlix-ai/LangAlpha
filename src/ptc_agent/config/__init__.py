"""Unified configuration package for Open PTC Agent.

This package consolidates all configuration-related code:
- core.py: Core infrastructure configs (Daytona, MCP, Filesystem, Security, Logging)
- agent.py: Agent-specific configs (AgentConfig, LLMConfig, LLMDefinition)
- loaders.py: File-based configuration loading
- utils.py: Shared utilities for config parsing

Usage:
    # Programmatic configuration (recommended)
    from langchain_anthropic import ChatAnthropic
    from ptc_agent.config import AgentConfig

    llm = ChatAnthropic(model="claude-sonnet-4-20250514")
    config = AgentConfig.create(llm=llm)

    # File-based configuration (for CLI, etc.)
    from ptc_agent.config import load_from_files
    config = await load_from_files()

    # Core config only (for SessionManager)
    from ptc_agent.config import load_core_from_files
    core_config = await load_core_from_files()
"""

# Core data classes
# Agent data classes
from ptc_agent.config.agent import (
    AgentConfig,
    FlashConfig,
    LLMConfig,
    LLMDefinition,
    CompactionConfig,
)
from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    DockerConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    MCPServerConfig,
    SandboxConfig,
    SecurityConfig,
)

# File-based loading
from ptc_agent.config.loaders import (
    generate_config_template,
    load_core_from_files,
    load_from_dict,
    load_from_files,
)
from ptc_agent.config.file_utils import (
    ConfigContext,
    ensure_config_dir,
    find_config_file,
    find_project_root,
    get_config_search_paths,
    get_default_config_dir,
)

# Utilities
from ptc_agent.config.utils import configure_structlog

# Backward compat alias
configure_logging = configure_structlog

__all__ = [
    # Agent data classes
    "AgentConfig",
    "FlashConfig",
    # Context enum
    "ConfigContext",
    # Core data classes
    "CoreConfig",
    "DaytonaConfig",
    "DockerConfig",
    "FilesystemConfig",
    "LLMConfig",
    "LLMDefinition",
    "LoggingConfig",
    "MCPConfig",
    "MCPServerConfig",
    "SandboxConfig",
    "SecurityConfig",
    "CompactionConfig",
    # Utilities
    "configure_logging",  # backward compat alias
    "configure_structlog",
    "ensure_config_dir",
    "find_config_file",
    "find_project_root",
    # Template generation
    "generate_config_template",
    "get_config_search_paths",
    # Config path utilities
    "get_default_config_dir",
    "load_core_from_files",
    "load_from_dict",
    # Config loading
    "load_from_files",
]
