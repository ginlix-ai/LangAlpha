"""
Unified configuration loader for both infrastructure and agent configs.

This module re-exports config file utilities from ptc_agent.config.file_utils
and adds server-specific infrastructure config loading.

Config Files:
- config.yaml: Infrastructure settings (server, Redis, background tasks, logging, CORS)
- agent_config.yaml: Agent capabilities (LLM, MCP, tools, crawler, web_fetch, embedding, security)
"""

import logging
from functools import lru_cache
from pathlib import Path
from typing import Optional

from src.config.models import InfrastructureConfig

# Re-export all config file utilities from ptc_agent (single source of truth)
from ptc_agent.config.file_utils import (  # noqa: F401
    AGENT_CONFIG_FILE,
    ConfigContext,
    clear_config_cache as _clear_file_cache,
    ensure_config_dir,
    find_config_file,
    find_project_root,
    get_config_search_paths,
    get_default_config_dir,
    load_yaml_config,
    substitute_env_vars,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Infrastructure Config Loading
# =============================================================================

# Config filenames
INFRASTRUCTURE_CONFIG_FILE = "config.yaml"


@lru_cache(maxsize=1)
def load_infrastructure_config(
    config_path: Optional[str] = None,
) -> InfrastructureConfig:
    """
    Load infrastructure configuration from config.yaml.

    This function is cached to avoid repeated file reads and validation.

    Args:
        config_path: Optional explicit path to config file

    Returns:
        Validated InfrastructureConfig instance
    """
    if config_path:
        path = Path(config_path)
    else:
        path = find_config_file(INFRASTRUCTURE_CONFIG_FILE)

    if path is None:
        logger.warning("No infrastructure config file found, using defaults")
        return InfrastructureConfig()

    config_dict = load_yaml_config(str(path))
    return InfrastructureConfig(**config_dict)


def get_infrastructure_config() -> InfrastructureConfig:
    """
    Get the infrastructure configuration.

    Convenience function that calls load_infrastructure_config().

    Returns:
        InfrastructureConfig instance
    """
    return load_infrastructure_config()


def clear_config_cache() -> None:
    """Clear all configuration caches (YAML file cache + infrastructure LRU cache)."""
    _clear_file_cache()
    load_infrastructure_config.cache_clear()
    logger.debug("All configuration caches cleared")
