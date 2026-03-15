"""
Tool-specific configuration access module.

This module provides configuration helpers for tools (crawler, sitemap, search, etc.)
that read from agent_config.yaml via the shared YAML cache.
"""

import logging
from typing import Any

from src.config.core import load_yaml_config, find_config_file

logger = logging.getLogger(__name__)


def _get_agent_config_dict() -> dict:
    """Get the agent_config.yaml as a raw dict via shared YAML cache."""
    path = find_config_file("agent_config.yaml")
    if path is None:
        return {}
    return load_yaml_config(str(path))


def _get_tool_config(key_path: str, default: Any = None) -> Any:
    """
    Get tool configuration from agent_config.yaml.

    Args:
        key_path: Dot-separated key path (e.g., 'crawler.max_concurrent_crawls')
        default: Default value if key not found

    Returns:
        Configuration value or default
    """
    agent_config = _get_agent_config_dict()
    keys = key_path.split('.')
    value = agent_config

    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default

    return value


# =============================================================================
# Crawler Configuration
# =============================================================================

def get_crawler_max_concurrent(default: int = 10) -> int:
    """Get maximum concurrent crawler operations."""
    return int(_get_tool_config('crawler.max_concurrent_crawls', default))


def get_crawler_page_timeout(default: int = 60000) -> int:
    """Get crawler page timeout in milliseconds."""
    return int(_get_tool_config('crawler.page_timeout', default))


def get_crawler_circuit_failure_threshold(default: int = 5) -> int:
    """Get circuit breaker failure threshold."""
    return int(_get_tool_config('crawler.circuit_breaker.failure_threshold', default))


def get_crawler_circuit_recovery_timeout(default: int = 60) -> int:
    """Get circuit breaker recovery timeout in seconds."""
    return int(_get_tool_config('crawler.circuit_breaker.recovery_timeout', default))


def get_crawler_circuit_success_threshold(default: int = 2) -> int:
    """Get circuit breaker success threshold for closing."""
    return int(_get_tool_config('crawler.circuit_breaker.success_threshold', default))


def get_crawler_queue_max_size(default: int = 100) -> int:
    """Get maximum crawler queue size."""
    return int(_get_tool_config('crawler.queue.max_size', default))


def get_crawler_queue_slot_timeout(default: int = 10) -> int:
    """Get crawler queue slot timeout in seconds."""
    return int(_get_tool_config('crawler.queue.slot_timeout', default))


# =============================================================================
# Sitemap Configuration
# =============================================================================

def is_sitemap_enabled() -> bool:
    """Check if sitemap fetching is enabled."""
    return bool(_get_tool_config('web_fetch.sitemap_enabled', True))


def get_sitemap_max_urls(default: int = 100) -> int:
    """Get maximum URLs to fetch from sitemap."""
    return int(_get_tool_config('web_fetch.sitemap_max_urls', default))


def get_sitemap_max_examples(default: int = 3) -> int:
    """Get maximum example URLs per path prefix in sitemap summary."""
    return int(_get_tool_config('web_fetch.sitemap_max_examples', default))


def get_sitemap_timeout(default: int = 10) -> int:
    """Get sitemap fetch timeout in seconds."""
    return int(_get_tool_config('web_fetch.sitemap_timeout', default))
