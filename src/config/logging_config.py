"""
Centralized Logging Configuration

This module provides centralized logging configuration for the entire application.
It reads settings from config.yaml and configures the root logger as well as
module-specific loggers based on environment (development/production).

Usage:
    from src.config.logging_config import configure_logging

    # Call once at application startup
    configure_logging()
"""

import logging

from src.config.settings import (
    get_log_level,
    get_log_format,
    get_module_log_levels,
    is_sse_event_log_enabled,
    get_sse_event_log_level,
)


# Flag to ensure configuration is only applied once
_logging_configured = False


# =============================================================================
# Library Group Mappings for Grouped Logging Configuration
# =============================================================================

# Third-party libraries (Network/HTTP/LLM clients)
THIRD_PARTY_LIBRARIES = [
    'openai',
    'anthropic',
    'httpx',
    'httpcore',
    'urllib3',
    'requests',
    'litellm',
    'edgar',  # SEC EDGAR library (verbose cache/request logs)
    'httpxthrottlecache',  # HTTP cache/throttle library used by edgar
    'mcp',  # MCP SDK library (verbose ListToolsRequest logs)
]

# LangChain ecosystem libraries
LANGCHAIN_LIBRARIES = [
    'langchain',
    'langchain_core',
    'langchain_community',
    'langchain_openai',
    'langchain_anthropic',
    'langchain_google_genai',
    'langchain_deepseek',
    'langgraph',
    'langsmith',
]

# Infrastructure libraries (Databases/Server)
INFRASTRUCTURE_LIBRARIES = [
    'fastapi',
    'uvicorn',
    'redis',
    'psycopg',
    'sqlalchemy',
]

# Mapping dictionary for group expansion
LIBRARY_GROUPS = {
    'third_party_libraries': THIRD_PARTY_LIBRARIES,
    'langchain_libraries': LANGCHAIN_LIBRARIES,
    'infrastructure_libraries': INFRASTRUCTURE_LIBRARIES,
}


def expand_module_log_levels(raw_config: dict) -> dict:
    """
    Expand grouped logger configurations to individual parent logger names.

    This function supports two configuration formats:
    1. Grouped: 'group:third_party_libraries: WARNING'
       Expands to parent loggers (openai, httpx, etc.) which automatically
       affect all children due to Python's logger hierarchy
    2. Individual: 'src.server: DEBUG'
       Used as-is for specific module configuration

    Python's logging hierarchy means setting a parent logger (e.g., 'openai')
    automatically affects all child loggers (e.g., 'openai._base_client').

    Args:
        raw_config: Raw module_log_levels dictionary from config.yaml

    Returns:
        Expanded dictionary with individual parent logger names

    Example:
        Input:  {'group:third_party_libraries': 'WARNING', 'src.tools': 'DEBUG'}
        Output: {'openai': 'WARNING', 'httpx': 'WARNING', ..., 'src.tools': 'DEBUG'}
    """
    expanded = {}

    for key, level in raw_config.items():
        if key.startswith('group:'):
            # Extract group name (remove 'group:' prefix)
            group_name = key.replace('group:', '')

            if group_name in LIBRARY_GROUPS:
                # Expand to parent loggers only (children inherit automatically)
                for logger_name in LIBRARY_GROUPS[group_name]:
                    expanded[logger_name] = level
            else:
                logging.warning(
                    f"Unknown logger group '{group_name}'. "
                    f"Valid groups: {list(LIBRARY_GROUPS.keys())}"
                )
        else:
            # Regular module name (not grouped) - use as-is
            expanded[key] = level

    return expanded


def configure_logging(force: bool = False) -> None:
    """
    Configure logging based on environment settings from config.yaml.

    This function should be called once at application startup (main.py or server startup).
    It configures:
    1. Root logger with level and format from config.yaml
    2. Module-specific loggers with their own levels

    Args:
        force: If True, reconfigure logging even if already configured.
               Useful for testing. Default: False

    Example:
        >>> configure_logging()  # Use settings from config.yaml
    """
    global _logging_configured

    if _logging_configured and not force:
        # Already configured, skip
        return

    # Get configuration from config.yaml
    log_level = get_log_level()
    log_format = get_log_format()
    raw_module_log_levels = get_module_log_levels()

    # Expand grouped configurations (e.g., group:third_party_libraries -> openai, httpx, etc.)
    module_log_levels = expand_module_log_levels(raw_module_log_levels)

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        force=True  # Override any existing basicConfig calls
    )

    # Configure module-specific loggers
    for module_name, level_str in module_log_levels.items():
        module_logger = logging.getLogger(module_name)
        try:
            level = getattr(logging, level_str)
            module_logger.setLevel(level)
        except AttributeError:
            # Invalid level name, log warning and skip
            logging.warning(
                f"Invalid log level '{level_str}' for module '{module_name}'. "
                f"Valid levels: DEBUG, INFO, WARNING, ERROR, CRITICAL"
            )

    # Configure SSE event logger with dedicated handler (independent of root level)
    if is_sse_event_log_enabled():
        sse_level_str = get_sse_event_log_level().upper()
        sse_level = getattr(logging, sse_level_str, logging.INFO)
        sse_logger = logging.getLogger("sse_events")
        sse_logger.setLevel(sse_level)
        # Add dedicated handler so SSE logs output independently of root logger level
        if not sse_logger.handlers:  # Avoid duplicate handlers on reload
            sse_handler = logging.StreamHandler()
            sse_handler.setLevel(sse_level)
            sse_handler.setFormatter(logging.Formatter("%(message)s"))
            sse_logger.addHandler(sse_handler)
        # Prevent duplicate logs by not propagating to root logger
        sse_logger.propagate = False

    _logging_configured = True

    # Log the configuration (at DEBUG level to avoid noise in production)
    root_logger = logging.getLogger()
    root_logger.debug(
        f"Logging configured: root_level={log_level}, "
        f"modules={list(module_log_levels.keys())}"
    )


def reset_logging_config() -> None:
    """
    Reset the logging configuration flag.

    This is primarily useful for testing, allowing configure_logging()
    to be called multiple times.
    """
    global _logging_configured
    _logging_configured = False


def is_logging_configured() -> bool:
    """
    Check if logging has been configured.

    Returns:
        True if configure_logging() has been called, False otherwise
    """
    return _logging_configured
