"""
Centralized configuration access module.

This module provides a unified interface to access configuration from both
environment variables (.env) and YAML config files.

Configuration loading strategy:
1. Credentials come from environment variables (.env)
2. Infrastructure settings come from config.yaml
3. Agent/tool settings come from agent_config.yaml
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from functools import lru_cache

from src.config.core import load_yaml_config

logger = logging.getLogger(__name__)

# Config file paths
_PROJECT_ROOT = Path(__file__).parent.parent.parent
INFRASTRUCTURE_CONFIG_PATH = _PROJECT_ROOT / "config.yaml"
AGENT_CONFIG_PATH = _PROJECT_ROOT / "agent_config.yaml"


@lru_cache(maxsize=1)
def load_app_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load application configuration from config.yaml.

    This function is cached to avoid repeated file reads.

    Args:
        config_path: Path to configuration file (defaults to config.yaml in project root)

    Returns:
        Configuration dictionary
    """
    if config_path is None:
        config_path = INFRASTRUCTURE_CONFIG_PATH

    config = load_yaml_config(str(config_path))
    return config


@lru_cache(maxsize=1)
def load_agent_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load agent configuration from agent_config.yaml.

    This function is cached to avoid repeated file reads.

    Args:
        config_path: Path to configuration file (defaults to agent_config.yaml in project root)

    Returns:
        Configuration dictionary
    """
    if config_path is None:
        config_path = AGENT_CONFIG_PATH

    config = load_yaml_config(str(config_path))
    return config


def get_config(key: str, default: Any = None, config_path: Optional[Path] = None) -> Any:
    """
    Get a configuration value from config.yaml.

    Args:
        key: Configuration key (e.g., 'debug', 'agent_recursion_limit')
        default: Default value if key not found
        config_path: Optional custom config file path

    Returns:
        Configuration value or default
    """
    config = load_app_config(config_path)
    return config.get(key, default)


def get_nested_config(
    key_path: str,
    default: Any = None,
    config_path: Optional[Path] = None
) -> Any:
    """
    Get a nested configuration value using dot notation.

    Example:
        get_nested_config('redis.cache_enabled') -> config['redis']['cache_enabled']

    Args:
        key_path: Dot-separated key path (e.g., 'redis.ttl.results_list')
        default: Default value if key not found
        config_path: Optional custom config file path

    Returns:
        Configuration value or default
    """
    config = load_app_config(config_path)

    keys = key_path.split('.')
    value = config

    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default

    return value


# =============================================================================
# Application Settings
# =============================================================================

def get_debug_mode() -> bool:
    """Get debug mode flag from config.yaml."""
    return bool(get_config('debug', False))




def get_agent_recursion_limit(default: int = 50) -> int:
    """
    Get agent recursion limit from config.yaml.

    Args:
        default: Default value if not configured

    Returns:
        Configured recursion limit
    """
    try:
        limit = int(get_config('agent_recursion_limit', default))
        if limit > 0:
            return limit
        else:
            logger.warning(
                f"agent_recursion_limit value {limit} is not positive. "
                f"Using default value {default}."
            )
            return default
    except (ValueError, TypeError) as e:
        logger.warning(
            f"Invalid agent_recursion_limit value: {e}. "
            f"Using default value {default}."
        )
        return default


def get_workflow_timeout(default: int = 1600) -> int:
    """
    Get workflow timeout in seconds from config.yaml.

    Args:
        default: Default timeout in seconds

    Returns:
        Configured timeout in seconds
    """
    try:
        timeout = int(get_config('workflow_timeout', default))
        if timeout >= 0:  # 0 means no timeout
            return timeout
        else:
            logger.warning(
                f"workflow_timeout value {timeout} is negative. "
                f"Using default value {default}."
            )
            return default
    except (ValueError, TypeError) as e:
        logger.warning(
            f"Invalid workflow_timeout value: {e}. "
            f"Using default value {default}."
        )
        return default


def get_sse_keepalive_interval(default: float = 15.0) -> float:
    """
    Get SSE keepalive interval in seconds from config.yaml.

    Args:
        default: Default interval in seconds

    Returns:
        Configured keepalive interval in seconds
    """
    try:
        interval = float(get_config('sse_keepalive_interval', default))
        if interval > 0:
            return interval
        else:
            logger.warning(
                f"sse_keepalive_interval value {interval} is not positive. "
                f"Using default value {default}."
            )
            return default
    except (ValueError, TypeError) as e:
        logger.warning(
            f"Invalid sse_keepalive_interval value: {e}. "
            f"Using default value {default}."
        )
        return default


# =============================================================================
# Feature Flags
# =============================================================================

def is_result_log_db_enabled() -> bool:
    """Check if result logging to database is enabled."""
    return bool(get_config('result_log_db_enabled', True))


def is_redis_warm_on_startup_enabled() -> bool:
    """Check if Redis cache warming on startup is enabled."""
    return bool(get_config('redis_warm_on_startup', True))


def is_langsmith_tracing_enabled() -> bool:
    """Check if LangSmith tracing is enabled."""
    return bool(get_config('langsmith_tracing', False))


# =============================================================================
# SSE Event Logging
# =============================================================================

def is_sse_event_log_enabled() -> bool:
    """Check if SSE event logging is enabled."""
    return bool(get_config('sse_event_log_enabled', True))


def get_sse_event_log_level() -> str:
    """
    Get SSE event log level from config.yaml.

    Returns:
        Log level string (debug, info, warning, error, critical)
    """
    level = str(get_config('sse_event_log_level', 'info')).lower()
    valid_levels = {'debug', 'info', 'warning', 'error', 'critical'}

    if level in valid_levels:
        return level
    else:
        logger.warning(
            f"Invalid sse_event_log_level value: {level}. "
            f"Using default 'info'."
        )
        return 'info'


# =============================================================================
# General Application Logging
# =============================================================================

def get_log_level() -> str:
    """
    Get root logger level from config.yaml.

    Returns:
        Log level string (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    level = str(get_config('log_level', 'INFO')).upper()
    valid_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}

    if level in valid_levels:
        return level
    else:
        logger.warning(
            f"Invalid log_level value: {level}. "
            f"Using default 'INFO'."
        )
        return 'INFO'


def get_log_format() -> str:
    """
    Get log format string from config.yaml.

    Returns:
        Log format string
    """
    return str(get_config(
        'log_format',
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))


def get_module_log_levels() -> dict:
    """
    Get module-specific log levels from config.yaml.

    Returns:
        Dictionary mapping module names to log levels
    """
    module_levels = get_config('module_log_levels', {})
    if isinstance(module_levels, dict):
        # Convert all values to uppercase for consistency
        return {k: v.upper() for k, v in module_levels.items()}
    return {}


# =============================================================================
# CORS Settings
# =============================================================================

def get_allowed_origins() -> List[str]:
    """
    Get allowed CORS origins from config.yaml.

    Returns:
        List of allowed origin URLs
    """
    origins = get_config('allowed_origins', [])
    if isinstance(origins, list):
        return origins
    elif isinstance(origins, str):
        # Handle comma-separated string format
        return [origin.strip() for origin in origins.split(',')]
    else:
        logger.warning(
            f"Invalid allowed_origins type: {type(origins)}. "
            f"Using default localhost."
        )
        return ['http://localhost:3000']


# =============================================================================
# Locale and Timezone Configuration
# =============================================================================

def get_locale_config(locale: str, prompt_language: str) -> Dict[str, str]:
    """
    Get locale-specific timezone configuration.

    This function maps locales to their appropriate timezones for consistent
    timestamp formatting across the application. The timezone is used for
    displaying current time in prompts and maintaining temporal consistency
    during workflow execution.

    Args:
        locale: Locale string (e.g., "en-US", "zh-CN")
        prompt_language: Prompt language code (e.g., "en", "zh")

    Returns:
        Dictionary containing:
        - locale: Original locale string
        - prompt_language: Original prompt language string
        - timezone: Timezone identifier (e.g., "America/New_York", "Asia/Shanghai", "UTC")
        - timezone_label: Display label for timezone (e.g., "EST", "EDT", "CST", "UTC")
          Note: Label is DST-aware and based on current time

    Timezone mapping:
        - en-US → Eastern Time (America/New_York, "EST" or "EDT" depending on DST)
        - zh-CN → China Standard Time (Asia/Shanghai, "CST")
        - others → UTC (UTC, "UTC")
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from src.utils.timezone_utils import get_timezone_label

    # Normalize locale to lowercase for comparison
    locale_lower = locale.lower() if locale else ""

    # Determine timezone based on locale
    if locale_lower == "en-us":
        timezone = "America/New_York"
    elif locale_lower == "zh-cn":
        timezone = "Asia/Shanghai"
    else:
        # Default to UTC for all other locales
        timezone = "UTC"

    # Extract timezone label using current time (DST-aware)
    tz = ZoneInfo(timezone)
    current_time = datetime.now(tz)
    timezone_label = get_timezone_label(current_time)

    return {
        "locale": locale,
        "prompt_language": prompt_language,
        "timezone": timezone,
        "timezone_label": timezone_label,
    }


# =============================================================================
# Service Configuration
# =============================================================================

def get_search_api() -> str:
    """
    Get configured search API from agent_config.yaml.

    Returns:
        Search API name (tavily, duckduckgo, brave_search, arxiv, bocha)
    """
    agent_config = load_agent_config()
    return str(agent_config.get('search_api', 'tavily'))






# =============================================================================
# Redis Configuration
# =============================================================================

def is_redis_cache_enabled() -> bool:
    """Check if Redis caching is enabled."""
    return bool(get_nested_config('redis.cache_enabled', True))


def get_redis_max_connections(default: int = 10) -> int:
    """Get Redis connection pool max connections."""
    return int(get_nested_config('redis.max_connections', default))


def get_redis_ttl_results_list(default: int = 300) -> int:
    """Get Redis TTL for results list (seconds)."""
    return int(get_nested_config('redis.ttl.results_list', default))


def get_redis_ttl_result_detail(default: int = 900) -> int:
    """Get Redis TTL for result detail (seconds)."""
    return int(get_nested_config('redis.ttl.result_detail', default))


def get_redis_ttl_metadata(default: int = 900) -> int:
    """Get Redis TTL for metadata (seconds)."""
    return int(get_nested_config('redis.ttl.metadata', default))


def get_redis_ttl_metadata_summary(default: int = 600) -> int:
    """Get Redis TTL for metadata summary (seconds)."""
    return int(get_nested_config('redis.ttl.metadata_summary', default))


def is_cache_invalidate_on_write_enabled() -> bool:
    """Check if cache invalidation on write is enabled."""
    return bool(get_nested_config('redis.cache_invalidate_on_write', True))



# =============================================================================
# Summarization Middleware Configuration (from agent_config.yaml)
# =============================================================================

def _get_agent_nested_config(key_path: str, default: Any = None) -> Any:
    """Get nested config from agent_config.yaml."""
    agent_config = load_agent_config()
    keys = key_path.split('.')
    value = agent_config
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default
    return value


def get_summarization_config() -> Dict[str, Any]:
    """
    Get summarization middleware configuration from agent_config.yaml.

    Returns:
        Dictionary containing:
        - enabled: Whether summarization is enabled
        - llm: Model name for generating summaries
        - token_threshold: Token count threshold to trigger summarization
        - keep_messages: Number of recent messages to preserve after summarization
    """
    return {
        "enabled": bool(_get_agent_nested_config('summarization.enabled', True)),
        "llm": str(_get_agent_nested_config('summarization.llm', 'gpt-5-nano')),
        "token_threshold": int(_get_agent_nested_config('summarization.token_threshold', 120000)),
        "keep_messages": int(_get_agent_nested_config('summarization.keep_messages', 5)),
    }


def is_summarization_enabled() -> bool:
    """Check if conversation summarization middleware is enabled."""
    return bool(_get_agent_nested_config('summarization.enabled', True))


def get_summarization_token_threshold(default: int = 120000) -> int:
    """Get token threshold for triggering summarization."""
    return int(_get_agent_nested_config('summarization.token_threshold', default))


def get_summarization_keep_messages(default: int = 5) -> int:
    """Get number of recent messages to preserve after summarization."""
    return int(_get_agent_nested_config('summarization.keep_messages', default))


# =============================================================================
# Background Execution Configuration
# =============================================================================

def is_background_execution_enabled() -> bool:
    """Check if background execution is enabled."""
    return bool(get_config('background_execution_enabled', True))


def get_max_concurrent_workflows(default: int = 100) -> int:
    """Get maximum number of concurrent background workflows."""
    return int(get_nested_config('background_execution.max_concurrent_workflows', default))


def get_workflow_result_ttl(default: int = 86400) -> int:
    """Get workflow result TTL in seconds (default: 24 hours)."""
    return int(get_nested_config('background_execution.workflow_result_ttl', default))


def get_abandoned_workflow_timeout(default: int = 3600) -> int:
    """Get abandoned workflow timeout in seconds (default: 1 hour)."""
    return int(get_nested_config('background_execution.abandoned_workflow_timeout', default))


def get_cleanup_interval(default: int = 300) -> int:
    """Get background cleanup interval in seconds (default: 5 minutes)."""
    return int(get_nested_config('background_execution.cleanup_interval', default))


def is_intermediate_storage_enabled() -> bool:
    """Check if intermediate result storage is enabled."""
    return bool(get_nested_config('background_execution.enable_intermediate_storage', True))


def get_max_stored_messages_per_agent(default: int = 100) -> int:
    """Get maximum stored messages per agent."""
    return int(get_nested_config('background_execution.max_stored_messages_per_agent', default))


def get_event_storage_backend(default: str = "redis") -> str:
    """
    Get event storage backend (redis or memory).

    Args:
        default: Default backend if not configured

    Returns:
        Event storage backend: "redis" or "memory"
    """
    backend = str(get_nested_config('background_execution.event_storage_backend', default))
    if backend not in ["redis", "memory"]:
        logger.warning(f"Invalid event_storage_backend: {backend}, using {default}")
        return default
    return backend


def is_event_storage_fallback_enabled() -> bool:
    """Check if fallback to memory storage is enabled on Redis failure."""
    return bool(get_nested_config('background_execution.event_storage_fallback_to_memory', True))


def get_redis_ttl_workflow_events(default: int = 86400) -> int:
    """
    Get Redis TTL for workflow event buffers (seconds).

    Args:
        default: Default TTL if not configured (24 hours)

    Returns:
        TTL in seconds
    """
    return int(get_nested_config('redis.ttl.workflow_events', default))


# =============================================================================
# LangSmith Tracing Configuration
# =============================================================================

def get_langsmith_tags(
    msg_type: str,
    deepthinking: bool = False,
    auto_accepted_plan: bool = False,
    locale: Optional[str] = None,
) -> List[str]:
    """
    Generate LangSmith tags for workflow tracing.

    Tags are used for filtering and grouping traces in the LangSmith UI.
    All tags should be known upfront at request time (no runtime-determined values).

    Args:
        msg_type: Workflow type (chat, technical_analysis, fundamental_analysis)
        deepthinking: Whether deep thinking mode is enabled
        auto_accepted_plan: Whether plan auto-acceptance is enabled
        locale: Locale string (e.g., "zh-CN", "en-US")

    Returns:
        List of tags for LangSmith tracing

    Example tags:
        With deepthinking=true: ["workflow:deep_research", "feature:deepthinking", "locale:zh-CN"]
        With deepthinking=false: ["workflow:chat", "locale:zh-CN"]
    """
    tags = []

    # Workflow type tag
    # For chat msg_type, distinguish between deep_research and chat based on deepthinking flag
    if msg_type == "chat":
        workflow_tag = "workflow:deep_research" if deepthinking else "workflow:chat"
    else:
        workflow_map = {
            "technical_analysis": "workflow:technical_analysis",
            "fundamental_analysis": "workflow:fundamental_analysis",
            "podcast_generation": "workflow:podcast_generation",
        }
        workflow_tag = workflow_map.get(msg_type, "workflow:chat")
    tags.append(workflow_tag)

    # Feature flags
    if deepthinking:
        tags.append("feature:deepthinking")
    if auto_accepted_plan:
        tags.append("feature:auto_plan")

    # Locale tag
    if locale:
        tags.append(f"locale:{locale}")

    return tags


def get_langsmith_metadata(
    user_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    thread_id: Optional[str] = None,
    workflow_type: Optional[str] = None,
    stock_code: Optional[str] = None,
    locale: Optional[str] = None,
    timezone: Optional[str] = None,
    deepthinking: bool = False,
    auto_accepted_plan: bool = False,
    track_tokens: bool = True,
    max_plan_iterations: Optional[int] = None,
    max_step_num: Optional[int] = None,
    agent_llm_preset: Optional[str] = None,
    report_style: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate LangSmith metadata for workflow tracing.

    Metadata provides detailed contextual information for traces.
    Unlike tags, metadata can contain complex JSON-serializable values.

    Args:
        user_id: User identifier
        workspace_id: Workspace identifier
        thread_id: Thread identifier (LangGraph checkpoint ID)
        workflow_type: Workflow type (chat, technical_analysis, etc.)
        stock_code: Stock ticker symbol
        locale: Locale string
        timezone: Timezone string
        deepthinking: Deep thinking mode flag
        auto_accepted_plan: Auto plan acceptance flag
        track_tokens: Token tracking flag (always True, kept for compatibility)
        max_plan_iterations: Maximum plan iterations
        max_step_num: Maximum step number
        agent_llm_preset: Agent LLM preset name
        report_style: Report style

    Returns:
        Dictionary of metadata for LangSmith tracing
    """
    metadata = {}

    # User and session info
    if user_id:
        metadata["user_id"] = user_id
    if workspace_id:
        metadata["workspace_id"] = workspace_id
    if thread_id:
        metadata["thread_id"] = thread_id

    # Workflow configuration
    if workflow_type:
        metadata["workflow_type"] = workflow_type
    if stock_code:
        metadata["stock_code"] = stock_code
    if locale:
        metadata["locale"] = locale
    if timezone:
        metadata["timezone"] = timezone

    # Feature flags
    metadata["deepthinking"] = deepthinking
    metadata["auto_accepted_plan"] = auto_accepted_plan

    # Execution parameters
    if max_plan_iterations is not None:
        metadata["max_plan_iterations"] = max_plan_iterations
    if max_step_num is not None:
        metadata["max_step_num"] = max_step_num

    # Agent configuration
    if agent_llm_preset:
        metadata["agent_llm_preset"] = agent_llm_preset
    if report_style:
        metadata["report_style"] = report_style

    return metadata
