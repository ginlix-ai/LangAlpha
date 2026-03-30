"""
Centralized configuration access module.

This module provides a unified interface to access configuration from both
environment variables (.env) and YAML config files.

Configuration loading strategy:
1. Credentials come from environment variables (.env)
2. Infrastructure settings come from config.yaml via InfrastructureConfig
3. Agent/tool settings come from agent_config.yaml via AgentConfig (ptc_agent.config)
"""

import logging
from typing import Any, Dict, List, Optional

from src.config.core import get_infrastructure_config

# Re-export env-var constants for backward compatibility
from src.config.env import (  # noqa: F401
    AUTH_ENABLED,
    AUTH_SERVICE_URL,
    AUTOMATION_WEBHOOK_SECRET,
    AUTOMATION_WEBHOOK_URL,
    GINLIX_DATA_ENABLED,
    GINLIX_DATA_URL,
    GINLIX_DATA_WS_URL,
    LOCAL_DEV_USER_ID,
    SUPABASE_URL,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Application Settings — delegates to InfrastructureConfig
# =============================================================================


def get_debug_mode() -> bool:
    """Get debug mode flag from config.yaml."""
    return get_infrastructure_config().debug


def get_ptc_recursion_limit() -> int:
    """Get PTC agent recursion limit from config.yaml."""
    return get_infrastructure_config().ptc_recursion_limit


def get_flash_recursion_limit() -> int:
    """Get Flash agent recursion limit from config.yaml."""
    return get_infrastructure_config().flash_recursion_limit


def get_workflow_timeout() -> int:
    """Get workflow timeout in seconds from config.yaml."""
    return get_infrastructure_config().workflow_timeout


def get_sse_keepalive_interval() -> float:
    """Get SSE keepalive interval in seconds from config.yaml."""
    return get_infrastructure_config().sse_keepalive_interval


# =============================================================================
# Feature Flags
# =============================================================================


def get_market_data_providers() -> list[dict]:
    """Return the ordered provider list from ``market_data.providers`` in config.yaml."""
    cfg = get_infrastructure_config()
    providers = cfg.market_data.providers
    if not providers:
        return [{"name": "fmp", "markets": ["all"]}]
    return [p.model_dump() for p in providers]


def get_news_data_providers() -> list[dict]:
    """Return the ordered provider list from ``news_data.providers`` in config.yaml."""
    cfg = get_infrastructure_config()
    providers = cfg.news_data.providers
    if not providers:
        return [{"name": "fmp"}]
    return [p.model_dump() for p in providers]


def is_result_log_db_enabled() -> bool:
    """Check if result logging to database is enabled."""
    return get_infrastructure_config().result_log_db_enabled


def is_redis_warm_on_startup_enabled() -> bool:
    """Check if Redis cache warming on startup is enabled."""
    return get_infrastructure_config().redis_warm_on_startup


def is_langsmith_tracing_enabled() -> bool:
    """Check if LangSmith tracing is enabled."""
    return get_infrastructure_config().langsmith_tracing


# =============================================================================
# SSE Event Logging
# =============================================================================


def is_sse_event_log_enabled() -> bool:
    """Check if SSE event logging is enabled."""
    return get_infrastructure_config().sse_event_log_enabled


def get_sse_event_log_level() -> str:
    """Get SSE event log level from config.yaml."""
    return get_infrastructure_config().sse_event_log_level


# =============================================================================
# General Application Logging
# =============================================================================


def get_log_level() -> str:
    """Get root logger level from config.yaml."""
    return get_infrastructure_config().log_level.upper()


def get_log_format() -> str:
    """Get log format string from config.yaml."""
    return get_infrastructure_config().log_format


def get_module_log_levels() -> dict:
    """Get module-specific log levels from config.yaml."""
    levels = get_infrastructure_config().module_log_levels
    return {k: v.upper() for k, v in levels.items()} if levels else {}


# =============================================================================
# CORS Settings
# =============================================================================


def get_allowed_origins() -> List[str]:
    """Get allowed CORS origins from config.yaml."""
    return get_infrastructure_config().allowed_origins


# =============================================================================
# Locale and Timezone Configuration
# =============================================================================


def get_locale_config(locale: str, prompt_language: str) -> Dict[str, str]:
    """Get locale-specific timezone configuration."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from src.utils.timezone_utils import get_timezone_label

    locale_lower = locale.lower() if locale else ""

    if locale_lower == "en-us":
        timezone = "America/New_York"
    elif locale_lower == "zh-cn":
        timezone = "Asia/Shanghai"
    else:
        timezone = "UTC"

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
# Redis Configuration
# =============================================================================


def is_redis_cache_enabled() -> bool:
    """Check if Redis caching is enabled."""
    return get_infrastructure_config().redis.cache_enabled


def get_redis_max_connections() -> int:
    """Get Redis connection pool max connections."""
    return get_infrastructure_config().redis.max_connections


def get_redis_socket_timeout() -> int:
    """Get Redis socket read/write timeout in seconds."""
    return get_infrastructure_config().redis.socket_timeout


def get_redis_socket_connect_timeout() -> int:
    """Get Redis socket connect timeout in seconds."""
    return get_infrastructure_config().redis.socket_connect_timeout


def get_redis_ttl_results_list() -> int:
    """Get Redis TTL for results list (seconds)."""
    return get_infrastructure_config().redis.ttl.results_list


def get_redis_ttl_result_detail() -> int:
    """Get Redis TTL for result detail (seconds)."""
    return get_infrastructure_config().redis.ttl.result_detail


def get_redis_ttl_metadata() -> int:
    """Get Redis TTL for metadata (seconds)."""
    return get_infrastructure_config().redis.ttl.metadata


def get_redis_ttl_metadata_summary() -> int:
    """Get Redis TTL for metadata summary (seconds)."""
    return get_infrastructure_config().redis.ttl.metadata_summary


def get_redis_ttl_workflow_status() -> int:
    """Get Redis TTL for completed/cancelled workflow status keys (seconds)."""
    return get_infrastructure_config().redis.ttl.workflow_status


def get_redis_ttl_cancel_flag() -> int:
    """Get Redis TTL for workflow cancel flag (seconds)."""
    return get_infrastructure_config().redis.ttl.cancel_flag


def get_redis_ttl_steering() -> int:
    """Get Redis TTL for steering message keys (seconds)."""
    return get_infrastructure_config().redis.ttl.steering


def is_cache_invalidate_on_write_enabled() -> bool:
    """Check if cache invalidation on write is enabled."""
    return get_infrastructure_config().redis.cache_invalidate_on_write


# Fallback TTLs matching config.yaml defaults (interval_seconds × 1.5)
_DEFAULT_OHLCV_TTLS: Dict[str, int] = {
    "1s": 5,
    "1min": 90,
    "5min": 360,
    "15min": 1080,
    "30min": 2100,
    "1hour": 4200,
    "4hour": 16200,
    "1day": 86400,
}


def get_ohlcv_ttl(interval: str) -> int:
    """Get the Redis TTL for a given OHLCV interval."""
    cfg = get_infrastructure_config()
    if interval in cfg.redis.ttl.ohlcv:
        return cfg.redis.ttl.ohlcv[interval]
    return _DEFAULT_OHLCV_TTLS.get(interval, 90)


# =============================================================================
# Background Execution Configuration
# =============================================================================


def get_max_concurrent_workflows() -> int:
    """Get maximum number of concurrent background workflows."""
    return get_infrastructure_config().background_execution.max_concurrent_workflows


def get_workflow_result_ttl() -> int:
    """Get workflow result TTL in seconds."""
    return get_infrastructure_config().background_execution.workflow_result_ttl


def get_abandoned_workflow_timeout() -> int:
    """Get abandoned workflow timeout in seconds."""
    return get_infrastructure_config().background_execution.abandoned_workflow_timeout


def get_cleanup_interval() -> int:
    """Get background cleanup interval in seconds."""
    return get_infrastructure_config().background_execution.cleanup_interval


def is_intermediate_storage_enabled() -> bool:
    """Check if intermediate result storage is enabled."""
    return get_infrastructure_config().background_execution.enable_intermediate_storage


def get_max_stored_messages_per_agent() -> int:
    """Get maximum stored messages per agent."""
    return get_infrastructure_config().background_execution.max_stored_messages_per_agent


def get_subagent_collector_timeout() -> float:
    """Get initial subagent collector timeout in seconds."""
    return get_infrastructure_config().background_execution.subagent_collector_timeout


def get_subagent_orphan_collector_timeout() -> float:
    """Get orphan subagent collector idle timeout in seconds."""
    return get_infrastructure_config().background_execution.subagent_orphan_collector_timeout


def get_event_storage_backend() -> str:
    """Get event storage backend (redis or memory)."""
    return get_infrastructure_config().background_execution.event_storage_backend


def is_event_storage_fallback_enabled() -> bool:
    """Check if fallback to memory storage is enabled on Redis failure."""
    return get_infrastructure_config().background_execution.event_storage_fallback_to_memory


def get_live_queue_maxsize() -> int:
    """Get max backpressure for live SSE subscriber queues."""
    return get_infrastructure_config().background_execution.live_queue_maxsize


def get_subagent_event_buffer_size() -> int:
    """Get max events per subagent task in Redis buffer."""
    return get_infrastructure_config().background_execution.subagent_event_buffer_size


def get_subagent_event_buffer_ttl() -> int:
    """Get TTL (seconds) for per-task subagent Redis event buffer."""
    return get_infrastructure_config().background_execution.subagent_event_buffer_ttl


def get_subagent_task_max_wait() -> int:
    """Get max seconds to wait for subagent task to appear in registry."""
    return get_infrastructure_config().background_execution.subagent_task_max_wait


def get_sse_drain_timeout() -> float:
    """Get seconds to wait for per-task SSE drain before clearing events."""
    return get_infrastructure_config().background_execution.sse_drain_timeout


def get_shutdown_timeout() -> float:
    """Get max seconds for graceful shutdown of running workflows."""
    return get_infrastructure_config().background_execution.shutdown_timeout


def get_checkpoint_flush_timeout() -> float:
    """Get timeout (seconds) for checkpoint state reads/writes."""
    return get_infrastructure_config().background_execution.checkpoint_flush_timeout


def get_wait_for_persistence_timeout() -> float:
    """Get max seconds callers block waiting for persistence completion."""
    return get_infrastructure_config().background_execution.wait_for_persistence_timeout


def get_soft_interrupt_wait_timeout() -> float:
    """Get max seconds to wait for soft-interrupted workflow to finish."""
    return get_infrastructure_config().background_execution.soft_interrupt_wait_timeout


def get_max_workflow_retries() -> int:
    """Get max transient-error retry count for workflow execution."""
    return get_infrastructure_config().background_execution.max_workflow_retries


def get_merged_chunk_max_bytes() -> int:
    """Get max bytes for merged SSE event chunks before split."""
    return get_infrastructure_config().background_execution.merged_chunk_max_bytes


def get_redis_ttl_workflow_events() -> int:
    """Get Redis TTL for workflow event buffers (seconds)."""
    return get_infrastructure_config().redis.ttl.workflow_events


# =============================================================================
# Nested config accessor — kept for remaining callers
# =============================================================================


def get_nested_config(
    key_path: str, default: Any = None, config_path: Optional[Any] = None
) -> Any:
    """Get a nested configuration value using dot notation.

    This function is kept for backward compatibility with callers that need
    raw dict-style access. New code should use get_infrastructure_config() directly.
    """
    cfg = get_infrastructure_config()
    # Walk the pydantic model using getattr for typed access
    keys = key_path.split(".")
    value: Any = cfg
    for key in keys:
        if isinstance(value, dict):
            if key in value:
                value = value[key]
            else:
                return default
        elif hasattr(value, key):
            value = getattr(value, key)
        else:
            return default
    return value


def get_config(
    key: str, default: Any = None, config_path: Optional[Any] = None
) -> Any:
    """Get a top-level configuration value from config.yaml.

    Kept for backward compatibility. New code should use get_infrastructure_config().
    """
    return get_nested_config(key, default)


# =============================================================================
# LangSmith Tracing Configuration
# =============================================================================


def get_langsmith_tags(
    msg_type: str,
    locale: Optional[str] = None,
) -> List[str]:
    """Generate LangSmith tags for workflow tracing."""
    tags = []

    workflow_map = {
        "technical_analysis": "workflow:technical_analysis",
        "fundamental_analysis": "workflow:fundamental_analysis",
        "podcast_generation": "workflow:podcast_generation",
    }
    tags.append(workflow_map.get(msg_type, "workflow:chat"))

    if locale:
        tags.append(f"locale:{locale}")

    return tags


def get_langsmith_metadata(
    user_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    thread_id: Optional[str] = None,
    workflow_type: Optional[str] = None,
    locale: Optional[str] = None,
    timezone: Optional[str] = None,
    llm_model: Optional[str] = None,
    reasoning_effort: Optional[str] = None,
    fast_mode: Optional[bool] = None,
    plan_mode: bool = False,
    is_byok: bool = False,
    platform: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate LangSmith metadata for workflow tracing."""
    metadata = {}

    if user_id:
        metadata["user_id"] = user_id
    if workspace_id:
        metadata["workspace_id"] = workspace_id
    if thread_id:
        metadata["thread_id"] = thread_id
    if workflow_type:
        metadata["workflow_type"] = workflow_type
    if locale:
        metadata["locale"] = locale
    if timezone:
        metadata["timezone"] = timezone
    if llm_model:
        metadata["llm_model"] = llm_model
    if reasoning_effort:
        metadata["reasoning_effort"] = reasoning_effort
    if fast_mode is not None:
        metadata["fast_mode"] = fast_mode
    if plan_mode:
        metadata["plan_mode"] = plan_mode
    if is_byok:
        metadata["is_byok"] = is_byok
    if platform:
        metadata["platform"] = platform

    return metadata
