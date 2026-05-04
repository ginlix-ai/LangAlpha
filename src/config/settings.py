"""
Centralized configuration access.

Credentials come from environment variables (.env). Infrastructure settings
come from config.yaml via InfrastructureConfig. Agent/tool settings come from
agent_config.yaml via AgentConfig (ptc_agent.config).
"""

import logging
import os
from typing import Any, Dict, List, Optional

from src.config.core import get_infrastructure_config

# Re-export env-var constants for backward compatibility
from src.config.env import (  # noqa: F401
    AUTH_SERVICE_URL,
    AUTOMATION_WEBHOOK_SECRET,
    AUTOMATION_WEBHOOK_URL,
    GINLIX_DATA_ENABLED,
    GINLIX_DATA_URL,
    GINLIX_DATA_WS_URL,
    HOST_MODE,
    LOCAL_DEV_USER_ID,
    SUPABASE_URL,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Application Settings — delegates to InfrastructureConfig
# =============================================================================


def get_debug_mode() -> bool:
    return get_infrastructure_config().debug


def get_ptc_recursion_limit() -> int:
    return get_infrastructure_config().ptc_recursion_limit


def get_flash_recursion_limit() -> int:
    return get_infrastructure_config().flash_recursion_limit


def get_workflow_timeout() -> int:
    """Workflow timeout in seconds."""
    return get_infrastructure_config().workflow_timeout


def get_sse_keepalive_interval() -> float:
    """SSE keepalive interval in seconds."""
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
    return get_infrastructure_config().result_log_db_enabled


def is_redis_warm_on_startup_enabled() -> bool:
    return get_infrastructure_config().redis_warm_on_startup


def is_langsmith_tracing_enabled() -> bool:
    return get_infrastructure_config().langsmith_tracing


# =============================================================================
# SSE Event Logging
# =============================================================================


def is_sse_event_log_enabled() -> bool:
    return get_infrastructure_config().sse_event_log_enabled


def get_sse_event_log_level() -> str:
    return get_infrastructure_config().sse_event_log_level


# =============================================================================
# General Application Logging
# =============================================================================


def get_log_level() -> str:
    return get_infrastructure_config().log_level.upper()


def get_log_format() -> str:
    return get_infrastructure_config().log_format


def get_module_log_levels() -> dict:
    levels = get_infrastructure_config().module_log_levels
    return {k: v.upper() for k, v in levels.items()} if levels else {}


# =============================================================================
# CORS Settings
# =============================================================================


def get_allowed_origins() -> List[str]:
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
    return get_infrastructure_config().redis.cache_enabled


def get_redis_max_connections() -> int:
    """Get Redis connection pool max connections.

    Env var REDIS_MAX_CONNECTIONS overrides config.yaml so operators can
    bump the pool size without a redeploy when diagnosing pool exhaustion.
    """
    env_override = os.getenv("REDIS_MAX_CONNECTIONS")
    if env_override:
        try:
            parsed = int(env_override)
        except ValueError:
            # A typo during incident response would otherwise silently ignore
            # the operator's intent and use the YAML default — log loudly.
            logger.warning(
                "REDIS_MAX_CONNECTIONS=%r is not an integer; "
                "falling back to config.yaml value",
                env_override,
            )
        else:
            # Bound-check: 0 is a footgun (redis-py silently coerces to a
            # 31-bit cap), negatives break the pool, huge values exhaust fds.
            if 1 <= parsed <= 10000:
                return parsed
            logger.warning(
                "REDIS_MAX_CONNECTIONS=%d is outside the safe range [1, 10000]; "
                "falling back to config.yaml value",
                parsed,
            )
    return get_infrastructure_config().redis.max_connections


def get_redis_socket_timeout() -> int:
    """Socket read/write timeout in seconds."""
    return get_infrastructure_config().redis.socket_timeout


def _env_pool_size(env_var: str, default: int, ceiling: int = 500) -> int:
    """Read a pool-size env var with bounds check; fall back to `default`."""
    raw = os.getenv(env_var)
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        logger.warning("%s=%r is not an integer; using default=%d", env_var, raw, default)
        return default
    if 1 <= parsed <= ceiling:
        return parsed
    logger.warning("%s=%d outside [1, %d]; using default=%d", env_var, parsed, ceiling, default)
    return default


def get_checkpointer_pool_max() -> int:
    """Env POSTGRES_CHECKPOINTER_POOL_MAX, default 25."""
    return _env_pool_size("POSTGRES_CHECKPOINTER_POOL_MAX", default=25)


def get_conversation_pool_max() -> int:
    """Env POSTGRES_CONVERSATION_POOL_MAX, default 50."""
    return _env_pool_size("POSTGRES_CONVERSATION_POOL_MAX", default=50)


def get_redis_socket_connect_timeout() -> int:
    return get_infrastructure_config().redis.socket_connect_timeout


def get_redis_ttl_results_list() -> int:
    return get_infrastructure_config().redis.ttl.results_list


def get_redis_ttl_result_detail() -> int:
    return get_infrastructure_config().redis.ttl.result_detail


def get_redis_ttl_metadata() -> int:
    return get_infrastructure_config().redis.ttl.metadata


def get_redis_ttl_metadata_summary() -> int:
    return get_infrastructure_config().redis.ttl.metadata_summary


def get_redis_ttl_workflow_status() -> int:
    return get_infrastructure_config().redis.ttl.workflow_status


def get_redis_ttl_cancel_flag() -> int:
    return get_infrastructure_config().redis.ttl.cancel_flag


def get_redis_ttl_steering() -> int:
    return get_infrastructure_config().redis.ttl.steering


def get_redis_ttl_memo_metadata_inflight() -> int:
    """Cross-worker visibility key TTL for in-flight memo metadata tasks (seconds)."""
    return get_infrastructure_config().redis.ttl.memo_metadata_inflight


def get_redis_ttl_memo_metadata_cancel() -> int:
    """Cooperative cross-worker memo metadata cancel flag TTL (seconds)."""
    return get_infrastructure_config().redis.ttl.memo_metadata_cancel


def is_cache_invalidate_on_write_enabled() -> bool:
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
    return get_infrastructure_config().background_execution.max_concurrent_workflows


def get_workflow_result_ttl() -> int:
    return get_infrastructure_config().background_execution.workflow_result_ttl


def get_abandoned_workflow_timeout() -> int:
    """Timeout in seconds for workflows with no active connections."""
    return get_infrastructure_config().background_execution.abandoned_workflow_timeout


def get_cleanup_interval() -> int:
    return get_infrastructure_config().background_execution.cleanup_interval


def is_intermediate_storage_enabled() -> bool:
    return get_infrastructure_config().background_execution.enable_intermediate_storage


def get_max_stored_messages_per_agent() -> int:
    return get_infrastructure_config().background_execution.max_stored_messages_per_agent


def get_subagent_collector_timeout() -> float:
    return get_infrastructure_config().background_execution.subagent_collector_timeout


def get_subagent_orphan_collector_timeout() -> float:
    """Orphan collector idle timeout; resets on any subagent progress."""
    return get_infrastructure_config().background_execution.subagent_orphan_collector_timeout


def get_event_storage_backend() -> str:
    return get_infrastructure_config().background_execution.event_storage_backend


def get_subagent_task_max_wait() -> int:
    return get_infrastructure_config().background_execution.subagent_task_max_wait


def get_in_memory_event_tail_max_events() -> int:
    """Max captured-event records held in the per-subagent in-memory hot tail."""
    return get_infrastructure_config().background_execution.in_memory_event_tail_max_events


def is_subagent_event_redis_spill_enabled() -> bool:
    """Kill-switch for the per-event Redis spill of subagent captured events."""
    return get_infrastructure_config().background_execution.spill_subagent_events_to_redis


def get_sse_drain_timeout() -> float:
    return get_infrastructure_config().background_execution.sse_drain_timeout


def get_shutdown_timeout() -> float:
    return get_infrastructure_config().background_execution.shutdown_timeout


def get_checkpoint_flush_timeout() -> float:
    return get_infrastructure_config().background_execution.checkpoint_flush_timeout


def get_wait_for_persistence_timeout() -> float:
    return get_infrastructure_config().background_execution.wait_for_persistence_timeout


def get_soft_interrupt_wait_timeout() -> float:
    return get_infrastructure_config().background_execution.soft_interrupt_wait_timeout


def get_max_workflow_retries() -> int:
    return get_infrastructure_config().background_execution.max_workflow_retries


def get_merged_chunk_max_bytes() -> int:
    return get_infrastructure_config().background_execution.merged_chunk_max_bytes


def get_redis_ttl_workflow_events() -> int:
    return get_infrastructure_config().redis.ttl.workflow_events


# =============================================================================
# Nested config accessor — kept for remaining callers
# =============================================================================


def get_nested_config(
    key_path: str, default: Any = None, config_path: Optional[Any] = None
) -> Any:
    """Dot-notation config lookup; kept for backward-compat callers."""
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
    """Top-level config lookup; kept for backward compatibility."""
    return get_nested_config(key, default)


# =============================================================================
# LangSmith Tracing Configuration
# =============================================================================


def get_langsmith_tags(
    msg_type: str,
    locale: Optional[str] = None,
) -> List[str]:
    """Build LangSmith tags list for a workflow run."""
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
    """Build LangSmith metadata dict for a workflow run (omits None values)."""
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
