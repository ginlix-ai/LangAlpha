"""
Pydantic models for infrastructure configuration.

These models define the schema for config.yaml (infrastructure settings).
"""

from typing import Dict, List

from pydantic import BaseModel, ConfigDict, Field


class BackgroundExecutionConfig(BaseModel):
    """Configuration for background workflow execution."""

    max_concurrent_workflows: int = Field(
        default=100, description="Maximum number of concurrent background workflows"
    )
    workflow_result_ttl: int = Field(
        default=86400, description="Workflow result retention time in seconds (24 hours)"
    )
    abandoned_workflow_timeout: int = Field(
        default=3600,
        description="Auto-cleanup timeout for workflows with no active connections (1 hour)",
    )
    cleanup_interval: int = Field(
        default=300, description="Background cleanup task interval in seconds (5 minutes)"
    )
    enable_intermediate_storage: bool = Field(
        default=True, description="Store intermediate results during execution"
    )
    max_stored_messages_per_agent: int = Field(
        default=150000, description="Maximum events to buffer per workflow"
    )
    event_storage_backend: str = Field(
        default="redis", description='Backend for event buffering: "redis" or "memory"'
    )
    event_storage_fallback_to_memory: bool = Field(
        default=True, description="Fallback to in-memory storage if Redis fails"
    )
    subagent_collector_timeout: int = Field(
        default=120, description="Initial subagent collector timeout in seconds"
    )
    subagent_orphan_collector_timeout: int = Field(
        default=600, description="Orphan subagent collector idle timeout in seconds"
    )


class RedisTTLConfig(BaseModel):
    """Redis TTL settings for various cache types."""

    results_list: int = Field(default=300, description="Results list cache TTL (5 minutes)")
    result_detail: int = Field(default=900, description="Result detail cache TTL (15 minutes)")
    metadata: int = Field(default=900, description="Metadata tags/tickers cache TTL (15 minutes)")
    metadata_summary: int = Field(
        default=600, description="Metadata summary cache TTL (10 minutes)"
    )
    workflow_events: int = Field(
        default=86400, description="Workflow event buffer TTL (24 hours)"
    )
    ohlcv: Dict[str, int] = Field(
        default_factory=dict, description="Per-interval OHLCV cache TTLs"
    )


class RedisSWRConfig(BaseModel):
    """Stale-While-Revalidate configuration for Redis cache."""

    enabled: bool = Field(default=True, description="Enable SWR for cache reads")
    soft_ttl_ratio: float = Field(
        default=0.6,
        description="Refresh when remaining TTL < this ratio of original",
    )
    warm_after_invalidation: bool = Field(
        default=True, description="Pre-populate cache after invalidation"
    )


class RedisConfig(BaseModel):
    """Redis cache configuration."""

    cache_enabled: bool = Field(default=True, description="Enable/disable caching globally")
    max_connections: int = Field(default=10, description="Connection pool size")
    ttl: RedisTTLConfig = Field(default_factory=RedisTTLConfig)
    cache_invalidate_on_write: bool = Field(
        default=True, description="Invalidate cache on writes"
    )
    swr: RedisSWRConfig = Field(default_factory=RedisSWRConfig)


class MarketDataProviderConfig(BaseModel):
    """Configuration for a single market data provider."""

    name: str
    markets: List[str] = Field(default_factory=lambda: ["all"])


class MarketDataConfig(BaseModel):
    """Market data provider chain configuration."""

    providers: List[MarketDataProviderConfig] = Field(default_factory=list)


class NewsDataConfig(BaseModel):
    """News data provider chain configuration."""

    providers: List[MarketDataProviderConfig] = Field(default_factory=list)


class InfrastructureConfig(BaseModel):
    """Root model for infrastructure configuration (config.yaml)."""

    model_config = ConfigDict(extra="allow")

    # Application Settings
    debug: bool = Field(default=False, description="Debug mode flag")
    agent_recursion_limit: int = Field(default=100, description="Agent recursion limit")
    workflow_timeout: int = Field(default=3200, description="Workflow timeout in seconds")
    sse_keepalive_interval: int = Field(
        default=15, description="SSE keepalive interval in seconds"
    )

    # Feature Flags
    result_log_db_enabled: bool = Field(
        default=True, description="Enable result logging to database"
    )
    redis_warm_on_startup: bool = Field(
        default=True, description="Enable Redis cache warming on startup"
    )
    langsmith_tracing: bool = Field(default=False, description="Enable LangSmith tracing")

    # SSE Event Logging
    sse_event_log_enabled: bool = Field(default=True, description="Enable SSE event logging")
    sse_event_log_level: str = Field(default="info", description="SSE event log level")

    # General Application Logging
    log_level: str = Field(default="error", description="Root logger level")
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string",
    )
    module_log_levels: Dict[str, str] = Field(
        default_factory=dict, description="Module-specific log levels"
    )

    # CORS Settings
    allowed_origins: List[str] = Field(
        default_factory=lambda: ["*"], description="Allowed CORS origins"
    )

    # Background Execution
    background_execution: BackgroundExecutionConfig = Field(
        default_factory=BackgroundExecutionConfig
    )

    # Redis Cache
    redis: RedisConfig = Field(default_factory=RedisConfig)

    # Market Data
    market_data: MarketDataConfig = Field(default_factory=MarketDataConfig)
    news_data: NewsDataConfig = Field(default_factory=NewsDataConfig)
