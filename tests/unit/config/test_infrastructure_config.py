"""
Tests for InfrastructureConfig (src/config/models.py) — typed config.yaml model.

Covers:
- InfrastructureConfig loads all sections from config.yaml
- BackgroundExecutionConfig subagent timeouts
- MarketDataConfig / NewsDataConfig
- settings.py accessors delegate to InfrastructureConfig (backward compat)
"""

from unittest.mock import patch

import pytest

from src.config.models import (
    BackgroundExecutionConfig,
    InfrastructureConfig,
    MarketDataConfig,
    MarketDataProviderConfig,
    NewsDataConfig,
    RedisConfig,
)


# ---------------------------------------------------------------------------
# New model fields
# ---------------------------------------------------------------------------


class TestMarketDataConfig:
    def test_defaults(self):
        cfg = MarketDataConfig()
        assert cfg.providers == []

    def test_with_providers(self):
        cfg = MarketDataConfig(providers=[
            MarketDataProviderConfig(name="fmp", markets=["all"]),
            MarketDataProviderConfig(name="ginlix-data", markets=["us"]),
        ])
        assert len(cfg.providers) == 2
        assert cfg.providers[0].name == "fmp"
        assert cfg.providers[1].markets == ["us"]


class TestNewsDataConfig:
    def test_defaults(self):
        cfg = NewsDataConfig()
        assert cfg.providers == []


class TestBackgroundExecutionSubagentTimeouts:
    def test_subagent_collector_timeout(self):
        cfg = BackgroundExecutionConfig(subagent_collector_timeout=60)
        assert cfg.subagent_collector_timeout == 60

    def test_subagent_orphan_collector_timeout(self):
        cfg = BackgroundExecutionConfig(subagent_orphan_collector_timeout=300)
        assert cfg.subagent_orphan_collector_timeout == 300

    def test_defaults(self):
        cfg = BackgroundExecutionConfig()
        assert cfg.subagent_collector_timeout == 120
        assert cfg.subagent_orphan_collector_timeout == 600


class TestInfrastructureConfigComplete:
    def test_extra_fields_allowed(self):
        """InfrastructureConfig allows extra fields (forward compat)."""
        cfg = InfrastructureConfig(unknown_future_field="hello")
        assert cfg.unknown_future_field == "hello"

    def test_market_data_defaults(self):
        cfg = InfrastructureConfig()
        assert isinstance(cfg.market_data, MarketDataConfig)
        assert cfg.market_data.providers == []

    def test_news_data_defaults(self):
        cfg = InfrastructureConfig()
        assert isinstance(cfg.news_data, NewsDataConfig)

    def test_full_config_yaml_shape(self):
        """Test loading a dict that mirrors real config.yaml structure."""
        data = {
            "debug": True,
            "workflow_timeout": 1600,
            "sse_keepalive_interval": 30,
            "agent_recursion_limit": 50,
            "allowed_origins": ["http://localhost:3000"],
            "log_level": "DEBUG",
            "background_execution": {
                "max_concurrent_workflows": 50,
                "subagent_collector_timeout": 60,
                "subagent_orphan_collector_timeout": 300,
            },
            "redis": {
                "cache_enabled": True,
                "max_connections": 20,
            },
            "market_data": {
                "providers": [
                    {"name": "ginlix-data", "markets": ["us"]},
                    {"name": "fmp", "markets": ["all"]},
                ],
            },
            "news_data": {
                "providers": [
                    {"name": "ginlix-data"},
                    {"name": "fmp"},
                ],
            },
        }
        cfg = InfrastructureConfig(**data)
        assert cfg.debug is True
        assert cfg.workflow_timeout == 1600
        assert cfg.background_execution.subagent_collector_timeout == 60
        assert len(cfg.market_data.providers) == 2
        assert cfg.market_data.providers[0].name == "ginlix-data"
        assert len(cfg.news_data.providers) == 2


# ---------------------------------------------------------------------------
# settings.py backward compatibility (accessors delegate to typed config)
# ---------------------------------------------------------------------------


class TestSettingsBackwardCompat:
    """Verify settings.py accessor functions still return correct values."""

    def _patch_infra_config(self, **overrides):
        """Return a patch context that makes get_infrastructure_config return a custom config."""
        cfg = InfrastructureConfig(**overrides)
        return patch("src.config.settings.get_infrastructure_config", return_value=cfg)

    def test_get_workflow_timeout(self):
        from src.config.settings import get_workflow_timeout
        with self._patch_infra_config(workflow_timeout=999):
            assert get_workflow_timeout() == 999

    def test_get_debug_mode(self):
        from src.config.settings import get_debug_mode
        with self._patch_infra_config(debug=True):
            assert get_debug_mode() is True

    def test_get_allowed_origins(self):
        from src.config.settings import get_allowed_origins
        with self._patch_infra_config(allowed_origins=["http://example.com"]):
            assert get_allowed_origins() == ["http://example.com"]

    def test_get_redis_max_connections(self):
        from src.config.settings import get_redis_max_connections
        with self._patch_infra_config(redis={"cache_enabled": True, "max_connections": 42}):
            assert get_redis_max_connections() == 42

    def test_is_redis_cache_enabled(self):
        from src.config.settings import is_redis_cache_enabled
        with self._patch_infra_config(redis={"cache_enabled": False}):
            assert is_redis_cache_enabled() is False

    def test_get_market_data_providers(self):
        from src.config.settings import get_market_data_providers
        providers = [{"name": "fmp", "markets": ["all"]}]
        with self._patch_infra_config(market_data={"providers": providers}):
            result = get_market_data_providers()
            assert result[0]["name"] == "fmp"

    def test_get_sse_keepalive_interval(self):
        from src.config.settings import get_sse_keepalive_interval
        with self._patch_infra_config(sse_keepalive_interval=30):
            assert get_sse_keepalive_interval() == 30

    def test_background_execution_accessors(self):
        from src.config.settings import (
            get_max_concurrent_workflows,
            get_subagent_collector_timeout,
        )
        bg = {"max_concurrent_workflows": 25, "subagent_collector_timeout": 60}
        with self._patch_infra_config(background_execution=bg):
            assert get_max_concurrent_workflows() == 25
            assert get_subagent_collector_timeout() == 60
