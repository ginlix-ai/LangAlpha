"""Market watch settings defaults (the on/off flag lives in the features registry)."""

from src.config.settings import (
    get_market_watch_max_symbols,
    get_market_watch_min_interval,
    get_market_watch_target_warning_minutes,
    get_redis_ttl_market_watch,
)


def test_defaults():
    assert get_market_watch_min_interval() == 25
    assert get_market_watch_max_symbols() == 10
    assert get_market_watch_target_warning_minutes() == 60
    assert get_redis_ttl_market_watch() == 21600
