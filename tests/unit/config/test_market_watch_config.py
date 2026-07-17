"""Market watch settings defaults (the on/off flag lives in the features registry)."""

from src.config.settings import (
    get_market_watch_cache_pin,
    get_market_watch_max_symbols,
    get_market_watch_min_interval,
    get_redis_ttl_market_watch,
)


def test_defaults():
    assert get_market_watch_min_interval() == 25
    assert get_market_watch_max_symbols() == 10
    assert get_market_watch_cache_pin() is True
    assert get_redis_ttl_market_watch() == 21600
