"""Market watch settings defaults."""

from src.config.settings import (
    get_market_watch_max_symbols,
    get_market_watch_min_interval,
    get_redis_ttl_market_watch,
)


def test_defaults():
    assert get_market_watch_min_interval() == 25
    assert get_market_watch_max_symbols() == 10
    assert get_redis_ttl_market_watch() == 21600
