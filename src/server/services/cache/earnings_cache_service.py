"""Simple Redis TTL cache for earnings calendar data (daily refresh)."""

from __future__ import annotations

import json
import logging
from typing import Any

from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

_TTL = 86400  # 24 hours


def _cache_key(from_date: str, to_date: str) -> str:
    return f"earnings:v2:{from_date}:{to_date}"


class EarningsCacheService:
    _instance: EarningsCacheService | None = None

    def __new__(cls) -> EarningsCacheService:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def get(self, from_date: str, to_date: str) -> list[dict[str, Any]] | None:
        try:
            cache = get_cache_client()
            raw = await cache.get(_cache_key(from_date, to_date))
            if raw is not None:
                return json.loads(raw)
        except Exception:
            logger.debug("earnings_cache.get.miss", exc_info=True)
        return None

    async def set(self, data: list[dict[str, Any]], from_date: str, to_date: str) -> None:
        try:
            cache = get_cache_client()
            await cache.set(_cache_key(from_date, to_date), json.dumps(data), ttl=_TTL)
        except Exception:
            logger.debug("earnings_cache.set.failed", exc_info=True)
