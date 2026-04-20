"""
Cache management endpoints.

This module handles:
- Cache statistics retrieval
- Cache invalidation (pattern-based or nuclear)
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.utils.cache.invalidation import get_cache_invalidator
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)
INTERNAL_SERVER_ERROR_DETAIL = "Internal Server Error"

# Create router
router = APIRouter(prefix="/api/v1/cache", tags=["Cache"])


@router.get("/stats")
async def get_cache_stats():
    """
    Get cache statistics and performance metrics.

    Returns cache hit/miss rates, total requests, and health status.
    Useful for monitoring cache performance.
    """
    try:
        cache = get_cache_client()
        stats = cache.get_stats()
        health = await cache.health_check()

        return {
            **stats,
            "healthy": health,
        }

    except Exception as e:
        logger.exception(f"Error in get_cache_stats endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve cache stats.")


@router.post("/clear")
async def clear_cache(
    pattern: Optional[str] = Query(None, description="Cache key pattern to clear (e.g., 'workflow:*'). If not provided, clears ALL caches.")
):
    """
    Clear cache entries (admin endpoint).

    WARNING: Use with caution - this will clear cached data.
    """
    try:
        invalidator = get_cache_invalidator()

        if pattern:
            # Clear specific pattern
            results = await invalidator.invalidate_patterns([pattern])
            total_deleted = sum(results.values())
            return {
                "message": f"Cleared {total_deleted} cache entries matching pattern: {pattern}",
                "deleted": total_deleted,
                "pattern": pattern,
            }
        else:
            # Nuclear option - clear all
            success = await invalidator.invalidate_all()
            return {
                "message": "Cleared ALL caches",
                "success": success,
            }

    except Exception as e:
        logger.exception(f"Error in clear_cache endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to clear cache.")