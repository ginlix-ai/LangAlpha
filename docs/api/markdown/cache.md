# Cache API

## Overview

The Cache API provides endpoints for monitoring cache performance and managing cached data. Use these endpoints to check cache health, view statistics, and clear caches when needed.

## Endpoints

### Get Cache Statistics

`GET /api/v1/cache/stats`

Get cache statistics and performance metrics. Returns cache hit/miss rates, total requests, and health status.

**Response** `200 OK`

```json
{
  "hits": 1500,
  "misses": 300,
  "total_requests": 1800,
  "hit_rate": 0.833,
  "healthy": true
}
```

| Field | Type | Description |
|-------|------|-------------|
| hits | integer | Number of cache hits |
| misses | integer | Number of cache misses |
| total_requests | integer | Total cache requests |
| hit_rate | float | Hit rate (0.0 - 1.0) |
| healthy | boolean | Cache health status |

**Example**

```bash
curl "http://localhost:8000/api/v1/cache/stats"
```

---

### Clear Cache

`POST /api/v1/cache/clear`

Clear cache entries. **Admin endpoint** - use with caution as this will remove cached data.

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| pattern | string | null | Cache key pattern to clear (e.g., "workflow:*"). If not provided, clears ALL caches. |

**Response** `200 OK` (with pattern)

```json
{
  "message": "Cleared 42 cache entries matching pattern: workflow:*",
  "deleted": 42,
  "pattern": "workflow:*"
}
```

**Response** `200 OK` (without pattern - nuclear option)

```json
{
  "message": "Cleared ALL caches",
  "success": true
}
```

**Common Patterns:**

| Pattern | Description |
|---------|-------------|
| `workflow:*` | All workflow-related caches |
| `session:*` | All session caches |
| `user:*` | All user-related caches |
| `workspace:*` | All workspace caches |

**Example - Clear specific pattern:**

```bash
curl -X POST "http://localhost:8000/api/v1/cache/clear?pattern=workflow:*"
```

**Example - Clear all caches:**

```bash
curl -X POST "http://localhost:8000/api/v1/cache/clear"
```

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 500 | INTERNAL_ERROR | Failed to clear cache |
