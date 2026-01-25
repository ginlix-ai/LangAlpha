# Market Data API

## Overview

The Market Data API provides cached access to FMP (Financial Modeling Prep) intraday OHLCV data for stocks and indexes. It implements a Stale-While-Revalidate (SWR) caching pattern with Redis for optimal performance.

**Base path:** `/api/v1/market-data`

**Key features:**
- 60-second TTL caching with SWR background refresh
- Single and batch endpoints for stocks and indexes
- Multiple interval support (1min, 5min, 15min, 30min, 1hour, 4hour)
- Automatic cache key generation with interval and date range support

**Supported Intervals:**

| Asset Type | Intervals |
|------------|-----------|
| Stocks | 1min, 5min, 15min, 30min, 1hour, 4hour |
| Indexes | 1min, 5min, 1hour |

---

## Endpoints

### Get Stock Intraday Data

`GET /api/v1/market-data/intraday/stocks/{symbol}`

Retrieve intraday OHLCV data for a single stock symbol.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| symbol | string | Yes | Stock ticker symbol (e.g., AAPL, MSFT) |

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| interval | string | 1min | Data interval (1min, 5min, 15min, 30min, 1hour, 4hour) |
| from | string | null | Start date in YYYY-MM-DD format |
| to | string | null | End date in YYYY-MM-DD format |

**Response** `200 OK`

```json
{
  "symbol": "AAPL",
  "interval": "1min",
  "data": [
    {
      "date": "2024-01-15 09:30:00",
      "open": 185.50,
      "high": 185.75,
      "low": 185.25,
      "close": 185.60,
      "volume": 1500000
    }
  ],
  "count": 1,
  "cache": {
    "cached": true,
    "cache_key": "fmp:intraday:stock:symbol=AAPL:interval=1min",
    "ttl_remaining": 45,
    "refreshed_in_background": false
  }
}
```

**Example**

```bash
curl "http://localhost:8000/api/v1/market-data/intraday/stocks/AAPL?interval=5min&from=2024-01-15&to=2024-01-15"
```

---

### Get Batch Stock Intraday Data

`POST /api/v1/market-data/intraday/stocks`

Retrieve intraday OHLCV data for multiple stock symbols (max 50).

**Request Body**

```json
{
  "symbols": ["AAPL", "MSFT", "GOOGL"],
  "interval": "15min",
  "from": "2024-01-01",
  "to": "2024-01-15"
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| symbols | string[] | Yes | - | List of stock symbols (1-50) |
| interval | string | No | 1min | Data interval (1min, 5min, 15min, 30min, 1hour, 4hour) |
| from | string | No | null | Start date (YYYY-MM-DD) |
| to | string | No | null | End date (YYYY-MM-DD) |

**Response** `200 OK`

```json
{
  "interval": "15min",
  "results": {
    "AAPL": [
      {
        "date": "2024-01-15 09:30:00",
        "open": 185.50,
        "high": 185.75,
        "low": 185.25,
        "close": 185.60,
        "volume": 1500000
      }
    ],
    "MSFT": [
      {
        "date": "2024-01-15 09:30:00",
        "open": 375.00,
        "high": 375.50,
        "low": 374.80,
        "close": 375.25,
        "volume": 800000
      }
    ]
  },
  "errors": {
    "INVALID": "Symbol not found"
  },
  "cache_stats": {
    "total_requests": 3,
    "cache_hits": 2,
    "cache_misses": 1,
    "background_refreshes": 1
  }
}
```

**Example**

```bash
curl -X POST "http://localhost:8000/api/v1/market-data/intraday/stocks" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["AAPL", "MSFT", "GOOGL"],
    "interval": "15min",
    "from": "2024-01-15",
    "to": "2024-01-15"
  }'
```

---

### Get Index Intraday Data

`GET /api/v1/market-data/intraday/indexes/{symbol}`

Retrieve intraday OHLCV data for a single index symbol.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| symbol | string | Yes | Index symbol (e.g., ^GSPC, ^DJI, ^IXIC) |

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| interval | string | 1min | Data interval (1min, 5min, 1hour) |
| from | string | null | Start date in YYYY-MM-DD format |
| to | string | null | End date in YYYY-MM-DD format |

**Response** `200 OK`

```json
{
  "symbol": "GSPC",
  "interval": "1min",
  "data": [
    {
      "date": "2024-01-15 09:30:00",
      "open": 4780.50,
      "high": 4782.00,
      "low": 4779.25,
      "close": 4781.75,
      "volume": 0
    }
  ],
  "count": 1,
  "cache": {
    "cached": true,
    "cache_key": "fmp:intraday:index:symbol=GSPC:interval=1min",
    "ttl_remaining": 30,
    "refreshed_in_background": true
  }
}
```

**Note:** Index symbols are normalized by stripping the `^` prefix for cache keys and response (e.g., `^GSPC` becomes `GSPC`).

**Example**

```bash
curl "http://localhost:8000/api/v1/market-data/intraday/indexes/^GSPC?interval=1hour&from=2024-01-15&to=2024-01-15"
```

---

### Get Batch Index Intraday Data

`POST /api/v1/market-data/intraday/indexes`

Retrieve intraday OHLCV data for multiple index symbols (max 50).

**Request Body**

```json
{
  "symbols": ["^GSPC", "^DJI", "^IXIC"],
  "interval": "5min",
  "from": "2024-01-01",
  "to": "2024-01-15"
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| symbols | string[] | Yes | - | List of index symbols (1-50) |
| interval | string | No | 1min | Data interval (1min, 5min, 1hour) |
| from | string | No | null | Start date (YYYY-MM-DD) |
| to | string | No | null | End date (YYYY-MM-DD) |

**Response** `200 OK`

```json
{
  "interval": "5min",
  "results": {
    "GSPC": [...],
    "DJI": [...],
    "IXIC": [...]
  },
  "errors": {},
  "cache_stats": {
    "total_requests": 3,
    "cache_hits": 3,
    "cache_misses": 0,
    "background_refreshes": 0
  }
}
```

**Example**

```bash
curl -X POST "http://localhost:8000/api/v1/market-data/intraday/indexes" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["^GSPC", "^DJI"],
    "interval": "5min",
    "from": "2024-01-15",
    "to": "2024-01-15"
  }'
```

---

## Error Responses

| Status | Description |
|--------|-------------|
| 422 | Invalid interval for the asset type |
| 500 | Internal server error (API failure, FMP error) |

**Error Response Format**

```json
{
  "detail": "Error message describing what went wrong"
}
```

**Invalid Interval Example:**

```json
{
  "detail": "Invalid interval '4hour' for indexes. Supported: 1min, 5min, 1hour"
}
```

---

## Caching Behavior

The Market Data API uses a Stale-While-Revalidate (SWR) caching strategy:

| Setting | Value | Description |
|---------|-------|-------------|
| TTL | 60 seconds | Cache expiration time |
| Soft TTL Ratio | 0.5 | Triggers background refresh at 30s remaining |
| Max Concurrent Fetches | 10 | Semaphore limit for batch API calls |

**Cache Key Format:**
- Stocks: `fmp:intraday:stock:symbol={SYMBOL}:interval={INTERVAL}[:from={DATE}][:to={DATE}]`
- Indexes: `fmp:intraday:index:symbol={SYMBOL}:interval={INTERVAL}[:from={DATE}][:to={DATE}]`

**SWR Flow:**
1. If data is cached and TTL > 30s: Return cached data immediately
2. If data is cached and TTL < 30s: Return cached data + trigger background refresh
3. If data is not cached: Fetch from FMP API and cache result

The `cache` metadata in responses indicates:
- `cached`: Whether data was served from cache
- `ttl_remaining`: Seconds until cache expires
- `refreshed_in_background`: Whether a background refresh was triggered
