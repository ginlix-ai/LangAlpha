# PTC Agent API Collection

Bruno API collection for testing PTC Agent server endpoints.

## Prerequisites

- [Bruno](https://www.usebruno.com/) - API client for testing

## Usage

1. Open Bruno
2. Click "Open Collection"
3. Navigate to `docs/ptc-agent-api/` and select the folder
4. The collection will load with all available endpoints

## Folder Structure

```
ptc-agent-api/
├── opencollection.yml    # Collection configuration
├── README.md             # This file
└── <feature>/            # Feature-specific endpoints
    ├── folder.yml        # Folder metadata
    └── <Request>.yml     # Individual request files
```

## Adding New Endpoints

### Creating a New Feature Folder

1. Create a folder with the feature name (e.g., `authentication/`)
2. Add a `folder.yml` with the folder metadata:

```yaml
name: Authentication
```

### Creating a New Request

1. Create a `.yml` file in the appropriate feature folder
2. Use Title Case for the filename (e.g., `Get User.yml`)

## Request File Schema

### Full Example

```yaml
info:
  name: Stock Batch
  type: http
  seq: 1

http:
  method: POST
  url: http://localhost:8000/api/v1/market-data/intraday/stocks
  params:
    - name: symbol
      value: AAPL
      type: path
  headers:
    - name: Content-Type
      value: application/json
    - name: Authorization
      value: Bearer {{token}}
  body:
    type: json
    data: |-
      {
        "symbols": ["AAPL", "MSFT"]
      }
  auth: inherit

settings:
  encodeUrl: true
  timeout: 0
  followRedirects: true
  maxRedirects: 5
```

### Schema Reference

#### `info` (required)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Display name for the request |
| `type` | string | Yes | Always `http` for HTTP requests |
| `seq` | number | No | Order within folder (lower = first) |

#### `http` (required)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `method` | string | Yes | HTTP method: `GET`, `POST`, `PUT`, `PATCH`, `DELETE` |
| `url` | string | Yes | Full URL with path parameters as `:param` |
| `params` | array | No | URL parameters (path or query) |
| `headers` | array | No | Request headers |
| `body` | object | No | Request body configuration |
| `auth` | string | No | Auth mode: `inherit`, `none`, or auth config |

#### `http.params[]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Parameter name |
| `value` | string | Yes | Parameter value (can use `{{var}}`) |
| `type` | string | Yes | `path` or `query` |

#### `http.headers[]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Header name |
| `value` | string | Yes | Header value (can use `{{var}}`) |

#### `http.body`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Body type: `json`, `text`, `xml`, `formUrlEncoded`, `multipartForm`, `none` |
| `data` | string | Yes | Body content (use `|-` for multiline JSON) |

#### `settings` (optional)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `encodeUrl` | boolean | `true` | URL-encode special characters |
| `timeout` | number | `0` | Request timeout in ms (0 = no timeout) |
| `followRedirects` | boolean | `true` | Follow HTTP redirects |
| `maxRedirects` | number | `5` | Maximum redirects to follow |

### Minimal Examples

**GET with path parameter:**

```yaml
info:
  name: Get Stock
  type: http

http:
  method: GET
  url: http://localhost:8000/api/v1/stocks/:symbol
  params:
    - name: symbol
      value: AAPL
      type: path
  auth: inherit
```

**POST with JSON body:**

```yaml
info:
  name: Create Order
  type: http

http:
  method: POST
  url: http://localhost:8000/api/v1/orders
  headers:
    - name: Content-Type
      value: application/json
  body:
    type: json
    data: |-
      {
        "symbol": "AAPL",
        "quantity": 100
      }
  auth: inherit
```

## Naming Conventions

| Item | Convention | Example |
|------|------------|---------|
| Folder (filesystem) | lowercase with spaces | `market data/` |
| Folder name (in folder.yml) | Title Case | `Market Data` |
| Request file | Title Case | `Stock Batch.yml` |
| Request name | Title Case | `Stock Batch` |

## Environment Variables

Configure these in Bruno's environment settings:

| Variable | Description | Default |
|----------|-------------|---------|
| `base_url` | Server base URL | `http://localhost:8000` |
