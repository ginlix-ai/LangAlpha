# PTC Agent API Reference

## Overview

Base URL: `http://localhost:8000`
Version: 0.1.0

The PTC Agent API provides endpoints for interacting with the PTC (Plan-Think-Code) AI agent system. The agent executes code in isolated Daytona sandboxes and supports real-time streaming responses via Server-Sent Events (SSE).

## Bruno Collection

Open this folder (`docs/api/`) directly in Bruno to test API endpoints interactively.

**Structure:**
```
docs/api/
├── opencollection.yml           # Collection root
├── environments/
│   └── development.yml          # Local development (localhost:8000)
├── 00-health/                   # Health check
├── 15-threads/                  # Thread CRUD, messages, SSE streaming, workflow control
├── 30-workspaces/               # Workspace CRUD & lifecycle
├── 35-workspace-files/          # Sandbox file operations
├── 37-workspace-sandbox/        # Sandbox stats, packages, previews
├── 38-vault/                    # Workspace secrets management
├── 39-sessions/                 # Active session stats
├── 50-users/                    # User management & auth sync
├── 52-api-keys/                 # BYOK API key management & model listing
├── 55-portfolio/                # Portfolio holdings
├── 58-oauth/                    # OAuth flows (Codex device code, Claude PKCE)
├── 60-watchlist/                # Watchlist CRUD
├── 65-automations/              # Scheduled automation CRUD & execution
├── 70-market-data/              # Market data (intraday, daily, snapshots, search)
├── 72-news/                     # News feed & articles
├── 74-calendar/                 # Economic & earnings calendar
├── 78-insights/                 # AI market insights
├── 79-sec-proxy/                # SEC EDGAR document proxy
├── 80-cache/                    # Cache management
├── 85-public/                   # Public shared thread access
├── 87-skills/                   # Agent skills listing
└── 90-websocket/                # Real-time market data WebSocket
```

**Getting Started with Bruno:**
1. Install [Bruno](https://www.usebruno.com/)
2. Open this folder as a collection
3. Select "development" environment
4. Create a workspace via `30-workspaces/create-workspace.yml`
5. Send a message via `15-threads/create-thread-message.yml`

---

## Quick Start: Complete API Flow

### Step 1: Create a Workspace

```bash
curl -X POST "http://localhost:8000/api/v1/workspaces" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-123" \
  -d '{"name": "My Project"}'
```

### Step 2: Start a Chat

Create a new thread and send the first message (SSE stream):

```bash
curl -N -X POST "http://localhost:8000/api/v1/threads/messages" \
  -H "Content-Type: application/json" \
  -d '{
    "workspace_id": "ws-abc123",
    "messages": [{"role": "user", "content": "Create a Python script that prints Hello World"}]
  }'
```

The first SSE frame is `event: metadata` carrying `{thread_id, run_id}` — the canonical per-turn identity. The HTTP response also sets `Content-Location: /api/v1/threads/{thread_id}/messages/stream?run_id={run_id}` so the client knows the exact reconnect URL for this turn.

### Step 3: Continue the Conversation

```bash
curl -N -X POST "http://localhost:8000/api/v1/threads/THREAD_ID/messages" \
  -H "Content-Type: application/json" \
  -d '{
    "workspace_id": "ws-abc123",
    "messages": [{"role": "user", "content": "Now run the script"}]
  }'
```

### Step 4: Reconnect if Disconnected

Reconnect targets a specific run via `run_id`. If omitted, the server falls back to the latest run on the thread.

```bash
curl -N "http://localhost:8000/api/v1/threads/THREAD_ID/messages/stream?run_id=RUN_ID&last_event_id=42"
```

### Step 5: Check Status

```bash
curl "http://localhost:8000/api/v1/threads/THREAD_ID/status"
```

---

## Resuming a Historical Conversation

### Step 1: List Threads

```bash
curl "http://localhost:8000/api/v1/threads?limit=50" \
  -H "X-User-Id: user-123"
```

### Step 2: Replay

```bash
curl -N "http://localhost:8000/api/v1/threads/THREAD_ID/messages/replay"
```

### Step 3: Continue

```bash
curl -N -X POST "http://localhost:8000/api/v1/threads/THREAD_ID/messages" \
  -H "Content-Type: application/json" \
  -d '{
    "workspace_id": "ws-abc123",
    "messages": [{"role": "user", "content": "Continue from where we left off"}]
  }'
```

---

## Authentication

User identification is handled via:
- Bearer JWT token (when Supabase auth is enabled)
- `X-User-Id` header (for workspace/user endpoints)

## API Groups

| Group | Description | Prefix |
|-------|-------------|--------|
| Health | Service health check | `/health` |
| Threads | Thread CRUD, SSE chat, workflow control, sharing, feedback | `/api/v1/threads` |
| Workspaces | Workspace CRUD & lifecycle | `/api/v1/workspaces` |
| Workspace Files | Sandbox file read/write/upload/download | `/api/v1/workspaces/{id}/files` |
| Workspace Sandbox | Sandbox stats, packages, preview URLs | `/api/v1/workspaces/{id}/sandbox` |
| Vault | Workspace secrets management | `/api/v1/workspaces/{id}/vault` |
| Sessions | Active PTC session stats | `/api/v1/sessions` |
| Users | User profile & preferences | `/api/v1/users` |
| API Keys | BYOK key management & model listing | `/api/v1/users/me/api-keys` |
| OAuth | Codex & Claude OAuth flows | `/api/v1/oauth` |
| Portfolio | Portfolio holdings CRUD | `/api/v1/users/me/portfolio` |
| Watchlist | Watchlist & items CRUD | `/api/v1/users/me/watchlists` |
| Automations | Scheduled automation CRUD & execution | `/api/v1/automations` |
| Market Data | Intraday, daily, snapshots, search, overview | `/api/v1/market-data` |
| News | News feed & articles | `/api/v1/news` |
| Calendar | Economic & earnings calendar | `/api/v1/calendar` |
| Insights | AI market insights | `/api/v1/insights` |
| SEC Proxy | SEC EDGAR document proxy | `/api/v1/sec-proxy` |
| Cache | Cache stats & management | `/api/v1/cache` |
| Public | Shared thread access (no auth) | `/api/v1/public` |
| Skills | Agent skills listing | `/api/v1/skills` |
| WebSocket | Real-time market data streaming | `/ws/v1/market-data` |

## SSE Event Types

The streaming endpoints emit Server-Sent Events. Key event types:
- `metadata` — first frame of every stream, carries `{thread_id, run_id}` for reconnect/steering
- `message_chunk` / `reasoning_content` / `reasoning_signal` — text + reasoning streaming
- `tool_calls` / `tool_call_chunks` / `tool_call_result` — tool execution
- `artifact` — file operations and outputs
- `provenance` — record of external data the agent accessed (one per source). Flat payload: `record_id`, `source_type`, `identifier`, `title?`, `provider?`, `tool_call_id?`, `args_fingerprint?`, `result_sha256?`, `result_size?`, `result_snippet?`, `timestamp`, plus `agent` (`main`/`task:{id}`, resolved server-side from the LangGraph namespace)
- `user_message` — echo of user input (including mid-turn steering)
- `workflow_status` / `thread_created` — lifecycle signals
- `steering_delivered` / `task_steering_accepted` — steering acknowledgements
- `interrupt` — human-in-the-loop pause
- `error` / `keepalive` — control events
- `finish` — workflow completion

## Versioning

All API endpoints are versioned with the `/api/v1/` prefix.
