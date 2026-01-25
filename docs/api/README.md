# PTC Agent API Reference

## Overview

Base URL: `http://localhost:8000`
Version: 0.1.0

The PTC Agent API provides endpoints for interacting with the PTC (Plan-Think-Code) AI agent system. The agent executes code in isolated Daytona sandboxes and supports real-time streaming responses via Server-Sent Events (SSE).

## Quick Start: Complete API Flow

This section demonstrates the typical workflow for using the PTC Agent API.

### Step 1: Create a Workspace

Create a workspace with a dedicated Daytona sandbox. This provides an isolated environment for code execution.

```bash
curl -X POST "http://localhost:8000/api/v1/workspaces" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-123" \
  -d '{
    "name": "My Project",
    "description": "Development workspace for my project"
  }'
```

**Response:**

```json
{
  "workspace_id": "ws-abc123-def456",
  "user_id": "user-123",
  "name": "My Project",
  "description": "Development workspace for my project",
  "sandbox_id": "sandbox-xyz789",
  "status": "running",
  "created_at": "2025-01-15T10:30:00Z",
  "updated_at": "2025-01-15T10:30:00Z"
}
```

### Step 2: Start a Chat Session

Use the workspace to run agent tasks via the streaming chat endpoint. The agent will execute code in the workspace's sandbox.

```bash
curl -N -X POST "http://localhost:8000/api/v1/chat/stream" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-123",
    "workspace_id": "ws-abc123-def456",
    "messages": [
      {
        "role": "user",
        "content": "Create a Python script that prints Hello World"
      }
    ]
  }'
```

**SSE Response Stream:**

```
event: message_chunk
data: {"content": "I'll create a simple Python script", "agent": "assistant"}

event: tool_calls
data: {"tool_name": "write_file", "arguments": {"path": "/workspace/hello.py", "content": "print('Hello World')"}, "tool_call_id": "call_001"}

event: artifact
data: {"artifact_type": "file_operation", "artifact_id": "call_001", "agent": "ptc", "status": "completed", "payload": {"operation": "write_file", "file_path": "/workspace/hello.py", "line_count": 1}}

event: tool_call_result
data: {"tool_name": "write_file", "result": "File created successfully", "tool_call_id": "call_001"}

event: done
data: {"status": "completed", "thread_id": "thread-xyz"}
```

**Note:** The response includes a `thread_id` which you'll need to reconnect if disconnected.

### Step 3: Reconnect if Disconnected

If your connection drops, you can reconnect to a running or completed workflow using the thread ID:

```bash
curl -N "http://localhost:8000/api/v1/chat/stream/thread-xyz/reconnect"
```

To avoid duplicate events, pass the last event ID you received:

```bash
curl -N "http://localhost:8000/api/v1/chat/stream/thread-xyz/reconnect?last_event_id=42"
```

The reconnect endpoint will:
1. Replay buffered events you may have missed
2. Continue streaming live events if the workflow is still running

### Step 4: Check Workflow Status (Optional)

Check the status of a workflow at any time:

```bash
curl "http://localhost:8000/api/v1/chat/status/thread-xyz"
```

**Response:**

```json
{
  "thread_id": "thread-xyz",
  "status": "completed",
  "workspace_id": "ws-abc123-def456",
  "sandbox_id": "sandbox-xyz789",
  "started_at": "2025-01-15T10:30:00Z",
  "completed_at": "2025-01-15T10:30:45Z"
}
```

### Step 5: Continue the Conversation

Send follow-up messages using the same `workspace_id`:

```bash
curl -N -X POST "http://localhost:8000/api/v1/chat/stream" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-123",
    "workspace_id": "ws-abc123-def456",
    "messages": [
      {
        "role": "user",
        "content": "Now run the script and show me the output"
      }
    ]
  }'
```

### Complete Flow Diagram

```
┌─────────────────┐      ┌──────────────────┐     ┌─────────────────┐
│  Create         │      │  Chat Stream     │     │  Reconnect      │
│  Workspace      │────▶ │ (with workspace) │────▶│  (if needed)    │
│ POST /workspaces│      │ POST /chat/stream│     │  GET /reconnect │
└─────────────────┘      └──────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌──────────────────┐
                        │  Continue Chat   │
                        │  (same workspace)│
                        │ POST /chat/stream│
                        └──────────────────┘
```

---

## Resuming a Historical Conversation

To display and continue from a past conversation, follow this workflow:

### Step 1: List User Conversations

Fetch the user's conversation history to get available `thread_id` and `workspace_id` values:

```bash
curl "http://localhost:8000/api/v1/conversations?limit=50" \
  -H "X-User-Id: user-123"
```

**Response:**

```json
{
  "threads": [
    {
      "thread_id": "thread-abc123",
      "workspace_id": "ws-xyz789",
      "first_query_content": "Create a Python script...",
      "current_status": "completed",
      "updated_at": "2025-01-15T10:35:00Z"
    }
  ],
  "total": 15,
  "limit": 50,
  "offset": 0
}
```

### Step 2: Replay the Conversation

Use the replay endpoint to stream the full conversation history as SSE events:

```bash
curl -N "http://localhost:8000/api/v1/threads/thread-abc123/replay"
```

This streams all messages, tool calls, and results exactly as they occurred, allowing the UI to reconstruct the conversation display.

### Step 3: Continue the Conversation

Once replay is complete, send new messages using the same `thread_id` and `workspace_id`:

```bash
curl -N -X POST "http://localhost:8000/api/v1/chat/stream" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-123",
    "workspace_id": "ws-xyz789",
    "thread_id": "thread-abc123",
    "messages": [
      {
        "role": "user",
        "content": "Now add error handling to that script"
      }
    ]
  }'
```

The agent will have full context from the previous conversation and can continue the work.

### Historical Conversation Flow Diagram

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│  List Conversations │     │  Replay Thread      │     │  Continue Chat      │
│  GET /conversations │────▶│  GET /threads/      │────▶│  POST /chat/stream  │
│  (get thread_id,    │     │  {thread_id}/replay │     │  (same thread_id &  │
│   workspace_id)     │     │  (display history)  │     │   workspace_id)     │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
```

See [Conversations API](./conversations.md) for detailed endpoint documentation.

---

## Authentication

Currently, user identification is handled via:
- `user_id` field in request bodies (for chat endpoints)
- `X-User-Id` header (for workspace endpoints)

## API Groups

| Group | Description | Prefix |
|-------|-------------|--------|
| [Chat](./chat.md) | Streaming chat with SSE, workflow control | `/api/v1/chat` |
| [Workflow](./workflow.md) | Workflow state, checkpoints, cancellation | `/api/v1/workflow` |
| [Workspaces](./workspaces.md) | Workspace CRUD, thread listing, messages | `/api/v1/workspaces` |
| [Conversations](./conversations.md) | Conversation history, replay, thread messages | `/api/v1/conversations`, `/api/v1/threads` |
| [Market Data](./market-data.md) | FMP intraday data proxy with caching | `/api/v1/market-data` |
| [Cache](./cache.md) | Cache statistics and management | `/api/v1/cache` |
| [Health](./chat.md#health-check) | Health check | `/health` |

## Common Response Formats

### Success Response

```json
{
  "status": "success",
  "data": { ... }
}
```

### Error Response

```json
{
  "detail": "Error message describing what went wrong"
}
```

### Paginated Response

```json
{
  "items": [...],
  "total": 100,
  "limit": 20,
  "offset": 0
}
```

## SSE Event Types

The streaming endpoints emit Server-Sent Events. See [Chat API - SSE Events](./chat.md#sse-event-types) for the complete list of event types.

## Data Models

See [Models Reference](./models.md) for all request/response schemas including:
- ChatRequest / StatusResponse
- WorkspaceCreate / WorkspaceResponse
- WorkspaceThreadListItem / ThreadMessagesResponse

## Rate Limits

No rate limits are currently enforced. This may change in production deployments.

## Versioning

All API endpoints are versioned with the `/api/v1/` prefix. Breaking changes will be introduced in new versions (e.g., `/api/v2/`).
