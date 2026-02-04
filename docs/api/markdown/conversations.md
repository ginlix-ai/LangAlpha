# Conversations API

## Overview

The Conversations API provides endpoints for viewing conversation history and replaying past interactions. This enables users to:
- Resume work from a previous conversation
- Review past decisions and agent interactions
- Create audit trails of agent activity

## Endpoints

### List User Conversations

Retrieve all conversation threads for a user across all workspaces.

**Endpoint:** `GET /api/v1/conversations`

**Headers:**
| Header | Required | Description |
|--------|----------|-------------|
| `X-User-Id` | Yes | User identifier |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 20 | Maximum threads to return (1-100) |
| `offset` | integer | 0 | Pagination offset |
| `sort_by` | string | `updated_at` | Sort field (`created_at`, `updated_at`) |
| `sort_order` | string | `desc` | Sort direction (`asc`, `desc`) |

**Response:** `WorkspaceThreadsListResponse`

```json
{
  "threads": [
    {
      "thread_id": "thread-abc123",
      "workspace_id": "ws-xyz789",
      "thread_index": 1,
      "current_status": "completed",
      "msg_type": "user",
      "first_query_content": "Create a hello world script",
      "created_at": "2025-01-15T10:30:00Z",
      "updated_at": "2025-01-15T10:35:00Z"
    }
  ],
  "total": 15,
  "limit": 20,
  "offset": 0
}
```

**Example:**

```bash
curl "http://localhost:8000/api/v1/conversations?limit=50" \
  -H "X-User-Id: user-123"
```

---

### Get Thread Messages

Retrieve the complete message history for a specific conversation thread.

**Endpoint:** `GET /api/v1/conversations/{thread_id}/messages`

**Path Parameters:**
| Parameter | Description |
|-----------|-------------|
| `thread_id` | Thread identifier |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 50 | Maximum messages to return (1-200) |
| `offset` | integer | 0 | Pagination offset |

**Response:** `WorkspaceMessagesResponse`

```json
{
  "workspace_id": "ws-xyz789",
  "user_id": "user-123",
  "name": "My Project",
  "messages": [
    {
      "pair_index": 0,
      "thread_id": "thread-abc123",
      "thread_index": 1,
      "query": {
        "query_id": "q-001",
        "content": "Create a hello world script",
        "type": "user",
        "timestamp": "2025-01-15T10:30:00Z"
      },
      "response": {
        "response_id": "r-001",
        "status": "completed",
        "agent_messages": [...],
        "execution_time": 12.5,
        "timestamp": "2025-01-15T10:30:12Z"
      }
    }
  ],
  "total_messages": 5,
  "has_more": false,
  "created_at": "2025-01-15T10:00:00Z",
  "updated_at": "2025-01-15T10:35:00Z"
}
```

**Example:**

```bash
curl "http://localhost:8000/api/v1/conversations/thread-abc123/messages?limit=100"
```

---

### Replay Thread (SSE) -- Prefer this over the messages endpoint!

Stream the complete history of a thread as Server-Sent Events. This endpoint replays persisted streaming chunks, allowing clients to reconstruct the conversation display exactly as it occurred.

**Endpoint:** `GET /api/v1/threads/{thread_id}/replay`

**Path Parameters:**
| Parameter | Description |
|-----------|-------------|
| `thread_id` | Thread identifier |

**Response:** `text/event-stream`

The endpoint streams events that replay the original conversation:

#### SSE Event Types

| Event | Description | Payload |
|-------|-------------|---------|
| `user_message` | User query content | `{thread_id, pair_index, content, timestamp}` |
| `message_chunk` | Agent text/reasoning | `{content, agent, thread_id, pair_index, response_id}` |
| `tool_calls` | Complete tool call | `{tool_name, arguments, tool_call_id, ...}` |
| `tool_call_result` | Tool execution result | `{tool_name, result, tool_call_id, ...}` |
| `replay_done` | Terminal sentinel | `{thread_id}` |

**Event Format:**

```
id: 1
event: user_message
data: {"thread_id": "thread-abc123", "pair_index": 0, "content": "Create a hello world script", "timestamp": "2025-01-15T10:30:00Z"}

id: 2
event: message_chunk
data: {"content": "I'll create a simple Python script for you.", "agent": "assistant", "thread_id": "thread-abc123", "pair_index": 0, "response_id": "r-001"}

id: 3
event: tool_calls
data: {"tool_name": "write_file", "arguments": {"path": "/workspace/hello.py", "content": "print('Hello World')"}, "tool_call_id": "call_001", "thread_id": "thread-abc123", "pair_index": 0, "response_id": "r-001"}

id: 4
event: tool_call_result
data: {"tool_name": "write_file", "result": "File created successfully", "tool_call_id": "call_001", "thread_id": "thread-abc123", "pair_index": 0, "response_id": "r-001"}

id: 5
event: replay_done
data: {"thread_id": "thread-abc123"}
```

**Example with curl:**

```bash
curl -N "http://localhost:8000/api/v1/threads/thread-abc123/replay"
```

---

## Continuing a Conversation

After replaying a conversation, users can continue from where it left off:

1. **Replay completes** - History is displayed to the user
2. **User sends new message** - CLI submits to `/api/v1/chat/stream` with:
   - Same `workspace_id` from the conversation
   - Same `thread_id` to continue the thread
3. **Agent resumes** - Agent has full context from previous interactions

**Example flow:**

```bash
# 1. User selects conversation from /conversation menu
# 2. CLI replays history via GET /api/v1/threads/{thread_id}/replay
# 3. User types a new message
# 4. CLI sends to POST /api/v1/chat/stream:

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

The agent will have full context from the replayed conversation and can continue the work.

---

## Error Responses

| Status Code | Description |
|-------------|-------------|
| 404 | Thread not found |
| 500 | Database error during retrieval |

**Error Response Format:**

```json
{
  "detail": "Thread not found: thread-abc123"
}
```
