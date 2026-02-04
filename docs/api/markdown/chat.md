# Chat API

## Overview

The Chat API provides streaming chat functionality with the PTC agent. Responses are delivered in real-time via Server-Sent Events (SSE), enabling progressive display of agent reasoning, tool calls, and generated content.

## Endpoints

### Stream Chat

`POST /api/v1/chat/stream`

Stream PTC agent responses as Server-Sent Events. This is the primary endpoint for interacting with the agent.

**Features:**
- Creates or reuses a Daytona sandbox session per workspace
- Streams agent responses in real-time
- Supports tool execution and file operations
- Handles interrupts for human-in-the-loop (HITL) review
- Background execution with event buffering

**Request Headers**

| Header | Type | Required | Description |
|--------|------|----------|-------------|
| X-User-Id | string | **Yes** | User identifier |
| Content-Type | string | Yes | Must be `application/json` |

**Request Body**

```json
{
  "workspace_id": "workspace-uuid",
  "thread_id": "__default__",
  "messages": [
    {
      "role": "user",
      "content": "Write a hello world script in Python"
    }
  ],
  "plan_mode": false,
  "locale": "en-US",
  "timezone": "America/New_York"
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| workspace_id | string | **Yes** | - | Workspace ID (create first via POST /workspaces) |
| thread_id | string | No | "__default__" | Thread identifier for checkpointing |
| messages | array | No | [] | History of messages |
| subagents_enabled | array | No | from config | List of subagent names to enable |
| plan_mode | boolean | No | false | Require plan approval before execution |
| hitl_response | object | No | null | Structured HITL response for interrupt handling |
| checkpoint_id | string | No | null | Specific checkpoint to resume from |
| locale | string | No | null | Locale for output (e.g., "en-US", "zh-CN") |
| timezone | string | No | null | IANA timezone (e.g., "America/New_York") |
| additional_context | array | No | null | Additional context for skill loading |
| llm_model | string | No | from config | LLM model name from models.json (see [Model Selection](#model-selection)) |

**Response** `200 OK`

Content-Type: `text/event-stream`

```
event: message_chunk
data: {"content": "I'll write a", "agent": "assistant"}

event: message_chunk
data: {"content": " Python script", "agent": "assistant"}

event: tool_calls
data: {"tool_name": "write_file", "arguments": {"path": "hello.py", "content": "print('Hello World')"}}

event: tool_call_result
data: {"tool_name": "write_file", "result": "File created successfully"}

event: done
data: {"status": "completed", "thread_id": "abc-123"}
```

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 409 | CONFLICT | Workflow still running, use /reconnect or /cancel |
| 503 | SERVICE_UNAVAILABLE | PTC Agent not initialized |
| 500 | INTERNAL_ERROR | Server error |

**Example**

```bash
curl -N -X POST "http://localhost:8000/api/v1/chat/stream" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-789" \
  -d '{
    "workspace_id": "ws-abc123",
    "messages": [{"role": "user", "content": "Write a hello world script"}]
  }'
```

---

### Reconnect to Workflow

`GET /api/v1/chat/stream/{thread_id}/reconnect`

Reconnect to a running or completed workflow. Supports event replay for seamless reconnection after disconnects.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| thread_id | string | Yes | Workflow thread identifier |

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| last_event_id | integer | null | Last received event ID for duplicate filtering |

**Response** `200 OK`

Content-Type: `text/event-stream`

Replays buffered events, then streams live events if workflow is still running.

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 404 | NOT_FOUND | Workflow not found |
| 410 | GONE | Workflow completed and results expired (24h TTL) |

**Example**

```bash
curl -N "http://localhost:8000/api/v1/chat/stream/abc-123/reconnect?last_event_id=42"
```

---

### Stream Subagent Status

`GET /api/v1/chat/stream/{thread_id}/status`

Stream `subagent_status` updates for background subagents after the main response finishes.
The stream closes once `active_tasks` is empty.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| thread_id | string | Yes | Workflow thread identifier |

**Response** `200 OK`

Content-Type: `text/event-stream`

```
event: subagent_status
data: {"thread_id": "abc-123", "active_tasks": [], "completed_tasks": ["Task-1"]}
```

---

### Cancel Workflow

`POST /api/v1/chat/cancel/{thread_id}`

Cancel a running PTC workflow.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| thread_id | string | Yes | Workflow thread identifier |

**Response** `200 OK`

```json
{
  "status": "cancelled",
  "thread_id": "abc-123"
}
```

**Example**

```bash
curl -X POST "http://localhost:8000/api/v1/chat/cancel/abc-123"
```

---

### Get Workflow Status

`GET /api/v1/chat/status/{thread_id}`

Get status of a PTC workflow.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| thread_id | string | Yes | Workflow thread identifier |

**Response** `200 OK`

```json
{
  "thread_id": "abc-123",
  "status": "running",
  "workspace_id": "ws-abc123",
  "sandbox_id": "sandbox-789",
  "started_at": "2025-01-15T10:30:00Z",
  "completed_at": null,
  "error": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| thread_id | string | Thread identifier |
| status | string | Status: running, completed, failed, cancelled |
| workspace_id | string | Associated workspace ID |
| sandbox_id | string | Daytona sandbox ID |
| started_at | string | ISO 8601 start timestamp |
| completed_at | string | ISO 8601 completion timestamp |
| error | string | Error message if failed |

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 404 | NOT_FOUND | Workflow not found |

---

### Get Active Sessions

`GET /api/v1/chat/sessions`

Get information about active PTC sessions.

**Response** `200 OK`

```json
{
  "active_sessions": 5,
  "message": "PTC Session Service active"
}
```

---

## SSE Event Types

### Content Events

| Event | Description | Data Fields |
|-------|-------------|-------------|
| message_chunk | Text/reasoning streaming from agents | content, agent, role |
| tool_call_chunks | Incremental tool call arguments | tool_name, arguments (partial) |
| tool_calls | Complete tool call (finish signal) | tool_name, arguments, tool_call_id |
| tool_call_result | Tool execution result | tool_name, result, tool_call_id |

### Control Events

| Event | Description | Data Fields |
|-------|-------------|-------------|
| interrupt | Human-in-the-loop pause | interrupt_id, reason, actions |
| warning | Timeout or other warnings | message, type |
| error | Execution errors | error, type, thread_id |
| done | Stream complete | status, thread_id |
| keepalive | Periodic heartbeat (every 15s) | timestamp |
| retry | Recoverable error occurred | message, retry_count, max_retries |

### Subagent Status Events

| Event | Description | Data Fields |
|-------|-------------|-------------|
| subagent_status | Status of subagent tasks | active_tasks, completed_tasks |

### Artifact Events

| Event | Description | Data Fields |
|-------|-------------|-------------|
| artifact | Universal artifact event for tool outputs | artifact_type, artifact_id, agent, timestamp, status, payload |

**Artifact Structure:**
- `artifact_type`: Type discriminator (e.g., `"file_operation"`, `"todo_update"`)
- `artifact_id`: Unique ID derived from `tool_call_id`
- `agent`: Agent that created the artifact
- `timestamp`: ISO 8601 timestamp
- `status`: `"completed"` or `"failed"`
- `payload`: Type-specific data (varies by artifact_type)

**For `file_operation` payload:**
- `operation`: `"write_file"` or `"edit_file"`
- `file_path`: Full path to the file
- `line_count`: Number of lines
- `content`: File content (write_file only)
- `old_string`, `new_string`: Diff content (edit_file only)
- `error`: Error message (failed status only)

**For `todo_update` payload:**
- `todos`: Array of todo items with `content`, `status`, `activeForm`
- `total`: Total number of todos
- `completed`: Count of completed todos
- `in_progress`: Count of in-progress todos
- `pending`: Count of pending todos
- `error`: Error message (failed status only)

## SSE Event Data Schemas

### message_chunk

```json
{
  "content": "Hello, I'll help you with...",
  "agent": "assistant",
  "role": "assistant"
}
```

### tool_calls

```json
{
  "tool_name": "write_file",
  "arguments": {
    "path": "/workspace/hello.py",
    "content": "print('Hello World')"
  },
  "tool_call_id": "call_abc123"
}
```

### interrupt

```json
{
  "interrupt_id": "int_xyz",
  "reason": "plan_review",
  "actions": [
    {
      "action_id": "action_1",
      "type": "write_file",
      "description": "Create hello.py"
    }
  ]
}
```

### error

```json
{
  "thread_id": "abc-123",
  "error": "Connection timeout",
  "type": "workflow_error",
  "error_class": "TimeoutError"
}
```

### artifact (todo_update)

```json
{
  "artifact_type": "todo_update",
  "artifact_id": "call_abc123",
  "agent": "ptc",
  "timestamp": "2025-01-15T10:30:00Z",
  "status": "completed",
  "payload": {
    "todos": [
      {
        "content": "Implement user authentication",
        "status": "completed",
        "activeForm": "Implementing user authentication"
      },
      {
        "content": "Add unit tests",
        "status": "in_progress",
        "activeForm": "Adding unit tests"
      },
      {
        "content": "Update documentation",
        "status": "pending",
        "activeForm": "Updating documentation"
      }
    ],
    "total": 3,
    "completed": 1,
    "in_progress": 1,
    "pending": 1
  }
}
```

## HITL (Human-in-the-Loop) Resume

To resume from an interrupt, send a new request with the `hitl_response` field:

```json
{
  "thread_id": "abc-123",
  "hitl_response": {
    "interrupt_id_1": {
      "decisions": [
        {"type": "approve", "message": null},
        {"type": "reject", "message": "Please use a different approach"}
      ]
    }
  },
  "messages": [{"role": "user", "content": "Continue"}]
}
```

---

## Skill Loading

Use `additional_context` to load skill instructions for the agent. Skills are markdown-based instruction files that extend agent capabilities.

**Skill Context Structure:**

```json
{
  "type": "skills",
  "name": "skill-name",
  "instruction": "Optional additional instruction for the skill"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | Yes | Must be `"skills"` |
| name | string | Yes | Skill name (e.g., `"user-profile"`) |
| instruction | string | No | Additional instruction for the skill |

**Example: User Onboarding with Skill**

```bash
curl -N -X POST "http://localhost:8000/api/v1/chat/stream" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-789" \
  -d '{
    "workspace_id": "ws-abc123",
    "messages": [{"role": "user", "content": "Hi! I am new here and would like to set up my profile."}],
    "additional_context": [
      {
        "type": "skills",
        "name": "user-profile",
        "instruction": "Help the user with first time onboarding. Reference the skills/user-profile/onboarding.md for details. You should use load_skill tool to load the user-profile skill before calling any of the tools."
      }
    ]
  }'
```

The agent will load the skill's `SKILL.md` file and follow its instructions when processing the request.

---

## Model Selection

The `llm_model` field allows you to override the default LLM model on a per-request basis. Available models are defined in `src/llms/manifest/models.json`.

**Popular Models:**

| Model Name | Provider | Description |
|------------|----------|-------------|
| `gpt-5` | OpenAI | GPT-5 with minimal reasoning |
| `gpt-5-medium` | OpenAI | GPT-5 with medium reasoning effort |
| `gpt-5-mini` | OpenAI | GPT-5 Mini with minimal reasoning |
| `gpt-4.1` | OpenAI | GPT-4.1 standard |
| `claude-sonnet-4-5` | Anthropic | Claude Sonnet 4.5 with extended thinking |
| `claude-opus-4` | Anthropic | Claude Opus 4 |
| `gemini-2.5-pro` | Google | Gemini 2.5 Pro |
| `gemini-2.5-flash` | Google | Gemini 2.5 Flash |
| `doubao-seed-1.6` | Volcengine | Doubao Seed 1.6 with thinking |
| `deepseek-reasoner` | DeepSeek | DeepSeek Reasoner |
| `minimax-m2.1` | MiniMax | MiniMax M2.1 with thinking |
| `qwen3-max` | Dashscope | Qwen3 Max |

**Example: Using a specific model**

```bash
curl -N -X POST "http://localhost:8000/api/v1/chat/stream" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-789" \
  -d '{
    "workspace_id": "ws-abc123",
    "llm_model": "claude-sonnet-4-5",
    "messages": [{"role": "user", "content": "Explain quantum computing"}]
  }'
```

**Notes:**
- Model selection is optional; defaults to the model configured in `agent_config.yaml`
- Invalid model names will result in configuration errors
- Some models support reasoning/thinking modes (configured in models.json)
- Model costs vary significantly; check provider pricing

---

## Health Check

`GET /health`

Health check endpoint (unversioned).

**Response** `200 OK`

```json
{
  "status": "healthy",
  "timestamp": "2025-01-15T10:30:00Z",
  "version": "0.1.0",
  "service": "ptc-agent"
}
```
