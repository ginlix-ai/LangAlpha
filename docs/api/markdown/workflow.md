# Workflow API

## Overview

The Workflow API provides endpoints for managing workflow state, checkpoints, and execution control. Use these endpoints to inspect workflow progress, debug execution, and control running workflows.

## Endpoints

### Get Workflow State

`GET /api/v1/workflow/state/{thread_id}`

Get the complete workflow state for a given thread_id. Retrieves the latest checkpoint state from the checkpointer.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| thread_id | string | Yes | Thread ID |

**Response** `200 OK`

```json
{
  "thread_id": "abc-123",
  "checkpoint_id": "1ef663ba-28fe-6528-8002-5a559208592c",
  "messages": [
    {"role": "human", "content": "Write a script"},
    {"role": "ai", "content": "I'll help you write..."}
  ],
  "plan": null,
  "observations": [],
  "final_report": null,
  "research_topic": null,
  "market_type": null,
  "locale": "en-US",
  "deepthinking": false,
  "auto_accepted_plan": true,
  "plan_iterations": 0,
  "completed": true,
  "next_nodes": [],
  "created_at": "2025-01-15T10:30:00Z",
  "updated_at": "2025-01-15T10:35:00Z"
}
```

| Field | Type | Description |
|-------|------|-------------|
| thread_id | string | Thread identifier |
| checkpoint_id | string | Current checkpoint ID |
| messages | array | All conversation messages |
| plan | object | Research plan (null for PTC agent) |
| observations | array | Agent observations (empty for PTC) |
| final_report | string | Final generated report |
| completed | boolean | Whether workflow is complete |
| next_nodes | array | Next nodes to execute |

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 404 | NOT_FOUND | No workflow state found for thread_id |
| 500 | INTERNAL_ERROR | Checkpointer not initialized |

**Example**

```bash
curl "http://localhost:8000/api/v1/workflow/state/abc-123"
```

---

### Get Checkpoint History

`GET /api/v1/workflow/{thread_id}/checkpoints`

Get checkpoint history for a workflow thread. Returns chronologically ordered list (newest first).

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| thread_id | string | Yes | Thread identifier |

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| limit | integer | 10 | Maximum checkpoints to return (1-50) |

**Response** `200 OK`

```json
{
  "thread_id": "abc-123",
  "total_checkpoints": 4,
  "checkpoints": [
    {
      "checkpoint_id": "1ef663ba-28fe-6528-8002-5a559208592c",
      "parent_checkpoint_id": "1ef663ba-28f9-6ec4-8001-31981c2c39f8",
      "created_at": "2025-01-15T10:35:00Z",
      "metadata": {
        "source": "loop",
        "step": 2,
        "writes": {"agent": {"messages": [...]}}
      },
      "next_nodes": [],
      "pending_tasks": 0,
      "tasks": [],
      "completed": true,
      "state_preview": {
        "research_topic": null,
        "plan_iterations": 0,
        "has_final_report": false,
        "message_count": 5,
        "deepthinking": false
      }
    }
  ]
}
```

**Use Cases:**
- Time-travel debugging: Inspect state at different execution points
- State inspection: Understand workflow progression
- Debugging: Identify where interrupts or errors occurred

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 404 | NOT_FOUND | No checkpoints found for thread_id |
| 500 | INTERNAL_ERROR | Checkpointer not initialized |

**Example**

```bash
curl "http://localhost:8000/api/v1/workflow/abc-123/checkpoints?limit=5"
```

---

### Cancel Workflow

`POST /api/v1/workflow/{thread_id}/cancel`

Explicitly cancel a workflow execution. Sets cancellation flag that the streaming generator will check.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| thread_id | string | Yes | Thread ID to cancel |

**Response** `200 OK`

```json
{
  "cancelled": true,
  "thread_id": "abc-123",
  "message": "Cancellation signal sent. Workflow will stop shortly."
}
```

**Example**

```bash
curl -X POST "http://localhost:8000/api/v1/workflow/abc-123/cancel"
```

---

### Soft Interrupt Workflow

`POST /api/v1/workflow/{thread_id}/soft-interrupt`

Soft interrupt a workflow - pause main agent while keeping subagents running.

Unlike `/cancel` which stops everything, soft interrupt:
- Signals the main agent to pause at the next safe point
- Background subagents continue execution
- Workflow can be resumed with new input

This is designed for the CLI ESC key behavior where the user wants to interrupt the current response but keep background work running.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| thread_id | string | Yes | Thread ID to soft interrupt |

**Response** `200 OK`

```json
{
  "status": "soft_interrupted",
  "thread_id": "abc-123",
  "can_resume": true,
  "background_tasks": ["researcher", "analyst"]
}
```

| Field | Type | Description |
|-------|------|-------------|
| status | string | "soft_interrupted" or "not_supported" |
| thread_id | string | Thread identifier |
| can_resume | boolean | Whether workflow can be resumed |
| background_tasks | array | List of still-running background subagents |

**Example**

```bash
curl -X POST "http://localhost:8000/api/v1/workflow/abc-123/soft-interrupt"
```

---

### Get Workflow Status

`GET /api/v1/workflow/{thread_id}/status`

Get current workflow execution status. Checks Redis for active/disconnected/completed status and combines with checkpoint data.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| thread_id | string | Yes | Thread ID to check |

**Response** `200 OK`

```json
{
  "thread_id": "abc-123",
  "status": "disconnected",
  "can_reconnect": true,
  "last_update": "2025-01-15T10:35:00Z",
  "workspace_id": "ws-456",
  "user_id": "user-789",
  "progress": {
    "has_plan": false,
    "has_final_report": false,
    "message_count": 15,
    "completed": false,
    "checkpoint_id": "ckpt-abc"
  },
  "active_subagents": ["researcher"],
  "completed_subagents": ["analyst"],
  "soft_interrupted": false
}
```

**Status Values:**

| Status | Description |
|--------|-------------|
| active | Workflow is running with active connection |
| disconnected | Workflow is running but client disconnected |
| completed | Workflow finished successfully |
| cancelled | Workflow was explicitly cancelled by user |
| unknown | No tracking info found |

**Example**

```bash
curl "http://localhost:8000/api/v1/workflow/abc-123/status"
```

---

### Resume Workflow (Deprecated)

`POST /api/v1/workflow/{thread_id}/resume`

**DEPRECATED**: Use the chat endpoint with `hitl_response` parameter instead.

```bash
# Instead of this deprecated endpoint, use:
curl -X POST "http://localhost:8000/api/v1/chat/stream" \
  -H "Content-Type: application/json" \
  -d '{
    "workspace_id": "ws-abc123",
    "thread_id": "abc-123",
    "hitl_response": {"interrupt-1": {"decisions": [{"type": "approve"}]}},
    "messages": [{"role": "user", "content": "Continue"}]
  }'
```

**Response** `410 GONE`

```json
{
  "message": "This endpoint is deprecated. Use POST /api/v1/chat/stream with hitl_response instead.",
  "migration": {
    "endpoint": "POST /api/v1/chat/stream",
    "example": {
      "workspace_id": "ws-abc123",
      "thread_id": "abc-123",
      "hitl_response": {"interrupt-1": {"decisions": [{"type": "approve"}]}},
      "messages": [{"role": "user", "content": "Continue"}]
    }
  }
}
```
