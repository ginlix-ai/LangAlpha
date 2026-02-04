# Workspaces API

## Overview

The Workspaces API provides CRUD operations for managing workspaces. Each workspace has a dedicated Daytona sandbox (1:1 mapping), providing isolated execution environments for PTC agents.

## Endpoints

### Create Workspace

`POST /api/v1/workspaces`

Create a new workspace with dedicated sandbox. The operation may take 30-60 seconds as the sandbox needs to be initialized.

**Headers**

| Header | Type | Required | Description |
|--------|------|----------|-------------|
| X-User-Id | string | Yes | User ID |

**Request Body**

```json
{
  "name": "My Development Workspace",
  "description": "Workspace for Python development",
  "config": {
    "python_version": "3.11"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | Yes | Workspace name (1-255 chars) |
| description | string | No | Workspace description (max 1000 chars) |
| config | object | No | Optional configuration settings |

**Response** `201 Created`

```json
{
  "workspace_id": "ws-abc-123",
  "user_id": "user-456",
  "name": "My Development Workspace",
  "description": "Workspace for Python development",
  "sandbox_id": "sandbox-789",
  "status": "running",
  "created_at": "2025-01-15T10:30:00Z",
  "updated_at": "2025-01-15T10:30:00Z",
  "last_activity_at": null,
  "stopped_at": null,
  "config": {"python_version": "3.11"}
}
```

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 400 | BAD_REQUEST | Invalid request parameters |
| 500 | INTERNAL_ERROR | Failed to create workspace |

**Example**

```bash
curl -X POST "http://localhost:8000/api/v1/workspaces" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-456" \
  -d '{
    "name": "My Development Workspace",
    "description": "Workspace for Python development"
  }'
```

---

### List Workspaces

`GET /api/v1/workspaces`

List workspaces for a user with pagination.

**Headers**

| Header | Type | Required | Description |
|--------|------|----------|-------------|
| X-User-Id | string | Yes | User ID |

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| limit | integer | 20 | Maximum results (1-100) |
| offset | integer | 0 | Number to skip |

**Response** `200 OK`

```json
{
  "workspaces": [
    {
      "workspace_id": "ws-abc-123",
      "user_id": "user-456",
      "name": "My Development Workspace",
      "description": "Workspace for Python development",
      "sandbox_id": "sandbox-789",
      "status": "running",
      "created_at": "2025-01-15T10:30:00Z",
      "updated_at": "2025-01-15T10:30:00Z",
      "last_activity_at": "2025-01-15T14:00:00Z",
      "stopped_at": null,
      "config": {}
    }
  ],
  "total": 1,
  "limit": 20,
  "offset": 0
}
```

**Example**

```bash
curl "http://localhost:8000/api/v1/workspaces?limit=10" \
  -H "X-User-Id: user-456"
```

---

### Get Workspace

`GET /api/v1/workspaces/{workspace_id}`

Get workspace details.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| workspace_id | string | Yes | Workspace UUID |

**Response** `200 OK`

```json
{
  "workspace_id": "ws-abc-123",
  "user_id": "user-456",
  "name": "My Development Workspace",
  "description": "Workspace for Python development",
  "sandbox_id": "sandbox-789",
  "status": "running",
  "created_at": "2025-01-15T10:30:00Z",
  "updated_at": "2025-01-15T10:30:00Z",
  "last_activity_at": "2025-01-15T14:00:00Z",
  "stopped_at": null,
  "config": {}
}
```

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 404 | NOT_FOUND | Workspace not found |

**Example**

```bash
curl "http://localhost:8000/api/v1/workspaces/ws-abc-123"
```

---

### Update Workspace

`PUT /api/v1/workspaces/{workspace_id}`

Update workspace metadata.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| workspace_id | string | Yes | Workspace UUID |

**Request Body**

```json
{
  "name": "Updated Workspace Name",
  "description": "Updated description",
  "config": {"key": "value"}
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | No | New workspace name (1-255 chars) |
| description | string | No | New description (max 1000 chars) |
| config | object | No | New config (replaces existing) |

**Response** `200 OK`

Returns the updated workspace object.

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 404 | NOT_FOUND | Workspace not found |

**Example**

```bash
curl -X PUT "http://localhost:8000/api/v1/workspaces/ws-abc-123" \
  -H "Content-Type: application/json" \
  -d '{"name": "Renamed Workspace"}'
```

---

### Start Workspace

`POST /api/v1/workspaces/{workspace_id}/start`

Start a stopped workspace. This restarts the Daytona sandbox, which is much faster than creating a new one (~5 seconds vs ~60 seconds).

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| workspace_id | string | Yes | Workspace UUID |

**Response** `200 OK`

```json
{
  "workspace_id": "ws-abc-123",
  "status": "running",
  "message": "Workspace started successfully"
}
```

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 400 | BAD_REQUEST | Cannot start workspace in current state |
| 404 | NOT_FOUND | Workspace not found |

**Example**

```bash
curl -X POST "http://localhost:8000/api/v1/workspaces/ws-abc-123/start"
```

---

### Stop Workspace

`POST /api/v1/workspaces/{workspace_id}/stop`

Stop a running workspace. This stops the Daytona sandbox but preserves all data. The workspace can be quickly restarted later.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| workspace_id | string | Yes | Workspace UUID |

**Response** `200 OK`

```json
{
  "workspace_id": "ws-abc-123",
  "status": "stopped",
  "message": "Workspace stopped successfully"
}
```

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 400 | BAD_REQUEST | Cannot stop workspace in current state |
| 404 | NOT_FOUND | Workspace not found |

**Example**

```bash
curl -X POST "http://localhost:8000/api/v1/workspaces/ws-abc-123/stop"
```

---

### Delete Workspace

`DELETE /api/v1/workspaces/{workspace_id}`

Delete a workspace and its sandbox. This permanently deletes the workspace and its associated Daytona sandbox. **All data will be lost.**

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| workspace_id | string | Yes | Workspace UUID |

**Response** `204 No Content`

No response body.

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 404 | NOT_FOUND | Workspace not found |

**Example**

```bash
curl -X DELETE "http://localhost:8000/api/v1/workspaces/ws-abc-123"
```

---

### List Workspace Threads

`GET /api/v1/workspaces/{workspace_id}/threads`

List all threads within a workspace with pagination.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| workspace_id | string | Yes | Workspace UUID |

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| limit | integer | 20 | Maximum results (1-100) |
| offset | integer | 0 | Number to skip |
| sort_by | string | "updated_at" | Sort field (created_at, updated_at) |
| sort_order | string | "desc" | Sort order (asc, desc) |

**Response** `200 OK`

```json
{
  "threads": [
    {
      "thread_id": "thread-abc-123",
      "workspace_id": "ws-abc-123",
      "thread_index": 0,
      "current_status": "completed",
      "msg_type": "initial",
      "created_at": "2025-01-15T10:30:00Z",
      "updated_at": "2025-01-15T10:35:00Z"
    }
  ],
  "total": 1,
  "limit": 20,
  "offset": 0
}
```

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 404 | NOT_FOUND | Workspace not found |

**Example**

```bash
curl "http://localhost:8000/api/v1/workspaces/ws-abc-123/threads?limit=10" \
  -H "X-User-Id: user-456"
```

---

### Get Thread Messages

`GET /api/v1/workspaces/{workspace_id}/threads/{thread_id}/messages`

Get all messages for a specific thread within a workspace.

**Path Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| workspace_id | string | Yes | Workspace UUID |
| thread_id | string | Yes | Thread ID |

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| limit | integer | 50 | Max messages per page (1-200) |
| offset | integer | 0 | Pagination offset |

**Response** `200 OK`

```json
{
  "workspace_id": "ws-abc-123",
  "thread_id": "thread-abc-123",
  "messages": [
    {
      "pair_index": 0,
      "thread_id": "thread-abc-123",
      "thread_index": 0,
      "query": {
        "query_id": "query-1",
        "content": "Create a Python script",
        "type": "initial",
        "feedback_action": null,
        "metadata": {},
        "timestamp": "2025-01-15T10:30:00Z"
      },
      "response": {
        "response_id": "resp-1",
        "status": "completed",
        "interrupt_reason": null,
        "final_output": {"text": "I've created the script..."},
        "agent_messages": {"ptc": []},
        "execution_time": 45.2,
        "warnings": [],
        "errors": [],
        "timestamp": "2025-01-15T10:30:45Z",
        "file_snapshot": null,
        "file_operations": null
      }
    }
  ],
  "total_messages": 1,
  "has_more": false,
  "created_at": "2025-01-15T10:30:00Z",
  "updated_at": "2025-01-15T10:30:45Z"
}
```

**Errors**

| Status | Code | Description |
|--------|------|-------------|
| 404 | NOT_FOUND | Workspace or thread not found |

**Example**

```bash
curl "http://localhost:8000/api/v1/workspaces/ws-abc-123/threads/thread-abc-123/messages?limit=50"
```

---

## Workspace Status Values

| Status | Description |
|--------|-------------|
| creating | Workspace is being created with new sandbox |
| running | Workspace is active and ready for use |
| stopping | Workspace is shutting down |
| stopped | Workspace is stopped but can be restarted |
| error | Workspace encountered an error |
| deleted | Workspace has been deleted |
