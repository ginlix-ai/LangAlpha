# Data Models

## Overview

This document describes all Pydantic models used in the PTC Agent API for request and response validation.

## Chat Models

### ChatRequest

Request model for streaming chat endpoint.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| workspace_id | string | Yes | - | Workspace ID (required) |
| user_id | string | No | "test_user_001" | User identifier |
| thread_id | string | No | "__default__" | Thread identifier for checkpointing (auto-generates UUID on first use) |
| messages | ChatMessage[] | No | [] | History of messages |
| subagents_enabled | string[] | No | from config | List of subagent names to enable |
| background_auto_wait | boolean | No | from config | Whether to wait for background tasks |
| plan_mode | boolean | No | false | Require plan approval before execution |
| hitl_response | object | No | null | Structured HITL response |
| checkpoint_id | string | No | null | Specific checkpoint to resume from |
| locale | string | No | null | Locale for output (e.g., "en-US") |
| timezone | string | No | null | IANA timezone |
| additional_context | array | No | null | Additional context for state restoration |
| llm_model | string | No | from config | LLM model name override |

**Example:**

```json
{
  "workspace_id": "ws-abc-123",
  "user_id": "user-123",
  "thread_id": "__default__",
  "messages": [
    {"role": "user", "content": "Hello"}
  ],
  "plan_mode": false
}
```

### ChatMessage

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| role | string | Yes | Message role (user, assistant) |
| content | string \| ContentItem[] | Yes | Message content |

### ContentItem

For multi-modal messages.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | Yes | Content type (text, image) |
| text | string | No | Text content if type is "text" |
| image_url | string | No | Image URL if type is "image" |

### StatusResponse

Response model for workflow status.

| Field | Type | Description |
|-------|------|-------------|
| thread_id | string | Thread identifier |
| status | string | Status: running, completed, failed, cancelled |
| workspace_id | string | Associated workspace ID (optional) |
| sandbox_id | string | Daytona sandbox ID (optional) |
| started_at | string | ISO 8601 start timestamp (optional) |
| completed_at | string | ISO 8601 completion timestamp (optional) |
| error | string | Error message if failed (optional) |

---

## HITL Models

### HITLDecision

Decision for a single HITL action request.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | Yes | "approve" or "reject" |
| message | string | No | Feedback message (typically for rejections) |

### HITLResponse

Response to a HITL interrupt.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| decisions | HITLDecision[] | Yes | List of decisions for each action |

**Example:**

```json
{
  "decisions": [
    {"type": "approve", "message": null},
    {"type": "reject", "message": "Use a different approach"}
  ]
}
```

---

## Workflow Models

### WorkflowStateResponse

Complete workflow state from checkpoints.

| Field | Type | Description |
|-------|------|-------------|
| thread_id | string | Thread identifier |
| checkpoint_id | string | Current checkpoint ID |
| messages | object[] | All conversation messages |
| plan | PlanResponse | Research plan (null for PTC) |
| observations | ObservationResponse[] | Agent observations |
| final_report | string | Final generated report |
| research_topic | string | Research topic/query |
| market_type | string | Market type identified |
| locale | string | Locale/language |
| deepthinking | boolean | Deep thinking mode enabled |
| auto_accepted_plan | boolean | Plan auto-accepted |
| plan_iterations | integer | Number of plan iterations |
| completed | boolean | Workflow completed |
| next_nodes | string[] | Next nodes to execute |
| created_at | datetime | Checkpoint creation timestamp |
| updated_at | datetime | Last update timestamp |

### CheckpointResponse

Single checkpoint snapshot.

| Field | Type | Description |
|-------|------|-------------|
| checkpoint_id | string | Unique checkpoint identifier |
| parent_checkpoint_id | string | Parent checkpoint for lineage |
| created_at | datetime | Creation timestamp |
| metadata | CheckpointMetadata | Checkpoint metadata |
| next_nodes | string[] | Next nodes to execute |
| pending_tasks | integer | Number of pending tasks |
| tasks | TaskInfo[] | Detailed task information |
| completed | boolean | Whether workflow completed |
| state_preview | object | Preview of state fields |

### CheckpointMetadata

| Field | Type | Description |
|-------|------|-------------|
| source | string | Source (input, loop, update) |
| step | integer | Execution step number |
| writes | object | What nodes wrote to state |

### TaskInfo

Information about a pending task.

| Field | Type | Description |
|-------|------|-------------|
| id | string | Task ID |
| name | string | Node/task name |
| has_error | boolean | Whether task has an error |
| error_message | string | Error message if failed |
| has_interrupts | boolean | Whether task has interrupts |
| interrupt_count | integer | Number of interrupts |

---

## Workspace Models

### WorkspaceCreate

Request model for creating a workspace.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | Yes | Workspace name (1-255 chars) |
| description | string | No | Description (max 1000 chars) |
| config | object | No | Configuration settings |

### WorkspaceUpdate

Request model for updating a workspace.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | No | New name (1-255 chars) |
| description | string | No | New description |
| config | object | No | New config (replaces existing) |

### WorkspaceResponse

Response model for workspace details.

| Field | Type | Description |
|-------|------|-------------|
| workspace_id | string | Unique identifier |
| user_id | string | Owner user ID |
| name | string | Workspace name |
| description | string | Description |
| sandbox_id | string | Daytona sandbox ID |
| status | string | Workspace status |
| created_at | datetime | Creation timestamp |
| updated_at | datetime | Last update timestamp |
| last_activity_at | datetime | Last agent activity |
| stopped_at | datetime | When stopped |
| config | object | Configuration settings |

### WorkspaceStatus (Enum)

| Value | Description |
|-------|-------------|
| creating | Being created with new sandbox |
| running | Active and ready for use |
| stopping | Shutting down |
| stopped | Stopped but can be restarted |
| error | Encountered an error |
| deleted | Has been deleted |

### WorkspaceListResponse

| Field | Type | Description |
|-------|------|-------------|
| workspaces | WorkspaceResponse[] | List of workspaces |
| total | integer | Total count |
| limit | integer | Page size |
| offset | integer | Items skipped |

### WorkspaceActionResponse

Response for start/stop actions.

| Field | Type | Description |
|-------|------|-------------|
| workspace_id | string | Workspace identifier |
| status | string | New status |
| message | string | Action result message |

---

## Workspace Thread Models

### WorkspaceThreadListItem

Thread in list view within a workspace.

| Field | Type | Description |
|-------|------|-------------|
| thread_id | string | Thread identifier |
| workspace_id | string | Parent workspace ID |
| thread_index | integer | Thread index within workspace |
| current_status | string | Current thread status |
| msg_type | string | Message type (optional) |
| first_query_content | string | First user query content preview (optional) |
| created_at | datetime | Creation timestamp |
| updated_at | datetime | Last update timestamp |

### WorkspaceThreadsListResponse

| Field | Type | Description |
|-------|------|-------------|
| threads | WorkspaceThreadListItem[] | List of threads |
| total | integer | Total count |
| limit | integer | Page limit |
| offset | integer | Page offset |

### ThreadMessage

A message pair (query + response) within a thread.

| Field | Type | Description |
|-------|------|-------------|
| pair_index | integer | Index within thread (0-based) |
| thread_id | string | Thread ID |
| thread_index | integer | Thread index in workspace |
| query | MessageQuery | Query details |
| response | MessageResponse | Response details (may be null) |

### MessageQuery

| Field | Type | Description |
|-------|------|-------------|
| query_id | string | Query ID |
| content | string | Query content |
| type | string | Type (initial, resume_feedback) |
| feedback_action | string | Feedback action if applicable |
| metadata | object | Query metadata |
| timestamp | datetime | Query timestamp |

### MessageResponse

| Field | Type | Description |
|-------|------|-------------|
| response_id | string | Response ID |
| status | string | Status (completed, interrupted, error, timeout) |
| interrupt_reason | string | Interrupt reason |
| final_output | object | Final output (text, reasoning) |
| agent_messages | object | Agent messages by name |
| execution_time | float | Execution time in seconds |
| warnings | string[] | Warning messages |
| errors | string[] | Error messages |
| timestamp | datetime | Response timestamp |
| file_snapshot | object | File state at thread start |
| file_operations | FileOperationEvent[] | File operations in thread |

### FileSnapshot

| Field | Type | Description |
|-------|------|-------------|
| file_id | string | File ID |
| content | string | File contents |
| line_count | integer | Number of lines |
| updated_in_thread_id | string | Last modifying thread |
| updated_in_pair_index | integer | Last modifying pair index |

### FileOperationEvent

| Field | Type | Description |
|-------|------|-------------|
| operation | string | Type: write_file, edit_file, delete |
| file_path | string | Full file path |
| content | string | File contents (write_file only) |
| line_count | integer | Number of lines |
| agent | string | Performing agent |
| thread_id | string | Thread ID |
| pair_index | integer | Pair index |
| timestamp | datetime | Operation timestamp |
| operation_index | integer | Sequential index per file |
| old_string | string | For edit_file: replaced string |
| new_string | string | For edit_file: replacement |
| tool_call_id | string | LangChain tool call ID |
| file_id | string | File ID for tracking |

### ResponseFullDetail

Complete response details (admin endpoint).

| Field | Type | Description |
|-------|------|-------------|
| response_id | string | Response ID |
| thread_id | string | Thread ID |
| pair_index | integer | Pair index |
| status | string | Response status |
| interrupt_reason | string | Interrupt reason |
| final_output | object | Final output |
| state_snapshot | object | Complete LangGraph state |
| agent_messages | object | Agent messages by name |
| token_usage | object | Token usage and cost |
| metadata | object | Response metadata |
| warnings | string[] | Warning messages |
| errors | string[] | Error messages |
| execution_time | float | Execution time in seconds |
| timestamp | datetime | Response timestamp |

### ThreadMessagesResponse

| Field | Type | Description |
|-------|------|-------------|
| workspace_id | string | Workspace ID |
| thread_id | string | Thread ID |
| messages | ThreadMessage[] | All messages chronologically |
| total_messages | integer | Total message count |
| has_more | boolean | More messages available |
| created_at | datetime | Creation timestamp |
| updated_at | datetime | Last update timestamp |

---

## Market Data Models

### Supported Intervals

| Asset Type | Intervals |
|------------|-----------|
| Stocks | 1min, 5min, 15min, 30min, 1hour, 4hour |
| Indexes | 1min, 5min, 1hour |

### IntradayDataPoint

Single OHLCV data point for intraday chart data.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| date | string | Yes | Timestamp in ISO format (YYYY-MM-DD HH:MM:SS) |
| open | float | Yes | Opening price |
| high | float | Yes | High price |
| low | float | Yes | Low price |
| close | float | Yes | Closing price |
| volume | integer | Yes | Trading volume |

**Example:**

```json
{
  "date": "2024-01-15 09:30:00",
  "open": 185.50,
  "high": 185.75,
  "low": 185.25,
  "close": 185.60,
  "volume": 1500000
}
```

### CacheMetadata

Cache metadata included in intraday responses.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| cached | boolean | Yes | Whether data was served from cache |
| cache_key | string | No | Cache key used |
| ttl_remaining | integer | No | Remaining TTL in seconds |
| refreshed_in_background | boolean | No | Whether a background refresh was triggered |

### IntradayResponse

Response for single symbol intraday data request.

| Field | Type | Description |
|-------|------|-------------|
| symbol | string | Stock/index symbol |
| interval | string | Data interval (e.g., 1min, 5min, 1hour) |
| data | IntradayDataPoint[] | Intraday OHLCV data points |
| count | integer | Number of data points returned |
| cache | CacheMetadata | Cache metadata |

### BatchIntradayRequest

Request for batch intraday data.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| symbols | string[] | Yes | - | List of stock/index symbols (1-50) |
| interval | string | No | 1min | Data interval |
| from | string | No | null | Start date (YYYY-MM-DD) |
| to | string | No | null | End date (YYYY-MM-DD) |

**Example:**

```json
{
  "symbols": ["AAPL", "MSFT", "GOOGL"],
  "interval": "15min",
  "from": "2024-01-01",
  "to": "2024-01-15"
}
```

### BatchCacheStats

Cache statistics for batch requests.

| Field | Type | Description |
|-------|------|-------------|
| total_requests | integer | Total number of symbols requested |
| cache_hits | integer | Number of symbols served from cache |
| cache_misses | integer | Number of symbols fetched from API |
| background_refreshes | integer | Number of background refreshes triggered |

### BatchIntradayResponse

Response for batch intraday data request.

| Field | Type | Description |
|-------|------|-------------|
| interval | string | Data interval used for the request |
| results | object | Map of symbol to IntradayDataPoint[] |
| errors | object | Map of symbol to error message for failed requests |
| cache_stats | BatchCacheStats | Aggregated cache statistics |

**Example:**

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
    "MSFT": [...]
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
