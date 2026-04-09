---
name: secretary
description: Workspace and research management — dispatch analyses, monitor running agents, manage workspaces and threads.
---

# Secretary Skill

Workflow patterns and operational details for the secretary tools. Basic tool signatures are in the tool descriptions — this covers what they don't.

---

## Operational Details

### HITL approval

These actions pause for user confirmation before executing:
- `manage_workspaces(action="create"|"delete"|"stop")`
- `ptc_agent(...)` — always, before dispatch
- `manage_threads(action="delete")`

These run immediately (no approval):
- `manage_workspaces(action="list")`
- `manage_threads(action="list"|"get_output")`
- `agent_output(...)`

### ptc_agent dispatch

`ptc_agent` is **asynchronous** — it dispatches the question and returns immediately. The PTC agent runs in the background with full code execution, charts, and financial data tools.

Return: `{ success, workspace_id, thread_id, status: "dispatched" }`

- Omit `workspace_id` → auto-creates a new workspace (blocks ~8-10s for sandbox init)
- Pass `workspace_id` → dispatches to existing workspace (new thread)
- Pass `thread_id` → continues an existing conversation (overrides `workspace_id`)

Use the returned `thread_id` with `agent_output` to check progress later.

### agent_output

Return: `{ text, status, thread_id, workspace_id }`

- `status: "running"` — analysis still in progress, text is partial
- `status: "completed"` — full output available
- `status: "error"` — something went wrong

---

## Workflow Patterns

### "What's going on?" — Status overview

When the user asks for a status overview, combine workspace and thread information:
1. Call `manage_workspaces(action="list")` to get workspace states
2. Call `manage_threads(action="list")` to get recent thread activity
3. Present a concise summary: running analyses, recently completed work, workspace count

### Dispatch + Monitor — Full research cycle

1. User asks a complex question → call `ptc_agent(question="...")`
2. User asks "what happened?" or "is it done?" → call `agent_output(thread_id="...")`
3. Summarize the key findings concisely

### Continue an existing analysis

When the user wants to follow up on a prior dispatch:
1. Call `ptc_agent(question="...", thread_id="...")` with the original thread_id
2. The PTC agent continues in the same thread with full prior context

### Workspace cleanup

When the user wants to tidy up:
1. Call `manage_workspaces(action="list")` to identify stale workspaces
2. Stop idle sandboxes with `manage_workspaces(action="stop", workspace_id="...")`
3. Delete workspaces the user no longer needs with `manage_workspaces(action="delete", workspace_id="...")`
