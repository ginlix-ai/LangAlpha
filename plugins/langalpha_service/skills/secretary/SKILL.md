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

Return: `{ success, workspace_id, thread_id, status: "dispatched", report_back }`

- Omit `workspace_id` → auto-creates a new workspace (blocks ~8-10s for sandbox init)
- Pass `workspace_id` → dispatches to existing workspace (new thread)
- Pass `thread_id` → continues an existing conversation (overrides `workspace_id`)
- `report_back=True` (default) → when PTC completes, you'll automatically receive the results and should summarize them for the user
- `report_back=False` → fire-and-forget; the user will check results in the workspace themselves
- The returned `report_back` field is authoritative, not an echo of your request: it can come back `false` even when you asked for `true` (degraded backend). If it does, results will NOT arrive automatically — poll with `agent_output`.
- Concurrency caps (report-back dispatches only): at most 5 pending analyses per conversation and 10 per user. Over the cap the dispatch fails with an error starting "too many concurrent analyses" — wait for one to finish, or dispatch with `report_back=False`.

Use the returned `thread_id` with `agent_output` to check progress later (only needed when the returned `report_back` is `false`).

### agent_output

Return: `{ text, status, thread_id, workspace_id }`

- `status: "running"` — analysis still in progress, text is partial
- `status: "completed"` — full output available
- `status: "error"` — something went wrong

**`turns` window** (also on `manage_threads(action="get_output")`): by default you get only the **latest turn's** output. For a thread continued several times, pass `turns=N` for the last N turns or `turns=0` for recent history (up to the 50 most recent turns) — turns come back oldest→newest, separated by `---`. A still-streaming turn always returns just that live turn, regardless of `turns`.

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
3. To review the whole conversation (not just the newest answer), read it back with `agent_output(thread_id="...", turns=0)`

### Workspace cleanup

When the user wants to tidy up:
1. Call `manage_workspaces(action="list")` to identify stale workspaces
2. Stop idle sandboxes with `manage_workspaces(action="stop", workspace_id="...")`
3. Delete workspaces the user no longer needs with `manage_workspaces(action="delete", workspace_id="...")`
