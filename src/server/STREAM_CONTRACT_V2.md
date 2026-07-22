# Stream Contract v2 — run-scoped lanes

Contract for thread streaming on the subagent run ledger. Milestone status: **live on
the wire** — the mux v2 server and the client cutover shipped (M1–M7), so run-scoped
streams, the control lane, lane_open/run_end, and the snapshot semantics below describe
the current transport, not a target. What remains transitional until the M8 deletions:
the v1 per-task streams (keyed by task id) are still dual-written for the legacy
readers, and the sentinel handshake survives on the compatibility surfaces (foreground
POST SSE, pre-v2 streams).

## Identity

- The unit of lifecycle, streaming, cursoring, and replay identity is the **run** — one
  execution. Root turns already have this (`conversation_response_id`); every background
  task init / resume / continuation gets an immutable **`task_run_id`**.
- `lane` (`main` | `task:{task_id}`) is presentation metadata for grouping, never an
  identity. A logical task (`task_id`) is a chain of runs.
- Streams are **immutable per run**: `subagent:stream:{thread}:{task_run_id}`. A resume
  creates a new run and a new stream. Nothing ever resets, deletes, or re-incarnates a
  stream under the same key while readers may hold cursors into it.

## Frame envelope

```
{run_id, seq, lane, type, payload}
```

- `seq` is the Redis stream entry id assigned by XADD (`<ms>-<n>`) — nobody allocates
  or increments it. Monotonic per stream by construction, so any worker — including a
  recovery finalizer appending a terminal frame — appends without coordination.
- Cursor = `run:<run_id>#<entry_id>`; resume is exclusive (XREAD after the entry id).
- **Every render-affecting frame carries a cursor**, including `lane_open`, interrupts,
  and `run_end`. Keepalives are the only cursorless traffic.

## Exclusive lane ownership

Every event belongs to exactly one run. The main stream carries main-lane events plus
the **parent-owned** task frames only: the Task tool call/result and task lifecycle
artifacts (`artifact_type: task` — init/resume/status). Task-lane content (message and
reasoning chunks, tool calls/results, `context_window`, `provenance`, errors,
interrupts, steering delivery) is delivered exclusively by the task run's channel.

Transitional exception (until the detail view renders them from the task channel):
task-attributed `model_retry` / `model_fallback` and ui/artifact events still ride the
main stream. They are discrete and idempotent (`artifact_id`-keyed), so dual delivery is
harmless; they migrate at mux-v2 cutover.

## Causal ordering

No cross-lane total order is promised. The promised causal chain per task run:

1. The parent's Task tool call exists (main lane).
2. `lane_open{task_run_id, task_id, cause, launch_tool_call_id?, description,
   subagent_type}` becomes visible — it carries enough to create the card, because the
   Task result artifact may legitimately trail early task output. `launch_tool_call_id`
   is nullable (a HITL continuation has no fresh Task call).
3. Task content begins.
4. The terminal CAS commits (ledger).
5. Cursor-bearing `run_end{outcome}` is appended.

A mux or client must not render task frames before their anchor (2) is delivered or
present in the snapshot.

## Discovery

Ledger rows do not notify connected consumers. A per-thread **control lane**
(`subagent:control:{thread_id}`, a bounded Redis stream) announces
`run_started{run_id}` for root turns and `task_run_started{run_id, task_id, cause,
parent_run_id}` for task runs (the run-id field is named `run_id` and carries the
task_run_id) push-style; because it is a stream, an attaching mux reads the backlog —
there is no subscribe-after-snapshot race. It is MAXLEN-trimmed and best-effort; periodic
ledger reconciliation is the backstop. `lane_open` alone is not discoverable (it
lives inside the stream it announces).

## Terminal semantics

- `run_end` is written **only after** the terminal status is durably committed
  (commit-then-signal), and — on the owning worker — only after the steering sweep, so
  `steering_returned` frames precede it and nothing follows it. The append is
  idempotent by last-frame inspection; recovery finalizers (scanner, admission abort)
  append it together with their CAS. A worker dying between CAS and XADD is healed by
  ledger reconciliation — a terminal row whose stream lacks `run_end` closes the
  channel from row truth, never by consumer timeout heuristics.
- A run torn by transport loss gets **no** `run_end`: a stream with an undetectable
  hole must resolve through the resync path, never read as complete.
- The legacy two-empty-round handshake and task sentinel remain in force for streams
  that predate v2 and for the main compatibility stream until root turns adopt v2
  `run_end`; they are deleted per-consumer at cutover, not globally.

## Retention (correctness contract, not tuning)

- An **active** run's stream must not expire or trim: retention is part of correctness.
  It is bounded by an explicit byte/event quota; breaching the quota — or losing a
  Redis write mid-run — finalizes the run `error(transport_lost)` and then resyncs
  consumers to the terminal projection. Silent holes are never served.
- A **terminal** run's stream may TTL after a minimum attach-grace window (a client that
  just received `run_end` can still attach/replay); after expiry the snapshot owns the
  transcript.
- A cursor gap on an active stream returns **`resync_required`** — never
  gap-and-continue. `resync_required` on an active run is preceded by its
  `error(transport_lost)` finalize: a resync target must be a terminal projection.
- **Implementation caveat**: task-run v2 streams now carry the same 2× MAXLEN
  backstop as the main lane (content spill and control-frame appends alike). The
  event/quota *circuit* for task content is still driven by the v1 leg's counter —
  content is dual-written under one lock, so a breach tears the run for both keys —
  and must move to a v2-native counter with the M8 deletion of the v1 leg.

## Snapshot

History = checkpoints + ledger. Replay projects a task run's segment **iff its ledger
row is terminal**; an in-flight run belongs to its stream, and the snapshot returns its
resume cursor instead of its content. Because Postgres, checkpoints, and Redis cannot be
sampled transactionally, the snapshot algorithm is a revalidation loop:

1. Classify runs terminal / active from the ledger.
2. Build the checkpoint projection for terminal runs.
3. Sample active streams' high-water marks and anchor presence
   (distinguishing "not opened yet" from "opened then lost").
4. Re-read ledger statuses; if any classification changed, repeat.
5. Return: projection + per-run cursors + already-satisfied anchor ids.

## Consumers

- **Foreground POST SSE** stays as a main-lane-only compatibility surface (gateway,
  curl, OSS). A mux-exclusive client opts into dispatch with the
  `X-Dispatch: background` request header and receives a **200**
  `{status: "dispatched", run_id, …}` once the START txn has committed (a durable
  receipt — the handler primes the workflow generator past START before responding)
  instead of draining the POST body.
- **Mux v2** carries all lanes (including main) as v2 frames over per-run streams; task
  discovery comes from the control lane + ledger, and anchor ordering is enforced
  server-side (frames buffer in Redis until the anchor is delivered or snapshotted).
- **Socket control frames** (mux-level, cursorless — distinct from in-stream frames):
  `chan_open{chan, lane, mode, started}` when the mux admits a channel (mode `replay`
  for a live run, `drain` for a settled backlog; `started` = ledger started_at epoch
  ms, the client orders per-task outcome votes by it), and `chan_close{chan, reason}`
  when it releases one. `chan_close{reason: terminal, outcome}` carries the ledger
  row's outcome — it is how a channel closes from row truth when the worker died
  between the terminal CAS and the `run_end` append. Other reasons:
  `resync_required` (cursor gap / aged-out stream) and `transport_error`.
- Client item identity is `(lane, run_id, item_id)` — never a positional index. Delivery
  is at-least-once: consumers keep a per-run applied-seq high-water mark; no semantic
  dedup is required beyond it.
