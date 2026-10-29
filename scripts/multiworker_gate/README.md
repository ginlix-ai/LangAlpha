# Multi-worker gate

Re-runnable fault-injection suite for the multi-worker contract, distilled from
the v4 turn-lifecycle pre-merge fault-injection matrix (17/17 PASS) and since
extended past the turn tier. Run the automated cells whenever a change touches
**turn lifecycle, streaming/SSE, the hook outbox, subagent ownership, or the
workspace↔sandbox binding** — the failure mode they catch is always the same:
*works when producer and consumer land on the same worker, breaks ~50% of the
time at `--workers 2`.*

Cells 1–17 cover the turn/run tier. Cell 18 opened the **workspace/sandbox**
tier: v4 put runs under a durable-truth contract and left
`WorkspaceManager._sessions` on process memory, which is how a worker can go on
calling a sandbox Postgres has already replaced, serving "File not found" for
files that exist. Cells 19–23 cover **steering on the outbox**: a steer
is a `hook_outbox` row (`hook_type='steer'`) owned by the thread, and each
cell checks that every accepted steer ends `done` exactly once with an
`outcome`, none stays pending/claimed/dead (polled up to 150s for the drainer
and scanner), and the model reads it at most once, counted by a per-steer
token in the checkpoint (read in the container through the history reader).

## Topology

`gate.py` launches two extra single-uvicorn-worker servers **inside** the
running backend container — A on `:8001`, B on `:8002`, each with
`RECOVERY_SCAN_INTERVAL=5` — against the stack's one Postgres + Redis.
Multiple processes × shared stores is the multi-worker topology; requests are
driven cross-worker (dispatch on A, mutate via B) and every verdict is read
from the run ledger (`conversation_responses`), never from process memory.

## Run

```bash
# stack up (docker compose), service token in the container env
uv run python scripts/multiworker_gate/gate.py --user <user_id>

uv run python scripts/multiworker_gate/gate.py --cells 1,7,13   # subset
uv run python scripts/multiworker_gate/gate.py --user … --clean # delete gate threads
```

Requirements: Docker access to this worktree's stack, `INTERNAL_SERVICE_TOKEN`
in the backend container, a working key for the user's default model, and the
user's computer (gate turns name Home's id and run as the Chief of Staff there,
so they are real LLM calls on a sandbox). Cell 14 needs one manifest model
whose provider key is **absent** so a run fails deterministically after START
(default `claude-sonnet-5-5`; override with `GATE_FAIL_MODEL`).

Because worker routing is probabilistic in real deployments, a cell that
passes once here passes deterministically — the driver pins which worker owns
the run and which worker attacks it.

## Matrix

| # | Cell | Automated | Notes |
|---|------|-----------|-------|
| 1 | Two-process START (slot exclusivity) | ✅ | one 2xx + one 409; ≤1 `in_progress` row |
| 2 | SIGKILL mid-run → scanner finalizes | ✅ | `error` + `metadata.recovery=scanner` |
| 3 | Guard-connection loss, Python alive | manual | `pg_terminate_backend` on the run's advisory-lock backend; expect a guard-monitor abort or FencedSaver refusal, no dual writer |
| 4 | Redis outage (mid-run / pre-START / scan) | manual | `docker stop redis` mid-run → owner finalizes `error`; pre-START → instant 503, zero rows; `docker pause` blocks (doesn't fail) the probe |
| 5 | Owner alive (SIGSTOP) → no reap | manual | SIGSTOP owner ≥3 scanner cycles: guard held, no scanner reap; SIGCONT → owner finalizes |
| 6 | Two scanners racing one orphan | implicit | every cell-2 run has ≥3 live scanners (main×2 + survivor); single terminal transition is trigger-enforced |
| 7 | Cancel races (cross-worker, idempotent, orphan-cancel) | ✅ partial | orphan-cancel (intent stamped, owner killed → scanner adopts `cancelled`) stays manual |
| 8 | HITL interrupt + SIGKILL → interrupted+resumable | manual | needs an interrupt turn; kill AFTER durable checkpoint → `interrupted`, resume from other worker; kill BEFORE → `error`, never false resumability |
| 9 | Compaction vs live run → 409 | ✅ | |
| 10 | Delete vs live/orphan namespace → refusal | ✅ partial | orphan-namespace variant manual |
| 11 | Outbox lease takeover, exactly-once | manual | log-triggered SIGKILL at finalize (post-outbox-commit, pre-drain); timing-sensitive |
| 12 | Repeated burst release idempotent | covered | same job state machine as 2/11; unit-pinned |
| 13 | Request-key replay → one run | ✅ | thread-scoped; new-thread shape manual |
| 14 | Retry race → one attempt-2 row | ✅ | loser is 409 or bounded 503 (`writer_capacity` residual is by design) |
| 15 | Steering vs root finalization | manual | steer cross-worker to the live owner; archived on the response's `steering_inputs` |
| 16 | `--workers 2` boot / split-DB / pool exhaustion | manual | 16b: split unreachable by construction in the server path; 16c: `POSTGRES_WRITER_POOL_MAX=1` → bounded 503 |
| 17 | Graceful SIGTERM with open run | ✅ | owner-finalized `cancelled` (`cancelled_by_user=false`, no `recovery`) before exit |
| 18 | Sandbox replaced behind a worker → re-attach | ✅ | first **workspace/sandbox**-tier cell (1–17 are all turn/run tier). `/spec` on A recreates the sandbox while B is probed concurrently; requires `sandbox_id` changed, B converged to 200 with the exact sentinel bytes, zero 404s, and a `is stale` detection in B's log. A 503 inside the window is fine; a 503 at the end is not |
| 19 | Steer vs finalize | ✅ | 3 turns on A, steers on B at 0.6–1.1× a calibrated turn length. Accepted → `delivered` or `turn` (that turn exists and completes), read once; refused → `not_running` with no row written |
| 20 | Steer vs cancel | ✅ | steer on B, `/cancel?run_id` on A or B at 0–1s gaps. `returned` and listed in that `/cancel`'s `returned_steers` (or the re-asked one), never in the checkpoint; or `delivered`, never returned; nothing else |
| 21 | Chief of Staff hand-off steered into a live turn | ✅ | the `dispatch.py` POST (dispatched, `origin` agent + `origin_home_thread_id`/`origin_dispatch_gen`) on B into A's analyst turn → 200 `steered` naming the live run, row `steer:dispatch:{gen}` `delivered`/`turn`, gen in the run's `dispatch_gens` (and on the turn's `origin_dispatch_gen`). Home is a fresh gate thread. A hand-off that finds the turn over is a plain `dispatched` run carrying the gen |
| 22 | Task steer vs task finish | ✅ | PTC turn with one background Task (real sandbox, 2 reps). `delivered`, `resumed` (a `cause='resume'` run, read once in `task:{id}`) or `returned` (one `replay_facts.steering_returned` entry, unread), exactly one of them |
| 23 | Missed steer vs new send | ✅ | steer on B mid-generation, new send on B or A the moment the run ends → steer `turn`/`delivered`, each read once, steer before the new message in the checkpoint |

Manual procedures and full evidence for every cell live in the original
fault-injection report (kept out of the repo — summarized in the PR that
introduced this suite).

## Reading failures

Every cell prints its thread id. Inspect with the ledger, not the UI:

```sql
SELECT conversation_response_id, turn_index, attempt_no, status,
       metadata->>'recovery' AS recovery, cancel_requested_at
FROM conversation_responses
WHERE conversation_thread_id = '<tid>' ORDER BY turn_index, attempt_no;
```

Worker logs live in the container at `/tmp/gate_A.log` / `/tmp/gate_B.log`
(left in place after teardown).
