# Multi-worker gate

Re-runnable fault-injection suite for the v4 turn-lifecycle multi-worker
contract. Derived from the Gate 2 matrix (17/17 PASS, 2026-07-14); run the
automated cells whenever a change touches **turn lifecycle, streaming/SSE,
the hook outbox, or subagent ownership** — the failure mode they catch is
always the same: *works when producer and consumer land on the same worker,
breaks ~50% of the time at `--workers 2`.*

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
in the backend container, a working flash-model key (turns are real LLM calls —
they cost a few flash generations per run). Cell 14 needs one manifest model
whose provider key is **absent** so a run fails deterministically after START
(default `claude-sonnet-5`; override with `GATE_FAIL_MODEL`).

Because worker routing is probabilistic in real deployments, a cell that
passes once here passes deterministically — the driver pins which worker owns
the run and which worker attacks it.

## Matrix

| # | Cell | Automated | Notes |
|---|------|-----------|-------|
| 1 | Two-process START (slot exclusivity) | ✅ | one 2xx + one 409; ≤1 `in_progress` row |
| 2 | SIGKILL mid-run → scanner finalizes | ✅ | `error` + `metadata.recovery=scanner` |
| 3 | Guard-connection loss, Python alive | manual | `pg_terminate_backend` on the run's advisory-lock backend; expect FencedSaver refusal (flash) / guard-monitor abort (PTC), no dual writer |
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

Manual procedures and full evidence for every cell: the original Gate 2 report
(kept out of the repo — see the turn-lifecycle build notes / PR description).

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
