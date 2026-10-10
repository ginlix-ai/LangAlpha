"""Replay parity gate: checkpoint-sourced vs sse-sourced, without the merge layer.

Before sse_events writes stop, every turn must replay UI-equivalently from
checkpoints, tables and run facts alone. ``--compare`` picks the two sides:

- ``sse`` (default): ``build_checkpoint_replay_items`` with the stored-event
  merge disabled (``_stored_events`` patched to empty) against
  ``build_sse_replay_items``. Each diff is a turn still dependent on the
  dual-write (legacy payloads, unresolved images, historical event shapes),
  except on a stopped or failed turn whose checkpoints hold the start of what
  it streamed: it is expected to differ by the output it never committed, and
  is reported apart.
- ``sse-merged``: the same with the merge on, a sanity check.
- ``facts``: each turn replayed twice with the merge on, once with its replay
  facts and once with them hidden, reporting where the facts do not
  reproduce what the stored events gave. The facts are the run facts and a
  backfilled row's legacy facts, so after the backfill this checks that each
  row's legacy facts replay as its stored events do.

Every replay runs with ``cache=False``: turns and runs are cut from their
checkpoints, stored slices and lines are neither read nor written, so a
stale or wrong cache row cannot hide a diff, and the script writes nothing.

Run inside the backend container (needs DB env + venv):

    /app/.venv/bin/python scripts/utils/replay_parity.py --all
    /app/.venv/bin/python scripts/utils/replay_parity.py --thread <id> [--verbose]
    /app/.venv/bin/python scripts/utils/replay_parity.py --all --compare sse-merged
    /app/.venv/bin/python scripts/utils/replay_parity.py --all --compare facts

Exit code 0 = no unexpected diffs; 1 = diffs found.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import os
import sys
from collections import defaultdict
from typing import Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from scripts.utils._thread_job import app_infra  # noqa: E402

# Event types that never replay (mirrors the ledger's live-only set).
_IGNORED = {
    "metadata",
    "workspace_status",
    "warning",
    "retry",
    "steering_accepted",
    "model_retry",
    "model_fallback",  # ckpt-projected from the ui channel on new turns, but
    # legacy checkpoints predate the ui record → corpus-wide compare is noise
    "tool_call_chunks",
    "compaction_chunk",
    "subagent_stream_end",
    "replay_done",
}

# Fields that legitimately differ between the sources.
_VOLATILE_KEYS = {"timestamp", "record_id", "artifact_id", "threshold", "response_id"}

# A turn that ended here may hold streamed output its checkpoints never
# committed, which replay no longer shows.
_UNCOMMITTED_STATUSES = {"cancelled", "error", "interrupted"}


def _streamed_past_commit(key: str, committed: Any, streamed: Any) -> bool:
    """Whether a stopped turn's committed ``key`` is the start of what it
    streamed, so the two differ only by output after its last checkpoint. A
    change to what the checkpoints did commit, or to any other key, is a diff
    like any other."""
    if key == "tool_calls":
        return (committed or set()) <= (streamed or set())
    committed, streamed = committed or {}, streamed or {}
    if key == "text":
        return all(streamed.get(k, "").startswith(v) for k, v in committed.items())
    if key == "signals":
        return all(v <= streamed.get(k, 0) for k, v in committed.items())
    if key == "elapsed_ms":
        return all(streamed.get(k, [])[: len(v)] == v for k, v in committed.items())
    if key == "results":
        return all(streamed.get(k) == v for k, v in committed.items())
    return False


def _lane(data: dict) -> str:
    agent = data.get("agent")
    if isinstance(agent, str) and agent.startswith("task:"):
        return agent
    return "main"


def _canon(value: Any) -> str:
    if isinstance(value, dict):
        value = sorted((str(k), v) for k, v in value.items())
    elif isinstance(value, (set, frozenset)):
        value = sorted(map(str, value))
    return json.dumps(value, sort_keys=True, default=str)


def _strip(data: dict) -> dict:
    return {k: v for k, v in data.items() if k not in _VOLATILE_KEYS}


def _normal_form(items: list[dict]) -> dict[Any, dict]:
    """Fold a replay item stream into per-turn UI-equivalent normal forms."""
    turns: dict[Any, dict] = defaultdict(
        lambda: {
            "user": [],
            "text": defaultdict(str),
            "signals": defaultdict(int),
            "elapsed_ms": defaultdict(list),
            "widgets": [],
            "steering_returned": [],
            "tool_calls": set(),
            "results": {},
            "artifacts": [],
            "context": [],
            "provenance": set(),
            "steering_delivered": [],
            "interrupt": [],
            "error": [],
            "credit_usage": [],
        }
    )
    for item in items:
        event, data = item["event"], item["data"]
        if event in _IGNORED:
            continue
        turn = turns[data.get("turn_index")]
        if event == "user_message":
            turn["user"].append(data.get("content"))
        elif event == "message_chunk":
            content_type = data.get("content_type")
            if content_type == "reasoning_signal":
                turn["signals"][_lane(data)] += 1
                if data.get("elapsed_ms") is not None:
                    turn["elapsed_ms"][_lane(data)].append(data.get("elapsed_ms"))
            elif content_type in ("text", "reasoning"):
                turn["text"][(_lane(data), content_type)] += data.get("content") or ""
        elif event == "tool_calls":
            for tc in data.get("tool_calls") or []:
                turn["tool_calls"].add((tc.get("name"), _canon(tc.get("args"))))
        elif event == "tool_call_result":
            turn["results"][data.get("tool_call_id")] = (
                data.get("content_type"),
                data.get("content"),
            )
        elif event == "artifact":
            payload = data.get("payload") or {}
            if data.get("artifact_type") == "html_widget":
                turn["widgets"].append(
                    (payload.get("title"), _canon(payload.get("data")))
                )
            turn["artifacts"].append(
                (
                    data.get("artifact_type"),
                    payload.get("task_id")
                    or payload.get("file_path")
                    or payload.get("title"),
                )
            )
        elif event == "context_window":
            turn["context"].append(
                (
                    data.get("action"),
                    data.get("signal"),
                    data.get("kind"),
                    # Live labels vary ("model:{message_id}", manual-compact
                    # "agent"); the UI lane-routes on the "task:" prefix only.
                    _lane(data),
                    data.get("total_tokens"),
                    data.get("offloaded_args"),
                    data.get("offloaded_reads"),
                    data.get("summary_length"),
                )
            )
        elif event == "provenance":
            turn["provenance"].add(
                (
                    data.get("source_type"),
                    data.get("identifier"),
                    data.get("result_sha256"),
                    data.get("result_size"),
                    data.get("agent"),
                    data.get("tool_call_id"),
                )
            )
        elif event == "steering_returned":
            turn[event].append(
                (
                    _lane(data),
                    data.get("input_id"),
                    data.get("reason"),
                    data.get("content"),
                )
            )
        elif event in ("steering_delivered", "interrupt", "error", "credit_usage"):
            turn[event].append(_strip(data))
    return dict(turns)


def _diff_turn(a: dict, b: dict) -> list[str]:
    reasons = []
    for key in a.keys() | b.keys():
        va, vb = a.get(key), b.get(key)
        if key in ("text", "signals", "elapsed_ms"):
            va, vb = dict(va or {}), dict(vb or {})
        if key == "artifacts":
            va, vb = sorted(map(_canon, va or [])), sorted(map(_canon, vb or []))
        if key in ("context", "widgets", "steering_returned"):
            va, vb = sorted(map(_canon, va or [])), sorted(map(_canon, vb or []))
        if key in ("steering_delivered", "interrupt", "error", "credit_usage"):
            va = sorted(map(_canon, va or []))
            vb = sorted(map(_canon, vb or []))
        if va != vb:
            reasons.append(key)
    return reasons


async def _thread_ids(only: list[str]) -> list[str]:
    if only:
        return only
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn:
        cur = await conn.execute(
            """SELECT conversation_thread_id FROM conversation_threads
               WHERE latest_checkpoint_id IS NOT NULL
               ORDER BY updated_at DESC"""
        )
        return [str(r[0]) for r in await cur.fetchall()]


async def _checkpoint_items(rows, tip, *, merge=True, facts=True):
    """One uncached checkpoint replay of the whole thread, with the
    stored-event merge and the run facts each on or off. A row's legacy facts
    are the merge's output, so they go with either."""
    from src.server.services.history import replay
    from src.server.services.history.replay import facts as replay_facts
    from src.server.services.history.replay import legacy

    saved_events = replay.stored_merge._stored_events
    saved_durations = replay_facts.reasoning_durations
    saved_legacy = legacy.facts_of
    if not merge:
        replay.stored_merge._stored_events = lambda response: []
    if not merge or not facts:
        legacy.facts_of = lambda response: None
    if not facts:
        replay_facts.reasoning_durations = lambda response: None
        rows = dataclasses.replace(rows, run_facts=[])
    try:
        return await replay.build_checkpoint_replay_items(rows, tip, cache=False)
    finally:
        replay.stored_merge._stored_events = saved_events
        replay_facts.reasoning_durations = saved_durations
        legacy.facts_of = saved_legacy


async def _compare_thread(
    thread_id: str, compare: str, verbose: bool
) -> tuple[int, int, int]:
    """Returns (turns_compared, turns_diff, turns_expected_diff)."""
    from src.server.services.history import replay

    loaded = await replay.load_thread_inputs(thread_id)
    if loaded is None:
        print(f"{thread_id}  SKIP (no commit pointer)")
        return 0, 0, 0
    rows, tip = loaded
    # Rows arrive without their stored events; both sides need them here,
    # including the rows whose legacy facts stand in for them.
    responses_by_turn = await replay.with_stored_events(
        rows.responses_by_turn, list(rows.responses_by_turn), verbatim=True
    )
    rows = dataclasses.replace(rows, responses_by_turn=responses_by_turn)

    try:
        if compare == "facts":
            left = await _checkpoint_items(rows, tip)
            right = await _checkpoint_items(rows, tip, facts=False)
            labels = ("facts", "stored")
        else:
            left = await _checkpoint_items(rows, tip, merge=compare == "sse-merged")
            right = replay.build_sse_replay_items(
                thread_id, rows.queries, responses_by_turn
            )
            labels = ("ckpt", "sse")
    except replay.CheckpointReplayUnavailable as e:
        print(f"{thread_id}  FALLBACK ({e})")
        return 0, 0, 0

    a, b = _normal_form(left), _normal_form(right)
    diffs = expected = 0
    for turn_index in sorted(a.keys() | b.keys(), key=lambda x: (x is None, x)):
        ckpt, sse = a.get(turn_index, {}), b.get(turn_index, {})
        reasons = _diff_turn(ckpt, sse)
        if not reasons:
            continue
        status = (responses_by_turn.get(turn_index) or {}).get("status")
        if (
            compare != "facts"
            and status in _UNCOMMITTED_STATUSES
            and all(
                _streamed_past_commit(key, ckpt.get(key), sse.get(key))
                for key in reasons
            )
        ):
            expected += 1
            verdict = f"EXPECTED ({status}, uncommitted output)"
        else:
            diffs += 1
            verdict = "DIFF"
        print(f"{thread_id}  turn {turn_index}  {verdict}: {', '.join(sorted(reasons))}")
        if verbose:
            for key in reasons:
                print(f"    {labels[0]:<6} {key}: {_canon(a.get(turn_index, {}).get(key))[:400]}")
                print(f"    {labels[1]:<6} {key}: {_canon(b.get(turn_index, {}).get(key))[:400]}")
    total = len(a.keys() | b.keys())
    if not diffs and not expected:
        print(f"{thread_id}  OK ({total} turns)")
    return total, diffs, expected


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--thread", action="append", default=[], help="thread id (repeatable)")
    parser.add_argument("--all", action="store_true", help="all threads with a commit pointer")
    parser.add_argument(
        "--compare",
        choices=("sse", "sse-merged", "facts"),
        default="sse",
        help="sse: unmerged replay vs stored events; sse-merged: merged replay "
        "vs stored events; facts: replay with replay facts vs with them hidden",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if not args.thread and not args.all:
        parser.error("pass --thread <id> or --all")
    async with app_infra(redis=False, agent_config=False):
        return await _run(args)


async def _run(args: argparse.Namespace) -> int:
    total_turns = total_diffs = total_expected = threads = 0
    for thread_id in await _thread_ids(args.thread):
        compared, diffs, expected = await _compare_thread(
            thread_id, args.compare, args.verbose
        )
        threads += 1
        total_turns += compared
        total_diffs += diffs
        total_expected += expected
    print(
        f"\n{threads} threads, {total_turns} turns compared, "
        f"{total_diffs} turn diffs, {total_expected} expected on stopped or "
        f"failed turns (--compare {args.compare})"
    )
    return 1 if total_diffs else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
