"""Replay parity gate: checkpoint-sourced vs sse-sourced, without the merge layer.

Cutover step 3 of single-source replay: before sse_events writes stop, every
turn must replay UI-equivalently from checkpoints + tables alone. This script
runs ``build_checkpoint_replay_items`` with the stored-event merge disabled
(``_stored_events`` patched to empty) against ``build_sse_replay_items`` for a
thread corpus and reports per-turn diffs — each diff is a turn still dependent
on the dual-write (legacy payloads, unresolved images, historical event shapes).

Run inside the backend container (needs DB env + venv):

    /app/.venv/bin/python scripts/utils/replay_parity.py --all
    /app/.venv/bin/python scripts/utils/replay_parity.py --thread <id> [--verbose]
    /app/.venv/bin/python scripts/utils/replay_parity.py --all --merge   # sanity: with merge on

Exit code 0 = all compared turns equivalent; 1 = diffs found.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import defaultdict
from typing import Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

# Event types that never replay (mirrors the ledger's live-only set).
_IGNORED = {
    "metadata",
    "workspace_status",
    "warning",
    "retry",
    "steering_accepted",
    "steering_returned",
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
        elif event in ("steering_delivered", "interrupt", "error", "credit_usage"):
            turn[event].append(_strip(data))
    return dict(turns)


def _diff_turn(a: dict, b: dict) -> list[str]:
    reasons = []
    for key in a.keys() | b.keys():
        va, vb = a.get(key), b.get(key)
        if key in ("text", "signals"):
            va, vb = dict(va or {}), dict(vb or {})
        if key == "artifacts":
            va, vb = sorted(map(_canon, va or [])), sorted(map(_canon, vb or []))
        if key == "context":
            va, vb = sorted(map(_canon, va or [])), sorted(map(_canon, vb or []))
        if key in ("steering_delivered", "interrupt", "error", "credit_usage"):
            va = sorted(map(_canon, va or []))
            vb = sorted(map(_canon, vb or []))
        if va != vb:
            reasons.append(key)
    return reasons


async def _open_infra():
    from src.server.app import setup
    from src.server.database import conversation as qr_db
    from src.server.utils.checkpointer import (
        get_checkpointer,
        open_checkpointer_pool,
    )

    pool = qr_db.get_or_create_pool()
    await pool.open()
    checkpointer = get_checkpointer(
        "postgres",
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_name=os.getenv("DB_NAME", "postgres"),
        db_user=os.getenv("DB_USER", "postgres"),
        db_password=os.getenv("DB_PASSWORD", "postgres"),
    )
    await open_checkpointer_pool(checkpointer)
    setup.checkpointer = checkpointer


async def _thread_ids(only: list[str]) -> list[str]:
    if only:
        return only
    from src.server.database.conversation import get_db_connection

    async with get_db_connection() as conn:
        cur = await conn.execute(
            """SELECT conversation_thread_id FROM conversation_threads
               WHERE latest_checkpoint_id IS NOT NULL
               ORDER BY updated_at DESC"""
        )
        return [str(r[0]) for r in await cur.fetchall()]


async def _compare_thread(thread_id: str, merge: bool, verbose: bool) -> tuple[int, int]:
    """Returns (turns_compared, turns_diff)."""
    from src.server.database.conversation import get_replay_thread_data
    from src.server.services.history import replay

    _, thread, queries, responses, usages, provenance = await get_replay_thread_data(
        thread_id
    )
    if not thread or thread.get("latest_checkpoint_id") is None:
        print(f"{thread_id}  SKIP (no commit pointer)")
        return 0, 0
    responses_by_turn = {
        r.get("turn_index"): r for r in responses if isinstance(r, dict)
    }

    sse_items = replay.build_sse_replay_items(thread_id, queries, responses_by_turn)

    original_stored_events = replay._stored_events
    if not merge:
        replay._stored_events = lambda response: []
    try:
        checkpoint_items = await replay.build_checkpoint_replay_items(
            thread_id,
            queries,
            responses_by_turn,
            branch_tip_checkpoint_id=thread.get("latest_checkpoint_id"),
            usages=usages,
            provenance=provenance,
        )
    except replay.CheckpointReplayUnavailable as e:
        print(f"{thread_id}  FALLBACK ({e})")
        return 0, 0
    finally:
        replay._stored_events = original_stored_events

    ckpt, sse = _normal_form(checkpoint_items), _normal_form(sse_items)
    diffs = 0
    for turn_index in sorted(ckpt.keys() | sse.keys(), key=lambda x: (x is None, x)):
        reasons = _diff_turn(ckpt.get(turn_index, {}), sse.get(turn_index, {}))
        if reasons:
            diffs += 1
            print(f"{thread_id}  turn {turn_index}  DIFF: {', '.join(sorted(reasons))}")
            if verbose:
                for key in reasons:
                    print(f"    ckpt {key}: {_canon(ckpt.get(turn_index, {}).get(key))[:400]}")
                    print(f"    sse  {key}: {_canon(sse.get(turn_index, {}).get(key))[:400]}")
    total = len(ckpt.keys() | sse.keys())
    if not diffs:
        print(f"{thread_id}  OK ({total} turns)")
    return total, diffs


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--thread", action="append", default=[], help="thread id (repeatable)")
    parser.add_argument("--all", action="store_true", help="all threads with a commit pointer")
    parser.add_argument("--merge", action="store_true", help="keep the stored-event merge on")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if not args.thread and not args.all:
        parser.error("pass --thread <id> or --all")

    await _open_infra()
    total_turns = total_diffs = threads = 0
    for thread_id in await _thread_ids(args.thread):
        compared, diffs = await _compare_thread(thread_id, args.merge, args.verbose)
        threads += 1
        total_turns += compared
        total_diffs += diffs
    print(
        f"\n{threads} threads, {total_turns} turns compared, "
        f"{total_diffs} turn diffs ({'merge ON' if args.merge else 'merge OFF'})"
    )
    return 1 if total_diffs else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
