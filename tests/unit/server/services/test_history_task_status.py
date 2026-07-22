"""Locks for read-time task-status stamping in history replay.

The stamp is the only completion signal a refreshed client gets for
subagent cards: running requires proven liveness, terminal is the default,
and stamping must copy — stored/cached event dicts are shared objects.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.history.task_status import (
    collect_task_ids,
    resolve_task_details,
    stamp_replay_task_status,
    stamp_task_artifact_data,
)


def _artifact(task_id: str, **extra) -> dict:
    return {
        "event": "artifact",
        "data": {
            "artifact_type": "task",
            "artifact_id": f"task:{task_id}",
            "tool_call_id": f"call_{task_id}",
            "payload": {"task_id": task_id, "action": "init", **extra},
        },
    }


class TestCollectTaskIds:
    def test_collects_unique_ids_in_order(self):
        items = [
            _artifact("aaa"),
            {"event": "message_chunk", "data": {"content": "x"}},
            _artifact("bbb"),
            _artifact("aaa"),
            {"event": "artifact", "data": {"artifact_type": "todo_list"}},
            {"event": "artifact", "data": "not-a-dict"},
        ]
        assert collect_task_ids(items) == ["aaa", "bbb"]


class TestLegacyStatusFallback:
    def _patch(self, live, metas: dict):
        return (
            patch(
                "src.server.database.runs.subagent_runs.get_latest_run_details",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "src.server.services.subagent_liveness.resolve_task_liveness",
                new=AsyncMock(return_value=live),
            ),
            patch(
                "ptc_agent.agent.middleware.background_subagent.redis_stream"
                ".read_task_meta",
                new=AsyncMock(side_effect=lambda t, tid: metas.get(tid)),
            ),
        )

    @pytest.mark.asyncio
    async def test_liveness_wins_then_meta_labels_terminal(self):
        p0, p1, p2 = self._patch(
            live={"live1"},
            metas={
                "canc1": {"status": "cancelled"},
                "done1": {"status": "completed"},
            },
        )
        with p0, p1, p2:
            details = await resolve_task_details(
                "t", ["live1", "canc1", "done1", "gone1"]
            )
        assert details == {
            "live1": {"status": "running", "error": None},
            "canc1": {"status": "cancelled", "error": None},
            "done1": {"status": "completed", "error": None},
            "gone1": {"status": "completed", "error": None},  # no meta -> terminal default
        }

    @pytest.mark.asyncio
    async def test_probe_failure_falls_back_to_meta(self):
        p0, p1, p2 = self._patch(
            live=None,
            metas={
                "run1": {"status": "running"},
                "done1": {"status": "completed"},
            },
        )
        with p0, p1, p2:
            details = await resolve_task_details("t", ["run1", "done1", "gone1"])
        # Availability over precision: meta says running, probe unknown.
        assert details == {
            "run1": {"status": "running", "error": None},
            "done1": {"status": "completed", "error": None},
            "gone1": {"status": "completed", "error": None},
        }


class TestStamping:
    def test_stamp_copies_never_mutates(self):
        data = _artifact("aaa")["data"]
        stamped = stamp_task_artifact_data(
            data, {"aaa": {"status": "completed", "error": None}}
        )
        assert stamped["payload"]["status"] == "completed"
        assert "status" not in data["payload"]  # original untouched
        assert stamped is not data

    def test_stamp_errored_task_carries_reason(self):
        data = _artifact("aaa")["data"]
        stamped = stamp_task_artifact_data(
            data, {"aaa": {"status": "error", "error": "transport_lost: boom"}}
        )
        assert stamped["payload"]["status"] == "error"
        assert stamped["payload"]["error"] == "transport_lost: boom"

    def test_status_only_stamps_status_never_error(self):
        # Public replay contract: only the whitelisted status value may reach
        # an unauthenticated viewer — never the ledger failure text.
        data = _artifact("aaa")["data"]
        stamped = stamp_task_artifact_data(
            data,
            {"aaa": {"status": "error", "error": "transport_lost: boom"}},
            status_only=True,
        )
        assert stamped["payload"]["status"] == "error"
        assert "error" not in stamped["payload"]

    def test_stamp_omits_error_key_when_no_reason(self):
        data = _artifact("aaa")["data"]
        stamped = stamp_task_artifact_data(
            data, {"aaa": {"status": "completed", "error": None}}
        )
        assert "error" not in stamped["payload"]

    def test_non_task_and_unknown_ids_pass_through_identically(self):
        chunk = {"content_type": "text"}
        details = {"aaa": {"status": "completed", "error": None}}
        assert stamp_task_artifact_data(chunk, details) is chunk
        other = _artifact("zzz")["data"]
        assert stamp_task_artifact_data(other, details) is other

    @pytest.mark.asyncio
    async def test_replay_stamp_replaces_positionally(self):
        items = [_artifact("aaa"), {"event": "message_chunk", "data": {"c": 1}}]
        original_artifact = items[0]
        with patch(
            "src.server.services.history.task_status.resolve_task_details",
            new=AsyncMock(return_value={"aaa": {"status": "cancelled", "error": None}}),
        ):
            await stamp_replay_task_status("t", items)
        assert items[0]["data"]["payload"]["status"] == "cancelled"
        # Shared/cached original object untouched; replaced, not mutated.
        assert "status" not in original_artifact["data"]["payload"]
        assert items[1] == {"event": "message_chunk", "data": {"c": 1}}

    @pytest.mark.asyncio
    async def test_resolution_failure_is_swallowed(self):
        items = [_artifact("aaa")]
        with patch(
            "src.server.services.history.task_status.resolve_task_details",
            new=AsyncMock(side_effect=RuntimeError("redis down")),
        ):
            await stamp_replay_task_status("t", items)  # must not raise
        assert "status" not in items[0]["data"]["payload"]


class TestResolveTaskDetails:
    @pytest.mark.asyncio
    async def test_error_reason_only_on_error_status(self):
        ledger = {
            "err1": {"status": "error", "error": "transport_lost: boom"},
            "done1": {"status": "completed", "error": None},
        }
        with patch(
            "src.server.database.runs.subagent_runs.get_latest_run_details",
            new=AsyncMock(return_value=ledger),
        ):
            details = await resolve_task_details("t", ["err1", "done1"])
        assert details["err1"] == {"status": "error", "error": "transport_lost: boom"}
        assert details["done1"] == {"status": "completed", "error": None}

    @pytest.mark.asyncio
    async def test_interrupted_maps_to_error_without_reason(self):
        ledger = {"int1": {"status": "interrupted", "error": None}}
        with patch(
            "src.server.database.runs.subagent_runs.get_latest_run_details",
            new=AsyncMock(return_value=ledger),
        ):
            details = await resolve_task_details("t", ["int1"])
        assert details["int1"] == {"status": "error", "error": None}


class TestResolveTaskLiveness:
    @pytest.mark.asyncio
    async def test_local_and_lock_held_union(self):
        from src.server.services.subagent_liveness import resolve_task_liveness

        with patch(
            "src.server.services.subagent_liveness._local_live_task_ids",
            new=AsyncMock(return_value=["loc1"]),
        ), patch(
            "src.server.services.writer_guard.held_task_namespaces",
            new=AsyncMock(return_value={"rem1"}),
        ):
            live = await resolve_task_liveness("t", ["loc1", "rem1", "dead1"])
        assert live == {"loc1", "rem1"}

    @pytest.mark.asyncio
    async def test_probe_failure_returns_none(self):
        from src.server.services.subagent_liveness import resolve_task_liveness

        with patch(
            "src.server.services.subagent_liveness._local_live_task_ids",
            new=AsyncMock(return_value=[]),
        ), patch(
            "src.server.services.writer_guard.held_task_namespaces",
            new=AsyncMock(return_value=None),
        ):
            assert await resolve_task_liveness("t", ["x"]) is None
