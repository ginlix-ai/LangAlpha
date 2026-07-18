"""Locks the snapshot frame's placement in the replay stream.

The frame is additive — it must take the next flat ``id:`` and must never
displace ``replay_done``, which v1 clients latch on. A snapshot outage is
not a replay failure, so the None path is part of the contract.
"""

from unittest.mock import AsyncMock, patch

import pytest

THREAD = "11111111-1111-1111-1111-111111111111"


def _replay_data():
    """Owner matches create_test_app's override; no checkpoint pointer, so
    replay takes the (empty) sse path without touching a DB."""
    thread = {"conversation_thread_id": THREAD, "latest_checkpoint_id": None}
    return ("test-user-123", thread, [], [], {}, {})


def _parse(body: str) -> list[tuple[str, str]]:
    """SSE text -> [(id, event)] in wire order."""
    out = []
    for block in body.strip().split("\n\n"):
        lines = dict(
            line.split(": ", 1) for line in block.splitlines() if ": " in line
        )
        if "event" in lines:
            out.append((lines.get("id"), lines["event"]))
    return out


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshot,expect_snapshot",
    [
        ({"active_runs": [], "revalidations": 0}, True),
        (None, False),
    ],
)
async def test_snapshot_frame_precedes_replay_done(
    threads_client, snapshot, expect_snapshot
):
    with (
        patch(
            "src.server.app.threads.get_replay_thread_data",
            new=AsyncMock(return_value=_replay_data()),
        ),
        patch(
            "src.server.services.history.snapshot.build_thread_snapshot",
            new=AsyncMock(return_value=snapshot),
        ),
    ):
        resp = await threads_client.get(
            f"/api/v1/threads/{THREAD}/messages/replay"
        )

    assert resp.status_code == 200
    frames = _parse(resp.text)
    events = [event for _, event in frames]

    if expect_snapshot:
        assert events == ["snapshot", "replay_done"]
    else:
        assert events == ["replay_done"]

    # Flat counter stays contiguous from 1 regardless of the snapshot.
    assert [fid for fid, _ in frames] == [
        str(i) for i in range(1, len(frames) + 1)
    ]
