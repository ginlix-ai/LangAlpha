"""A shared workspace's thread names who queued each waiting message, since a
stop by another member keeps it on the thread where every member reads it."""

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.shared_workspaces import viewer


@pytest.mark.asyncio
async def test_each_waiting_message_is_named_under_its_sender():
    steers = [
        {"content": "Add EV/EBITDA.", "request_key": "k1", "timestamp": 1.0, "user_id": "maya"},
        {"content": "Older one.", "request_key": "k2", "timestamp": 2.0, "user_id": None},
    ]
    with patch.object(viewer, "names_of", AsyncMock(return_value={"maya": "Maya Okafor"})):
        named = await viewer.name_steer_senders(steers)

    assert named == [
        {
            "content": "Add EV/EBITDA.",
            "request_key": "k1",
            "timestamp": 1.0,
            "sender": {"user_id": "maya", "name": "Maya Okafor"},
        },
        {"content": "Older one.", "request_key": "k2", "timestamp": 2.0},
    ]
