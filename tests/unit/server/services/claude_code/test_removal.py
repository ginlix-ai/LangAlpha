"""A removed computer's token leaves its machine, after any check or save of it in flight."""

import asyncio
from unittest.mock import patch

import pytest

from src.server.services.claude_code import machine as machines
from src.server.services.claude_code.machine import answer_key, change_lock, remove_for_removed_computer
from tests.unit.server.services.claude_code.fakes import Cache, serving

CID = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21"


@pytest.mark.asyncio
@pytest.mark.parametrize("released_after", [0.5, None])
async def test_removal_waits_for_a_change_in_flight_then_removes_whether_or_not_it_ended(monkeypatch, released_after):
    cache = Cache()
    cache.locks[change_lock(CID)] = "a-check-in-flight"
    cache.values[answer_key(CID)] = {"file": "works", "email": None, "expires_at": None, "checked_at": 1.0}
    seen = []

    async def remove(link, computer_id):
        seen.append((computer_id, dict(cache.locks)))
        return True

    async def release():
        await asyncio.sleep(released_after)
        cache.locks.pop(change_lock(CID))

    monkeypatch.setattr(machines, "REMOVAL_WAIT_S", 2)
    with serving(cache), patch("ptc_agent.agent.claude_code.machine_token.remove_machine_token", new=remove):
        ending = asyncio.create_task(release()) if released_after else None
        assert await remove_for_removed_computer("link", CID)
        if ending:
            await ending
    [(computer_id, locks)] = seen
    held_by = locks.get(change_lock(CID))
    assert computer_id == CID
    assert held_by != "a-check-in-flight" if released_after else held_by == "a-check-in-flight"
    assert answer_key(CID) not in cache.values
    assert cache.locks.get(change_lock(CID)) in (None, "a-check-in-flight")
