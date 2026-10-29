"""A command the Chief of Staff left on another computer mirrors that
computer when it ends: no turn of that computer's own sweeps what it wrote."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.commands import mirror

HOME = "22222222-2222-2222-2222-222222222222"
OTHER = "33333333-3333-3333-3333-333333333333"
ANALYST = "44444444-4444-4444-4444-444444444444"


def _ended(command_id="bg-000000000001"):
    return {"command_id": command_id, "workspace_id": HOME, "computer_id": OTHER}


@pytest.fixture
def manager():
    m = MagicMock()
    m.backup_computer_changes = AsyncMock()
    with (
        patch("src.server.services.workspace_manager.WorkspaceManager.current", return_value=m),
        patch.object(mirror, "_redis", return_value=None),
    ):
        yield m
    mirror._mirrors.clear()
    mirror._again.clear()


def _on_computer(*workspace_ids):
    return patch.object(
        mirror,
        "get_live_workspace_ids_for_computer",
        new=AsyncMock(return_value=list(workspace_ids)),
    )


async def _drain():
    while mirror._mirrors:
        await asyncio.gather(*mirror._mirrors.values())


@pytest.mark.asyncio
async def test_an_end_on_its_threads_own_computer_leaves_the_mirror_to_its_turn(manager):
    """The end reports to a turn there, and that turn's end mirrors the computer."""
    with _on_computer(ANALYST, HOME):
        await mirror.mirror_after_end(_ended())
    await _drain()
    manager.backup_computer_changes.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_end_on_another_computer_mirrors_that_computer(manager):
    with _on_computer(ANALYST):
        await mirror.mirror_after_end(_ended())
    await _drain()
    manager.backup_computer_changes.assert_awaited_once_with(OTHER)


@pytest.mark.asyncio
async def test_ends_during_a_mirror_coalesce_into_one_more_pass(manager):
    """Commands ending together are one mirror and one after it, which sees
    whatever the later ones wrote, not one mirror each."""
    first = asyncio.Event()

    async def backup(computer_id):
        if manager.backup_computer_changes.await_count == 1:
            await first.wait()
        return True

    manager.backup_computer_changes.side_effect = backup
    with _on_computer():
        await mirror.mirror_after_end(_ended("bg-00000000000a"))
        await asyncio.sleep(0)
        for command_id in ("bg-00000000000b", "bg-00000000000c", "bg-00000000000d"):
            await mirror.mirror_after_end(_ended(command_id))
        first.set()
        await _drain()
    assert manager.backup_computer_changes.await_count == 2


@pytest.mark.asyncio
async def test_a_failed_mirror_leaves_the_next_end_its_own(manager):
    manager.backup_computer_changes.side_effect = [RuntimeError("sweep timed out"), None]
    with _on_computer():
        await mirror.mirror_after_end(_ended("bg-00000000000a"))
        await _drain()
        await mirror.mirror_after_end(_ended("bg-00000000000b"))
        await _drain()
    assert manager.backup_computer_changes.await_count == 2


@pytest.mark.asyncio
async def test_an_end_it_cannot_place_mirrors_anyway_and_never_fails_the_settle(manager):
    """The end is recorded and does not come round again, so skipping it
    could leave the command's writes unmirrored on a computer that never
    stops; an extra pass on the thread's own computer costs one sweep."""
    failing = AsyncMock(side_effect=RuntimeError("pool is not open"))
    with patch.object(mirror, "get_live_workspace_ids_for_computer", new=failing):
        await mirror.mirror_after_end(_ended())
    await _drain()
    manager.backup_computer_changes.assert_awaited_once_with(OTHER)


# --- a worker without the computer's session ------------------------------


class _Owed:
    """The Redis the debts and their leases live in, kept in dicts, with the
    two scripts played as Redis runs them."""

    def __init__(self):
        self.scores: dict[str, float] = {}
        self.leases: dict[str, str] = {}

    async def zadd(self, key, mapping):
        self.scores.update(mapping)

    async def expire(self, key, seconds):
        pass

    async def zremrangebyscore(self, key, low, high):
        for member, score in list(self.scores.items()):
            if score <= high:
                del self.scores[member]

    async def zrange(self, key, start, stop):
        return [m.encode() for m in sorted(self.scores, key=self.scores.get)]

    async def eval(self, script, numkeys, owed_key, lease_key, member, *argv):
        if script == mirror._TAKE:
            token, _ttl = argv
            if lease_key in self.leases:
                return None
            self.leases[lease_key] = token
            return repr(self.scores[member]).encode() if member in self.scores else b""
        assert script == mirror._PAY
        paid, token = argv
        if member in self.scores and repr(self.scores[member]) == paid:
            del self.scores[member]
        if self.leases.get(lease_key) == token:
            del self.leases[lease_key]
        return 0


@pytest.fixture
def owed():
    store = _Owed()
    with patch.object(mirror, "_redis", return_value=store):
        yield store


def _held(manager, *held):
    manager.cached_session_for_computer = MagicMock(
        side_effect=lambda row: object() if row["computer_id"] in held else None
    )
    return patch.object(
        mirror,
        "get_computer",
        new=AsyncMock(side_effect=lambda cid: {"computer_id": cid, "status": "running"}),
    )


@pytest.mark.asyncio
async def test_an_end_on_a_worker_without_the_session_owes_the_computer(manager, owed):
    """The lease goes at once, so the worker that holds it takes it on its next pass."""
    manager.backup_computer_changes.return_value = False
    with _on_computer():
        await mirror.mirror_after_end(_ended())
    await _drain()
    assert list(owed.scores) == [OTHER]
    assert owed.leases == {}


@pytest.mark.asyncio
async def test_the_worker_holding_an_owed_computer_mirrors_it_and_others_leave_it(manager, owed):
    owed.scores = {OTHER: time.time(), ANALYST: time.time()}
    with _held(manager, OTHER):
        await mirror.take_owed()
    await _drain()
    manager.backup_computer_changes.assert_awaited_once_with(OTHER)
    assert list(owed.scores) == [ANALYST]
    assert owed.leases == {}


@pytest.mark.asyncio
async def test_a_computer_another_worker_is_mirroring_is_left_to_it(manager, owed):
    owed.scores = {OTHER: time.time()}
    owed.leases = {f"{mirror._LEASE_KEY}{OTHER}": "another worker's"}
    with _held(manager, OTHER):
        await mirror.take_owed()
    await _drain()
    manager.backup_computer_changes.assert_not_awaited()
    assert list(owed.scores) == [OTHER]


@pytest.mark.asyncio
async def test_a_debt_stays_owed_until_a_mirror_of_it_succeeds(manager, owed):
    """A mirror that fails, like a worker lost mid-mirror, leaves the debt
    and its lease, and the first pass after the lease ends mirrors again."""
    owed.scores = {OTHER: time.time()}
    manager.backup_computer_changes.side_effect = [RuntimeError("upload timed out"), True]
    with _held(manager, OTHER):
        await mirror.take_owed()
        await _drain()
        await mirror.take_owed()
        await _drain()
        assert manager.backup_computer_changes.await_count == 1
        assert list(owed.scores) == [OTHER]
        owed.leases.clear()
        await mirror.take_owed()
        await _drain()
    assert manager.backup_computer_changes.await_count == 2
    assert owed.scores == {}
    assert owed.leases == {}


@pytest.mark.asyncio
async def test_an_end_owed_during_a_mirror_stays_owed_after_it(manager, owed):
    """The mirror may have swept before that command's last write, so the
    debt is the next mirror's."""
    owed.scores = {OTHER: time.time() - 5}
    started, release = asyncio.Event(), asyncio.Event()

    async def backup(computer_id):
        started.set()
        await release.wait()
        return True

    manager.backup_computer_changes.side_effect = backup
    with _held(manager, OTHER):
        await mirror.take_owed()
        await started.wait()
        owed.scores[OTHER] = time.time()
        release.set()
        await _drain()
    assert list(owed.scores) == [OTHER]
    assert owed.leases == {}


@pytest.mark.asyncio
async def test_a_turn_that_reached_a_computer_owes_its_mirror_until_one_succeeds(manager, owed):
    """The turn's own end mirrors only its own computer, and no later turn
    there need come, so a failed mirror of a reached one is tried again."""
    manager.backup_computer_changes.side_effect = RuntimeError("upload timed out")
    await mirror.mirror_computer(OTHER)
    await _drain()
    manager.backup_computer_changes.assert_awaited_once_with(OTHER)
    assert list(owed.scores) == [OTHER]


@pytest.mark.asyncio
async def test_an_owed_computer_nobody_takes_within_the_hour_is_left_to_its_next_sweep(manager, owed):
    owed.scores = {OTHER: time.time() - mirror._OWED_FOR_S - 1}
    with _held(manager, OTHER):
        await mirror.take_owed()
    await _drain()
    manager.backup_computer_changes.assert_not_awaited()
    assert owed.scores == {}
