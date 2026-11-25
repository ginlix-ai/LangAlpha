"""The Drive allowance a person gets: the account service's number when it
gives one, the deployment's config otherwise, and the database's own cap
whenever the database holds the bytes. Verified live first against a stubbed
validate answer; the numbers here are made up."""

from __future__ import annotations

import httpx
import pytest

from src.config.models import DriveConfig
from src.server.services import drive, membership
from tests.unit.server.services.test_account_contract import _Auth, _Cache, _deployment

pytestmark = pytest.mark.asyncio

MIB = 1024 * 1024


def _answer(**extra):
    return {"valid": True, "access_tier": 2, "plan_display_name": "Any", **extra}


async def _given(*answers, door=False) -> list[int | None]:
    """What ``drive_allowance`` reads after each answer, asked fresh."""
    cache, seen = _Cache(), []
    with _deployment(cache, _Auth(*answers), door=door):
        for _ in answers:
            await membership.read_membership("u", refresh=True)
            seen.append(await membership.drive_allowance("u"))
    return seen


@pytest.mark.parametrize("door", [False, True])
async def test_the_number_rides_the_membership(door):
    assert await _given(
        _answer(drive_max_bytes=7 * MIB),
        _answer(drive_max_bytes=-1),
        _answer(),
        door=door,
    ) == [7 * MIB, -1, None]


@pytest.mark.parametrize("sent", ["7", 7.5, True, -2, None, {"bytes": 1}])
async def test_a_number_that_does_not_read_as_one_is_no_answer(sent):
    assert await _given(_answer(drive_max_bytes=sent)) == [None]


async def test_the_last_answer_stands_in_while_the_account_service_is_down():
    cache = _Cache()
    with _deployment(cache, _Auth(_answer(drive_max_bytes=3 * MIB), httpx.ConnectError("down")), door=False):
        assert await membership.drive_allowance("u") == 3 * MIB
        await membership.read_membership("u", refresh=True)
        assert await membership.drive_allowance("u") == 3 * MIB


async def test_without_an_account_service_nothing_is_asked(monkeypatch):
    monkeypatch.setattr("src.server.services.platform_client.HOST_MODE", "oss")
    assert await membership.drive_allowance("u") is None


def _deployment_with(monkeypatch, given, *, store=True, **cfg):
    monkeypatch.setattr(drive, "_config", lambda: DriveConfig(**cfg))
    monkeypatch.setattr(drive, "is_storage_enabled", lambda: store)
    monkeypatch.setattr(drive, "multipart_enabled", lambda: store)

    async def drive_allowance(user_id):
        return given

    monkeypatch.setattr(drive.membership, "drive_allowance", drive_allowance)


@pytest.mark.parametrize(
    ("given", "store", "cap"),
    [
        (None, True, 64 * MIB),  # no number: the config
        (8 * MIB, True, 8 * MIB),  # the account service's number
        (-1, True, None),  # no cap on bytes
        (-1, False, 32 * MIB),  # the database holds the bytes: its cap wins
        (8 * MIB, False, 32 * MIB),
    ],
)
async def test_whose_number_caps_the_space(monkeypatch, given, store, cap):
    _deployment_with(
        monkeypatch, given, store=store, max_space_bytes=64 * MIB, max_space_bytes_in_database=32 * MIB
    )
    assert await drive.allowance("u") == cap


async def test_no_cap_on_bytes_leaves_files_to_the_store_and_the_trash_to_its_days(monkeypatch):
    _deployment_with(monkeypatch, -1, max_space_bytes=64 * MIB)
    limits = await drive._limits("u")
    assert (limits.max_space_bytes, limits.max_trash_bytes) == (None, None)
    assert limits.max_file_bytes == 5 * 1024**4  # the store's own object limit


async def test_the_trash_bound_follows_the_persons_number(monkeypatch):
    _deployment_with(monkeypatch, 8 * MIB, max_space_bytes=64 * MIB, trash_max_ratio=0.5)
    assert (await drive._limits("u")).max_trash_bytes == 4 * MIB
