"""The mount token a computer's file daemon authenticates with.

The server keeps only a digest per slot (current and previous), so a copy
taken out of a sandbox stops working at its expiry, after two rotations, or
at once on revocation. A daemon mid-request keeps working across one
rotation, which is why the previous slot exists at all.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from src.server.services.livefs import cache, tokens
from src.server.services.livefs.tokens import (
    LivefsAuthError,
    LivefsIdentity,
    authenticate,
    mint_token,
    revoke,
    runs_low,
)

COMPUTER = "c0ffee00-0000-4000-8000-000000000001"
OTHER_COMPUTER = "c0ffee00-0000-4000-8000-000000000002"
USER = "user-livefs-test"
ROOT = "/home/workspace"


class _TokenTable:
    """``livefs_tokens`` joined to its computer, in memory.

    ``save_token`` mirrors the upsert: the current digest and expiry move to
    the previous slot, and whatever sat there is gone.
    """

    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.lookups: list[str] = []
        #: Computers stopping, stopped or deleted, which the fence refuses.
        self.leaving: set[str] = set()

    async def save_token(self, computer_id, user_id, digest, expires_at) -> bool:
        if computer_id in self.leaving:
            return False
        row = self.rows.get(computer_id)
        if row is None:
            row = self.rows[computer_id] = {
                "user_id": user_id,
                "token_sha256": digest,
                "expires_at": expires_at,
                "prev_token_sha256": None,
                "prev_expires_at": None,
            }
        else:
            row.update(
                prev_token_sha256=row["token_sha256"],
                prev_expires_at=row["expires_at"],
                token_sha256=digest,
                expires_at=expires_at,
                user_id=user_id,
            )
        return True

    async def load_token(self, computer_id):
        self.lookups.append(computer_id)
        row = self.rows.get(computer_id)
        if row is None or computer_id in self.leaving:
            return None
        return {**row, "root_dir": ROOT}

    async def delete_token(self, computer_id) -> None:
        self.rows.pop(computer_id, None)


@pytest.fixture
def table(monkeypatch) -> _TokenTable:
    fake = _TokenTable()
    monkeypatch.setattr(tokens, "db", fake)
    # The row alone, as with Redis down.
    monkeypatch.setattr(
        cache, "get_cache_client", lambda: SimpleNamespace(enabled=False, client=None)
    )
    return fake


def _bearer(token: str) -> str:
    return f"Bearer {token}"


def _secret(token: str) -> str:
    return token.split(".", 2)[2]


async def _rejects(authorization: str | None) -> bool:
    try:
        await authenticate(authorization)
    except LivefsAuthError:
        return True
    return False


# -- minting -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_minted_token_names_its_computer_and_the_server_keeps_only_its_digest(
    table,
):
    minted = await mint_token(COMPUTER, USER)

    prefix, computer_id, secret = minted.token.split(".", 2)
    assert (prefix, computer_id) == ("lfs1", COMPUTER)
    row = table.rows[COMPUTER]
    assert row["token_sha256"] == hashlib.sha256(secret.encode()).digest()
    assert row["expires_at"] == minted.expires_at
    lifetime = minted.expires_at - datetime.now(UTC)
    assert timedelta(minutes=59) < lifetime <= timedelta(hours=1)


# -- authentication --------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_minted_token_authenticates_as_its_computer_user_and_root(table):
    minted = await mint_token(COMPUTER, USER)

    identity = await authenticate(_bearer(minted.token))

    assert identity == LivefsIdentity(COMPUTER, USER, ROOT)


@pytest.mark.asyncio
async def test_a_forged_secret_for_a_real_computer_is_rejected(table):
    await mint_token(COMPUTER, USER)

    assert await _rejects(_bearer(f"lfs1.{COMPUTER}.not-the-minted-secret"))


@pytest.mark.asyncio
async def test_a_secret_presented_under_another_computers_id_is_rejected(table):
    """The id picks the row to compare against, so a token moved onto a
    second computer of the same user must not match that computer's row."""
    minted = await mint_token(COMPUTER, USER)
    await mint_token(OTHER_COMPUTER, USER)

    moved = f"lfs1.{OTHER_COMPUTER}.{_secret(minted.token)}"

    assert await _rejects(_bearer(moved))


@pytest.mark.parametrize(
    "authorization",
    [
        None,
        "",
        f"Basic lfs1.{COMPUTER}.secret",
        "Bearer",
        f"Bearer lfs2.{COMPUTER}.secret",
        f"Bearer lfs1.{COMPUTER}",
        f"Bearer lfs1.{COMPUTER}.",
        "Bearer lfs1.not-a-uuid.secret",
    ],
)
@pytest.mark.asyncio
async def test_a_malformed_authorization_is_rejected_before_any_lookup(
    table, authorization
):
    """A computer id that is not a UUID would fail the uuid column as a
    database error; it has to answer as a bad token instead."""
    assert await _rejects(authorization)
    assert table.lookups == []


@pytest.mark.asyncio
async def test_an_expired_token_is_rejected(table):
    minted = await mint_token(COMPUTER, USER)
    table.rows[COMPUTER]["expires_at"] = datetime.now(UTC) - timedelta(seconds=1)

    assert await _rejects(_bearer(minted.token))


# -- rotation and revocation -----------------------------------------------------


@pytest.mark.asyncio
async def test_after_a_rotation_the_previous_token_still_authenticates(table):
    """A daemon mid-request holds the token it read before the rewrite."""
    first = await mint_token(COMPUTER, USER)
    second = await mint_token(COMPUTER, USER)

    assert (await authenticate(_bearer(first.token))).computer_id == COMPUTER
    assert (await authenticate(_bearer(second.token))).computer_id == COMPUTER


@pytest.mark.asyncio
async def test_a_second_rotation_drops_the_token_from_two_mints_ago(table):
    first = await mint_token(COMPUTER, USER)
    await mint_token(COMPUTER, USER)
    await mint_token(COMPUTER, USER)

    assert await _rejects(_bearer(first.token))


@pytest.mark.asyncio
async def test_the_previous_token_stops_at_its_own_expiry(table):
    first = await mint_token(COMPUTER, USER)
    await mint_token(COMPUTER, USER)
    table.rows[COMPUTER]["prev_expires_at"] = datetime.now(UTC) - timedelta(seconds=1)

    assert await _rejects(_bearer(first.token))


@pytest.mark.asyncio
async def test_revocation_ends_the_current_and_the_previous_token(table):
    first = await mint_token(COMPUTER, USER)
    second = await mint_token(COMPUTER, USER)

    await revoke(COMPUTER)

    assert await _rejects(_bearer(first.token))
    assert await _rejects(_bearer(second.token))


@pytest.mark.asyncio
async def test_a_computer_leaving_service_authenticates_nothing_though_its_row_stays(table):
    """A stop whose revoke failed leaves the row; the stop itself ends it."""
    minted = await mint_token(COMPUTER, USER)
    table.leaving.add(COMPUTER)

    assert await _rejects(_bearer(minted.token))
    assert COMPUTER in table.rows


@pytest.mark.asyncio
async def test_a_computer_leaving_service_is_minted_no_token(table):
    table.leaving.add(COMPUTER)

    with pytest.raises(tokens.NotServing):
        await mint_token(COMPUTER, USER)

    assert COMPUTER not in table.rows


@pytest.mark.asyncio
async def test_a_mirror_the_revoke_could_not_drop_is_dropped_once_redis_answers(
    table, monkeypatch
):
    answers = iter([False, False, False, False, True])
    drops: list[str] = []

    async def drop(computer_id):
        drops.append(computer_id)
        return next(answers)

    monkeypatch.setattr(tokens, "_drop_mirror", drop)
    monkeypatch.setattr(tokens, "_DROP_RETRY_S", 0)
    monkeypatch.setattr(tokens, "_REVOKE_ATTEMPTS", 3)

    await revoke(COMPUTER)
    (pending,) = tokens._dropping
    await pending

    assert drops == [COMPUTER] * 5
    assert not tokens._dropping


@pytest.mark.asyncio
async def test_a_revoke_whose_row_delete_fails_still_drops_the_mirror(table, monkeypatch):
    """Nothing rewrites the mirror after a revoke, so one left standing would
    keep serving the ended tokens until it lapses."""
    drops: list[str] = []

    async def drop(computer_id):
        drops.append(computer_id)
        return True

    async def delete_token(computer_id):
        raise OSError("db down")

    monkeypatch.setattr(tokens, "_drop_mirror", drop)
    monkeypatch.setattr(table, "delete_token", delete_token)

    with pytest.raises(OSError):
        await revoke(COMPUTER)

    assert drops == [COMPUTER]


# -- refresh -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("minutes_left", "due"),
    [(31, False), (29, True), (-5, True)],
)
def test_a_token_is_due_for_refresh_once_under_thirty_minutes_remain(minutes_left, due):
    assert runs_low(datetime.now(UTC) + timedelta(minutes=minutes_left)) is due


def test_a_token_of_unknown_expiry_is_due_for_refresh():
    assert runs_low(None) is True


@pytest.mark.parametrize(
    ("minutes_left", "gone"),
    [(29, False), (2, False), (0.5, True), (-5, True)],
)
def test_only_a_token_under_a_minute_is_as_good_as_gone(minutes_left, gone):
    """Anything above that still carries a command while a renewal runs."""
    assert tokens.lapsed(datetime.now(UTC) + timedelta(minutes=minutes_left)) is gone
