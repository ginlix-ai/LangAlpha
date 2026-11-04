"""The token a computer's ``la`` CLI presents: when a sandbox is owed one, how
one is written without ever failing a turn, and how one authenticates."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from src.server.auth import computer_token_core as core
from src.server.database import computer_cli_tokens as table
from src.server.services import cli_tokens

COMPUTER = "c0ffee00-0000-4000-8000-000000000001"
USER = "user-cli-token-1"
SANDBOX = "sandbox-cli-1"
PROVIDER = "daytona"
BASE_URL = "http://backend.internal:8000/"


def _minted(token: str, expires_at: datetime) -> core.Minted:
    return core.Minted(token, expires_at, expires_at - cli_tokens.KIND.ttl)


def _left(delta: timedelta) -> datetime:
    return datetime.now(UTC) + delta


class FakeDb:
    """``computer_cli_tokens`` with one row, noting each call in ``events``."""

    def __init__(self, events: list[str], row: table.TokenRow | None = None) -> None:
        self.events = events
        self.row = row or table.TokenRow()
        self.saved: list[tuple] = []
        self.held: list[tuple] = []
        self.deleted: list[str] = []
        self.save_wins = True
        self.fail_at: str | None = None

    def _maybe_fail(self, step: str) -> None:
        if self.fail_at == step:
            raise RuntimeError(f"{step} failed")

    async def current(self, computer_id):
        self.events.append("current")
        self._maybe_fail("current")
        return self.row

    async def save_token(self, computer_id, user_id, digest, expires_at, seen):
        self.events.append("save")
        self._maybe_fail("save")
        self.saved.append((computer_id, user_id, digest, expires_at, seen))
        return self.save_wins

    async def mark_held(self, computer_id, sandbox_id, expires_at):
        self.events.append("mark_held")
        self._maybe_fail("mark_held")
        self.held.append((computer_id, sandbox_id, expires_at))

    async def delete_token(self, computer_id):
        self.events.append("delete")
        self._maybe_fail("delete")
        self.deleted.append(computer_id)


class Harness:
    def __init__(self, monkeypatch) -> None:
        self.events: list[str] = []
        self.db = FakeDb(self.events)
        self.written: list[dict] = []
        self.write_ok = True
        self.write_error: Exception | None = None
        self.mirror_dropped = True
        self.dropped: list[tuple] = []
        self.addresses: list[str] = []
        monkeypatch.setattr(cli_tokens, "db", self.db)
        monkeypatch.setattr(cli_tokens, "_client", lambda: "redis-client")
        monkeypatch.setattr(core, "_DROP_RETRY_S", 0)

        async def drop_mirror(kind, computer_id):
            self.events.append("drop")
            self.dropped.append((kind.client(), kind, computer_id))
            return self.mirror_dropped

        monkeypatch.setattr(core, "drop_mirror", drop_mirror)

        def base_url(provider):
            self.addresses.append(provider)
            return BASE_URL

        monkeypatch.setattr(
            "src.server.services.egress.reachability.effective_relay_base_url", base_url
        )

    async def write(self, payload: dict) -> bool:
        self.events.append("write")
        if self.write_error is not None:
            raise self.write_error
        self.written.append(payload)
        return self.write_ok

    async def keep(self, **overrides) -> bool:
        args = {
            "computer_id": COMPUTER,
            "user_id": USER,
            "sandbox_id": SANDBOX,
            "provider": PROVIDER,
            "write": self.write,
            **overrides,
        }
        return await cli_tokens.keep(**args)


@pytest.fixture
def h(monkeypatch) -> Harness:
    return Harness(monkeypatch)


# -- the spelling -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("token", "is_cli"),
    [(f"lac1.{COMPUTER}.s", True), ("lac1.", True), ("lac1", False), (f"lfs1.{COMPUTER}.s", False), ("", False)],
)
def test_only_the_lac1_prefix_is_a_cli_token(token, is_cli):
    assert cli_tokens.is_cli_token(token) is is_cli


# -- the file a sandbox is given -------------------------------------------------


def test_the_payload_names_the_token_computer_server_address_and_expiry():
    expires = datetime(2031, 5, 6, 7, 8, 9, 123456, tzinfo=UTC)

    got = cli_tokens.payload(_minted("lac1.x.y", expires), COMPUTER, "https://backend.example.test///")

    assert got["token"] == "lac1.x.y"
    assert got["computer_id"] == COMPUTER
    assert got["base_url"] == "https://backend.example.test"
    assert got["expires_at"] == "2031-05-06T07:08:09Z"
    assert set(got) == {"token", "iat_ms", "computer_id", "base_url", "expires_at"}


def test_the_payload_is_dated_by_its_mint_so_an_older_mints_late_write_loses():
    """Two keeps can mint T1 then T2 and write T2 first; T1's write, dated
    when it is written, would then put back a token already pushed out of
    the row. Dated by the mint, the sandbox keeps T2."""
    first = datetime(2031, 5, 6, 7, 0, 0, tzinfo=UTC)
    second = first + timedelta(seconds=1)

    newer = cli_tokens.payload(_minted("lac1.x.t2", second), COMPUTER, "http://b")
    older = cli_tokens.payload(_minted("lac1.x.t1", first), COMPUTER, "http://b")

    assert older["iat_ms"] < newer["iat_ms"]
    assert older["iat_ms"] == int((first - cli_tokens.KIND.ttl).timestamp() * 1000)
    assert newer["iat_ms"] - older["iat_ms"] == 1000


def test_an_expiry_in_another_zone_is_written_as_utc():
    from datetime import timezone

    east = timezone(timedelta(hours=8))
    expires = datetime(2031, 5, 6, 15, 8, 9, tzinfo=east)

    assert cli_tokens.payload(_minted("t", expires), COMPUTER, "http://b")["expires_at"] == (
        "2031-05-06T07:08:09Z"
    )


# -- keep: when it writes ---------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing", [{"user_id": None}, {"user_id": ""}, {"sandbox_id": None}, {"provider": None}]
)
async def test_keep_does_nothing_without_a_user_a_sandbox_and_a_provider(h, missing):
    assert await h.keep(**missing) is False

    assert h.events == []


@pytest.mark.asyncio
async def test_keep_does_nothing_when_the_server_has_no_address_to_give(h, monkeypatch):
    monkeypatch.setattr(
        "src.server.services.egress.reachability.effective_relay_base_url", lambda p: ""
    )

    assert await h.keep() is False

    assert h.events == []


@pytest.mark.asyncio
async def test_the_address_written_is_the_one_the_sandboxs_provider_dials(h):
    await h.keep()

    assert h.addresses == [PROVIDER]
    assert h.written[0]["base_url"] == BASE_URL.rstrip("/")


@pytest.mark.asyncio
async def test_keep_leaves_a_sandbox_that_is_not_owed_a_token_alone(h):
    h.db.row = table.TokenRow(_left(timedelta(hours=1)), SANDBOX)

    assert await h.keep() is False

    assert h.events == ["current"]


@pytest.mark.asyncio
async def test_keep_writes_in_order_save_then_drop_mirror_then_write_then_mark_held(h):
    assert await h.keep() is True

    assert h.events == ["current", "save", "drop", "write", "mark_held"]


@pytest.mark.asyncio
async def test_what_is_saved_is_the_digest_of_the_secret_in_the_written_token(h):
    await h.keep()

    _, _, digest, _, _ = h.db.saved[0]
    token = h.written[0]["token"]
    prefix, computer_id, secret = token.split(".", 2)
    assert (prefix, computer_id) == ("lac1", COMPUTER)
    assert digest == hashlib.sha256(secret.encode()).digest()
    assert len(secret) >= 32


@pytest.mark.asyncio
async def test_the_token_is_saved_for_the_user_with_an_hour_to_live(h):
    await h.keep()

    computer_id, user_id, _, expires_at, _ = h.db.saved[0]
    assert (computer_id, user_id) == (COMPUTER, USER)
    assert timedelta(minutes=59) < expires_at - datetime.now(UTC) <= cli_tokens.KIND.ttl


@pytest.mark.asyncio
async def test_the_written_file_and_the_held_mark_carry_the_saved_expiry(h):
    await h.keep()

    expires_at = h.db.saved[0][3]
    assert h.written[0]["expires_at"] == expires_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert h.written[0]["base_url"] == "http://backend.internal:8000"
    assert h.db.held == [(COMPUTER, SANDBOX, expires_at)]


@pytest.mark.asyncio
async def test_the_mirror_is_dropped_for_this_computer_and_kind(h):
    await h.keep()

    assert h.dropped == [("redis-client", cli_tokens.KIND, COMPUTER)]


@pytest.mark.asyncio
async def test_every_mint_makes_a_new_secret(h):
    await h.keep()
    h.db.row = table.TokenRow()
    await h.keep()

    assert h.written[0]["token"] != h.written[1]["token"]


@pytest.mark.asyncio
async def test_a_computer_id_that_is_not_a_string_is_written_as_one(h):
    import uuid

    await h.keep(computer_id=uuid.UUID(COMPUTER))

    assert h.db.saved[0][0] == COMPUTER
    assert h.written[0]["computer_id"] == COMPUTER


# -- keep: the compare-and-set ----------------------------------------------------


@pytest.mark.asyncio
async def test_the_save_names_the_expiry_it_read_so_a_racing_mint_loses(h):
    seen = _left(timedelta(minutes=5))
    h.db.row = table.TokenRow(seen, SANDBOX)

    await h.keep()

    assert h.db.saved[0][4] == seen


@pytest.mark.asyncio
async def test_the_first_mint_names_no_expiry_as_seen(h):
    await h.keep()

    assert h.db.saved[0][4] is None


@pytest.mark.asyncio
async def test_a_lost_compare_and_set_writes_nothing(h):
    h.db.save_wins = False

    assert await h.keep() is False

    assert h.events == ["current", "save"]
    assert h.written == []
    assert h.db.held == []


@pytest.mark.asyncio
async def test_a_failed_write_is_not_marked_held_so_the_next_pass_tries_again(h):
    h.write_ok = False

    assert await h.keep() is False

    assert h.events == ["current", "save", "drop", "write"]
    assert h.db.held == []


# -- keep: never raises -----------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("step", ["current", "save", "mark_held"])
async def test_a_database_error_makes_keep_false_and_never_raises(h, step):
    h.db.fail_at = step

    assert await h.keep() is False


@pytest.mark.asyncio
async def test_a_write_that_raises_makes_keep_false_and_never_raises(h):
    h.write_error = OSError("sandbox gone")

    assert await h.keep() is False

    assert h.db.held == []


@pytest.mark.asyncio
async def test_a_mirror_drop_that_raises_makes_keep_false_and_never_raises(h, monkeypatch):
    async def explode(kind, computer_id):
        raise RuntimeError("redis exploded")

    monkeypatch.setattr(core, "drop_mirror", explode)

    assert await h.keep() is False

    assert h.written == []


@pytest.mark.asyncio
async def test_a_failure_log_names_the_error_type_and_never_the_token(h, caplog):
    h.db.fail_at = "save"

    await h.keep()

    assert "RuntimeError" in caplog.text
    assert "lac1." not in caplog.text


# -- revoke -----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revoke_deletes_the_row_and_drops_the_mirror(h):
    await cli_tokens.revoke(COMPUTER)

    assert h.events == ["delete", "drop"]
    assert h.db.deleted == [COMPUTER]


@pytest.mark.asyncio
async def test_revoke_still_drops_the_mirror_when_the_row_stays(h):
    h.db.fail_at = "delete"

    await cli_tokens.revoke(COMPUTER)

    assert h.events == ["delete", "drop"]


@pytest.mark.asyncio
async def test_a_mirror_the_revoke_could_not_drop_is_retried_until_it_drops(
    h, monkeypatch, caplog
):
    """A Redis back before the mirror lapses must not serve the ended tokens."""
    answers = iter([False, False, False, True])

    async def drop_mirror(kind, computer_id):
        h.events.append("drop")
        return next(answers)

    monkeypatch.setattr(core, "drop_mirror", drop_mirror)

    await cli_tokens.revoke(COMPUTER)
    (pending,) = core._dropping
    await pending

    assert h.events == ["delete", "drop", "drop", "drop", "drop"]
    assert "mirror not dropped" in caplog.text


@pytest.mark.asyncio
async def test_revoke_never_fails_the_stop_when_the_row_will_not_go(h, caplog):
    h.db.fail_at = "delete"

    await cli_tokens.revoke(COMPUTER)

    assert "RuntimeError" in caplog.text


# -- authenticate -----------------------------------------------------------------


def _row(secret: str) -> dict:
    return {
        "user_id": USER,
        "token_sha256": core.digest(secret),
        "expires_at": _left(timedelta(minutes=30)),
        "prev_token_sha256": None,
        "prev_expires_at": None,
    }


@pytest.fixture
def stored(monkeypatch):
    """The row ``load_token`` answers, with no Redis."""
    rows: dict[str, dict] = {}
    load = AsyncMock(side_effect=lambda computer_id: rows.get(computer_id))
    monkeypatch.setattr(cli_tokens, "db", type("Db", (), {"load_token": load}))
    monkeypatch.setattr(cli_tokens, "_client", lambda: None)
    return rows


@pytest.mark.asyncio
async def test_a_live_token_authenticates_as_its_computer_and_user_with_every_scope(stored):
    stored[COMPUTER] = _row("secret")

    identity = await cli_tokens.authenticate(f"lac1.{COMPUTER}.secret")

    assert identity == cli_tokens.CliIdentity(COMPUTER, USER, cli_tokens.SCOPES)


@pytest.mark.asyncio
async def test_a_forged_secret_is_refused(stored):
    stored[COMPUTER] = _row("secret")

    with pytest.raises(core.Refused):
        await cli_tokens.authenticate(f"lac1.{COMPUTER}.forged")


@pytest.mark.asyncio
async def test_a_token_of_another_kind_is_refused_before_any_lookup(stored):
    with pytest.raises(core.Refused):
        await cli_tokens.authenticate(f"lfs1.{COMPUTER}.secret")

    cli_tokens.db.load_token.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_computer_without_a_row_refuses(stored):
    with pytest.raises(core.Refused):
        await cli_tokens.authenticate(f"lac1.{COMPUTER}.secret")


@pytest.mark.asyncio
async def test_a_computer_past_its_rate_is_throttled_with_whole_seconds_to_wait(monkeypatch):
    monkeypatch.setattr(
        core, "admit", AsyncMock(side_effect=core.Throttled(COMPUTER, USER, wait_ms=2500))
    )

    with pytest.raises(cli_tokens.Throttled) as caught:
        await cli_tokens.authenticate(f"lac1.{COMPUTER}.secret")

    assert caught.value.retry_after == 3


@pytest.mark.asyncio
async def test_a_token_the_mirror_admits_acts_for_the_user_it_holds(monkeypatch):
    monkeypatch.setattr(core, "admit", AsyncMock(return_value=core.Admitted(COMPUTER, USER)))

    identity = await cli_tokens.authenticate(f"lac1.{COMPUTER}.secret")

    assert identity == cli_tokens.CliIdentity(COMPUTER, USER, cli_tokens.SCOPES)
