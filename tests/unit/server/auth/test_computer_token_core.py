"""What the file mount's token and the CLI's share: the shape, the digest, the
mint and revoke, and the admission that reads the Redis mirror first and the
row when it lacks one.

The Lua scripts are not run here. ``FakeRedis`` answers the three of them in
Python with the contract their comments state, so these tests pin how
``admit`` calls and reads them.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import time
from datetime import UTC, datetime, timedelta

import pytest

from src.server.auth import computer_token_core as core

COMPUTER = "c0ffee00-0000-4000-8000-000000000001"
KIND = core.Kind(
    "tst1",
    lambda computer_id: f"tst:{{{computer_id}}}",
    rate_per_s=10,
    burst=60,
    ttl=timedelta(hours=1),
    client=lambda: None,
)
WHO = ["user-core-1", ["agent:read"]]


def _now_ms() -> int:
    return int(time.time() * 1000)


class FakeRedis:
    """The mirror, generation and bucket one computer's tokens keep."""

    def __init__(self) -> None:
        self.mirror: dict[str, str] = {}
        self.mirror_until_ms: int | None = None
        self.generation = 0
        self.wait_ms = 0
        self.calls: list[str] = []
        self.args: dict[str, tuple] = {}
        self.fail: set[str] = set()

    async def eval(self, script, numkeys, *args):
        name = {core.ADMIT: "admit", core.ADMIT_FROM_ROW: "from_row", core.DROP: "drop"}[script]
        self.calls.append(name)
        self.args[name] = (numkeys, *args)
        if name in self.fail:
            raise ConnectionError("redis down")
        return getattr(self, f"_{name}")(*args)

    def _admit(self, mirror_key, gen_key, rate_key, field, rate, burst):
        entry = self.mirror.get(field)
        if entry is not None and int(entry.partition("|")[0]) <= _now_ms():
            entry = None
        if entry is None:
            return [2] if self.mirror else [0, str(self.generation)]
        return [1, entry, self.wait_ms]

    def _from_row(self, mirror_key, gen_key, rate_key, seen, rate, burst, *mirror):
        if mirror and str(self.generation) == seen:
            self.mirror = dict(zip(mirror[1::2], mirror[2::2], strict=True))
            self.mirror_until_ms = int(mirror[0])
        return self.wait_ms

    def _drop(self, mirror_key, gen_key, ttl):
        self.generation += 1
        self.mirror = {}
        return 1


def _row(secret: str, *, current_for=timedelta(minutes=30), prev_secret=None, prev_for=None):
    now = datetime.now(UTC)
    return {
        "user_id": WHO[0],
        "scopes": WHO[1],
        "token_sha256": core.digest(secret),
        "expires_at": now + current_for,
        "prev_token_sha256": core.digest(prev_secret) if prev_secret else None,
        "prev_expires_at": now + prev_for if prev_secret and prev_for else None,
    }


class Rows:
    """``load_row``, counting how often the row is read."""

    def __init__(self, row) -> None:
        self.row = row
        self.reads = 0

    async def __call__(self):
        self.reads += 1
        return self.row


def _who(row):
    return [row["user_id"], list(row["scopes"])]


def _kind(client):
    return dataclasses.replace(KIND, client=lambda: client)


async def _admit(client, secret, rows):
    return await core.admit(_kind(client), COMPUTER, core.digest(secret), rows, _who)


# -- the shape of a token ---------------------------------------------------------


def test_a_token_names_its_computer_and_the_digest_of_its_secret():
    computer_id, token_digest = core.parse(f"tst1.{COMPUTER}.the-secret", "tst1")

    assert computer_id == COMPUTER
    assert token_digest == hashlib.sha256(b"the-secret").digest()


def test_the_computer_id_is_read_in_its_canonical_spelling():
    computer_id, _ = core.parse(f"tst1.{COMPUTER.upper()}.s", "tst1")

    assert computer_id == COMPUTER


def test_a_secret_may_hold_dots():
    _, token_digest = core.parse(f"tst1.{COMPUTER}.a.b.c", "tst1")

    assert token_digest == core.digest("a.b.c")


def test_surrounding_whitespace_is_ignored():
    computer_id, _ = core.parse(f"  tst1.{COMPUTER}.s\n", "tst1")

    assert computer_id == COMPUTER


@pytest.mark.parametrize(
    "token",
    [
        "",
        "tst1",
        f"tst1.{COMPUTER}",
        f"tst1.{COMPUTER}.",
        f"other.{COMPUTER}.secret",
        f"TST1.{COMPUTER}.secret",
        "tst1.not-a-uuid.secret",
        "tst1..secret",
        f".{COMPUTER}.secret",
    ],
)
def test_a_token_not_of_this_kind_is_refused(token):
    with pytest.raises(core.Refused):
        core.parse(token, "tst1")


def test_the_mirror_field_is_a_fixed_prefix_of_the_digest_in_hex():
    token_digest = core.digest("s")

    assert core.field(token_digest) == token_digest.hex()[:32]
    assert len(core.field(token_digest)) == 32


def test_a_kind_keeps_the_mirror_generation_and_rate_under_one_computer_tag():
    assert KIND.keys(COMPUTER) == (
        f"tst:{{{COMPUTER}}}:auth",
        f"tst:{{{COMPUTER}}}:gen",
        f"tst:{{{COMPUTER}}}:rate",
    )


# -- live -------------------------------------------------------------------------


def test_a_digest_is_live_until_its_expiry():
    d = core.digest("s")
    soon = datetime.now(UTC) + timedelta(minutes=1)

    assert core.live(d, soon, d)
    assert not core.live(d, datetime.now(UTC) - timedelta(seconds=1), d)


def test_a_slot_that_holds_nothing_is_not_live():
    d = core.digest("s")
    soon = datetime.now(UTC) + timedelta(minutes=1)

    assert not core.live(None, soon, d)
    assert not core.live(d, None, d)


def test_a_different_digest_is_not_live():
    soon = datetime.now(UTC) + timedelta(minutes=1)

    assert not core.live(core.digest("one"), soon, core.digest("two"))


def test_a_digest_a_driver_returns_as_a_buffer_still_compares():
    d = core.digest("s")
    soon = datetime.now(UTC) + timedelta(minutes=1)

    assert core.live(memoryview(d), soon, d)


# -- the mirror's arguments -------------------------------------------------------


def test_the_mirror_is_empty_when_no_slot_is_live():
    past = datetime.now(UTC) - timedelta(minutes=1)

    assert core.mirror(WHO, [(core.digest("a"), past), (None, None)]) == []


def test_the_mirror_lists_each_live_slot_with_its_expiry_and_who():
    first, second = core.digest("a"), core.digest("b")
    soon = datetime.now(UTC) + timedelta(minutes=2)
    later = soon + timedelta(minutes=1)

    args = core.mirror(WHO, [(first, soon), (second, later)])

    assert args[1] == core.field(first)
    assert args[3] == core.field(second)
    expires_ms, _, serialized = args[2].partition("|")
    assert int(expires_ms) == int(soon.timestamp() * 1000)
    assert json.loads(serialized) == WHO


def test_the_mirror_leaves_out_a_slot_already_past():
    live_digest, dead_digest = core.digest("a"), core.digest("b")
    now = datetime.now(UTC)

    args = core.mirror(
        WHO, [(live_digest, now + timedelta(minutes=2)), (dead_digest, now - timedelta(seconds=1))]
    )

    assert args[1::2] == [core.field(live_digest)]


def test_the_mirror_lapses_when_the_last_slot_does_if_that_is_sooner_than_five_minutes():
    d = core.digest("a")
    expires = datetime.now(UTC) + timedelta(minutes=2)

    args = core.mirror(WHO, [(d, expires)])

    assert args[0] == int(expires.timestamp() * 1000)


def test_the_mirror_lapses_within_five_minutes_whatever_the_tokens_outlast():
    d = core.digest("a")

    args = core.mirror(WHO, [(d, datetime.now(UTC) + timedelta(hours=1))])

    lapse = datetime.fromtimestamp(args[0] / 1000, UTC) - datetime.now(UTC)
    assert timedelta(minutes=4, seconds=55) < lapse <= core.MIRROR_TTL


# -- how long a wait is told -----------------------------------------------------


@pytest.mark.parametrize(
    ("wait_ms", "seconds"), [(0, 1), (1, 1), (1000, 1), (1001, 2), (2500, 3)]
)
def test_retry_after_is_whole_seconds_and_at_least_one(wait_ms, seconds):
    assert core.Throttled(COMPUTER, WHO, wait_ms).retry_after == seconds


# -- admission by the mirror ------------------------------------------------------


@pytest.mark.asyncio
async def test_a_token_the_mirror_holds_is_admitted_without_reading_the_row():
    redis = FakeRedis()
    rows = Rows(_row("secret"))
    await _admit(redis, "secret", rows)  # fills the mirror
    rows.reads = 0

    admitted = await _admit(redis, "secret", rows)

    assert admitted == core.Admitted(COMPUTER, WHO)
    assert rows.reads == 0


@pytest.mark.asyncio
async def test_a_computer_past_its_rate_is_throttled_with_who_its_token_acts_for():
    redis = FakeRedis()
    rows = Rows(_row("secret"))
    await _admit(redis, "secret", rows)
    redis.wait_ms = 1500

    with pytest.raises(core.Throttled) as throttled:
        await _admit(redis, "secret", rows)

    assert (throttled.value.computer_id, throttled.value.who) == (COMPUTER, WHO)
    assert throttled.value.retry_after == 2


@pytest.mark.asyncio
async def test_the_scripts_take_the_computers_three_keys_and_the_kinds_rate():
    redis = FakeRedis()

    await _admit(redis, "secret", Rows(_row("secret")))

    numkeys, *args = redis.args["admit"]
    assert numkeys == 3
    assert tuple(args[:3]) == KIND.keys(COMPUTER)
    assert args[3:] == [core.field(core.digest("secret")), 10, 60]


@pytest.mark.asyncio
async def test_an_expired_mirror_entry_is_refused_without_reading_the_row():
    """The row's slot ran out with it: the mirror copies each slot's expiry."""
    redis = FakeRedis()
    redis.mirror[core.field(core.digest("secret"))] = f"{_now_ms() - 1}|{json.dumps(WHO)}"
    rows = Rows(_row("secret"))

    with pytest.raises(core.Refused):
        await _admit(redis, "secret", rows)

    assert rows.reads == 0


# -- admission by the row ---------------------------------------------------------


@pytest.mark.asyncio
async def test_a_token_the_mirror_lacks_is_admitted_by_the_row_and_mirrored():
    redis = FakeRedis()
    rows = Rows(_row("secret"))

    admitted = await _admit(redis, "secret", rows)

    assert admitted == core.Admitted(COMPUTER, WHO)
    assert rows.reads == 1
    assert core.field(core.digest("secret")) in redis.mirror


@pytest.mark.asyncio
async def test_the_row_admitted_token_is_then_admitted_by_the_mirror_with_the_same_identity():
    redis = FakeRedis()
    rows = Rows(_row("secret"))
    first = await _admit(redis, "secret", rows)

    second = await _admit(redis, "secret", rows)

    assert second.who == first.who
    assert rows.reads == 1
    assert redis.calls == ["admit", "from_row", "admit"]


@pytest.mark.asyncio
async def test_the_row_path_passes_the_generation_it_saw_before_reading_the_row():
    redis = FakeRedis()
    redis.generation = 7

    await _admit(redis, "secret", Rows(_row("secret")))

    _, *args = redis.args["from_row"]
    assert args[3] == "7"


@pytest.mark.asyncio
async def test_a_change_to_the_token_set_while_the_row_loads_keeps_the_stale_copy_out_of_the_mirror():
    redis = FakeRedis()
    row = _row("secret")

    async def load_row():
        await core.drop_mirror(_kind(redis), COMPUTER)  # a mint or revoke lands
        return row

    admitted = await _admit(redis, "secret", load_row)

    assert admitted.who == WHO
    assert redis.mirror == {}


@pytest.mark.asyncio
async def test_the_row_path_takes_from_the_rate_too():
    redis = FakeRedis()
    redis.wait_ms = 2000

    with pytest.raises(core.Throttled) as throttled:
        await _admit(redis, "secret", Rows(_row("secret")))

    assert throttled.value.wait_ms == 2000


# -- tokens the row refuses -------------------------------------------------------


@pytest.mark.asyncio
async def test_a_digest_the_row_does_not_hold_is_refused_and_spends_no_rate():
    redis = FakeRedis()

    with pytest.raises(core.Refused):
        await _admit(redis, "forged", Rows(_row("secret")))

    assert redis.calls == ["admit"]
    assert redis.mirror == {}


@pytest.mark.asyncio
async def test_no_row_for_the_computer_refuses():
    with pytest.raises(core.Refused):
        await _admit(FakeRedis(), "secret", Rows(None))


@pytest.mark.asyncio
async def test_an_expired_current_token_is_refused():
    row = _row("secret", current_for=timedelta(seconds=-1))

    with pytest.raises(core.Refused):
        await _admit(FakeRedis(), "secret", Rows(row))


@pytest.mark.asyncio
async def test_a_dead_digest_the_mirror_never_held_is_refused_even_with_a_live_neighbour():
    redis = FakeRedis()
    rows = Rows(_row("secret"))
    await _admit(redis, "secret", rows)

    with pytest.raises(core.Refused):
        await _admit(redis, "forged", rows)

    assert rows.reads == 1


@pytest.mark.asyncio
async def test_a_digest_a_standing_mirror_lacks_is_refused_without_reading_the_row():
    """Each mint and revoke drops the mirror, so one standing holds every live
    digest the row does: a made-up token costs no database read."""
    redis = FakeRedis()
    rows = Rows(_row("secret"))
    await _admit(redis, "secret", rows)

    for _ in range(3):
        with pytest.raises(core.Refused):
            await _admit(redis, "forged", rows)

    assert rows.reads == 1
    assert redis.calls == ["admit", "from_row", "admit", "admit", "admit"]


# -- the previous slot ------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_previous_token_is_admitted_until_its_own_expiry():
    row = _row("new", prev_secret="old", prev_for=timedelta(minutes=10))

    admitted = await _admit(FakeRedis(), "old", Rows(row))

    assert admitted.who == WHO


@pytest.mark.asyncio
async def test_the_previous_token_is_refused_once_its_expiry_passes():
    row = _row("new", prev_secret="old", prev_for=timedelta(seconds=-1))

    with pytest.raises(core.Refused):
        await _admit(FakeRedis(), "old", Rows(row))


@pytest.mark.asyncio
async def test_a_row_admission_mirrors_both_live_slots():
    redis = FakeRedis()
    row = _row("new", prev_secret="old", prev_for=timedelta(minutes=10))

    await _admit(redis, "new", Rows(row))

    assert set(redis.mirror) == {core.field(core.digest("new")), core.field(core.digest("old"))}


@pytest.mark.asyncio
async def test_a_previous_token_in_the_mirror_is_admitted_without_the_row():
    redis = FakeRedis()
    row = _row("new", prev_secret="old", prev_for=timedelta(minutes=10))
    rows = Rows(row)
    await _admit(redis, "new", rows)

    admitted = await _admit(redis, "old", rows)

    assert admitted.who == WHO
    assert rows.reads == 1


# -- with no Redis, or Redis failing ---------------------------------------------


@pytest.mark.asyncio
async def test_with_no_redis_the_row_answers_alone_and_nothing_waits():
    rows = Rows(_row("secret"))

    admitted = await _admit(None, "secret", rows)

    assert admitted == core.Admitted(COMPUTER, WHO)
    assert rows.reads == 1


@pytest.mark.asyncio
async def test_with_no_redis_a_dead_digest_is_still_refused():
    with pytest.raises(core.Refused):
        await _admit(None, "forged", Rows(_row("secret")))


@pytest.mark.asyncio
async def test_a_failing_admission_script_falls_back_to_the_row_without_a_second_try():
    redis = FakeRedis()
    redis.fail = {"admit"}

    admitted = await _admit(redis, "secret", Rows(_row("secret")))

    assert admitted == core.Admitted(COMPUTER, WHO)
    assert redis.calls == ["admit"]


@pytest.mark.asyncio
async def test_a_failing_mirror_write_still_admits_what_the_row_admitted():
    redis = FakeRedis()
    redis.fail = {"from_row"}

    admitted = await _admit(redis, "secret", Rows(_row("secret")))

    assert admitted == core.Admitted(COMPUTER, WHO)


# -- dropping the mirror ----------------------------------------------------------


@pytest.mark.asyncio
async def test_dropping_the_mirror_empties_it_and_moves_the_generation():
    redis = FakeRedis()
    await _admit(redis, "secret", Rows(_row("secret")))

    assert await core.drop_mirror(_kind(redis), COMPUTER) is True

    assert redis.mirror == {}
    assert redis.generation == 1
    numkeys, *args = redis.args["drop"]
    assert numkeys == 2
    assert args == [*KIND.keys(COMPUTER)[:2], core.GENERATION_TTL_S]


@pytest.mark.asyncio
async def test_there_is_nothing_to_drop_without_redis():
    assert await core.drop_mirror(_kind(None), COMPUTER) is True


@pytest.mark.asyncio
async def test_a_drop_redis_cannot_do_reports_false():
    redis = FakeRedis()
    redis.fail = {"drop"}

    assert await core.drop_mirror(_kind(redis), COMPUTER) is False


# -- minting ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_mint_saves_the_digest_of_the_secret_it_hands_out_and_drops_the_mirror():
    redis = FakeRedis()
    saved = []

    async def save(token_digest, expires_at):
        saved.append((token_digest, expires_at))
        return True

    minted = await core.mint(_kind(redis), COMPUTER, save)

    prefix, computer_id, secret = minted.token.split(".", 2)
    assert (prefix, computer_id) == ("tst1", COMPUTER)
    assert saved == [(core.digest(secret), minted.expires_at)]
    assert timedelta(minutes=59) < minted.expires_at - datetime.now(UTC) <= KIND.ttl
    assert redis.calls == ["drop"]


@pytest.mark.asyncio
async def test_a_save_that_refuses_mints_nothing_and_leaves_the_mirror():
    redis = FakeRedis()

    async def refuse(token_digest, expires_at):
        return False

    assert await core.mint(_kind(redis), COMPUTER, refuse) is None
    assert redis.calls == []


@pytest.mark.asyncio
async def test_a_mirror_the_mint_could_not_drop_is_dropped_once_redis_answers(monkeypatch):
    """Left standing, it would refuse the token just minted until it lapses."""
    answers = iter([False, False, False, True])
    drops: list[str] = []

    async def drop(kind, computer_id):
        drops.append(computer_id)
        return next(answers)

    async def save(token_digest, expires_at):
        return True

    monkeypatch.setattr(core, "drop_mirror", drop)
    monkeypatch.setattr(core, "_DROP_RETRY_S", 0)
    monkeypatch.setattr(core, "_REVOKE_ATTEMPTS", 3)

    assert await core.mint(KIND, COMPUTER, save) is not None
    (pending,) = core._dropping
    await asyncio.wait_for(pending, timeout=5)

    assert drops == [COMPUTER] * 4
    assert not core._dropping


@pytest.mark.asyncio
async def test_every_mint_makes_a_new_secret():
    async def save(token_digest, expires_at):
        return True

    first = await core.mint(KIND, COMPUTER, save)
    second = await core.mint(KIND, COMPUTER, save)

    assert first.token != second.token


# -- revoking ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_revoke_deletes_the_row_then_drops_the_mirror():
    redis = FakeRedis()

    async def delete():
        # The mirror still stands while the row goes.
        redis.calls.append("delete")

    await core.revoke(_kind(redis), COMPUTER, delete)

    assert redis.calls == ["delete", "drop"]
    assert redis.generation == 1


@pytest.mark.asyncio
async def test_a_revoke_whose_row_delete_fails_still_drops_the_mirror_and_raises():
    """Nothing rewrites the mirror after a revoke, so one left standing would
    keep serving the ended tokens until it lapses."""
    redis = FakeRedis()

    async def delete():
        raise OSError("db down")

    with pytest.raises(OSError):
        await core.revoke(_kind(redis), COMPUTER, delete)

    assert redis.calls == ["drop"]


@pytest.mark.asyncio
async def test_a_mirror_the_revoke_could_not_drop_is_dropped_once_redis_answers(monkeypatch):
    answers = iter([False, False, False, False, True])
    drops: list[str] = []

    async def drop(kind, computer_id):
        drops.append(computer_id)
        return next(answers)

    async def delete():
        return None

    monkeypatch.setattr(core, "drop_mirror", drop)
    monkeypatch.setattr(core, "_DROP_RETRY_S", 0)
    monkeypatch.setattr(core, "_REVOKE_ATTEMPTS", 3)

    await core.revoke(KIND, COMPUTER, delete)
    (pending,) = core._dropping
    await asyncio.wait_for(pending, timeout=5)

    assert drops == [COMPUTER] * 5
    assert not core._dropping


# -- when a sandbox is owed a token -----------------------------------------------


def _held(expires_in: timedelta | None, held_by: str | None):
    expires_at = None if expires_in is None else datetime.now(UTC) + expires_in
    return dataclasses.make_dataclass("Row", ["expires_at", "held_by"])(expires_at, held_by)


REFRESH = timedelta(minutes=30)


@pytest.mark.parametrize(
    ("row", "sandbox_id"),
    [
        (_held(None, None), "sb-1"),
        (_held(timedelta(hours=1), None), "sb-1"),
        (_held(timedelta(hours=1), "another"), "sb-1"),
        (_held(timedelta(hours=1), "sb-1"), None),
        (_held(REFRESH - timedelta(minutes=1), "sb-1"), "sb-1"),
        (_held(timedelta(minutes=-1), "sb-1"), "sb-1"),
    ],
    ids=["no-token", "no-write-landed", "another-sandbox", "unknown-sandbox", "runs-low", "expired"],
)
def test_a_sandbox_is_owed_a_token_unless_it_holds_one_with_time_left(row, sandbox_id):
    assert core.due(row, sandbox_id, REFRESH)


def test_a_sandbox_holding_a_token_with_time_left_is_not_owed_one():
    assert not core.due(_held(REFRESH + timedelta(minutes=1), "sb-1"), "sb-1", REFRESH)
