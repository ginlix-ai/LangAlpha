"""The Claude plan's rows (migration 111's ``claude_code_plan``) against real
PostgreSQL: each report merges into the credential's state under the row's
lock, and a replaced token's reports never land on the new one's."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from psycopg.rows import dict_row, tuple_row

from src.llms.claude_code import PlanLimit, PlanReading

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

T0 = datetime(2026, 11, 6, 18, 0, tzinfo=UTC)
RESETS = T0 + timedelta(hours=5)
SONNET, OPUS = "claude-sonnet-5-5", "claude-opus-5-5"


@pytest_asyncio.fixture
async def user(seed_user, patched_get_db_connection, monkeypatch):
    """The store gets tuple rows, as the app's pool hands them."""
    from src.server.database import claude_code_plan

    @asynccontextmanager
    async def _app_rows(conn=None):
        async with patched_get_db_connection(conn) as owned:
            owned.row_factory = tuple_row
            try:
                yield owned
            finally:
                owned.row_factory = dict_row

    monkeypatch.setattr(claude_code_plan, "get_db_connection", _app_rows)
    yield seed_user["user_id"]


async def _report(user_id, minutes, credential="saved", *, generation=0, model=SONNET, windows=None, limit=None,
                  billed=False, credits=None) -> bool:
    from src.server.database.claude_code_plan import update_plan
    from src.server.services.claude_code.plan import PlanState

    reading = PlanReading(T0 + timedelta(minutes=minutes), model, windows or {}, limit,
                          refused=limit is not None and not billed, billed=billed, credits=credits)
    return await update_plan(user_id, credential, generation, reading.seen_at,
                             lambda state: PlanState.of(state).merge(reading).to_json())


async def _row(user_id, credential="saved"):
    from src.server.database.claude_code_plan import get_plans

    return (await get_plans(user_id, [credential])).get(credential)


async def test_a_report_moves_only_the_windows_it_carried_and_a_late_one_never_undoes_a_newer_one(user):
    await _report(user, 0, windows={"five_hour": (0.2, RESETS), "seven_day": (0.5, RESETS + timedelta(days=3))})
    await _report(user, 10, windows={"five_hour": (0.9, RESETS)}, credits="off")
    await _report(user, 5, windows={"five_hour": (0.5, RESETS)}, credits="on")
    row = await _row(user)
    windows = row["state"]["windows"]
    assert (windows["five_hour"]["used"], windows["seven_day"]["used"], row["state"]["credits"]["value"]) == (0.9, 0.5, "off")
    assert row["updated_at"] == T0 + timedelta(minutes=10)


async def test_every_limit_holds_until_its_own_reset_or_a_request_its_model_ran(user):
    week = T0 + timedelta(days=3)
    await _report(user, 0, model=OPUS, limit=PlanLimit("seven_day_opus", week))
    await _report(user, 1, limit=PlanLimit("five_hour", RESETS))
    assert set((await _row(user))["state"]["limits"]) == {"seven_day_opus", "five_hour"}
    await _report(user, 2)
    assert set((await _row(user))["state"]["limits"]) == {"seven_day_opus"}
    await _report(user, 3, model=OPUS)
    assert (await _row(user))["state"]["limits"] == {}


async def test_writes_at_once_each_land_under_the_rows_lock(user):
    await asyncio.gather(*(_report(user, i, windows={name: (0.1 * i, RESETS)})
                           for i, name in enumerate(["five_hour", "seven_day"], start=1)))
    assert set((await _row(user))["state"]["windows"]) == {"five_hour", "seven_day"}


async def test_a_replaced_tokens_report_is_dropped_and_the_same_token_saved_again_keeps_its_state(user):
    from src.server.database.claude_code_plan import forget_plan

    await forget_plan(user, "saved", token_digest="a")
    await _report(user, 0, generation=1, limit=PlanLimit("five_hour", RESETS))
    await forget_plan(user, "saved", token_digest="a")
    assert (await _row(user))["state"]["limits"], "saving the same token again keeps what it reported"
    await forget_plan(user, "saved", token_digest="b")
    row = await _row(user)
    assert (row["generation"], row["state"], row["token_digest"]) == (2, {}, "b")
    assert not await _report(user, 1, generation=1, limit=PlanLimit("five_hour", RESETS))
    assert (await _row(user))["state"] == {}, "a run still on the old token writes nothing onto the new one"
    await forget_plan(user, "saved")
    assert (await _row(user))["generation"] == 3, "a removal always moves it"
    await _report(user, 0, "computer:c1", limit=PlanLimit("five_hour", RESETS))
    await forget_plan(user, "computer:c1")
    row = await _row(user, "computer:c1")
    assert (row["generation"], row["state"]) == (1, {}), "so does one of a row no token was ever saved on"


async def test_choosing_usage_credits_makes_them_unknown_on_every_row_until_a_newer_report(user):
    from src.server.services.claude_code.plan import credits_unknown

    await _report(user, 0, credits="off")
    await _report(user, 0, "computer:c1", credits="off")
    await _report(user, 0, "computer:c2", windows={"five_hour": (0.1, RESETS)})
    await credits_unknown(user)
    assert [(await _row(user, c))["state"].get("credits", {}).get("value") for c in ("saved", "computer:c1")] == [None, None]
    assert (await _row(user, "computer:c2"))["state"]["credits"]["value"] is None, "a row that never said is fenced too"
    await _report(user, 1, credits="off")
    await _report(user, 1, "computer:c2", credits="off")
    assert [(await _row(user, c))["state"]["credits"]["value"] for c in ("saved", "computer:c2")] == [None, None], (
        "a report seen before the choice"
    )


async def test_a_removed_computers_row_goes(user):
    from src.server.database.claude_code_plan import drop_plan, forget_plan, get_plans

    await _report(user, 0, "computer:c1", windows={"five_hour": (0.7, RESETS)})
    await _report(user, 0, windows={"five_hour": (0.1, RESETS)})
    await drop_plan(user, "computer:c1")
    assert set(await get_plans(user)) == {"saved"}
    await forget_plan(user, "computer:c2", token_digest="d")
    await drop_plan(user, "computer:c2")
    assert not await _report(user, 1, "computer:c2", generation=1), "a run still on it writes nothing"
    assert "computer:c2" not in await get_plans(user)


async def test_each_credential_keeps_its_own_row_and_goes_with_the_user(user, patched_get_db_connection):
    from src.server.database.claude_code_plan import forget_plan, get_plans

    await _report(user, 0, windows={"five_hour": (0.1, RESETS)})
    await _report(user, 0, "computer:c1", windows={"five_hour": (0.7, RESETS)})
    await forget_plan(user, "saved")
    rows = await get_plans(user)
    assert (rows["saved"]["state"], rows["computer:c1"]["state"]["windows"]["five_hour"]["used"]) == ({}, 0.7)
    async with patched_get_db_connection() as conn:
        await conn.execute("DELETE FROM users WHERE user_id = %s", (user,))
    assert await get_plans(user) == {}
