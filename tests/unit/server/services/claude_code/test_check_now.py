"""Check now: one minimal request on a credential's plan, held 5 minutes per
credential on its row, given back where no request was spent, run under the
holds the token's own check and its changes take, and answering only once
the plan it read is written."""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from src.llms.claude_code import PlanReading
from src.server.app import claude_code as route
from src.server.database import claude_code_plan as store
from src.server.services.claude_code import machine, saved
from src.server.services.claude_code import checks
from src.server.services.claude_code import plan as plan_module
from src.server.services.claude_code.check_now import CHECK_EVERY
from src.server.services.claude_code.machine import change_lock, unreached_key
from src.server.models.claude_code import ClaudeCodeComputer
from src.server.services.claude_code.plan import CredentialPlan, check_in, plan_view
from src.server.services.claude_code.saved import SavedHome, check_lock
from src.server.services.oauth_signin import OAuthRefusal
from tests.conftest import create_test_app
from tests.unit.server.services.claude_code.fakes import Cache, serving

NOW = datetime(2026, 11, 7, 8, 0, tzinfo=UTC)
TOKEN = "sk-ant-oat01-placeholder"
#: The store's own, which the unit suite stubs out (``_no_claude_plan_rows``).
FORGET_PLAN = store.forget_plan


def _row(state: dict, checked_at: datetime | None) -> dict:
    return {"state": state, "checked_at": checked_at, "updated_at": NOW - timedelta(hours=1), "generation": 1, "read_at": NOW}


def test_a_check_within_5_minutes_holds_the_credential_for_the_rest_of_them():
    assert check_in(_row({}, NOW - timedelta(minutes=1))) == 240
    assert check_in(_row({}, NOW - CHECK_EVERY)) is None and check_in(_row({}, None)) is None
    assert plan_view(_row({}, NOW - timedelta(minutes=1)), NOW) is None, "a hold is no report of the plan"


@pytest.fixture
def db(monkeypatch, mock_connection, mock_cursor):
    @asynccontextmanager
    async def connection(conn=None):
        yield mock_connection

    monkeypatch.setattr(store, "get_db_connection", connection)
    return mock_cursor


def _statement(cursor, i: int = 0) -> str:
    return re.sub(r"\s+", " ", cursor.execute.call_args_list[i].args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("taken", [True, False])
async def test_the_hold_is_taken_in_one_statement_and_counted_on_the_databases_clock(db, taken):
    db.fetchone.side_effect = [(NOW,)] if taken else [None, (Decimal("179.2"),)]
    assert await store.hold_check("u", "saved", CHECK_EVERY) == ((NOW, 300) if taken else (None, 180))
    assert "ON CONFLICT (user_id, credential) DO UPDATE SET checked_at = NOW()" in _statement(db)
    assert "WHERE p.checked_at IS NULL OR p.checked_at <= NOW() - %s RETURNING checked_at" in _statement(db)
    if not taken:
        assert "EXTRACT(EPOCH FROM checked_at + %s - NOW())" in _statement(db, 1)


@pytest.mark.asyncio
async def test_a_hold_given_back_frees_only_the_one_taken(db):
    await store.give_back_check("u", "saved", NOW)
    assert "SET checked_at = NULL WHERE user_id = %s AND credential = %s AND checked_at = %s" in _statement(db)
    assert db.execute.call_args.args[1] == ("u", "saved", NOW)


@pytest.mark.asyncio
async def test_a_new_token_frees_its_credentials_hold(db):
    await FORGET_PLAN("u", "saved", token_digest="d")
    assert "token_digest = EXCLUDED.token_digest, checked_at = NULL," in _statement(db)


@pytest.fixture
def holds(monkeypatch):
    """The store's hold, taken unless ``held``, and the holds given back."""
    state = SimpleNamespace(held=False, taken=[], given=[])

    async def hold_check(user_id, credential, every):
        state.taken.append(credential)
        return (None, 120) if state.held else (NOW, 300)

    async def give_back_check(user_id, credential, stamp):
        state.given.append((credential, stamp))

    monkeypatch.setattr(store, "hold_check", hold_check)
    monkeypatch.setattr(store, "give_back_check", give_back_check)
    return state


@pytest.fixture
def saved_token(monkeypatch, holds):
    """A saved token that works, where this server runs turns on it, read
    with whether the save hold stood (``read_under``), and what its check
    answers."""
    state = SimpleNamespace(cache=Cache(), checks=[], answer="works", read_under=[], on_plan=None, skips=False)

    async def yes(*_):
        return True

    async def stored_row(user_id):
        state.read_under.append(SavedHome(user_id).lock in state.cache.locks)
        return {"access_token": TOKEN, "updated_at": NOW}

    async def no(*_):
        return False

    async def check_token(user_id, row, key, held, *, answered=lambda: None, skipped=lambda: None, plan=None):
        state.checks.append(key)
        if state.skips:
            skipped()
            return state.answer
        if state.on_plan is not None:
            answered()
            state.on_plan(plan, "saved")
        return state.answer

    for name, value in (("rows_on", yes), ("saved_token_runs", yes), ("usable", yes), ("stored_row", stored_row),
                        ("check_token", check_token), ("local_managed", no)):
        monkeypatch.setattr(saved, name, value)
    with serving(state.cache):
        yield state


@pytest.mark.asyncio
async def test_a_check_runs_once_per_hold_and_says_when_the_next_may_go(saved_token, holds):
    await saved.check_now("u")
    holds.held = True
    with pytest.raises(OAuthRefusal) as refused:
        await saved.check_now("u")
    assert (refused.value.status_code, refused.value.error["code"], refused.value.error["check_in"]) == (429, "check_held", 120)
    assert len(saved_token.checks) == 1, "a held check sends nothing"
    assert holds.given == [], "a check that ran keeps its hold"
    assert saved_token.read_under == [True, True], "the token checked is the one saved, read under the save hold"


@pytest.mark.asyncio
@pytest.mark.parametrize("lock", ["check", "save"])
async def test_a_check_beside_the_tokens_own_or_a_change_takes_no_hold(saved_token, holds, lock):
    saved_token.cache.locks[check_lock(TOKEN) if lock == "check" else SavedHome("u").lock] = "another worker"
    with pytest.raises(OAuthRefusal) as refused:
        await saved.check_now("u")
    assert (refused.value.status_code, refused.value.error["code"]) == (409, "check_in_progress")
    assert holds.taken == [], "the hold stays free for the check after"


@pytest.mark.asyncio
async def test_a_check_that_could_not_reach_claude_says_so_and_gives_the_hold_back(saved_token, holds):
    saved_token.answer = "unknown"
    with pytest.raises(OAuthRefusal) as refused:
        await saved.check_now("u")
    assert (refused.value.status_code, refused.value.error["code"]) == (503, "check_unavailable")
    assert holds.given == [("saved", NOW)]


@pytest.mark.asyncio
@pytest.mark.parametrize("ends", [TimeoutError, asyncio.CancelledError])
async def test_a_check_cut_off_gives_the_hold_back(saved_token, holds, monkeypatch, ends):
    async def check_token(user_id, row, key, held, *, answered=lambda: None, skipped=lambda: None, plan=None):
        raise ends

    monkeypatch.setattr(saved, "check_token", check_token)
    with pytest.raises(OAuthRefusal if ends is TimeoutError else asyncio.CancelledError):
        await saved.check_now("u")
    assert holds.given == [("saved", NOW)]


@pytest.mark.asyncio
async def test_a_hold_that_cannot_be_given_back_leaves_the_checks_own_refusal(saved_token, holds, monkeypatch):
    monkeypatch.setattr(store, "give_back_check", AsyncMock(side_effect=RuntimeError("db down")))
    saved_token.answer = "unknown"
    with pytest.raises(OAuthRefusal) as refused:
        await saved.check_now("u")
    assert refused.value.error["code"] == "check_unavailable"


@pytest.mark.asyncio
async def test_a_hold_being_given_back_outlasts_a_second_cancel(saved_token, holds, monkeypatch):
    entered, release, given = asyncio.Event(), asyncio.Event(), []

    async def give_back_check(user_id, credential, stamp):
        entered.set()
        await release.wait()
        given.append(credential)

    async def check_token(*_, **__):
        raise asyncio.CancelledError

    monkeypatch.setattr(store, "give_back_check", give_back_check)
    monkeypatch.setattr(saved, "check_token", check_token)
    task = asyncio.ensure_future(saved.check_now("u"))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.01)
    assert given == ["saved"]


@pytest.mark.asyncio
async def test_a_check_claude_answered_keeps_its_hold_whatever_fails_after(saved_token, holds, monkeypatch):
    async def check_token(user_id, row, key, held, *, answered=lambda: None, skipped=lambda: None, plan=None):
        answered()
        raise RuntimeError("the flag could not be cleared")

    monkeypatch.setattr(saved, "check_token", check_token)
    with pytest.raises(RuntimeError):
        await saved.check_now("u")
    assert holds.given == [], "the request was spent"


@pytest.mark.asyncio
@pytest.mark.parametrize("on", ["saved", "computer"])
async def test_a_check_cut_off_after_claude_answered_says_the_usage_was_not_kept_and_keeps_its_hold(request, holds, monkeypatch, on):
    request.getfixturevalue("saved_token" if on == "saved" else "computer")

    async def answered_then_cut_off(*_, answered=lambda: None, **__):
        answered()
        raise TimeoutError

    monkeypatch.setattr(*((saved, "check_token") if on == "saved" else (machine, "check_then_keep")), answered_then_cut_off)
    with pytest.raises(OAuthRefusal) as refused:
        await (saved.check_now("u") if on == "saved" else machine.check_now("u", "c-1"))
    assert (refused.value.status_code, refused.value.error["code"]) == (503, "plan_unsaved")
    assert holds.given == [], "the request was spent"


@pytest.mark.asyncio
@pytest.mark.parametrize("on", ["saved", "computer"])
async def test_a_check_without_redis_to_hold_it_says_checking_is_unavailable(request, holds, on):
    request.getfixturevalue("saved_token" if on == "saved" else "computer")
    with serving(Cache(redis=False)), pytest.raises(OAuthRefusal) as refused:
        await (saved.check_now("u") if on == "saved" else machine.check_now("u", "c-1"))
    assert (refused.value.status_code, refused.value.error["code"]) == (503, "redis_unavailable")
    assert refused.value.error["message"] == "Checking usage is unavailable right now. Try again shortly."
    assert holds.taken == []


@pytest.mark.asyncio
async def test_where_an_organizations_policy_is_installed_nothing_is_checked(saved_token, holds, monkeypatch):
    async def yes(*_):
        return True

    monkeypatch.setattr(saved, "local_managed", yes)
    with pytest.raises(OAuthRefusal) as refused:
        await saved.check_now("u")
    assert (refused.value.status_code, refused.value.error["code"]) == (400, "policy_blocked")
    assert (holds.taken, saved_token.checks) == ([], [])


@pytest.mark.asyncio
async def test_only_a_token_that_works_is_checked(saved_token, holds, monkeypatch):
    async def no(*_):
        return False

    monkeypatch.setattr(saved, "usable", no)
    with pytest.raises(OAuthRefusal) as refused:
        await saved.check_now("u")
    assert (refused.value.status_code, refused.value.error["code"], holds.taken) == (409, "not_checkable", [])


@pytest.fixture
def computer(monkeypatch, holds):
    """An own computer, awake unless ``awake`` is False, whose last answer
    is ``last``, read with whether its change hold stood, and what its
    check finds (``found``)."""
    state = SimpleNamespace(cache=Cache(), awake=True, last=None, found=SimpleNamespace(file="works"), read_under=[],
                            answered_then=None, on_plan=None, skips=False)

    async def yes(*_):
        return True

    async def of(user_id, computer_id):
        return object()

    async def read_answer(computer_id):
        state.read_under.append(change_lock(computer_id) in state.cache.locks)
        return state.last

    async def get_computer(computer_id):
        return {"computer_id": computer_id}

    async def tunnels_for(user_id, computers):
        return {}

    async def check_then_keep(user_id, computer_id, last, held, *, answered=lambda: None, skipped=lambda: None, plan=None):
        if state.skips:
            skipped()
            return state.found
        if state.on_plan is not None:
            answered()
            state.on_plan(plan, "machine")
        if state.answered_then is not None:
            answered()
            raise state.answered_then
        if isinstance(state.found, BaseException):
            raise state.found
        return state.found

    monkeypatch.setattr(machine.MachineHome, "of", of)
    for name, value in (("rows_on", yes), ("read_answer", read_answer), ("get_computer", get_computer),
                        ("tunnels_for", tunnels_for), ("check_then_keep", check_then_keep),
                        ("_awake", lambda computer, tunnels: state.awake)):
        monkeypatch.setattr(machine, name, value)
    with serving(state.cache):
        yield state


@pytest.mark.asyncio
async def test_a_computer_holds_its_own_credential_on_the_answer_kept_under_its_hold(computer, holds):
    await machine.check_now("u", "c-1")
    assert (holds.taken, holds.given, computer.read_under) == (["computer:c-1"], [], [True])


def _answer(file: str, relays: bool = False) -> SimpleNamespace:
    return SimpleNamespace(file=file, relays=relays)


@pytest.mark.asyncio
@pytest.mark.parametrize(("case", "status", "code", "given"), [
    ("relays", 409, "not_checkable", False),
    ("asleep", 503, "computer_unreachable", False),
    ("just unreached", 503, "computer_unreachable", False),
    ("not reached", 503, "computer_unreachable", True),
    ("could not tell", 503, "check_unavailable", True),
    ("cut off", 503, "check_unavailable", True),
    ("token file gone", 409, "not_checkable", True),
    ("policy", 409, "not_checkable", True),
])
async def test_a_computer_check_that_spent_no_request_keeps_no_hold_and_says_what_was_out_of_reach(computer, holds, case, status, code, given):
    if case == "relays":
        computer.last = _answer("none", relays=True)
    elif case == "asleep":
        computer.awake = False
    elif case == "just unreached":
        computer.cache.values[unreached_key("c-1")] = True
    elif case == "cut off":
        computer.found = TimeoutError()
    else:
        computer.found = {"not reached": None, "could not tell": _answer("unknown"),
                          "token file gone": _answer("none", relays=True), "policy": _answer("policy")}[case]
    with pytest.raises(OAuthRefusal) as refused:
        await machine.check_now("u", "c-1")
    assert (refused.value.status_code, refused.value.error["code"]) == (status, code)
    assert holds.given == ([("computer:c-1", NOW)] if given else []) and len(holds.taken) == int(given)


@pytest.mark.asyncio
async def test_a_computer_check_claude_answered_keeps_its_hold_whatever_fails_after(computer, holds):
    computer.answered_then = RuntimeError("the answer could not be kept")
    with pytest.raises(RuntimeError):
        await machine.check_now("u", "c-1")
    assert (holds.taken, holds.given) == (["computer:c-1"], [])


@pytest.mark.asyncio
async def test_a_computer_whose_token_claude_refused_spent_its_check(computer, holds):
    computer.found = _answer("refused")
    await machine.check_now("u", "c-1")
    assert (holds.taken, holds.given) == (["computer:c-1"], [])


def test_a_computer_shows_the_hold_of_the_credential_its_turns_sign_in_with():
    plans = {"saved": CredentialPlan(None, 100), "computer:c-1": CredentialPlan(None, 200)}
    entry = ClaudeCodeComputer(computer_id="c-1", name="Studio Mac", kind="ssh", state="signed_in", source="saved")
    assert machine._with_plan(entry, plans).check_in == 100
    assert machine._with_plan(entry.model_copy(update={"source": "machine"}), plans).check_in == 200


@pytest.mark.asyncio
async def test_a_sign_in_code_pasted_again_answers_with_the_plan_and_the_hold_as_its_save_did(monkeypatch):
    plans = {"saved": CredentialPlan(None, 100), "computer:c-1": CredentialPlan(None, 200)}

    async def plan_views(user_id, credentials=None):
        return plans

    async def stored_row(user_id):
        return {"access_token": TOKEN, "updated_at": NOW}

    async def signed_in(*_):
        return "signed_in"

    async def answer(computer_id):
        return SimpleNamespace(file="works")

    async def from_answer(user_id, computer, answer, relayed):
        return ClaudeCodeComputer(computer_id="c-1", name="Studio Mac", kind="ssh", state="signed_in", source="machine")

    async def relayed(user_id):
        return None

    for module, name, value in ((saved, "plan_views", plan_views), (saved, "stored_row", stored_row), (saved, "token_state", signed_in),
                                (machine, "plan_views", plan_views), (machine, "read_answer", answer),
                                (machine, "_from_answer", from_answer), (machine, "_relayed", relayed)):
        monkeypatch.setattr(module, name, value)
    home = object.__new__(machine.MachineHome)
    home.user_id, home.computer_id, home.computer = "u", "c-1", {"computer_id": "c-1"}
    with serving(Cache()):
        assert ((await SavedHome("u").current()).check_in, (await home.current()).check_in) == (100, 200)


@pytest.mark.asyncio
async def test_a_tokens_check_answers_once_the_plan_it_read_is_written(monkeypatch):
    written = []

    class Plan:
        def __init__(self, *args, **kwargs):
            pass

        async def written(self):
            written.append(True)

    async def run_check(launcher):
        return True

    async def stored_row(user_id):
        return None

    async def keep_check(key, answer):
        pass

    monkeypatch.setattr(saved, "UserPlan", Plan)
    monkeypatch.setattr(saved, "run_check", run_check)
    monkeypatch.setattr(saved, "stored_row", stored_row)
    monkeypatch.setattr(saved, "keep_check", keep_check)
    heard = []
    answer = await saved.check_token("u", {"access_token": TOKEN, "updated_at": NOW}, "k", None, answered=lambda: heard.append(True))
    assert (answer, written, heard) == ("works", [True], [True]), "Check now hears the request was spent"


@pytest.mark.asyncio
async def test_a_machines_check_says_when_claude_answered(monkeypatch):
    from ptc_agent.agent.claude_code import machine_token

    class Plan:
        def __init__(self, *args, **kwargs):
            pass

        async def written(self):
            pass

    monkeypatch.setattr(machine_token, "machine_state", AsyncMock(return_value=SimpleNamespace(file="file", present=True, folder=None)))
    monkeypatch.setattr(machine, "run_check", AsyncMock(return_value=True))
    monkeypatch.setattr(machine, "check_launcher", lambda *args, **kwargs: None)
    monkeypatch.setattr(machine, "UserPlan", Plan)
    heard = []
    computer_id = "00000000-0000-4000-8000-000000000001"
    answer = await machine.check_machine(SimpleNamespace(link=AsyncMock()), "u", computer_id, None, answered=lambda: heard.append(True))
    assert (answer.file, heard) == ("works", [True])


@pytest.mark.asyncio
@pytest.mark.parametrize("on", ["saved", "computer"])
async def test_a_check_that_would_be_billed_sends_nothing_and_takes_no_hold(request, monkeypatch, holds, on):
    found = request.getfixturevalue("saved_token" if on == "saved" else "computer")
    asked = []

    async def would_bill(user_id, credential):
        asked.append(credential)
        return True

    monkeypatch.setattr(saved if on == "saved" else machine, "check_would_bill", would_bill)
    with pytest.raises(OAuthRefusal) as refused:
        await (saved.check_now("u") if on == "saved" else machine.check_now("u", "c-1"))
    assert (refused.value.status_code, refused.value.error["code"]) == (409, "limit_held")
    assert asked == [{"saved": "saved", "computer": "computer:c-1"}[on]]
    assert holds.taken == [] and getattr(found, "checks", []) == [], "no request on the plan, and its hold stays free"


@pytest.mark.asyncio
@pytest.mark.parametrize(("credits", "out", "bills"), [
    ("on", "stop", True), (None, "stop", True), ("off", "stop", False), ("on", "credits", False), ("on", None, True),
])
async def test_settings_offers_no_check_that_would_be_billed(monkeypatch, credits, out, bills):
    seen = datetime.now(UTC)
    state = {"limits": {"five_hour": {"until": (seen + timedelta(hours=1)).isoformat(), "seen_at": seen.isoformat()}},
             **({"credits": {"value": credits, "seen_at": seen.isoformat()}} if credits else {})}

    async def get_plans(user_id, credentials=None):
        return {"saved": _row(state, None), "computer:c-1": _row({}, None)}

    async def plan_out(user_id):
        if out is None:
            raise OSError("preferences unread")
        return out

    monkeypatch.setattr(store, "get_plans", get_plans)
    monkeypatch.setattr(plan_module, "plan_out", plan_out)
    monkeypatch.setattr(checks, "token_check_model", lambda: "claude-haiku-5-5")
    views = await plan_module.plan_views("u")
    assert (views["saved"].check_bills, views["computer:c-1"].check_bills) == (bills, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("on", ["saved", "computer"])
async def test_a_check_that_found_it_would_be_billed_once_held_gives_the_hold_back(request, holds, on):
    request.getfixturevalue("saved_token" if on == "saved" else "computer").skips = True
    with pytest.raises(OAuthRefusal) as refused:
        await (saved.check_now("u") if on == "saved" else machine.check_now("u", "c-1"))
    assert (refused.value.status_code, refused.value.error["code"]) == (409, "limit_held")
    assert holds.given == [({"saved": "saved", "computer": "computer:c-1"}[on], NOW)], "a check that sent nothing frees its hold"


@pytest.mark.asyncio
async def test_a_tokens_and_a_machines_check_say_when_they_skip_one_that_would_be_billed(monkeypatch):
    from ptc_agent.agent.claude_code import machine_token

    async def yes(*_):
        return True

    monkeypatch.setattr(saved, "check_would_bill", yes)
    monkeypatch.setattr(machine, "check_would_bill", yes)
    monkeypatch.setattr(saved, "keep_check", AsyncMock())
    monkeypatch.setattr(machine_token, "machine_state", AsyncMock(return_value=SimpleNamespace(file="file", present=True, folder=None)))
    heard = []
    await saved.check_token("u", {"access_token": TOKEN, "updated_at": NOW}, "k", None, skipped=lambda: heard.append("saved"))
    await machine.check_machine(SimpleNamespace(link=AsyncMock()), "u", "c-1", None, skipped=lambda: heard.append("computer"))
    assert heard == ["saved", "computer"]


def _report(plan, signed_in: str) -> None:
    """One report on the plan, as a response's ``rate_limit_event`` makes it."""
    plan._fences = {store.plan_credential(signed_in, plan.computer_id): 1}
    plan.record(signed_in, PlanReading.of({"status": "allowed"}, NOW, "claude-haiku-5-5"))


@pytest.fixture
def writes(monkeypatch):
    """The plan's writes, each landing after ``takes`` seconds, past what a
    stopped turn waits, unless it fails or is dropped as a replaced token's
    (``kept`` False); the credentials whose write landed."""
    state = SimpleNamespace(takes=0.05, fails=False, kept=True, landed=[])

    async def update_plan(user_id, credential, generation, seen_at, change):
        await asyncio.sleep(state.takes)
        if state.fails:
            raise OSError("no connection to spare")
        state.landed.append(credential)
        return state.kept

    monkeypatch.setattr(store, "update_plan", update_plan)
    monkeypatch.setattr(plan_module, "WRITTEN_WAIT_S", 0.01)
    return state


async def _check(request, on: str) -> None:
    """Check now on the saved token or on a computer's own, its check reporting once on the plan."""
    request.getfixturevalue("saved_token" if on == "saved" else "computer").on_plan = _report
    await (saved.check_now("u") if on == "saved" else machine.check_now("u", "c-1"))


@pytest.mark.asyncio
@pytest.mark.parametrize("on", ["saved", "computer"])
async def test_a_check_answers_once_the_plan_it_read_is_written_past_what_a_stopped_turn_waits(request, holds, writes, on):
    await _check(request, on)
    assert writes.landed == [{"saved": "saved", "computer": "computer:c-1"}[on]], "the read after it shows what it reported"


@pytest.mark.asyncio
@pytest.mark.parametrize("on", ["saved", "computer"])
@pytest.mark.parametrize("case", ["slow", "failed", "dropped"])
async def test_a_check_whose_plan_was_not_written_says_so_and_keeps_its_hold(request, monkeypatch, holds, writes, on, case):
    monkeypatch.setattr(saved if on == "saved" else machine, "PLAN_WRITTEN_S", 0.02)
    writes.takes, writes.fails, writes.kept = (0.2 if case == "slow" else 0), case == "failed", case != "dropped"
    with pytest.raises(OAuthRefusal) as refused:
        await _check(request, on)
    assert (refused.value.status_code, refused.value.error["code"]) == (503, "plan_unsaved")
    assert holds.given == [], "Claude answered, so the request was spent"
    if plan_module._writes:
        await asyncio.wait(set(plan_module._writes))


@pytest.mark.asyncio
async def test_the_route_checks_through_the_cli_once_and_answers_held_with_how_long(monkeypatch, holds):
    async def yes(*_):
        return True

    async def stored_row(user_id):
        return {"access_token": TOKEN, "updated_at": NOW}

    async def no(*_):
        return False

    for name, value in (("rows_on", yes), ("saved_token_runs", yes), ("usable", yes), ("stored_row", stored_row),
                        ("local_managed", no)):
        monkeypatch.setattr(saved, name, value)
    app = create_test_app(route.router)
    app.add_exception_handler(OAuthRefusal, lambda _, exc: JSONResponse({"detail": exc.detail, "error": exc.error}, exc.status_code))
    works = AsyncMock(return_value=True)
    with serving(Cache()), patch("ptc_agent.agent.claude_code.runtime.token_works", works):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            ran = await client.post("/api/v1/claude-code/check")
            holds.held = True
            held = await client.post("/api/v1/claude-code/check")
    assert (ran.status_code, ran.content) == (204, b"")
    assert held.status_code == 429 and held.json()["error"]["check_in"] == 120
    assert works.await_count == 1, "one request on the plan, through the token's own check"
