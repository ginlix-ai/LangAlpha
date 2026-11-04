"""What a turn's body may ask of a token, the origin the server states for it,
and the helpers routes call for what a path or query does not name
(``check_turn_body``, ``stamp_origin``, ``assert_reaches``,
``assert_automation_target``, ``pin_computer``)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from src.server.database import order_attempts
from src.server.database import principal_reach as reach
from src.server.database.home_workspace import get_home_workspace_id
from src.server.dependencies.principal import (
    _CLI_CALL,
    CliCall,
    Principal,
    assert_automation_target,
    assert_reaches,
    check_turn_body,
    pin_computer,
    stamp_origin,
)
from src.server.models.chat import ChatRequest, HITLDecision, HITLResponse, ThreadOrigin
from tests.unit.server.dependencies.reach_world import (
    COMPUTER,
    OTHER_COMPUTER,
    OTHER_THREAD,
    OTHER_WORKSPACE,
    THREAD,
    UNKNOWN,
    USER,
    WORKSPACE,
    ReachWorld,
)

CALL_ID = "call-0123456789"
HOME = get_home_workspace_id(USER)

SESSION = Principal(USER, "session")
SERVICE = Principal(USER, "service")
PAT = Principal(USER, "pat", scopes=("agent:run",))
APP = Principal(USER, "oauth", client_id="https://app.example.test/client.json", scopes=("agent:run",))
COMPUTER_TOKEN = Principal(USER, "computer", scopes=("agent:run",), computer_id=COMPUTER)

NOT_A_COMPUTER = [PAT, APP]


@pytest.fixture
def world(monkeypatch) -> ReachWorld:
    world = ReachWorld(monkeypatch)
    # The threads with an order waiting on the user's verdict.
    world.awaiting_orders = set()

    async def awaits(thread_id):
        return thread_id in world.awaiting_orders

    monkeypatch.setattr(order_attempts, "thread_awaits_order", awaits)
    return world


def _orders() -> dict[str, HITLResponse]:
    return {
        "interrupt-1": HITLResponse(order_decisions={"attempt-1": HITLDecision(type="approve")})
    }


def _approval() -> dict[str, HITLResponse]:
    return {"interrupt-1": HITLResponse(decisions=[HITLDecision(type="approve")])}


def _body(**fields) -> ChatRequest:
    return ChatRequest(**fields)


def _request(principal: Principal = COMPUTER_TOKEN, call_thread: str | None = None):
    """A request as ``get_principal`` leaves it: a computer's carries its call,
    which ran in ``call_thread``."""
    state = {}
    if principal.computer_id is not None:
        state[_CLI_CALL] = CliCall(principal, CALL_ID if call_thread else None, call_thread)
    return SimpleNamespace(scope={"state": state})


async def _check(principal, body, thread_id=THREAD, request=None, **kw) -> None:
    await check_turn_body(principal, request or _request(principal), body, thread_id, **kw)


async def _refused(principal, body, thread_id=THREAD, request=None, **kw) -> HTTPException:
    with pytest.raises(HTTPException) as caught:
        await _check(principal, body, thread_id, request, **kw)
    assert caught.value.status_code == 403
    return caught.value


def _never_home(refusal: HTTPException) -> None:
    assert refusal.detail["message"] == "A computer's token never reaches Home"


# -- orders and approvals --------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", [SESSION, SERVICE])
async def test_a_session_and_the_servers_own_call_may_answer_an_order(world, principal):
    await _check(principal, _body(hitl_response=_orders()))

    assert world.lookups == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", [PAT, APP, COMPUTER_TOKEN])
async def test_no_token_answers_an_order(world, principal):
    refusal = await _refused(principal, _body(hitl_response=_orders()))

    assert refusal.detail["type"] == "order_decision_forbidden"


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", NOT_A_COMPUTER)
async def test_a_token_that_is_not_a_computers_may_answer_a_plain_approval(world, principal):
    await _check(principal, _body(hitl_response=_approval()))


@pytest.mark.asyncio
async def test_a_computer_token_answers_no_approval_at_all(world):
    refusal = await _refused(COMPUTER_TOKEN, _body(hitl_response=_approval()))

    assert refusal.detail["type"] == "forbidden_for_token"
    assert world.lookups == 0


@pytest.mark.asyncio
async def test_an_order_mixed_into_an_otherwise_plain_answer_is_still_an_order(world):
    mixed = {**_approval(), **{"interrupt-2": _orders()["interrupt-1"]}}

    refusal = await _refused(PAT, _body(hitl_response=mixed))

    assert refusal.detail["type"] == "order_decision_forbidden"


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", NOT_A_COMPUTER)
async def test_no_token_answers_a_thread_whose_order_waits(world, principal):
    """The order gate refuses an attempt given no keyed verdict, so a plain
    answer, or an empty order map, would decide the order all the same."""
    world.awaiting_orders.add(THREAD)

    for answer in (_approval(), {"interrupt-1": HITLResponse(order_decisions={})}):
        refusal = await _refused(principal, _body(hitl_response=answer))
        assert refusal.detail["type"] == "order_decision_forbidden"
    await _check(SESSION, _body(hitl_response=_approval()))


@pytest.mark.asyncio
async def test_an_empty_order_map_is_not_an_order(world):
    answer = {"interrupt-1": HITLResponse(order_decisions={})}

    await _check(PAT, _body(hitl_response=answer))


# -- reach: the thread and workspace the body names ------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", NOT_A_COMPUTER)
async def test_reach_does_not_apply_to_a_token_that_is_not_a_computers(world, principal):
    await _check(principal, _body(workspace_id=OTHER_WORKSPACE), OTHER_THREAD)

    assert world.lookups == 0


@pytest.mark.asyncio
async def test_a_computer_token_takes_a_turn_in_a_thread_of_its_computer(world):
    await _check(COMPUTER_TOKEN, _body())


@pytest.mark.asyncio
async def test_a_computer_token_cannot_take_a_turn_in_a_thread_of_another_computer(world):
    await _refused(COMPUTER_TOKEN, _body(), thread_id=OTHER_THREAD)


@pytest.mark.asyncio
async def test_a_follow_up_names_no_workspace_and_asks_no_one_for_home(world):
    await _check(COMPUTER_TOKEN, _body())

    assert world.asked("workspace") == []


@pytest.mark.asyncio
async def test_a_new_thread_in_a_workspace_of_its_computer_is_allowed(world):
    await _check(COMPUTER_TOKEN, _body(workspace_id=WORKSPACE), UNKNOWN)

    assert world.asked("workspace") == [(USER, WORKSPACE)]


@pytest.mark.asyncio
async def test_a_new_thread_in_a_workspace_of_another_computer_is_refused(world):
    await _refused(COMPUTER_TOKEN, _body(workspace_id=OTHER_WORKSPACE), thread_id=UNKNOWN)


@pytest.mark.asyncio
async def test_a_workspace_named_beside_a_thread_of_its_own_computer_is_held_to_it(world):
    await _refused(COMPUTER_TOKEN, _body(workspace_id=OTHER_WORKSPACE), thread_id=THREAD)


@pytest.mark.asyncio
async def test_a_thread_minted_for_the_turn_is_not_looked_up(world):
    await _check(COMPUTER_TOKEN, _body(workspace_id=WORKSPACE), None)

    assert world.asked("thread") == []
    assert world.asked("workspace") == [(USER, WORKSPACE)]


# -- never Home ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_new_thread_naming_no_workspace_is_refused_as_home(world):
    """It would run in Home, the Chief of Staff's, which reaches every computer."""
    _never_home(await _refused(COMPUTER_TOKEN, _body(), thread_id=None))


@pytest.mark.asyncio
async def test_home_is_refused_though_it_sits_on_the_tokens_computer(world):
    world.computers[HOME] = COMPUTER

    _never_home(await _refused(COMPUTER_TOKEN, _body(workspace_id=HOME), thread_id=UNKNOWN))


@pytest.mark.asyncio
async def test_home_is_refused_before_its_row_exists_without_a_lookup(world):
    """Its id is known without the row, which the turn would create; a missing
    row must not pass as one the user lacks."""
    _never_home(await _refused(COMPUTER_TOKEN, _body(workspace_id=HOME.upper()), None))

    assert world.lookups == 0


@pytest.mark.asyncio
async def test_a_turn_in_a_thread_in_home_is_refused(world):
    world.computers[THREAD] = reach.HOME

    _never_home(await _refused(COMPUTER_TOKEN, _body()))


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", NOT_A_COMPUTER)
async def test_a_token_that_is_not_a_computers_may_start_a_turn_in_home(world, principal):
    await _check(principal, _body(workspace_id=HOME), None)


# -- history ---------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields",
    [
        {"checkpoint_id": "checkpoint-1", "fork_from_turn": 2},
        {"checkpoint_id": "checkpoint-1"},
        {"fork_from_turn": 0},
    ],
)
async def test_a_computer_token_cannot_edit_or_regenerate_a_turn(world, fields):
    """A fork deletes the turns after it, and a checkpoint replay rewinds the
    thread past them."""
    refusal = await _refused(COMPUTER_TOKEN, _body(**fields))

    assert "edit or regenerate" in refusal.detail["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", NOT_A_COMPUTER)
async def test_a_token_that_is_not_a_computers_may_fork(world, principal):
    await _check(principal, _body(checkpoint_id="checkpoint-1", fork_from_turn=2))


@pytest.mark.asyncio
async def test_a_workspace_the_user_does_not_have_passes_to_the_routes_own_404(world):
    await _check(COMPUTER_TOKEN, _body(workspace_id=UNKNOWN), UNKNOWN)


@pytest.mark.asyncio
async def test_a_thread_id_that_is_not_a_uuid_is_left_to_the_route(world):
    await _check(COMPUTER_TOKEN, _body(workspace_id=WORKSPACE), "not-a-uuid")

    assert world.asked("thread") == []


@pytest.mark.asyncio
async def test_what_the_request_already_read_is_not_asked_again(world):
    """The path's check reads the thread first; the body's reads it from the
    request rather than the database."""
    request = _request()

    await _check(COMPUTER_TOKEN, _body(), request=request, from_path=True)
    await _check(COMPUTER_TOKEN, _body(), request=request, from_path=True)

    assert world.asked("thread") == [(USER, THREAD)]


# -- not into the thread the call came from --------------------------------------


@pytest.mark.asyncio
async def test_a_computer_token_cannot_send_a_turn_into_the_thread_its_call_runs_in(world):
    refusal = await _refused(COMPUTER_TOKEN, _body(), request=_request(call_thread=THREAD))

    assert "conversation it runs in" in refusal.detail["message"]


@pytest.mark.asyncio
async def test_a_computer_token_may_send_a_turn_into_a_sibling_thread(world):
    world.computers[OTHER_THREAD] = COMPUTER

    await _check(COMPUTER_TOKEN, _body(), OTHER_THREAD, request=_request(call_thread=THREAD))


@pytest.mark.asyncio
async def test_a_turn_from_no_thread_is_not_held_to_one(world):
    await _check(COMPUTER_TOKEN, _body(), request=_request(call_thread=None))


@pytest.mark.asyncio
async def test_a_thread_the_path_named_is_left_to_the_paths_guard(world):
    """``require_scopes`` already refused it there."""
    await _check(
        COMPUTER_TOKEN, _body(), request=_request(call_thread=THREAD), from_path=True
    )


# -- who a turn comes from -------------------------------------------------------


CLAIMED = ThreadOrigin(type="automation", id="automation-1", name="Morning brief")


def test_a_computer_turn_names_the_thread_its_command_ran_in():
    body = stamp_origin(COMPUTER_TOKEN, _request(call_thread=THREAD), _body(origin=CLAIMED))

    assert body.origin.model_dump(exclude_none=True) == {
        "type": "agent",
        "agent": "cli",
        "id": THREAD,
    }


def test_a_computer_turn_from_no_thread_is_the_users_and_drops_what_it_claimed():
    """A person or a schedule on the machine is the user: neither another
    thread's agent nor whatever origin the body named."""
    body = stamp_origin(COMPUTER_TOKEN, _request(call_thread=None), _body(origin=CLAIMED))

    assert body.origin is None


@pytest.mark.parametrize("principal", [SESSION, SERVICE])
def test_a_session_and_the_servers_own_call_keep_the_origin_their_body_names(principal):
    body = stamp_origin(principal, _request(principal), _body(origin=CLAIMED))

    assert body.origin == CLAIMED


@pytest.mark.parametrize("principal", NOT_A_COMPUTER)
def test_an_outside_app_or_access_token_cannot_claim_an_origin(principal):
    """Else it could label its message another agent's, linking any thread."""
    claimed = ThreadOrigin(type="agent", agent="chief_of_staff", id=OTHER_THREAD)

    body = stamp_origin(principal, _request(principal), _body(origin=claimed))

    assert body.origin is None


# -- assert_reaches --------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", [SESSION, SERVICE, PAT, APP])
async def test_assert_reaches_checks_nothing_for_a_caller_that_is_not_a_computer(
    world, principal
):
    await assert_reaches(principal, workspace_id=OTHER_WORKSPACE, thread_id=OTHER_THREAD)

    assert world.lookups == 0


@pytest.mark.asyncio
async def test_assert_reaches_with_nothing_named_asks_nothing(world):
    await assert_reaches(COMPUTER_TOKEN)

    assert world.lookups == 0


@pytest.mark.asyncio
async def test_assert_reaches_allows_what_the_computer_holds(world):
    await assert_reaches(COMPUTER_TOKEN, workspace_id=WORKSPACE, thread_id=THREAD)


@pytest.mark.asyncio
async def test_assert_reaches_refuses_a_workspace_of_another_computer(world):
    with pytest.raises(HTTPException) as caught:
        await assert_reaches(COMPUTER_TOKEN, workspace_id=OTHER_WORKSPACE)

    assert caught.value.status_code == 403


@pytest.mark.asyncio
async def test_assert_reaches_refuses_a_thread_of_another_computer(world):
    with pytest.raises(HTTPException) as caught:
        await assert_reaches(COMPUTER_TOKEN, thread_id=OTHER_THREAD)

    assert caught.value.status_code == 403


@pytest.mark.asyncio
async def test_assert_reaches_refuses_home_by_its_id(world):
    """What ``POST /workspaces/home`` and an automation naming no workspace ask."""
    world.computers[HOME] = COMPUTER

    with pytest.raises(HTTPException) as caught:
        await assert_reaches(COMPUTER_TOKEN, workspace_id=HOME)

    _never_home(caught.value)
    assert world.lookups == 0


# -- assert_automation_target ----------------------------------------------------


@pytest.mark.asyncio
async def test_an_automation_cannot_send_into_the_thread_the_call_runs_in(world):
    with pytest.raises(HTTPException) as caught:
        await assert_automation_target(
            COMPUTER_TOKEN,
            _request(call_thread=THREAD),
            workspace_id=WORKSPACE,
            thread_id=THREAD,
        )

    assert caught.value.status_code == 403
    assert "conversation it runs in" in caught.value.detail["message"]


@pytest.mark.asyncio
async def test_an_automation_named_by_a_call_whose_filing_is_gone_cannot_pin_a_thread(world):
    """The create route lets a command touch its own thread, so the loop
    guard alone holds here, and a call it cannot place might be that one."""
    request = _request()
    request.scope["state"][_CLI_CALL] = CliCall(COMPUTER_TOKEN, CALL_ID, None)

    with pytest.raises(HTTPException) as caught:
        await assert_automation_target(
            COMPUTER_TOKEN, request, workspace_id=WORKSPACE, thread_id=THREAD
        )

    assert caught.value.status_code == 403
    assert "Cannot tell which conversation" in caught.value.detail["message"]


@pytest.mark.asyncio
async def test_an_automation_may_send_into_a_sibling_thread(world):
    world.computers[OTHER_THREAD] = COMPUTER

    await assert_automation_target(
        COMPUTER_TOKEN,
        _request(call_thread=THREAD),
        workspace_id=WORKSPACE,
        thread_id=OTHER_THREAD,
    )


@pytest.mark.asyncio
async def test_an_automation_placed_in_home_is_refused(world):
    with pytest.raises(HTTPException) as caught:
        await assert_automation_target(
            COMPUTER_TOKEN, _request(), workspace_id=HOME, thread_id=None
        )

    _never_home(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", [SESSION, PAT])
async def test_any_other_caller_may_point_an_automation_at_its_thread(world, principal):
    await assert_automation_target(
        principal, _request(principal), workspace_id=HOME, thread_id=THREAD
    )


# -- pin_computer ----------------------------------------------------------------


@pytest.mark.parametrize("principal", [SESSION, SERVICE, PAT, APP])
def test_any_other_caller_puts_a_workspace_where_it_names(principal):
    assert pin_computer(principal, OTHER_COMPUTER) == OTHER_COMPUTER
    assert pin_computer(principal, None) is None


def test_a_computer_token_puts_a_new_workspace_on_its_own_computer():
    assert pin_computer(COMPUTER_TOKEN, None) == COMPUTER
    assert pin_computer(COMPUTER_TOKEN, COMPUTER) == COMPUTER


def test_a_computer_token_naming_another_computer_finds_none():
    with pytest.raises(HTTPException) as caught:
        pin_computer(COMPUTER_TOKEN, OTHER_COMPUTER)

    assert caught.value.status_code == 404
