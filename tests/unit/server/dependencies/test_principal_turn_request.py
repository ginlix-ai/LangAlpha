"""What a turn's body may ask of a token, and the origin the server states for
it (``check_turn_body``, ``stamp_origin``). Where a computer's turn runs, and
where an automation sends, is the approval's."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from src.server.database import order_attempts
from src.server.database.home_workspace import get_home_workspace_id
from src.server.dependencies.principal import (
    _CLI_CALL,
    CliCall,
    Principal,
    check_turn_body,
    stamp_origin,
)
from src.server.models.chat import ChatRequest, HITLDecision, HITLResponse, ThreadOrigin
from tests.unit.server.dependencies.reach_world import (
    COMPUTER,
    OTHER_THREAD,
    THREAD,
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


# -- orders and approvals --------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", [SESSION, SERVICE])
async def test_a_session_and_the_servers_own_call_may_answer_an_order(world, principal):
    await _check(principal, _body(hitl_response=_orders()))


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


# -- not into the thread the call came from --------------------------------------


@pytest.mark.asyncio
async def test_a_computer_token_cannot_send_a_turn_into_the_thread_its_call_runs_in(world):
    refusal = await _refused(COMPUTER_TOKEN, _body(), request=_request(call_thread=THREAD))

    assert "conversation it runs in" in refusal.detail["message"]


@pytest.mark.asyncio
async def test_a_computer_token_may_send_a_turn_into_a_sibling_thread(world):
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


@pytest.mark.asyncio
async def test_a_turn_into_the_thread_that_sent_the_calls_turn_is_refused(world):
    """An analyst a hand-off from Home started, sending into that Home
    thread, would start its next turn, which would hand off again."""
    world.senders[THREAD] = OTHER_THREAD

    refusal = await _refused(
        COMPUTER_TOKEN, _body(), OTHER_THREAD, request=_request(call_thread=THREAD)
    )

    assert "sent it its current turn" in refusal.detail["message"]


@pytest.mark.asyncio
async def test_a_turn_into_a_thread_that_sent_nothing_passes(world):
    world.senders[THREAD] = WORKSPACE  # some other thread

    await _check(COMPUTER_TOKEN, _body(), OTHER_THREAD, request=_request(call_thread=THREAD))


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
