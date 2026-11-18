"""The user's OK before a computer's token acts (``dependencies.cli_approval``).

Two layers. The gate's flow (``gate``) runs against a scripted store: each
``ask`` answers what the test queued, as the store's one script would, so
the policy is pinned apart from Redis, whose scripts run for real in
``tests/integration/test_cli_approvals_redis.py``. Each surface's builder
runs against fixed rows, with ``gate`` replaced by one that only builds, so
what a surface asks, and what it refuses or leaves to its route, is read
directly.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

from src.server.database.turn_targets import Place
from src.server.database.workspace_names import WorkspaceNameTaken
from src.server.dependencies import cli_approval
from src.server.dependencies.principal import (
    _CLI_CALL,
    SHARED_WORKSPACE_REFUSAL,
    CliCall,
    Principal,
)
from src.server.models.automation import AutomationCreate, AutomationUpdate
from src.server.models.chat import ChatRequest
from src.server.models.computer import ComputerRemoveRequest
from src.server.models.workflow import RetryRequest
from src.server.models.workspace import WorkspaceCreate
from src.server.services.automations.lifecycle import AutomationRefusal
from src.server.services import cli_approvals as store
from src.server.services.cli_tokens import SCOPES
from tests.unit.server.dependencies.test_principal_shared_reach import (
    FOREIGN_THREAD,
    FOREIGN_WORKSPACE,
    MY_PRIVATE_THREAD,
    PRIVATE_THREAD,
    SHARED_THREAD,
    SHARED_WORKSPACE,
    UNREACHED_THREAD,
    UNREACHED_WORKSPACE,
    SharedWorld,
)

USER = "user-gate-1"
COMPUTER = "c0ffee00-0000-4000-8000-000000000001"
OTHER_COMPUTER = "c0ffee00-0000-4000-8000-000000000002"
WORKSPACE = "a1100000-0000-4000-8000-000000000001"
OTHER_WORKSPACE = "a1100000-0000-4000-8000-000000000002"
THREAD = "b2200000-0000-4000-8000-000000000001"
OTHER_THREAD = "b2200000-0000-4000-8000-000000000002"
UNKNOWN = "e5500000-0000-4000-8000-000000000009"
AUTOMATION = "d4400000-0000-4000-8000-000000000001"
APPROVAL = "f6600000-0000-4000-8000-000000000001"
OTHER_APPROVAL = "f6600000-0000-4000-8000-000000000002"
CALL_ID = "call-0123456789"

TOKEN = Principal(USER, "computer", scopes=SCOPES, computer_id=COMPUTER)
SESSION = Principal(USER, "session")
PAT = Principal(USER, "pat", scopes=("agent:run", "workspace:write"))


def _request(
    principal: Principal = TOKEN,
    *,
    call_thread: str | None = THREAD,
    call_id: str | None = CALL_ID,
    header: str | None = None,
    gone: bool = False,
):
    """A request as ``get_principal`` leaves it: a computer's carries its
    call, filed as run in ``call_thread``."""
    state = {}
    if principal.computer_id is not None:
        state[_CLI_CALL] = CliCall(principal, call_id, call_thread)
    headers = {cli_approval.APPROVAL_HEADER: header} if header else {}

    async def is_disconnected() -> bool:
        return gone

    return SimpleNamespace(
        scope={"state": state}, headers=headers, is_disconnected=is_disconnected
    )


def _approval(approval_id: str = APPROVAL, status: str = store.PENDING) -> store.Approval:
    now = datetime(2026, 11, 14, 12, tzinfo=UTC)
    return store.Approval(
        approval_id=approval_id,
        action="thread_delete",
        status=status,
        computer_id=COMPUTER,
        digest="d",
        summary={"target": {}, "details": {}, "requester": {}},
        created_at=now,
        expires_at=now + timedelta(minutes=30),
    )


# -- the gate's flow -------------------------------------------------------------


@dataclass
class ScriptedStore:
    """The store's API, answering what each test queues, in order."""

    answers: list[Any] = field(default_factory=list)
    waits: list[store.Approval | None] = field(default_factory=list)
    asked: list[dict[str, Any]] = field(default_factory=list)
    announced: list[tuple[str, str]] = field(default_factory=list)

    async def ask(self, user_id, computer_id, digest, **kw):
        self.asked.append({"computer_id": computer_id, "digest": digest, **kw})
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def wait_for_answer(self, user_id, approval_id, deadline):
        return self.waits.pop(0)

    async def announce(self, user_id, approval_id, status):
        self.announced.append((approval_id, status))


@pytest.fixture
def scripted(monkeypatch) -> ScriptedStore:
    fake = ScriptedStore()
    for name in ("ask", "wait_for_answer", "announce"):
        monkeypatch.setattr(store, name, getattr(fake, name))
    return fake


@pytest.fixture
def requester(monkeypatch) -> list[str]:
    """Each time the gate reads who asked, which it does only to write a
    new request."""
    reads: list[str] = []

    async def read(principal, call):
        reads.append(principal.computer_id)
        return {"kind": "command"}

    monkeypatch.setattr(cli_approval, "_requester", read)
    return reads


ASK = cli_approval._ask("thread_delete", path={"thread_id": THREAD}, target={}, details={})


async def _gate(request=None, principal: Principal = TOKEN, ask=ASK) -> None:
    async def build(call):
        return ask

    await cli_approval.gate(request or _request(principal), principal, build)


async def _refused(request=None, **kw) -> HTTPException:
    with pytest.raises(HTTPException) as caught:
        await _gate(request, **kw)
    return caught.value


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", [SESSION, PAT])
async def test_only_a_computers_token_is_held(scripted, principal):
    built = []

    async def build(call):
        built.append(call)

    await cli_approval.gate(_request(principal), principal, build)

    assert built == [] and scripted.asked == []


@pytest.mark.asyncio
async def test_a_builder_answering_none_asks_nothing(scripted):
    async def build(call):
        return None

    await cli_approval.gate(_request(), TOKEN, build)

    assert scripted.asked == []


@pytest.mark.asyncio
async def test_an_approved_request_is_taken_and_runs_without_reading_who_asked(
    scripted, requester
):
    scripted.answers = [store.Found(store.TAKEN, APPROVAL)]

    await _gate()

    assert scripted.asked[0]["summary"] is None
    assert scripted.announced == [(APPROVAL, store.USED)]
    assert requester == []


@pytest.mark.asyncio
async def test_a_first_send_writes_a_request_and_is_refused_as_pending_with_its_id(
    scripted, requester
):
    scripted.answers = [store.Found(store.ABSENT), store.Found(store.CREATED, APPROVAL)]
    scripted.waits = [_approval()]

    refusal = await _refused()

    assert refusal.status_code == 403
    assert refusal.detail == {
        "type": "approval_pending",
        "message": "Waiting for the user to approve this in the LangAlpha app",
        "approval_id": APPROVAL,
        "expires_at": _approval().expires_at.isoformat(),
    }
    assert [a["summary"] is None for a in scripted.asked] == [True, False]
    assert scripted.asked[1]["summary"]["requester"] == {"kind": "command"}
    assert scripted.announced == [(APPROVAL, store.PENDING)]
    assert requester == [COMPUTER]


@pytest.mark.asyncio
async def test_the_same_request_from_another_thread_is_another_request(scripted, requester):
    # The dialog names the thread that asked; another thread's identical
    # request must not take that approval.
    scripted.answers = [store.Found(store.TAKEN, APPROVAL), store.Found(store.TAKEN, APPROVAL)]

    await _gate(_request(TOKEN))
    await _gate(_request(TOKEN, call_thread=OTHER_THREAD))

    assert scripted.asked[0]["digest"] != scripted.asked[1]["digest"]


@pytest.mark.asyncio
async def test_a_command_that_quit_takes_no_approval(scripted, requester):
    refusal = await _refused(_request(TOKEN, gone=True))

    assert refusal.detail["type"] == "approval_expired"
    assert scripted.asked == []


@pytest.mark.asyncio
async def test_an_approval_given_while_held_is_taken_in_the_same_request(scripted, requester):
    scripted.answers = [
        store.Found(store.ABSENT),
        store.Found(store.CREATED, APPROVAL),
        store.Found(store.TAKEN, APPROVAL),
    ]
    scripted.waits = [_approval(status=store.APPROVED)]

    await _gate()

    assert scripted.announced == [(APPROVAL, store.PENDING), (APPROVAL, store.USED)]


@pytest.mark.asyncio
async def test_an_approval_for_a_place_that_changed_while_held_runs_nothing(
    scripted, requester
):
    # The user approved the computer they were shown; the workspace moved
    # before the request came back to run.
    shown = cli_approval._ask("thread_send", target={}, details={}, computer_id=OTHER_COMPUTER)
    moved = cli_approval._ask("thread_send", target={}, details={}, computer_id=COMPUTER)
    builds = [shown, moved]
    scripted.answers = [store.Found(store.TAKEN, APPROVAL)]

    async def build(call):
        return builds.pop(0)

    with pytest.raises(HTTPException) as caught:
        await cli_approval.gate(_request(TOKEN), TOKEN, build)

    assert caught.value.detail["type"] == "approval_expired"
    assert scripted.announced == [(APPROVAL, store.USED)]


@pytest.mark.asyncio
async def test_a_no_given_while_held_is_refused(scripted, requester):
    scripted.answers = [store.Found(store.PENDING, APPROVAL)]
    scripted.waits = [_approval(status=store.REJECTED)]

    refusal = await _refused()

    assert refusal.detail["type"] == "approval_rejected"


@pytest.mark.asyncio
async def test_a_send_naming_a_declined_request_is_told_so(scripted, requester):
    scripted.answers = [store.Found(store.REJECTED, APPROVAL)]

    refusal = await _refused(_request(header=APPROVAL))

    assert refusal.status_code == 403
    assert refusal.detail == {
        "type": "approval_rejected",
        "message": "The user declined this in the app; nothing was done",
    }
    assert scripted.asked[0]["named"] == APPROVAL


@pytest.mark.asyncio
async def test_a_send_naming_a_request_no_longer_waiting_is_expired_and_asks_nothing(
    scripted, requester
):
    """Lapsed, or taken by a copy of the command: the CLI has stopped
    waiting on it, so a new request would ask the user about a command no
    one is running."""
    scripted.answers = [store.Found(store.ABSENT)]

    refusal = await _refused(_request(header=APPROVAL))

    assert refusal.status_code == 403
    assert refusal.detail == {
        "type": "approval_expired",
        "message": (
            "This request is no longer waiting in the app; nothing was done. "
            "Run the command again to ask again."
        ),
    }
    assert len(scripted.asked) == 1 and scripted.asked[0]["summary"] is None
    assert scripted.announced == [] and requester == []


@pytest.mark.asyncio
async def test_a_send_naming_one_request_holds_on_the_one_still_waiting(scripted, requester):
    """A copy of the command asked anew after the named one lapsed; the CLI
    adopts whatever id comes back."""
    scripted.answers = [store.Found(store.PENDING, OTHER_APPROVAL)]
    scripted.waits = [_approval(OTHER_APPROVAL)]

    refusal = await _refused(_request(header=APPROVAL))

    assert refusal.detail["approval_id"] == OTHER_APPROVAL
    assert scripted.announced == []


@pytest.mark.asyncio
async def test_a_header_that_is_not_an_id_names_nothing(scripted, requester):
    scripted.answers = [store.Found(store.TAKEN, APPROVAL)]

    await _gate(_request(header="not-an-id"))

    assert scripted.asked[0]["named"] is None


@pytest.mark.asyncio
async def test_a_request_moved_by_another_worker_is_asked_again(scripted, requester):
    scripted.answers = [store.Found(store.MOVED), store.Found(store.TAKEN, APPROVAL)]

    await _gate()

    assert len(scripted.asked) == 2


@pytest.mark.asyncio
async def test_a_request_taken_by_a_copy_while_held_asks_anew_without_a_header(
    scripted, requester
):
    scripted.answers = [
        store.Found(store.PENDING, APPROVAL),
        store.Found(store.ABSENT),
        store.Found(store.CREATED, OTHER_APPROVAL),
    ]
    scripted.waits = [_approval(status=store.USED), _approval(OTHER_APPROVAL)]

    refusal = await _refused()

    assert refusal.detail["approval_id"] == OTHER_APPROVAL


@pytest.mark.asyncio
async def test_too_many_waiting_is_refused_as_the_limit(scripted, requester):
    scripted.answers = [store.Found(store.ABSENT), store.Found(store.LIMIT)]

    refusal = await _refused()

    assert refusal.status_code == 429
    assert refusal.detail["type"] == "approval_limit"


@pytest.mark.asyncio
async def test_with_redis_down_nothing_is_asked_and_nothing_runs(scripted, requester):
    scripted.answers = [store.Unavailable()]

    refusal = await _refused()

    assert refusal.status_code == 503
    assert refusal.detail["type"] == "approval_unavailable"


@pytest.mark.asyncio
async def test_the_hold_answers_pending_once_its_time_is_up(monkeypatch, requester):
    """With the real wait: no answer within the hold, so the request is
    refused as pending rather than holding past the edge's limit."""
    monkeypatch.setattr(cli_approval, "get_cli_approval_hold_seconds", lambda: 0.05)
    monkeypatch.setattr(store, "REREAD_S", 0.01)

    async def ask(user_id, computer_id, digest, **kw):
        return store.Found(store.PENDING, APPROVAL)

    async def get(user_id, approval_id):
        return _approval()

    class NoChannel:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(store, "ask", ask)
    monkeypatch.setattr(store, "get", get)
    monkeypatch.setattr(store, "subscribe_to_channel", lambda channel: NoChannel())

    refusal = await asyncio.wait_for(_refused(), timeout=2)

    assert refusal.detail["type"] == "approval_pending"


def _send(text: str, **shown) -> cli_approval.Ask:
    return cli_approval._ask(
        "thread_send",
        path={"thread_id": THREAD},
        body=ChatRequest(messages=[{"role": "user", "content": text}]),
        target=shown.get("target", {}),
        details=shown.get("details", {}),
    )


def test_the_same_command_has_one_digest_and_another_its_own():
    one = _send("Go")
    again = _send("Go", target={"x": 1}, details={"y": 2})
    other = _send("Stop")

    assert one.digest == again.digest  # What the app shows is not what it is for.
    assert one.digest != other.digest


# -- what each surface asks -------------------------------------------------------


@pytest.fixture
def built(monkeypatch) -> list[cli_approval.Ask | None]:
    """The Ask each surface builds, with the gate's hold left out."""
    asks: list[cli_approval.Ask | None] = []

    async def gate(request, principal, build):
        if principal.computer_id is None:
            return
        from src.server.dependencies.principal import computer_call

        asks.append(await build(computer_call(request.scope)))

    monkeypatch.setattr(cli_approval, "gate", gate)
    return asks


PLACES = {
    WORKSPACE: Place(WORKSPACE, "Research", COMPUTER, "Cloud", False),
    OTHER_WORKSPACE: Place(OTHER_WORKSPACE, "Scratch", OTHER_COMPUTER, "Laptop", False),
}
HOME = Place("home-id", None, OTHER_COMPUTER, "Laptop", True)
HOME_WORKSPACE = "a1100000-0000-4000-8000-0000000000ff"


@pytest.fixture
def rows(monkeypatch) -> SimpleNamespace:
    world = SimpleNamespace(
        threads={
            THREAD: {
                "conversation_thread_id": THREAD,
                "workspace_id": WORKSPACE,
                "title": "Margins",
            },
            OTHER_THREAD: {
                "conversation_thread_id": OTHER_THREAD,
                "workspace_id": OTHER_WORKSPACE,
                "title": "Old notes",
            },
        },
        owners={THREAD: USER, OTHER_THREAD: USER},
        latest={OTHER_THREAD: {"conversation_response_id": "run-1", "status": "error"}},
        keyed=set(),
        automations={},
        on_computer={OTHER_COMPUTER: [OTHER_WORKSPACE]},
        prefs={},
        senders={},
        homes_bound=[],
        # Whether Home's computer can't be reached, and the cloud computer
        # Home's stand-in would run on then.
        home_away=False,
        stand_in=None,
        # The workspaces that are shared ones, on their holders' computers.
        shared=set(),
    )

    async def auth_meta(thread_id):
        thread = world.threads.get(thread_id)
        return None if thread is None else {**thread, "user_id": world.owners[thread_id]}

    async def thread_by_id(thread_id):
        return world.threads.get(thread_id)

    async def place(user_id, *, workspace_id=None, thread_id=None):
        if thread_id is not None:
            thread = world.threads.get(thread_id)
            return PLACES.get(thread["workspace_id"]) if thread else None
        return PLACES.get(workspace_id, HOME if workspace_id in (None, "home-id", HOME_WORKSPACE) else None)

    async def automation(automation_id, user_id):
        return world.automations.get(automation_id)

    async def latest(thread_id):
        return world.latest.get(thread_id)

    async def keyed(thread_id, request_key, *, except_job_id=None):
        return object() if (thread_id, request_key) in world.keyed else None

    async def workspace(workspace_id):
        return {"workspace_id": workspace_id, "name": "Scratch"}

    async def live(computer_id, *, exclude_stand_in=False):
        # The card never offers Home's stand-in as a workspace to delete.
        assert exclude_stand_in
        return list(world.on_computer.get(computer_id, []))

    async def prefs(user_id, *, cached=True):
        return {"other_preference": world.prefs}

    async def sender(user_id, thread_id):
        return world.senders.get(thread_id)

    async def in_shared(*, workspace_id=None, thread_id=None):
        thread = world.threads.get(thread_id) or {}
        return bool({workspace_id, thread.get("workspace_id")} & world.shared)

    monkeypatch.setattr(cli_approval, "get_thread_auth_meta", auth_meta)
    monkeypatch.setattr(cli_approval, "get_thread_by_id", thread_by_id)
    monkeypatch.setattr(cli_approval, "in_shared_workspace", in_shared)
    monkeypatch.setattr(cli_approval, "turn_place", place)
    monkeypatch.setattr(cli_approval, "get_automation", automation)
    monkeypatch.setattr(cli_approval.runs_db, "get_latest_attempt", latest)
    monkeypatch.setattr(cli_approval, "run_named_by_key", keyed)
    monkeypatch.setattr(cli_approval, "get_workspace", workspace)
    monkeypatch.setattr(cli_approval, "get_live_workspace_ids_for_computer", live)
    monkeypatch.setattr("src.server.database.user.get_user_preferences", prefs)
    monkeypatch.setattr("src.server.database.turn_targets.turn_sender", sender)

    async def bind_home(user_id):
        world.homes_bound.append(user_id)

    monkeypatch.setattr(cli_approval, "ensure_home", bind_home)

    async def home_turn_computer(user_id):
        if world.home_away and world.stand_in is not None:
            return world.stand_in, True
        return {"computer_id": HOME.computer_id}, False

    monkeypatch.setattr(cli_approval, "home_turn_computer", home_turn_computer)

    async def any_place(user_id, name, workspace_id, conn=None, pref=None):
        return None

    monkeypatch.setattr(cli_approval, "check_claude_code_hosted", any_place)
    return world


def _say(text: str, **fields) -> ChatRequest:
    return ChatRequest(messages=[{"role": "user", "content": text}], **fields)


async def _forbidden(coro) -> str:
    with pytest.raises(HTTPException) as caught:
        await coro
    assert caught.value.status_code == 403
    return caught.value.detail["message"]


# deletes and a new workspace


@pytest.mark.asyncio
async def test_a_thread_delete_names_the_thread(built, rows):
    await cli_approval.thread_delete(_request(), TOKEN, OTHER_THREAD, rows.threads[OTHER_THREAD])

    assert built[0].intent["path"] == {"thread_id": OTHER_THREAD}
    assert built[0].target == {"id": OTHER_THREAD, "name": "Old notes"}


@pytest.mark.asyncio
async def test_a_thread_delete_whose_id_another_account_holds_now_is_not_found(built, rows):
    # The gate builds again after the user's OK; by then the thread may be
    # gone and its id taken by another account.
    rows.owners[OTHER_THREAD] = "someone-else"

    with pytest.raises(HTTPException) as caught:
        await cli_approval.thread_delete(
            _request(), TOKEN, OTHER_THREAD, rows.threads[OTHER_THREAD]
        )

    assert caught.value.status_code == 404


@pytest.mark.asyncio
async def test_a_thread_made_again_at_a_deleted_ones_id_is_another_delete(built, rows):
    rows.threads[OTHER_THREAD]["created_at"] = "2026-11-01T00:00:00Z"
    await cli_approval.thread_delete(_request(), TOKEN, OTHER_THREAD, rows.threads[OTHER_THREAD])
    rows.threads[OTHER_THREAD]["created_at"] = "2026-11-15T00:00:00Z"
    await cli_approval.thread_delete(_request(), TOKEN, OTHER_THREAD, rows.threads[OTHER_THREAD])

    assert built[0].digest != built[1].digest


@pytest.mark.asyncio
async def test_a_command_cannot_ask_to_delete_the_thread_it_runs_in(built, rows):
    message = await _forbidden(
        cli_approval.thread_delete(_request(), TOKEN, THREAD, rows.threads[THREAD])
    )

    assert "conversation it runs in" in message
    assert built == []


@pytest.mark.asyncio
async def test_a_call_whose_filing_is_gone_cannot_ask_to_delete_a_thread(built, rows):
    message = await _forbidden(
        cli_approval.thread_delete(
            _request(call_thread=None), TOKEN, OTHER_THREAD, rows.threads[OTHER_THREAD]
        )
    )

    assert "Cannot tell which conversation" in message


@pytest.mark.asyncio
async def test_a_command_cannot_ask_to_delete_the_workspace_it_runs_in(built, rows):
    message = await _forbidden(
        cli_approval.workspace_delete(
            _request(), TOKEN, {"workspace_id": WORKSPACE, "name": "Research"}, delete_files=False
        )
    )

    assert "workspace it runs in" in message


@pytest.mark.asyncio
async def test_a_workspace_delete_is_for_its_files_choice_too(built, rows):
    workspace = {"workspace_id": OTHER_WORKSPACE, "name": "Scratch"}

    await cli_approval.workspace_delete(_request(), TOKEN, workspace, delete_files=True)
    await cli_approval.workspace_delete(_request(), TOKEN, workspace, delete_files=False)

    assert built[0].details == {"delete_files": True}
    assert built[0].digest != built[1].digest


@pytest.mark.asyncio
async def test_a_workspace_create_is_for_its_whole_body(built, rows, monkeypatch):
    async def free(user_id, name):
        return name

    async def computer(computer_id):
        return {"computer_id": computer_id, "user_id": USER, "name": "Laptop"}

    monkeypatch.setattr(cli_approval, "free_workspace_name", free)
    monkeypatch.setattr(cli_approval, "get_computer", computer)
    plain = WorkspaceCreate(name="Notes", computer_id=OTHER_COMPUTER)
    configured = WorkspaceCreate(name="Notes", computer_id=OTHER_COMPUTER, config={"a": 1})

    await cli_approval.workspace_create(_request(), TOKEN, plain)
    await cli_approval.workspace_create(_request(), TOKEN, configured)

    assert built[0].target == {"id": None, "name": "Notes"}
    assert built[0].details == {
        "description": None,
        "computer_id": OTHER_COMPUTER,
        "computer_name": "Laptop",
    }
    assert built[0].digest != built[1].digest


@pytest.mark.asyncio
async def test_a_taken_name_is_refused_before_anything_is_asked(built, rows, monkeypatch):
    async def taken(user_id, name):
        raise WorkspaceNameTaken(name, WORKSPACE)

    monkeypatch.setattr(cli_approval, "free_workspace_name", taken)

    with pytest.raises(WorkspaceNameTaken):
        await cli_approval.workspace_create(_request(), TOKEN, WorkspaceCreate(name="Research"))

    assert built == []


@pytest.mark.asyncio
async def test_a_workspace_naming_no_computer_is_created_on_the_one_approved(
    built, rows, monkeypatch
):
    # The dialog shows the primary, which may change after the user's OK.
    async def free(user_id, name):
        return name

    primary = {"computer_id": OTHER_COMPUTER, "user_id": USER, "name": "Laptop"}

    async def get_primary(user_id):
        return primary

    monkeypatch.setattr(cli_approval, "free_workspace_name", free)
    monkeypatch.setattr(cli_approval, "get_primary_computer", get_primary)

    on = await cli_approval.workspace_create(_request(), TOKEN, WorkspaceCreate(name="Notes"))
    primary = {"computer_id": COMPUTER, "user_id": USER, "name": "Cloud"}
    await cli_approval.workspace_create(_request(), TOKEN, WorkspaceCreate(name="Notes"))

    assert on == OTHER_COMPUTER
    assert built[0].intent["computer_id"] == OTHER_COMPUTER
    assert built[0].digest != built[1].digest


@pytest.mark.asyncio
async def test_a_user_who_lets_workspaces_be_created_is_not_asked(built, rows):
    rows.prefs["auto_approve_workspace_creation"] = True

    await cli_approval.workspace_create(_request(), TOKEN, WorkspaceCreate(name="Notes"))

    assert built == [None]


@pytest.mark.asyncio
async def test_a_computer_the_user_does_not_have_is_left_to_the_routes_404(
    built, rows, monkeypatch
):
    async def free(user_id, name):
        return name

    async def computer(computer_id):
        return {"computer_id": computer_id, "user_id": "someone-else"}

    monkeypatch.setattr(cli_approval, "free_workspace_name", free)
    monkeypatch.setattr(cli_approval, "get_computer", computer)

    await cli_approval.workspace_create(
        _request(), TOKEN, WorkspaceCreate(name="Notes", computer_id=OTHER_COMPUTER)
    )

    assert built == [None]


@pytest.mark.asyncio
async def test_an_automation_the_user_does_not_have_is_left_to_the_route(built, rows):
    await cli_approval.automation_delete(_request(), TOKEN, AUTOMATION)

    assert built == [None]


@pytest.mark.asyncio
@pytest.mark.parametrize(("kind", "told"), [("ssh", True), ("daytona", False)])
async def test_a_computer_remove_names_its_workspaces_and_only_an_ssh_machines_files(
    built, rows, kind, told
):
    computer = {"computer_id": OTHER_COMPUTER, "name": "Laptop", "kind": kind}

    await cli_approval.computer_remove(
        _request(),
        TOKEN,
        computer,
        remove_key=True,
        delete_files=True,
        delete_workspaces=[],
        body=ComputerRemoveRequest(delete_workspaces=[OTHER_WORKSPACE]),
    )

    assert built[0].details == {
        "remove_key": told,
        "delete_files": told,
        "delete_workspaces": [OTHER_WORKSPACE],
        "delete_workspace_names": ["Scratch"],
    }


# a turn


@pytest.mark.asyncio
async def test_a_message_to_a_thread_on_its_own_computer_asks_nothing(built, rows):
    await cli_approval.admit_turn(
        _request(call_thread=None, call_id=None), TOKEN, _say("Hi"), THREAD, from_path=True
    )

    assert built == [None]


@pytest.mark.asyncio
async def test_a_turn_returns_the_computer_it_was_checked_on_whether_it_asked_or_not(
    built, rows
):
    # Its START is held to that computer, so a move in between refuses it.
    here = await cli_approval.admit_turn(
        _request(call_thread=None, call_id=None), TOKEN, _say("Hi"), THREAD, from_path=True
    )
    there = await cli_approval.admit_turn(
        _request(), TOKEN, _say("Summarize"), OTHER_THREAD, from_path=True
    )

    assert (here, there) == (COMPUTER, OTHER_COMPUTER)


@pytest.fixture
def shared(monkeypatch, rows) -> SharedWorld:
    """Two organizations' shared workspaces, the user a member of the first
    (``test_principal_shared_reach``), with the person-level checks run for
    real and ``in_shared_workspace``'s read counted in ``lookups``."""
    world = SharedWorld(USER)
    world.install(monkeypatch)
    world.lookups = []

    async def in_shared(*, workspace_id=None, thread_id=None):
        world.lookups.append((workspace_id, thread_id))
        return world.is_shared(workspace_id=workspace_id, thread_id=thread_id)

    monkeypatch.setattr(cli_approval, "in_shared_workspace", in_shared)
    return world


def _shared_sends(target: str) -> list:
    """Each way a body names ``target``: a new thread in a workspace, a
    thread an external id resolved to (which no path named), and a retry
    run in a workspace."""
    return [
        lambda: cli_approval.admit_turn(_request(), TOKEN, _say("Hi", workspace_id=target), None),
        lambda: cli_approval.admit_turn(
            _request(call_thread=OTHER_THREAD), TOKEN, _say("Hi"), target
        ),
        lambda: cli_approval.admit_retry(
            _request(), TOKEN, OTHER_THREAD, RetryRequest(workspace_id=target)
        ),
    ]


async def _outcome(send, built) -> tuple:
    before = len(built)
    try:
        checked = await send()
    except HTTPException as e:
        return ("refused", e.status_code, e.detail)
    return ("admitted", checked, built[before:])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reached", "sends"),
    [(SHARED_WORKSPACE, (0, 2)), (SHARED_THREAD, (1,)), (MY_PRIVATE_THREAD, (1,))],
)
async def test_a_token_starts_no_turn_in_a_shared_workspace_the_user_reaches(
    built, shared, reached, sends
):
    # It runs on the workspace's own computer, so no OK of the user's covers it.
    # A path's thread is ``require_scopes``'s (test_principal_shared_reach).
    for index in sends:
        assert await _forbidden(_shared_sends(reached)[index]()) == SHARED_WORKSPACE_REFUSAL
    assert built == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unreached",
    [UNREACHED_WORKSPACE, FOREIGN_WORKSPACE, UNREACHED_THREAD, FOREIGN_THREAD, PRIVATE_THREAD],
)
async def test_what_the_user_does_not_reach_is_left_to_the_route_as_a_made_up_id(
    built, shared, unreached
):
    # A refusal here would tell the token the id exists and is shared.
    for index, send in enumerate(_shared_sends(unreached)):
        made_up = await _outcome(_shared_sends(UNKNOWN)[index], built)
        assert made_up[0] == "admitted"
        assert await _outcome(send, built) == made_up


@pytest.mark.asyncio
async def test_someone_in_no_organization_is_left_to_the_route_too(built, shared):
    shared.seat = None
    for target in (SHARED_WORKSPACE, SHARED_THREAD):
        for index, send in enumerate(_shared_sends(target)):
            made_up = await _outcome(_shared_sends(UNKNOWN)[index], built)
            assert await _outcome(send, built) == made_up


@pytest.mark.asyncio
async def test_with_the_teams_door_shut_a_body_costs_no_lookup(built, shared):
    # No one reaches a shared workspace then; its route answers 404 alone.
    shared.door = False
    for target in (SHARED_WORKSPACE, SHARED_THREAD, FOREIGN_WORKSPACE):
        for index, send in enumerate(_shared_sends(target)):
            made_up = await _outcome(_shared_sends(UNKNOWN)[index], built)
            assert await _outcome(send, built) == made_up
    assert shared.lookups == []
    assert shared.access_reads == []


@pytest.mark.asyncio
async def test_a_message_to_a_thread_on_another_computer_asks(built, rows):
    await cli_approval.admit_turn(
        _request(), TOKEN, _say("Summarize the notes"), OTHER_THREAD, from_path=True
    )

    ask = built[0]
    assert ask.action == "thread_send"
    assert ask.target == {"id": OTHER_THREAD, "name": "Old notes"}
    assert ask.details == {
        "kind": "send",
        "message_preview": "Summarize the notes",
        "workspace_name": "Scratch",
        "computer_name": "Laptop",
        "in_home": False,
    }


@pytest.mark.asyncio
async def test_a_send_is_refused_once_its_target_has_sent_the_callers_turn(built, rows):
    # The gate builds again after the user's OK; by then the target may have
    # sent the caller's thread a turn, and answering it would loop.
    await cli_approval.admit_turn(
        _request(), TOKEN, _say("Summarize"), OTHER_THREAD, from_path=True
    )
    rows.senders[THREAD] = OTHER_THREAD

    message = await _forbidden(
        cli_approval.admit_turn(
            _request(), TOKEN, _say("Summarize"), OTHER_THREAD, from_path=True
        )
    )

    assert built[0] is not None
    assert "sent it its current turn" in message


@pytest.mark.asyncio
async def test_a_different_message_is_a_different_request(built, rows):
    for text in ("One", "One", "Two"):
        await cli_approval.admit_turn(_request(), TOKEN, _say(text), OTHER_THREAD, from_path=True)

    assert built[0].digest == built[1].digest != built[2].digest


@pytest.mark.asyncio
async def test_a_home_that_cannot_be_put_on_a_computer_asks_nothing_and_runs_nothing(
    built, rows, monkeypatch
):
    async def fail(user_id):
        raise RuntimeError("no computer")

    monkeypatch.setattr(cli_approval, "ensure_home", fail)

    with pytest.raises(RuntimeError):
        await cli_approval.admit_turn(
            _request(), TOKEN, _say("Plan", workspace_id="home-id"), None
        )

    assert built == []


@pytest.mark.asyncio
async def test_a_new_thread_in_home_asks_off_the_computer_that_holds_it(built, rows):
    await cli_approval.admit_turn(_request(), TOKEN, _say("Plan", workspace_id="home-id"), None)

    assert built[0].action == "thread_create"
    assert built[0].details["in_home"] is True


@pytest.mark.asyncio
async def test_a_home_turn_from_homes_machine_asks_when_it_would_run_in_the_stand_in(
    built, rows
):
    # Home lives on the token's own machine, which can't be reached, so the
    # turn would run in Home's stand-in on a cloud computer.
    token = Principal(USER, "computer", scopes=SCOPES, computer_id=OTHER_COMPUTER)
    say = _say("Plan", workspace_id="home-id")
    here = await cli_approval.admit_turn(_request(), token, say, None)
    rows.home_away = True
    rows.stand_in = {"computer_id": COMPUTER, "name": "Cloud"}
    there = await cli_approval.admit_turn(_request(), token, say, None)

    assert built[0] is None
    assert built[1].action == "thread_create"
    assert built[1].details["computer_name"] == "Cloud"
    # Its START is held to the stand-in's computer.
    assert (here, there) == (OTHER_COMPUTER, COMPUTER)


@pytest.mark.asyncio
async def test_home_is_put_on_its_computer_before_the_user_is_asked(built, rows):
    # A Home on no computer follows the primary, which could change after
    # the user's OK; a bound one stays where the user approved.
    await cli_approval.admit_turn(_request(), TOKEN, _say("Plan", workspace_id="home-id"), None)
    await cli_approval.admit_turn(
        _request(), TOKEN, _say("Summarize the notes"), OTHER_THREAD, from_path=True
    )

    assert rows.homes_bound == [USER]


@pytest.mark.asyncio
async def test_a_new_thread_naming_no_workspace_is_left_to_the_routes_400(built, rows):
    await cli_approval.admit_turn(_request(), TOKEN, _say("Plan"), None)

    assert built == [None]


@pytest.mark.asyncio
async def test_another_users_thread_is_left_to_the_route(built, rows):
    rows.owners[OTHER_THREAD] = "someone-else"

    await cli_approval.admit_turn(_request(), TOKEN, _say("Hi"), OTHER_THREAD, from_path=True)

    assert built == [None]


@pytest.mark.asyncio
async def test_a_retransmit_is_left_to_the_routes_duplicate_answer(built, rows):
    key = "5c1f0000-0000-4000-8000-000000000001"
    rows.keyed.add((OTHER_THREAD, key))

    await cli_approval.admit_turn(
        _request(), TOKEN, _say("Hi", request_key=key), OTHER_THREAD, from_path=True
    )

    assert built == [None]


@pytest.mark.asyncio
async def test_a_thread_id_that_is_not_an_id_is_left_to_the_route(built, rows):
    await cli_approval.admit_turn(_request(), TOKEN, _say("Hi"), "not-a-uuid", from_path=True)

    assert built == [None]


@pytest.mark.asyncio
async def test_a_turns_body_is_checked_before_anything_is_asked(built, rows):
    """``check_turn_body`` runs first: a body naming the call's own thread is
    refused, never put to the user."""
    message = await _forbidden(cli_approval.admit_turn(_request(), TOKEN, _say("Hi"), THREAD))

    assert "conversation it runs in" in message
    assert built == []


# a retry


@pytest.mark.asyncio
async def test_a_retry_on_another_computer_asks(built, rows):
    body = RetryRequest(workspace_id=OTHER_WORKSPACE)

    await cli_approval.admit_retry(_request(), TOKEN, OTHER_THREAD, body)

    assert built[0].intent["kind"] == "retry"
    assert built[0].details["kind"] == "retry"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "latest",
    [None, {"conversation_response_id": "run-1", "status": "in_progress"},
     {"conversation_response_id": "run-1", "status": "completed"}],
)
async def test_a_retry_the_route_refuses_is_not_asked(built, rows, latest):
    rows.latest[OTHER_THREAD] = latest

    await cli_approval.admit_retry(
        _request(), TOKEN, OTHER_THREAD, RetryRequest(workspace_id=OTHER_WORKSPACE)
    )

    assert built == [None]


@pytest.mark.asyncio
async def test_a_retry_of_a_stale_run_is_not_asked(built, rows):
    body = RetryRequest(workspace_id=OTHER_WORKSPACE, run_id="run-0")

    await cli_approval.admit_retry(_request(), TOKEN, OTHER_THREAD, body)

    assert built == [None]


@pytest.mark.asyncio
async def test_a_retry_from_a_checkpoint_is_refused_unasked(built, rows):
    body = RetryRequest(workspace_id=OTHER_WORKSPACE, checkpoint_id="checkpoint-1")

    message = await _forbidden(cli_approval.admit_retry(_request(), TOKEN, OTHER_THREAD, body))

    assert "edit or regenerate" in message
    assert built == []


# an automation


def _automation(**fields) -> AutomationCreate:
    return AutomationCreate(
        name="Sweep",
        instruction="Check the scratch notes.",
        trigger_type="cron",
        cron_expression="0 9 * * *",
        **fields,
    )


@pytest.mark.asyncio
async def test_an_automation_created_on_another_computer_asks(built, rows):
    await cli_approval.automation_runs(
        _request(), TOKEN, "create", body=_automation(workspace_id=OTHER_WORKSPACE)
    )

    assert built[0].action == "automation_change"
    assert built[0].details == {
        "kind": "create",
        "automation_name": "Sweep",
        "prompt_preview": "Check the scratch notes.",
        "workspace_name": "Scratch",
        "computer_name": "Laptop",
        "in_home": False,
    }


@pytest.mark.asyncio
async def test_an_automation_on_its_own_computer_asks_nothing(built, rows):
    await cli_approval.automation_runs(
        _request(), TOKEN, "create", body=_automation(workspace_id=WORKSPACE)
    )

    assert built == [None]


@pytest.mark.asyncio
async def test_an_automation_naming_no_workspace_is_left_to_the_route(built, rows):
    # The route refuses it, so the user is never asked to approve it.
    await cli_approval.automation_runs(_request(), TOKEN, "create", body=_automation())

    assert built == [None]


@pytest.mark.asyncio
async def test_an_automation_in_home_asks_from_another_computer(built, rows):
    await cli_approval.automation_runs(
        _request(), TOKEN, "create", body=_automation(workspace_id=HOME_WORKSPACE)
    )

    assert built[0].details["in_home"] is True


@pytest.mark.asyncio
async def test_a_move_that_names_no_thread_asks_where_the_unpinned_runs_go(built, rows):
    # The route unpins the thread on a move, so the runs follow the new
    # workspace, not the thread on the token's own computer.
    rows.automations[AUTOMATION] = {
        "name": "Brief",
        "instruction": "Summarize.",
        "workspace_id": WORKSPACE,
        "conversation_thread_id": THREAD,
    }

    await cli_approval.automation_runs(
        _request(call_thread=None, call_id=None), TOKEN, "edit", automation_id=AUTOMATION,
        body=AutomationUpdate(workspace_id=OTHER_WORKSPACE),
    )

    assert built[0].details["workspace_name"] == "Scratch"
    assert built[0].details["computer_name"] == "Laptop"


@pytest.mark.asyncio
async def test_a_move_that_unpins_by_name_asks_where_the_unpinned_runs_go(built, rows):
    rows.automations[AUTOMATION] = {
        "name": "Brief",
        "instruction": "Summarize.",
        "workspace_id": WORKSPACE,
        "conversation_thread_id": THREAD,
    }

    await cli_approval.automation_runs(
        _request(call_thread=None, call_id=None), TOKEN, "edit", automation_id=AUTOMATION,
        body=AutomationUpdate(workspace_id=OTHER_WORKSPACE, conversation_thread_id=None),
    )

    assert built[0].details["computer_name"] == "Laptop"


@pytest.mark.asyncio
async def test_an_edit_naming_a_null_pin_is_another_request_than_one_naming_none(
    built, rows
):
    rows.automations[AUTOMATION] = {
        "name": "Brief",
        "instruction": "Summarize.",
        "workspace_id": OTHER_WORKSPACE,
        "conversation_thread_id": OTHER_THREAD,
    }

    for body in (
        AutomationUpdate(name="New"),
        AutomationUpdate(name="New", conversation_thread_id=None),
    ):
        await cli_approval.automation_runs(
            _request(), TOKEN, "edit", automation_id=AUTOMATION, body=body
        )

    assert built[0].digest != built[1].digest


@pytest.mark.asyncio
async def test_an_edit_the_stored_trigger_refuses_is_left_to_the_route(built, rows):
    rows.automations[AUTOMATION] = {
        "name": "Brief",
        "instruction": "Summarize.",
        "trigger_type": "cron",
        "workspace_id": OTHER_WORKSPACE,
        "conversation_thread_id": None,
    }

    await cli_approval.automation_runs(
        _request(), TOKEN, "edit", automation_id=AUTOMATION,
        body=AutomationUpdate(trigger_type="once"),
    )

    assert built == [None]


@pytest.mark.asyncio
async def test_an_edit_asks_where_its_runs_would_go_after_it(built, rows):
    rows.automations[AUTOMATION] = {
        "name": "Brief",
        "instruction": "Summarize.",
        "workspace_id": WORKSPACE,
        "conversation_thread_id": None,
    }

    await cli_approval.automation_runs(
        _request(), TOKEN, "edit", automation_id=AUTOMATION,
        body=AutomationUpdate(workspace_id=OTHER_WORKSPACE),
    )
    await cli_approval.automation_runs(
        _request(), TOKEN, "edit", automation_id=AUTOMATION, body=AutomationUpdate(name="New")
    )

    assert built[0].details["automation_name"] == "Brief"
    assert built[0].details["workspace_name"] == "Scratch"
    assert built[1] is None


@pytest.mark.asyncio
async def test_a_run_now_asks_off_its_computer(built, rows):
    rows.automations[AUTOMATION] = {
        "name": "Sweep",
        "instruction": "Check.",
        "status": "active",
        "workspace_id": OTHER_WORKSPACE,
        "conversation_thread_id": OTHER_THREAD,
    }

    await cli_approval.automation_runs(_request(), TOKEN, "trigger", automation_id=AUTOMATION)

    intent = dict(built[0].intent)
    assert intent.pop("acts_on")["instruction"] == "Check."
    assert intent == {
        "action": "automation_change",
        "kind": "trigger",
        "path": {"automation_id": AUTOMATION},
        "query": {},
        "body": None,
        "computer_id": OTHER_COMPUTER,
    }


@pytest.mark.asyncio
async def test_a_run_whose_pinned_thread_is_gone_asks_where_its_workspace_is(built, rows):
    # The executor makes a missing pinned thread again in the workspace.
    rows.automations[AUTOMATION] = {
        "name": "Sweep",
        "instruction": "Check.",
        "status": "active",
        "workspace_id": OTHER_WORKSPACE,
        "conversation_thread_id": "d4400000-0000-4000-8000-000000000004",
    }

    await cli_approval.automation_runs(_request(), TOKEN, "trigger", automation_id=AUTOMATION)

    assert built[0].details["computer_name"] == "Laptop"


@pytest.mark.asyncio
async def test_a_run_whose_automation_was_edited_meanwhile_is_another_request(built, rows):
    rows.automations[AUTOMATION] = {
        "name": "Sweep",
        "instruction": "Check.",
        "status": "active",
        "workspace_id": OTHER_WORKSPACE,
        "conversation_thread_id": None,
    }
    await cli_approval.automation_runs(_request(), TOKEN, "trigger", automation_id=AUTOMATION)
    rows.automations[AUTOMATION] = {**rows.automations[AUTOMATION], "instruction": "Sell."}
    await cli_approval.automation_runs(_request(), TOKEN, "trigger", automation_id=AUTOMATION)

    assert built[0].digest != built[1].digest


@pytest.mark.asyncio
async def test_a_run_into_the_callers_thread_or_its_sender_is_refused(built, rows):
    # The run would start a turn in that thread, which would answer back.
    rows.senders[THREAD] = OTHER_THREAD
    for pinned in (THREAD, OTHER_THREAD):
        rows.automations[AUTOMATION] = {
            "name": "Sweep",
            "instruction": "Check.",
            "status": "active",
            "workspace_id": OTHER_WORKSPACE,
            "conversation_thread_id": pinned,
        }
        message = await _forbidden(
            cli_approval.automation_runs(
                _request(), TOKEN, "trigger", automation_id=AUTOMATION
            )
        )
        assert "conversation" in message


@pytest.mark.asyncio
async def test_a_move_that_unpins_the_senders_thread_is_not_a_loop(built, rows):
    # The route clears the pin on a move, so the runs no longer reach it.
    rows.senders[THREAD] = OTHER_THREAD
    rows.automations[AUTOMATION] = {
        "name": "Sweep",
        "instruction": "Check.",
        "workspace_id": OTHER_WORKSPACE,
        "conversation_thread_id": OTHER_THREAD,
    }

    await cli_approval.automation_runs(
        _request(), TOKEN, "edit", automation_id=AUTOMATION,
        body=AutomationUpdate(workspace_id=WORKSPACE),
    )

    assert built == [None]  # Its own computer: nothing to ask, and no loop.


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "status", "trigger_type"),
    [("trigger", "disabled", "cron"), ("resume", "active", "cron"), ("resume", "paused", "once")],
)
async def test_a_run_or_resume_the_route_refuses_is_left_to_it(
    built, rows, kind, status, trigger_type
):
    rows.automations[AUTOMATION] = {
        "name": "Sweep",
        "instruction": "Check.",
        "status": status,
        "trigger_type": trigger_type,
        "next_run_at": None,
        "workspace_id": OTHER_WORKSPACE,
        "conversation_thread_id": None,
    }

    await cli_approval.automation_runs(_request(), TOKEN, kind, automation_id=AUTOMATION)

    assert built == [None]


@pytest.mark.asyncio
async def test_a_claude_code_place_the_route_refuses_is_left_to_it(built, rows, monkeypatch):
    async def refuse(user_id, name, workspace_id, conn=None, pref=None):
        raise AutomationRefusal("Claude Code can't run there", field="llm_model")

    monkeypatch.setattr(cli_approval, "check_claude_code_hosted", refuse)

    await cli_approval.automation_runs(
        _request(), TOKEN, "create", body=_automation(workspace_id=OTHER_WORKSPACE)
    )

    assert built == [None]


@pytest.mark.asyncio
async def test_a_model_the_route_refuses_is_left_to_the_route(built, rows, monkeypatch):
    async def refuse(user_id, name, pref):
        raise AutomationRefusal(f"unknown model {name!r}", field="llm_model")

    monkeypatch.setattr(cli_approval, "check_model", refuse)

    await cli_approval.automation_runs(
        _request(), TOKEN, "create",
        body=_automation(workspace_id=OTHER_WORKSPACE, llm_model="no-such-model"),
    )

    assert built == [None]


@pytest.mark.asyncio
async def test_a_pin_to_a_thread_that_does_not_exist_is_left_to_the_route(built, rows):
    await cli_approval.automation_runs(
        _request(), TOKEN, "create",
        body=_automation(
            workspace_id=OTHER_WORKSPACE,
            conversation_thread_id="d4400000-0000-4000-8000-000000000004",
        ),
    )

    assert built == [None]


@pytest.mark.asyncio
async def test_a_pin_outside_the_automations_workspace_is_left_to_the_route(built, rows):
    await cli_approval.automation_runs(
        _request(), TOKEN, "create",
        body=_automation(workspace_id=WORKSPACE, conversation_thread_id=OTHER_THREAD),
    )

    assert built == [None]


@pytest.mark.asyncio
async def test_an_approval_holds_only_for_the_computer_it_named(built, rows, monkeypatch):
    # Home with no binding follows the primary, which may change while the
    # request waits: the request then names another computer and asks anew.
    body = _automation(workspace_id=HOME_WORKSPACE)
    await cli_approval.automation_runs(_request(), TOKEN, "create", body=body)
    moved = Place("home-id", None, "c3300000-0000-4000-8000-000000000003", "Desk", True)

    async def place(user_id, *, workspace_id=None, thread_id=None):
        return moved

    monkeypatch.setattr(cli_approval, "turn_place", place)
    await cli_approval.automation_runs(_request(), TOKEN, "create", body=body)

    assert built[0].intent["computer_id"] == OTHER_COMPUTER
    assert built[1].intent["computer_id"] == moved.computer_id
    assert built[0].digest != built[1].digest


@pytest.mark.asyncio
async def test_an_automation_cannot_send_into_the_thread_the_call_runs_in(built, rows):
    message = await _forbidden(
        cli_approval.automation_runs(
            _request(), TOKEN, "create", body=_automation(conversation_thread_id=THREAD)
        )
    )

    assert "conversation it runs in" in message


@pytest.mark.asyncio
async def test_an_automation_into_the_thread_that_sent_the_turn_is_refused(built, rows):
    rows.senders[THREAD] = OTHER_THREAD

    message = await _forbidden(
        cli_approval.automation_runs(
            _request(), TOKEN, "create", body=_automation(conversation_thread_id=OTHER_THREAD)
        )
    )

    assert "sent it its current turn" in message


@pytest.mark.asyncio
async def test_an_automation_named_by_a_call_whose_filing_is_gone_cannot_pin_a_thread(
    built, rows
):
    message = await _forbidden(
        cli_approval.automation_runs(
            _request(call_thread=None),
            TOKEN,
            "create",
            body=_automation(conversation_thread_id=OTHER_THREAD),
        )
    )

    assert "Cannot tell which conversation" in message


@pytest.mark.asyncio
@pytest.mark.parametrize("principal", [SESSION, PAT])
async def test_any_other_caller_points_an_automation_anywhere_unasked(built, rows, principal):
    await cli_approval.automation_runs(
        _request(principal), principal, "create", body=_automation(conversation_thread_id=THREAD)
    )

    assert built == []


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", [USER, "user-gate-2"])
async def test_the_request_names_the_calling_thread_only_while_it_is_the_users(
    monkeypatch, owner
):
    # A thread deleted after its command started can be made again at its id
    # by another account.
    async def computer(computer_id):
        return {"name": "Laptop"}

    async def meta(thread_id):
        return {"user_id": owner, "workspace_id": WORKSPACE, "title": "Bonds"}

    async def workspace(workspace_id):
        return {"workspace_id": workspace_id, "name": "Rates"}

    monkeypatch.setattr(cli_approval, "get_computer", computer)
    monkeypatch.setattr(cli_approval, "get_thread_auth_meta", meta)
    monkeypatch.setattr(cli_approval, "get_workspace", workspace)

    named = await cli_approval._requester(TOKEN, CliCall(TOKEN, CALL_ID, THREAD))

    if owner == USER:
        assert (named["kind"], named["thread_title"], named["workspace_name"]) == (
            "analyst",
            "Bonds",
            "Rates",
        )
    else:
        assert (named["kind"], named["thread_id"], named["thread_title"]) == ("command", None, None)
        assert named["workspace_name"] is None


@pytest.mark.asyncio
async def test_a_null_pin_keeps_the_stored_one_so_a_loop_through_it_is_refused(built, rows):
    # The route drops a field the body sets to null, so the stored pin stays.
    rows.automations[AUTOMATION] = {
        "name": "Brief",
        "instruction": "Summarize.",
        "workspace_id": WORKSPACE,
        "conversation_thread_id": THREAD,
    }

    message = await _forbidden(
        cli_approval.automation_runs(
            _request(), TOKEN, "edit", automation_id=AUTOMATION,
            body=AutomationUpdate(name="New", conversation_thread_id=None),
        )
    )

    assert "conversation it runs in" in message


@pytest.mark.asyncio
async def test_a_null_workspace_keeps_the_stored_one_so_an_edit_there_asks(built, rows):
    rows.automations[AUTOMATION] = {
        "name": "Brief",
        "instruction": "Summarize.",
        "workspace_id": OTHER_WORKSPACE,
        "conversation_thread_id": None,
    }

    await cli_approval.automation_runs(
        _request(), TOKEN, "edit", automation_id=AUTOMATION,
        body=AutomationUpdate(instruction="Shorter", workspace_id=None),
    )

    assert built[0].details["computer_name"] == "Laptop"


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace", [WORKSPACE, OTHER_WORKSPACE])
async def test_a_chat_the_route_would_refuse_to_deliver_to_asks_nothing(
    built, rows, monkeypatch, workspace
):
    from src.server.services.automations.lifecycle import DeliveryRefused

    checked: list[list[str]] = []

    async def refuse(user_id, methods, *, stored=()):
        checked.append(list(methods))
        raise DeliveryRefused([("slack:T1/C1", "no messaging service")])

    monkeypatch.setattr(cli_approval, "check_delivery", refuse)

    await cli_approval.automation_runs(
        _request(), TOKEN, "create",
        body=_automation(workspace_id=workspace, delivery_config={"methods": ["slack:T1/C1"]}),
    )

    assert built == [None]
    # Asked of the messaging service only when the user would be asked.
    assert checked == ([] if workspace == WORKSPACE else [["slack:T1/C1"]])


@pytest.mark.asyncio
@pytest.mark.parametrize(("workspace", "computer"), [(WORKSPACE, COMPUTER), (OTHER_WORKSPACE, OTHER_COMPUTER)])
async def test_a_run_now_names_the_computer_it_was_checked_to_run_on(
    built, rows, workspace, computer
):
    rows.automations[AUTOMATION] = {
        "name": "Sweep",
        "instruction": "Check.",
        "status": "active",
        "workspace_id": workspace,
        "conversation_thread_id": None,
    }

    checked = await cli_approval.automation_runs(
        _request(), TOKEN, "trigger", automation_id=AUTOMATION
    )

    assert checked == computer
