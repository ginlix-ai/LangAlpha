"""Who hears what happens in a shared workspace.

An event on an organization's channel reaches a feed connection only while
its account reaches the workspace, read again for every event: a member
removed, suspended, or out of the feature hears nothing more, told once that
they lost it; an organization member outside the workspace and an account of
another organization hear nothing at all, not even that something happened.
A deleted workspace is told as deleted to those who reached it. Personal
events never go to an organization's channel.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Optional

import pytest

from src.server.services import thread_lifecycle_feed as feed_mod
from src.server.services.shared_workspaces import feed
from src.server.services.shared_workspaces.feed import (
    ACCESS_ENDED_EVENT,
    MEMBERS_EVENT,
    WORKSPACE_EVENT,
    Reach,
    SharedFeed,
)

pytestmark = pytest.mark.asyncio

ORG, OTHER_ORG = "org-n", "org-h"
W, W2 = "ws-desk", "ws-other"
MAYA, LEE = "maya", "lee"


class _Reaches:
    """``reach_of`` as the database would answer it, changed by the test."""

    def __init__(self, **by_user: Optional[Reach]) -> None:
        self.by_user = dict(by_user)
        self.fail = False
        self.deleted: set[str] = set()
        #: Accounts taken out of a workspace before it was deleted.
        self.removed: set[str] = set()

    async def __call__(self, user_id: str, **_: object) -> Optional[Reach]:
        if self.fail:
            raise RuntimeError("database unavailable")
        return self.by_user.get(user_id)


@pytest.fixture
def reaches(monkeypatch) -> _Reaches:
    r = _Reaches()
    monkeypatch.setattr(feed, "reach_of", r)

    async def _deleted(workspace_id: str, user_id: str) -> bool:
        return workspace_id in r.deleted and user_id not in r.removed

    monkeypatch.setattr(feed, "_deleted", _deleted)
    return r


def _thread_event(workspace_id: str = W, **extra) -> dict:
    return {
        "v": 1,
        "type": "run_started",
        "thread_id": "t-1",
        "workspace_id": workspace_id,
        "run_id": "r-1",
        "run_seq": 4,
        "status": "running",
        feed_mod.SENDER_KEY: MAYA,
        **extra,
    }


async def _open(reaches: _Reaches, user: str, *workspaces: str, org: str = ORG) -> SharedFeed:
    reaches.by_user[user] = Reach(org, frozenset(workspaces))
    opened = await SharedFeed.open(user)
    assert opened is not None
    return opened


async def test_an_account_that_reaches_no_shared_workspace_opens_nothing(reaches):
    reaches.by_user[LEE] = None
    assert await SharedFeed.open(LEE) is None
    reaches.fail = True
    assert await SharedFeed.open(LEE) is None


async def test_a_member_hears_the_event_without_its_sender_field(reaches):
    lee = await _open(reaches, LEE, W)

    (frame,) = await lee.on_event(_thread_event())

    assert frame["type"] == "run_started" and frame["workspace_id"] == W
    assert feed_mod.SENDER_KEY not in frame


async def test_an_org_member_outside_the_workspace_hears_nothing(reaches):
    nate = await _open(reaches, "nate", W2)

    assert await nate.on_event(_thread_event()) == []
    assert await nate.on_event({"v": 1, "type": MEMBERS_EVENT, "workspace_id": W}) == []
    assert await nate.on_event({"v": 1, "type": WORKSPACE_EVENT, "workspace_id": W, "change": "deleted"}) == []


async def test_a_member_removed_hears_once_that_they_lost_it_and_nothing_after(reaches):
    lee = await _open(reaches, LEE, W, W2)
    reaches.by_user[LEE] = Reach(ORG, frozenset({W2}))

    frames = await lee.on_event(_thread_event())
    assert frames == [{"v": 1, "type": ACCESS_ENDED_EVENT, "workspace_id": W}]
    assert await lee.on_event(_thread_event()) == []
    assert await lee.reconcile() == []
    # The workspace still reached is heard as before.
    assert [f["type"] for f in await lee.on_event(_thread_event(W2))] == ["run_started"]


@pytest.mark.parametrize("why", ["suspended", "feature off", "left the organization"])
async def test_an_account_that_can_reach_none_now_hears_nothing(reaches, why):
    """Each makes ``reach_of`` None: the account is told once of what it lost."""
    lee = await _open(reaches, LEE, W)
    reaches.by_user[LEE] = None

    assert await lee.on_event(_thread_event()) == [
        {"v": 1, "type": ACCESS_ENDED_EVENT, "workspace_id": W}
    ]
    assert await lee.on_event(_thread_event()) == []


async def test_an_account_moved_to_another_organization_hears_nothing_of_this_one(reaches):
    lee = await _open(reaches, LEE, W)
    reaches.by_user[LEE] = Reach(OTHER_ORG, frozenset({W}))

    frames = await lee.on_event(_thread_event())
    assert [f["type"] for f in frames] == [ACCESS_ENDED_EVENT]


async def test_a_failed_reach_read_passes_nothing_on(reaches):
    lee = await _open(reaches, LEE, W)
    reaches.fail = True

    assert await lee.on_event(_thread_event()) == []
    assert await lee.reconcile() == []
    reaches.fail = False
    assert [f["type"] for f in await lee.on_event(_thread_event())] == ["run_started"]


async def test_an_account_that_cannot_be_read_keeps_what_it_reached(monkeypatch):
    """An outage is not a loss: taken for one, every open page would be told
    its access had ended. Nothing is passed on until the account reads again."""
    from src.server.services import account

    seats = {"unknown": False}

    async def _seat(user_id):
        if seats["unknown"]:
            raise account.AccountUnknown()
        return SimpleNamespace(org_id=ORG, team_ids=frozenset())

    async def _rows(user_id, org_id, team_ids):
        return [(SimpleNamespace(workspace_id=W), None)]

    monkeypatch.setattr(feed.access, "caller_seat", _seat)
    monkeypatch.setattr(feed.db, "list_for_member", _rows)
    lee = await SharedFeed.open(LEE)
    seats["unknown"] = True

    assert await lee.reconcile() == []
    assert await lee.on_event(_thread_event()) == []
    assert lee.workspace_ids == [W]
    # Elsewhere an unreadable account reaches none.
    assert await feed.reach_of(LEE) is None
    seats["unknown"] = False
    assert [f["type"] for f in await lee.on_event(_thread_event())] == ["run_started"]


async def test_an_account_moved_to_another_organization_gets_a_feed_for_it(reaches):
    lee = await _open(reaches, LEE, W)
    assert lee.moved_on() is None
    reaches.by_user[LEE] = Reach(OTHER_ORG, frozenset({W2}))

    assert [f["type"] for f in await lee.reconcile()] == [ACCESS_ENDED_EVENT]
    there = lee.moved_on()
    assert (there.reach, there.channel) == (
        Reach(OTHER_ORG, frozenset({W2})), feed_mod.shared_events_channel(OTHER_ORG)
    )


async def test_a_member_added_hears_from_the_next_event(reaches):
    nate = await _open(reaches, "nate", W2)
    reaches.by_user["nate"] = Reach(ORG, frozenset({W, W2}))

    assert [f["type"] for f in await nate.on_event({"v": 1, "type": MEMBERS_EVENT, "workspace_id": W})] == [
        MEMBERS_EVENT
    ]


async def test_a_deleted_workspace_is_told_as_deleted_to_those_who_reached_it(reaches):
    lee = await _open(reaches, LEE, W)
    nate = await _open(reaches, "nate", W2)
    deleted = {"v": 1, "type": WORKSPACE_EVENT, "workspace_id": W, "change": "deleted"}
    reaches.by_user[LEE] = Reach(ORG, frozenset())
    reaches.deleted.add(W)

    assert await lee.on_event(deleted) == [deleted]
    assert await nate.on_event(deleted) == []
    # Told it was deleted, not also that they lost it.
    assert await lee.reconcile() == []


async def test_a_member_removed_before_the_delete_only_learns_they_lost_it(reaches):
    """A connection's reach can be older than a removal. Whether the
    workspace still exists is not theirs to learn, by event or by reconcile."""
    deleted = {"v": 1, "type": WORKSPACE_EVENT, "workspace_id": W, "change": "deleted"}
    ended = [{"v": 1, "type": ACCESS_ENDED_EVENT, "workspace_id": W}]
    reaches.deleted.add(W)
    reaches.removed.add(LEE)

    by_event = await _open(reaches, LEE, W)
    reaches.by_user[LEE] = Reach(ORG, frozenset())
    assert await by_event.on_event(deleted) == ended

    reaches.by_user[LEE] = Reach(ORG, frozenset({W}))
    by_reconcile = await _open(reaches, LEE, W)
    reaches.by_user[LEE] = Reach(ORG, frozenset())
    assert await by_reconcile.reconcile() == ended


async def test_a_workspace_found_gone_before_its_event_is_told_as_deleted(reaches):
    """The reach can be read between the delete and its event: the loss is
    told as the deletion it was, and the event then adds nothing."""
    lee = await _open(reaches, LEE, W)
    reaches.by_user[LEE] = Reach(ORG, frozenset())
    reaches.deleted.add(W)

    assert await lee.reconcile() == [
        {"v": 1, "type": WORKSPACE_EVENT, "workspace_id": W, "change": "deleted"}
    ]
    deleted = {"v": 1, "type": WORKSPACE_EVENT, "workspace_id": W, "change": "deleted"}
    assert await lee.on_event(deleted) == []


async def test_a_turn_paused_for_its_sender_reads_as_running_to_others(reaches):
    paused = _thread_event(type="run_settled", status="interrupted", interrupt_reason="user_question")
    lee = await _open(reaches, LEE, W)
    maya = await _open(reaches, MAYA, W)

    (to_lee,) = await lee.on_event(paused)
    (to_maya,) = await maya.on_event(paused)

    assert (to_lee["status"], to_lee["interrupt_reason"]) == ("running", None)
    assert (to_maya["status"], to_maya["interrupt_reason"]) == ("interrupted", "user_question")


async def test_reconcile_tells_a_lost_workspace_once(reaches):
    lee = await _open(reaches, LEE, W)
    reaches.by_user[LEE] = Reach(ORG, frozenset())

    assert await lee.reconcile() == [{"v": 1, "type": ACCESS_ENDED_EVENT, "workspace_id": W}]
    assert await lee.reconcile() == []


# ---------------------------------------------------------------------------
# Which channel an event goes to
# ---------------------------------------------------------------------------


class _Redis:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []

    async def publish(self, channel: str, data: str) -> None:
        self.sent.append((channel, json.loads(data)))


@pytest.fixture
def redis(monkeypatch) -> _Redis:
    r = _Redis()

    class _Cache:
        enabled = True
        client = r

    monkeypatch.setattr(feed_mod, "get_cache_client", lambda: _Cache())
    return r


async def test_a_personal_event_goes_to_its_user_only(redis):
    await feed_mod.publish_thread_title(user_id=MAYA, thread_id="t", workspace_id="home", title="x")
    assert [c for c, _ in redis.sent] == [feed_mod.user_events_channel(MAYA)]


async def test_a_shared_event_goes_to_the_organization_and_its_notice_to_the_sender(redis):
    from src.server.contracts.notices import Notice

    await feed_mod.publish_run_started(
        user_id=MAYA,
        thread_id="t",
        workspace_id=W,
        run_id="r",
        run_seq=3,
        notification=Notice("started", "Desk", "Checking a price alert…"),
        shared_org=ORG,
    )

    (org_channel, to_org), (own_channel, to_maya) = redis.sent
    assert org_channel == feed_mod.shared_events_channel(ORG)
    assert to_org.get("notification") is None and to_org[feed_mod.SENDER_KEY] == MAYA
    assert own_channel == feed_mod.user_events_channel(MAYA)
    assert to_maya["notification"] is not None


async def test_a_shared_settle_goes_to_the_organization_without_a_cursor(redis):
    await feed_mod._exec_user_feed(
        {
            "payload": {
                "user_id": MAYA,
                "thread_id": "t",
                "workspace_id": W,
                "run_id": "r",
                "run_seq": 3,
                "status": "completed",
                "last_seen_run_seq": 3,
                "shared_org_id": ORG,
            }
        }
    )

    ((channel, event),) = redis.sent
    assert channel == feed_mod.shared_events_channel(ORG)
    assert event.get("last_seen_run_seq") is None and event[feed_mod.SENDER_KEY] == MAYA
