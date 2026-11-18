"""A shared workspace's thread is its starter's alone until shared.

Pins each refusal of sharing and making private, the one answer every
owner-only route gives a thread the caller does not reach, the START hook that
refuses a turn in a thread made private meanwhile, the private thread's
address its feed events carry, and a bound thread left out of an artifact a colleague reads.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.database.conversation import thread_reach
from src.server.services import thread_lifecycle_feed as feed_mod
from src.server.services.shared_workspaces import feed, thread_visibility
from src.server.services.shared_workspaces.feed import Reach, SharedFeed

pytestmark = pytest.mark.asyncio

ORG, W = "org-n", "ws-desk"
MAYA, LEE = "maya", "lee"
T = "7157f2a5-26af-4dab-9b63-0219dbd5d814"


async def test_the_rule_is_theirs_shared_or_started_by_them():
    assert thread_reach.visible_sql("ct", "w.user_id", "%(u)s") == (
        "(w.user_id = %(u)s OR (ct.workspace_shared_at IS NOT NULL OR ct.created_by = %(u)s))"
    )


# --- sharing and making private -------------------------------------------


def _access(shared: bool = True):
    return SimpleNamespace(
        is_shared=shared,
        shared=SimpleNamespace(workspace_id=W) if shared else None,
        org_id=ORG if shared else None,
    )


async def _set(outcome: str, *, shared_ws: bool = True, to: bool = False):
    publish = AsyncMock()
    with patch.object(thread_visibility, "thread_access", AsyncMock(return_value=_access(shared_ws))), \
         patch.object(thread_reach, "set_shared", AsyncMock(return_value=(outcome, None, None))), \
         patch.object(thread_visibility, "publish_user_event", publish):
        try:
            await thread_visibility.set_shared(T, MAYA, to)
        except HTTPException as e:
            return e.status_code, e.detail, publish
    return 200, None, publish


@pytest.mark.parametrize(
    ("outcome", "status", "type_"),
    [
        ("not_starter", 403, thread_visibility.STARTER_ONLY),
        ("others_sent", 409, thread_visibility.OTHERS_SENT),
        ("missing", 404, None),
    ],
)
async def test_each_refusal(outcome, status, type_):
    got, detail, publish = await _set(outcome)
    assert got == status
    if type_:
        assert detail["type"] == type_ and detail["message"]
    else:
        assert detail == "Thread not found"
    publish.assert_not_called()


async def test_only_a_shared_workspaces_thread_can_be_shared():
    got, detail, publish = await _set("done", shared_ws=False, to=True)
    assert (got, detail["type"]) == (400, thread_visibility.NOT_SHARED_WORKSPACE)
    publish.assert_not_called()


async def test_a_thread_the_caller_does_not_reach_is_refused_before_anything_is_read():
    flip = AsyncMock()
    with patch.object(
        thread_visibility, "thread_access",
        AsyncMock(side_effect=HTTPException(status_code=404, detail="Thread not found")),
    ), patch.object(thread_reach, "set_shared", flip):
        with pytest.raises(HTTPException) as e:
            await thread_visibility.set_shared(T, LEE, False)
    assert (e.value.status_code, e.value.detail) == (404, "Thread not found")
    flip.assert_not_called()


@pytest.mark.parametrize("to", [True, False])
async def test_a_flip_tells_the_organization_and_no_change_tells_no_one(to):
    _, _, publish = await _set("done", to=to)
    (owner, event), kwargs = publish.call_args
    assert owner is None and kwargs == {"shared_org": ORG}
    assert event["type"] == feed_mod.VISIBILITY_EVENT
    assert (event["thread_id"], event["workspace_id"], event["shared"]) == (T, W, to)
    _, _, publish = await _set("unchanged", to=to)
    publish.assert_not_called()


class _LockedRow:
    """The thread row ``set_shared`` reads under its lock, and what it ran."""

    def __init__(self, *, link_on: bool) -> None:
        self.row = {
            "created_by": MAYA, "workspace_shared_at": "then", "is_shared": link_on,
            "share_permissions": '{"allow_files": true, "audience": "organization"}',
        }
        self.ran: list[tuple[str, object]] = []

    def get_db_connection(self, conn=None):
        test = self

        class _Conn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return None

            def transaction(self):
                return self

            def cursor(self, **_):
                return _Cur()

        class _Cur:
            last = ""

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return None

            async def execute(self, sql, params=None):
                test.ran.append((sql, params))
                self.last = sql

            async def fetchone(self):
                if "FOR UPDATE" in self.last:
                    return dict(test.row)
                if "AS others FROM" in self.last:
                    return {"others": False}
                return {"created_by": MAYA, "shared_at": None, "shared_by": None, "others_sent": False}

        return _Conn()


@pytest.mark.parametrize("link_on", [True, False])
async def test_making_private_turns_the_link_off_in_the_flip(link_on):
    row = _LockedRow(link_on=link_on)
    with patch.object(thread_reach, "pool", row):
        outcome, _, link_off = await thread_reach.set_shared(T, MAYA, False)
    assert outcome == "done"
    assert link_off == ({"allow_files": True, "audience": "organization"} if link_on else None)
    (flip,) = [(sql, p) for sql, p in row.ran if sql == thread_reach.SET_SHARED_SQL]
    # One statement on the locked row: private now, and no link either way.
    assert "is_shared = is_shared AND %(shared)s" in flip[0] and flip[1]["shared"] is False


async def test_a_link_turned_off_is_recorded_and_answered():
    record = patch.object(thread_visibility.activity, "record")
    link_off = {"allow_files": True, "audience": "organization"}
    with patch.object(thread_visibility, "thread_access", AsyncMock(return_value=_access())), \
         patch.object(thread_reach, "set_shared", AsyncMock(return_value=("done", None, link_off))), \
         patch.object(thread_visibility, "publish_user_event", AsyncMock()), record as recorded:
        assert await thread_visibility.set_shared(T, MAYA, False) is True
    (user, action), kwargs = recorded.call_args
    assert (user, action) == (MAYA, thread_visibility.Action.THREAD_LINK_OFF)
    assert kwargs["detail"] == {"allow_files": True} and kwargs["thread_id"] == T


async def test_a_colleagues_comment_words_delivered_or_waiting_keep_the_thread_shared():
    """A starter's send carries any member's comments, so Lee's words reach
    Maya's turn under her run; they count as his, sent or still queued."""
    sql = " ".join(thread_reach.OTHERS_SENT_SQL.split())
    assert (
        "FROM artifact_comment_messages m WHERE m.thread_id = t.conversation_thread_id"
        " AND m.author = 'user' AND m.written_by IS DISTINCT FROM t.created_by"
    ) in sql
    assert (
        "WHERE c.thread_id = t.conversation_thread_id AND c.delivery = 'queued'"
        " AND m.author = 'user' AND m.sent_at IS NULL"
        " AND m.written_by IS DISTINCT FROM t.created_by"
    ) in sql


class _Cursor:
    def __init__(self, reached: bool) -> None:
        self.reached, self.ran = reached, []

    async def execute(self, sql, params=None):
        self.ran.append((" ".join(sql.split()), params))

    async def fetchone(self):
        return {"reached": self.reached}


async def test_words_wait_only_for_a_thread_their_writer_reaches():
    """Lee writing again on a comment Maya queued for her private thread would
    have his words carried into it; refused as a missing destination, read
    under the thread row's share lock so a make-private cannot slip between."""
    from src.server.database.artifacts import comments, routing

    queued = {"delivery": "queued", "thread_id": T}
    cur = _Cursor(reached=False)
    with pytest.raises(routing.DestinationUnavailable):
        await comments._queue_thread(
            cur, user_id="shared-0000", workspace_id=W, current=queued, thread_id=None, caller=LEE
        )
    ((sql, params),) = cur.ran
    assert sql.endswith("FOR SHARE OF t")
    assert params == {"thread_id": T, "user_id": "shared-0000", "caller": LEE}

    assert await comments._queue_thread(
        _Cursor(reached=True), user_id="shared-0000", workspace_id=W, current=queued,
        thread_id=None, caller=MAYA,
    ) == T
    # A personal workspace's comment keeps waiting as before, asking nothing.
    personal = _Cursor(reached=False)
    assert await comments._queue_thread(
        personal, user_id="alice", workspace_id=W, current=queued, thread_id=None,
        caller="alice",
    ) == T
    assert personal.ran == []


# --- owner-only routes -----------------------------------------------------


async def _owner_answer(owner: str, *, reached: bool) -> tuple[int, object]:
    from src.server.utils.api import require_thread_owner

    gate = AsyncMock(
        side_effect=None if reached else HTTPException(status_code=404, detail="Thread not found")
    )
    with patch("src.server.database.conversation.get_thread_owner_id", AsyncMock(return_value=owner)), \
         patch("src.server.services.shared_workspaces.access.thread_access", gate):
        with pytest.raises(HTTPException) as e:
            await require_thread_owner(T, LEE)
    return (e.value.status_code, e.value.detail), gate


async def test_an_owner_only_route_answers_a_thread_out_of_reach_as_no_thread():
    answer, _ = await _owner_answer("shared-0000", reached=False)
    assert answer == (404, "Thread not found")
    answer, _ = await _owner_answer("shared-0000", reached=True)
    assert answer == (403, "Forbidden")


async def test_someone_elses_personal_thread_is_still_forbidden_and_asks_nothing_more():
    answer, gate = await _owner_answer("alice", reached=False)
    assert answer == (403, "Forbidden")
    gate.assert_not_called()


# --- the START hook ---------------------------------------------------------


async def test_start_refuses_a_turn_in_a_thread_made_private_meanwhile():
    from src.server.services.runs import admission

    then = AsyncMock()
    with patch.object(thread_reach, "hidden", AsyncMock(return_value={T})):
        with pytest.raises(HTTPException) as e:
            await admission._refuse_private_thread(T, LEE, "conn", then=then)
    assert (e.value.status_code, e.value.detail) == (404, "Thread not found")
    then.assert_not_called()

    with patch.object(thread_reach, "hidden", AsyncMock(return_value=set())) as hidden:
        await admission._refuse_private_thread(T, LEE, "conn", then=then)
    # Read on START's own connection, after its row lock.
    assert hidden.call_args.kwargs == {"conn": "conn"}
    then.assert_awaited_once_with("conn")


async def test_start_refuses_a_sender_who_lost_the_workspace_meanwhile():
    from src.server.services.account_contract import Team
    from src.server.services.runs import admission
    from src.server.services.shared_workspaces.access import Seat

    holder = "shared-0000"
    seat = Seat(ORG, (Team("team-1", "Desk", "member"),))
    then = AsyncMock()
    reach = "src.server.database.shared_workspaces.reach_held"
    with patch(reach, AsyncMock(return_value=False)):
        with pytest.raises(HTTPException) as e:
            await admission._refuse_unreached_workspace(holder, LEE, seat, "conn", then=then)
    # The answer a thread out of reach gets, word for word.
    assert (e.value.status_code, e.value.detail) == (404, "Thread not found")
    then.assert_not_called()

    with patch(reach, AsyncMock(return_value=True)) as held:
        await admission._refuse_unreached_workspace(holder, LEE, seat, "conn", then=then)
    # Read on START's own connection, with the teams read before it.
    held.assert_awaited_once_with("conn", holder, LEE, ORG, frozenset({"team-1"}))
    then.assert_awaited_once_with("conn")


async def test_a_sender_with_no_seat_is_refused_before_start():
    from src.server.services.runs import admission

    with patch(
        "src.server.services.shared_workspaces.access.caller_seat", AsyncMock(return_value=None)
    ):
        with pytest.raises(HTTPException) as e:
            await admission._sender_seat(LEE)
    assert (e.value.status_code, e.value.detail) == (404, "Thread not found")


# --- open streams ------------------------------------------------------------


async def _streamed(viewer: str, *, private_after: int, step: float) -> tuple[list[str], AsyncMock]:
    """What ``viewer``'s open run stream on Maya's thread delivers when she
    makes it private after ``private_after`` frames went out, ``step``
    seconds passing between frames."""
    from src.server.app.threads import messaging
    from src.server.services.shared_workspaces import access as access_mod
    from src.server.services.shared_workspaces.access import Access

    sent: list[str] = []
    # Started from the real clock, which the watch's own fields were read from.
    clock = [access_mod.time.monotonic()]

    async def run_stream(*_a):
        for n in range(3):
            yield f"frame {n}"
            sent.append(n)
            clock[0] += step

    def hidden(ids, user):
        return set(ids) if user != MAYA and len(sent) >= private_after else set()

    reaches = AsyncMock(side_effect=hidden)
    access = Access("shared-0000", SimpleNamespace(workspace_id=W))
    with patch.object(messaging, "thread_access", AsyncMock(return_value=access)), \
         patch.object(access_mod.time, "monotonic", lambda: clock[0]), \
         patch.object(thread_reach, "hidden", reaches), \
         patch("src.server.handlers.chat.reconnect_to_workflow_stream", run_stream), \
         patch("src.server.handlers.chat.reconnect_admission.classify_reconnect", AsyncMock(return_value="r")):
        response = await messaging.reconnect_to_stream(T, viewer, None, None, None)
        return [frame async for frame in response.body_iterator], reaches


async def test_a_colleagues_open_stream_stops_within_the_recheck_once_private():
    from src.server.services.shared_workspaces.access import THREAD_RECHECK_S

    frames, _ = await _streamed(LEE, private_after=1, step=THREAD_RECHECK_S + 0.5)
    assert frames == ["frame 0"]


async def test_the_starters_own_stream_stays_open():
    from src.server.services.shared_workspaces.access import THREAD_RECHECK_S

    frames, _ = await _streamed(MAYA, private_after=1, step=THREAD_RECHECK_S + 0.5)
    assert frames == ["frame 0", "frame 1", "frame 2"]


async def test_frames_inside_the_recheck_read_nothing():
    frames, reaches = await _streamed(LEE, private_after=1, step=0.1)
    assert frames == ["frame 0", "frame 1", "frame 2"]
    reaches.assert_not_called()


# --- the feed ----------------------------------------------------------------


@pytest.fixture
def opened(monkeypatch):
    reach = Reach(ORG, frozenset({W}))

    async def reach_of(user_id, **_):
        return reach

    monkeypatch.setattr(feed, "reach_of", reach_of)

    async def _open(user):
        f = await SharedFeed.open(user)
        assert f is not None
        return f

    return _open


def _run_event(**extra) -> dict:
    return {"v": 1, "type": "run_started", "thread_id": T, "workspace_id": W,
            "status": "running", feed_mod.SENDER_KEY: MAYA, **extra}


async def test_a_private_threads_events_reach_its_starter_alone(opened):
    private = _run_event(**{feed_mod.PRIVATE_TO_KEY: MAYA})
    (frame,) = await (await opened(MAYA)).on_event(dict(private))
    assert frame["thread_id"] == T and feed_mod.PRIVATE_TO_KEY not in frame
    assert await (await opened(LEE)).on_event(dict(private)) == []
    assert len(await (await opened(LEE)).on_event(_run_event())) == 1


async def test_a_comment_names_no_thread_its_reader_does_not_reach(opened):
    event = {"v": 1, "type": feed.COMMENT_EVENT, "thread_id": T, "workspace_id": W,
             "artifact_id": "a", feed_mod.PRIVATE_TO_KEY: MAYA}
    (frame,) = await (await opened(LEE)).on_event(dict(event))
    assert frame["thread_id"] is None and frame["artifact_id"] == "a"
    (frame,) = await (await opened(MAYA)).on_event(dict(event))
    assert frame["thread_id"] == T


async def test_a_flip_reaches_everyone_in_the_workspace(opened):
    event = {"v": 1, "type": feed_mod.VISIBILITY_EVENT, "thread_id": T, "workspace_id": W, "shared": False}
    for user in (MAYA, LEE):
        assert await (await opened(user)).on_event(dict(event)) == [event]


@pytest.fixture
def channel(monkeypatch):
    sent: list[tuple[str, dict]] = []

    async def publish(name, message):
        import json

        sent.append((name, json.loads(message)))

    cache = SimpleNamespace(enabled=True, client=SimpleNamespace(publish=publish))
    monkeypatch.setattr(feed_mod, "get_cache_client", lambda: cache)
    return sent


async def test_a_private_threads_event_names_its_starter_once_when_published(channel):
    starter = AsyncMock(return_value=MAYA)
    with patch.object(thread_reach, "private_to", starter):
        await feed_mod.publish_user_event(None, _run_event(), shared_org=ORG)
        await feed_mod.publish_user_event(None, _run_event(type="thread_visibility"), shared_org=ORG)
        await feed_mod.publish_user_event(MAYA, _run_event())
    (_, private), (_, flip), (_, own) = channel
    assert private[feed_mod.PRIVATE_TO_KEY] == MAYA
    # A flip goes to everyone, and a person's own channel needs no address.
    assert feed_mod.PRIVATE_TO_KEY not in flip and feed_mod.PRIVATE_TO_KEY not in own
    starter.assert_awaited_once_with(T)


async def test_an_unreadable_starter_drops_the_event(channel):
    with patch.object(thread_reach, "private_to", AsyncMock(side_effect=RuntimeError("down"))):
        await feed_mod.publish_user_event(None, _run_event(), shared_org=ORG)
        with pytest.raises(RuntimeError):
            await feed_mod.publish_user_event_strict(None, _run_event(), shared_org=ORG)
    assert channel == []


async def test_a_deleted_private_thread_is_told_to_its_starter_alone(opened, channel):
    starter = AsyncMock()
    with patch.object(thread_reach, "private_to", starter):
        await feed_mod.publish_thread_deleted(
            user_id=MAYA, thread_id=T, workspace_id=W, shared_org=ORG, private_to=MAYA
        )
    starter.assert_not_called()
    ((_, event),) = channel
    (frame,) = await (await opened(MAYA)).on_event(dict(event))
    assert feed_mod.PRIVATE_TO_KEY not in frame
    assert await (await opened(LEE)).on_event(dict(event)) == []


# --- artifacts --------------------------------------------------------------


async def test_a_bound_thread_out_of_reach_is_left_out_of_the_artifact():
    from src.server.app.artifacts import records

    artifact = SimpleNamespace(
        artifact_id="a", code="c", workspace_id=W, path="x.md", name="x.md", shared=False,
        shared_at=None, keep_updated=False, kept=False, latest_seq=0, shared_seq=0,
        bound_thread_id=T, created_at="2026-11-16T00:00:00Z", legacy_refused=None, audience=None,
    )
    counts = SimpleNamespace(open=0, unread=0)
    with patch.object(records.artifacts_db, "comment_counts", AsyncMock(return_value=counts)), \
         patch.object(records, "offered", AsyncMock(return_value=["organization"])), \
         patch.object(thread_reach, "hidden", AsyncMock(side_effect=lambda ids, u: set(ids) if u == LEE else set())) as hidden:
        assert (await records._response(artifact, "shared-0000", LEE)).bound_thread_id is None
        assert (await records._response(artifact, "shared-0000", MAYA)).bound_thread_id == T
        hidden.reset_mock()
        # The record's owner reading their own: nothing asked.
        assert (await records._response(artifact, "alice", "alice")).bound_thread_id == T
        hidden.assert_not_called()
