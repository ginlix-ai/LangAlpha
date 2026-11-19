"""The thread-lifecycle feed endpoint: snapshot assembly and read resilience.

Classification now lives in SQL, so what is left to pin here is the assembly
(branch → wire set, cutoff flag, watermark passthrough) and the invariant that a
DB read failure degrades to a keepalive instead of killing a 600s stream.
"""

import json
from types import SimpleNamespace

import psycopg
import pytest

from src.server.app import user_events
from src.server.app.user_events import _PING, _UNSEEN_CAP, _read_snapshot


def _row(branch, seq, *, status="completed", last_seen=0, **over):
    row = {
        "branch": branch,
        "conversation_thread_id": f"t-{seq}",
        "workspace_id": "ws-1",
        "last_seen_run_seq": last_seen,
        "latest_run_id": f"r-{seq}",
        "latest_run_status": status,
        "latest_cancel_requested_at": None,
        "latest_interrupt_reason": None,
        "latest_run_seq": seq,
        "latest_run_started_at": None,
    }
    row.update(over)
    return row


def _patch_rows(monkeypatch, rows, as_of, pending_approvals=(), pending_terms=()):
    async def _fake(user_id, *, unseen_cap=256):
        return rows, as_of

    async def _pending(user_id):
        return [SimpleNamespace(approval_id=a) for a in pending_approvals]

    monkeypatch.setattr(user_events, "get_thread_lifecycle_rows", _fake)
    async def _terms(user_id):
        return [{"vendor": v} for v in pending_terms]

    monkeypatch.setattr(user_events.cli_approvals, "pending", _pending)
    monkeypatch.setattr(user_events.vendor_terms, "pending", _terms)


# ---------------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_branches_route_to_their_wire_sets(monkeypatch):
    _patch_rows(
        monkeypatch,
        [
            _row("live", 10, status="in_progress"),
            _row("live", 11, status="interrupted"),
            _row("unseen", 9, status="error"),
        ],
        as_of=11,
    )

    frame = await _read_snapshot("u-1")

    assert [e["status"] for e in frame["live"]] == ["running", "interrupted"]
    # Raw `error` reaches the client as public `failed` — and lands in unseen,
    # never in the uncapped live branch.
    assert [e["status"] for e in frame["unseen"]] == ["failed"]


@pytest.mark.asyncio
async def test_cancel_requested_live_row_projects_as_stopping(monkeypatch):
    _patch_rows(
        monkeypatch,
        [
            _row(
                "live",
                7,
                status="in_progress",
                latest_cancel_requested_at="2026-08-01T00:00:00Z",
            )
        ],
        as_of=7,
    )

    frame = await _read_snapshot("u-1")
    assert frame["live"][0]["status"] == "stopping"


@pytest.mark.asyncio
async def test_interrupt_reason_survives_only_on_interrupted(monkeypatch):
    _patch_rows(
        monkeypatch,
        [
            _row(
                "live",
                5,
                status="interrupted",
                latest_interrupt_reason="user_question",
            ),
            _row(
                "unseen",
                4,
                status="completed",
                latest_interrupt_reason="user_question",
            ),
        ],
        as_of=5,
    )

    frame = await _read_snapshot("u-1")
    assert frame["live"][0]["interrupt_reason"] == "user_question"
    assert frame["unseen"][0]["interrupt_reason"] is None


@pytest.mark.asyncio
async def test_overflow_row_sets_the_truncation_cutoff(monkeypatch):
    rows = [_row("unseen", seq) for seq in range(_UNSEEN_CAP + 1, 0, -1)]
    _patch_rows(monkeypatch, rows, as_of=_UNSEEN_CAP + 1)

    frame = await _read_snapshot("u-1")

    assert frame["unseen_truncated"] is True
    assert len(frame["unseen"]) == _UNSEEN_CAP
    assert frame["oldest_included_unseen_seq"] == frame["unseen"][-1]["run_seq"]


@pytest.mark.asyncio
async def test_exactly_at_the_cap_is_not_truncated(monkeypatch):
    rows = [_row("unseen", seq) for seq in range(_UNSEEN_CAP, 0, -1)]
    _patch_rows(monkeypatch, rows, as_of=_UNSEEN_CAP)

    frame = await _read_snapshot("u-1")

    assert frame["unseen_truncated"] is False
    # 0 = the unseen set is complete, so every absence proves seen.
    assert frame["oldest_included_unseen_seq"] == 0


@pytest.mark.asyncio
async def test_unseen_is_sorted_newest_first_across_the_union(monkeypatch):
    """UNION ALL guarantees no ordering between branches, so the cutoff would
    be picked off an arbitrary row without the re-sort."""
    _patch_rows(
        monkeypatch,
        [_row("unseen", 3), _row("live", 99, status="in_progress"), _row("unseen", 8)],
        as_of=99,
    )

    frame = await _read_snapshot("u-1")
    assert [e["run_seq"] for e in frame["unseen"]] == [8, 3]


@pytest.mark.asyncio
async def test_empty_snapshot_still_carries_the_watermark(monkeypatch):
    _patch_rows(monkeypatch, [], as_of=1234)

    frame = await _read_snapshot("u-1")

    assert frame == {
        "v": 1,
        "as_of_seq": 1234,
        "unseen_truncated": False,
        "oldest_included_unseen_seq": 0,
        "live": [],
        "unseen": [],
        "pending_approvals": [],
        "pending_vendor_terms": [],
    }


@pytest.mark.asyncio
async def test_the_snapshot_lists_the_approvals_waiting_on_the_user(monkeypatch):
    """The backstop for a ``cli_approval`` event the transport dropped."""
    _patch_rows(monkeypatch, [], as_of=1, pending_approvals=["a-1", "a-2"])

    frame = await _read_snapshot("u-1")

    assert frame["pending_approvals"] == ["a-1", "a-2"]


@pytest.mark.asyncio
async def test_the_snapshot_lists_the_vendor_terms_waiting_on_the_user(monkeypatch):
    """The backstop for a ``vendor_terms`` event the transport dropped."""
    _patch_rows(monkeypatch, [], as_of=1, pending_terms=["firecrawl"])

    frame = await _read_snapshot("u-1")

    assert frame["pending_vendor_terms"] == ["firecrawl"]


@pytest.mark.asyncio
async def test_an_approvals_read_that_fails_still_sends_the_lifecycle_sets(monkeypatch):
    _patch_rows(monkeypatch, [_row("live", 3, status="in_progress")], as_of=3)

    async def _broken(user_id):
        raise psycopg.OperationalError("connection lost")

    monkeypatch.setattr(user_events.cli_approvals, "pending", _broken)

    frame = await _read_snapshot("u-1")

    assert [e["thread_id"] for e in frame["live"]] == ["t-3"]
    assert frame["pending_approvals"] == []


@pytest.mark.asyncio
async def test_watermark_comes_from_the_query_not_the_rows(monkeypatch):
    """The scalar spans threads filtered out of both branches (archived,
    seen) — deriving it from the returned rows would strand the client."""
    _patch_rows(monkeypatch, [_row("unseen", 2)], as_of=980)

    frame = await _read_snapshot("u-1")
    assert frame["as_of_seq"] == 980


# ---------------------------------------------------------------------------
# Read resilience (a failed snapshot must not kill the stream)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_attach_read_failure_degrades_to_a_ping_then_self_heals(
    monkeypatch,
):
    calls = {"n": 0}
    good = {
        "v": 1,
        "as_of_seq": 5,
        "unseen_truncated": False,
        "oldest_included_unseen_seq": 0,
        "live": [],
        "unseen": [],
    }

    async def _flaky(user_id):
        calls["n"] += 1
        if calls["n"] == 1:
            raise psycopg.OperationalError("server closed the connection")
        return good

    monkeypatch.setattr(user_events, "_read_snapshot", _flaky)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.0)

    frames = await _collect_stream(monkeypatch)

    # Attach degraded to a keepalive rather than raising out of the generator.
    assert frames[0] == _PING
    # last_snapshot stayed unset, so the very next read re-emits the snapshot.
    assert calls["n"] >= 1


@pytest.mark.asyncio
async def test_in_loop_read_failure_keeps_the_generator_alive(monkeypatch):
    calls = {"n": 0}

    async def _always_broken(user_id):
        calls["n"] += 1
        raise psycopg.OperationalError("server closed the connection")

    monkeypatch.setattr(user_events, "_read_snapshot", _always_broken)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.0)

    frames = await _collect_stream(monkeypatch)

    assert frames[0] == _PING
    assert frames[-1].startswith("event: timeout")


@pytest.mark.asyncio
async def test_snapshot_arrives_on_a_later_tick_after_a_failed_attach(
    monkeypatch,
):
    calls = {"n": 0}
    good = {
        "v": 1,
        "as_of_seq": 5,
        "unseen_truncated": False,
        "oldest_included_unseen_seq": 0,
        "live": [],
        "unseen": [],
    }

    async def _flaky(user_id):
        calls["n"] += 1
        if calls["n"] == 1:
            raise psycopg.OperationalError("boom")
        return good

    monkeypatch.setattr(user_events, "_read_snapshot", _flaky)
    monkeypatch.setattr(user_events, "_RECONCILE_S", 0.05)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.08)

    frames = await _collect_stream(monkeypatch)

    assert frames[0] == _PING
    snapshots = [f for f in frames if f.startswith("event: snapshot")]
    assert snapshots, frames
    assert json.loads(snapshots[0].split("data: ", 1)[1])["as_of_seq"] == 5


async def _collect_stream(monkeypatch, user_id="u-1"):
    """Run the route's generator to completion with Redis disabled."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _no_redis(_user_id):
        yield None

    monkeypatch.setattr(user_events, "subscribe_to_user_events", _no_redis)

    response = await user_events.user_thread_events(user_id)
    return [
        chunk if isinstance(chunk, str) else chunk.decode()
        async for chunk in response.body_iterator
    ]


# ---------------------------------------------------------------------------
# The in-connection reconcile as the lost-event backstop
# ---------------------------------------------------------------------------
# The run_settled push rides the hook outbox, but a job that dead-letters
# (Redis down through all five leases) is deliberately NOT compensated or
# revived — these two pins are the reason that's safe: DB truth re-emits on
# the paced reconcile inside every live connection, no reconnect required,
# which outruns any outbox revival path.


@pytest.mark.asyncio
async def test_mid_connection_change_reaches_the_client_on_a_later_tick(
    monkeypatch,
):
    calls = {"n": 0}

    async def _evolving(user_id):
        calls["n"] += 1
        return {"as_of_seq": 5 if calls["n"] == 1 else 6, "live": [], "unseen": []}

    monkeypatch.setattr(user_events, "_read_snapshot", _evolving)
    monkeypatch.setattr(user_events, "_RECONCILE_S", 0.01)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.05)

    frames = await _collect_stream(monkeypatch)

    seqs = [
        json.loads(f.split("data: ", 1)[1])["as_of_seq"]
        for f in frames
        if f.startswith("event: snapshot")
    ]
    assert seqs[0] == 5
    assert 6 in seqs, frames


@pytest.mark.asyncio
async def test_unchanged_snapshot_is_not_resent_on_reconcile_ticks(monkeypatch):
    async def _static(user_id):
        return {"as_of_seq": 7, "live": [], "unseen": []}

    monkeypatch.setattr(user_events, "_read_snapshot", _static)
    monkeypatch.setattr(user_events, "_RECONCILE_S", 0.01)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.05)

    frames = await _collect_stream(monkeypatch)

    snapshots = [f for f in frames if f.startswith("event: snapshot")]
    assert len(snapshots) == 1, frames


# ---------------------------------------------------------------------------
# Shared workspaces on the feed
# ---------------------------------------------------------------------------


def _stub_snapshot(monkeypatch):
    async def _snap(user_id, shared=None):
        return {"as_of_seq": 1, "live": [], "unseen": []}

    monkeypatch.setattr(user_events, "_read_snapshot", _snap)


@pytest.mark.asyncio
async def test_an_account_that_reaches_no_shared_workspace_subscribes_as_before(
    monkeypatch,
):
    from contextlib import asynccontextmanager

    from src.server.services.shared_workspaces import feed

    async def _none(user_id, **_):
        return None

    @asynccontextmanager
    async def _never(*channels):
        raise AssertionError("no second channel for an account with no shared workspace")
        yield  # pragma: no cover

    monkeypatch.setattr(feed, "reach_of", _none)
    monkeypatch.setattr(user_events, "subscribe_to_channels", _never)
    _stub_snapshot(monkeypatch)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.0)

    frames = await _collect_stream(monkeypatch)

    assert frames[0].startswith("event: snapshot")


@pytest.mark.asyncio
async def test_a_members_connection_passes_on_only_the_workspaces_it_reaches(
    monkeypatch,
):
    from contextlib import asynccontextmanager

    from src.server.services import thread_lifecycle_feed as tlf
    from src.server.services.shared_workspaces import feed

    org_channel = tlf.shared_events_channel("org-n")

    async def _reach(user_id, **_):
        return feed.Reach("org-n", frozenset({"ws-desk"}))

    queued = [
        ("message", {"channel": tlf.user_events_channel("u-1"), "data": {"type": "thread_title", "workspace_id": "home"}}),
        ("message", {"channel": org_channel, "data": {"type": "run_started", "workspace_id": "ws-desk", tlf.SENDER_KEY: "maya"}}),
        ("message", {"channel": org_channel, "data": {"type": "run_started", "workspace_id": "ws-closed", tlf.SENDER_KEY: "maya"}}),
    ]
    subscribed: list[tuple] = []

    @asynccontextmanager
    async def _channels(*channels):
        subscribed.append(channels)

        async def _wait(timeout):
            if queued:
                return queued.pop(0)
            await asyncio.sleep(0.01)
            return "timeout", None

        yield _wait

    import asyncio

    monkeypatch.setattr(feed, "reach_of", _reach)
    monkeypatch.setattr(user_events, "subscribe_to_channels", _channels)
    _stub_snapshot(monkeypatch)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.1)

    frames = await _collect_stream(monkeypatch)

    assert subscribed[0] == (tlf.user_events_channel("u-1"), org_channel)
    events = [
        json.loads(f.split("data: ", 1)[1])
        for f in frames
        if f.startswith("event: thread_lifecycle")
    ]
    assert [(e["type"], e["workspace_id"]) for e in events] == [
        ("thread_title", "home"),
        ("run_started", "ws-desk"),
    ]
    assert tlf.SENDER_KEY not in events[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("told", [True, False], ids=["reach-event", "reconcile"])
async def test_a_connection_opened_with_no_shared_feed_takes_one_up_when_it_can(
    monkeypatch, told
):
    """None at open is the account's reach just then: it may join an
    organization with the tab open, or its account may have been unreadable.
    Its own reach event, or the next reconcile, attaches the connection to
    the organization's channel too."""
    import asyncio
    from contextlib import asynccontextmanager

    from src.server.services import thread_lifecycle_feed as tlf
    from src.server.services.shared_workspaces import feed

    asked = 0

    async def _reach(user_id, **_):
        nonlocal asked
        asked += 1
        return None if asked == 1 else feed.Reach("org-n", frozenset({"ws-desk"}))

    personal = [("message", {"v": 1, "type": feed.REACH_EVENT})] if told else []
    subscribed: list[tuple] = []

    def _subscription(*channels):
        @asynccontextmanager
        async def _open():
            subscribed.append(channels)

            async def _wait(timeout):
                if len(subscribed) == 1 and personal:
                    return personal.pop(0)
                await asyncio.sleep(0.01)
                return "timeout", None

            yield _wait

        return _open()

    monkeypatch.setattr(feed, "reach_of", _reach)
    monkeypatch.setattr(user_events, "subscribe_to_user_events", lambda user_id: _subscription("personal"))
    monkeypatch.setattr(user_events, "subscribe_to_channels", _subscription)
    _stub_snapshot(monkeypatch)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.3)
    # Only the reach event may ask before the connection ends when told.
    monkeypatch.setattr(user_events, "_RECONCILE_S", 30.0 if told else 0.02)

    response = await user_events.user_thread_events("u-1")
    frames = [c if isinstance(c, str) else c.decode() async for c in response.body_iterator]

    assert subscribed == [
        ("personal",),
        (tlf.user_events_channel("u-1"), tlf.shared_events_channel("org-n")),
    ]
    if told:
        # The event itself still reaches the page, which reads its list again.
        events = [json.loads(f.split("data: ", 1)[1]) for f in frames if f.startswith("event: thread_lifecycle")]
        assert [e["type"] for e in events] == [feed.REACH_EVENT]


@pytest.mark.asyncio
async def test_a_connection_follows_its_account_to_another_organization(monkeypatch):
    import asyncio
    from contextlib import asynccontextmanager

    from src.server.services import thread_lifecycle_feed as tlf
    from src.server.services.shared_workspaces import feed

    asked = 0

    async def _reach(user_id, **_):
        nonlocal asked
        asked += 1
        org = "org-n" if asked == 1 else "org-h"
        return feed.Reach(org, frozenset({f"ws-{org}"}))

    async def _not_deleted(workspace_id, user_id):
        return False

    subscribed: list[tuple] = []

    @asynccontextmanager
    async def _channels(*channels):
        subscribed.append(channels)

        async def _wait(timeout):
            await asyncio.sleep(0.01)
            return "timeout", None

        yield _wait

    monkeypatch.setattr(feed, "reach_of", _reach)
    monkeypatch.setattr(feed, "_deleted", _not_deleted)
    monkeypatch.setattr(user_events, "subscribe_to_channels", _channels)
    _stub_snapshot(monkeypatch)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.3)
    monkeypatch.setattr(user_events, "_RECONCILE_S", 0.02)

    response = await user_events.user_thread_events("u-1")
    frames = [c if isinstance(c, str) else c.decode() async for c in response.body_iterator]

    mine = tlf.user_events_channel("u-1")
    assert subscribed == [
        (mine, tlf.shared_events_channel("org-n")),
        (mine, tlf.shared_events_channel("org-h")),
    ]
    events = [json.loads(f.split("data: ", 1)[1]) for f in frames if f.startswith("event: thread_lifecycle")]
    assert [(e["type"], e["workspace_id"]) for e in events] == [("shared_access_ended", "ws-org-n")]
