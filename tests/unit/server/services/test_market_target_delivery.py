"""The ``market_target`` outbox executor's routing, against faked ledgers.

Each claim reads the run ledger and takes one route: nothing owed (ack), a
live turn or one awaiting the user (park until it ends, the live turn taking
the notice in through the report inbox meanwhile), or an idle thread (wake
turn, then stamp). A wake that is not admitted holds the job for the thread's
next turn instead of dropping it. The stamp on the target row is the arbiter,
so these tests pin who stamps and when.
"""

from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.hook_outbox import MAX_ATTEMPTS
from src.server.services.market_targets import delivery
from src.server.services.market_targets.delivery import (
    execute_market_target,
    request_key,
)
from tests.unit.server.handlers.chat.redis_fakes import FakeCache

MT_DB = "src.server.database.market_targets"
TL_DB = "src.server.database.runs.lifecycle"
OUTBOX = "src.server.database.runs.outbox"
CONV = "src.server.database.conversation"
WS_DB = "src.server.database.workspace"


def _target(**overrides: Any) -> dict:
    row = {
        "target_id": "tg-1",
        "conversation_thread_id": "t-1",
        "user_id": "u-1",
        "symbol": "AAPL",
        "market": "stock",
        "kind": "above",
        "value": 230.0,
        "pct_from": None,
        "armed_price": 222.1,
        "status": "fired",
        "expires_at": datetime.now(timezone.utc) + timedelta(hours=3),
        "fired_at": datetime(2026, 10, 12, 14, 31, tzinfo=timezone.utc),
        "fired_price": 230.45,
        "hit_delivered_at": None,
        "warn_delivered_at": None,
    }
    row.update(overrides)
    return row


def _job(notice: str = "hit", attempts: int = 1, *, held: bool = False) -> dict:
    payload = {
        "target_id": "tg-1",
        "notice": notice,
        "thread_id": "t-1",
        "user_id": "u-1",
    }
    if held:
        payload["exhausted"] = True
    return {
        "hook_outbox_id": "job-1",
        "hook_type": "market_target",
        "attempts": attempts,
        "conversation_thread_id": "t-1",
        "payload": payload,
    }


_SAME = object()


@dataclass
class Env:
    target: dict | None
    # The thread's latest run as each read after the claim's first sees it.
    latest: list = field(default_factory=lambda: [{"status": "completed"}])
    # What the claim's first read sees; the first of ``latest`` unless set.
    at_claim: Any = _SAME
    # A run, None, or a list of either for successive request-key lookups.
    earlier: Any = None
    post_outcomes: list = field(default_factory=lambda: [("dispatched", "wake-1")])
    defer_status: str = "pending"
    cache: FakeCache = field(default_factory=FakeCache)
    mark: AsyncMock = field(default_factory=lambda: AsyncMock(return_value=True))
    post: AsyncMock | None = None
    defer: AsyncMock | None = None
    release: AsyncMock = field(default_factory=lambda: AsyncMock(return_value=1))
    exhaust: AsyncMock = field(default_factory=lambda: AsyncMock(return_value=True))
    wake: AsyncMock = field(default_factory=AsyncMock)
    drainer: MagicMock = field(default_factory=MagicMock)
    thread: dict = field(default_factory=lambda: {"workspace_id": "ws-1", "archived_at": None})
    workspace: dict | None = field(default_factory=lambda: {"workspace_id": "ws-1"})
    feature_on: bool = True


def _cycle(values: list):
    """Return each value in turn, then repeat the last."""
    it = iter(values)
    last = [values[-1]]

    async def _next(*_a, **_k):
        try:
            last[0] = next(it)
        except StopIteration:
            pass
        return last[0]

    return _next


@contextlib.asynccontextmanager
async def _conn():
    yield MagicMock()


@contextlib.contextmanager
def _env(env: Env):
    async def _get_target(_tid):
        return env.target

    env.post = env.post or AsyncMock(side_effect=_cycle(env.post_outcomes))
    env.defer = env.defer or AsyncMock(return_value=env.defer_status)
    later_reads = _cycle(env.latest)
    claim_read = [True]

    async def _latest(*_a, **_k):
        if claim_read[0]:
            claim_read[0] = False
            return env.latest[0] if env.at_claim is _SAME else env.at_claim
        return await later_reads()

    earlier = env.earlier if isinstance(env.earlier, list) else [env.earlier]
    patches = (
        patch(f"{MT_DB}.get_target", new=AsyncMock(side_effect=_get_target)),
        patch(f"{MT_DB}.mark_delivered", new=env.mark),
        patch(f"{TL_DB}.find_run_by_request_key", new=AsyncMock(side_effect=_cycle(earlier))),
        patch(f"{TL_DB}.get_latest_attempt", new=AsyncMock(side_effect=_latest)),
        patch(f"{CONV}.get_thread_by_id", new=AsyncMock(return_value=env.thread)),
        patch(f"{WS_DB}.get_workspace", new=AsyncMock(return_value=env.workspace)),
        patch(
            "src.server.services.features.user_feature_enabled",
            new=AsyncMock(return_value=env.feature_on),
        ),
        patch(f"{CONV}.get_thread_auth_meta", new=AsyncMock(return_value={"user_id": "u-1"})),
        patch(f"{OUTBOX}.extend_job_lease", new=AsyncMock(return_value=True)),
        patch(f"{OUTBOX}.defer_claimed_job", new=env.defer),
        patch(f"{OUTBOX}.release_deferred_notifications", new=env.release),
        patch(f"{OUTBOX}.exhaust_alert", new=env.exhaust),
        patch("src.server.database.pool.get_db_connection", new=_conn),
        patch(
            "src.server.services.report_back.notify_turn.post_notification_turn",
            new=env.post,
        ),
        patch("src.server.services.report_back.handoff.wake.publish_wake", new=env.wake),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=env.cache),
        patch(
            "src.server.services.hook_outbox.HookOutboxDrainer.get_instance",
            return_value=env.drainer,
        ),
        patch.object(delivery, "_BUSY_RETRY_DELAY", 0),
        patch.object(delivery, "last_quote", new=AsyncMock(return_value=None)),
    )
    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        yield env


@pytest.mark.asyncio
async def test_delivered_notice_acks_without_touching_the_thread():
    env = Env(target=_target(hit_delivered_at=datetime.now(timezone.utc)))
    with _env(env):
        await execute_market_target(_job())

    env.post.assert_not_awaited()
    env.defer.assert_not_awaited()
    env.mark.assert_not_awaited()


@pytest.mark.asyncio
async def test_gone_target_acks():
    env = Env(target=None)
    with _env(env):
        await execute_market_target(_job())

    env.post.assert_not_awaited()
    env.defer.assert_not_awaited()


@pytest.mark.asyncio
async def test_warning_for_a_target_no_longer_armed_acks():
    env = Env(target=_target(status="cancelled"))
    with _env(env):
        await execute_market_target(_job("expiring"))

    env.post.assert_not_awaited()
    env.defer.assert_not_awaited()


def _parked_without_a_wake(env: Env) -> None:
    env.defer.assert_awaited_once()
    env.post.assert_not_awaited()
    env.mark.assert_not_awaited()  # the live turn or the later wake stamps it
    env.wake.assert_not_awaited()
    env.release.assert_not_awaited()
    assert env.cache.client.lists == {}


@pytest.mark.asyncio
async def test_live_turn_parks_the_job():
    env = Env(target=_target(), latest=[{"status": "in_progress"}])
    with _env(env):
        await execute_market_target(_job())

    _parked_without_a_wake(env)


@pytest.mark.asyncio
async def test_interrupted_thread_parks_the_job():
    env = Env(target=_target(), latest=[{"status": "interrupted"}])
    with _env(env):
        await execute_market_target(_job())

    _parked_without_a_wake(env)


@pytest.mark.asyncio
async def test_park_released_when_the_turn_ended_before_the_park():
    env = Env(
        target=_target(),
        # The pre-park read sees the turn; the post-park read sees it over.
        latest=[{"status": "in_progress"}, {"status": "completed"}],
    )
    with _env(env):
        await execute_market_target(_job())

    env.defer.assert_awaited_once()
    env.release.assert_awaited_once()
    assert env.release.await_args.args[1] == "t-1"
    env.drainer.nudge.assert_called_once()


@pytest.mark.asyncio
async def test_idle_thread_wakes_then_stamps_the_wake_run():
    env = Env(target=_target())
    with _env(env):
        await execute_market_target(_job())

    env.post.assert_awaited_once()
    kwargs = env.post.await_args.kwargs
    body = kwargs["body"]
    assert body["query_type"] == "market_target"
    assert body["workspace_id"] == "ws-1"
    assert body["request_key"] == request_key("job-1")
    (message,) = body["messages"]
    assert message["role"] == "user"
    assert message["content"].startswith("<market-alert>")
    assert kwargs["busy_fails_fast"] is True
    assert kwargs["refusal_fails_fast"] is True
    assert body["market_target"]["target_id"] == "tg-1"
    assert body["market_target"]["notice"] == "hit"
    assert body["market_target"]["fired_price"] == 230.45
    json.dumps(body)  # the POST body must serialize as is
    env.mark.assert_awaited_once_with("tg-1", "hit", "wake-1")
    assert env.wake.await_args.kwargs["run_id"] == "wake-1"
    env.defer.assert_not_awaited()


@pytest.mark.asyncio
async def test_earlier_wake_is_adopted_not_reposted():
    env = Env(
        target=_target(),
        earlier={"conversation_response_id": "wake-0", "conversation_thread_id": "t-1"},
    )
    with _env(env):
        await execute_market_target(_job(attempts=2))

    env.post.assert_not_awaited()
    env.mark.assert_awaited_once_with("tg-1", "hit", "wake-0")
    assert env.wake.await_args.kwargs["run_id"] == "wake-0"


@pytest.mark.asyncio
async def test_a_run_holding_the_key_on_another_thread_is_not_adopted():
    """Request keys are unique across threads; that run never showed this
    thread the notice."""
    env = Env(
        target=_target(),
        earlier={"conversation_response_id": "run-x", "conversation_thread_id": "t-other"},
    )
    with _env(env):
        await execute_market_target(_job())

    env.post.assert_awaited_once()
    env.mark.assert_awaited_once_with("tg-1", "hit", "wake-1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "env_kwargs",
    [
        {"feature_on": False},
        {"thread": {"workspace_id": "ws-1", "archived_at": datetime(2026, 10, 12, tzinfo=timezone.utc)}},
    ],
    ids=["market-watch-off", "archived-thread"],
)
async def test_a_notice_nobody_would_see_is_dropped(env_kwargs):
    env = Env(target=_target(), **env_kwargs)
    with _env(env):
        await execute_market_target(_job())

    env.post.assert_not_awaited()
    env.defer.assert_not_awaited()
    env.mark.assert_not_awaited()


@pytest.mark.asyncio
async def test_busy_answer_parks_on_the_next_round_instead_of_posting_again():
    env = Env(
        target=_target(),
        latest=[{"status": "completed"}, {"status": "in_progress"}],
        post_outcomes=[("busy", None)],
    )
    with _env(env):
        await execute_market_target(_job())

    env.post.assert_awaited_once()
    env.defer.assert_awaited_once()
    env.mark.assert_not_awaited()


@pytest.mark.asyncio
async def test_refusals_past_the_cap_with_retries_left_nack_into_backoff():
    env = Env(target=_target(), post_outcomes=[("cap", None)])
    with _env(env), pytest.raises(RuntimeError):
        await execute_market_target(_job())

    env.mark.assert_not_awaited()
    env.exhaust.assert_not_awaited()


def _held_for_the_next_turn(env: Env, attempts: int) -> None:
    env.exhaust.assert_awaited_once_with("job-1", attempts=attempts)
    env.mark.assert_not_awaited()
    env.wake.assert_not_awaited()
    env.defer.assert_not_awaited()
    # No turn ended during the claim, so nothing buys it another look.
    env.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_wake_refused_through_the_last_attempt_is_held_not_dropped():
    env = Env(target=_target(), post_outcomes=[("cap", None)])
    with _env(env):
        await execute_market_target(_job(attempts=MAX_ATTEMPTS))

    _held_for_the_next_turn(env, MAX_ATTEMPTS)


@pytest.mark.asyncio
async def test_a_held_alert_refused_again_is_held_again_without_new_retries():
    """A turn's ending bought the held job one more look, not a fresh round
    of backoff: each further try waits on another turn's ending."""
    env = Env(target=_target(), post_outcomes=[("cap", None)])
    with _env(env):
        await execute_market_target(_job(attempts=2, held=True))

    _held_for_the_next_turn(env, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["refused", "drop"])
async def test_a_refusal_that_will_not_clear_holds_at_once(outcome):
    env = Env(target=_target(), post_outcomes=[(outcome, None)])
    with _env(env):
        await execute_market_target(_job(attempts=1))

    env.post.assert_awaited_once()
    _held_for_the_next_turn(env, 1)


@pytest.mark.asyncio
async def test_a_hold_whose_fence_was_lost_stands_down():
    env = Env(
        target=_target(),
        post_outcomes=[("refused", None)],
        exhaust=AsyncMock(return_value=False),
    )
    with _env(env):
        await execute_market_target(_job())

    env.exhaust.assert_awaited_once()
    env.mark.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_released_held_alert_wakes_a_thread_that_now_admits_it():
    env = Env(target=_target())
    with _env(env):
        await execute_market_target(_job(attempts=MAX_ATTEMPTS + 1, held=True))

    env.mark.assert_awaited_once_with("tg-1", "hit", "wake-1")
    env.exhaust.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_released_held_alert_finding_a_turn_parks_for_it():
    """The park at the ceiling holds it again (``defer_claimed_job``), for
    the running turn to take in."""
    env = Env(
        target=_target(),
        latest=[{"status": "in_progress"}],
        defer_status="exhausted",
    )
    with _env(env):
        await execute_market_target(_job(attempts=MAX_ATTEMPTS + 1, held=True))

    _parked_without_a_wake(env)
    env.exhaust.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "env_kwargs",
    [
        {"target": _target(hit_delivered_at=datetime.now(timezone.utc))},
        {"target": _target(), "feature_on": False},
        {
            "target": _target(),
            "thread": {
                "workspace_id": "ws-1",
                "archived_at": datetime(2026, 10, 12, tzinfo=timezone.utc),
            },
        },
    ],
    ids=["delivered", "market-watch-off", "archived-thread"],
)
async def test_a_released_held_alert_nobody_would_see_is_closed(env_kwargs):
    env = Env(**env_kwargs)
    with _env(env):
        await execute_market_target(_job(attempts=MAX_ATTEMPTS + 1, held=True))

    env.post.assert_not_awaited()
    env.exhaust.assert_not_awaited()
    env.defer.assert_not_awaited()


@pytest.mark.asyncio
async def test_superseded_wake_acks_unstamped():
    env = Env(target=_target(), post_outcomes=[("superseded", None)])
    with _env(env):
        await execute_market_target(_job())

    env.mark.assert_not_awaited()
    env.defer.assert_not_awaited()


def _run(run_id: str, status: str) -> dict:
    return {"conversation_response_id": run_id, "status": status}


_WAKE_0 = {"conversation_response_id": "wake-0", "conversation_thread_id": "t-1"}


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["cap", "refused", "drop"])
async def test_a_wake_admitted_unanswered_is_settled_to_its_run_not_held(outcome):
    """The POST timed out but was admitted: the run holding the request key
    already shows the notice, so the claim stamps it there instead of
    holding the job for a turn after it."""
    env = Env(target=_target(), post_outcomes=[(outcome, None)], earlier=[None, _WAKE_0])
    with _env(env):
        await execute_market_target(_job(attempts=MAX_ATTEMPTS))

    env.exhaust.assert_not_awaited()
    env.mark.assert_awaited_once_with("tg-1", "hit", "wake-0")
    assert env.wake.await_args.kwargs["run_id"] == "wake-0"


@pytest.mark.asyncio
async def test_a_wake_dispatched_without_a_run_id_is_found_by_its_key():
    env = Env(
        target=_target(), post_outcomes=[("dispatched", None)], earlier=[None, _WAKE_0]
    )
    with _env(env):
        await execute_market_target(_job(attempts=MAX_ATTEMPTS))

    env.mark.assert_awaited_once_with("tg-1", "hit", "wake-0")


@pytest.mark.asyncio
async def test_a_wake_dispatched_without_a_run_id_not_found_yet_is_retried():
    """Its START may not have committed; the next claim, or the live turn's
    inbox if the nack holds it, finds the run by its key."""
    env = Env(target=_target(), post_outcomes=[("dispatched", None)])
    with _env(env), pytest.raises(RuntimeError):
        await execute_market_target(_job())

    env.mark.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("at_claim", "after_hold"),
    [
        (_run("r-0", "completed"), _run("r-1", "completed")),
        (_run("r-0", "completed"), _run("r-1", "error")),
        # Running when the claim began, over by the time it looked.
        (_run("r-0", "in_progress"), _run("r-0", "completed")),
        (None, _run("r-1", "cancelled")),
    ],
    ids=["new-turn", "new-turn-failed", "turn-ended-before-the-post", "first-turn"],
)
async def test_a_turn_that_ended_during_the_claim_releases_the_hold(at_claim, after_hold):
    """Its finalize found the job claimed and released nothing for it, so the
    claim releases the thread's parked jobs itself after holding it."""
    env = Env(
        target=_target(),
        at_claim=at_claim,
        latest=[_run("r-0", "completed"), after_hold],
        post_outcomes=[("cap", None)],
    )
    with _env(env):
        await execute_market_target(_job(attempts=MAX_ATTEMPTS))

    env.exhaust.assert_awaited_once_with("job-1", attempts=MAX_ATTEMPTS)
    env.release.assert_awaited_once()
    assert env.release.await_args.args[1] == "t-1"
    env.drainer.nudge.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["in_progress", "interrupted"])
async def test_a_turn_live_at_the_hold_is_left_to_release_it(status):
    """Its own finalize, or the resume's, releases the held job."""
    env = Env(
        target=_target(),
        at_claim=_run("r-0", "completed"),
        latest=[_run("r-0", "completed"), _run("r-1", status)],
        post_outcomes=[("refused", None)],
    )
    with _env(env):
        await execute_market_target(_job())

    env.exhaust.assert_awaited_once()
    env.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_released_alert_refused_again_with_no_new_turn_stays_held():
    """The look the release bought is spent: the latest run is the one that
    ended before this claim began, so nothing is released and it cannot
    loop."""
    ended = _run("r-1", "completed")
    env = Env(
        target=_target(),
        at_claim=ended,
        latest=[ended, ended],
        post_outcomes=[("refused", None)],
    )
    with _env(env):
        await execute_market_target(_job(attempts=MAX_ATTEMPTS + 1, held=True))

    _held_for_the_next_turn(env, MAX_ATTEMPTS + 1)
    env.drainer.nudge.assert_not_called()


@pytest.mark.asyncio
async def test_a_hold_whose_fence_was_lost_releases_nothing():
    env = Env(
        target=_target(),
        at_claim=_run("r-0", "completed"),
        latest=[_run("r-0", "completed"), _run("r-1", "completed")],
        post_outcomes=[("refused", None)],
        exhaust=AsyncMock(return_value=False),
    )
    with _env(env):
        await execute_market_target(_job())

    env.release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_park_at_the_ceiling_is_released_by_the_re_read():
    """The executor tells the shared park its job stays parked at the
    ceiling, so a turn ending just before the park still releases it."""
    env = Env(
        target=_target(),
        latest=[_run("r-0", "in_progress"), _run("r-0", "completed")],
        defer_status="exhausted",
    )
    with _env(env):
        await execute_market_target(_job(attempts=MAX_ATTEMPTS + 1, held=True))

    env.defer.assert_awaited_once()
    env.release.assert_awaited_once()
    env.post.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_notice_on_a_thread_whose_workspace_was_deleted_is_dropped():
    env = Env(target=_target(), workspace=None)
    with _env(env):
        await execute_market_target(_job(attempts=MAX_ATTEMPTS + 1, held=True))

    env.post.assert_not_awaited()
    env.defer.assert_not_awaited()
    env.exhaust.assert_not_awaited()
    env.mark.assert_not_awaited()
