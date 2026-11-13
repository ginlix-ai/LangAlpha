"""What a settled run tells the user: what its news says, the desktop notice
its feed is told (``user_notification``), who of their phones gets the push,
and what APNs' answers do to the devices and the job."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from src.server.contracts.notices import Notice
from src.server.services import notices
from src.server.services.push import notify
from src.server.services.push.apns import ApnsOutcome, ApnsResult

MOD = "src.server.services.push.notify"
REGISTERED = datetime(2026, 11, 1, tzinfo=timezone.utc)


def _payload(**kw):
    payload = {
        "user_id": "u-1",
        "thread_id": "thread-1",
        "workspace_id": "ws-1",
        "run_id": "run-1",
        "run_seq": 5,
        "status": "completed",
        "interrupt_reason": None,
    }
    payload.update(kw)
    return payload


def _context(**kw):
    context = {
        "title": "NVDA after earnings",
        "archived_at": None,
        "last_seen_run_seq": 4,
        "owner_id": "u-1",
        "workspace_status": "running",
        "latest_run_seq": 5,
        "run_exists": True,
        "run_metadata": {},
        "query_type": "follow_up",
        "automation_name": None,
    }
    context.update(kw)
    return context


def _device(n, environment="production", **prefs):
    return {
        "push_device_id": f"dev-{n}",
        "token": f"{n:02d}" * 32,
        "bundle_id": "com.example.app",
        "environment": environment,
        "preferences": prefs,
        "updated_at": REGISTERED,
    }


class FakeApns:
    """Answers by (token, environment); SENT unless told otherwise."""

    def __init__(self, answers=None):
        self.answers = answers or {}
        self.sent = []

    async def send(
        self, *, token, bundle_id, environment, payload, collapse_id, deadline
    ):
        self.sent.append(
            {
                "token": token,
                "environment": environment,
                "payload": payload,
                "collapse_id": collapse_id,
            }
        )
        return ApnsResult(self.answers.get((token, environment), ApnsOutcome.SENT))


@pytest.fixture
def db(monkeypatch):
    """Every DB call the executor makes, patched."""
    fakes = {
        "list_push_devices": AsyncMock(return_value=[_device(1)]),
        "get_push_context": AsyncMock(return_value=_context()),
        "delete_push_device_by_token": AsyncMock(return_value=True),
        "set_push_device_environment": AsyncMock(),
        "stamp_push_devices_sent": AsyncMock(),
    }
    for name, fake in fakes.items():
        monkeypatch.setattr(notify.push_db, name, fake)
    fakes["badge"] = AsyncMock(return_value=3)
    monkeypatch.setattr(notify, "count_threads_needing_attention", fakes["badge"])
    fakes["merge"] = AsyncMock(return_value=True)
    monkeypatch.setattr(notify.outbox_db, "add_to_job_payload_list", fakes["merge"])
    fakes["renew"] = AsyncMock(return_value=True)
    monkeypatch.setattr(notify.outbox_db, "extend_job_lease", fakes["renew"])
    fakes["feed"] = AsyncMock()
    monkeypatch.setattr(notices, "publish_user_event_strict", fakes["feed"])
    return fakes


@pytest.fixture
def apns(monkeypatch):
    fake = FakeApns()
    monkeypatch.setattr(notify, "get_apns_client", AsyncMock(return_value=fake))
    return fake


_EXECUTORS = {
    "push": notify.execute_push,
    "user_notification": notices.execute_user_notification,
}


async def _job(hook_type, payload=None, **job):
    await _EXECUTORS[hook_type](
        {
            "hook_outbox_id": "job-1",
            "hook_type": hook_type,
            "attempts": 1,
            "payload": payload or _payload(),
            **job,
        }
    )


# ---------------------------------------------------------------------------
# What it says
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload, context, kind, body",
    [
        ({}, {}, "turn", "Finished"),
        ({"status": "error"}, {}, "turn", "Stopped with an error"),
        (
            {"status": "interrupted", "interrupt_reason": "approval_required"},
            {},
            "needs_input",
            "Waiting for your approval",
        ),
        (
            {"status": "interrupted", "interrupt_reason": "user_question"},
            {},
            "needs_input",
            "Has a question for you",
        ),
        (
            {"status": "interrupted", "interrupt_reason": "credit_pause"},
            {},
            "needs_input",
            "Paused: out of credits",
        ),
        ({"status": "interrupted"}, {}, "needs_input", "Waiting on you"),
        (
            {},
            {"run_metadata": {"automation_id": "a-1"}, "automation_name": "Morning brief"},
            "automation",
            "Morning brief ran",
        ),
        (
            {"status": "error"},
            {"run_metadata": {"automation_id": "a-1"}, "automation_name": "Morning brief"},
            "automation",
            "Morning brief failed",
        ),
        ({}, {"run_metadata": {"automation_id": "a-1"}}, "automation", "Automation ran"),
        (
            {"status": "error"},
            {"run_metadata": {"automation_id": "a-1"}},
            "automation",
            "Automation failed",
        ),
        (
            {"status": "interrupted"},
            {"run_metadata": {"automation_id": "a-1"}},
            "needs_input",
            "Waiting on you",
        ),
        (
            {},
            {"run_metadata": {"report_back_ptc_thread_id": "ptc-1"}, "query_type": "system"},
            "wakeup",
            "Reported back",
        ),
        (
            {"status": "error"},
            {"run_metadata": {"report_back_ptc_thread_id": "ptc-1"}, "query_type": "system"},
            "wakeup",
            "Stopped with an error",
        ),
        # The user's message past an interrupted summary carries its pair.
        (
            {},
            {"run_metadata": {"report_back_ptc_thread_id": "ptc-1"}},
            "turn",
            "Finished",
        ),
        ({}, {"query_type": "system"}, "wakeup", "Reported back"),
        ({}, {"query_type": "wakeup"}, "wakeup", "Checked back in"),
        ({}, {"query_type": "market_target"}, "wakeup", "Price alert"),
        ({"status": "error"}, {"query_type": "wakeup"}, "wakeup", "Stopped with an error"),
    ],
)
@pytest.mark.asyncio
async def test_the_kind_and_body_follow_the_run(db, payload, context, kind, body):
    db["get_push_context"].return_value = _context(**context)
    news = await notices.settle_news(_payload(**payload))
    assert (news.notice.kind, news.notice.body) == (kind, body)


@pytest.mark.asyncio
async def test_an_automation_is_named_as_its_start_named_it(db):
    """START stamps the name its start notice used; the join reads the
    automation as it is now, so it names only a run from before the stamp."""
    db["get_push_context"].return_value = _context(
        run_metadata={"automation_id": "a-1", "automation_name": "Morning brief"},
        automation_name="Renamed since",
    )
    news = await notices.settle_news(_payload())
    assert news.notice.body == "Morning brief ran"


def test_the_title_is_the_threads_cut_short_or_the_apps_name():
    assert notices.push_title("  NVDA  ") == "NVDA"
    assert notices.push_title(None) == "LangAlpha"
    assert notices.push_title("   ") == "LangAlpha"
    long = notices.push_title("x" * 200)
    assert len(long) == 80 and long.endswith("…")


@pytest.mark.asyncio
async def test_the_push_names_the_thread_and_carries_the_badge(db, apns):
    await _job("push")
    (sent,) = apns.sent
    assert sent["collapse_id"] == "thread-1"
    assert sent["payload"] == {
        "aps": {
            "alert": {"title": "NVDA after earnings", "body": "Finished"},
            "sound": "default",
            # Two unseen threads and one waiting on the user.
            "badge": 3,
            "thread-id": "thread-1",
        },
        "body": {
            "thread_id": "thread-1",
            "workspace_id": "ws-1",
            "run_id": "run-1",
            "kind": "turn",
        },
    }
    db["stamp_push_devices_sent"].assert_awaited_once_with(["dev-1"])


# ---------------------------------------------------------------------------
# When it is skipped
# ---------------------------------------------------------------------------


def test_a_settle_is_news_by_default():
    assert notices.push_owed("completed", "thread-1", {})
    assert notices.push_owed("error", "thread-1", {})
    assert notices.push_owed("interrupted", "thread-1", {})


def test_a_quiet_check_that_changed_nothing_is_not_news():
    quiet = {"wakeup_quiet": {"changed": False}}
    assert not notices.push_owed("completed", "thread-1", quiet)
    assert notices.push_owed("completed", "thread-1", {"wakeup_quiet": {"changed": True}})
    # One that failed or waits on the user still is.
    assert notices.push_owed("error", "thread-1", quiet)


def test_a_dispatched_analysts_ending_leaves_the_push_to_its_report_back():
    analyst = {"origin_home_thread_id": "cos-1"}
    for status in ("completed", "error"):
        assert not notices.push_owed(status, "thread-1", analyst), status
    # Waiting on the user, it has no report coming.
    assert notices.push_owed("interrupted", "thread-1", analyst)
    # The report-back summary runs on the Chief of Staff thread itself.
    assert notices.push_owed("completed", "cos-1", analyst)
    summary = {"origin_home_thread_id": "cos-1", "report_back_ptc_thread_id": "ptc-1"}
    assert notices.push_owed("completed", "thread-1", summary)


@pytest.mark.parametrize(
    "platform, owed",
    [
        (None, True),
        ("web", True),
        ("market_view:AAPL", True),
        ("telegram", False),
        ("slack", False),
        ("imessage", False),
    ],
)
def test_a_turn_on_a_chat_app_is_answered_there(platform, owed):
    meta = {"surface": {"platform": platform, "rules": "plain text"}} if platform else {}
    assert notices.push_owed("completed", "thread-1", meta) is owed


@pytest.mark.asyncio
async def test_a_settle_that_is_not_news_sends_nothing(db, apns):
    db["get_push_context"].return_value = _context(
        run_metadata={"surface": {"platform": "telegram"}}
    )
    await _job("push")
    assert apns.sent == []
    db["badge"].assert_not_awaited()


@pytest.mark.asyncio
async def test_push_off_is_a_no_op(db, monkeypatch):
    monkeypatch.setattr(notify, "get_apns_client", AsyncMock(return_value=None))
    monkeypatch.setattr(notify, "push_available", lambda: False)
    await _job("push")
    db["list_push_devices"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_key_that_will_not_load_is_retried_not_dropped(db, monkeypatch):
    monkeypatch.setattr(notify, "get_apns_client", AsyncMock(return_value=None))
    monkeypatch.setattr(notify, "push_available", lambda: True)
    with pytest.raises(notify.PushRetry):
        await _job("push")
    db["list_push_devices"].assert_not_awaited()


@pytest.mark.asyncio
async def test_no_devices_reads_nothing_more(db, apns):
    db["list_push_devices"].return_value = []
    await _job("push")
    db["get_push_context"].assert_not_awaited()
    assert apns.sent == []


@pytest.mark.asyncio
async def test_the_push_never_tells_the_feed(db, apns):
    await _job("push")
    assert apns.sent
    db["feed"].assert_not_awaited()


# ---------------------------------------------------------------------------
# What the feed is told, for the desktop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_feed_gets_the_pushs_words_as_a_notification_event(db):
    db["get_push_context"].return_value = _context(query_type="wakeup")
    await _job("user_notification", _payload(status="error"))
    user_id, event = db["feed"].await_args.args
    assert user_id == "u-1"
    assert event == {
        "v": 1,
        "type": "notification",
        "thread_id": "thread-1",
        "workspace_id": "ws-1",
        "run_id": "run-1",
        "run_seq": 5,
        "status": "failed",
        "interrupt_reason": None,
        "origin": "wakeup",
        # Under `notification`, as on run_started: top-level `title` is the thread's.
        "notification": {
            "kind": "wakeup",
            "title": "NVDA after earnings",
            "body": "Stopped with an error",
        },
    }


@pytest.mark.asyncio
async def test_a_pause_reaches_the_feed_with_its_reason(db):
    await _job("user_notification", _payload(status="interrupted", interrupt_reason="approval_required"))
    _, event = db["feed"].await_args.args
    assert (event["status"], event["interrupt_reason"]) == (
        "interrupted",
        "approval_required",
    )
    assert (event["origin"], event["notification"]["kind"], event["notification"]["body"]) == (
        "turn",
        "needs_input",
        "Waiting for your approval",
    )


@pytest.mark.asyncio
async def test_the_notice_needs_no_apns_and_no_device(db, monkeypatch):
    """The desktop notice is its own job: nothing about APNs or phones is
    read, so a slow or failing APNs never holds it back."""
    client = AsyncMock(return_value=None)
    monkeypatch.setattr(notify, "get_apns_client", client)
    db["list_push_devices"].return_value = []
    await _job("user_notification")
    db["feed"].assert_awaited_once()
    client.assert_not_awaited()
    db["list_push_devices"].assert_not_awaited()
    db["badge"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_feed_failure_raises_for_the_outbox_to_retry(db):
    db["feed"].side_effect = ConnectionError("redis down")
    with pytest.raises(ConnectionError):
        await _job("user_notification")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "context",
    [
        {"last_seen_run_seq": 5},  # watched during the grace delay
        {"latest_run_seq": 6},  # the thread moved on
        {"archived_at": "2026-11-01"},
        {"workspace_status": "deleted"},
        {"owner_id": "u-2"},
        {"run_exists": False},
        {"run_metadata": {"surface": {"platform": "telegram"}}},
    ],
)
async def test_a_settle_that_is_not_news_tells_the_feed_nothing(db, context):
    db["get_push_context"].return_value = _context(**context)
    await _job("user_notification")
    db["feed"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_notice_job_missing_its_ids_is_dropped(db):
    await _job("user_notification", _payload(run_id=None))
    db["get_push_context"].assert_not_awaited()
    db["feed"].assert_not_awaited()


# ---------------------------------------------------------------------------
# Where a run came from, as the settle reads it back
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_retried_wake_reaches_the_feed_as_a_turn(db):
    """A ``/retry`` writes no query row, so START stamps ``turn``, and the
    settle's join, which finds the turn's old wake row, must not relabel it."""
    from src.server.contracts.turn_kinds import RUN_ORIGIN_META

    db["get_push_context"].return_value = _context(
        query_type="wakeup", run_metadata={RUN_ORIGIN_META: "turn"}
    )
    await _job("user_notification")
    _, event = db["feed"].await_args.args
    assert (event["origin"], event["notification"]["kind"], event["notification"]["body"]) == ("turn", "turn", "Finished")


# ---------------------------------------------------------------------------
# What a background run says as it starts
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "origin, name, body",
    [
        ("market_target", None, "Checking a price alert…"),
        ("automation", " Morning brief ", "Morning brief running…"),
        ("automation", None, "Automation running…"),
    ],
)
def test_a_background_run_announces_its_start(origin, name, body):
    assert notices.compose_started(origin, "NVDA", name) == Notice("started", "NVDA", body)


def test_a_start_notice_on_a_thread_gone_names_the_app():
    notice = notices.compose_started("market_target", None, None)
    assert notice == Notice("started", "LangAlpha", "Checking a price alert…")


@pytest.mark.parametrize("origin", ["turn", "report", "wakeup"])
def test_a_run_the_user_started_or_a_wakeup_announces_nothing(origin):
    assert notices.compose_started(origin, "NVDA", None) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "context",
    [
        None,
        _context(owner_id="someone-else"),
        _context(workspace_status="deleted"),
        _context(archived_at="2026-11-01T00:00:00Z"),
        _context(run_exists=False),
    ],
    ids=["gone", "not_theirs", "workspace_deleted", "archived", "run_gone"],
)
async def test_a_thread_the_user_cannot_open_is_not_pushed(db, apns, context):
    db["get_push_context"].return_value = context
    await _job("push")
    assert apns.sent == []


@pytest.mark.asyncio
async def test_a_thread_that_moved_on_is_not_pushed(db, apns):
    db["get_push_context"].return_value = _context(latest_run_seq=6)
    await _job("push")
    assert apns.sent == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "error"])
async def test_a_turn_seen_during_the_grace_is_not_pushed(db, apns, status):
    db["get_push_context"].return_value = _context(last_seen_run_seq=5)
    await _job("push", _payload(status=status))
    assert apns.sent == []


@pytest.mark.asyncio
async def test_a_pause_pushes_whatever_the_seen_pointer_says(db, apns):
    db["get_push_context"].return_value = _context(last_seen_run_seq=5)
    await _job("push", _payload(status="interrupted", interrupt_reason="user_question"))
    assert len(apns.sent) == 1


@pytest.mark.asyncio
async def test_a_kind_switched_off_skips_that_device(db, apns):
    db["list_push_devices"].return_value = [
        _device(1, turn=False),
        _device(2, needs_input=False),
        _device(3, turn=True),
    ]
    await _job("push")
    assert [s["token"] for s in apns.sent] == ["02" * 32, "03" * 32]


@pytest.mark.asyncio
async def test_every_device_off_reads_no_badge(db, apns):
    db["list_push_devices"].return_value = [_device(1, turn=False)]
    await _job("push")
    db["badge"].assert_not_awaited()
    assert apns.sent == []


# ---------------------------------------------------------------------------
# What APNs' answers do
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_unregistered_token_is_pruned(db, apns):
    apns.answers[("01" * 32, "production")] = ApnsOutcome.UNREGISTERED
    await _job("push")
    db["delete_push_device_by_token"].assert_awaited_once_with("01" * 32, REGISTERED)
    db["stamp_push_devices_sent"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_bad_token_is_tried_in_the_other_environment_and_moved(db, apns):
    apns.answers[("01" * 32, "production")] = ApnsOutcome.BAD_TOKEN
    await _job("push")
    assert [s["environment"] for s in apns.sent] == ["production", "sandbox"]
    db["set_push_device_environment"].assert_awaited_once_with("dev-1", "sandbox", REGISTERED)
    db["delete_push_device_by_token"].assert_not_awaited()
    db["stamp_push_devices_sent"].assert_awaited_once_with(["dev-1"])


@pytest.mark.asyncio
async def test_a_bad_token_in_both_environments_is_pruned(db, apns):
    apns.answers[("01" * 32, "production")] = ApnsOutcome.BAD_TOKEN
    apns.answers[("01" * 32, "sandbox")] = ApnsOutcome.BAD_TOKEN
    await _job("push")
    db["set_push_device_environment"].assert_not_awaited()
    db["delete_push_device_by_token"].assert_awaited_once_with("01" * 32, REGISTERED)


@pytest.mark.asyncio
async def test_a_refusal_on_the_other_host_keeps_the_device(db, apns):
    apns.answers[("01" * 32, "production")] = ApnsOutcome.BAD_TOKEN
    apns.answers[("01" * 32, "sandbox")] = ApnsOutcome.REJECTED
    await _job("push")
    db["delete_push_device_by_token"].assert_not_awaited()
    db["set_push_device_environment"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_refusal_keeps_the_device_and_acks(db, apns):
    apns.answers[("01" * 32, "production")] = ApnsOutcome.REJECTED
    await _job("push")
    db["delete_push_device_by_token"].assert_not_awaited()
    db["stamp_push_devices_sent"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failure_to_retry_raises_after_every_device_and_remembers_the_rest(
    db, apns
):
    db["list_push_devices"].return_value = [_device(1), _device(2), _device(3)]
    apns.answers[("02" * 32, "production")] = ApnsOutcome.RETRY
    with pytest.raises(notify.PushRetry):
        await _job("push")
    assert len(apns.sent) == 3
    db["stamp_push_devices_sent"].assert_awaited_once_with(["dev-1", "dev-3"])
    db["merge"].assert_awaited_once_with(
        "job-1", "sent_device_ids", ["dev-1", "dev-3"]
    )


@pytest.mark.asyncio
async def test_a_retried_job_reaches_only_the_devices_that_missed_it(db, apns):
    db["list_push_devices"].return_value = [_device(1), _device(2), _device(3)]
    await _job("push", _payload(sent_device_ids=["dev-1", "dev-3"]))
    assert [s["token"] for s in apns.sent] == ["02" * 32]


@pytest.mark.asyncio
async def test_a_device_whose_send_raises_is_retried_and_the_rest_recorded(db, apns):
    db["list_push_devices"].return_value = [_device(1), _device(2)]
    db["delete_push_device_by_token"].side_effect = RuntimeError("db down")
    apns.answers[("02" * 32, "production")] = ApnsOutcome.UNREGISTERED
    with pytest.raises(notify.PushRetry):
        await _job("push")
    db["stamp_push_devices_sent"].assert_awaited_once_with(["dev-1"])
    db["merge"].assert_awaited_once_with("job-1", "sent_device_ids", ["dev-1"])
    # The lease is renewed before what was sent is recorded.
    db["renew"].assert_awaited_once_with("job-1", 60, attempts=1)


def test_the_executors_are_registered():
    from src.server.services import hook_outbox

    notify.register_outbox_executors()
    notices.register_outbox_executors()
    for hook_type, executor in _EXECUTORS.items():
        assert hook_outbox._EXECUTORS[hook_type] is executor
