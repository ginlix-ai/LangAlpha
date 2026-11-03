"""The push executor: what a settled run's push says, who gets it, and what
APNs' answers do to the devices and the job."""

from unittest.mock import AsyncMock

import pytest

from src.server.services.push import notify
from src.server.services.push.apns import ApnsOutcome, ApnsResult

MOD = "src.server.services.push.notify"


def _payload(**kw):
    payload = {
        "user_id": "u-1",
        "thread_id": "thread-1",
        "workspace_id": "ws-1",
        "run_id": "run-1",
        "run_seq": 5,
        "status": "completed",
        "interrupt_reason": None,
        "automation_id": None,
        "report_back": False,
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
    }


class FakeApns:
    """Answers by (token, environment); SENT unless told otherwise."""

    def __init__(self, answers=None):
        self.answers = answers or {}
        self.sent = []

    async def send(self, *, token, bundle_id, environment, payload, collapse_id):
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
    fakes["lifecycle"] = AsyncMock(
        return_value=(
            [
                {"branch": "unseen", "latest_run_status": "completed"},
                {"branch": "unseen", "latest_run_status": "error"},
                {"branch": "live", "latest_run_status": "interrupted"},
                {"branch": "live", "latest_run_status": "in_progress"},
            ],
            9,
        )
    )
    monkeypatch.setattr(notify, "get_thread_lifecycle_rows", fakes["lifecycle"])
    fakes["merge"] = AsyncMock(return_value=True)
    monkeypatch.setattr(notify.outbox_db, "merge_job_payload", fakes["merge"])
    return fakes


@pytest.fixture
def apns(monkeypatch):
    fake = FakeApns()
    monkeypatch.setattr(notify, "get_apns_client", AsyncMock(return_value=fake))
    return fake


async def _run(payload=None, **job):
    await notify.execute_push(
        {"hook_outbox_id": "job-1", "payload": payload or _payload(), **job}
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
            {"automation_id": "a-1"},
            {"automation_name": "Morning brief"},
            "automation",
            "Morning brief ran",
        ),
        (
            {"automation_id": "a-1", "status": "error"},
            {"automation_name": "Morning brief"},
            "automation",
            "Morning brief failed",
        ),
        ({"automation_id": "a-1"}, {}, "automation", "Automation ran"),
        ({"automation_id": "a-1", "status": "error"}, {}, "automation", "Automation failed"),
        ({"report_back": True}, {"query_type": "system"}, "wakeup", "Reported back"),
        ({}, {"query_type": "system"}, "wakeup", "Reported back"),
        ({}, {"query_type": "wakeup"}, "wakeup", "Checked back in"),
        ({}, {"query_type": "market_target"}, "wakeup", "Price alert"),
        ({"status": "error"}, {"query_type": "wakeup"}, "wakeup", "Stopped with an error"),
    ],
)
def test_the_kind_and_body_follow_the_run(payload, context, kind, body):
    message = notify.compose(_payload(**payload), _context(**context))
    assert (message.kind, message.body) == (kind, body)


def test_the_title_is_the_threads_cut_short_or_the_apps_name():
    assert notify.push_title("  NVDA  ") == "NVDA"
    assert notify.push_title(None) == "LangAlpha"
    assert notify.push_title("   ") == "LangAlpha"
    long = notify.push_title("x" * 200)
    assert len(long) == 80 and long.endswith("…")


@pytest.mark.asyncio
async def test_the_push_names_the_thread_and_carries_the_badge(db, apns):
    await _run()
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


@pytest.mark.asyncio
async def test_push_off_is_a_no_op(db, monkeypatch):
    monkeypatch.setattr(notify, "get_apns_client", AsyncMock(return_value=None))
    await _run()
    db["list_push_devices"].assert_not_awaited()


@pytest.mark.asyncio
async def test_no_devices_reads_nothing_more(db, apns):
    db["list_push_devices"].return_value = []
    await _run()
    db["get_push_context"].assert_not_awaited()
    assert apns.sent == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "context",
    [
        None,
        _context(owner_id="someone-else"),
        _context(workspace_status="deleted"),
        _context(archived_at="2026-11-01T00:00:00Z"),
    ],
    ids=["gone", "not_theirs", "workspace_deleted", "archived"],
)
async def test_a_thread_the_user_cannot_open_is_not_pushed(db, apns, context):
    db["get_push_context"].return_value = context
    await _run()
    assert apns.sent == []


@pytest.mark.asyncio
async def test_a_thread_that_moved_on_is_not_pushed(db, apns):
    db["get_push_context"].return_value = _context(latest_run_seq=6)
    await _run()
    assert apns.sent == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "error"])
async def test_a_turn_seen_during_the_grace_is_not_pushed(db, apns, status):
    db["get_push_context"].return_value = _context(last_seen_run_seq=5)
    await _run(_payload(status=status))
    assert apns.sent == []


@pytest.mark.asyncio
async def test_a_pause_pushes_whatever_the_seen_pointer_says(db, apns):
    db["get_push_context"].return_value = _context(last_seen_run_seq=5)
    await _run(_payload(status="interrupted", interrupt_reason="user_question"))
    assert len(apns.sent) == 1


@pytest.mark.asyncio
async def test_a_kind_switched_off_skips_that_device(db, apns):
    db["list_push_devices"].return_value = [
        _device(1, turn=False),
        _device(2, needs_input=False),
        _device(3, turn=True),
    ]
    await _run()
    assert [s["token"] for s in apns.sent] == ["02" * 32, "03" * 32]


@pytest.mark.asyncio
async def test_every_device_off_reads_no_badge(db, apns):
    db["list_push_devices"].return_value = [_device(1, turn=False)]
    await _run()
    db["lifecycle"].assert_not_awaited()
    assert apns.sent == []


# ---------------------------------------------------------------------------
# What APNs' answers do
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_unregistered_token_is_pruned(db, apns):
    apns.answers[("01" * 32, "production")] = ApnsOutcome.UNREGISTERED
    await _run()
    db["delete_push_device_by_token"].assert_awaited_once_with("01" * 32)
    db["stamp_push_devices_sent"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_bad_token_is_tried_in_the_other_environment_and_moved(db, apns):
    apns.answers[("01" * 32, "production")] = ApnsOutcome.BAD_TOKEN
    await _run()
    assert [s["environment"] for s in apns.sent] == ["production", "sandbox"]
    db["set_push_device_environment"].assert_awaited_once_with("dev-1", "sandbox")
    db["delete_push_device_by_token"].assert_not_awaited()
    db["stamp_push_devices_sent"].assert_awaited_once_with(["dev-1"])


@pytest.mark.asyncio
async def test_a_bad_token_in_both_environments_is_pruned(db, apns):
    apns.answers[("01" * 32, "production")] = ApnsOutcome.BAD_TOKEN
    apns.answers[("01" * 32, "sandbox")] = ApnsOutcome.BAD_TOKEN
    await _run()
    db["set_push_device_environment"].assert_not_awaited()
    db["delete_push_device_by_token"].assert_awaited_once_with("01" * 32)


@pytest.mark.asyncio
async def test_a_refusal_keeps_the_device_and_acks(db, apns):
    apns.answers[("01" * 32, "production")] = ApnsOutcome.REJECTED
    await _run()
    db["delete_push_device_by_token"].assert_not_awaited()
    db["stamp_push_devices_sent"].assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failure_to_retry_raises_after_every_device_and_remembers_the_rest(
    db, apns
):
    db["list_push_devices"].return_value = [_device(1), _device(2), _device(3)]
    apns.answers[("02" * 32, "production")] = ApnsOutcome.RETRY
    with pytest.raises(notify.PushRetry):
        await _run()
    assert len(apns.sent) == 3
    db["stamp_push_devices_sent"].assert_awaited_once_with(["dev-1", "dev-3"])
    db["merge"].assert_awaited_once_with(
        "job-1", {"sent_device_ids": ["dev-1", "dev-3"]}
    )


@pytest.mark.asyncio
async def test_a_retried_job_reaches_only_the_devices_that_missed_it(db, apns):
    db["list_push_devices"].return_value = [_device(1), _device(2), _device(3)]
    await _run(_payload(sent_device_ids=["dev-1", "dev-3"]))
    assert [s["token"] for s in apns.sent] == ["02" * 32]


def test_the_executor_is_registered_for_push():
    from src.server.services import hook_outbox

    notify.register_outbox_executors()
    assert hook_outbox._EXECUTORS["push"] is notify.execute_push
