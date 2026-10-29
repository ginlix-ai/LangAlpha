"""``post_notification_turn``'s opt-in fail-fast answers.

With ``busy_fails_fast`` a caller that can hand its message to the running
turn hears "busy" at once, while our own duplicate is still adopted. With
``refusal_fails_fast`` a caller that can hand its message to the thread's
next turn hears "refused" at once on a refusal no wait outlives, while one
that clears by itself is still waited out. Callers that ask for neither, the
report-back executors, keep deferring through every refusal. ``on_unanswered``
hears of each POST that may still be admitted, and of no other."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.report_back.notify_turn import post_notification_turn


class _Resp:
    def __init__(self, status: int, body: dict) -> None:
        self.status = status
        self._body = body

    async def json(self):
        return self._body

    async def text(self):
        return str(self._body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Session:
    def __init__(self, responses: list[_Resp]) -> None:
        self._responses = responses
        self.posts = 0

    def post(self, *_a, **_k):
        self.posts += 1
        return self._responses.pop(0)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


async def _post(session: _Session, **kwargs):
    with (
        patch("aiohttp.ClientSession", return_value=session),
        patch(
            "src.config.settings.background_dispatch_requires_token",
            return_value=False,
        ),
        patch("asyncio.sleep", new=AsyncMock()),
    ):
        return await post_notification_turn(
            thread_id="t-1",
            body={},
            user_id="u-1",
            wait_cap=60.0,
            log_prefix="[test]",
            **kwargs,
        )


@pytest.mark.asyncio
async def test_busy_409_returns_at_once_when_asked():
    session = _Session([_Resp(409, {"detail": {"code": "thread_busy"}})])

    assert await _post(session, busy_fails_fast=True) == ("busy", None)
    assert session.posts == 1


@pytest.mark.asyncio
async def test_duplicate_409_is_still_adopted_when_failing_fast():
    session = _Session(
        [_Resp(409, {"detail": {"code": "duplicate_request", "run_id": "r-0"}})]
    )

    assert await _post(session, busy_fails_fast=True) == ("dispatched", "r-0")


@pytest.mark.asyncio
async def test_busy_409_defers_by_default():
    session = _Session(
        [
            _Resp(409, {"detail": {"code": "thread_busy"}}),
            _Resp(200, {"run_id": "r-1"}),
        ]
    )

    assert await _post(session) == ("dispatched", "r-1")
    assert session.posts == 2


# A usage limit is any 429 type but the server's own capacity types; this is
# the default type the credit gate stamps when the platform names none.
_USAGE_LIMIT = {"detail": {"type": "credit_limit", "message": "Limit reached"}}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body"),
    [(402, {"detail": "Payment required"}), (429, _USAGE_LIMIT)],
    ids=["402", "usage-limit-429"],
)
async def test_a_lasting_refusal_returns_at_once_when_asked(status, body):
    session = _Session([_Resp(status, body)])

    assert await _post(session, refusal_fails_fast=True) == ("refused", None)
    assert session.posts == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body"),
    [
        (429, {"detail": {"type": "burst_limit"}}),
        (429, {"detail": {"type": "service_unavailable"}}),
        (429, {"detail": "Too many requests"}),
        (403, {"detail": "No provider"}),
        (503, {}),
    ],
    ids=["burst-429", "unavailable-429", "untyped-429", "403", "503"],
)
async def test_a_refusal_that_clears_by_itself_is_still_waited_out(status, body):
    session = _Session([_Resp(status, body), _Resp(200, {"run_id": "r-1"})])

    assert await _post(session, refusal_fails_fast=True) == ("dispatched", "r-1")
    assert session.posts == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body"),
    [(402, {"detail": "Payment required"}), (429, _USAGE_LIMIT)],
    ids=["402", "usage-limit-429"],
)
async def test_report_backs_keep_deferring_on_a_lasting_refusal(status, body):
    session = _Session([_Resp(status, body), _Resp(200, {"run_id": "r-1"})])

    assert await _post(session) == ("dispatched", "r-1")
    assert session.posts == 2


class _NoAnswer:
    async def __aenter__(self):
        raise TimeoutError("sock_read")

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_each_post_with_no_answer_is_reported():
    unanswered = AsyncMock()
    session = _Session([_NoAnswer(), _Resp(503, {}), _Resp(200, {"run_id": "r-1"})])

    assert await _post(session, on_unanswered=unanswered) == ("dispatched", "r-1")
    assert unanswered.await_count == 2


@pytest.mark.asyncio
async def test_a_definite_refusal_is_not_reported():
    unanswered = AsyncMock()
    session = _Session([_Resp(409, {"detail": {"code": "thread_busy"}})])

    assert await _post(session, busy_fails_fast=True, on_unanswered=unanswered) == (
        "busy",
        None,
    )
    unanswered.assert_not_awaited()
