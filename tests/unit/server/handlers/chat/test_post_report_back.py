"""Outcome classification for ``_post_report_back``.

Maps the HTTP result of the report-back POST to ``(outcome, run_id)``:
2xx -> dispatched; 404 -> deleted (discard queue); other permanent 4xx -> drop;
409/402/403/429/5xx/network error -> retry with backoff; busy-wait cap -> drop.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat import report_back

_ORIGIN = {
    "ptc_workspace_id": "ws-ptc",
    "flash_workspace_id": "ws-flash",
    "user_id": "u-1",
}


class _FakeResp:
    """Async-context-manager HTTP response with a fixed status + body."""

    def __init__(self, status, *, json_data=None, json_raises=False, text_data=""):
        self.status = status
        self._json_data = json_data
        self._json_raises = json_raises
        self._text_data = text_data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        if self._json_raises:
            raise ValueError("response was not json")
        return self._json_data or {}

    async def text(self):
        return self._text_data


class _FakeSession:
    """Async-context-manager session that returns queued steps per ``post``.

    ``_post_report_back`` opens a fresh ``ClientSession`` each retry, so patching
    ``aiohttp.ClientSession`` to always return this one instance lets a single
    step list drive the whole retry loop. A step that is an Exception is raised
    from ``post`` to simulate a network failure.
    """

    def __init__(self, steps):
        self._steps = list(steps)
        self.post_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, *args, **kwargs):
        self.post_calls += 1
        step = self._steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


def _patch_session(steps):
    session = _FakeSession(steps)
    return session, patch("aiohttp.ClientSession", MagicMock(return_value=session))


async def _run(steps):
    """Drive _post_report_back over ``steps`` with sleeps stubbed out."""
    session, sess_patch = _patch_session(steps)
    with sess_patch, patch("asyncio.sleep", new=AsyncMock()):
        outcome = await report_back._post_report_back(
            cache=None,
            flash_thread_id="flash-1",
            ptc_thread_id="ptc-1",
            origin=_ORIGIN,
        )
    return outcome, session


@pytest.mark.asyncio
async def test_2xx_returns_dispatched_with_run_id():
    outcome, session = await _run([_FakeResp(200, json_data={"run_id": "rid-1"})])
    assert outcome == ("dispatched", "rid-1")
    assert session.post_calls == 1


@pytest.mark.asyncio
async def test_2xx_without_parseable_run_id_dispatches_with_none():
    """A 2xx whose body isn't JSON still dispatches; run_id is just None."""
    outcome, _ = await _run([_FakeResp(201, json_raises=True)])
    assert outcome == ("dispatched", None)


@pytest.mark.asyncio
async def test_404_returns_deleted():
    """Flash thread gone -> caller discards the whole queue."""
    outcome, session = await _run([_FakeResp(404, text_data="not found")])
    assert outcome == ("deleted", None)
    assert session.post_calls == 1


@pytest.mark.parametrize("status", [400, 422])
@pytest.mark.asyncio
async def test_permanent_4xx_returns_drop_without_retry(status):
    """A non-404, non-gate 4xx won't change on retry -> drop this member."""
    outcome, session = await _run([_FakeResp(status, text_data="permanent")])
    assert outcome == ("drop", None)
    assert session.post_calls == 1


@pytest.mark.parametrize(
    "first_step",
    [
        pytest.param(_FakeResp(409, text_data="busy"), id="409-busy"),
        pytest.param(_FakeResp(402, text_data="payment"), id="402-payment-gate"),
        pytest.param(_FakeResp(403, text_data="no_provider"), id="403-access-gate"),
        pytest.param(_FakeResp(429, text_data="rate limited"), id="429-rate-limited"),
        pytest.param(_FakeResp(503, text_data="upstream"), id="5xx-transient"),
        pytest.param(ConnectionError("boom"), id="network-error"),
    ],
)
@pytest.mark.asyncio
async def test_transient_then_dispatched_retries(first_step):
    """Transient failures/gates retry until the thread admits — never drop."""
    outcome, session = await _run(
        [first_step, _FakeResp(200, json_data={"run_id": "rid-2"})]
    )
    assert outcome == ("dispatched", "rid-2")
    assert session.post_calls == 2


@pytest.mark.asyncio
async def test_busy_wait_cap_exhausted_returns_drop():
    """A flash thread that never frees up -> give up at the busy-wait cap."""
    session, sess_patch = _patch_session([_FakeResp(409, text_data="busy")])
    with (
        sess_patch,
        patch("asyncio.sleep", new=AsyncMock()),
        # Past deadline on the first check, so one 409 exhausts the budget.
        patch.object(report_back, "_RB_BUSY_WAIT_CAP", -1.0),
    ):
        outcome = await report_back._post_report_back(
            cache=None,
            flash_thread_id="flash-1",
            ptc_thread_id="ptc-1",
            origin=_ORIGIN,
        )
    assert outcome == ("drop", None)
    assert session.post_calls == 1


# ---------------------------------------------------------------------------
# A configured INTERNAL_SERVICE_TOKEN is the normal production state and is now
# a precondition for dispatch (the ptc_agent / _post_report_back preflight guard
# aborts without it, to avoid a silent foreground credit burn). These tests
# exercise the dispatch path itself, not the guard, so give the whole module a
# token. The dedicated guard test module asserts the unset behaviour separately.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _internal_service_token(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "test-internal-service-token")
