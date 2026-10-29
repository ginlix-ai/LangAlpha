"""Failed-dispatch lifecycle: the report-back reservation.

A dispatch reserves its report-back slot before the request goes out, and
rolls it back only on a definitive receipt that the run never started: a cap
hit in ``reserve()`` (nothing is sent), a >=400 response (the endpoint's error
paths all precede its create_task), or a connection never established.
Ambiguous outcomes (timeout, mid-flight transport loss, odd statuses/bodies)
reconcile against the endpoint's admission marker, scoped to THIS dispatch's
generation (the endpoint stamps the POST's ``origin_dispatch_gen`` into the
marker and refuses to schedule if the write fails): a marker carrying OUR
generation means delivered, plain success. The marker is POSITIVE-ONLY and
reconciliation NEVER rolls back: no finite absence proves a delivered request
won't admit later, and a foreign marker may be a stale terminal our own
admission is about to replace, so every unproven verdict retains the
reservation (TTL-bounded, orphan-reaped once the origin lapses).
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from src.server.services.report_back.handoff import keys, reserve
from src.tools.secretary.chief_of_staff import delegate_to_analyst
from tests.unit.server.handlers.chat.redis_fakes import FakeCache as _FakeCache

from .conftest import FakeResp, FakeSession

USER_ID = "user-1"
DISPATCHER_THREAD_ID = "cos-thread-1"
WORKSPACE_ID = "22222222-2222-2222-2222-222222222222"


def _tool_call(args: dict, call_id: str = "call_test") -> dict:
    # ``state`` is what the graph injects; nothing here was approved in advance.
    return {"name": "delegate_to_analyst", "args": {**args, "state": {}}, "id": call_id, "type": "tool_call"}


def _new_thread() -> dict:
    """A hand-off starting a new thread in the user's workspace."""
    return _tool_call({"question": "analyze this", "workspace_id": WORKSPACE_ID})


def _config() -> dict:
    # thread_id = the dispatching thread -> report_back wiring is live.
    return {"configurable": {"user_id": USER_ID, "thread_id": DISPATCHER_THREAD_ID}}


def _payload(result) -> dict:
    return json.loads(result.update["messages"][0].content)


@pytest.fixture(autouse=True)
def _owned_workspace():
    with patch(
        "src.server.database.workspace.get_workspace",
        AsyncMock(
            return_value={
                "user_id": USER_ID,
                "workspace_id": WORKSPACE_ID,
                "name": "Semis",
            }
        ),
    ):
        yield


@pytest.fixture
def cache(monkeypatch):
    c = _FakeCache()
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: c)
    return c


def _fill_thread_cap(cache) -> None:
    cache.client.sets[keys.watch_key(DISPATCHER_THREAD_ID)] = {
        f"p{i}" for i in range(reserve.MAX_DISPATCH_PER_THREAD)
    }
    # Live origins too: originless members would be reaped as orphans on the
    # over-cap retry, and the cap wouldn't hold.
    for i in range(reserve.MAX_DISPATCH_PER_THREAD):
        cache.kv[keys.ptc_origin_key(f"p{i}")] = {
            "home_thread_id": DISPATCHER_THREAD_ID,
            "report_back": True,
        }


@pytest.mark.asyncio
async def test_a_full_cap_refuses_before_any_request(cache):
    """A cap hit in reserve() aborts the dispatch with nothing sent."""
    _fill_thread_cap(cache)
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("dispatch must not run")),
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is False
    assert "too many concurrent analyses" in payload["error"]


@pytest.mark.asyncio
async def test_dispatch_error_status_rolls_back_the_reservation(cache):
    """A >=400 dispatch response (e.g. the credit gate) proves the run never
    started: the endpoint's error paths all precede its create_task."""
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession", return_value=FakeSession(FakeResp(status=402))
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    assert _reservation(cache) is None


@pytest.mark.asyncio
async def test_dispatch_timeout_with_probe_down_keeps_the_reservation(cache, ledger):
    """A timed-out dispatch may have started the run server-side; when the
    admission-marker probe can't answer either (DB blip), the report-back
    reservation must survive."""
    _ledger_probe_raises(ledger)
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession", return_value=FakeSession(post_exc=TimeoutError())
    ), patch(
        "src.tools.secretary.dispatch._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_timeout"
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]


# ---------------------------------------------------------------------------
# Reservation retention across dispatch-response loss (Codex round-6 F2).
# The internal endpoint's 2xx is sent only after its create_task, so a success
# STATUS proves the run is scheduled even when the body never arrives — a
# rollback at that point would erase the origin the run's report-back needs.
# ---------------------------------------------------------------------------


def _reservation(cache) -> str | None:
    """The reserved ptc thread id, or None when the reservation was rolled back."""
    members = cache.client.sets.get(keys.watch_key(DISPATCHER_THREAD_ID), set())
    if not members:
        return None
    (ptc,) = members
    assert keys.ptc_origin_key(ptc) in cache.kv  # membership implies live origin
    return ptc


@pytest.fixture(autouse=True)
def ledger():
    """The durable-run-ledger probe behind ``_confirm_dispatch_admission``
    (v4 2.4c: dispatched runs commit their attempt row — metadata stamped
    with the POST's dispatch generation — before replying). Default: no
    attempt row (nothing admitted)."""
    with patch(
        "src.server.database.runs.lifecycle.get_latest_attempt",
        new_callable=AsyncMock,
        return_value=None,
    ) as probe:
        yield probe


def _attempt_with_our_gen(cache, ledger) -> None:
    """Ledger probe sees the attempt row OUR dispatch committed: emulate the
    endpoint's gen-stamping by reading the generation off the origin the
    reservation just wrote (the dispatched thread id is minted inside the
    tool)."""

    async def _row(thread_id):
        o = cache.kv.get(keys.ptc_origin_key(thread_id))
        gen = o.get("dispatch_gen") if isinstance(o, dict) else None
        return {"metadata": {"origin_dispatch_gen": gen}}

    ledger.side_effect = _row


def _attempt_with_gen(ledger, gen: str) -> None:
    """Ledger probe sees an attempt row stamped with a FIXED foreign
    generation — another run's admission (e.g. a still-active predecessor),
    not ours."""
    ledger.return_value = {"metadata": {"origin_dispatch_gen": gen}}


def _ledger_probe_raises(ledger) -> None:
    """Make the ledger probe fail (DB blip) — verdict unknown."""
    ledger.side_effect = ConnectionError("db blip")


class _LostBodyResp(FakeResp):
    async def json(self) -> dict:
        raise aiohttp.ClientPayloadError("response body lost")


@pytest.mark.asyncio
async def test_success_status_with_lost_body_commits_and_reports_dispatched(cache):
    """A 2xx whose body can't be read is a DISPATCHED run: the status alone
    proves scheduling, so the reservation commits before the body parse and
    the tool reports success."""
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession", return_value=FakeSession(_LostBodyResp(status=200))
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is True
    assert payload["status"] == "dispatched"
    # Only the body names the run, so a later stop takes whatever is running.
    assert "run_id" not in payload
    assert _reservation(cache) == payload["thread_id"]


@pytest.mark.asyncio
async def test_the_dispatched_run_id_reaches_the_result(cache):
    """The run a later stop names, so it never ends a newer turn there."""
    run_id = "55555555-5555-5555-5555-555555555555"
    body = {"status": "dispatched", "run_id": run_id}
    with (
        patch("src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})),
        patch("aiohttp.ClientSession", return_value=FakeSession(FakeResp(body=body))),
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is True
    assert payload["run_id"] == run_id


@pytest.mark.asyncio
async def test_ambiguous_loss_with_admission_marker_reports_success(cache, ledger):
    """Codex round-8 F1: a lost exchange whose admission marker appears is a
    DELIVERED dispatch — the tool reports plain success (no unknown_retained
    ambiguity for the model to mis-handle)."""
    _attempt_with_our_gen(cache, ledger)
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession",
        return_value=FakeSession(post_exc=aiohttp.ServerDisconnectedError()),
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is True
    assert payload["status"] == "dispatched"
    assert _reservation(cache) == payload["thread_id"]


@pytest.mark.asyncio
async def test_ambiguous_loss_with_no_marker_on_fresh_pair_retains(cache):
    """Codex round-9 F2: clean absence is NOT a negative proof — the endpoint
    may still be mid-admission (platform auth/credit checks can outlast the
    grace) — and nothing pre-existing can be lost by keeping a fresh pair, so
    the reservation and workspace are retained as unknown (TTL-bounded)."""
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession",
        return_value=FakeSession(post_exc=aiohttp.ServerDisconnectedError()),
    ), patch(
        "src.tools.secretary.dispatch._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]


@pytest.mark.asyncio
async def test_foreign_marker_on_fresh_pair_is_not_success(cache, ledger):
    """Codex round-9 F3: gen-scoping — a marker that doesn't carry OUR
    generation no longer confirms the dispatch. On a fresh pair it retains
    as unknown instead of claiming success."""
    _attempt_with_gen(ledger, "someone-elses-gen")
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession",
        return_value=FakeSession(post_exc=aiohttp.ServerDisconnectedError()),
    ), patch(
        "src.tools.secretary.dispatch._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]


@pytest.mark.asyncio
async def test_ambiguous_loss_with_probe_down_retains_unknown(cache, ledger):
    """When the marker probe itself fails, the outcome stays unknown — keep
    the reservation (TTL-bounded) and surface the retained thread id so the
    model can check agent_output instead of blind-re-dispatching."""
    _ledger_probe_raises(ledger)
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession",
        return_value=FakeSession(post_exc=aiohttp.ServerDisconnectedError()),
    ), patch(
        "src.tools.secretary.dispatch._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]


@pytest.mark.asyncio
async def test_cancellation_mid_exchange_retains_reservation(cache):
    """Codex round-7 F1: CancelledError (the dispatching turn cancelled,
    worker shutdown) bypasses the except clauses — it must commit before
    propagating, or the CM rollback erases a possibly-scheduled run's
    report-back wiring."""
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession",
        return_value=FakeSession(post_exc=asyncio.CancelledError()),
    ):
        with pytest.raises(asyncio.CancelledError):
            await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    assert _reservation(cache) is not None


@pytest.mark.asyncio
async def test_non_200_success_status_is_not_scheduling_proof(cache):
    """Codex round-7 F2: a 3xx/2xx-non-200 is not the endpoint's reply (it
    answers exactly 200; redirects are disabled) — never success. Delivery
    stays unproven either way, so the fresh pair retains as unknown."""
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession", return_value=FakeSession(FakeResp(status=302))
    ), patch(
        "src.tools.secretary.dispatch._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]


@pytest.mark.asyncio
async def test_non_dict_200_body_reconciles_without_raising(cache, ledger):
    """Codex round-8 F3: a 200 carrying valid non-dict JSON (``[]``/``null``)
    must not AttributeError out of the tool — the 200 keeps the reservation
    committed (status proof stands) and the outcome reconciles as unknown
    when the marker can't confirm."""
    _ledger_probe_raises(ledger)
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession",
        return_value=FakeSession(FakeResp(status=200, body=[])),
    ), patch(
        "src.tools.secretary.dispatch._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]


@pytest.mark.asyncio
async def test_200_with_contradictory_body_never_rolls_back(cache):
    """A clean marker-absent verdict after a REAL 200 must still retain: the
    exact-status proof outranks the probe (the two contradicting is a state
    we can't explain, so the safe side is keep)."""
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession",
        return_value=FakeSession(FakeResp(status=200, body={"status": "nope"})),
    ), patch(
        "src.tools.secretary.dispatch._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]


class _CancelledOnClose(FakeResp):
    async def __aexit__(self, *_exc) -> bool:
        raise asyncio.CancelledError()


@pytest.mark.asyncio
async def test_cancel_as_a_rejected_response_closes_does_not_commit(cache):
    """Codex round-8 F2: a >=400 response settles non-scheduling before the
    response closes, so a cancellation landing as it closes must not commit
    the already-dead reservation."""
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession",
        return_value=FakeSession(_CancelledOnClose(status=402)),
    ):
        with pytest.raises(asyncio.CancelledError):
            await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    assert _reservation(cache) is None


@pytest.mark.asyncio
async def test_connection_never_established_rolls_back_reservation(cache):
    """A refused connection proves the request never reached the endpoint,
    so the reservation rolls back."""
    with patch(
        "src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})
    ), patch(
        "aiohttp.ClientSession",
        return_value=FakeSession(
            post_exc=aiohttp.ClientConnectorError(MagicMock(), OSError("refused"))
        ),
    ):
        result = await delegate_to_analyst.ainvoke(_new_thread(), config=_config())

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    assert _reservation(cache) is None


# ---------------------------------------------------------------------------
# Continuation reconciliation (Codex round-9 F3 + round-10 F1/F2). A retry of
# a live pair provisionally replaces the predecessor origin; when the
# exchange outcome is lost, only a marker carrying OUR generation upgrades to
# success — anything less RETAINS the provisional reservation (round-10: a
# foreign/absent marker is not a rejection receipt; our own admission may
# stamp moments later, and rolling back then orphans a live run's
# report-back). An unwired continuation has no identity to match, so it
# retains without probing — a prior run's marker must never read as ours.
# ---------------------------------------------------------------------------

PTC_THREAD_ID = "11111111-1111-1111-1111-111111111111"
CONT_WORKSPACE_ID = "44444444-4444-4444-4444-444444444444"
PRIOR_GEN = "gen-prior"


def _seed_predecessor(cache) -> None:
    """A prior wired dispatch of the same (dispatcher, ptc) pair, still live."""
    cache.client.sets[keys.watch_key(DISPATCHER_THREAD_ID)] = {PTC_THREAD_ID}
    cache.client.sets[keys.user_pending_key(USER_ID)] = {PTC_THREAD_ID}
    cache.kv[keys.ptc_origin_key(PTC_THREAD_ID)] = {
        "origin": "home",
        "report_back": True,
        "home_thread_id": DISPATCHER_THREAD_ID,
        "ptc_thread_id": PTC_THREAD_ID,
        "user_id": USER_ID,
        "dispatch_gen": PRIOR_GEN,
    }


def _continuation_patches(
    post_exc: Exception | None = None, resp: FakeResp | None = None
) -> list:
    owner = AsyncMock(return_value=USER_ID)
    by_id = AsyncMock(
        return_value={
            "conversation_thread_id": PTC_THREAD_ID,
            "workspace_id": CONT_WORKSPACE_ID,
        }
    )
    return [
        patch("src.server.database.conversation.threads_read.get_thread_owner_id", owner),
        patch("src.server.database.conversation.threads_read.get_thread_by_id", by_id),
        patch("src.tools.secretary.dispatch.hitl_confirm", return_value=(True, {})),
        patch(
            "aiohttp.ClientSession",
            return_value=FakeSession(resp, post_exc=post_exc),
        ),
        patch("src.tools.secretary.dispatch._DISPATCH_CONFIRM_GRACE_S", 0.0),
    ]


async def _continuation_dispatch(
    post_exc: Exception | None = None, resp: FakeResp | None = None
) -> dict:
    import contextlib

    with contextlib.ExitStack() as stack:
        for p in _continuation_patches(post_exc, resp):
            stack.enter_context(p)
        result = await delegate_to_analyst.ainvoke(
            _tool_call({"question": "follow up", "thread_id": PTC_THREAD_ID}),
            config=_config(),
        )
    return _payload(result)


@pytest.mark.asyncio
async def test_continuation_ambiguous_no_marker_retains_provisional(cache):
    """Codex round-10 F2: clean absence is not a rejection receipt — the
    endpoint may stamp our admission moments after the grace, and a rollback
    then orphans the live run's report-back. Retain the provisional
    generation as unknown."""
    _seed_predecessor(cache)
    payload = await _continuation_dispatch(aiohttp.ServerDisconnectedError())

    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    assert payload["outcome"] == "unknown_retained"
    assert cache.kv[keys.ptc_origin_key(PTC_THREAD_ID)]["dispatch_gen"] != PRIOR_GEN
    assert PTC_THREAD_ID in cache.client.sets[keys.watch_key(DISPATCHER_THREAD_ID)]


@pytest.mark.asyncio
async def test_continuation_foreign_marker_is_not_success(cache, ledger):
    """Codex round-9 F3 + round-10 F2: the still-active predecessor's own
    marker must NOT confirm our provisional generation (no false success) —
    but it is not a rejection receipt either, so the provisional reservation
    retains rather than rolling back (our own stamp may land next)."""
    _seed_predecessor(cache)
    _attempt_with_gen(ledger, PRIOR_GEN)
    payload = await _continuation_dispatch(aiohttp.ServerDisconnectedError())

    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"
    assert cache.kv[keys.ptc_origin_key(PTC_THREAD_ID)]["dispatch_gen"] != PRIOR_GEN
    assert PTC_THREAD_ID in cache.client.sets[keys.watch_key(DISPATCHER_THREAD_ID)]


@pytest.mark.asyncio
async def test_unwired_continuation_never_reads_a_marker_as_ours(cache, ledger):
    """Codex round-10 F1: an unwired dispatch carries no generation, so a
    marker on a continuation thread proves only that SOME run held it — a
    prior run's blob must not upgrade a lost 409 into plain success."""
    _attempt_with_gen(ledger, "prior-runs-gen")
    import contextlib

    with contextlib.ExitStack() as stack:
        for p in _continuation_patches(aiohttp.ServerDisconnectedError()):
            stack.enter_context(p)
        result = await delegate_to_analyst.ainvoke(
            _tool_call(
                {
                    "question": "follow up",
                    "thread_id": PTC_THREAD_ID,
                    "report_back": False,
                }
            ),
            config=_config(),
        )
    payload = _payload(result)

    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"


@pytest.mark.asyncio
async def test_continuation_our_marker_reports_success(cache, ledger):
    """Continuation + a marker carrying OUR generation: the lost reply was a
    real acceptance — commit the new incarnation and report plain success."""
    _seed_predecessor(cache)
    _attempt_with_our_gen(cache, ledger)
    payload = await _continuation_dispatch(aiohttp.ServerDisconnectedError())

    assert payload["success"] is True
    assert payload["status"] == "dispatched"
    new_gen = cache.kv[keys.ptc_origin_key(PTC_THREAD_ID)]["dispatch_gen"]
    assert new_gen != PRIOR_GEN


@pytest.mark.asyncio
async def test_a_busy_thread_says_so_and_restores_the_running_hand_off(cache):
    """A 409 on a continuation means the thread is mid-turn: the model is told
    what it can do instead of retrying, and the rollback hands the origin back
    to the hand-off still running there, whose report-back depends on it."""
    from src.tools.secretary.dispatch import _THREAD_BUSY

    _seed_predecessor(cache)
    payload = await _continuation_dispatch(resp=FakeResp(status=409))

    assert payload == {"success": False, "error": _THREAD_BUSY}
    assert cache.kv[keys.ptc_origin_key(PTC_THREAD_ID)]["dispatch_gen"] == PRIOR_GEN
    assert PTC_THREAD_ID in cache.client.sets[keys.watch_key(DISPATCHER_THREAD_ID)]


@pytest.mark.asyncio
async def test_continuation_probe_down_retains_unknown(cache, ledger):
    """Continuation + unanswerable probe: can't tell our admission from the
    predecessor's — retain the provisional generation as unknown rather than
    guess a rollback that could erase a live run's wiring."""
    _seed_predecessor(cache)
    _ledger_probe_raises(ledger)
    payload = await _continuation_dispatch(aiohttp.ServerDisconnectedError())

    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"
    assert cache.kv[keys.ptc_origin_key(PTC_THREAD_ID)]["dispatch_gen"] != PRIOR_GEN


# ---------------------------------------------------------------------------
# A configured INTERNAL_SERVICE_TOKEN is the normal production state and, with
# auth enabled, a precondition for dispatch (the preflight guard aborts without
# it). These tests exercise the dispatch path itself, not the guard, so give
# the whole module a token regardless of the ambient environment. The guard
# test module asserts the unset behaviour separately.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _internal_service_token(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "test-internal-service-token")
