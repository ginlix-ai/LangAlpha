"""Failed-dispatch lifecycle: auto-created workspaces AND the cap reservation.

Dispatch without a ``workspace_id`` provisions a workspace (real sandbox,
~8-10s) before ``reserve()`` admits the dispatch. A failed dispatch must not
leak that sandbox when the failure proves the run never started: a
deterministic cap hit is pre-checked BEFORE provisioning
(``check_dispatch_capacity``), the residual pre-check/reserve race deletes the
just-created workspace on ``slot.error`` (no HTTP was sent), and a >=400
dispatch response does too (the endpoint's error paths all precede its
create_task). Ambiguous outcomes (timeout, mid-flight transport loss, odd
statuses/bodies) reconcile against the endpoint's admission marker, scoped
to THIS dispatch's generation (the endpoint stamps the POST's
``origin_dispatch_gen`` into the marker and refuses to schedule if the
write fails): a marker carrying OUR generation → delivered, plain success.
The marker is POSITIVE-ONLY and reconciliation NEVER rolls back — no
finite absence proves a delivered request won't admit later, and a foreign
marker may be a stale terminal our own admission is about to replace — so
every unproven verdict retains the reservation (TTL-bounded, orphan-reaped
once the origin lapses). Rollback happens only on definitive receipts: a
>=400 status, or a provably-undelivered request (connection never
established).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from src.server.handlers.chat import report_back as rb
from src.tools.secretary.tools import ptc_agent
from tests.unit.server.handlers.chat.redis_fakes import FakeCache as _FakeCache

USER_ID = "user-1"
FLASH_THREAD_ID = "flash-thread-1"
NEW_WORKSPACE_ID = "33333333-3333-3333-3333-333333333333"


def _tool_call(args: dict, call_id: str = "call_test") -> dict:
    return {"name": "ptc_agent", "args": args, "id": call_id, "type": "tool_call"}


def _config() -> dict:
    # thread_id = the dispatching flash thread -> report_back wiring is live.
    return {"configurable": {"user_id": USER_ID, "thread_id": FLASH_THREAD_ID}}


def _payload(result) -> dict:
    return json.loads(result.update["messages"][0].content)


def _manager(delete: AsyncMock | None = None) -> MagicMock:
    mgr = MagicMock()
    mgr.create_workspace = AsyncMock(return_value={"workspace_id": NEW_WORKSPACE_ID})
    mgr.delete_workspace = delete or AsyncMock(return_value=True)
    return mgr


@pytest.fixture
def cache(monkeypatch):
    c = _FakeCache()
    monkeypatch.setattr("src.utils.cache.redis_cache.get_cache_client", lambda: c)
    return c


def _fill_flash_cap(cache) -> None:
    cache.client.sets[rb.flash_watch_key(FLASH_THREAD_ID)] = {
        f"p{i}" for i in range(rb.MAX_DISPATCH_PER_FLASH)
    }
    # Live origins too: originless members would be reaped as orphans on the
    # over-cap retry, and the cap wouldn't hold.
    for i in range(rb.MAX_DISPATCH_PER_FLASH):
        cache.kv[rb.ptc_origin_key(f"p{i}")] = {
            "flash_thread_id": FLASH_THREAD_ID,
            "report_back": True,
        }


class _FakeResp:
    def __init__(self, status: int = 200, body: dict | None = None) -> None:
        self.status = status
        self._body = body if body is not None else {"status": "dispatched"}

    async def __aenter__(self) -> "_FakeResp":
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    async def json(self) -> dict:
        return self._body


class _FakeSession:
    def __init__(
        self, resp: _FakeResp | None = None, post_exc: Exception | None = None
    ) -> None:
        self._resp = resp
        self._post_exc = post_exc

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *_exc) -> bool:
        return False

    def post(self, *_args, **_kwargs) -> _FakeResp:
        if self._post_exc is not None:
            raise self._post_exc
        return self._resp


@pytest.mark.asyncio
async def test_precheck_rejection_skips_workspace_creation(cache):
    """A deterministic cap hit fails BEFORE any sandbox is provisioned."""
    _fill_flash_cap(cache)
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("dispatch must not run")),
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert "too many concurrent analyses" in payload["error"]
    mgr.create_workspace.assert_not_awaited()
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_reserve_rejection_deletes_auto_created_workspace(cache):
    """The pre-check/reserve race path: the cap fills between the pre-check and
    reserve(), so the just-created workspace must be deleted, not leaked."""
    _fill_flash_cap(cache)
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        # Simulate the race: the pre-check saw capacity, reserve() did not.
        "src.server.handlers.chat.report_back.check_dispatch_capacity",
        new=AsyncMock(return_value=None),
    ), patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("dispatch must not run")),
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert "too many concurrent analyses" in payload["error"]
    mgr.create_workspace.assert_awaited_once()
    mgr.delete_workspace.assert_awaited_once_with(NEW_WORKSPACE_ID)


@pytest.mark.asyncio
async def test_cleanup_failure_still_returns_the_cap_error(cache):
    """A failed best-effort delete must not mask the cap rejection."""
    _fill_flash_cap(cache)
    mgr = _manager(delete=AsyncMock(side_effect=RuntimeError("sandbox teardown failed")))
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "src.server.handlers.chat.report_back.check_dispatch_capacity",
        new=AsyncMock(return_value=None),
    ), patch(
        "aiohttp.ClientSession",
        MagicMock(side_effect=AssertionError("dispatch must not run")),
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert "too many concurrent analyses" in payload["error"]
    mgr.delete_workspace.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_error_status_deletes_auto_created_workspace(cache):
    """A >=400 dispatch response (e.g. the credit gate) proves the run never
    started — the endpoint's error paths all precede its create_task — so the
    just-created workspace is deleted, not leaked."""
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession", return_value=_FakeSession(_FakeResp(status=402))
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    mgr.create_workspace.assert_awaited_once()
    mgr.delete_workspace.assert_awaited_once_with(NEW_WORKSPACE_ID)


@pytest.mark.asyncio
async def test_dispatch_timeout_with_probe_down_keeps_workspace_and_reservation(cache):
    """A timed-out dispatch may have started the run server-side; when the
    admission-marker probe can't answer either (Redis blip), both the
    workspace and the report-back reservation must survive."""
    mgr = _manager()
    _marker_probe_raises(cache)
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession", return_value=_FakeSession(post_exc=TimeoutError())
    ), patch(
        "src.tools.secretary.tools._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_timeout"
    assert payload["outcome"] == "unknown_retained"
    mgr.create_workspace.assert_awaited_once()
    mgr.delete_workspace.assert_not_awaited()
    assert _reservation(cache) == payload["thread_id"]


# ---------------------------------------------------------------------------
# Reservation retention across dispatch-response loss (Codex round-6 F2).
# The internal endpoint's 2xx is sent only after its create_task, so a success
# STATUS proves the run is scheduled even when the body never arrives — a
# rollback at that point would erase the origin the run's report-back needs.
# ---------------------------------------------------------------------------


def _reservation(cache) -> str | None:
    """The reserved ptc thread id, or None when the reservation was rolled back."""
    members = cache.client.sets.get(rb.flash_watch_key(FLASH_THREAD_ID), set())
    if not members:
        return None
    (ptc,) = members
    assert rb.ptc_origin_key(ptc) in cache.kv  # membership implies live origin
    return ptc


def _marker_present(cache) -> None:
    """Make the admission-marker probe see OUR request's blob: the endpoint
    stamps the POST's origin_dispatch_gen into the marker, so emulate it by
    reading the generation off the origin the reservation just wrote (the
    dispatched thread id is minted inside the tool)."""
    real = cache.get_strict

    async def _blobbed(key):
        if key.startswith("workflow:status:"):
            tid = key.removeprefix("workflow:status:")
            o = cache.kv.get(rb.ptc_origin_key(tid))
            gen = o.get("dispatch_gen") if isinstance(o, dict) else None
            return {"status": "active", "metadata": {"origin_dispatch_gen": gen}}
        return await real(key)

    cache.get_strict = _blobbed


def _marker_with_gen(cache, gen: str) -> None:
    """Marker probe sees a blob stamped with a FIXED foreign generation —
    another run's admission (e.g. a still-active predecessor), not ours."""
    real = cache.get_strict

    async def _blobbed(key):
        if key.startswith("workflow:status:"):
            return {"status": "active", "metadata": {"origin_dispatch_gen": gen}}
        return await real(key)

    cache.get_strict = _blobbed


def _marker_probe_raises(cache) -> None:
    """Make the admission-marker probe fail (Redis blip) — verdict unknown."""
    real = cache.get_strict

    async def _boom(key):
        if key.startswith("workflow:status:"):
            raise ConnectionError("redis blip")
        return await real(key)

    cache.get_strict = _boom


class _LostBodyResp(_FakeResp):
    async def json(self) -> dict:
        raise aiohttp.ClientPayloadError("response body lost")


@pytest.mark.asyncio
async def test_success_status_with_lost_body_commits_and_reports_dispatched(cache):
    """A 2xx whose body can't be read is a DISPATCHED run: the status alone
    proves scheduling, so the reservation commits before the body parse and
    the tool reports success."""
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession", return_value=_FakeSession(_LostBodyResp(status=200))
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is True
    assert payload["status"] == "dispatched"
    assert _reservation(cache) == payload["thread_id"]
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_loss_with_admission_marker_reports_success(cache):
    """Codex round-8 F1: a lost exchange whose admission marker appears is a
    DELIVERED dispatch — the tool reports plain success (no unknown_retained
    ambiguity for the model to mis-handle)."""
    mgr = _manager()
    _marker_present(cache)
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession",
        return_value=_FakeSession(post_exc=aiohttp.ServerDisconnectedError()),
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is True
    assert payload["status"] == "dispatched"
    assert _reservation(cache) == payload["thread_id"]
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_loss_with_no_marker_on_fresh_pair_retains(cache):
    """Codex round-9 F2: clean absence is NOT a negative proof — the endpoint
    may still be mid-admission (platform auth/credit checks can outlast the
    grace) — and nothing pre-existing can be lost by keeping a fresh pair, so
    the reservation and workspace are retained as unknown (TTL-bounded)."""
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession",
        return_value=_FakeSession(post_exc=aiohttp.ServerDisconnectedError()),
    ), patch(
        "src.tools.secretary.tools._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_foreign_marker_on_fresh_pair_is_not_success(cache):
    """Codex round-9 F3: gen-scoping — a marker that doesn't carry OUR
    generation no longer confirms the dispatch. On a fresh pair it retains
    as unknown instead of claiming success."""
    mgr = _manager()
    _marker_with_gen(cache, "someone-elses-gen")
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession",
        return_value=_FakeSession(post_exc=aiohttp.ServerDisconnectedError()),
    ), patch(
        "src.tools.secretary.tools._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_loss_with_probe_down_retains_unknown(cache):
    """When the marker probe itself fails, the outcome stays unknown — keep
    the reservation (TTL-bounded) and surface the retained thread id so the
    model can check agent_output instead of blind-re-dispatching."""
    mgr = _manager()
    _marker_probe_raises(cache)
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession",
        return_value=_FakeSession(post_exc=aiohttp.ServerDisconnectedError()),
    ), patch(
        "src.tools.secretary.tools._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancellation_mid_exchange_retains_reservation(cache):
    """Codex round-7 F1: CancelledError (flash turn cancelled, worker
    shutdown) bypasses the except clauses — it must commit before
    propagating, or the CM rollback erases a possibly-scheduled run's
    report-back wiring."""
    import asyncio

    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession",
        return_value=_FakeSession(post_exc=asyncio.CancelledError()),
    ):
        with pytest.raises(asyncio.CancelledError):
            await ptc_agent.ainvoke(
                _tool_call({"question": "analyze this"}), config=_config()
            )

    assert _reservation(cache) is not None
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_200_success_status_is_not_scheduling_proof(cache):
    """Codex round-7 F2: a 3xx/2xx-non-200 is not the endpoint's reply (it
    answers exactly 200; redirects are disabled) — never success. Delivery
    stays unproven either way, so the fresh pair retains as unknown."""
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession", return_value=_FakeSession(_FakeResp(status=302))
    ), patch(
        "src.tools.secretary.tools._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_dict_200_body_reconciles_without_raising(cache):
    """Codex round-8 F3: a 200 carrying valid non-dict JSON (``[]``/``null``)
    must not AttributeError out of the tool — the 200 keeps the reservation
    committed (status proof stands) and the outcome reconciles as unknown
    when the marker can't confirm."""
    mgr = _manager()
    _marker_probe_raises(cache)
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession",
        return_value=_FakeSession(_FakeResp(status=200, body=[])),
    ), patch(
        "src.tools.secretary.tools._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_200_with_contradictory_body_never_rolls_back(cache):
    """A clean marker-absent verdict after a REAL 200 must still retain: the
    exact-status proof outranks the probe (the two contradicting is a state
    we can't explain, so the safe side is keep)."""
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession",
        return_value=_FakeSession(_FakeResp(status=200, body={"status": "nope"})),
    ), patch(
        "src.tools.secretary.tools._DISPATCH_CONFIRM_GRACE_S", 0.0
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"
    assert _reservation(cache) == payload["thread_id"]
    mgr.delete_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_during_post_rejection_cleanup_does_not_commit(cache):
    """Codex round-8 F2: a >=400 response settles non-scheduling BEFORE the
    best-effort workspace cleanup — a cancellation landing during that
    cleanup must not commit the already-dead reservation."""
    import asyncio

    mgr = _manager(delete=AsyncMock(side_effect=asyncio.CancelledError()))
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession", return_value=_FakeSession(_FakeResp(status=402))
    ):
        with pytest.raises(asyncio.CancelledError):
            await ptc_agent.ainvoke(
                _tool_call({"question": "analyze this"}), config=_config()
            )

    assert _reservation(cache) is None


@pytest.mark.asyncio
async def test_connection_never_established_rolls_back_reservation(cache):
    """A refused connection proves the request never reached the endpoint:
    the reservation rolls back and the auto-created workspace is deleted."""
    mgr = _manager()
    with patch(
        "src.tools.secretary.tools._hitl_confirm", return_value=(True, {})
    ), patch(
        "src.server.services.workspace_manager.WorkspaceManager.get_instance",
        return_value=mgr,
    ), patch(
        "aiohttp.ClientSession",
        return_value=_FakeSession(
            post_exc=aiohttp.ClientConnectorError(MagicMock(), OSError("refused"))
        ),
    ):
        result = await ptc_agent.ainvoke(
            _tool_call({"question": "analyze this"}), config=_config()
        )

    payload = _payload(result)
    assert payload["success"] is False
    assert payload["error"] == "dispatch_failed"
    assert _reservation(cache) is None
    mgr.delete_workspace.assert_awaited_once_with(NEW_WORKSPACE_ID)


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
    """A prior wired dispatch of the same (flash, ptc) pair, still live."""
    cache.client.sets[rb.flash_watch_key(FLASH_THREAD_ID)] = {PTC_THREAD_ID}
    cache.client.sets[rb.flash_user_pending_key(USER_ID)] = {PTC_THREAD_ID}
    cache.kv[rb.ptc_origin_key(PTC_THREAD_ID)] = {
        "origin": "flash",
        "report_back": True,
        "flash_thread_id": FLASH_THREAD_ID,
        "ptc_thread_id": PTC_THREAD_ID,
        "user_id": USER_ID,
        "dispatch_gen": PRIOR_GEN,
    }


def _continuation_patches(post_exc: Exception) -> list:
    owner = AsyncMock(return_value=USER_ID)
    by_id = AsyncMock(
        return_value={
            "conversation_thread_id": PTC_THREAD_ID,
            "workspace_id": CONT_WORKSPACE_ID,
        }
    )
    return [
        patch("src.server.database.conversation.get_thread_owner_id", owner),
        patch("src.server.database.conversation.get_thread_by_id", by_id),
        patch("src.tools.secretary.tools._hitl_confirm", return_value=(True, {})),
        patch(
            "aiohttp.ClientSession",
            return_value=_FakeSession(post_exc=post_exc),
        ),
        patch("src.tools.secretary.tools._DISPATCH_CONFIRM_GRACE_S", 0.0),
    ]


async def _continuation_dispatch(post_exc: Exception) -> dict:
    import contextlib

    with contextlib.ExitStack() as stack:
        for p in _continuation_patches(post_exc):
            stack.enter_context(p)
        result = await ptc_agent.ainvoke(
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
    assert cache.kv[rb.ptc_origin_key(PTC_THREAD_ID)]["dispatch_gen"] != PRIOR_GEN
    assert PTC_THREAD_ID in cache.client.sets[rb.flash_watch_key(FLASH_THREAD_ID)]


@pytest.mark.asyncio
async def test_continuation_foreign_marker_is_not_success(cache):
    """Codex round-9 F3 + round-10 F2: the still-active predecessor's own
    marker must NOT confirm our provisional generation (no false success) —
    but it is not a rejection receipt either, so the provisional reservation
    retains rather than rolling back (our own stamp may land next)."""
    _seed_predecessor(cache)
    _marker_with_gen(cache, PRIOR_GEN)
    payload = await _continuation_dispatch(aiohttp.ServerDisconnectedError())

    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"
    assert cache.kv[rb.ptc_origin_key(PTC_THREAD_ID)]["dispatch_gen"] != PRIOR_GEN
    assert PTC_THREAD_ID in cache.client.sets[rb.flash_watch_key(FLASH_THREAD_ID)]


@pytest.mark.asyncio
async def test_unwired_continuation_never_reads_a_marker_as_ours(cache):
    """Codex round-10 F1: an unwired dispatch carries no generation, so a
    marker on a continuation thread proves only that SOME run held it — a
    prior run's blob must not upgrade a lost 409 into plain success."""
    _marker_with_gen(cache, "prior-runs-gen")
    import contextlib

    with contextlib.ExitStack() as stack:
        for p in _continuation_patches(aiohttp.ServerDisconnectedError()):
            stack.enter_context(p)
        result = await ptc_agent.ainvoke(
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
async def test_continuation_our_marker_reports_success(cache):
    """Continuation + a marker carrying OUR generation: the lost reply was a
    real acceptance — commit the new incarnation and report plain success."""
    _seed_predecessor(cache)
    _marker_present(cache)
    payload = await _continuation_dispatch(aiohttp.ServerDisconnectedError())

    assert payload["success"] is True
    assert payload["status"] == "dispatched"
    new_gen = cache.kv[rb.ptc_origin_key(PTC_THREAD_ID)]["dispatch_gen"]
    assert new_gen != PRIOR_GEN


@pytest.mark.asyncio
async def test_continuation_probe_down_retains_unknown(cache):
    """Continuation + unanswerable probe: can't tell our admission from the
    predecessor's — retain the provisional generation as unknown rather than
    guess a rollback that could erase a live run's wiring."""
    _seed_predecessor(cache)
    _marker_probe_raises(cache)
    payload = await _continuation_dispatch(aiohttp.ServerDisconnectedError())

    assert payload["success"] is False
    assert payload["outcome"] == "unknown_retained"
    assert cache.kv[rb.ptc_origin_key(PTC_THREAD_ID)]["dispatch_gen"] != PRIOR_GEN


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
