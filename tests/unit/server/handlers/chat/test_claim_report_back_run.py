"""Idempotent report-back run claim/release (server-side dedup of a retried POST).

``claim_report_back_run`` is the server-side guard that closes the report-back
double-deliver: a lost-response retry (or a drain re-POST after a crash) must NOT
start a second summary run. One Lua claims the per-(flash, ptc) run pointer;
identity is the POST's deterministic request_key — a prior admission of the SAME
POST makes the retry return that run, while ANY other job's pointer (a stale
incarnation's leftover, or a newer incarnation's terminal pointer a legacy job
must not adopt) is replaced.

The fake faithfully models the contract the helper depends on: the RAW client
holds JSON strings, and the claim/release scripts decode them — exactly how
RedisCache splits raw writes from decoded reads.
"""

from __future__ import annotations

import json

import pytest

from src.server.handlers.chat import report_back as rb
from src.server.handlers.chat.report_back import (
    claim_report_back_run,
    flash_rb_run_key,
    release_report_back_run,
)


class _Cache:
    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled
        self.client = self if enabled else None
        self.kv: dict[str, str] = {}
        # The claim's write is membership-gated; every claim test models a
        # live pair unless it removes the member explicitly.
        self.sets: dict[str, set[str]] = {
            rb.flash_watch_key("flash-1"): {"ptc-1"}
        }

    async def get(self, key):
        raw = self.kv.get(key)
        if raw is None:
            return None
        return json.loads(raw) if isinstance(raw, (str, bytes)) else raw

    async def delete(self, key):
        self.kv.pop(key, None)

    def _decoded(self, key):
        raw = self.kv.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return None

    async def eval(self, script, numkeys, *args):
        keys, argv = args[:numkeys], args[numkeys:]
        if script is rb._CLAIM_POINTER_LUA:
            watch_key, run_key = keys
            ptc_id, value, ttl, request_key, gen = argv
            data = self._decoded(run_key)
            if isinstance(data, dict) and isinstance(data.get("run_id"), str):
                if isinstance(data.get("request_key"), str):
                    if request_key == "" or data["request_key"] == request_key:
                        return [0, data["run_id"]]
                elif (request_key == "" and gen == "") or (
                    gen != ""
                    and isinstance(data.get("dispatch_gen"), str)
                    and data["dispatch_gen"] == gen
                ):
                    return [0, data["run_id"]]
            if ptc_id not in self.sets.get(watch_key, set()):
                return [2, ""]
            self.kv[run_key] = value
            return [1, ""]
        if script is rb._POINTER_COMPARE_DELETE_LUA:
            data = self._decoded(keys[0])
            if isinstance(data, dict) and data.get("run_id") == argv[0]:
                self.kv.pop(keys[0], None)
                return 1
            return 0
        raise AssertionError(f"unknown Lua script: {script[:60]!r}")


@pytest.mark.asyncio
async def test_claim_when_no_incumbent_claims_and_writes_pointer():
    cache = _Cache()
    won, claimed, _ = await claim_report_back_run(cache, "flash-1", "ptc-1", "run-1")
    assert (won, claimed) == ("run-1", True)
    # Pointer persisted in the shape the drain gate reads ({"run_id": ...}).
    stored = json.loads(cache.kv[flash_rb_run_key("flash-1", "ptc-1")])
    assert stored == {"run_id": "run-1"}


@pytest.mark.asyncio
async def test_fully_legacy_claim_adopts_fully_legacy_incumbent():
    """Pure pre-deploy path: bare pointer, bare claimer -> adopt, no overwrite."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps({"run_id": "run-A"})
    won, claimed, _ = await claim_report_back_run(cache, "flash-1", "ptc-1", "run-B")
    assert (won, claimed) == ("run-A", False)
    assert json.loads(cache.kv[key]) == {"run_id": "run-A"}


@pytest.mark.asyncio
async def test_claim_scopes_idempotency_to_the_request_key():
    """The idempotency identity is the POST's request_key: the same POST
    retried adopts its own prior admission; a DIFFERENT job replaces the
    pointer instead of adopting a summary that already ran (or never will)."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps(
        {"run_id": "run-A", "dispatch_gen": "g-1", "request_key": "rk-A"}
    )

    won, claimed, _ = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-A-retry", "g-1", "rk-A"
    )
    assert (won, claimed) == ("run-A", False)

    won, claimed, _ = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-B", "g-2", "rk-B"
    )
    assert (won, claimed) == ("run-B", True)
    assert json.loads(cache.kv[key]) == {
        "run_id": "run-B",
        "dispatch_gen": "g-2",
        "request_key": "rk-B",
    }


@pytest.mark.asyncio
async def test_legacy_job_never_adopts_another_jobs_pointer():
    """Codex round-5 F3: a LEGACY job (no dispatch_gen — pre-deploy row) still
    carries its own request_key; it must REPLACE a newer incarnation's
    lingering terminal pointer, not adopt it and drop its summary."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps(
        {"run_id": "run-G2-done", "dispatch_gen": "g-2", "request_key": "rk-G2"}
    )
    won, claimed, _ = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-legacy", None, "rk-legacy"
    )
    assert (won, claimed) == ("run-legacy", True)
    assert json.loads(cache.kv[key])["request_key"] == "rk-legacy"


@pytest.mark.asyncio
async def test_claimer_without_request_key_adopts_any_incumbent():
    """A claimer that can't prove identity (manual POST, no request_key)
    adopts whatever is there — stomping a live pointer would be worse."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps(
        {"run_id": "run-A", "dispatch_gen": "g-1", "request_key": "rk-A"}
    )
    won, claimed, _ = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-X", None, None
    )
    assert (won, claimed) == ("run-A", False)


@pytest.mark.asyncio
async def test_old_format_pointer_falls_back_to_generation_scoping():
    """Transitional: a pointer written before request_keys existed carries
    only a gen. Same gen adopts; a different generation replaces; and a
    request-key-carrying claimer never adopts on gen absence alone."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps({"run_id": "run-G1", "dispatch_gen": "g-1"})

    won, claimed, _ = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-retry", "g-1", "rk-1"
    )
    assert (won, claimed) == ("run-G1", False)

    # Legacy claimer WITH a request_key vs a generated old-format pointer:
    # replace (adopting an unrelated generated pointer is the F3 hole).
    won, claimed, _ = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-legacy", None, "rk-legacy"
    )
    assert (won, claimed) == ("run-legacy", True)


@pytest.mark.asyncio
async def test_claim_replaces_a_stale_incarnations_pointer():
    """A NEW generation replaces a stale incarnation's old-format pointer
    (its gen-mismatched clear left it behind)."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps({"run_id": "run-G1", "dispatch_gen": "g-1"})
    won, claimed, _ = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-G2", "g-2", "rk-2"
    )
    assert (won, claimed) == ("run-G2", True)
    assert json.loads(cache.kv[key])["run_id"] == "run-G2"


class _RaisingCache(_Cache):
    """The claim script itself fails (transport blip mid-eval)."""

    async def eval(self, script, numkeys, *args):
        raise RuntimeError("redis down")


@pytest.mark.asyncio
async def test_claim_fails_open_when_script_fails():
    """A Redis hiccup at the claim must not 500 the admission -> degrade to
    (run_id, True) so the dispatch still proceeds (a lost claim must not
    stall a completed analysis at the admission gate)."""
    cache = _RaisingCache()
    won, claimed, _ = await claim_report_back_run(cache, "flash-1", "ptc-1", "run-1")
    assert (won, claimed) == ("run-1", True)
    # Nothing persisted — we degraded to claimed without writing a bogus pointer.
    assert cache.kv == {}


@pytest.mark.asyncio
async def test_claim_when_cache_disabled_proceeds_without_write():
    cache = _Cache(enabled=False)
    won, claimed, _ = await claim_report_back_run(cache, "flash-1", "ptc-1", "run-1")
    # No idempotency available, but the dispatch must still proceed.
    assert (won, claimed) == ("run-1", True)
    assert cache.kv == {}


@pytest.mark.asyncio
async def test_claim_refuses_to_resurrect_a_pointer_after_the_pair_fell():
    """Codex round-17 P1: a resolution (or terminal clear) that dropped the
    watch membership AND the pointer must not have the pointer resurrected by
    a late admission-time claim — the pair reads settled while an orphan
    summary and pointer exist. The write is membership-gated in the same
    script; the route refuses the dispatch on ``pair_gone``."""
    cache = _Cache()
    cache.sets[rb.flash_watch_key("flash-1")].discard("ptc-1")

    result = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-late", "g-2", "rk-late"
    )

    assert result == (None, False, True)
    assert result.pair_gone is True
    assert cache.kv == {}  # nothing written behind the settled pair

    # A lost-response retry of a PRIOR admission still finds its run: adoption
    # writes nothing, so it stays legal even after the pair fell.
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps(
        {"run_id": "run-A", "dispatch_gen": "g-1", "request_key": "rk-A"}
    )
    won, claimed, pair_gone = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-A-retry", "g-1", "rk-A"
    )
    assert (won, claimed, pair_gone) == ("run-A", False, False)


@pytest.mark.asyncio
async def test_release_deletes_only_when_pointer_is_ours():
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps({"run_id": "run-A"})

    # A release for a different run must not delete someone else's pointer.
    await release_report_back_run(cache, "flash-1", "ptc-1", "run-B")
    assert key in cache.kv

    # Our own release removes it (so a later retry isn't short-circuited).
    await release_report_back_run(cache, "flash-1", "ptc-1", "run-A")
    assert key not in cache.kv
