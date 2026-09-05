"""Tests for src/tools/web/router.py — chain semantics with fake adapters."""

import logging
from typing import Dict, List

import pytest

import src.tools.web.router as router_module
from src.tools.decorators import start_tool_tracking, stop_tool_tracking
from src.tools.web.manifest import CapabilitySpec, LevelSpec, WebProviderSpec
from src.tools.web.router import FetchRouter
from src.tools.web.types import (
    FetchRequest,
    FetchResponse,
    FetchResult,
    WebError,
    WebErrorType,
)


class FakeAdapter:
    """Scripted adapter: url -> WebErrorType (or None for success)."""

    def __init__(self, name: str, outcomes: Dict[str, object]):
        self.name = name
        self.outcomes = outcomes
        self.calls: List[FetchRequest] = []
        self.native_params_seen: List[Dict] = []

    async def fetch(self, req: FetchRequest, native_params: Dict) -> FetchResponse:
        self.calls.append(req)
        self.native_params_seen.append(native_params)
        results = []
        for url in req.urls:
            outcome = self.outcomes.get(url)
            if outcome is None:
                results.append(FetchResult(url=url, markdown=f"content of {url}"))
            elif isinstance(outcome, WebError):
                results.append(FetchResult(url=url, error=outcome))
            else:
                results.append(
                    FetchResult(url=url, error=WebError(type=outcome, message="scripted"))
                )
        return FetchResponse(results=results, provider=self.name)


def _spec(name: str, levels=None, env_key=None, max_batch_size=10) -> WebProviderSpec:
    levels = levels or [
        LevelSpec(
            name="standard", display_name="Standard", native_params={}, min_tier=None,
            credits=1,
        )
    ]
    cap = CapabilitySpec(
        verb="fetch",
        tracking_name=f"{name.capitalize()}FetchTool",
        min_tier=None,
        default_level=levels[0].name,
        levels=tuple(levels),
        max_batch_size=max_batch_size,
    )
    return WebProviderSpec(
        name=name, display_name=name, env_key=env_key, capabilities={"fetch": cap}
    )


@pytest.fixture
def make_router(monkeypatch):
    """Router over fake adapters; providers need no env keys unless given one."""

    def _make(adapters: List[FakeAdapter], specs: Dict[str, WebProviderSpec] = None):
        specs = specs or {a.name: _spec(a.name) for a in adapters}
        monkeypatch.setattr(
            router_module, "get_web_provider_spec", lambda name: specs.get(name)
        )
        monkeypatch.setattr(
            router_module,
            "_ADAPTER_BUILDERS",
            {a.name: (lambda a=a: a) for a in adapters},
        )
        return FetchRouter([a.name for a in adapters])

    return _make


URL_A = "https://a.example/1"
URL_B = "https://b.example/2"


class TestAttemptOutcome:
    """The attempt label is the telemetry a fall-through is read from."""

    def _failed(self, native_kind, provider_fault, etype=WebErrorType.PROVIDER_ERROR):
        return FetchResult(
            url=URL_A,
            error=WebError(
                type=etype,
                message="scripted",
                provider_fault=provider_fault,
                native_kind=native_kind,
            ),
        )

    def test_prefers_the_providers_own_name_for_the_failure(self):
        """PROVIDER_ERROR buckets a dead target and a broken provider together,
        so a label built from the type alone cannot tell the two apart."""
        dead_target = self._failed("dns_error", provider_fault=False)
        our_capacity = self._failed("queue_full", provider_fault=True)

        assert str(router_module._attempt_outcome("inhouse", [dead_target])) == "inhouse:dns_error"
        assert str(router_module._attempt_outcome("inhouse", [our_capacity])) == "inhouse:queue_full"

    def test_falls_back_to_the_normalized_type(self):
        """Providers whose taxonomy is already granular carry no native kind."""
        timed_out = self._failed(None, provider_fault=False, etype=WebErrorType.TIMEOUT)

        assert str(router_module._attempt_outcome("exa", [timed_out])) == "exa:timeout"


@pytest.mark.asyncio
class TestChainSemantics:
    async def test_primary_success_stops_chain(self, make_router):
        first = FakeAdapter("first", {})
        second = FakeAdapter("second", {})
        router = make_router([first, second])

        resp = await router.fetch(FetchRequest(urls=[URL_A]))

        assert resp.results[0].ok
        assert resp.provider == "first"
        assert resp.providers_tried == ["first"]
        assert not second.calls

    async def test_retryable_error_falls_through_to_inhouse(self, make_router):
        first = FakeAdapter("first", {URL_A: WebErrorType.TIMEOUT})
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([first, inhouse])

        resp = await router.fetch(FetchRequest(urls=[URL_A]))

        assert resp.results[0].ok
        assert resp.provider == "inhouse"
        assert resp.providers_tried == ["first", "inhouse"]

    async def test_only_degrades_to_inhouse_not_a_second_provider(self, make_router):
        """A primary failure skips other third parties and drops to in-house."""
        first = FakeAdapter("first", {URL_A: WebErrorType.TIMEOUT})
        other = FakeAdapter("other", {})  # a second third party — must be inert
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([first, other, inhouse])

        resp = await router.fetch(FetchRequest(urls=[URL_A]))

        assert resp.results[0].ok
        assert resp.provider == "inhouse"
        assert not other.calls
        assert resp.providers_tried == ["first", "inhouse"]

    async def test_terminal_error_fails_fast(self, make_router):
        first = FakeAdapter("first", {URL_A: WebErrorType.NOT_FOUND})
        second = FakeAdapter("second", {})
        router = make_router([first, second])

        resp = await router.fetch(FetchRequest(urls=[URL_A]))

        assert not resp.results[0].ok
        assert resp.results[0].error.type == WebErrorType.NOT_FOUND
        assert not second.calls

    async def test_credential_failure_falls_through(self, make_router):
        """A provider-API auth/budget failure (revoked key, exhausted account)
        is provider-scoped and retryable — the URL must reach in-house rather
        than fail."""
        for etype in (WebErrorType.FORBIDDEN, WebErrorType.BUDGET_EXCEEDED):
            first = FakeAdapter(
                "first",
                {URL_A: WebError(type=etype, message="key rejected", retryable=True)},
            )
            inhouse = FakeAdapter("inhouse", {})
            router = make_router([first, inhouse])

            resp = await router.fetch(FetchRequest(urls=[URL_A]))

            assert resp.results[0].ok
            assert resp.provider == "inhouse"
            assert inhouse.calls

    async def test_partial_batch_falls_through_per_url(self, make_router):
        first = FakeAdapter("first", {URL_B: WebErrorType.PROVIDER_ERROR})
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([first, inhouse])

        resp = await router.fetch(FetchRequest(urls=[URL_A, URL_B]))

        assert [r.ok for r in resp.results] == [True, True]
        assert inhouse.calls and inhouse.calls[0].urls == [URL_B]

    async def test_exhausted_chain_returns_last_error(self, make_router):
        first = FakeAdapter("first", {URL_A: WebErrorType.TIMEOUT})
        inhouse = FakeAdapter("inhouse", {URL_A: WebErrorType.RATE_LIMITED})
        router = make_router([first, inhouse])

        resp = await router.fetch(FetchRequest(urls=[URL_A]))

        assert not resp.results[0].ok
        assert resp.results[0].error.type == WebErrorType.RATE_LIMITED

    async def test_results_preserve_request_order(self, make_router):
        first = FakeAdapter("first", {URL_A: WebErrorType.TIMEOUT})
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([first, inhouse])

        resp = await router.fetch(FetchRequest(urls=[URL_A, URL_B]))

        assert [r.url for r in resp.results] == [URL_A, URL_B]

    async def test_provider_without_env_key_is_skipped(self, make_router, monkeypatch):
        monkeypatch.delenv("NOPE_API_KEY", raising=False)
        keyed = FakeAdapter("keyed", {})
        fallback = FakeAdapter("fallback", {})
        specs = {
            "keyed": _spec("keyed", env_key="NOPE_API_KEY"),
            "fallback": _spec("fallback"),
        }
        router = make_router([keyed, fallback], specs)

        resp = await router.fetch(FetchRequest(urls=[URL_A]))

        assert resp.provider == "fallback"
        assert not keyed.calls

    async def test_unknown_chain_entries_fall_back_to_inhouse(self, monkeypatch):
        inhouse = FakeAdapter("inhouse", {})
        monkeypatch.setattr(
            router_module,
            "get_web_provider_spec",
            lambda name: _spec("inhouse") if name == "inhouse" else None,
        )
        monkeypatch.setattr(router_module, "_ADAPTER_BUILDERS", {"inhouse": lambda: inhouse})

        router = FetchRouter(["typo-provider"])

        assert router.provider_names == ["inhouse"]

    async def test_batch_split_respects_max_batch_size(self, make_router):
        small = FakeAdapter("small", {})
        router = make_router([small], {"small": _spec("small", max_batch_size=2)})

        urls = [f"https://x.example/{i}" for i in range(5)]
        resp = await router.fetch(FetchRequest(urls=urls))

        assert all(r.ok for r in resp.results)
        assert [len(c.urls) for c in small.calls] == [2, 2, 1]

    async def test_anti_bot_without_stealth_falls_to_inhouse(self, make_router):
        """A primary with no stealth level can't escalate — the anti_bot URL
        falls straight through to in-house."""
        plain = FakeAdapter("plain", {URL_A: WebErrorType.ANTI_BOT})
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([plain, inhouse])

        resp = await router.fetch(FetchRequest(urls=[URL_A]))

        assert resp.results[0].ok
        assert resp.provider == "inhouse"

    async def test_anti_bot_escalates_on_the_same_provider(self, make_router):
        """A provider that reports anti_bot at its default level gets a second
        attempt at its own stealth level before the chain moves on."""

        class AntiBotUnlessStealth(FakeAdapter):
            async def fetch(self, req, native_params):
                self.calls.append(req)
                self.native_params_seen.append(native_params)
                if native_params.get("proxy") == "stealth":
                    results = [
                        FetchResult(url=u, markdown=f"content of {u}") for u in req.urls
                    ]
                else:
                    results = [
                        FetchResult(
                            url=u,
                            error=WebError(type=WebErrorType.ANTI_BOT, message="blocked"),
                        )
                        for u in req.urls
                    ]
                return FetchResponse(results=results, provider=self.name)

        guard = AntiBotUnlessStealth("guard", {})
        specs = {
            "guard": _spec(
                "guard",
                levels=[
                    LevelSpec(
                        name="standard", display_name="Standard",
                        native_params={"proxy": "auto"}, min_tier=None, credits=1,
                    ),
                    LevelSpec(
                        name="stealth", display_name="Stealth",
                        native_params={"proxy": "stealth"}, min_tier=None, credits=5,
                    ),
                ],
            ),
        }
        router = make_router([guard], specs)

        start_tool_tracking()
        resp = await router.fetch(FetchRequest(urls=[URL_A]))
        usage = stop_tool_tracking()

        assert resp.results[0].ok
        assert guard.native_params_seen == [{"proxy": "auto"}, {"proxy": "stealth"}]
        # Only the successful escalated attempt bills, at the stealth rate.
        assert usage == {"GuardFetchTool:stealth": 1}

    async def test_stealth_failure_falls_to_inhouse(self, make_router):
        """anti_bot at the stealth level itself doesn't loop — the URL falls
        through to in-house."""
        hard = FakeAdapter("hard", {URL_A: WebErrorType.ANTI_BOT})
        inhouse = FakeAdapter("inhouse", {})
        specs = {
            "hard": _spec(
                "hard",
                levels=[
                    LevelSpec(
                        name="standard", display_name="Standard", native_params={},
                        min_tier=None, credits=1,
                    ),
                    LevelSpec(
                        name="stealth", display_name="Stealth", native_params={},
                        min_tier=None, credits=5,
                    ),
                ],
            ),
            "inhouse": _spec("inhouse"),
        }
        router = make_router([hard, inhouse], specs)

        resp = await router.fetch(FetchRequest(urls=[URL_A]))

        assert resp.results[0].ok
        assert resp.provider == "inhouse"
        assert len(hard.calls) == 2  # default attempt + one stealth attempt

    async def test_usage_recorded_with_level_qualified_names(self, make_router):
        """Standard and stealth attempts on the one paid provider bill under
        distinct level-qualified keys."""

        class AntiBotAtStandard(FakeAdapter):
            async def fetch(self, req, native_params):
                self.calls.append(req)
                self.native_params_seen.append(native_params)
                stealth = native_params.get("proxy") == "stealth"
                results = []
                for u in req.urls:
                    if u == URL_A and not stealth:
                        results.append(
                            FetchResult(
                                url=u,
                                error=WebError(type=WebErrorType.ANTI_BOT, message="blocked"),
                            )
                        )
                    else:
                        results.append(FetchResult(url=u, markdown=f"content of {u}"))
                return FetchResponse(results=results, provider=self.name)

        plain = AntiBotAtStandard("plain", {})
        specs = {
            "plain": _spec(
                "plain",
                levels=[
                    LevelSpec(
                        name="standard", display_name="Standard",
                        native_params={"proxy": "auto"}, min_tier=None, credits=1,
                    ),
                    LevelSpec(
                        name="stealth", display_name="Stealth",
                        native_params={"proxy": "stealth"}, min_tier=None, credits=5,
                    ),
                ],
            ),
        }
        router = make_router([plain], specs)

        start_tool_tracking()
        await router.fetch(FetchRequest(urls=[URL_A, URL_B]))
        usage = stop_tool_tracking()

        # URL_B succeeded at the standard level, URL_A escalated to stealth.
        assert usage == {"PlainFetchTool:standard": 1, "PlainFetchTool:stealth": 1}

    async def test_free_fetches_record_no_usage(self, make_router):
        free_spec = _spec(
            "free",
            levels=[
                LevelSpec(
                    name="standard", display_name="Standard", native_params={},
                    min_tier=None, credits=0,
                )
            ],
        )
        free = FakeAdapter("free", {})
        router = make_router([free], {"free": free_spec})

        start_tool_tracking()
        await router.fetch(FetchRequest(urls=[URL_A]))
        usage = stop_tool_tracking()

        assert usage == {}

    async def test_adapter_exception_is_contained(self, make_router):
        class ExplodingAdapter(FakeAdapter):
            async def fetch(self, req, native_params):
                raise RuntimeError("boom")

        exploding = ExplodingAdapter("exploding", {})
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([exploding, inhouse])

        resp = await router.fetch(FetchRequest(urls=[URL_A]))

        assert resp.results[0].ok
        assert resp.provider == "inhouse"

    async def test_one_bad_sub_batch_does_not_sink_siblings(self, make_router):
        """max_batch_size=1 → 2 sub-batches; one raising (e.g. non-JSON 200 →
        JSONDecodeError, which adapters don't catch) must fail only its URL."""

        class FlakyBatchAdapter(FakeAdapter):
            async def fetch(self, req, native_params):
                if req.urls == [URL_B]:
                    raise ValueError("Expecting value: line 1 column 1")
                return await super().fetch(req, native_params)

        flaky = FlakyBatchAdapter("flaky", {})
        specs = {"flaky": _spec("flaky", max_batch_size=1)}
        router = make_router([flaky], specs)

        resp = await router.fetch(FetchRequest(urls=[URL_A, URL_B]))

        by_url = {r.url: r for r in resp.results}
        assert by_url[URL_A].ok  # sibling sub-batch survived
        assert by_url[URL_B].error.type == WebErrorType.PROVIDER_ERROR

    async def test_open_breaker_skips_provider(self, make_router):
        """A provider whose breaker has opened is skipped; the chain falls to
        in-house instead of eating its failures again."""
        flaky = FakeAdapter("flaky", {URL_A: WebErrorType.PROVIDER_ERROR})
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([flaky, inhouse])
        router._chain[0].breaker.failure_threshold = 1

        # First call trips flaky's breaker (all-URL provider_error), falls to inhouse.
        first = await router.fetch(FetchRequest(urls=[URL_A]))
        assert first.provider == "inhouse"
        assert router._chain[0].breaker.is_open()

        flaky.calls.clear()
        second = await router.fetch(FetchRequest(urls=[URL_A]))
        assert not flaky.calls  # breaker open → provider never called
        assert second.provider == "inhouse"
        assert "flaky" not in second.providers_tried

    async def test_attempts_record_why_the_chain_fell_through(self, make_router):
        """providers_tried says a provider was tried; attempts says what it did.
        Without the outcome a silent fallback is unattributable after the fact."""
        primary = FakeAdapter("primary", {URL_A: WebErrorType.TIMEOUT})
        router = make_router([primary, FakeAdapter("inhouse", {})])

        resp = await router.fetch(FetchRequest(urls=[URL_A]))

        assert [str(a) for a in resp.attempts] == ["primary:timeout", "inhouse:ok"]
        assert resp.providers_tried == ["primary", "inhouse"]
        assert resp.provider == "inhouse"

    async def test_attempt_outcome_marks_partial_batches(self, make_router):
        """One URL up, one down is neither ok nor an error type."""
        only = FakeAdapter("only", {URL_B: WebErrorType.NOT_FOUND})
        router = make_router([only])

        resp = await router.fetch(FetchRequest(urls=[URL_A, URL_B]))

        assert [str(a) for a in resp.attempts] == ["only:partial"]

    async def test_breaker_open_log_carries_provider_and_cause(self, make_router, caplog):
        """The transition is the only line emitted — routine failures stay
        silent, so the one warning has to name the breaker and what opened it."""
        flaky = FakeAdapter("flaky", {URL_A: WebErrorType.PROVIDER_ERROR})
        router = make_router([flaky, FakeAdapter("inhouse", {})])
        router._chain[0].breaker.failure_threshold = 1

        with caplog.at_level(logging.WARNING, logger="src.tools.web.breaker"):
            await router.fetch(FetchRequest(urls=[URL_A]))

        opened = [r.getMessage() for r in caplog.records if "opening after" in r.getMessage()]
        assert len(opened) == 1
        assert "[fetch:flaky]" in opened[0]
        assert "provider_error" in opened[0] and "scripted" in opened[0]

    async def test_healthy_failures_emit_no_breaker_log(self, make_router, caplog):
        """Target-side failures are routine; they must not log at all."""
        flaky = FakeAdapter(
            "flaky",
            {URL_A: WebError(type=WebErrorType.PROVIDER_ERROR, message="Target server error",
                             provider_fault=False)},
        )
        router = make_router([flaky, FakeAdapter("inhouse", {})])
        router._chain[0].breaker.failure_threshold = 1

        with caplog.at_level(logging.WARNING, logger="src.tools.web.breaker"):
            await router.fetch(FetchRequest(urls=[URL_A]))

        assert caplog.records == []

    async def test_target_fault_errors_do_not_trip_breaker(self, make_router):
        """All-URL failures that are the TARGET's fault (429/5xx from the
        scraped site) must not open the provider's shared breaker — a down
        target site would otherwise disable the provider for every tenant."""
        flaky = FakeAdapter(
            "flaky",
            {
                URL_A: WebError(
                    type=WebErrorType.PROVIDER_ERROR,
                    message="Target server error",
                    provider_fault=False,
                )
            },
        )
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([flaky, inhouse])
        router._chain[0].breaker.failure_threshold = 1

        resp = await router.fetch(FetchRequest(urls=[URL_A]))
        assert resp.provider == "inhouse"
        assert not router._chain[0].breaker.is_open()

    async def test_per_url_timeouts_do_not_trip_breaker(self, make_router):
        """The negative contract: TIMEOUT-class per-URL failures never count
        toward the breaker even when every URL fails."""
        flaky = FakeAdapter("flaky", {URL_A: WebErrorType.TIMEOUT})
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([flaky, inhouse])
        router._chain[0].breaker.failure_threshold = 1

        resp = await router.fetch(FetchRequest(urls=[URL_A]))
        assert resp.provider == "inhouse"
        assert not router._chain[0].breaker.is_open()


VIDEO_URL = "https://www.youtube.com/watch?v=abc"


@pytest.mark.asyncio
class TestForcedInhouseRouting:
    """Dedicated-extractor URLs (YouTube/X) are a routing input: they always
    land on in-house, and on the chain's own entry (one breaker) when the
    chain carries one."""

    async def test_forced_urls_skip_the_primary(self, make_router):
        primary = FakeAdapter("primary", {})
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([primary, inhouse])

        resp = await router.fetch(FetchRequest(urls=[VIDEO_URL, URL_A]))

        assert primary.calls and primary.calls[0].urls == [URL_A]
        assert inhouse.calls and inhouse.calls[0].urls == [VIDEO_URL]
        assert all(r.ok for r in resp.results)
        assert [r.url for r in resp.results] == [VIDEO_URL, URL_A]

    async def test_chain_inhouse_entry_is_shared(self, make_router):
        """The forced stage reuses the chain's in-house entry — same breaker
        for forced and degraded traffic."""
        primary = FakeAdapter("primary", {})
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([primary, inhouse])

        assert router._inhouse_entry is router._chain[1]

    async def test_forced_urls_get_inhouse_even_when_chain_lacks_it(self, monkeypatch):
        """Pre-routing is unconditional: a chain without inhouse still sends
        dedicated-extractor URLs to the in-house adapter."""
        primary = FakeAdapter("primary", {})
        inhouse = FakeAdapter("inhouse", {})
        specs = {"primary": _spec("primary"), "inhouse": _spec("inhouse")}
        monkeypatch.setattr(
            router_module, "get_web_provider_spec", lambda name: specs.get(name)
        )
        monkeypatch.setattr(
            router_module,
            "_ADAPTER_BUILDERS",
            {"primary": lambda: primary, "inhouse": lambda: inhouse},
        )
        router = FetchRouter(["primary"])

        resp = await router.fetch(FetchRequest(urls=[VIDEO_URL, URL_A]))

        assert primary.calls and primary.calls[0].urls == [URL_A]
        assert inhouse.calls and inhouse.calls[0].urls == [VIDEO_URL]
        assert all(r.ok for r in resp.results)

    async def test_inhouse_only_chain_serves_forced_and_ordinary_together(
        self, make_router
    ):
        inhouse = FakeAdapter("inhouse", {})
        router = make_router([inhouse])

        resp = await router.fetch(FetchRequest(urls=[VIDEO_URL, URL_A]))

        assert inhouse.calls and inhouse.calls[0].urls == [VIDEO_URL, URL_A]
        assert all(r.ok for r in resp.results)
