"""Tests for ModelResilienceMiddleware — retry, fallback, events, error trace."""

import pytest

from src.ptc_agent.agent.middleware.model_resilience import (
    RESILIENCE_TRACE_ATTR,
    ModelResilienceMiddleware,
    build_fallback_pairs,
)


class _FakeModel:
    def __init__(self, name):
        self.model = name


class _FakeRequest:
    """Duck-typed stand-in for langchain's ModelRequest."""

    def __init__(self, model):
        self.model = model

    def override(self, *, model):
        return _FakeRequest(model)


def _status_error(message, status=None):
    exc = Exception(message)
    if status is not None:
        exc.status_code = status
    return exc


def _make_middleware(primary_client, fallbacks=(), **kwargs):
    kwargs.setdefault("initial_delay", 0.0)  # no real sleeps in tests
    kwargs.setdefault("jitter", False)
    return ModelResilienceMiddleware(
        primary_name="primary-model",
        primary_client=primary_client,
        fallbacks=list(fallbacks),
        **kwargs,
    )


@pytest.fixture
def events(monkeypatch):
    """Capture custom stream events emitted via get_stream_writer()."""
    captured = []
    monkeypatch.setattr(
        "langgraph.config.get_stream_writer", lambda: captured.append
    )
    return captured


class TestSuccessPath:
    @pytest.mark.asyncio
    async def test_success_first_try_no_events(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        calls = []

        async def handler(req):
            calls.append(req)
            return "ok"

        result = await mw.awrap_model_call(_FakeRequest(client), handler)
        assert result == "ok"
        assert len(calls) == 1
        assert calls[0].model is client
        assert events == []


class TestRetryBehavior:
    @pytest.mark.asyncio
    async def test_transient_error_retries_then_succeeds(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        attempts = []

        async def handler(req):
            attempts.append(req)
            if len(attempts) < 3:
                raise _status_error("upstream 500", status=500)
            return "ok"

        result = await mw.awrap_model_call(_FakeRequest(client), handler)
        assert result == "ok"
        assert len(attempts) == 3
        assert [e["type"] for e in events] == ["model_retry", "model_retry"]
        assert events[0]["model"] == "primary-model"
        assert events[0]["attempt"] == 1
        assert events[0]["max_retries"] == 3
        assert events[0]["status_code"] == 500
        assert events[1]["attempt"] == 2

    @pytest.mark.asyncio
    async def test_transient_error_max_attempts_per_model(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client, max_retries=2)
        calls = []

        async def handler(req):
            calls.append(req)
            raise _status_error("upstream 503", status=503)

        with pytest.raises(Exception):
            await mw.awrap_model_call(_FakeRequest(client), handler)
        # 1 initial + 2 retries
        assert len(calls) == 3

    @pytest.mark.asyncio
    async def test_non_retryable_error_skips_retries(self, events):
        client = _FakeModel("primary-model")
        fallback_client = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fallback_client)])
        calls = []

        async def handler(req):
            calls.append(req.model)
            if req.model is client:
                raise _status_error("Error code: 404 - model not found", status=404)
            return "ok-from-fallback"

        result = await mw.awrap_model_call(_FakeRequest(client), handler)
        assert result == "ok-from-fallback"
        # Exactly ONE attempt on the primary (no retries on a 404), then fallback
        assert calls == [client, fallback_client]
        assert [e["type"] for e in events] == ["model_fallback"]
        assert events[0]["from_model"] == "primary-model"
        assert events[0]["to_model"] == "fallback-a"
        assert events[0]["from_is_primary"] is True
        assert events[0]["attempts_on_from"] == 1
        assert events[0]["status_code"] == 404


class TestFallbackChain:
    @pytest.mark.asyncio
    async def test_second_fallback_emits_non_primary_switch(self, events):
        client = _FakeModel("primary-model")
        fb_a = _FakeModel("fallback-a")
        fb_b = _FakeModel("fallback-b")
        mw = _make_middleware(
            client, fallbacks=[("fallback-a", fb_a), ("fallback-b", fb_b)]
        )

        async def handler(req):
            if req.model is fb_b:
                return "ok"
            raise _status_error("bad request", status=400)

        result = await mw.awrap_model_call(_FakeRequest(client), handler)
        assert result == "ok"
        switches = [e for e in events if e["type"] == "model_fallback"]
        assert [(s["from_model"], s["to_model"], s["from_is_primary"]) for s in switches] == [
            ("primary-model", "fallback-a", True),
            ("fallback-a", "fallback-b", False),
        ]

    @pytest.mark.asyncio
    async def test_total_exhaustion_raises_primary_exception_with_trace(self, events):
        client = _FakeModel("primary-model")
        fb_a = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fb_a)])
        primary_exc = _status_error("Error calling model 'primary-model': 404", status=404)
        fallback_exc = _status_error("fallback param mismatch", status=400)

        async def handler(req):
            raise primary_exc if req.model is client else fallback_exc

        with pytest.raises(Exception) as exc_info:
            await mw.awrap_model_call(_FakeRequest(client), handler)

        # The PRIMARY model's exception surfaces, not the last fallback's
        assert exc_info.value is primary_exc
        trace = getattr(exc_info.value, RESILIENCE_TRACE_ATTR)
        assert trace["model"] == "primary-model"
        assert [a["model"] for a in trace["attempted_models"]] == [
            "primary-model",
            "fallback-a",
        ]
        assert trace["attempted_models"][0]["status_code"] == 404
        assert trace["attempted_models"][0]["attempts"] == 1
        assert trace["attempted_models"][1]["error"] == "fallback param mismatch"

    @pytest.mark.asyncio
    async def test_no_fallbacks_configured_raises_primary(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        exc = _status_error("unauthorized", status=401)

        async def handler(req):
            raise exc

        with pytest.raises(Exception) as exc_info:
            await mw.awrap_model_call(_FakeRequest(client), handler)
        assert exc_info.value is exc
        trace = getattr(exc_info.value, RESILIENCE_TRACE_ATTR)
        assert len(trace["attempted_models"]) == 1
        # No fallback switch events without fallbacks
        assert [e["type"] for e in events] == []


class TestRobustness:
    @pytest.mark.asyncio
    async def test_stream_writer_failure_does_not_break_resilience(self, monkeypatch):
        def _broken_writer():
            raise RuntimeError("no streaming context")

        monkeypatch.setattr("langgraph.config.get_stream_writer", _broken_writer)
        client = _FakeModel("primary-model")
        fb = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fb)])

        async def handler(req):
            if req.model is client:
                raise _status_error("nope", status=400)
            return "ok"

        assert await mw.awrap_model_call(_FakeRequest(client), handler) == "ok"

    @pytest.mark.asyncio
    async def test_subagent_request_uses_derived_model_name(self, events):
        # Shared instance: a subagent stack routes a different model through
        # the same middleware — its display name must come from the request.
        primary_client = _FakeModel("primary-model")
        sub_client = _FakeModel("subagent-model")
        mw = _make_middleware(primary_client)
        calls = []

        async def handler(req):
            calls.append(req)
            if len(calls) == 1:
                raise _status_error("upstream 500", status=500)
            return "ok"

        await mw.awrap_model_call(_FakeRequest(sub_client), handler)
        assert events[0]["model"] == "subagent-model"

    def test_sync_wrap_model_call_parity(self, events):
        client = _FakeModel("primary-model")
        fb = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fb)])

        def handler(req):
            if req.model is client:
                raise _status_error("bad request", status=400)
            return "ok"

        assert mw.wrap_model_call(_FakeRequest(client), handler) == "ok"
        assert [e["type"] for e in events] == ["model_fallback"]

    def test_sync_retry_loop_parity(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        attempts = []

        def handler(req):
            attempts.append(req)
            if len(attempts) < 3:
                raise _status_error("upstream 500", status=500)
            return "ok"

        assert mw.wrap_model_call(_FakeRequest(client), handler) == "ok"
        assert len(attempts) == 3
        assert [e["type"] for e in events] == ["model_retry", "model_retry"]
        assert events[0]["attempt"] == 1
        assert events[1]["attempt"] == 2

    def test_calculate_delay_backoff(self):
        mw = ModelResilienceMiddleware(
            primary_name="p",
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=3.0,
            jitter=False,
        )
        assert mw._calculate_delay(0) == 1.0
        assert mw._calculate_delay(1) == 2.0
        assert mw._calculate_delay(2) == 3.0  # capped at max_delay


class TestBuildFallbackPairs:
    class _Cfg:
        def __init__(self, clients=None, names=None, fallback=None):
            self.fallback_llm_clients = clients
            self.fallback_llm_names = names
            self.llm = type("_LLM", (), {"fallback": fallback})()

    def test_prefers_aligned_names(self):
        clients = [_FakeModel("sdk-id-a"), _FakeModel("sdk-id-b")]
        cfg = self._Cfg(clients=clients, names=["alias-a", "alias-b"])
        assert build_fallback_pairs(cfg) == [
            ("alias-a", clients[0]),
            ("alias-b", clients[1]),
        ]

    def test_misaligned_names_fall_back_to_derived(self):
        clients = [_FakeModel("sdk-id-a"), _FakeModel("sdk-id-b")]
        cfg = self._Cfg(clients=clients, names=["only-one"])
        assert build_fallback_pairs(cfg) == [
            ("sdk-id-a", clients[0]),
            ("sdk-id-b", clients[1]),
        ]

    def test_empty_config_returns_empty(self):
        assert build_fallback_pairs(self._Cfg()) == []
