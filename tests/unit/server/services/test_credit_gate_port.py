"""The lease adapter's own decisions: what it sends, and what it does with an
answer it did not want.

Every one of these is a place where the honest move is to say nothing rather
than say something wrong — the gate keeps enforcing its last trusted verdict,
which is the whole fail-open promise.
"""

import asyncio

import pytest

from src.server.services import credit_gate_port as port_module
from src.server.services.credit_gate_port import PlatformCreditGatePort


class FakeResponse:
    def __init__(self, status_code: int, payload=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no body")
        return self._payload


class FakeClient:
    """Answers with a queued script; records what it was asked."""

    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.posts: list[tuple] = []
        self.gets: list[tuple] = []

    def _next(self):
        return self.responses.pop(0) if self.responses else FakeResponse(200, {})

    async def post(self, url, json=None, headers=None, **kw):
        self.posts.append((url, json))
        return self._next()

    async def get(self, url, headers=None, **kw):
        self.gets.append((url, kw))
        return self._next()


@pytest.fixture
def client(monkeypatch):
    fake = FakeClient()

    async def _get_client():
        return fake

    monkeypatch.setattr(port_module.usage_limits, "get_http_client", _get_client)
    monkeypatch.setattr(port_module.usage_limits, "service_headers", lambda *a: {})
    monkeypatch.setattr(port_module.usage_limits, "service_token_missing", lambda: False)
    return fake


@pytest.fixture
def no_sleep(monkeypatch):
    slept: list[float] = []

    async def _sleep(delay):
        slept.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _sleep)
    return slept


# -- what we are willing to report ----------------------------------------


@pytest.mark.parametrize("spent", [float("nan"), float("inf"), 2_000_000.0])
@pytest.mark.asyncio
async def test_an_unreportable_spend_never_reaches_the_service(client, spent):
    """A number we do not believe is worse than no number: the gate keeps its
    last trusted verdict for a tick, and the next heartbeat corrects it."""
    assert await PlatformCreditGatePort().acquire("u", "run-1", spent) is None
    assert client.posts == []


@pytest.mark.parametrize("credits", [float("nan"), float("inf"), 2_000_000.0])
@pytest.mark.asyncio
async def test_an_unreportable_spend_never_reaches_the_ledger(monkeypatch, credits):
    """The beat is the write that has to hold the line. The ledger's write is
    monotone and Postgres ranks NaN above every real numeric, so one bad beat is
    permanent — no later correct value lowers it, and the per-user sum carries
    it to every other run that user has open."""
    wrote: list = []

    async def _heartbeat(kind, run_ref, value):
        wrote.append(value)
        return True

    monkeypatch.setattr(port_module.credit_ledger, "heartbeat", _heartbeat)
    assert await PlatformCreditGatePort().heartbeat("run", "r-1", credits) is None
    assert wrote == []


@pytest.mark.asyncio
async def test_a_believable_spend_reaches_the_ledger_and_its_answer_comes_back(
    monkeypatch,
):
    async def _heartbeat(kind, run_ref, value):
        return False

    monkeypatch.setattr(port_module.credit_ledger, "heartbeat", _heartbeat)
    assert await PlatformCreditGatePort().heartbeat("run", "r-1", 12.5) is False


@pytest.mark.asyncio
async def test_a_small_negative_is_clamped_rather_than_refused(client):
    client.responses = [FakeResponse(200, {"granted": True, "ceiling_credits": 5.0})]
    await PlatformCreditGatePort().acquire("u", "run-1", -0.0000001)
    assert client.posts[0][1]["spent_credits"] == 0.0


# -- the fence ------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_grant_generation_is_carried_out_of_the_verdict(client):
    client.responses = [
        FakeResponse(200, {"granted": True, "ceiling_credits": 5.0, "generation": 9})
    ]
    verdict = await PlatformCreditGatePort().acquire("u", "run-1", 1.0)
    assert verdict.generation == 9


@pytest.mark.parametrize("raw", [True, "9", 9.5, None])
@pytest.mark.asyncio
async def test_a_generation_that_is_not_an_integer_leaves_the_release_unfenced(
    client, raw
):
    """Unfenced is the pre-fence behaviour, which is a worse race but a working
    release. A bogus fence would silently retire nothing at all."""
    client.responses = [
        FakeResponse(200, {"granted": True, "ceiling_credits": 5.0, "generation": raw})
    ]
    verdict = await PlatformCreditGatePort().acquire("u", "run-1", 1.0)
    assert verdict.generation is None


@pytest.mark.asyncio
async def test_release_sends_the_generation_only_when_it_has_one(client):
    await PlatformCreditGatePort().release("u", "run-1", 4)
    await PlatformCreditGatePort().release("u", "run-1")
    assert client.posts[0][1]["generation"] == 4
    assert "generation" not in client.posts[1][1]


# -- giving up on a release, and not too early ----------------------------


@pytest.mark.asyncio
async def test_release_retries_a_server_error_and_stops_on_success(
    client, no_sleep
):
    client.responses = [FakeResponse(503), FakeResponse(200, {"released": True})]
    await PlatformCreditGatePort().release("u", "run-1", 1)
    assert len(client.posts) == 2
    assert no_sleep == [0.5]


@pytest.mark.asyncio
async def test_release_does_not_retry_an_answer(client, no_sleep):
    """A 4xx is the service answering, 404 included — there was no lease, which
    is the idempotent case and not a failure."""
    client.responses = [FakeResponse(404)]
    await PlatformCreditGatePort().release("u", "run-1", 1)
    assert len(client.posts) == 1
    assert no_sleep == []


@pytest.mark.asyncio
async def test_release_gives_up_after_a_bounded_number_of_attempts(
    client, no_sleep
):
    client.responses = [FakeResponse(500) for _ in range(10)]
    await PlatformCreditGatePort().release("u", "run-1", 1)
    assert len(client.posts) == port_module._RELEASE_ATTEMPTS


@pytest.mark.asyncio
async def test_a_hanging_service_costs_one_timeout_not_three(
    client, no_sleep, monkeypatch
):
    """The attempt count bounds the tries, not the wait. This runs from the
    lane's teardown, so its total is what the user's stream waits out after
    output has already stopped."""
    now = [0.0]
    monkeypatch.setattr(port_module.time, "monotonic", lambda: now[0])

    async def hang(url, json=None, headers=None, timeout=None, **kw):
        client.posts.append((url, json))
        now[0] += timeout  # accepts the connection, then never answers
        raise TimeoutError("read timeout")

    monkeypatch.setattr(client, "post", hang)
    await PlatformCreditGatePort().release("u", "run-1", 1)

    assert len(client.posts) == 1
    assert now[0] == pytest.approx(port_module._RELEASE_BUDGET_SECONDS)


# -- the startup check ----------------------------------------------------


@pytest.mark.asyncio
async def test_capability_reports_nothing_when_the_route_is_not_there(client):
    client.responses = [FakeResponse(404, text="Not Found")]
    assert await PlatformCreditGatePort().capability() is None


@pytest.mark.asyncio
async def test_the_wiring_check_is_silent_without_platform_gating(
    client, monkeypatch
):
    """OSS builds have no service to ask, and a boot-time GET at a URL that
    means nothing to them is noise, not a check."""
    monkeypatch.setattr(port_module.usage_limits, "platform_gating_active", lambda: False)
    await port_module.verify_credit_gate_wiring()
    assert client.gets == []


@pytest.mark.parametrize(
    "ttl", [port_module.LEASE_RENEW_MARGIN_SECONDS, 30, port_module.LEASE_RENEW_MARGIN_SECONDS + 1]
)
@pytest.mark.asyncio
async def test_the_wiring_check_never_raises_on_any_ttl(client, monkeypatch, ttl):
    """It runs inside the lifespan. Whatever it finds, the server still boots —
    the gate is an extra guard, not an availability dependency."""
    monkeypatch.setattr(port_module.usage_limits, "platform_gating_active", lambda: True)
    client.responses = [FakeResponse(200, {"enabled": True, "lease_ttl_seconds": ttl})]
    await port_module.verify_credit_gate_wiring()
    assert len(client.gets) == 1


# -- what the reservation is sized in -------------------------------------


@pytest.mark.asyncio
async def test_the_rate_reaches_the_service(client):
    client.responses = [FakeResponse(200, {"granted": True, "ceiling_credits": 5.0})]
    await PlatformCreditGatePort().acquire("u", "run-1", 1.0, 8.5)
    assert client.posts[0][1]["rate_multiplier"] == 8.5


@pytest.mark.asyncio
async def test_the_billing_source_reaches_the_service(client):
    """Admission forwards it, so the lease must too: the service folds the same
    billing row for both, and without this it answers the run's own admission
    verdict with the opposite one."""
    client.responses = [
        FakeResponse(200, {"granted": True, "ceiling_credits": 5.0}),
        FakeResponse(200, {"granted": True, "ceiling_credits": 5.0}),
    ]
    await PlatformCreditGatePort().acquire("u", "run-1", 1.0, 1.0, True)
    assert client.posts[0][1]["byok"] is True

    # Absent, not false, for a platform-funded turn: the payload a service that
    # has never heard of the field still reads exactly as it did before.
    await PlatformCreditGatePort().acquire("u", "run-1", 1.0)
    assert "byok" not in client.posts[1][1]


@pytest.mark.parametrize(
    "sent, expected",
    [
        (0.0, port_module._MIN_RATE_MULTIPLIER),
        (1e9, port_module._MAX_RATE_MULTIPLIER),
        (float("nan"), 1.0),
        (float("inf"), 1.0),
        (None, 1.0),
        ("eight", 1.0),
    ],
)
@pytest.mark.asyncio
async def test_a_rate_we_cannot_use_is_clamped_not_refused(client, sent, expected):
    """Unlike an unreportable spend, a bad rate never withholds the ask. The
    spend is the figure the verdict is computed from, so a wrong one is worse
    than none; the rate only sizes the headroom, and the honest fallback is to
    ask at the baseline rather than leave the turn without a ceiling."""
    client.responses = [FakeResponse(200, {"granted": True, "ceiling_credits": 5.0})]
    await PlatformCreditGatePort().acquire("u", "run-1", 1.0, sent)
    assert client.posts[0][1]["rate_multiplier"] == expected


@pytest.mark.asyncio
async def test_an_unpriced_model_reserves_at_the_baseline(monkeypatch):
    """A model the manifest cannot price leaves the reservation where it was,
    which is the same thing every OSS build gets."""
    monkeypatch.setattr(port_module.usage_limits, "platform_gating_active", lambda: True)
    gate = port_module.build_run_credit_gate(
        "u", "run-1", None, None, "not-a-real-model-anywhere"
    )
    assert gate.rate_multiplier == 1.0


# -- the numbers the service publishes so this build can check them --------


def test_ttl_prefers_the_service_differenced_remainder():
    """Skew lives entirely in the subtraction. ``expires_in_seconds`` is
    differenced on one clock, so a stamp this host reads as long past still
    yields the real remaining TTL."""
    body = {"expires_at": "2000-01-01T00:00:00+00:00", "expires_in_seconds": 300}
    assert port_module._ttl_seconds(body) == 300.0


def test_ttl_falls_back_to_the_stamp_then_to_the_stand_in():
    assert port_module._ttl_seconds({"expires_at": "2000-01-01T00:00:00+00:00"}) == 0.0
    assert port_module._ttl_seconds({}) == port_module._UNKNOWN_TTL_SECONDS


@pytest.mark.asyncio
async def test_the_wiring_check_reports_a_bounds_disagreement(
    client, monkeypatch, caplog
):
    """A multiplier the service rejects comes back 422, which reads as no
    verdict and leaves the turn ungated. Boot is the only place that can see
    that coming."""
    monkeypatch.setattr(
        port_module.usage_limits, "platform_gating_active", lambda: True
    )
    client.responses = [
        FakeResponse(
            200,
            {
                "enabled": True,
                "lease_ttl_seconds": port_module.LEASE_RENEW_MARGIN_SECONDS + 60,
                "rate_multiplier_min": port_module._MIN_RATE_MULTIPLIER * 5,
                "rate_multiplier_max": port_module._MAX_RATE_MULTIPLIER,
            },
        )
    ]
    with caplog.at_level("ERROR"):
        await port_module.verify_credit_gate_wiring()
    assert "bounds disagree" in caplog.text


@pytest.mark.asyncio
async def test_the_wiring_check_is_quiet_when_the_bounds_agree(
    client, monkeypatch, caplog
):
    monkeypatch.setattr(
        port_module.usage_limits, "platform_gating_active", lambda: True
    )
    client.responses = [
        FakeResponse(
            200,
            {
                "enabled": True,
                "lease_ttl_seconds": port_module.LEASE_RENEW_MARGIN_SECONDS + 60,
                "rate_multiplier_min": port_module._MIN_RATE_MULTIPLIER,
                "rate_multiplier_max": port_module._MAX_RATE_MULTIPLIER,
            },
        )
    ]
    with caplog.at_level("ERROR"):
        await port_module.verify_credit_gate_wiring()
    assert caplog.text == ""
