"""The APNs client: the request it makes, its provider token, and how it reads
Apple's answers."""

import asyncio
import json
import time

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from src.server.services.push import apns
from src.server.services.push.apns import (
    TOKEN_TTL_SECONDS,
    ApnsClient,
    ApnsOutcome,
)
from src.server.services.push.config import ApnsConfig

TOKEN = "ab" * 32


@pytest.fixture(scope="module")
def key():
    private = ec.generate_private_key(ec.SECP256R1())
    pem = private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return pem, private.public_key()


class Clock:
    def __init__(self, now=1_700_000_000.0):
        self.now = now

    def __call__(self):
        return self.now


def _client(key, handler, clock=None):
    config = ApnsConfig(
        key_id="KEY123", team_id="TEAM456", private_key=key[0], bundle_ids=("app.x",)
    )
    return ApnsClient(
        config, transport=httpx.MockTransport(handler), clock=clock or Clock()
    )


def _answer(status, reason=None):
    def handler(request):
        if reason is None:
            return httpx.Response(status)
        return httpx.Response(status, json={"reason": reason})

    return handler


async def _send(client, environment="production", deadline=None):
    return await client.send(
        token=TOKEN,
        bundle_id="app.x",
        environment=environment,
        payload={"aps": {"alert": {"title": "t", "body": "b"}}},
        collapse_id="thread-1",
        deadline=deadline,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "environment, host",
    [
        ("production", "api.push.apple.com"),
        ("sandbox", "api.sandbox.push.apple.com"),
    ],
)
async def test_the_request_names_the_device_topic_and_thread(key, environment, host):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200)

    clock = Clock()
    client = _client(key, handler, clock)
    result = await _send(client, environment)
    assert result.outcome is ApnsOutcome.SENT
    (request,) = seen
    assert request.method == "POST"
    assert str(request.url) == f"https://{host}/3/device/{TOKEN}"
    assert request.headers["apns-topic"] == "app.x"
    assert request.headers["apns-push-type"] == "alert"
    assert request.headers["apns-priority"] == "10"
    assert request.headers["apns-expiration"] == str(int(clock.now) + 86400)
    assert request.headers["apns-collapse-id"] == "thread-1"
    assert json.loads(request.content) == {"aps": {"alert": {"title": "t", "body": "b"}}}

    scheme, token = request.headers["authorization"].split(" ")
    assert scheme == "bearer"
    assert jwt.get_unverified_header(token) == {"alg": "ES256", "kid": "KEY123", "typ": "JWT"}
    claims = jwt.decode(token, key[1], algorithms=["ES256"])
    assert claims == {"iss": "TEAM456", "iat": int(clock.now)}
    await client.aclose()


@pytest.mark.asyncio
async def test_the_provider_token_is_kept_until_it_ages_out(key):
    tokens = []

    def handler(request):
        tokens.append(request.headers["authorization"])
        return httpx.Response(200)

    clock = Clock()
    client = _client(key, handler, clock)
    await _send(client)
    clock.now += TOKEN_TTL_SECONDS - 1
    await _send(client)
    clock.now += 2
    await _send(client)
    assert tokens[0] == tokens[1] != tokens[2]


@pytest.mark.asyncio
async def test_an_expired_provider_token_is_minted_again_and_sent_once_more(key):
    tokens = []
    clock = Clock()

    def handler(request):
        tokens.append(request.headers["authorization"])
        if len(tokens) == 1:
            clock.now += 5  # well inside the cache's 50 minutes
            return httpx.Response(403, json={"reason": "ExpiredProviderToken"})
        return httpx.Response(200)

    client = _client(key, handler, clock)
    result = await _send(client)
    assert result.outcome is ApnsOutcome.SENT
    assert len(tokens) == 2 and tokens[0] != tokens[1]
    claims = jwt.decode(tokens[1].split(" ")[1], key[1], algorithms=["ES256"])
    assert claims["iat"] == int(clock.now)
    # The fresh token is the one kept.
    await _send(client)
    assert tokens[2] == tokens[1]

    tokens.clear()

    def always_expired(request):
        tokens.append(request.headers["authorization"])
        return httpx.Response(403, json={"reason": "ExpiredProviderToken"})

    client = _client(key, always_expired, clock)
    result = await _send(client)
    assert len(tokens) == 2
    assert result.outcome is ApnsOutcome.RETRY


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, reason, outcome",
    [
        (200, None, ApnsOutcome.SENT),
        (410, "Unregistered", ApnsOutcome.UNREGISTERED),
        (400, "Unregistered", ApnsOutcome.UNREGISTERED),
        (400, "DeviceTokenNotForTopic", ApnsOutcome.UNREGISTERED),
        (400, "BadDeviceToken", ApnsOutcome.BAD_TOKEN),
        (429, "TooManyRequests", ApnsOutcome.RETRY),
        (500, "InternalServerError", ApnsOutcome.RETRY),
        (503, "ServiceUnavailable", ApnsOutcome.RETRY),
        (400, "IdleTimeout", ApnsOutcome.RETRY),
        (400, "PayloadTooLarge", ApnsOutcome.REJECTED),
        (403, "InvalidProviderToken", ApnsOutcome.REJECTED),
    ],
)
async def test_apples_answer_is_classified(key, status, reason, outcome):
    result = await _send(_client(key, _answer(status, reason)))
    assert result.outcome is outcome
    assert result.status == status
    assert result.reason == (reason if status != 200 else None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [httpx.ConnectError("refused"), httpx.ReadTimeout("slow")]
)
async def test_a_network_failure_is_retried_later(key, error):
    def handler(request):
        raise error

    result = await _send(_client(key, handler))
    assert result.outcome is ApnsOutcome.RETRY
    assert result.status is None


@pytest.mark.asyncio
async def test_sends_go_one_at_a_time(key):
    # APNs allows one stream on a new connection, and httpx's HTTP/2 pool hangs
    # past it, so concurrent sends must not overlap on the wire.
    in_flight = peak = 0

    async def handler(request):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return httpx.Response(200)

    client = _client(key, handler)
    results = await asyncio.gather(*(_send(client) for _ in range(4)))
    assert [r.outcome for r in results] == [ApnsOutcome.SENT] * 4
    assert peak == 1


@pytest.mark.asyncio
async def test_a_hung_request_gives_up_and_the_connection_is_replaced(key, monkeypatch):
    monkeypatch.setattr(apns, "REQUEST_SECONDS", 0.05)
    hang = True

    async def handler(request):
        if hang:
            await asyncio.sleep(10)
        return httpx.Response(200)

    client = _client(key, handler)
    first = client._http
    assert (await _send(client)).outcome is ApnsOutcome.RETRY
    assert client._http is not first
    hang = False
    assert (await _send(client)).outcome is ApnsOutcome.SENT


@pytest.mark.asyncio
async def test_a_send_queued_past_its_deadline_is_not_made(key):
    # Sends queue on one connection, so a slow Apple can hold a job's last ones
    # past its outbox lease; those are left to the job's retry instead.
    clock = Clock()
    requests = []

    async def handler(request):
        requests.append(request)
        clock.now += 20
        return httpx.Response(200)

    client = _client(key, handler, clock)
    deadline = clock.now + 30
    results = await asyncio.gather(*(_send(client, deadline=deadline) for _ in range(3)))
    assert [r.outcome for r in results] == [
        ApnsOutcome.SENT,
        ApnsOutcome.SENT,
        ApnsOutcome.RETRY,
    ]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_a_send_waiting_behind_another_gives_up_at_its_deadline(key):
    async def handler(request):
        await asyncio.sleep(0.5)
        return httpx.Response(200)

    client = _client(key, handler, time.time)
    slow = asyncio.create_task(_send(client))
    await asyncio.sleep(0)
    late = asyncio.create_task(_send(client, deadline=time.time() + 0.05))
    done, _ = await asyncio.wait({slow, late}, return_when=asyncio.FIRST_COMPLETED)
    assert done == {late}
    assert late.result().outcome is ApnsOutcome.RETRY
    assert (await slow).outcome is ApnsOutcome.SENT
