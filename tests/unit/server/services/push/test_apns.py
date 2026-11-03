"""The APNs client: the request it makes, its provider token, and how it reads
Apple's answers."""

import json

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

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


async def _send(client, environment="production"):
    return await client.send(
        token=TOKEN,
        bundle_id="app.x",
        environment=environment,
        payload={"aps": {"alert": {"title": "t", "body": "b"}}},
        collapse_id="thread-1",
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
