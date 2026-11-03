"""One APNs send over HTTP/2 with token auth, and what came of it.

The client is per process and opened lazily: execution context, never truth.
Each worker mints its own provider token and keeps it 50 minutes, under
Apple's 60-minute limit and over its 20-minute refresh throttle.

Sends go one at a time. APNs opens a connection allowing a single stream, and
httpx's HTTP/2 pool hangs when more requests than that are in flight, then
fails every later one on the same connection. So a send holds the client for
its request, gives up at a hard deadline, and a transport error replaces the
connection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable, Mapping, Optional

import httpx
import jwt

from src.server.services.push.config import ApnsConfig, apns_config

logger = logging.getLogger(__name__)

HOSTS = {
    "production": "https://api.push.apple.com",
    "sandbox": "https://api.sandbox.push.apple.com",
}
TOKEN_TTL_SECONDS = 50 * 60
#: One request's limit, connection included. A send answers in well under a
#: second; a short wait keeps a job's devices inside the outbox lease.
REQUEST_SECONDS = 5.0
EXPIRATION_SECONDS = 24 * 60 * 60


class ApnsOutcome(StrEnum):
    SENT = "sent"
    #: The token no longer reaches the app (410, or not this app's): drop it.
    UNREGISTERED = "unregistered"
    #: Not a token of this host; it may be one of the other environment's.
    BAD_TOKEN = "bad_token"
    #: Apple or the network failed; sending again later may succeed.
    RETRY = "retry"
    #: Apple refused this request for good.
    REJECTED = "rejected"


@dataclass(frozen=True)
class ApnsResult:
    outcome: ApnsOutcome
    status: Optional[int] = None
    reason: Optional[str] = None


def _classify(status: int, reason: Optional[str]) -> ApnsOutcome:
    if status == 200:
        return ApnsOutcome.SENT
    if status == 410 or (
        status == 400 and reason in ("DeviceTokenNotForTopic", "Unregistered")
    ):
        return ApnsOutcome.UNREGISTERED
    if status == 400 and reason == "BadDeviceToken":
        return ApnsOutcome.BAD_TOKEN
    if status == 429 or status >= 500 or reason == "IdleTimeout":
        # IdleTimeout: APNs closed a pooled connection that sat idle; the push
        # never went out.
        return ApnsOutcome.RETRY
    if status == 403 and reason == "ExpiredProviderToken":
        # Refused after a fresh mint too: this host's clock is off.
        return ApnsOutcome.RETRY
    return ApnsOutcome.REJECTED


def _reason(response: httpx.Response) -> Optional[str]:
    try:
        body = response.json()
    except ValueError:
        return None
    return body.get("reason") if isinstance(body, dict) else None


class ApnsClient:
    def __init__(
        self,
        config: ApnsConfig,
        *,
        transport: Optional[httpx.AsyncBaseTransport] = None,
        clock: Callable[[], float] = time.time,
    ):
        self._config = config
        self._clock = clock
        self._transport = transport
        self._http = self._open()
        self._sending = asyncio.Lock()
        self._token: Optional[str] = None
        self._minted_at = 0.0

    def _open(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            http2=True,
            transport=self._transport,
            timeout=httpx.Timeout(REQUEST_SECONDS),
        )

    async def _reopen(self) -> None:
        """Drop a connection a failed request may have left unusable."""
        stale, self._http = self._http, self._open()
        try:
            async with asyncio.timeout(REQUEST_SECONDS):
                await stale.aclose()
        except Exception:
            pass

    async def _take_turn(self, deadline: Optional[float]) -> bool:
        """Take the connection for one send, or give up once ``deadline``
        passes, whether waiting behind other sends or on taking it late."""
        if deadline is None:
            await self._sending.acquire()
            return True
        try:
            async with asyncio.timeout(max(deadline - self._clock(), 0.0)):
                await self._sending.acquire()
        except TimeoutError:
            return False
        if self._clock() > deadline:
            self._sending.release()
            return False
        return True

    def _provider_token(self, *, fresh: bool = False) -> str:
        now = self._clock()
        if fresh or self._token is None or now - self._minted_at >= TOKEN_TTL_SECONDS:
            self._token = jwt.encode(
                {"iss": self._config.team_id, "iat": int(now)},
                self._config.private_key,
                algorithm="ES256",
                headers={"kid": self._config.key_id},
            )
            self._minted_at = now
        return self._token

    async def send(
        self,
        *,
        token: str,
        bundle_id: str,
        environment: str,
        payload: Mapping[str, Any],
        collapse_id: Optional[str] = None,
        deadline: Optional[float] = None,
    ) -> ApnsResult:
        """Send one alert. Never raises for Apple's answer or the network's;
        the outcome says what to do with the device and the job. A send not
        started by ``deadline`` (this client's clock) is not made and reads as
        a retry: sends queue here, so a slow Apple would otherwise carry a
        job's last ones past its outbox lease."""
        url = f"{HOSTS[environment]}/3/device/{token}"
        body = json.dumps(payload, separators=(",", ":")).encode()

        async def post(provider_token: str) -> tuple[httpx.Response, Optional[str]]:
            headers = {
                "authorization": f"bearer {provider_token}",
                "apns-topic": bundle_id,
                "apns-push-type": "alert",
                "apns-priority": "10",
                "apns-expiration": str(int(self._clock()) + EXPIRATION_SECONDS),
            }
            if collapse_id:
                headers["apns-collapse-id"] = collapse_id
            response = await self._http.post(url, content=body, headers=headers)
            reason = _reason(response) if response.status_code != 200 else None
            return response, reason

        if not await self._take_turn(deadline):
            return ApnsResult(ApnsOutcome.RETRY)
        try:
            async with asyncio.timeout(REQUEST_SECONDS):
                response, reason = await post(self._provider_token())
            if response.status_code == 403 and reason == "ExpiredProviderToken":
                # Re-mint and try once more.
                async with asyncio.timeout(REQUEST_SECONDS):
                    response, reason = await post(self._provider_token(fresh=True))
        except (httpx.HTTPError, TimeoutError) as e:
            logger.warning("[PUSH] APNs %s unreachable: %s", environment, type(e).__name__)
            await self._reopen()
            return ApnsResult(ApnsOutcome.RETRY)
        finally:
            self._sending.release()
        outcome = _classify(response.status_code, reason)
        if outcome is ApnsOutcome.REJECTED:
            logger.warning(
                "[PUSH] APNs refused a push to %s (%s): %s %s",
                bundle_id,
                environment,
                response.status_code,
                reason,
            )
        return ApnsResult(outcome, response.status_code, reason)

    async def aclose(self) -> None:
        await self._http.aclose()


_client: Optional[ApnsClient] = None
_client_lock = asyncio.Lock()


async def get_apns_client() -> Optional[ApnsClient]:
    """This process's client, or None when push is off."""
    global _client
    async with _client_lock:
        if _client is None:
            config = apns_config()
            if config is None:
                return None
            _client = ApnsClient(config)
        return _client


async def close_apns_client() -> None:
    """Close this process's client. Call during application shutdown."""
    global _client
    async with _client_lock:
        if _client is not None:
            await _client.aclose()
            _client = None
