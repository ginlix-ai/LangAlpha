"""One APNs send over HTTP/2 with token auth, and what came of it.

The client is per process and opened lazily: execution context, never truth.
Each worker mints its own provider token and keeps it 50 minutes, under
Apple's 60-minute limit and over its 20-minute refresh throttle.
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
    if status == 429 or status >= 500:
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
        self._http = httpx.AsyncClient(
            http2=True,
            transport=transport,
            timeout=httpx.Timeout(10.0, connect=5.0),
        )
        self._token: Optional[str] = None
        self._minted_at = 0.0

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
    ) -> ApnsResult:
        """Send one alert. Never raises for Apple's answer or the network's;
        the outcome says what to do with the device and the job."""
        url = f"{HOSTS[environment]}/3/device/{token}"
        body = json.dumps(payload, separators=(",", ":")).encode()
        provider_token = self._provider_token()
        for attempt in range(2):
            headers = {
                "authorization": f"bearer {provider_token}",
                "apns-topic": bundle_id,
                "apns-push-type": "alert",
                "apns-priority": "10",
                "apns-expiration": str(int(self._clock()) + EXPIRATION_SECONDS),
            }
            if collapse_id:
                headers["apns-collapse-id"] = collapse_id
            try:
                response = await self._http.post(url, content=body, headers=headers)
            except httpx.HTTPError as e:
                logger.warning(
                    "[PUSH] APNs %s unreachable: %s", environment, type(e).__name__
                )
                return ApnsResult(ApnsOutcome.RETRY)
            reason = _reason(response) if response.status_code != 200 else None
            if (
                response.status_code == 403
                and reason == "ExpiredProviderToken"
                and attempt == 0
            ):
                # Re-mint unless a concurrent send already has.
                provider_token = self._provider_token(
                    fresh=provider_token == self._token
                )
                continue
            outcome = _classify(response.status_code, reason)
            if response.status_code == 403 and reason == "ExpiredProviderToken":
                # A fresh token refused as expired: this host's clock is off.
                outcome = ApnsOutcome.RETRY
            if outcome is ApnsOutcome.REJECTED:
                logger.warning(
                    "[PUSH] APNs refused a push to %s (%s): %s %s",
                    bundle_id,
                    environment,
                    response.status_code,
                    reason,
                )
            return ApnsResult(outcome, response.status_code, reason)
        raise AssertionError("unreachable")

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
