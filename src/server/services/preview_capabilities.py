"""Capabilities that let a browser reach a preview whose sandbox wants a header.

A private E2B sandbox answers a port only when the request carries its traffic
token, which an iframe cannot add. The server mints a short-lived capability
instead, names it in the hostname (``<port>-<token>.<preview base>``) and keeps
what it opens in Redis, so the preview proxy on any worker can resolve it.
Nothing rides in a cookie or the path: third-party cookie rules cannot touch
it, and a page inside the preview links relative to it unchanged.

The record holds the sandbox's traffic token, re-derived at every mint and kept
no longer than the capability, so it lives in Redis rather than in Postgres
backups. The proxy never hands it to a browser, but E2B passes it into the
sandbox with every request, so the sandbox's own code, or an app that echoes
request headers, can read it; it opens every port of that sandbox until the
sandbox is recreated, the only way to rotate it. What a capability bounds is
the browser's reach: one port, until it lapses or is revoked. Revocation is a
per-sandbox generation counter, so revoking needs no index of what was minted,
and a lost counter fails closed.

A provider URL marked process-local is a forward only the minting process
holds, so its record names no upstream: the worker serving a request opens
its own (``preview_upstreams``). A stored origin is dialed as it is by
whichever worker serves it, so one on loopback is refused at mint.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import secrets
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from ptc_agent.core.sandbox.preview_probe import is_loopback_url
from ptc_agent.core.sandbox.runtime import PreviewInfo
from src.server.services.preview_base import PreviewBase, preview_base
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

__all__ = [
    "RECHECK_S",
    "Capability",
    "PreviewProxyNotConfigured",
    "PreviewProxyUnavailable",
    "Upstream",
    "is_current",
    "mint",
    "preview_origin",
    "publish",
    "resolve",
    "resolve_host",
    "revoke",
    "until_revoked",
]

# A record that names its upstream stays a 2, the shape a reader that knows
# only 2 still serves: during a cutover both colors share Redis, and after a
# rollback the older one reads what this one minted. Only a null upstream,
# which that reader would dial as "None", is a 3 it refuses.
_RECORD_VERSION = 2
_NULL_UPSTREAM_VERSION = 3
_READABLE_VERSIONS = (_RECORD_VERSION, _NULL_UPSTREAM_VERSION)
# Outlives every record minted against it: refreshed on each mint and revoke
# to past the longest lifetime the preview route lets a caller ask for.
_GENERATION_TTL_S = 86_400 + 3600
# 20 random bytes are 32 base32 characters with no padding. Lowercase because
# a hostname is case-insensitive and the browser may fold it.
_TOKEN_BYTES = 20
# How often an open stream or socket re-reads its capability's revocation.
RECHECK_S = 60


class PreviewProxyNotConfigured(Exception):
    """A preview needs the proxy, and this deployment runs none."""


class PreviewProxyUnavailable(Exception):
    """Redis could not mint or answer for a capability."""


@dataclass(frozen=True)
class Upstream:
    """Where a worker dials a capability's port, and the headers it needs."""

    origin: str
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class Capability:
    """What one capability opens: one port of one sandbox, until it lapses.

    ``upstream`` is None for a port only the process that opened its forward
    can dial; the serving worker resolves it for itself from the sandbox, port
    and workspace.
    """

    sandbox_id: str
    port: int
    upstream: Upstream | None
    workspace_id: str
    generation: int
    expires_at: int

    def to_record(self) -> dict[str, Any]:
        upstream = self.upstream
        return {
            "v": _NULL_UPSTREAM_VERSION if upstream is None else _RECORD_VERSION,
            "sandbox_id": self.sandbox_id,
            "port": self.port,
            "upstream": None if upstream is None else upstream.origin,
            "workspace_id": self.workspace_id,
            "generation": self.generation,
            "expires_at": self.expires_at,
            "headers": {} if upstream is None else dict(upstream.headers),
        }

    @classmethod
    def from_record(cls, record: Any) -> Capability | None:
        """The capability a stored record holds; None for any other shape."""
        if not isinstance(record, dict) or record.get("v") not in _READABLE_VERSIONS:
            return None
        try:
            origin = record["upstream"]
            headers = {str(k): str(v) for k, v in (record["headers"] or {}).items()}
            return cls(
                sandbox_id=str(record["sandbox_id"]),
                port=int(record["port"]),
                upstream=None if origin is None else Upstream(str(origin), headers),
                workspace_id=str(record["workspace_id"]),
                generation=int(record["generation"]),
                expires_at=int(record["expires_at"]),
            )
        except (AttributeError, KeyError, TypeError, ValueError):
            return None


def _record_key(token: str) -> str:
    # A digest, so listing the keys names no live host. Reading the values
    # does: the record holds the traffic token and the signed-URL cache the
    # whole capability URL, so read access to Redis is access to previews.
    return f"preview:cap:{hashlib.sha256(token.encode()).hexdigest()[:40]}"


def _generation_key(sandbox_id: str) -> str:
    return f"preview:cap:gen:{sandbox_id}"


def _new_token() -> str:
    return base64.b32encode(secrets.token_bytes(_TOKEN_BYTES)).decode().lower()


def _redis() -> Any:
    cache = get_cache_client()
    if not cache.enabled or cache.client is None:
        raise PreviewProxyUnavailable("Redis is not available")
    return cache.client


def preview_origin(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError(f"not an origin a preview can be served from: {url!r}")
    return f"{parts.scheme}://{parts.netloc}"


async def mint(
    info: PreviewInfo,
    *,
    sandbox_id: str,
    port: int,
    workspace_id: str,
    expires_in: int,
    base: PreviewBase,
) -> str:
    """A capability URL for *info*, live for *expires_in* seconds.

    Fails closed: a record that could not be written raises rather than hand
    out a URL that would answer 404.
    """
    origin = preview_origin(info.url)
    if info.process_local:
        # Another worker holds no listener on that port, or one forwarding to
        # another machine, so the record keeps only what re-resolves it there.
        upstream = None
    elif is_loopback_url(origin):
        raise ValueError(f"a preview on loopback that is not process-local: {origin}")
    else:
        upstream = Upstream(origin, dict(info.auth_headers))
    client = _redis()
    key = _generation_key(sandbox_id)
    try:
        async with client.pipeline(transaction=True) as pipe:
            # A random start, so a counter lost and recreated never matches a
            # record minted against the old one.
            pipe.set(key, secrets.randbits(62), nx=True)
            pipe.expire(key, _GENERATION_TTL_S)
            pipe.get(key)
            _, _, raw = await pipe.execute()
        generation = int(raw)
    except Exception as e:
        raise PreviewProxyUnavailable("could not read the revocation counter") from e

    token = _new_token()
    capability = Capability(
        sandbox_id=sandbox_id,
        port=int(port),
        upstream=upstream,
        workspace_id=workspace_id,
        generation=generation,
        expires_at=int(time.time()) + expires_in,
    )
    if not await get_cache_client().set(
        _record_key(token), capability.to_record(), ttl=expires_in
    ):
        raise PreviewProxyUnavailable("could not store the capability")
    return base.url(port, token)


async def resolve(token: str) -> Capability | None:
    """The live capability *token* names, or None when it never existed,
    lapsed or was revoked. Raises ``PreviewProxyUnavailable`` when Redis
    cannot say, which the proxy answers with a 503 rather than a 404."""
    try:
        record = await get_cache_client().get_strict(_record_key(token))
    except ValueError:
        # Not JSON: no record this code wrote.
        return None
    except Exception as e:
        raise PreviewProxyUnavailable("could not read the capability") from e
    if record is None:
        return None
    capability = Capability.from_record(record)
    if capability is None or capability.expires_at <= time.time():
        return None
    if not await is_current(capability):
        return None
    return capability


async def resolve_host(host: str | None, *, base: PreviewBase) -> Capability | None:
    """The live capability a request to *host* names, or None when it names
    none. Raises ``PreviewProxyUnavailable`` when Redis cannot say."""
    named = base.parse(host)
    if named is None:
        return None
    port, token = named
    capability = await resolve(token)
    if capability is None or capability.port != port:
        return None
    return capability


async def is_current(capability: Capability) -> bool:
    """Whether *capability* has not been revoked since it was resolved.

    Expiry is not revocation: an open connection outlives its capability, as
    a page loaded on a signed URL does.
    """
    try:
        current = await _redis().get(_generation_key(capability.sandbox_id))
    except Exception as e:
        raise PreviewProxyUnavailable("could not read the revocation counter") from e
    return current is not None and int(current) == capability.generation


async def until_revoked(capability: Capability) -> None:
    """Return once *capability* is revoked, re-reading every ``RECHECK_S``.

    Unknown is not revoked: an open stream or socket rides out a Redis blip.
    """
    while True:
        await asyncio.sleep(RECHECK_S)
        try:
            if not await is_current(capability):
                return
        except PreviewProxyUnavailable:
            continue


async def revoke(sandbox_id: str | None) -> None:
    """End every capability minted for *sandbox_id*, on every worker.

    Best effort, and idempotent: a failure is logged, and the capabilities
    still lapse on their own expiry.
    """
    if not sandbox_id:
        return
    try:
        client = _redis()
        key = _generation_key(sandbox_id)
        async with client.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, _GENERATION_TTL_S)
            await pipe.execute()
    except Exception as e:
        logger.warning("Could not revoke previews of sandbox %s: %s", sandbox_id, e)


async def publish(
    info: PreviewInfo,
    *,
    sandbox_id: str,
    port: int,
    workspace_id: str,
    expires_in: int,
) -> str:
    """The URL a browser is handed for *info*.

    The one place a preview URL becomes browser-facing: a URL the provider
    made for browsers passes through, and any other is exchanged for a
    capability, so a raw sandbox host never reaches a page.
    """
    if info.browser_ready:
        return info.url
    base = preview_base()
    if base is None:
        raise PreviewProxyNotConfigured(
            "this computer's previews need the preview proxy"
        )
    return await mint(
        info,
        sandbox_id=sandbox_id,
        port=port,
        workspace_id=workspace_id,
        expires_in=expires_in,
        base=base,
    )
