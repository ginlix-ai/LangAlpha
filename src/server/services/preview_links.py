"""The preview URL a browser is handed for a machine port, and who owns that port.

The signed-URL cache, the owner claim and the launch lease all live in Redis,
so any worker can answer for a port another worker launched. The cache only
ever holds a URL ``publish`` returned: ``_publish_and_cache`` is its one writer.
"""

from __future__ import annotations

import asyncio
import hashlib
import shlex
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

from ptc_agent.core.sandbox.preview_probe import head_preview
from ptc_agent.core.sandbox.runtime import PreviewInfo
from src.server.database.workspace import get_preview_command
from src.server.services.preview_capabilities import publish
from src.server.services.preview_upstreams import probe_target
from src.utils.cache.redis_cache import get_cache_client

_SIGNED_URL_TTL = 3000  # 50 min (signed URLs expire in 1h)
# What a provider signs a preview URL for when the caller names no lifetime.
SIGNED_URL_LIFETIME = 3600
# How long before its expiry a cached signed URL stops being handed out.
_SIGNED_URL_CACHE_MARGIN = 600
_PREVIEW_LAUNCH_LEASE_TTL_MS = 60_000
_PREVIEW_LAUNCH_WAIT_S = 45.0


def _preview_cache_key(sandbox_id: str, port: int) -> str:
    """Redis key for cached signed preview URL."""
    return f"preview:signed_url:{sandbox_id}:{port}"


def _preview_owner_key(sandbox_id: str, port: int) -> str:
    return f"preview:owner:{sandbox_id}:{port}"


def _preview_launch_key(sandbox_id: str, port: int) -> str:
    return f"preview:launch:{sandbox_id}:{port}"


def _preview_expiry_key(signed_url: str) -> str:
    """Keyed by the URL itself, so no reader can pair one URL with another's expiry."""
    digest = hashlib.sha256(signed_url.encode("utf-8")).hexdigest()[:32]
    return f"preview:signed_url_exp:{digest}"


@asynccontextmanager
async def preview_launch_lease(sandbox_id: str, port: int) -> AsyncIterator[None]:
    """Serialize one machine port across workers while its owner is decided."""
    cache = get_cache_client()
    key = _preview_launch_key(sandbox_id, port)
    token = uuid.uuid4().hex
    held = False
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _PREVIEW_LAUNCH_WAIT_S
    while True:
        acquired = await cache.acquire_lock(
            key, token, _PREVIEW_LAUNCH_LEASE_TTL_MS
        )
        if acquired is None:
            raise RuntimeError(
                "Preview coordination is unavailable. Try again."
            )
        if acquired:
            held = True
            break
        if loop.time() >= deadline:
            raise RuntimeError(f"Port {port} is busy. Try again.")
        await asyncio.sleep(0.05)
    try:
        yield
    finally:
        if held:
            await asyncio.shield(cache.release_lock(key, token))


async def get_preview_owner(sandbox_id: str, port: int) -> str | None:
    return await get_cache_client().get(_preview_owner_key(sandbox_id, port))


async def set_preview_owner(sandbox_id: str, port: int, workspace_id: str) -> None:
    # The sandbox id scopes this claim to one machine lifetime. Keep it while
    # that machine exists so another worker can recognize the running server
    # after the short signed-URL cache expires.
    await get_cache_client().set(
        _preview_owner_key(sandbox_id, port), workspace_id
    )


async def delete_preview_owner(sandbox_id: str, port: int) -> None:
    await get_cache_client().delete(_preview_owner_key(sandbox_id, port))


async def _get_cached_signed_url(sandbox_id: str, port: int) -> str | None:
    """Get cached signed URL from Redis."""
    cache = get_cache_client()
    return await cache.get(_preview_cache_key(sandbox_id, port))


async def _set_cached_signed_url(
    sandbox_id: str,
    port: int,
    url: str,
    *,
    expires_in: int | None = None,
    owner_workspace_id: str | None = None,
    url_expires_at: int | None = None,
) -> None:
    """Cache a signed URL in Redis with TTL, and record when the URL itself lapses.

    Args:
        expires_in: Actual signed URL expiry in seconds. When provided the
            cache TTL is set to ``expires_in - _SIGNED_URL_CACHE_MARGIN``,
            clamped to [60, _SIGNED_URL_TTL]. Falls back to _SIGNED_URL_TTL.
        url_expires_at: Epoch seconds the URL stops working. Omitted, the URL
            was minted just now for ``SIGNED_URL_LIFETIME``.
    """
    if expires_in is not None:
        ttl = max(60, min(expires_in - _SIGNED_URL_CACHE_MARGIN, _SIGNED_URL_TTL))
    else:
        ttl = _SIGNED_URL_TTL
    now = int(time.time())
    url_expires_at = url_expires_at or now + SIGNED_URL_LIFETIME
    cache = get_cache_client()
    await cache.set(_preview_cache_key(sandbox_id, port), url, ttl=ttl)
    await cache.set(
        _preview_expiry_key(url), url_expires_at, ttl=max(url_expires_at - now, 1)
    )
    if owner_workspace_id:
        await set_preview_owner(sandbox_id, port, owner_workspace_id)


async def _publish_and_cache(
    sandbox_id: str,
    port: int,
    info: PreviewInfo,
    *,
    workspace_id: str,
    expires_in: int,
    url_expires_at: int,
    cache_for: int | None = None,
    claim_owner: bool = True,
) -> str:
    """Publish *info* and cache what a browser will be handed.

    *cache_for* sizes the cache TTL as ``_set_cached_signed_url``'s
    ``expires_in`` does; *claim_owner* also records this workspace as the
    port's owner.
    """
    url = await publish(
        info,
        sandbox_id=sandbox_id,
        port=port,
        workspace_id=workspace_id,
        expires_in=expires_in,
    )
    await _set_cached_signed_url(
        sandbox_id,
        port,
        url,
        expires_in=cache_for,
        owner_workspace_id=workspace_id if claim_owner else None,
        url_expires_at=url_expires_at,
    )
    return url


async def signed_url_expires_at(signed_url: str) -> int:
    """When a URL ``resolve_preview`` returned stops working, counted from its mint.

    A cached URL is older than the request that got it, so the answer comes
    from the record made at mint time. A URL cached without one is still at
    least ten minutes from lapsing: that is the margin the cache TTL keeps.
    """
    recorded = await get_cache_client().get(_preview_expiry_key(signed_url))
    if isinstance(recorded, int):
        return recorded
    return int(time.time()) + _SIGNED_URL_CACHE_MARGIN


async def cache_tool_preview(
    sandbox_id: str, port: int, info: PreviewInfo, *, workspace_id: str
) -> None:
    """Record a preview the agent's tool started, as a browser will be handed it.

    The tool holds the provider's URL; the cache holds what the panel opens,
    so a private sandbox's preview is cached as a capability. The owner claim
    lets any worker reuse the running server rather than call it a collision.
    """
    url_expires_at = int(time.time()) + SIGNED_URL_LIFETIME
    await _publish_and_cache(
        sandbox_id,
        port,
        info,
        workspace_id=workspace_id,
        expires_in=SIGNED_URL_LIFETIME,
        url_expires_at=url_expires_at,
    )


async def delete_cached_signed_url(sandbox_id: str, port: int) -> None:
    """Delete a cached signed URL from Redis."""
    cache = get_cache_client()
    await cache.delete(_preview_cache_key(sandbox_id, port))


async def _is_preview_live_confirmed(sandbox_id: str, port: int) -> bool:
    """Check if the preview was recently confirmed live (avoids repeated HEAD probes)."""
    cache = get_cache_client()
    return await cache.get(f"preview:live:{sandbox_id}:{port}") is not None


async def _set_preview_live_confirmed(sandbox_id: str, port: int, *, ttl: int = 10) -> None:
    """Mark preview as confirmed live for a short window."""
    cache = get_cache_client()
    await cache.set(f"preview:live:{sandbox_id}:{port}", "1", ttl=ttl)


async def _check_signed_url_healthy(signed_url: str) -> bool:
    """HEAD-check what the iframe would load.

    A capability URL is checked at its sandbox, with the header the sandbox
    needs, never through this server's own preview host; one that no longer
    opens anything is unhealthy, so the caller mints a fresh one.
    """
    target = await probe_target(signed_url)
    if target is None:
        return False
    url, headers = target
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await head_preview(client, url, headers)
            return 200 <= resp.status_code < 400
    except Exception:
        return False


UNSET = object()


async def resolve_preview(
    sandbox: Any,
    workspace_id: str,
    port: int,
    *,
    command: str | None | object = UNSET,
    force: bool = False,
    expires_in: int = SIGNED_URL_LIFETIME,
    work_dir: str,
) -> str:
    """Core preview URL resolution, behind ``owner_preview_url``.

    When a *command* is available (supplied by the caller or read from the
    workspace ``artifacts`` column), ``start_and_get_preview_url`` is called.
    This is the same code path as clicking the artifact: it health-checks
    the port, restarts the server if it's down, polls for readiness, and
    returns a fresh signed URL.

    Falls back to a plain ``get_preview_url`` only when no command is known.

    Pass ``command=None`` to indicate "already looked up, no command stored"
    (skips the DB lookup).  Omit the argument (or pass ``UNSET``) to have
    this function look it up from the database.
    """
    # Resolve command: explicit arg → DB lookup (only when caller didn't provide)
    cmd = command
    if cmd is UNSET:
        cmd = await get_preview_command(workspace_id, port)
    # Taken before the provider signs, so a fresh URL's recorded expiry is
    # never later than its real one.
    url_expires_at = int(time.time()) + expires_in

    if cmd:
        async with preview_launch_lease(sandbox.sandbox_id, port):
            # Read ownership only after winning the cross-worker launch lease.
            # The owner is reserved before provider work, so a sibling cannot
            # launch a second command while this one is still becoming ready.
            owner = await get_preview_owner(sandbox.sandbox_id, port)
            # Short-lived cache (60s) when a command is stored — covers burst
            # asset requests (CSS/JS/images) without risking long-lived stale URLs.
            if not force:
                cached_url = await _get_cached_signed_url(sandbox.sandbox_id, port)
                if cached_url:
                    healthy = (
                        await _is_preview_live_confirmed(sandbox.sandbox_id, port)
                        or await _check_signed_url_healthy(cached_url)
                    )
                    if healthy and owner not in (None, workspace_id):
                        raise RuntimeError(
                            f"Port {port} is already in use on this computer. "
                            "Choose another port."
                        )
                    if healthy:
                        await _set_preview_live_confirmed(
                            sandbox.sandbox_id, port, ttl=10
                        )
                        await set_preview_owner(
                            sandbox.sandbox_id, port, workspace_id
                        )
                        return cached_url

            # A different worker does not have the provider session in its local
            # map. The durable owner claim lets it reuse the healthy process rather
            # than treating the workspace's own server as a port collision.
            if owner == workspace_id and await sandbox._is_preview_reachable(port):
                preview_info = await sandbox.get_preview_url(port, expires_in)
                return await _publish_and_cache(
                    sandbox.sandbox_id,
                    port,
                    preview_info,
                    workspace_id=workspace_id,
                    expires_in=expires_in,
                    url_expires_at=url_expires_at,
                    cache_for=60,
                )
            if owner not in (None, workspace_id):
                if await sandbox._is_preview_reachable(port):
                    raise RuntimeError(
                        f"Port {port} is already in use on this computer. "
                        "Choose another port."
                    )
                await delete_preview_owner(sandbox.sandbox_id, port)

            await set_preview_owner(sandbox.sandbox_id, port, workspace_id)
            try:
                preview_info = await sandbox.start_and_get_preview_url(
                    f"cd {shlex.quote(work_dir)} && {cmd}",
                    port,
                    expires_in=expires_in,
                    owner=workspace_id,
                )
                return await _publish_and_cache(
                    sandbox.sandbox_id,
                    port,
                    preview_info,
                    workspace_id=workspace_id,
                    expires_in=expires_in,
                    url_expires_at=url_expires_at,
                    cache_for=60,
                )
            except BaseException:
                await delete_preview_owner(sandbox.sandbox_id, port)
                raise

    # A commandless redirect still addresses a shared machine port. Serialize
    # with launch so it cannot observe half-published ownership, and fail closed
    # when Redis cannot prove which workspace owns the service.
    async with preview_launch_lease(sandbox.sandbox_id, port):
        owner = await get_preview_owner(sandbox.sandbox_id, port)
        if owner not in (None, workspace_id):
            if await sandbox._is_preview_reachable(port):
                raise RuntimeError(
                    f"Port {port} is already in use on this computer. "
                    "Choose another port."
                )
            await delete_preview_owner(sandbox.sandbox_id, port)

        # No command known — try signed-URL cache, then generate fresh.
        if not force:
            cached_url = await _get_cached_signed_url(sandbox.sandbox_id, port)
            if cached_url:
                healthy = (
                    await _is_preview_live_confirmed(sandbox.sandbox_id, port)
                    or await _check_signed_url_healthy(cached_url)
                )
                if healthy:
                    await _set_preview_live_confirmed(
                        sandbox.sandbox_id, port, ttl=10
                    )
                    return cached_url

        await delete_cached_signed_url(sandbox.sandbox_id, port)
        preview_info = await sandbox.get_preview_url(port, expires_in=expires_in)
        return await _publish_and_cache(
            sandbox.sandbox_id,
            port,
            preview_info,
            workspace_id=workspace_id,
            expires_in=expires_in,
            url_expires_at=url_expires_at,
            cache_for=expires_in,
            claim_owner=False,
        )
