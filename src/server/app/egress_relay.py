"""Sandbox egress relay — the only path from a sandbox to an OAuth vendor.

POST /v1/egress/{grant_id}: the sandbox's generated MCP client dials this
route instead of the vendor; the relay authenticates the sandbox (relay JWT —
NEVER the app's user auth, which would let any logged-in browser drive
grants), attaches the vendor bearer host-side, and streams the exchange
through. No vendor token ever exists inside a sandbox in any form.

Deliberately outside the /api namespace: a machine endpoint, with clean
URLs on a dedicated API host (api.example.com/v1/egress/...).

Ships in OSS as an ordinary route: with no EGRESS_RELAY_SECRET configured it
answers 503 and is inert.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack

import anyio
from fastapi import APIRouter, Request, Response
from fastapi.responses import StreamingResponse

from src.server.services.egress import RelayError, RelayRejection
from src.server.services.egress.jsonrpc import MAX_BODY_BYTES
from src.server.services.egress.limits import acquire_slot
from src.server.services.egress.relay import (
    WALL_CLOCK_S,
    authenticate_relay,
    open_upstream,
    prepare_relay,
    sandbox_response_headers,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Egress Relay"])


def _reject(e: RelayRejection) -> Response:
    headers = {"X-Relay-Error": e.code}
    if e.retry_after is not None:
        headers["Retry-After"] = str(e.retry_after)
    return Response(
        status_code=e.status,
        content=e.detail,
        media_type="text/plain",
        headers=headers,
    )


async def _read_capped_body(request: Request) -> bytes:
    """Buffer the body, refusing anything past the canonical cap.

    Content-Length (when present and parseable) is rejected up front; the
    streaming read then enforces the same bound so a chunked or lying-length
    body can't slip a huge payload into memory. Same 400/"exceeds" contract the
    canonicalizer would raise — only now the bytes are never all held at once.
    """
    cap = MAX_BODY_BYTES
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared = int(content_length)
        except ValueError:
            declared = None
        if declared is not None and declared > cap:
            raise RelayRejection(
                400, RelayError.BAD_REQUEST, f"body exceeds {cap} bytes"
            )
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > cap:
            raise RelayRejection(
                400, RelayError.BAD_REQUEST, f"body exceeds {cap} bytes"
            )
        chunks.append(chunk)
    return b"".join(chunks)


@router.post("/v1/egress/{grant_id}")
async def relay(grant_id: str, request: Request) -> Response:
    # One wall-clock budget covers authenticate-to-last-byte, and the
    # concurrency slot is entered BEFORE the expensive grant read + token
    # decrypt so those run throttled — never after, where a flood would drive
    # unbounded key derivations and DB connections past the limiter.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WALL_CLOCK_S

    resources = AsyncExitStack()
    try:
        # A malformed grant id answers the same uniform 404 as an unknown one
        # (the column is a uuid) — never a 500 with a stack trace. Canonicalize,
        # not just validate: the limiter keys Redis by this string while
        # Postgres' uuid cast accepts every spelling (case, hyphens, braces), so
        # a respelled id would otherwise mint itself a fresh rate bucket.
        try:
            grant_id = str(uuid.UUID(grant_id))
        except ValueError:
            raise RelayRejection(404, RelayError.NOT_FOUND)
        try:
            async with asyncio.timeout_at(deadline):
                # Authenticate first — before the body read and before a slot
                # is taken — so an unauthenticated caller spends neither worker
                # memory nor a concurrency slot.
                claims = authenticate_relay(request.headers.get("authorization"))
                await resources.enter_async_context(acquire_slot(grant_id))
                raw_body = await _read_capped_body(request)
                prepared = await prepare_relay(
                    grant_id, claims=claims, raw_body=raw_body
                )
                upstream = await open_upstream(prepared, dict(request.headers))
        except TimeoutError:
            raise RelayRejection(
                504, RelayError.WALL_CLOCK, "relay wall clock exceeded"
            )
    except RelayRejection as e:
        with anyio.CancelScope(shield=True):
            await resources.aclose()
        return _reject(e)
    except BaseException:
        # Shield the release: a cancellation here (client gone during setup)
        # would otherwise skip aclose and leak the slot + connection.
        with anyio.CancelScope(shield=True):
            await resources.aclose()
        raise

    async def stream() -> AsyncIterator[bytes]:
        # The vendor stream lives INSIDE the generator: starlette runs this
        # (and the finally) whether the exchange completes, the wall clock
        # fires, or the sandbox disconnects mid-stream.
        try:
            aiter = upstream.aiter_bytes()
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    logger.warning(
                        "[egress_relay] wall clock cut stream for grant %s",
                        grant_id,
                    )
                    break
                try:
                    chunk = await asyncio.wait_for(
                        aiter.__anext__(), timeout=remaining
                    )
                except StopAsyncIteration:
                    break
                except TimeoutError:
                    logger.warning(
                        "[egress_relay] wall clock cut stream for grant %s",
                        grant_id,
                    )
                    break
                yield chunk
        finally:
            # Starlette runs this generator inside an anyio cancel scope that
            # re-delivers CancelledError at EVERY await, so an unshielded first
            # close would take the cancellation and skip the slot + connection
            # release — leaking a concurrency slot that never ages out (each new
            # request re-EXPIREs the key) and an upstream connection. Shield so
            # both closes run to completion.
            with anyio.CancelScope(shield=True):
                await upstream.aclose()
                await resources.aclose()

    return StreamingResponse(
        stream(),
        status_code=upstream.status_code,
        headers=sandbox_response_headers(upstream),
    )
