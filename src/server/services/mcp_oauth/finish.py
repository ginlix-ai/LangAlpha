"""What a callback does around its token exchange, and the save it may park.

A reconnect stores a connection under a row that already exists; a sign-in
that saves its server (``services.mcp_saves``) also writes the row around it.
``complete_callback`` meets both as a ``CallbackFinish``, so it is one flow
whichever it is. A save's payload waits for its callback in
``mcp_oauth_pending``, encrypted, parked and claimed here.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from pydantic import BaseModel, ValidationError

from src.server.database.mcp_oauth_pending import (
    claim_pending,
    drop_pending,
    store_pending,
)
from src.server.services.mcp_oauth.redirects import CallbackError

logger = logging.getLogger(__name__)

# The shape of every parked save. A sign-in outlives a deploy only by its TTL,
# and one parked in another shape is started again.
PARKED_VERSION = 1

StoreConnection = Callable[[], Awaitable[Any]]


class CallbackFailed(Exception):
    """A callback that could not finish; nothing of its save is left.

    ``detail`` is a sentence for the user, carried on the redirect.
    """

    def __init__(self, reason: CallbackError, detail: str | None = None) -> None:
        super().__init__(detail or reason.value)
        self.reason = reason
        self.detail = detail


def stale_sign_in() -> CallbackFailed:
    return CallbackFailed(
        CallbackError.INSTALL_FAILED,
        "this sign-in was started by another version; start it again",
    )


class CallbackFinish(Protocol):
    """What a callback does around its exchange, in this order.

    ``before_exchange`` refuses while the code is unspent, ``after_exchange``
    asks again with the new token in hand, and ``write`` stores the connection
    through the callable it is handed, with whatever goes with it. Each raises
    CallbackFailed with nothing of the finish left behind. ``after_connect``
    follows a write that landed, best effort: it may fail without the connect
    being reported as failed.
    """

    # The plugin a sign-in installs, which the redirect names.
    plugin: str | None

    async def before_exchange(self) -> None: ...

    async def after_exchange(self, bearer: str) -> None: ...

    async def write(self, store: StoreConnection) -> None: ...

    async def after_connect(self) -> None: ...


@dataclass(frozen=True)
class PreparedSignIn:
    """A save a start parks for its callback, and the server it signs in to.

    ``server`` is the row the connection is stored under, which for a plugin
    is the name its install will give the sign-in server.
    """

    kind: str
    server: str
    url: str
    body: BaseModel
    plugin: str | None = None


class Parked(BaseModel):
    """The one envelope every parked save is stored in."""

    kind: str
    version: int
    body: dict[str, Any]


async def park(
    state: str, *, user_id: str, prepared: PreparedSignIn, ttl_seconds: int
) -> None:
    # A plugin parks its whole package, up to 64 MiB as base64 text: encoded
    # on the event loop, it would hold every stream on this worker meanwhile.
    body = await asyncio.to_thread(prepared.body.model_dump, mode="json")
    envelope = Parked(kind=prepared.kind, version=PARKED_VERSION, body=body)
    await store_pending(
        state,
        user_id=user_id,
        kind=prepared.kind,
        server_name=prepared.server,
        payload=envelope.model_dump(),
        ttl_seconds=ttl_seconds,
    )


async def discard(state: str) -> None:
    """Drop the payload of a sign-in that can no longer reach its callback.

    It can hold secret values, so not later than now, though the sweep would
    reach it once it expired: best effort, logged.
    """
    try:
        await drop_pending(state)
    except Exception:
        logger.warning("[mcp_oauth] could not drop a sign-in payload", exc_info=True)


async def claim_parked(state: str, user_id: str, server_name: str) -> Parked:
    """The save parked under ``state``, taken once.

    Missing when it expired, was retired by a newer sign-in, or was parked
    for another server under this state: all read as an expired sign-in.
    """
    row = await claim_pending(state, user_id)
    if row is None or row.server_name != server_name:
        raise CallbackFailed(CallbackError.INVALID_STATE)
    try:
        parked = Parked.model_validate(row.payload)
    except ValidationError:
        raise stale_sign_in() from None
    if parked.version != PARKED_VERSION:
        raise stale_sign_in()
    return parked
