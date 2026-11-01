"""The record of every request a computer's ``la`` makes, with its thread.

``get_principal`` reads a computer's call once (``computer_call``); this
middleware writes its row once the status is known, at the response's start
rather than its end, so a turn's stream is recorded when it opens. The write
runs in the background, so the response never waits on it. A request whose
token is refused never has a call and is not recorded. A failed write is
logged and never fails the request.
"""

from __future__ import annotations

import asyncio
import logging

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.server.database import computer_cli_calls
from src.server.dependencies.principal import CliCall, computer_call

logger = logging.getLogger(__name__)

# A write stuck past this is given up on, so a slow database cannot pile up
# the writes of every call it stalls.
_WRITE_TIMEOUT_S = 2.0

# Strong references to writes in flight, so the loop does not collect one
# before it lands. Process-local by design: the record is best-effort, and
# nothing reads this set as state.
_writes: set[asyncio.Task] = set()


def _targets(scope: Scope) -> dict[str, str]:
    """The ids the route names in its path, the request's targets."""
    params = scope.get("path_params") or {}
    return {k: str(v) for k, v in params.items() if k.endswith("_id")}


async def _record(call: CliCall, scope: Scope, status: int) -> None:
    route = scope.get("route")
    try:
        await asyncio.wait_for(
            computer_cli_calls.record(
                computer_id=call.principal.computer_id,
                user_id=call.principal.user_id,
                thread_id=call.thread_id,
                call_id=call.call_id,
                method=scope["method"],
                route=getattr(route, "path", None) or scope["path"],
                targets=_targets(scope),
                status=status,
            ),
            _WRITE_TIMEOUT_S,
        )
    except Exception:
        logger.warning("[cli_calls] call not recorded", exc_info=True)


def _record_soon(scope: Scope, status: int) -> None:
    call = computer_call(scope)
    if call is None:
        return
    task = asyncio.create_task(_record(call, scope, status))
    _writes.add(task)
    task.add_done_callback(_writes.discard)


class CliCallRecorder:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        recorded = False

        async def recording_send(message: Message) -> None:
            nonlocal recorded
            if message["type"] == "http.response.start" and not recorded:
                recorded = True
                _record_soon(scope, int(message["status"]))
            await send(message)

        try:
            await self.app(scope, receive, recording_send)
        except Exception:
            if not recorded:
                recorded = True
                _record_soon(scope, 500)
            raise
