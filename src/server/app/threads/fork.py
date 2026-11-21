"""POST /api/v1/threads/{thread_id}/fork — copy a chat into a new one from a turn."""

from typing import Optional

from fastapi.encoders import jsonable_encoder

from src.config.settings import HOST_MODE
from src.server.database.conversation import threads_read
from src.server.dependencies.principal import RunPrincipal, refuse_computer
from src.server.models.conversation import ThreadForkRequest
from src.server.services.forks import CUTS, ForkRequest, fork_thread
from src.server.services.shared_workspaces.access import thread_access

from ._deps import router
from .crud import get_thread


@router.post("/{thread_id}/fork", status_code=201)
async def fork_thread_endpoint(
    thread_id: str, request: ThreadForkRequest, principal: RunPrincipal
):
    """A new thread holding this one's history up to ``turn_index``. It runs
    nothing: the caller sends the fork's first message as any other. Answers
    the new thread as ``GET /{thread_id}`` reads it, with ``turns_copied``
    and, for a ``before_turn`` cut, the cut turn's message as ``prefill``."""
    refuse_computer(principal, "A computer's token cannot copy a chat")
    await _admit(thread_id, principal.user_id, request.platform)
    result = await fork_thread(
        ForkRequest(
            source_thread_id=thread_id,
            cut=CUTS[request.cut](request.turn_index),
            user_id=principal.user_id,
            title=request.title,
            platform=request.platform,
        )
    )
    return {
        "thread": jsonable_encoder(await get_thread(result.thread_id, principal.user_id)),
        "turns_copied": result.turns_copied,
        "prefill": result.prefill,
    }


async def _admit(thread_id: str, user_id: str, platform: Optional[str]) -> None:
    """The credit gate ``POST /threads`` applies, with the same account, byok
    and surface: a quota-blocked account mints no durable rows through a copy
    either. After the access check, so a refusal never tells a reader who
    cannot see the chat that it exists."""
    if HOST_MODE == "oss":
        return
    from src.server.dependencies.usage_limits import enforce_credit_limit
    from src.server.services.account import own_access
    from src.server.services.shared_workspaces.turn import TurnIdentity

    meta = await threads_read.get_thread_auth_meta(thread_id)
    await thread_access(thread_id, user_id, meta=meta)
    identity = await TurnIdentity.of(user_id, str(meta["user_id"]))
    await enforce_credit_limit(
        identity.recorded_as, byok=(await own_access(user_id)).any, surface=platform
    )
