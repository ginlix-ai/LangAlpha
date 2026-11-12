"""What fills a thread's context ahead of its messages, read for the composer's
context panel.

Counted when the panel opens, and never in a turn: it converts and tokenizes
every tool schema, which most turns nobody looks at. It writes nothing, so it
takes no mutation fence and runs beside a live turn.
"""

from __future__ import annotations

from contextlib import AsyncExitStack

from fastapi import HTTPException


async def read_context_parts(thread_id: str, *, user_id: str | None = None) -> dict:
    """What the thread's next call sends ahead of its messages, in tokens by
    part (``ThreadAgent.ahead_parts``), built as the next turn would build it.

    ``parts`` is None, with the ``reason``, where there is nothing to count
    now: the thread's config is refused (a refused login, a used-up plan, no
    credits, a revoked key), none of which a panel can act on; the computer
    is not running, which a panel does not start (``attach_only``); or the
    agent compacts nothing.
    """
    from ptc_agent.agent.middleware.compaction import NoCompaction
    from ptc_agent.core.sandbox.attach_only import SandboxNotRunning
    from src.server.handlers.thread_maintenance import _compaction_config, _thread_agent
    from src.server.services.computer_manager import attach_only

    try:
        agent_cfg, role = await _compaction_config(thread_id, user_id, clear_dead=False)
    except HTTPException:
        return _no_parts(thread_id, "refused")
    try:
        with attach_only():
            async with AsyncExitStack() as held:
                agent = await _thread_agent(
                    thread_id, "count", config=agent_cfg, user_id=user_id, held=held, role=role
                )
                parts = await agent.ahead_parts()
    except SandboxNotRunning:
        return _no_parts(thread_id, "computer_stopped")
    except NoCompaction:
        return _no_parts(thread_id, "no_compaction")
    return {"thread_id": thread_id, "parts": parts}


def _no_parts(thread_id: str, reason: str) -> dict:
    return {"thread_id": thread_id, "parts": None, "reason": reason}
