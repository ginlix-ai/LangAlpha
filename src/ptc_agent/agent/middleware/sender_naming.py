"""Opens each member message of a shared workspace's call with its writer.

Installed only in a shared workspace's build, outside the row carrier, so the
request the carrier composes is already named (``transcript/sender.py``). It
fails closed: a member's text sent without the harness's tag, with any tag
they typed left unescaped, would let one member speak as another, so an error
here fails the call rather than sending the messages as stored. A user
message with no record (from before writers were recorded, or one no member
wrote) gets no tag but has any sender tag in it escaped. The rewrite is
request-only; the stored messages stay as each member wrote them.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse

from ptc_agent.agent.transcript.sender import name_senders


class SenderNamingMiddleware(AgentMiddleware):
    """Names the writer of each member message a call sends."""

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        return handler(request.override(messages=name_senders(request.messages, shared=True)))

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        return await handler(request.override(messages=name_senders(request.messages, shared=True)))
