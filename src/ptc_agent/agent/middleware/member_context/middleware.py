"""The main agent's middleware that adds a member's own context to a call.

Built for one turn of one member, and only when their organization lets
their own context reach the shared workspace (the server's ``grant``), so a
turn of anyone else has no such middleware at all. Subagents never get it:
their brief is the main agent's to write.

It sits inside ``SenderNamingMiddleware``, so the message it adds to is
already named, and just outside ``RowCarrierMiddleware``, so what it adds is
request only: the carrier composes the copy and nothing reaches the
checkpoint. A
provider's own compaction request is left as it is, since the thread keeps
its summary for every member's later calls; the summaries asked in a
trailing harness message need nothing here, since that message, which no
member wrote, ends the streak.

The member's memory files reach the main agent's Read and Glob too
(``files.py``). What such a call returns is kept from the thread the same
way: the tool message stored is a stub naming whose memory was read, with
what to read again under ``MEMBER_READ_KEY``, and each call in that member's
streak gets the real result in its place, from this turn's own read or, on a
later turn, a read made again through the same tool. Being innermost of the
tool wrappers, it stubs the result before any other wrapper, the large-result
eviction and provenance among them, sees it.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import ToolMessage
from langgraph.errors import GraphBubbleUp

from ptc_agent.agent.middleware.member_context.context import (
    MemberContext,
    render_block,
    streak_anchor,
    with_block,
)
from ptc_agent.agent.middleware.member_context.files import (
    MEMBER_READ_KEY,
    MEMORY_READ_CHARS,
    READ_ARG,
    MemberMount,
    MemberRead,
)
from ptc_agent.agent.transcript.sender import MemberSender
from src.llms.provider_compaction import is_compaction_request

logger = logging.getLogger(__name__)

#: The index before the turn's one read of it.
_UNREAD = object()
#: How much of the member's reads one call is sent filled back in, newest
#: first; older ones stay stubs past it. The compaction trigger measures the
#: thread as stored, stubs and all, so this bounds what it cannot see.
FILL_BUDGET_CHARS = MEMORY_READ_CHARS
#: How Read and Glob report an exception they caught, the exception's text
#: after it.
_CAUGHT = ("ERROR: Failed to read file:", "ERROR: Failed to glob files:")


def stub_text(member: MemberContext, tool: str) -> str:
    """What the thread keeps of a read of ``member``'s memory.

    Where the content is not filled back in (another member's turn, or this
    workspace no longer using members' own context), the model reads only this,
    so it says the read did return something: without that, a later turn took
    the stub for all the read ever gave and told the member it never saw it.
    """
    verb = "Listed" if tool == "Glob" else "Read from"
    name = member.shown_name
    return (
        f"[{verb} {name}'s memory. What it returned is shown to you only on "
        f"{name}'s messages while this workspace uses members' own context; "
        "where it is not shown here, it was there when read and is hidden now.]"
    )


def grep_refusal(member: MemberContext) -> str:
    return (
        f"ERROR: Grep cannot search {member.shown_name}'s memory. List it with Glob and open "
        "a file with Read."
    )


class MemberContextMiddleware(AgentMiddleware):
    """Adds ``member``'s block to every call in their streak.

    ``mount`` is their memory on the main agent's file tools
    (``member_memory``), None for a build without it, whose block names no
    folder and which stubs nothing. Its index is read once a turn; the block
    is rendered from it on every call, the same bytes each time.
    """

    def __init__(self, member: MemberContext, *, mount: MemberMount | None) -> None:
        super().__init__()
        self.member = member
        self._mount = mount
        self._index: Any = _UNREAD
        self._lock = asyncio.Lock()
        # This turn's reads of the memory, by tool call id and what the call
        # read, since a provider may give two calls one id: execution context
        # for the one worker running the turn, never consulted across turns.
        self._reads: dict[tuple[str, str], Any] = {}

    async def _memory_index(self) -> str | None:
        if self._mount is None:
            return None
        async with self._lock:
            if self._index is _UNREAD:
                self._index = await self._mount.index()
        return self._index

    async def block(self, name: str | None = None) -> str | None:
        """The block under ``name``, the member's own by default."""
        return render_block(
            self.member,
            await self._memory_index(),
            folder=self._mount.folder if self._mount is not None else None,
            name=name,
        )

    async def compose(self, messages: list[Any]) -> list[Any] | None:
        """``messages`` as this call sends them, or None to send them as
        they are: the member's reads in their streak filled back in, and
        the block at the streak's start."""
        anchor = streak_anchor(messages, self.member.user_id)
        if anchor is None:
            return None
        out = list(messages)
        filled = False
        room = FILL_BUDGET_CHARS
        for index in range(len(out) - 1, anchor.index, -1):
            read = MemberRead.of(out[index])
            if read is None or read.user_id != self.member.user_id:
                continue
            message = await self._filled(out[index], read)
            size = len(str(message.content))
            if message is out[index]:
                continue
            # The newest is always sent: it is what the model just asked for.
            if filled and size > room:
                break
            out[index], filled, room = message, True, room - size
        # Named as the streak's first message was sent, so a rename while it
        # runs leaves the bytes where they were.
        sender = None if anchor.after_summary else MemberSender.of(messages[anchor.index])
        block = await self.block(sender.name if sender is not None else None)
        if block is not None:
            return with_block(out, anchor, block)
        return out if filled else None

    async def _filled(self, stub: ToolMessage, read: MemberRead) -> ToolMessage:
        """``stub`` as this call sends it: the read's result in its place."""
        key = (stub.tool_call_id, read.key)
        if key in self._reads:
            content = self._reads[key]
        elif self._mount is None:
            return stub
        else:
            try:
                content = await self._mount.read_again(read)
            except Exception:  # noqa: BLE001 - the stub stands where a read fails
                logger.warning("[member_context] reading a stub's file again failed", exc_info=True)
                return stub
            if content is None:
                return stub
            self._reads[key] = content
        kwargs = {k: v for k, v in (stub.additional_kwargs or {}).items() if k != MEMBER_READ_KEY}
        return stub.model_copy(update={"content": content, "additional_kwargs": kwargs})

    def _failed(self) -> str:
        return f"ERROR: reading {self.member.shown_name}'s memory failed. Try again."

    def wrap_tool_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return handler(request)

    async def awrap_tool_call(self, request: Any, handler: Callable[[Any], Awaitable[Any]]) -> Any:
        mount = self._mount
        if mount is None:
            return await handler(request)
        call = request.tool_call
        tool, args, call_id = call.get("name"), call.get("args") or {}, call.get("id")
        # No search from above reaches the memory (``reached_only_inside``);
        # this refuses the one aimed at it.
        if tool == "Grep" and mount.inside(args.get("path")):
            return ToolMessage(
                content=grep_refusal(self.member), tool_call_id=call_id, name=tool, status="error"
            )
        if tool not in READ_ARG or not mount.inside(args.get(READ_ARG[tool])):
            return await handler(request)
        # The aliasing too: a listing that fails under it must not reach the
        # thread as its own error text.
        try:
            result = await handler(request)
            content = getattr(result, "content", result)
            filled = await mount.aliased(tool, args, content)
            if getattr(result, "status", None) == "error" or (
                isinstance(filled, str) and filled.startswith("ERROR")
            ):
                # A read that returned nothing is kept as its error, no stub:
                # there is nothing to fill back in on a later call. A failure
                # the tool caught is kept as ours, since its text could name a
                # real file; a refusal (not found, a folder) is kept as it is.
                if isinstance(filled, str) and filled.startswith(_CAUGHT):
                    filled = self._failed()
                return ToolMessage(content=filled, tool_call_id=call_id, name=tool, status="error")
        except GraphBubbleUp:
            raise
        except Exception:  # noqa: BLE001 - its text could name a real file
            logger.warning("[member_context] reading the memory failed", exc_info=True)
            return ToolMessage(
                content=self._failed(),
                tool_call_id=call_id,
                name=tool,
                status="error",
            )
        read = MemberRead(self.member.user_id, tool, dict(args))
        self._reads[(call_id, read.key)] = filled
        return ToolMessage(
            content=stub_text(self.member, tool),
            tool_call_id=call_id,
            name=tool,
            id=getattr(result, "id", None),
            status=getattr(result, "status", "success"),
            additional_kwargs={MEMBER_READ_KEY: read.as_dict()},
        )

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        if is_compaction_request(request.model_settings):
            return await handler(request)
        try:
            composed = await self.compose(list(request.messages or []))
        except Exception:  # noqa: BLE001 - leaving it out never fails a turn
            logger.warning("[member_context] composing the call failed", exc_info=True)
            composed = None
        if composed is None:
            return await handler(request)
        return await handler(request.override(messages=composed))
