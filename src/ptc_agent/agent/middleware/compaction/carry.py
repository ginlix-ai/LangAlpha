"""The last step, carried past a summary that keeps no turn mid-turn.

A summary in the middle of a turn that rebuilds a stale system prompt keeps
no turn, since the thinking in any it kept is bound to the old prompt. The
step under way would go with them, and the summary only paraphrases what its
results showed, so the calls the newest answer made are sent after the summary
in one row, each with the arguments it was sent and the result it returned as
they were. A row, because a tool result cannot follow the summary without the
call that asked for it, and that call not without its thinking.

Text a tool returned reaches the model in a row this way, so the row frames it
as data, is carried in the user role on every carrier (see
``carrier.USER_ROLE_KINDS``), and escapes the tags that would open or close
one of its own, or the reminder around it. The row is sent on every later call, so an
argument or a result too large to repeat, or an image or a PDF, is saved in the
thread's large-results folder beside what eviction saves, under names of its
own, and carried as its path, with the first and last lines of text.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import re
from collections.abc import Sequence
from typing import Any

from deepagents.backends.utils import sanitize_tool_call_id
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, ToolMessage

from src.llms.attachment_payload import is_attachment_block, is_image_block

from ptc_agent.agent.middleware.compaction.offloading import MIME_TO_EXT, extract_base64_info
from ptc_agent.agent.middleware.large_result_eviction import (
    NUM_CHARS_PER_TOKEN,
    content_preview,
    detect_extension,
)
from ptc_agent.agent.middleware.runtime_context.changes import UPDATE_SCHEMA_VERSION
from ptc_agent.agent.middleware.runtime_context.durable import DurableUpdate, written_row
from ptc_agent.agent.middleware.runtime_context.envelope import ENVELOPE_CLOSE
from ptc_agent.agent.middleware.runtime_context.turn import LAST_STEP_ROW_KIND

logger = logging.getLogger(__name__)

#: The longest a call's arguments, and its result, are carried in the row, in
#: tokens; past that each is saved to a file and carried as its path.
ARGS_TOKENS = 2_000
RESULT_TOKENS = 8_000
#: The most text the row carries in all, past which the rest is saved too,
#: each with only its first and last line once the previews have used it up.
TOTAL_TOKENS = 16_000

_SAVED_ARGUMENTS = (
    "The arguments were {size} and are saved in `{path}`; their first and last lines follow. "
    "Read the file for the rest."
)
_SAVED_RESULT = (
    "The result was {size} and is saved in `{path}`; its first and last lines follow. "
    "Read the file for the rest."
)
_SAVED_ATTACHMENT = "The result was {size} and is saved in `{path}`. Read the file to see it."

_FRAME_TAG = re.compile(rf"<(?=/?(?:tool_call|arguments|result|{re.escape(ENVELOPE_CLOSE[2:-1])})\b)", re.IGNORECASE)


def _escaped(text: str) -> str:
    """``text`` with no tag in it that could open or close one of the row's
    own, or the reminder around it."""
    return _FRAME_TAG.sub("&lt;", text)


def _attribute(value: Any) -> str:
    return str(value).replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")


async def last_step_row(step: Sequence[AnyMessage], *, backend: Any | None, folder: str) -> HumanMessage | None:
    """The row carrying the calls of the newest answer in ``step``, the
    messages a summary ends on, each with its arguments and its result; None
    when that answer called no tool. What is too large to carry is saved in
    ``folder`` through ``backend``, and carried whole when it cannot be."""
    at = next((i for i in range(len(step) - 1, -1, -1) if isinstance(step[i], AIMessage)), None)
    if at is None or not step[at].tool_calls:
        return None
    results = {m.tool_call_id: m for m in step[at + 1 :] if isinstance(m, ToolMessage)}
    carrier = _Carrier(backend, folder, TOTAL_TOKENS * NUM_CHARS_PER_TOKEN)
    calls = []
    for call in step[at].tool_calls:
        name = sanitize_tool_call_id(call["id"])
        arguments = await carrier.text(
            json.dumps(call.get("args") or {}, ensure_ascii=False),
            ARGS_TOKENS,
            f"{name}.carried-args.json",
            _SAVED_ARGUMENTS,
        )
        result = results.get(call["id"])
        returned = await carrier.result(result, name) if result is not None else ""
        status = ' status="error"' if result is not None and result.status == "error" else ""
        calls.append(
            f'<tool_call tool="{_attribute(call["name"])}" id="{_attribute(call["id"])}">\n'
            f"<arguments>{arguments}</arguments>\n<result{status}>{returned}</result>\n</tool_call>"
        )
    return written_row(
        DurableUpdate(
            kind=LAST_STEP_ROW_KIND,
            schema_version=UPDATE_SCHEMA_VERSION,
            text="\n".join(calls),
            provenance={"source": "harness", "calls": len(calls)},
        )
    )


class _Carrier:
    """Carries each text inline while it fits under its own limit and the
    room the row has left, and saves the rest in ``folder``, the preview
    taking room too. Names end in ``.carried``, so no save replaces a result
    eviction saved under the call's id."""

    def __init__(self, backend: Any | None, folder: str, room: int) -> None:
        self._backend = backend
        self._folder = folder
        self._room = room

    async def result(self, message: ToolMessage, name: str) -> str:
        """``message``'s content as carried: its text, then each attachment
        as the file it is saved in."""
        content = message.content
        texts: list[str] = []
        attachments: list[str] = []
        for block in content if isinstance(content, list) else [content]:
            if isinstance(block, str):
                texts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                texts.append(str(block.get("text") or ""))
            elif is_attachment_block(block):
                attachments.append(await self._attachment(block, f"{name}_{len(attachments)}"))
            else:
                texts.append(json.dumps(block, ensure_ascii=False, default=str))
        joined = "\n".join(texts)
        text = await self.text(joined, RESULT_TOKENS, f"{name}.carried{detect_extension(joined)}", _SAVED_RESULT)
        return "\n".join(part for part in (text, *attachments) if part)

    async def text(self, text: str, limit_tokens: int, name: str, saved: str) -> str:
        if len(text) <= min(limit_tokens * NUM_CHARS_PER_TOKEN, self._room):
            self._room -= len(text)
            return _escaped(text)
        path = f"{self._folder}/{name}"
        if not await self._save(path, text):
            self._room -= len(text)
            return _escaped(text)
        preview = content_preview(text)
        if len(preview) > self._room:
            preview = content_preview(text, head_lines=1, tail_lines=1)
        self._room -= len(preview)
        return f"{saved.format(size=f'{len(text):,} characters', path=path)}\n{_escaped(preview)}"

    async def _attachment(self, block: dict[str, Any], name: str) -> str:
        kind = "image" if is_image_block(block) else "PDF"
        info = extract_base64_info(block)
        if info is None:
            return f"[{kind}]"
        data, mime, _label = info
        try:
            raw = base64.b64decode(data)
        except (binascii.Error, ValueError):
            return f"[{kind}]"
        path = f"{self._folder}/{name}.carried.{MIME_TO_EXT.get(mime, 'bin')}"
        if not await self._save(path, raw):
            return f"[{kind}]"
        return _SAVED_ATTACHMENT.format(size=f"a {max(len(raw) // 1024, 1):,} KB {kind}", path=path)

    async def _save(self, path: str, data: str | bytes) -> bool:
        if self._backend is None:
            return False
        try:
            if isinstance(data, str):
                written = await self._backend.awrite(path, data, overwrite=True)
                saved = written is not None and not written.error
            else:
                uploaded = await self._backend.aupload_files([(path, data)])
                saved = bool(uploaded) and not any(getattr(u, "error", None) for u in uploaded)
        except Exception:  # noqa: BLE001 - carried whole rather than lost
            logger.warning("[Compaction] saving %s for the carried step failed", path, exc_info=True)
            return False
        if not saved:
            logger.warning("[Compaction] saving %s for the carried step failed", path)
        return saved
