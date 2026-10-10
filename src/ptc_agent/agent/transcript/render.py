"""Render a checkpointed message list as JSONL, one file per turn.

The transcript is a cache of the checkpoint shaped for ``rg`` and ``Read``: one
event per line, one line per user message, assistant text, tool call or tool
result. Reasoning, compaction summaries and model-facing injections (runtime
rows, market watch, credit gate) are left out, since none is something the
user or the agent said or did.
"""

from __future__ import annotations

import hashlib
import json
import re
import zlib
from dataclasses import dataclass, field
from functools import cache, cached_property
from typing import Any, Iterable

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, ToolMessage

from ptc_agent.agent.transcript.classify import human_kind, is_run_boundary_message
from src.llms.attachment_payload import FILE_BLOCK_TYPES, IMAGE_BLOCK_TYPES

SCHEMA_VERSION = 2


@dataclass
class Segment:
    """One turn (or one subagent run) of rendered events."""

    number: int
    lines: list[str] = field(default_factory=list)
    first_id: str | None = None
    last_id: str | None = None
    at: str | None = None

    @cached_property
    def data(self) -> bytes:
        """The file's bytes, joined and encoded once: its size, its digest and
        the stored copy all come from these."""
        # A lone surrogate (text a model produced) becomes "?", as the
        # checkpoint stores it, rather than failing the whole render.
        return "".join(line + "\n" for line in self.lines).encode(errors="replace")

    @cached_property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()


@cache
def _evicted_pointer() -> re.Pattern[str]:
    """The first line of the pointer LargeResultEvictionMiddleware leaves in
    place of a large result, its path captured."""
    # Imported here: the middleware package imports this one.
    from ptc_agent.agent.middleware.large_result_eviction import TOO_LARGE_TOOL_MSG

    line = re.escape(TOO_LARGE_TOOL_MSG.split("\n", 1)[0])
    line = line.replace(re.escape("{tool_call_id}"), r"\S+")
    return re.compile(line.replace(re.escape("{file_path}"), r"(\S+)"))


def evicted_path(message: ToolMessage) -> str | None:
    text = message.content if isinstance(message.content, str) else visible_text(message.content)
    match = _evicted_pointer().match(text)
    return match.group(1) if match else None


def _turn_opened_at(message: HumanMessage) -> str | None:
    """The stamp a turn row carries, verbatim: ``runtime_update_from_message``
    would parse it, and invent one when it is missing."""
    # Imported here: the middleware package imports this one.
    from ptc_agent.agent.middleware.runtime_context.durable import RUNTIME_UPDATE_KEY
    from ptc_agent.agent.middleware.runtime_context.turn import TURN_ROW_KIND

    meta = (message.additional_kwargs or {}).get(RUNTIME_UPDATE_KEY)
    if isinstance(meta, dict) and meta.get("kind") == TURN_ROW_KIND:
        created = meta.get("created_at")
        return created if isinstance(created, str) else None
    return None


def visible_text(content: Any, *, attachments: bool = True) -> str:
    """Visible text of a content value; attachments become short placeholders."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict):
            kind = block.get("type", "")
            if kind == "text":
                parts.append(str(block.get("text", "")))
            elif attachments and kind in IMAGE_BLOCK_TYPES:
                parts.append("[image]")
            elif attachments and kind in FILE_BLOCK_TYPES:
                parts.append(f"[file: {block.get('filename') or 'file'}]")
    return "\n".join(p for p in parts if p)


def split_runs(messages: Iterable[AnyMessage]) -> list[list[AnyMessage]]:
    """Group messages into turns, each opened by a real user message.

    Anything before the first user message (a ``system`` or ``assistant``
    message a client opened the thread with) stays with the first turn, as
    replay counts it.
    """
    runs: list[list[AnyMessage]] = [[]]
    opened = False
    for message in messages:
        if is_run_boundary_message(message):
            if opened:
                runs.append([])
            opened = True
        runs[-1].append(message)
    return runs if runs[0] else []


def render_segment(
    number: int,
    messages: list[AnyMessage],
    *,
    unit: str = "turn",
    tool_names: dict[str, str] | None = None,
) -> Segment:
    """Render one turn."""
    names = tool_names if tool_names is not None else {}
    segment = Segment(number=number)

    def emit(event: dict[str, Any], message: AnyMessage) -> None:
        segment.lines.append(
            json.dumps(
                {"seq": len(segment.lines) + 1, unit: number, **event},
                ensure_ascii=False,
                default=str,
            )
        )
        segment.first_id = segment.first_id or message.id
        segment.last_id = message.id

    for message in messages:
        if isinstance(message, HumanMessage):
            at = _turn_opened_at(message)
            if at and segment.at is None:
                segment.at = at
            kind = human_kind(message)
            if kind not in ("plain", "steering"):
                continue
            event = {"type": "user", "id": message.id, "text": visible_text(message.content)}
            if kind == "steering":
                event["steering"] = True
            emit(event, message)
        elif isinstance(message, AIMessage):
            text = visible_text(message.content, attachments=False)
            if text.strip():
                emit({"type": "assistant", "id": message.id, "text": text}, message)
            for call in message.tool_calls or ():
                names[call["id"]] = call["name"]
                emit(
                    {
                        "type": "tool_call",
                        "id": message.id,
                        "call_id": call["id"],
                        "tool": call["name"],
                        "args": call.get("args", {}),
                    },
                    message,
                )
        elif isinstance(message, ToolMessage):
            event = {
                "type": "tool_result",
                "id": message.id,
                "call_id": message.tool_call_id,
                "tool": names.get(message.tool_call_id) or message.name,
                "status": message.status,
                "text": visible_text(message.content),
            }
            path = evicted_path(message)
            if path:
                event["evicted"] = path
            emit(event, message)

    # The turn row lands right after the user message, so the user event is
    # written before its time is known; stamp it now.
    if segment.at and segment.lines:
        first = json.loads(segment.lines[0])
        if first.get("type") == "user":
            first["at"] = segment.at
            segment.lines[0] = json.dumps(first, ensure_ascii=False, default=str)
    return segment


def segment_shape(
    number: int,
    messages: list[AnyMessage],
    *,
    unit: str,
    previous: str,
    tool_names: dict[str, str],
) -> str:
    """A key that changes whenever the segment's render would.

    It covers every value the render reads: each message's text and tool-call
    args by checksum, the rest as they are. A message replaced under its id
    (the reducer's way) with text of the same length still changes it. A
    checksum costs one pass over the text, a fraction of the JSON encoding a
    render does. The key chains the previous segment's, so a rewritten
    earlier turn (a regenerate, a truncation) changes every key after it.
    Records each tool call's name on the way, the map a render of a later
    segment needs.
    """
    parts = [f"{SCHEMA_VERSION}|{unit}|{number}|{previous}"]
    for message in messages:
        part = f"{type(message).__name__}|{message.id}"
        if isinstance(message, AIMessage):
            part += "|" + _checksum(visible_text(message.content, attachments=False))
            for call in message.tool_calls or ():
                tool_names[call["id"]] = call["name"]
                part += f"|{call['id']}|{call['name']}|{_args_checksum(call.get('args', {}))}"
        elif isinstance(message, ToolMessage):
            part += f"|{_checksum(visible_text(message.content))}|{message.tool_call_id}"
            part += f"|{message.status}|{message.name}"
        elif isinstance(message, HumanMessage):
            source = (message.additional_kwargs or {}).get("lc_source")
            part += f"|{_checksum(visible_text(message.content))}|{source}|{_turn_opened_at(message)}"
        parts.append(part)
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:32]


def _checksum(text: str) -> str:
    # CRC-32 beside the length: it catches any change a same-length edit
    # makes short of a deliberate collision, at memory speed, and releases
    # the GIL on a large buffer. surrogatepass: a digest must not raise on
    # text a model produced.
    return f"{len(text)}:{zlib.crc32(text.encode('utf-8', 'surrogatepass')):08x}"


def _args_checksum(args: Any) -> str:
    """Tool-call args in the order the render writes them. A string value,
    usually code, is checksummed as is, which skips escaping it."""
    if not isinstance(args, dict):
        return "j" + _checksum(json.dumps(args, ensure_ascii=False, default=str))
    return ",".join(
        f"{key!r}=s{_checksum(value)}"
        if isinstance(value, str)
        else f"{key!r}=j{_checksum(json.dumps(value, ensure_ascii=False, default=str))}"
        for key, value in args.items()
    )


def message_turns(messages: Iterable[AnyMessage], *, base: int) -> dict[str, int]:
    """The turn each message belongs to, by message id."""
    return turn_map(messages, base=base)[0]


def turn_map(
    messages: Iterable[AnyMessage], *, base: int
) -> tuple[dict[str, int], dict[int, str]]:
    """The turn each message belongs to, by message id, and the text of the
    user message that opened each turn, by turn number.

    ``base`` is how many turns the window trimmed from the head of
    ``messages`` (``Window.runs``): numbers stay those of the whole thread,
    the ones its files and earlier summaries already name.
    """
    turns: dict[str, int] = {}
    requests: dict[int, str] = {}
    for number, run in enumerate(split_runs(messages), start=base + 1):
        for message in run:
            if message.id:
                turns[message.id] = number
        opener = next((m for m in run if is_run_boundary_message(m)), None)
        if opener is not None:
            requests[number] = visible_text(opener.content)
    return turns, requests
