"""How long the main agent thought, kept for replay.

The checkpoint keeps a model call's reasoning but not its duration. A call
streams under a chunk id the checkpoint never sees (``lc_run--…``), so its
closes wait until the node's update names the message it committed, and a
call that commits nothing (an attempt a retry replaced) is dropped with them.
A run whose worker died has only its stream, so ``saved_durations`` matches
its calls to the messages its checkpoint saved instead.
"""

from collections.abc import Iterable
from typing import Any, Optional

from langchain_core.messages import AIMessage

from ptc_agent.agent.middleware.image_capture import IMAGE_MD_RE


class ReasoningDurations:
    """Reasoning closes attributed to the main agent's committed messages.

    Only the newest main-agent chunk's closes are ever committed, so a close
    from a subagent, a tool's inner model or a stop just waits under its own
    id until the next commit drops it.
    """

    def __init__(self) -> None:
        self._committed: dict[str, list[int]] = {}
        self._pending: dict[str, list[int]] = {}
        # (langgraph_node, chunk id) of the newest main-agent model chunk.
        self._last_root_chunk: Optional[tuple[str, str]] = None

    def saw_root_chunk(self, node: str, chunk_id: str) -> None:
        self._last_root_chunk = (node, chunk_id)

    def closed(self, message_id: str, elapsed_ms: int) -> None:
        self._pending.setdefault(message_id, []).append(elapsed_ms)

    def node_updated(self, update: dict) -> None:
        """Attribute the waiting closes to the message the node committed:
        the one its newest model call streamed. Closes from an earlier call
        in the same node belonged to an attempt it replaced."""
        if self._last_root_chunk is None:
            return
        node, chunk_id = self._last_root_chunk
        if node not in update:
            return
        pending, self._pending = self._pending, {}
        self._last_root_chunk = None
        closes = pending.get(chunk_id)
        output = update.get(node)
        if not closes or not isinstance(output, dict):
            return
        messages = output.get("messages")
        messages = getattr(messages, "value", messages)  # Overwrite
        if not isinstance(messages, (list, tuple)):
            messages = [messages]
        committed = next(
            (m for m in reversed(messages) if isinstance(m, AIMessage) and m.id),
            None,
        )
        if committed is not None:
            self._committed.setdefault(committed.id, []).extend(closes)

    def as_facts(self) -> dict[str, list[int]]:
        return {k: list(v) for k, v in self._committed.items()}


def saved_durations(
    frames: Iterable[tuple[str, dict[str, Any]]], saved: Iterable[Any]
) -> dict[str, list[int]]:
    """A lost run's reasoning closes from its stream, keyed by the saved
    message each timed call became.

    The stream holds no node updates to name a committed message, so a call
    is matched to the saved AIMessage that holds its tool calls, or else its
    text, each saved message once and equal texts in order. A call that saved
    nothing (the one the worker died in, an attempt a retry replaced) matches
    none and keeps no durations.
    """
    closes: dict[str, list[int]] = {}
    calls: dict[str, list[str]] = {}
    texts: dict[str, list[str]] = {}
    for event_type, data in frames:
        if not isinstance(data, dict) or str(data.get("agent")).startswith("task:"):
            continue  # a subagent lane's thinking is not the main agent's
        message_id = data.get("id")
        if not isinstance(message_id, str):
            continue
        if event_type == "tool_calls":
            calls.setdefault(message_id, []).extend(
                c["id"]
                for c in data.get("tool_calls") or ()
                if isinstance(c, dict) and isinstance(c.get("id"), str)
            )
            continue
        if event_type != "message_chunk":
            continue
        content = data.get("content")
        if data.get("content_type") == "text" and isinstance(content, str):
            texts.setdefault(message_id, []).append(content)
        elapsed = data.get("elapsed_ms")
        if (
            data.get("content_type") == "reasoning_signal"
            and content == "complete"
            and isinstance(elapsed, int)
            and not isinstance(elapsed, bool)
        ):
            closes.setdefault(message_id, []).append(elapsed)

    by_call: dict[str, str] = {}
    by_text: dict[str, list[str]] = {}
    for message in saved:
        if not isinstance(message, AIMessage) or not message.id:
            continue
        for call in message.tool_calls:
            if call.get("id"):
                by_call[call["id"]] = message.id
        if not message.tool_calls and (text := _comparable(message.text)):
            by_text.setdefault(text, []).append(message.id)

    out: dict[str, list[int]] = {}
    for message_id, spans in closes.items():
        target = next(
            (by_call[c] for c in calls.get(message_id, ()) if c in by_call), None
        )
        if target is None and message_id not in calls:
            # The call the worker died in streams after every saved one, so
            # in order it is the one left over when its text repeats.
            equal = by_text.get(_comparable("".join(texts.get(message_id, ()))))
            target = equal.pop(0) if equal else None
        if target is not None:
            out.setdefault(target, []).extend(spans)
    return out


def _comparable(text: str) -> str:
    """Text as both sides hold it: the saved copy names a captured image by
    its URL where the stream named its sandbox path."""
    return IMAGE_MD_RE.sub(lambda m: f"![{m.group(1)}]", str(text)).strip()
