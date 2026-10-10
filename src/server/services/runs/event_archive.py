"""The merged event archive a run persists as ``sse_events``.

Token-level frames merge per message and per tool call, so the archive grows
with the turn rather than with its token count. Recovery salvages a dead
run's Redis stream through the same accumulator, so both archives merge alike.
"""

import copy
from typing import Any, Dict, List

from src.config.settings import get_merged_chunk_max_bytes

MERGED_STREAM_CHUNK_MAX_BYTES_DEFAULT = get_merged_chunk_max_bytes()


class StreamEventAccumulator:
    """Accumulates and merges token-level SSE events for persistence."""

    def __init__(self, max_merged_bytes: int = MERGED_STREAM_CHUNK_MAX_BYTES_DEFAULT):
        self._max_merged_bytes = max_merged_bytes
        self._events: List[Dict[str, Any]] = []

    def get_events(self) -> List[Dict[str, Any]]:
        return copy.deepcopy(self._events)

    def add(self, event_type: str, data: Dict[str, Any]) -> None:
        if not isinstance(data, dict):
            return

        incoming = copy.deepcopy(data)

        if not self._events:
            self._events.append({"event": event_type, "data": incoming})
            return

        prev = self._events[-1]
        if prev.get("event") != event_type:
            self._events.append({"event": event_type, "data": incoming})
            return

        if event_type == "message_chunk" and self._try_merge_message_chunk(prev, incoming):
            return

        if event_type == "tool_call_chunks" and self._try_merge_tool_call_chunks(prev, incoming):
            return

        self._events.append({"event": event_type, "data": incoming})

    def _try_merge_message_chunk(self, prev_event: Dict[str, Any], incoming: Dict[str, Any]) -> bool:
        prev_data = prev_event.get("data")
        if not isinstance(prev_data, dict):
            return False

        if incoming.get("content_type") == "reasoning_signal":
            return False
        if prev_data.get("content_type") == "reasoning_signal":
            return False

        merge_keys = ("thread_id", "agent", "id", "role", "content_type", "phase")
        if any(prev_data.get(k) != incoming.get(k) for k in merge_keys):
            return False

        prev_content = prev_data.get("content") or ""
        incoming_content = incoming.get("content") or ""
        incoming_finish = incoming.get("finish_reason")

        if incoming_content:
            if len(prev_content.encode("utf-8")) + len(incoming_content.encode("utf-8")) > self._max_merged_bytes:
                return False
            prev_data["content"] = f"{prev_content}{incoming_content}"

        if incoming_finish is not None:
            prev_data["finish_reason"] = incoming_finish

        return bool(incoming_content) or (incoming_finish is not None)

    def _try_merge_tool_call_chunks(self, prev_event: Dict[str, Any], incoming: Dict[str, Any]) -> bool:
        prev_data = prev_event.get("data")
        if not isinstance(prev_data, dict):
            return False

        merge_keys = ("thread_id", "agent", "id")
        if any(prev_data.get(k) != incoming.get(k) for k in merge_keys):
            return False

        prev_chunks = prev_data.get("tool_call_chunks")
        incoming_chunks = incoming.get("tool_call_chunks")
        if not (isinstance(prev_chunks, list) and isinstance(incoming_chunks, list)):
            return False
        if len(prev_chunks) != 1 or len(incoming_chunks) != 1:
            return False

        prev_chunk = prev_chunks[0]
        incoming_chunk = incoming_chunks[0]
        if not (isinstance(prev_chunk, dict) and isinstance(incoming_chunk, dict)):
            return False

        prev_call_id = prev_chunk.get("id")
        incoming_call_id = incoming_chunk.get("id")
        if prev_call_id is not None or incoming_call_id is not None:
            if prev_call_id != incoming_call_id:
                return False
        else:
            if prev_chunk.get("index") != incoming_chunk.get("index"):
                return False

        prev_args = prev_chunk.get("args") or ""
        incoming_args = incoming_chunk.get("args") or ""
        if not isinstance(prev_args, str) or not isinstance(incoming_args, str):
            return False

        if incoming_args:
            if len(prev_args.encode("utf-8")) + len(incoming_args.encode("utf-8")) > self._max_merged_bytes:
                return False
            prev_chunk["args"] = f"{prev_args}{incoming_args}"

        return bool(incoming_args)
