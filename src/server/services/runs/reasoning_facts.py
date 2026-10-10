"""How long the main agent thought, kept for replay.

The checkpoint keeps a model call's reasoning but not its duration. A call
streams under a chunk id the checkpoint never sees (``lc_run--…``), so its
closes wait until the node's update names the message it committed, and a
call that commits nothing (an attempt a retry replaced) is dropped with them.
"""

from typing import Optional

from langchain_core.messages import AIMessage


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
