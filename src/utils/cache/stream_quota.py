"""How much one event stream may hold before its run is finalized.

Live streams carry no TTL until terminal, so nothing evicts them while the
run lives, and the writer bounds them itself: by events, so a MAXLEN trim
never removes a replay's head, and by bytes, since the event count bounds no
size (one tool result can be megabytes, and joined deltas make every frame
larger). Both lanes, the run stream writer and the subagent spill, charge one
of these per stream and turn a breach into their own error vocabulary, as
``stream_append`` leaves its errors to them.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

logger = logging.getLogger(__name__)

QuotaBreach = Literal["events", "bytes"]

_WARN_AT = 0.9
# Past the warning line the event warning repeats once per this many events.
_EVENT_WARN_EVERY = 1000


@dataclass
class StreamQuota:
    """One stream's event and byte budget, and the bytes charged so far."""

    max_events: int
    max_bytes: int
    bytes: int = 0

    @property
    def maxlen(self) -> int:
        """The stream's MAXLEN, a backstop only: the quota finalizes the run
        long before a FIFO trim could touch the head."""
        return self.max_events * 2

    def charge(self, event_id: int, nbytes: int, *, label: str) -> QuotaBreach | None:
        """Count one event before it is written, returning the budget it
        would break, if any; a writer never writes a frame that breaks one.

        ``event_id`` is the stream's sequential frame id. It counts every
        frame the stream was given, including any never written, so gating
        on it can only fire early.
        """
        before = self.bytes
        self.bytes += nbytes
        if event_id > self.max_events:
            return "events"
        if self.bytes > self.max_bytes:
            return "bytes"

        event_line = int(self.max_events * _WARN_AT)
        if event_id >= event_line and (event_id - event_line) % _EVENT_WARN_EVERY == 0:
            logger.warning(
                "Stream %s near its event quota: %d/%d events. "
                "At quota the run finalizes error(transport_lost).",
                label, event_id, self.max_events,
            )
        byte_line = int(self.max_bytes * _WARN_AT)
        if before < byte_line <= self.bytes:
            logger.warning(
                "Stream %s near its byte quota: %d/%d bytes. "
                "At quota the run finalizes error(transport_lost).",
                label, self.bytes, self.max_bytes,
            )
        return None
