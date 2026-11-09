"""Turns the server posts itself: a report-back, and the wake a market target's
notice or a wakeup starts.

Nobody typed them, and their own message is model-facing, so everything that
draws a turn reads two things from here: the query type that marks one, and
the keys that name what it announces. Those keys are the request fields the
server sets, the query row's metadata that labels the turn in history, and
the stream's first frame that labels it for a live attach.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# A report-back posts as ``system``; a wake as its hook's type.
NOTIFICATION_QUERY_TYPES = frozenset({"system", "market_target", "wakeup"})

# ``bot_message_source`` names a bot-to-bot message the server posted, so a
# thread can draw its sender; like the others it never reaches the model.

# The one wake turn that may end without a reply.
WAKE_QUERY_TYPE = "wakeup"

ANNOUNCED_KEYS = ("report_source", "market_target", "wakeup", "bot_message_source")


def announced(metadata: Mapping[str, Any]) -> dict[str, Any]:
    """What a turn's query metadata says it announces, in key order."""
    return {key: metadata[key] for key in ANNOUNCED_KEYS if metadata.get(key)}
