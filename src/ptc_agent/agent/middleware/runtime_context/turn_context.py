"""What a turn knows about itself, carried whole from the request to the stack.

These values are read when the turn opens, mostly off its request, and are used
at the turn boundary: the anchor row, and the zones its clock is stamped in and
its tools read. Threading them one by one made every builder between the
handler and the middleware restate a list it has no other interest in, and
adding one meant editing every signature between them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TurnContext:
    """Per-turn runtime context for the turn anchor row. Every field is optional.

    A context-free build (thread maintenance, a subagent stack, a test) passes
    no context at all, which is the same thing as passing one whose fields are
    all None: the row states the market unconditionally and drops the gap line.
    """

    last_turn_at: datetime | None = None
    platform: str | None = None
    origin: str | None = None
    surface_rules: str | None = None
    # A turn nobody sent (the harness reporting finished background work) runs
    # under the last rules stated, the way a resumed interrupt does, rather
    # than taking back the rules of the conversation it reports into.
    inherits_rules: bool = False
    # Set only once the shared disk is low enough to change what the agent
    # should do; None states nothing. A subagent's stack never carries it.
    disk_free_mb: int | None = None
    # Whether a current reading backs disk_free_mb being None; only then may a
    # turn take back an earlier low-disk line.
    disk_known: bool = False
    # The zone the profile or the request names. The stamp, the identity block
    # and a subagent's row state it, so "9am tomorrow" means the same thing to
    # the model and a tool. None when neither names one: the stamp then keeps
    # the frozen identity's zone, never a locale default.
    timezone: str | None = None
    # The zone the turn's tools read a local time in, and the run records:
    # ``timezone``, else the request locale's default, since a tool needs a
    # clock even when nothing named one.
    tool_timezone: str = "UTC"
