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
from typing import Any, Literal, Optional, get_args

# A report-back posts as ``system``; a wake as its hook's type.
NOTIFICATION_QUERY_TYPES = frozenset({"system", "market_target", "wakeup"})

# The one wake turn that may end without a reply.
WAKE_QUERY_TYPE = "wakeup"

ANNOUNCED_KEYS = ("report_source", "market_target", "wakeup")


def announced(metadata: Mapping[str, Any]) -> dict[str, Any]:
    """What a turn's query metadata says it announces, in key order."""
    return {key: metadata[key] for key in ANNOUNCED_KEYS if metadata.get(key)}


#: Where a run came from, as the user is told of it: their own turn, a
#: report-back, a wake of some kind, or an automation.
RunOrigin = Literal["turn", "report", "wakeup", "market_target", "automation"]

#: The origins nobody started by hand, which announce themselves at start.
BACKGROUND_ORIGINS = frozenset({"wakeup", "market_target", "automation"})

#: Run metadata key START stamps with the run's origin (``run_origin``).
RUN_ORIGIN_META = "run_origin"


def classify_origin(run_meta: Mapping[str, Any], query_type: Optional[str]) -> RunOrigin:
    """Where a run came from, as START knows it: by its metadata and the
    query type of the query row it writes. A retry or a replay writes none,
    so it is the user's own turn however its turn began.

    The report-back pair alone marks a report only on a run that asks nothing
    of its own: a turn past an interrupted summary carries the summary's pair
    (``carried_pair``) though the user sent it."""
    if run_meta.get("automation_id"):
        return "automation"
    if query_type == "system" or (
        query_type is None and run_meta.get("report_back_ptc_thread_id")
    ):
        return "report"
    if query_type in ("wakeup", "market_target"):
        return query_type
    return "turn"


def run_origin(run_meta: Mapping[str, Any], query_type: Optional[str]) -> RunOrigin:
    """Where a run came from, read back after START: the stamp START left
    (``RUN_ORIGIN_META``), since a ``/retry`` runs on its turn's old query row
    and a wake's retry would otherwise read as a wake though the user started
    it. A row from before the stamp is classified as START would have."""
    stamped = run_meta.get(RUN_ORIGIN_META)
    if stamped in get_args(RunOrigin):
        return stamped
    return classify_origin(run_meta, query_type)
