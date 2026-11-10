"""The chart views a turn's request carries, for its sandbox jobs.

A message sent from a surface showing a chart names the view (symbol,
interval, theme, visible range, size) in the request's ``chart_views``, so a
chart snapshot the agent takes in that turn looks like what the user sees.
They are never rendered for the model: they ride the turn's graph config (``command_handoff.JOB_ENV_KEY``), which every tool call
of the turn, a subagent's included, inherits, to every Bash and ExecuteCode
job as ``PTC_CHART_VIEWS``. chartkit's ``snapshot()`` reads it and bounds the
sizes itself, since it must check the variable anyway.
"""

import json
from collections.abc import Sequence
from typing import Optional

from chartkit.protocol import CHART_VIEWS_ENV
from src.server.models.chart_layer import make_chart_id
from src.server.models.chart_view import ChartView

#: One per chart (SYMBOL:timeframe), the latest ones kept.
MAX_CHART_VIEWS = 8


def chart_views_env(views: Optional[Sequence[ChartView]]) -> dict[str, str]:
    """The environment the request's chart views add to its turn's jobs:
    the latest view of each chart in the order last seen, at most
    ``MAX_CHART_VIEWS``, as a compact JSON list; nothing when it has none."""
    latest: dict[str, dict] = {}
    for view in views or ():
        key = make_chart_id(view.symbol, view.timeframe)
        latest.pop(key, None)
        latest[key] = view.model_dump(exclude_none=True)
    if not latest:
        return {}
    kept = list(latest.values())[-MAX_CHART_VIEWS:]
    return {CHART_VIEWS_ENV: json.dumps(kept, separators=(",", ":"))}
