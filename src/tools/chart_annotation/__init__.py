"""Chart annotation tools — skill-gated tools for drawing on MarketView charts.

Tools:
- draw_chart_annotation: draw an annotation (price line, trendline, marker,
  vertical line, rectangle, text, event, or fib retracement) on the chart
- manage_chart_annotations: list, remove, or clear annotations for a symbol
"""

from src.tools.chart_annotation.tools import (
    draw_chart_annotation,
    manage_chart_annotations,
)

CHART_ANNOTATION_TOOLS = [
    draw_chart_annotation,
    manage_chart_annotations,
]

__all__ = [
    "draw_chart_annotation",
    "manage_chart_annotations",
    "CHART_ANNOTATION_TOOLS",
]
