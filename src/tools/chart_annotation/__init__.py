"""Chart annotation tools — skill-gated tools for drawing on MarketView charts.

Tools:
- draw_chart_annotation: draw a price line, trendline, or marker on the chart
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
