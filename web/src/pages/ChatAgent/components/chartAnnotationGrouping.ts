/**
 * Helpers for deciding which `chart_annotation` tool call renders the rich
 * preview card/pill and which fold into the activity timeline as ordinary rows.
 *
 * The agent typically draws a chart in several `draw_chart_annotation` calls,
 * each returning the FULL cumulative annotation set for its
 * `(workspace_id, chart_id)` instance (chart_id = `SYMBOL:timeframe`). Only the
 * LATEST draw per instance is worth a card — it is a strict superset of the
 * earlier ones. The earlier draws still render, but as normal tool-call rows so
 * the user can watch the chart get built up step by step.
 */

interface ToolCallProcessLike {
  toolCallResult?: { artifact?: Record<string, unknown> } | Record<string, unknown>;
}

interface SegmentLike {
  type: string;
  toolCallId?: string;
}

/** Stable per-chart-instance key: `workspace_id|SYMBOL:timeframe`. Uses `||` so
 *  an empty-string `chart_id`/`timeframe` falls through to the derived form
 *  rather than collapsing distinct charts onto one key. */
export function chartInstanceKey(artifact: Record<string, unknown>): string {
  const ws = (artifact.workspace_id as string) ?? '';
  const chartId =
    (artifact.chart_id as string) ||
    `${String(artifact.symbol ?? '').toUpperCase()}:${(artifact.timeframe as string) || '1day'}`;
  return `${ws}|${chartId}`;
}

function artifactOf(proc: ToolCallProcessLike | undefined): Record<string, unknown> | undefined {
  return (proc?.toolCallResult as Record<string, unknown> | undefined)?.artifact as
    | Record<string, unknown>
    | undefined;
}

export interface ChartCardPlan {
  /** First artifact-ready draw — where the single card is pinned in the
   *  transcript, so it stays put while later draws land below it. */
  anchorCallId: string;
  /** Last artifact-ready draw — whose cumulative artifact the pinned card
   *  renders, so the card grows in place to the full picture. */
  latestCallId: string;
}

/**
 * Walk the segments in order and, per chart instance, record the first
 * artifact-ready `chart_annotation` draw (the card's anchor position) and the
 * latest (the cumulative artifact to show). In-progress draws (no artifact yet)
 * are ignored, so the card reflects the most recent COMPLETED draw while a newer
 * one is still streaming.
 */
export function planChartAnnotationCards(
  segments: ReadonlyArray<SegmentLike>,
  toolCallProcesses: Record<string, ToolCallProcessLike | undefined>,
): Map<string, ChartCardPlan> {
  const plan = new Map<string, ChartCardPlan>();
  for (const seg of segments) {
    if (seg.type !== 'tool_call' || !seg.toolCallId) continue;
    const artifact = artifactOf(toolCallProcesses[seg.toolCallId]);
    if (artifact?.type !== 'chart_annotation') continue;
    const key = chartInstanceKey(artifact);
    const existing = plan.get(key);
    if (existing) {
      existing.latestCallId = seg.toolCallId;
    } else {
      plan.set(key, { anchorCallId: seg.toolCallId, latestCallId: seg.toolCallId });
    }
  }
  return plan;
}
