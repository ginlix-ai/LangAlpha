/**
 * Helpers for deciding which `chart_annotation` tool call renders the chart
 * card/chip and which fold into the activity timeline as ordinary rows.
 *
 * The agent typically draws a chart in several `draw_chart_annotation` calls,
 * each returning the FULL cumulative annotation set for its
 * `(workspace_id, chart_id)` instance (chart_id = `SYMBOL:timeframe`). Only the
 * freshest of those snapshots is worth a card. The earlier draws still render,
 * but as normal tool-call rows so the user can watch the chart get built up
 * step by step.
 */

interface ToolCallProcessLike {
  toolCallResult?: { artifact?: Record<string, unknown> } | Record<string, unknown> | null;
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

function snapshotHas(artifact: Record<string, unknown> | undefined, annotationId: unknown): boolean {
  if (typeof annotationId !== 'string' || !Array.isArray(artifact?.annotations)) return false;
  return (artifact.annotations as Array<{ annotation_id?: unknown }>).some(
    (a) => a?.annotation_id === annotationId,
  );
}

/** Whether `candidate` was read no earlier than `current`. The read stamp
 *  decides when both carry one. A draw whose read-back failed carries none and
 *  holds only itself, so it never displaces a real read. Artifacts persisted
 *  before the stamp fall back to membership, which is exact unless a removal
 *  ran between the two reads. */
function isFresher(candidate: Record<string, unknown>, current: Record<string, unknown> | undefined): boolean {
  const a = typeof candidate.read_at_us === 'number' ? candidate.read_at_us : null;
  const b = typeof current?.read_at_us === 'number' ? current.read_at_us : null;
  if (a !== null && b !== null) return a >= b;
  if (a !== null || b !== null) return a !== null;
  return !snapshotHas(current, candidate.annotation_id);
}

export interface ChartCardPlan {
  /** First artifact-ready draw — where the single card is pinned in the
   *  transcript, so it stays put while later draws land below it. */
  anchorCallId: string;
  /** Draw with the freshest cumulative artifact, which the pinned card
   *  renders so it grows in place to the full picture. Often not the last
   *  call: parallel draws finish in any order. */
  freshestCallId: string;
}

/**
 * Walk the segments in order and, per chart instance, record the first
 * artifact-ready `chart_annotation` draw (the card's anchor position) and the
 * freshest (the cumulative artifact to show). In-progress draws (no artifact
 * yet) are ignored, so the card reflects the freshest COMPLETED draw while a
 * newer one is still streaming.
 *
 * Segment order is call order, not finish order: draws issued in parallel
 * commit in any order, so the last call can hold the oldest snapshot. Each
 * artifact stamps when its set was read (`read_at_us`), and the latest read
 * wins.
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
      if (isFresher(artifact, artifactOf(toolCallProcesses[existing.freshestCallId]))) {
        existing.freshestCallId = seg.toolCallId;
      }
    } else {
      plan.set(key, { anchorCallId: seg.toolCallId, freshestCallId: seg.toolCallId });
    }
  }
  return plan;
}
