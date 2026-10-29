/**
 * What an activity block draws: the slot above its rows, where the summary
 * goes, and the rows under it. A block lays itself out one of two ways, and
 * the two never mix: a row per step (the default), or the status line (opt-in,
 * `tool_call_layout: 'status'`), one row naming the step in flight under a
 * count of the finished ones.
 */
import { type ActivityItem, type PreparingCall, isRunning } from './activityTypes';
import { groupFileToolRuns } from './groupFileToolRuns';

export type Step = { kind: 'call'; call: PreparingCall } | { kind: 'item'; item: ActivityItem };

/** The step the status line names, under its own row key. `running` until it
 *  lands, after which the line keeps it until a newer step takes its place. */
export interface LineStep {
  key: string;
  step: Step;
  running: boolean;
}

/** The tool calls a row or the slot draws, by id: the one whose card lands
 *  takes over its place (LiveRow, CardArrivals). */
type Draws = { calls: readonly string[] };

export type Slot = Draws & (
  | { kind: 'summary'; counted: ActivityItem[] }
  /** The step the block opened on, run where the summary will be, and the
   *  status line while the line names it. */
  | { kind: 'lead'; key: string; step: Step; isLine: boolean }
);

export type Row = { key: string; live: boolean } & Draws & (
  | { kind: 'line'; line: LineStep }
  /** One item, or a run of calls of one file verb drawn as a row. */
  | { kind: 'run'; run: ActivityItem[] }
  | { kind: 'call'; call: PreparingCall }
);

export interface ActivityLayout {
  slot: Slot | null;
  rows: Row[];
}

interface RowKeys {
  forItem: (item: ActivityItem) => string;
  forCall: (call: PreparingCall) => string;
}

interface LayoutInput {
  timeline: ActivityItem[];
  calls: readonly PreparingCall[];
  keys: RowKeys;
  expanded: boolean;
}

/** A thought streaming open (verbose), which keeps its own row, since its
 *  text is what it shows. */
export function isOpenThought(item: ActivityItem, liveReasoningOpen: boolean): boolean {
  return item.type === 'reasoning' && isRunning(item) && liveReasoningOpen;
}

const NO_CALLS: readonly string[] = [];

const callsOf = (items: readonly ActivityItem[]): readonly string[] =>
  items.flatMap((item) => (item.type === 'tool_call' ? [item.id] : []));

const runRow = (keys: RowKeys, run: ActivityItem[]): Row => ({
  kind: 'run', key: keys.forItem(run[0]), run, calls: callsOf(run),
  live: run.some((entry) => entry._liveState !== 'completed'),
});

/** A call still being written has no card to hand over to. */
const callRow = (keys: RowKeys, call: PreparingCall): Row => ({ kind: 'call', key: keys.forCall(call), call, calls: NO_CALLS, live: true });

/** A settled block of nothing but thoughts shows them rather than a count of
 *  them. */
function onlyThoughts(timeline: ActivityItem[], counted: ActivityItem[], calls: readonly PreparingCall[]): boolean {
  return counted.length > 0 && counted.length === timeline.length && calls.length === 0
    && counted.every((item) => item.type === 'reasoning');
}

/** A row per step: the live ones always, the finished ones once the summary
 *  is opened. Inline is the compatibility layout for a transcript without a
 *  turn fold, and has no summary to show its thoughts behind. */
export function layoutRows({ timeline, calls, keys, expanded, inline }: LayoutInput & { inline: boolean }): ActivityLayout {
  const counted = timeline.filter((item) => item._liveState === 'completed');
  const bare = !inline && onlyThoughts(timeline, counted, calls);
  const rows: Row[] = [];
  for (const run of groupFileToolRuns(timeline)) {
    const row = runRow(keys, run);
    if (row.live || expanded || bare) rows.push(row);
  }
  // A row per call, so a second call streaming beside the first is its own row
  // from its first chunk, and each one carries on into its result.
  for (const call of calls) rows.push(callRow(keys, call));
  return { slot: counted.length > 0 && !bare ? { kind: 'summary', counted, calls: NO_CALLS } : null, rows };
}

/**
 * The status line, under the count of every step as soon as it finishes.
 * Folded, nothing but an open thought is a row of its own; opened, it is the
 * row layout's full timeline under the count. The line is `up` while a step
 * is in flight or the block holds it for its running turn, which keeps the
 * step that last finished on it until the next takes its place.
 */
export function layoutStatusLine({ timeline, calls, keys, expanded, liveReasoningOpen, line, up }: LayoutInput & {
  liveReasoningOpen: boolean;
  line: LineStep | null;
  up: boolean;
}): ActivityLayout {
  const onLine = !expanded && up ? line : null;
  const counted = timeline.filter((item) => !isRunning(item));
  const bare = !onLine && onlyThoughts(timeline, counted, calls);
  // The step a block opens on runs in the summary's place until a step after
  // it is counted, so nothing is ever held empty above the line: a band held
  // for the count left a gap over the line until a step landed, and one held
  // over a thought closed when the reply began, and the page dropped by it.
  // A step after it turns it into the count there, the line coming in below.
  // An open (verbose) thought is a row of its own, and stays one.
  const first: Step | null = timeline.length > 0 ? { kind: 'item', item: timeline[0] }
    : calls.length > 0 ? { kind: 'call', call: calls[0] }
    : null;
  const firstItem = first?.kind === 'item' ? first.item : null;
  const firstKey = !first ? null : first.kind === 'item' ? keys.forItem(first.item) : keys.forCall(first.call);
  const firstOnLine = onLine !== null && onLine.key === firstKey;
  const lead = first && !expanded && !(liveReasoningOpen && firstItem?.type === 'reasoning')
    && counted.every((item) => item === firstItem)
    && (firstOnLine || !firstItem || isRunning(firstItem) || bare)
    ? first : null;

  const rows: Row[] = [];
  // The line stands for the block's work in flight, so it gives its place to
  // whichever of the block's calls lands and takes it away.
  if (onLine && !(lead && firstOnLine)) {
    rows.push({ kind: 'line', key: 'status-line', line: onLine, calls: callsOf(timeline), live: onLine.running });
  }
  for (const run of groupFileToolRuns(timeline)) {
    if (lead && run[0] === firstItem) continue;
    if (expanded || bare || isOpenThought(run[0], liveReasoningOpen)) rows.push(runRow(keys, run));
  }
  if (expanded) for (const call of calls) rows.push(callRow(keys, call));

  const slot: Slot | null = lead && firstKey
    ? { kind: 'lead', key: firstKey, step: lead, isLine: firstOnLine, calls: lead.kind === 'item' ? callsOf([lead.item]) : NO_CALLS }
    : counted.length > 0 && (onLine || !bare) ? { kind: 'summary', counted, calls: NO_CALLS }
    : null;
  return { slot, rows };
}
