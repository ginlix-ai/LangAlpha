import type { ThreadContextPart, ThreadContextResponse } from '@/pages/ChatAgent/utils/api/threads';

/** The parts in the order the context is assembled. A record rather than a
 *  list so that a part the API adds fails to compile until it has a place. */
const AHEAD_ORDER: Record<ThreadContextPart, null> = {
  system: null, tools: null, mcp: null, skills: null, memory: null, environment: null,
};
const AHEAD_PARTS = Object.keys(AHEAD_ORDER) as ThreadContextPart[];

export type ContextPart = ThreadContextPart | 'conversation';

/** Every row the panel can show, the conversation last. */
export const CONTEXT_PARTS: readonly ContextPart[] = [...AHEAD_PARTS, 'conversation'];

export interface ContextRow {
  key: ContextPart;
  tokens: number;
}

/** `part` as a fraction of `whole`, clamped to the bar. */
export const share = (part: number, whole: number) => (whole > 0 ? Math.min(Math.max(part / whole, 0), 1) : 0);

/**
 * The panel's rows from the parts counted on request and the live window.
 *
 * The parts were counted at one moment and `total` keeps moving with the
 * stream, so the conversation is whatever of `total` the parts do not hold,
 * and its row and the bar follow the turn without another count. The parts
 * cannot hold more than the newest call read (`lastInput`): a count taken
 * after a tool was added, say, can exceed it, and is scaled down to fit
 * rather than squeezing the conversation to nothing. With no call read yet
 * the window itself is the cap.
 */
export function contextRows(
  parts: NonNullable<ThreadContextResponse['parts']>,
  { total, lastInput }: { total: number; lastInput: number },
): ContextRow[] {
  const counted = AHEAD_PARTS.map((key) => ({ key, tokens: parts[key] ?? 0 }));
  const sum = counted.reduce((acc, { tokens }) => acc + tokens, 0);
  const cap = Math.max(lastInput > 0 ? lastInput : total, 0);
  const scale = sum > cap ? cap / sum : 1;
  const ahead: ContextRow[] = counted.map(({ key, tokens }) => ({
    key,
    tokens: scale === 1 ? tokens : Math.floor(tokens * scale),
  }));
  const held = ahead.reduce((acc, { tokens }) => acc + tokens, 0);
  const rows: ContextRow[] = [...ahead, { key: 'conversation', tokens: Math.max(total - held, 0) }];
  return rows.filter(({ tokens }) => tokens > 0);
}

/** Each row's share of the threshold, cut off where the bar ends. */
export function barSegments(rows: ContextRow[], threshold: number): { key: ContextPart; width: number }[] {
  const segments: { key: ContextPart; width: number }[] = [];
  let before = 0;
  for (const { key, tokens } of rows) {
    const width = share(before + tokens, threshold) - share(before, threshold);
    if (width > 0) segments.push({ key, width });
    before += tokens;
  }
  return segments;
}
