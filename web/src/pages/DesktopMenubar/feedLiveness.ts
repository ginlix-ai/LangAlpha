/**
 * How live the panel's figures are, as one word for the footer. Each row is
 * judged by the app's freshness rules (`lib/freshness`, the same words Market
 * View and the chat cards use), plus two things only this panel knows: the
 * socket streaming its stocks, and a refresh that stopped landing.
 *
 * A fault anywhere names the whole panel, since one stale row among live ones
 * is still a wrong number on screen. Otherwise the liveliest state does, and
 * the tooltip lists the rows that are less live than that: an index's last
 * close beside streaming stocks before the open is the product, not a fault.
 */
import { FRESHNESS_TONE, resolveFreshnessBadge, type FreshnessFields, type FreshnessSpec } from '@/lib/freshness';

/** The dot, in Market View's vocabulary: a pulse only for a streaming tick. */
export type LivenessDot = 'live' | 'realtime' | 'delayed' | 'closed' | 'warning';

export interface LivenessRow {
  key: string;
  isIndex: boolean;
  fields: FreshnessFields | null;
  /** When the cache last took a figure for it: a REST read or a tick. */
  updatedAt: number;
  /** A read for it is in flight now. */
  fetching: boolean;
  /** The socket is live for it: connected, with a recent tick for it. */
  streaming: boolean;
}

/** What a row reads as; `spec` is null for the panel's own two states. */
export interface RowLiveness {
  kind: 'streaming' | 'overdue' | 'freshness';
  spec: FreshnessSpec | null;
  dot: LivenessDot;
}

export interface Liveness {
  dot: LivenessDot;
  /** The state the badge names, which the tooltip groups the rows under too. */
  row: RowLiveness;
}

/**
 * A figure this old with no read in flight has stopped refreshing: past two of
 * the slowest poll (a streamed stock's REST read, every minute, which a quiet
 * name before the open may get no tick between).
 */
export const OVERDUE_MS = 150_000;

const DOT_FOR_STATE: Record<FreshnessSpec['state'], LivenessDot> = {
  live: 'realtime',
  delayed: 'delayed',
  eod: 'closed',
  stale: 'warning',
  incomplete: 'warning',
};

/** Liveliest first; a fault is not ranked, it wins outright. */
const RANK: Record<LivenessDot, number> = { live: 0, realtime: 1, delayed: 2, closed: 3, warning: -1 };

export function rowLiveness(row: LivenessRow, now: number): RowLiveness | null {
  if (!row.fetching && now - row.updatedAt > OVERDUE_MS) return { kind: 'overdue', spec: null, dot: 'warning' };
  if (row.streaming) return { kind: 'streaming', spec: null, dot: 'live' };
  const spec = row.fields ? resolveFreshnessBadge(row.fields) : null;
  if (!spec) return null;
  return { kind: 'freshness', spec, dot: FRESHNESS_TONE[spec.state] === 'warning' ? 'warning' : DOT_FOR_STATE[spec.state] };
}

/** The panel's one state, or null when no row on screen says anything about itself. */
export function panelLiveness(rows: readonly LivenessRow[], now: number): Liveness | null {
  let best: RowLiveness | null = null;
  for (const row of rows) {
    const state = rowLiveness(row, now);
    if (!state) continue;
    if (state.dot === 'warning') {
      // A refresh that stopped says more than a stale label the last read carried.
      if (state.kind === 'overdue' || best?.dot !== 'warning') best = state;
      continue;
    }
    if (best?.dot === 'warning') continue;
    if (!best || RANK[state.dot] < RANK[best.dot]) best = state;
  }
  return best && { dot: best.dot, row: best };
}

/** One tooltip group per distinct state: rows reading the same are listed together. */
export function stateKey(state: RowLiveness): string {
  return state.spec ? `${state.spec.key}|${state.spec.minutes ?? ''}` : state.kind;
}
