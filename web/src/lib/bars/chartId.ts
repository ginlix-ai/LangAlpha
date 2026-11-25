/**
 * A chart's identity. Its id, `SYMBOL:timeframe` (`ChartId`), is what the
 * backend and the agent key its layers and its comments by; on the client an
 * instance also names its workspace (`ChartKey`). Each is made only here: the
 * ticker in its display spelling (`600519.SH`, never `.SS`) and the interval
 * normalized to a timeframe the agent draws on, so every reader of the same
 * chart lands on the same value whatever spelling it started from.
 */

import { displaySpelling } from './exchanges';

// Timeframes the agent can draw on (mirrors the backend `Timeframe` enum).
// A chart `interval` outside this set (e.g. a legacy stored value) simply
// never matches an agent-drawn chart instance.
export const TIMEFRAMES = ['1min', '5min', '15min', '30min', '1hour', '4hour', '1day'] as const;
export type Timeframe = (typeof TIMEFRAMES)[number];
const VALID_TIMEFRAMES: ReadonlySet<string> = new Set(TIMEFRAMES);

export const DEFAULT_TIMEFRAME: Timeframe = '1day';

const isTimeframe = (v: string): v is Timeframe => VALID_TIMEFRAMES.has(v);

/** Coerce a chart interval to a timeframe the agent understands. */
export function normalizeTimeframe(interval: string | null | undefined): Timeframe {
  const tf = (interval ?? '').trim();
  return isTimeframe(tf) ? tf : DEFAULT_TIMEFRAME;
}

/** A chart's id, `SYMBOL:timeframe`. Made only by `chartIdFor`. */
export type ChartId = string & { readonly __chartId: unique symbol };

/** The id of the chart showing `symbol` at `interval`; null until the symbol is known. */
export function chartIdFor(symbol: string | null | undefined, interval: string | null | undefined): ChartId | null {
  const sym = symbol ? displaySpelling(symbol) : '';
  return sym ? `${sym}:${normalizeTimeframe(interval)}` as ChartId : null;
}

/** A chart's id as its symbol and timeframe; null for one that is not `SYMBOL:timeframe`. */
export function chartParts(chartId: string): { symbol: string; timeframe: Timeframe } | null {
  const at = chartId.lastIndexOf(':');
  if (at <= 0) return null;
  const timeframe = chartId.slice(at + 1);
  return isTimeframe(timeframe) ? { symbol: chartId.slice(0, at), timeframe } : null;
}

/** One chart instance on the client. Made only by `chartKeyFor`. */
export type ChartKey = string & { readonly __chartKey: unique symbol };

const SEP = '||';

/**
 * The key of the chart showing `symbol` at `interval` in a workspace. Null
 * until all three are known.
 */
export function chartKeyFor(
  workspaceId: string | null | undefined,
  symbol: string | null | undefined,
  interval: string | null | undefined,
): ChartKey | null {
  if (!workspaceId || !interval) return null;
  const chartId = chartIdFor(symbol, interval);
  return chartId ? `${workspaceId}${SEP}${chartId}` as ChartKey : null;
}

export interface ChartKeyParts {
  workspaceId: string;
  /** Display spelling. */
  symbol: string;
  timeframe: Timeframe;
}

/** What a key names, read back from it. */
export function chartKeyParts(key: ChartKey): ChartKeyParts {
  const sep = key.indexOf(SEP);
  // A key holds a chart id `chartIdFor` made, so it always reads back.
  const { symbol, timeframe } = chartParts(key.slice(sep + SEP.length))!;
  return { workspaceId: key.slice(0, sep), symbol, timeframe };
}
