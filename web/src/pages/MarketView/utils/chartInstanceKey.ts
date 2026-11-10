/**
 * A chart instance's identity: `(workspace_id, chart_id)` with
 * `chart_id = "{SYMBOL}:{timeframe}"`, the identity the backend and the agent
 * use. On the client it is one value, `ChartKey`, which only `chartKeyFor`
 * makes: the ticker in its display spelling (`600519.SH`, never `.SS`) and the
 * interval normalized to a timeframe the agent draws on, so every reader of
 * the same chart lands on the same key whatever spelling it started from.
 */

import { displaySpelling } from '@/lib/bars/exchanges';

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
  if (!workspaceId || !symbol || !interval) return null;
  const sym = displaySpelling(symbol);
  if (!sym) return null;
  return `${workspaceId}${SEP}${sym}:${normalizeTimeframe(interval)}` as ChartKey;
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
  const rest = key.slice(sep + SEP.length);
  const colon = rest.lastIndexOf(':');
  return {
    workspaceId: key.slice(0, sep),
    symbol: rest.slice(0, colon),
    timeframe: rest.slice(colon + 1) as Timeframe,
  };
}
