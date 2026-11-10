/**
 * The chart's time grid: its bars plus the bars it would have next, and the
 * mapping between layer times, chart seconds and grid indices. Every surface
 * that turns a time into a place on the chart (the layer painter, the framing
 * rule, the chart views a message carries (`chart_views`), the snapshot's time range) goes
 * through one grid, so a time in the empty room past the last bar lands on the
 * same slot everywhere.
 *
 * Coordinates live in "grid space": `i` is a bar index on the chart's own bar
 * sequence (fractional between bars, past the last bar for the future, negative
 * before the first), `p` a price. The renderer maps `i` to x through the time
 * scale's logical index, which keeps going past the data, and `p` to y through
 * the series. Grid space is affine to pixel space on a linear price scale, so
 * parallels (channels, pitchfork tines) stay parallel on screen.
 */

import { INTERVAL_SECONDS } from '@/lib/bars/chartConstants';

import type { LayerPoint, LayerTime } from '../../types/chartLayer';

export const DAY = 86400;

/**
 * Layer time → chart seconds. Layer times are exchange wall clock with no zone,
 * which the chart encodes as if it were UTC, so a zone-less string is read as
 * UTC. A string that does carry a zone is taken as written.
 */
export function layerTimeToSeconds(t: LayerTime | null | undefined): number | null {
  if (typeof t !== 'string' || !t) return null;
  const hasZone = t.includes('T') && /([zZ]|[+-]\d{2}:?\d{2})$/.test(t);
  const iso = hasZone ? t : t.length <= 10 ? `${t}T00:00:00Z` : `${t}Z`;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : null;
}

/** Chart seconds → the layer time convention, for anchors handed back to the agent. */
export function secondsToLayerTime(secs: number, daily: boolean): LayerTime {
  const iso = new Date(secs * 1000).toISOString();
  return daily ? iso.slice(0, 10) : iso.slice(0, 16);
}

/**
 * The chart's bars plus the bars it would have next. `future` repeats the
 * recent sessions' bar times forward (the times of day bars were seen at, on
 * weekdays unless the symbol trades weekends), so a future anchor lands where
 * that bar will be rather than at a naive multiple of the interval, which would
 * march through nights and weekends.
 */
export interface TimeGrid {
  bars: readonly number[];
  future: readonly number[];
  /** `bars` then `future`: grid index `k` is `slots[k]`, which every lookup below searches. */
  slots: readonly number[];
  /** Interval in seconds; extrapolation step before the first bar, and past the slots with no session. */
  step: number;
  /**
   * The session `future` repeats, which goes on past it: `future` is capped,
   * and a time beyond it still lands on the session's slots, not on a count
   * of elapsed intervals that would take in nights and weekends.
   */
  session?: Session | null;
}

/** A day's bar times (seconds past midnight, ascending) and whether the symbol trades weekends. */
export interface Session {
  offsets: readonly number[];
  weekends: boolean;
}

export function makeTimeGrid(
  bars: readonly number[],
  future: readonly number[],
  step: number,
  session: Session | null = null,
): TimeGrid {
  return { bars, future, slots: future.length ? [...bars, ...future] : bars, step, session };
}

/** Days a walk along the session goes before giving up (about a century). */
const SESSION_WALK_DAYS = 40_000;

const trades = (s: Session, day: number) => s.weekends || !isWeekend(day);

/**
 * Where `secs` falls among the session's slots after `from` (itself a slot):
 * how many lie in `(from, secs]`, the last of them (`from` if none) and the
 * first one past `secs`. Whole days are counted, not walked.
 */
function sessionCount(s: Session, from: number, secs: number): { count: number; prev: number; next: number } | null {
  const offs = s.offsets;
  const per = offs.length;
  if (!per) return null;
  let count = 0;
  let prev = from;
  let day = Math.floor(from / DAY) * DAY;
  for (let g = 0; g < SESSION_WALK_DAYS; g++, day += DAY) {
    if (!trades(s, day)) continue;
    if (day + offs[0] > from && day + offs[per - 1] <= secs) {
      count += per;
      prev = day + offs[per - 1];
      continue;
    }
    for (const off of offs) {
      const t = day + off;
      if (t <= from) continue;
      if (t > secs) return { count, prev, next: t };
      count += 1;
      prev = t;
    }
  }
  return null;
}

/** The `j`th session slot after `from` (`j` ≥ 1). Whole days are counted, not walked. */
function sessionNth(s: Session, from: number, j: number): number | null {
  const offs = s.offsets;
  const per = offs.length;
  if (!per || j < 1) return null;
  let left = j;
  let day = Math.floor(from / DAY) * DAY;
  for (let g = 0; g < SESSION_WALK_DAYS; g++, day += DAY) {
    if (!trades(s, day)) continue;
    if (day + offs[0] > from && left > per) {
      left -= per;
      continue;
    }
    for (const off of offs) {
      const t = day + off;
      if (t > from && --left === 0) return t;
    }
  }
  return null;
}

/** A point in grid space: bar index and price. */
export interface IP {
  i: number;
  p: number;
}

/** As far past the bars as a layer's series may run (chartkit's MAX_SERIES_POINTS). */
const MAX_FUTURE_SLOTS = 20_000;
/** Bars of empty room kept past the furthest future anchor, so its label is not on the edge. */
const FUTURE_PAD_SLOTS = 3;

function isWeekend(secs: number): boolean {
  const d = new Date(secs * 1000).getUTCDay();
  return d === 0 || d === 6;
}

/**
 * Build the grid. `until` is the furthest time any layer reaches; slots are
 * generated through it plus `FUTURE_PAD_SLOTS`, and at least `minSlots` of
 * them, capped.
 */
export function buildTimeGrid(
  barTimes: readonly number[],
  stepSecs: number,
  until: number | null,
  minSlots = 0,
): TimeGrid {
  const step = stepSecs > 0 ? stepSecs : DAY;
  const n = barTimes.length;
  const reach = n > 0 && until != null && until > barTimes[n - 1];
  if (n === 0 || (!reach && minSlots <= 0)) {
    return makeTimeGrid(barTimes, [], step);
  }
  const last = barTimes[n - 1];

  // Session template from the last ~10 calendar days of bars.
  const windowStart = last - 10 * DAY;
  const offsets = new Set<number>();
  let weekendTrading = false;
  for (let k = n - 1; k >= 0 && barTimes[k] >= windowStart; k--) {
    const t = barTimes[k];
    offsets.add(((t % DAY) + DAY) % DAY);
    if (isWeekend(t)) weekendTrading = true;
  }
  // Too little history to read a session from: fall back to a plain step.
  const daily = step >= DAY;
  const sorted = daily ? [((last % DAY) + DAY) % DAY] : [...offsets].sort((a, b) => a - b);

  const future: number[] = [];
  // Without an `until` to pad past, the pad is already spent.
  let padLeft = reach ? FUTURE_PAD_SLOTS : -1;
  const done = () => (padLeft < 0 && future.length >= minSlots) || future.length >= MAX_FUTURE_SLOTS;
  let day = Math.floor(last / DAY) * DAY;
  // A day with no slot past `last` advances to the next; bounded by the cap.
  for (let guard = 0; guard < MAX_FUTURE_SLOTS * 2 && future.length < MAX_FUTURE_SLOTS; guard++) {
    if (weekendTrading || !isWeekend(day)) {
      for (const off of sorted) {
        const t = day + off;
        if (t <= last) continue;
        future.push(t);
        if (reach && t >= until!) padLeft -= 1;
        if (done()) break;
      }
      if (done()) break;
    }
    day += DAY;
  }
  return makeTimeGrid(barTimes, future, step, { offsets: sorted, weekends: weekendTrading });
}

/**
 * The grid for a chart of `interval` holding `barTimes`: future slots through
 * `until` (plus the pad), and at least `minSlots` of them.
 */
export function gridFor(
  barTimes: readonly number[],
  interval: string,
  opts: { until?: number | null; minSlots?: number } = {},
): TimeGrid {
  return buildTimeGrid(barTimes, INTERVAL_SECONDS[interval] ?? DAY, opts.until ?? null, opts.minSlots ?? 0);
}

/** Index of the first element >= target (lower bound). */
function lowerBound(arr: readonly number[], target: number): number {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] < target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/**
 * Chart seconds → grid index. Exact on a slot (a bar or a future slot),
 * interpolated between slots (a weekend date sits between Friday and Monday),
 * extrapolated by the interval before the first bar and past the last slot.
 * Null with no bars.
 */
export function resolveIndex(grid: TimeGrid, secs: number): number | null {
  const { slots, step } = grid;
  const n = slots.length;
  if (grid.bars.length === 0 || !Number.isFinite(secs)) return null;
  if (secs < slots[0]) return (secs - slots[0]) / step;
  const k = lowerBound(slots, secs);
  if (k === n) {
    const at = grid.session && sessionCount(grid.session, slots[n - 1], secs);
    if (at) return n - 1 + at.count + (secs - at.prev) / (at.next - at.prev);
    return n - 1 + (secs - slots[n - 1]) / step;
  }
  if (slots[k] === secs) return k;
  return k - 1 + (secs - slots[k - 1]) / (slots[k] - slots[k - 1]);
}

/**
 * Grid index → chart seconds, the inverse of `resolveIndex`. `slot` reads the
 * index's nearest slot (a render report names whole bars); `interpolate` keeps
 * a fractional index between its neighbours (a view edge mid-bar). Both
 * extrapolate by the interval before the first bar and past the last slot.
 */
export function indexToSeconds(grid: TimeGrid, index: number, mode: 'slot' | 'interpolate'): number | null {
  const { slots, step } = grid;
  const n = slots.length;
  if (grid.bars.length === 0 || !Number.isFinite(index)) return null;
  // The `j`th slot past the last one, on the session when the grid has one.
  const past = (j: number): number =>
    (j > 0 && grid.session ? sessionNth(grid.session, slots[n - 1], j) : null) ?? slots[n - 1] + j * step;
  if (mode === 'slot') {
    const k = Math.round(index);
    if (k < 0) return slots[0] + k * step;
    return k < n ? slots[k] : past(k - n + 1);
  }
  if (index < 0) return slots[0] + index * step;
  if (index >= n - 1) {
    const j = Math.floor(index - (n - 1));
    const a = past(j);
    return a + (index - (n - 1) - j) * (past(j + 1) - a);
  }
  const k = Math.floor(index);
  return slots[k] + (index - k) * (slots[k + 1] - slots[k]);
}

/** Layer time → grid index. */
export function timeToIndex(grid: TimeGrid, t: LayerTime | null | undefined): number | null {
  const secs = layerTimeToSeconds(t);
  return secs == null ? null : resolveIndex(grid, secs);
}

/** A layer point → grid point; null when its time or price is unusable. */
export function pointToIP(grid: TimeGrid, pt: LayerPoint | null | undefined): IP | null {
  if (!pt || !Number.isFinite(pt.price)) return null;
  const i = timeToIndex(grid, pt.time);
  return i == null ? null : { i, p: pt.price };
}

/**
 * Snap a series point's time onto the grid (a bar or a future slot), for
 * lightweight-charts series data. A series time off the grid would add a time
 * point to the shared time scale and shift every bar's logical index, so points
 * snap to the nearest slot and points before the first bar or past the last
 * slot are dropped.
 */
export function snapToGrid(grid: TimeGrid, secs: number): number | null {
  const { bars, slots, step } = grid;
  const n = slots.length;
  if (bars.length === 0 || !Number.isFinite(secs)) return null;
  if (secs < slots[0] - step / 2 || secs > slots[n - 1] + step / 2) return null;
  const k = lowerBound(slots, secs);
  if (k === 0) return slots[0];
  if (k === n) return slots[n - 1];
  return secs - slots[k - 1] <= slots[k] - secs ? slots[k - 1] : slots[k];
}
