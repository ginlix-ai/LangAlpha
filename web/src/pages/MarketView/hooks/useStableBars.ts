/**
 * The bars' times and candle extremes, held by identity until they change. A
 * price tick rewrites the forming bar in a new `chartData` array, but a tick
 * that leaves the forming bar's high and low alone is not worth a new
 * identity, which would reconcile the whole chart on every tick.
 *
 * The extremes are held apart from the times. When any bar's high or low moves
 * (a new high on a live tick, a reload correcting an older candle), `highs` and
 * `lows` are new so a forecast's verdict reads the candles the chart shows,
 * while `times` keeps its identity and the grid and the plan built on it are
 * not recomputed: the layers only repaint.
 *
 * Every bar is compared, not just the ends and the forming bar: a reload can
 * replace an interior candle and leave the count and both ends as they were.
 * The compare reads the held arrays in place, so a tick that changes nothing
 * allocates nothing.
 *
 * What is held is state, not a memo's dependency list: a memo that reads
 * `chartData` but keys on something else takes the whole hook out of the React
 * Compiler, and the compiler would otherwise key it on `chartData` itself.
 */

import { useState } from 'react';

export interface BarLike {
  time: number;
  high?: number;
  low?: number;
}

export interface StableBars {
  times: readonly number[];
  /** Candle highs and lows by bar index, NaN where a bar has none. */
  highs: Float64Array;
  lows: Float64Array;
}

function extremesOf(bars: readonly BarLike[]): Pick<StableBars, 'highs' | 'lows'> {
  const highs = new Float64Array(bars.length);
  const lows = new Float64Array(bars.length);
  bars.forEach((b, k) => {
    highs[k] = b.high ?? NaN;
    lows[k] = b.low ?? NaN;
  });
  return { highs, lows };
}

const same = (a: number, b: number): boolean => Object.is(a, b);

function sameTimes(held: StableBars, bars: readonly BarLike[]): boolean {
  if (held.times.length !== bars.length) return false;
  for (let k = 0; k < bars.length; k++) if (held.times[k] !== bars[k].time) return false;
  return true;
}

function sameExtremes(held: StableBars, bars: readonly BarLike[]): boolean {
  // Newest first: a live tick moves the forming bar, the last one.
  for (let k = bars.length - 1; k >= 0; k--) {
    if (!same(held.highs[k], bars[k].high ?? NaN) || !same(held.lows[k], bars[k].low ?? NaN)) return false;
  }
  return true;
}

function held(bars: readonly BarLike[]): StableBars {
  return { times: bars.map((b) => b.time), ...extremesOf(bars) };
}

const NO_BARS: readonly BarLike[] = [];

export function useStableBars(chartData: readonly BarLike[] | null): StableBars {
  const bars = chartData ?? NO_BARS;
  const [current, setCurrent] = useState(() => held(bars));
  if (sameTimes(current, bars)) {
    if (sameExtremes(current, bars)) return current;
    const next: StableBars = { times: current.times, ...extremesOf(bars) };
    setCurrent(next);
    return next;
  }
  const next = held(bars);
  setCurrent(next);
  return next;
}
