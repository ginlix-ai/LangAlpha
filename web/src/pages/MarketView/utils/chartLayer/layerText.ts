/**
 * The words the layer renderer writes itself (a forecast's verdict, a
 * position's readouts, a replay's source), in the reader's language. Painters
 * cannot call hooks, so `useChartLayers` builds one from `t` and the locale and
 * hands it to the primitive (`setText`); anything that paints without one (a
 * test, a primitive not yet given one) gets the English default. The agent's
 * own text (labels, titles) is never translated.
 */

import type { TFunction } from 'i18next';

import { formatCalendarDayRange, formatWallClockRange, parseCalendarDay } from '@/lib/format';

import type { LayerTime } from '../../types/chartLayer';
import { layerTimeToSeconds } from './timeGrid';

export interface LayerText {
  /** A forecast whose target a bar reached. */
  hit: string;
  /** A forecast whose time passed without it. */
  missed: string;
  /** A replay's name when the agent gave none. */
  replay: string;
  /** "Replayed: Mar 31 – May 12, 2026", the tag on a replay's source stretch. */
  replayed(from: LayerTime, to: LayerTime): string;
  long: string;
  short: string;
  /** "Target", before the farthest target's price. */
  target: string;
  /** "T1", a nearer target. */
  targetN(n: number): string;
  stop: string;
  /** "R:R", before the position's best reward-to-risk. */
  riskReward: string;
  /** "12 bars", a measure's span. */
  bars(count: number): string;
  /** "16d", a measure's span in calendar days. */
  days(count: number): string;
  /** "POC", before a volume profile's point of control. */
  poc: string;
}

/** Two layer times as a short date range: days alone, or with the time of day for an intraday time. */
function layerTimeRange(from: LayerTime, to: LayerTime, locale: string): string {
  if (parseCalendarDay(from) && parseCalendarDay(to)) return formatCalendarDayRange(from, to, locale);
  const a = layerTimeToSeconds(from);
  const b = layerTimeToSeconds(to);
  return a != null && b != null ? formatWallClockRange(a, b, locale) : `${from} – ${to}`;
}

export const DEFAULT_LAYER_TEXT: LayerText = {
  hit: 'Hit',
  missed: 'Missed',
  replay: 'Replay',
  replayed: (from, to) => `Replayed: ${layerTimeRange(from, to, 'en-US')}`,
  long: 'Long',
  short: 'Short',
  target: 'Target',
  targetN: (n) => `T${n}`,
  stop: 'Stop',
  riskReward: 'R:R',
  bars: (count) => `${count} ${count === 1 ? 'bar' : 'bars'}`,
  days: (count) => `${count}d`,
  poc: 'POC',
};

export function buildLayerText(t: TFunction, locale: string): LayerText {
  return {
    hit: t('marketView.layerCanvas.hit'),
    missed: t('marketView.layerCanvas.missed'),
    replay: t('marketView.layerCanvas.replay'),
    replayed: (from, to) => t('marketView.layerCanvas.replayed', { dates: layerTimeRange(from, to, locale) }),
    long: t('marketView.layerCanvas.long'),
    short: t('marketView.layerCanvas.short'),
    target: t('marketView.layerCanvas.target'),
    targetN: (n) => t('marketView.layerCanvas.targetN', { n }),
    stop: t('marketView.layerCanvas.stop'),
    riskReward: t('marketView.layerCanvas.riskReward'),
    bars: (count) => t('marketView.layerCanvas.bars', { count }),
    days: (count) => t('marketView.layerCanvas.days', { count }),
    poc: t('marketView.layerCanvas.poc'),
  };
}
