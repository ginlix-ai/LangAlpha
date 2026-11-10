import { formatCalendarDay, formatCalendarMonthDay } from '@/lib/format';
import type { ChartLayer } from '../types/chartLayer';

const AS_OF = /^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}:\d{2}))?/;

/**
 * A layer's `as_of` for a reader: "Oct 28, 2026", plus the clock for an
 * intraday one ("Oct 28, 2026, 15:30"). Both are exchange-local wall-clock
 * strings with no zone, so they are read as written, never shifted into the
 * reader's offset.
 */
export function formatLayerAsOf(asOf: string | null | undefined, locale: string): string {
  if (!asOf) return '';
  const m = AS_OF.exec(asOf);
  if (!m) return asOf;
  const day = formatCalendarDay(m[1], locale);
  return m[2] ? `${day}, ${m[2]}` : day;
}

/** "Oct 28", plus the clock for an intraday layer: a chip has no room for the year. */
export function formatLayerAsOfShort(asOf: string | null | undefined, locale: string): string {
  if (!asOf) return '';
  const m = AS_OF.exec(asOf);
  if (!m) return asOf;
  const day = formatCalendarMonthDay(m[1], locale);
  return m[2] ? `${day} ${m[2]}` : day;
}

/** The colour the layer reads as on the chart: its first coloured element. */
export function layerAccent(layer: ChartLayer): string | null {
  const pools: Array<ReadonlyArray<{ color?: string | null }>> = [
    layer.series, layer.levels, layer.drawings, layer.markers, layer.regimes, layer.fills,
  ];
  for (const pool of pools) {
    for (const el of pool) if (el.color) return el.color;
  }
  return null;
}
