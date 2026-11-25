/**
 * A chart's marks as the comment UI names them: a region by its dates, a
 * point by its price and date, a shape by its layer and its own name.
 */
import type { TFunction } from 'i18next';
import i18n from '@/i18n';
import type { ChartElementMark, ChartMark, ChartTime } from '@/types/artifacts';
import { markPrice } from '../viewers/comments/chart/chartMarks';

/** The layer form's parts, read as the wall clock it is (the chart encodes it as UTC). */
const LAYER_TIME = /^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2}):(\d{2}))?/;

/**
 * A layer time in the reader's language: "Oct 3", "Oct 3, 2024" off this
 * year, and its time of day on an intraday chart.
 */
export function chartTimeLabel(time: ChartTime, locale: string = i18n.language): string {
  const m = LAYER_TIME.exec(time);
  if (!m) return time;
  const [, y, mo, d, h, mi] = m;
  const at = new Date(Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h ?? 0), Number(mi ?? 0)));
  const thisYear = Number(y) === new Date().getUTCFullYear();
  try {
    return new Intl.DateTimeFormat(locale, {
      timeZone: 'UTC',
      month: 'short',
      day: 'numeric',
      ...(thisYear ? {} : { year: 'numeric' }),
      ...(h === undefined ? {} : { hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }),
    }).format(at);
  } catch {
    return time;
  }
}

/** A price as a mark keeps it, no trailing zeros. */
export function chartPriceLabel(price: number): string {
  return Number.isFinite(price) ? String(markPrice(price)) : '';
}

/** A shape by its own name: its label or title, else its id, else what kind of shape it is. */
export function elementName(mark: ChartElementMark): string {
  return mark.element_name || mark.element_id || mark.element_type || mark.element_kind;
}

/** One mark by itself, as a chip and a single mark's heading name it. */
export function markLabel(t: TFunction, mark: ChartMark): string {
  switch (mark.type) {
    case 'region': {
      const from = chartTimeLabel(mark.start);
      const to = chartTimeLabel(mark.end);
      return from === to ? from : t('pageViewer:comments.chartRegion', { from, to });
    }
    case 'point':
      return t('pageViewer:comments.chartPoint', { price: chartPriceLabel(mark.price), time: chartTimeLabel(mark.time) });
    case 'element':
      return elementName(mark);
    default:
      return mark satisfies never;
  }
}

/** One mark as a comment's heading names it: a shape with the layer it is in. */
export function markHeading(t: TFunction, mark: ChartMark): string {
  return mark.type === 'element'
    ? t('pageViewer:comments.chartElement', { layer: mark.layer_title, name: elementName(mark) })
    : markLabel(t, mark);
}
