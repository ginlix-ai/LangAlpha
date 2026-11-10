/**
 * What the report reads off the page rather than the canvas: the legend's HTML
 * chips over the pane, with the part of their title an ellipsis cut off. Kept
 * out of the report's own module, which is geometry only and runs without a DOM.
 */

import type { IChartApi } from 'lightweight-charts';

import type { PaneChip } from './layerReport';

/**
 * The legend's chips (`data-layer-chip`) over the chart, in the price pane's
 * coordinates, each with what it shows of its title (`data-layer-title`).
 */
export function readLayerChips(chart: IChartApi): PaneChip[] {
  const root = chart.chartElement();
  const host = root.parentElement;
  if (!host) return [];
  const origin = root.getBoundingClientRect();
  let left0 = origin.left;
  try {
    left0 += chart.priceScale('left').width();
  } catch {
    /* no left scale */
  }
  const out: PaneChip[] = [];
  for (const chip of host.querySelectorAll<HTMLElement>('[data-layer-chip]')) {
    const r = chip.getBoundingClientRect();
    const titleEl = chip.querySelector<HTMLElement>('[data-layer-title]');
    out.push({
      left: r.left - left0,
      top: r.top - origin.top,
      width: r.width,
      height: r.height,
      layer: chip.dataset.layerChip || null,
      title: titleEl?.textContent ?? null,
      shown: titleEl ? shownText(titleEl) : null,
    });
  }
  return out;
}

/** What an ellipsis-truncated element shows of its text, or null when it shows it whole. */
function shownText(el: HTMLElement): string | null {
  if (el.scrollWidth <= el.clientWidth) return null;
  const text = el.textContent ?? '';
  const ctx = document.createElement('canvas').getContext('2d');
  if (!ctx) return '…';
  ctx.font = getComputedStyle(el).font;
  const room = el.clientWidth - ctx.measureText('…').width;
  let lo = 0;
  let hi = text.length;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    if (ctx.measureText(text.slice(0, mid)).width <= room) lo = mid;
    else hi = mid - 1;
  }
  return `${text.slice(0, lo).trimEnd()}…`;
}
