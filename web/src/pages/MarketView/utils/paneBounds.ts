import type { IChartApi } from 'lightweight-charts';

type PaneSource = Pick<IChartApi, 'panes'>;

/**
 * Height of the price pane (pane 0) in container pixels. The RSI pane sits
 * beneath it inside the same container, so a y measured against the
 * container is only a price coordinate while it stays above this line; past
 * it, `coordinateToPrice` extrapolates a price nothing on screen shows.
 * Returns 0 when the height is unknown (chart disposed, no pane laid out
 * yet). The host height is not a stand-in: it spans the RSI pane too, so a
 * caller that used it would place a price on the wrong pane.
 */
export function pricePaneHeight(chart: PaneSource | null | undefined): number {
  try {
    const h = chart?.panes()[0]?.getHeight();
    return h != null && Number.isFinite(h) && h > 0 ? h : 0;
  } catch {
    return 0;
  }
}

/**
 * The element whose box follows the price pane's height. A drag on the pane
 * separator moves it without changing the container, so an overlay that
 * anchors to the pane has to watch this rather than its host. The pane's
 * canvas is the observable: the pane row is a table row, which a
 * ResizeObserver does not report reliably. `null` before the chart has laid
 * the pane out, or once it is disposed.
 */
export function pricePaneElement(chart: PaneSource | null | undefined): HTMLElement | null {
  try {
    const row = chart?.panes()[0]?.getHTMLElement() ?? null;
    return row?.querySelector('canvas') ?? row;
  } catch {
    return null;
  }
}

/**
 * Clamp a container-relative y onto the price pane. Callers gate on a known
 * height first (`pricePaneHeight` > 0); with none there is no pane to clamp
 * onto, and the y collapses to its top edge rather than extrapolating.
 */
export function clampToPricePane(y: number, paneHeight: number): number {
  return Math.max(0, Math.min(y, paneHeight));
}

/**
 * Whether a container-relative y lands on the price pane rather than beneath
 * it. An unknown height is nowhere: the y could be on the RSI pane.
 */
export function isOnPricePane(y: number, paneHeight: number): boolean {
  if (paneHeight <= 0) return false;
  return y >= 0 && y <= paneHeight;
}
