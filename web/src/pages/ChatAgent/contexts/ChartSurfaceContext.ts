import { createContext, useContext } from 'react';

/**
 * Signals whether the chat transcript is rendered next to a live chart.
 *
 * The chat engine (`useChatMessages`) and its `MessageList` are shared by the
 * standalone ChatAgent page AND the MarketView desktop chat panel. When the
 * agent draws a chart annotation, the inline preview card behaves differently
 * by surface:
 * - ChatAgent (`chartPresent: false`, the default): render a live mini chart
 *   the user can click to expand into MarketView.
 * - MarketView (`chartPresent: true`): the real chart already shows the
 *   drawing live, so the card collapses to a one-line confirmation chip.
 *
 * MarketView also supplies the instance currently on screen (`activeSymbol` /
 * `activeTimeframe`) and an `onJumpToChart` callback, so a chip describing a
 * different ticker/timeframe than what's displayed can switch the live chart
 * to it on click.
 */
export interface ChartSurface {
  chartPresent: boolean;
  /** Symbol currently shown on the adjacent live chart (MarketView only). */
  activeSymbol?: string;
  /**
   * Raw interval currently shown on the adjacent live chart (NOT normalized) —
   * so a view-only interval like `1s` doesn't read as "active" for a `1day`
   * annotation.
   */
  activeTimeframe?: string;
  /** Switch the adjacent live chart to a symbol+timeframe (MarketView only). */
  onJumpToChart?: (symbol: string, timeframe: string) => void;
}

export const ChartSurfaceContext = createContext<ChartSurface>({ chartPresent: false });

export function useChartSurface(): ChartSurface {
  return useContext(ChartSurfaceContext);
}
