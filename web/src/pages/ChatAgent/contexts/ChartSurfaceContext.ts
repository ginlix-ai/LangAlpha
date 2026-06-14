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
 */
export interface ChartSurface {
  chartPresent: boolean;
}

export const ChartSurfaceContext = createContext<ChartSurface>({ chartPresent: false });

export function useChartSurface(): ChartSurface {
  return useContext(ChartSurfaceContext);
}
