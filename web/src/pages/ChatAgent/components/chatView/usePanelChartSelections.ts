import { useCallback, useEffect, useMemo } from 'react';
import { chartSelectionStore, useChartSelections } from '@/pages/MarketView/stores/chartSelectionStore';
import { buildChartSelectionSend, type ChartSelectionSend } from '@/pages/MarketView/utils/selectionSend';

/**
 * Selections picked on a panel chart tab go out with the thread's next send,
 * as they do from MarketView's chat. The store holds one chart's selections at
 * a time (a chart drops the rest when its symbol or interval changes), so every
 * confirmed one belongs to the same chart.
 */
export function usePanelChartSelections(isActive: boolean) {
  const { selections } = useChartSelections();
  const chips = useMemo(() => selections.filter((s) => s.status === 'confirmed'), [selections]);

  // Picks made beside this thread do not follow the user to another thread or
  // page. Inactive threads stay mounted, so leaving is the view going inactive,
  // and an inactive view's unmount must not clear the active thread's picks.
  useEffect(() => {
    if (!isActive) return;
    return () => chartSelectionStore.clearAll();
  }, [isActive]);

  const takeForSend = useCallback((message: string): ChartSelectionSend | null => {
    const first = chartSelectionStore.getAll().find((s) => s.status === 'confirmed');
    if (!first) return null;
    const send = buildChartSelectionSend(first.symbol, first.timeframe, message);
    chartSelectionStore.clearAll();
    return send;
  }, []);

  return { chips, takeForSend };
}
