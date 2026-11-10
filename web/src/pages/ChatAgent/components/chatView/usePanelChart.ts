import { useEffect, useMemo, useState, type RefObject } from 'react';
import type { ChatInputHandle } from '@/components/ui/chat-input';
import { createChartComposer } from '@/components/chart/chartComposer';
import { chartSelectionStore, useChartSelections } from '@/pages/MarketView/stores/chartSelectionStore';
import type { ChartView } from '@/pages/MarketView/types/chartView';
import { layerAskContext } from '@/pages/MarketView/utils/layerAsk';
import { buildChartSelectionSend, type ChartSelectionSend } from '@/pages/MarketView/utils/selectionSend';

export interface PanelChartSend {
  /** Regions and price levels picked on a panel chart, or null for none. */
  picked: ChartSelectionSend | null;
  /** What each chart beside the composer shows, so the agent's snapshot matches it. */
  chartViews: ChartView[];
}

/**
 * What the charts in a thread's panel hand its composer: the selections
 * picked on them, the views they show, and "ask about this layer", which
 * lands as a context pill. `composer` is provided around the view, so a
 * panel chart reaches this thread's composer and no other, inactive threads
 * staying mounted beside the active one.
 *
 * The selection store holds one chart's selections at a time (a chart drops
 * the rest when its symbol or interval changes), so every confirmed one
 * belongs to the same chart.
 */
export function usePanelChart(isActive: boolean, chatInputRef: RefObject<ChatInputHandle | null>) {
  const [composer] = useState(createChartComposer);
  useEffect(
    () => composer.setAskHandler((req) => chatInputRef.current?.addContext(layerAskContext(req))),
    [composer, chatInputRef],
  );

  const { selections } = useChartSelections();
  const chips = useMemo(() => selections.filter((s) => s.status === 'confirmed'), [selections]);

  // Picks made beside this thread do not follow the user to another thread or
  // page. Inactive threads stay mounted, so leaving is the view going inactive,
  // and an inactive view's unmount must not clear the active thread's picks.
  useEffect(() => {
    if (!isActive) return;
    return () => chartSelectionStore.clearAll();
  }, [isActive]);

  const takeForSend = (message: string): PanelChartSend => {
    const first = chartSelectionStore.getAll().find((s) => s.status === 'confirmed');
    const picked = first ? buildChartSelectionSend(first.symbol, first.timeframe, message) : null;
    if (picked) chartSelectionStore.clearAll();
    return { picked, chartViews: composer.collectViews() };
  };

  return { composer, chips, takeForSend };
}
