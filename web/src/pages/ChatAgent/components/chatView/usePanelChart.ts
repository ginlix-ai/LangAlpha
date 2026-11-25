import { useEffect, useState, type RefObject } from 'react';
import type { ChatInputHandle } from '@/components/ui/chat-input';
import { createChartComposer } from '@/components/chart/chartComposer';
import type { ChartView } from '@/pages/MarketView/types/chartView';
import { layerAskContext } from '@/pages/MarketView/utils/layerAsk';

/**
 * What the charts in a thread's panel hand its composer: the views they
 * show, and "ask about this layer", which lands as a context pill.
 * `composer` is provided around the view, so a panel chart reaches this
 * thread's composer and no other, inactive threads staying mounted beside
 * the active one. Comments on a chart go as every comment does, through the
 * thread's queue.
 */
export function usePanelChart(chatInputRef: RefObject<ChatInputHandle | null>) {
  const [composer] = useState(createChartComposer);
  useEffect(
    () => composer.setAskHandler((req) => chatInputRef.current?.addContext(layerAskContext(req))),
    [composer, chatInputRef],
  );

  /** What each chart beside the composer shows, so the agent's snapshot matches it. */
  const viewsForSend = (): ChartView[] => composer.collectViews();

  return { composer, viewsForSend };
}
