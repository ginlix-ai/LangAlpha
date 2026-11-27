import type { ChartView } from '@/pages/MarketView/types/chartView';

const MARKET_WATCH_SKILL = {
  type: 'skills',
  name: 'market-watch',
  instruction: 'Market watch mode is on for this message. If the central tickers are not yet registered, register them with watch_market.',
};

/**
 * What every send from the chat carries for the page around it, the
 * composer's and a widget's alike: the market-watch skill while the watch is
 * on (re-sent every turn, as MarketView's sends carry the chart-annotation
 * skill; not twice beside a typed /market-watch pill in `present`), and what
 * each chart in the panel shows, so the agent's snapshot paints the same view.
 */
export function hostSendParts(
  watching: boolean,
  present: readonly Record<string, unknown>[],
  chartViews: ChartView[],
): { contexts: Record<string, unknown>[]; options: { chartViews?: ChartView[] } } {
  const watch = watching && !present.some((c) => c.type === 'skills' && c.name === 'market-watch');
  return {
    contexts: watch ? [{ ...MARKET_WATCH_SKILL }] : [],
    options: chartViews.length > 0 ? { chartViews } : {},
  };
}
