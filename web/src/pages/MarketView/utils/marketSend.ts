/**
 * What every MarketView send carries about the chart on screen, from the
 * desktop chat panel and the phone's composer alike: the chart-annotation
 * skill, for its drawing guide and to say which ticker and timeframe "the
 * chart" is (drawing on another timeframe would not show on this one), and
 * what the chart shows (`chart_views`).
 */
import type { ChartView } from '../types/chartView';
import { normalizeTimeframe } from '@/lib/bars/chartId';

export interface MarketSendContexts {
  /** The chart-annotation skill, first in the send's `additional_context`. */
  skill: Record<string, unknown>;
  /** What the charts beside the composer show (`chart_views`). */
  chartViews: ChartView[];
}

/**
 * The skill's instruction: the chart the user is looking at, so the agent
 * draws on it (SYMBOL:timeframe) instead of guessing. `sym` is uppercased,
 * `tf` a normalized timeframe.
 */
export function marketViewAnnotationContext(sym: string, tf: string): string {
  return `The user is viewing the ${sym} chart on the ${tf} timeframe in MarketView. "The chart" is ${sym}:${tf}, built on bars("${sym}", "${tf}"), unless they name another ticker or interval.`;
}

export function buildMarketSendContexts(symbol: string, interval: string, chartViews: ChartView[]): MarketSendContexts {
  const sym = symbol.toUpperCase();
  const tf = normalizeTimeframe(interval);
  return {
    skill: {
      type: 'skills',
      name: 'chart-annotation',
      instruction: sym ? marketViewAnnotationContext(sym, tf) : undefined,
    },
    chartViews,
  };
}
