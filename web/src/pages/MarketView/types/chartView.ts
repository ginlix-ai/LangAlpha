import type { Locale } from '@/lib/locale';
import type { Timeframe } from '../utils/chartInstanceKey';

/**
 * What a chart on screen looks like, sent with a chat message so the agent's
 * snapshot can paint the same view: the instance, the theme, the language, the visible time
 * range and the size of the drawing region. Sent as the request's
 * `chart_views`, invisible to the user; the server hands it to the sandbox's
 * snapshot.
 *
 * Times are the chart's own convention (exchange wall clock, no zone):
 * `YYYY-MM-DD` on 1day, else `YYYY-MM-DDTHH:MM`. `to` may lie past the last bar
 * when the user left room on the right.
 */
export type ChartView = {
  symbol: string;
  timeframe: Timeframe;
  theme: 'dark' | 'light';
  /** The reader's language, so the snapshot's labels read as theirs do. */
  locale: Locale;
  from: string;
  to: string;
  /** CSS px of the chart's canvas region: every pane and both axes, no toolbar. */
  width: number;
  height: number;
};
