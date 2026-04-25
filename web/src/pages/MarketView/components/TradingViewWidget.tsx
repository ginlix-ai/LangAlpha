import { memo } from 'react';
import { useTheme } from '@/contexts/ThemeContext';
import { TradingViewEmbed } from '@/pages/Dashboard/widgets/framework/TradingViewEmbed';

// Map our interval keys to TradingView widget interval values
const TV_INTERVALS: Record<string, string> = {
  '1min': '1',
  '5min': '5',
  '15min': '15',
  '30min': '30',
  '1hour': '60',
  '4hour': '240',
  '1day': 'D',
};

interface TradingViewWidgetProps {
  symbol: string;
  interval?: string;
}

/**
 * TradingView Advanced Chart widget embed.
 *
 * Thin wrapper around the shared dashboard `TradingViewEmbed` so the full
 * app uses a single TV-embed abstraction (loader cache, error/retry UX,
 * theme sync, attribution placement). The Advanced Chart accepts many more
 * config keys than the dashboard embeds; we spread the app-specific ones
 * here so the host component stays uniform.
 *
 * Note on background/grid: the shared TV_COMMON_CONFIG sets transparent
 * defaults so dashboard widgets render cleanly inside `.dashboard-glass-card`.
 * MarketView's chart has no card behind it, and Advanced Chart's up-candles
 * use semi-transparent fills — with a transparent background they composite
 * against the page and render pastel/washed-out. We override with a solid
 * theme-matched background + gridColor here to restore the saturated look
 * the chart had on main.
 */
function TradingViewWidget({ symbol, interval = '1day' }: TradingViewWidgetProps) {
  const { theme } = useTheme();
  const isLight = theme === 'light';
  const config = {
    symbol,
    interval: TV_INTERVALS[interval] || 'D',
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'America/New_York',
    style: '1',
    isTransparent: false,
    backgroundColor: isLight ? '#FFFCF9' : '#000000',
    gridColor: isLight ? '#E8E2DB' : '#1A1A1A',
    allow_symbol_change: false,
    hide_side_toolbar: false,
    hide_top_toolbar: false,
    withdateranges: true,
    details: false,
    calendar: false,
    studies: ['RSI@tv-basicstudies'],
  };

  return (
    <TradingViewEmbed
      scriptKey="advanced-chart"
      config={config}
      className="h-full w-full"
    />
  );
}

export default memo(TradingViewWidget);
