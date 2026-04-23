import { lazy } from 'react';
import { CandlestickChart } from 'lucide-react';
import { registerWidget } from '../framework/WidgetRegistry';
import type { WidgetRenderProps, WidgetSettingsProps } from '../types';

type ChartConfig = {
  symbol: string;
  interval: '1min' | '5min' | '15min' | '30min' | '1hour' | '1day';
  chartType: 'candle' | 'area' | 'line';
};

const DEFAULT_CONFIG: ChartConfig = { symbol: 'NVDA', interval: '1day', chartType: 'candle' };

// Heavy chart module (~200KB gzip incl. lightweight-charts) stays out of the
// dashboard route chunk. Only loads when a chart widget actually renders — the
// Suspense boundary in DashboardGrid provides the fallback.
const LazyChartWidget = lazy(() => import('./ChartWidget'));
const LazyChartSettings = lazy(() =>
  import('./ChartWidget').then((m) => ({ default: m.ChartSettings }))
);

// React.lazy returns a special component type; the registry stores
// ComponentType<WidgetRenderProps<C>>, which LazyExoticComponent satisfies at
// render time via Suspense. The cast here keeps the registry signature stable.
registerWidget<ChartConfig>({
  type: 'chart.symbol',
  title: 'Price Chart',
  description: 'Candlestick, area, or line chart for any symbol.',
  category: 'markets',
  icon: CandlestickChart,
  component: LazyChartWidget as unknown as React.ComponentType<WidgetRenderProps<ChartConfig>>,
  settingsComponent: LazyChartSettings as unknown as React.ComponentType<WidgetSettingsProps<ChartConfig>>,
  defaultConfig: DEFAULT_CONFIG,
  defaultSize: { w: 6, h: 22 },
  minSize: { w: 3, h: 15 },
});
