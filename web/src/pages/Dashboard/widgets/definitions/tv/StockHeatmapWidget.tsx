import { Map } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface StockHeatmapConfig {
  dataSource: string;
  blockSize: string;
  blockColor: string;
}

function StockHeatmapWidget({ instance }: WidgetRenderProps<StockHeatmapConfig>) {
  const { dataSource, blockSize, blockColor } = instance.config;
  return (
    <TradingViewEmbed
      card
      scriptKey="stock-heatmap"
      config={{
        dataSource,
        blockSize,
        blockColor,
        grouping: 'sector',
        hasTopBar: false,
        isDataSetEnabled: false,
        isZoomEnabled: true,
        hasSymbolTooltip: true,
      }}
    />
  );
}

function StockHeatmapSettings({ config, onChange, onClose }: WidgetSettingsProps<StockHeatmapConfig>) {
  return (
    <div className="space-y-4">
      <EnumField
        label="Market"
        value={config.dataSource ?? 'SPX500'}
        onChange={(v) => onChange({ dataSource: v })}
        options={[
          { value: 'SPX500', label: 'S&P 500' },
          { value: 'NASDAQ100', label: 'NASDAQ 100' },
          { value: 'DJI', label: 'Dow Jones 30' },
          { value: 'FTSE100', label: 'FTSE 100' },
          { value: 'HSI', label: 'Hang Seng' },
        ]}
      />
      <EnumField
        label="Cell size"
        value={config.blockSize ?? 'market_cap_basic'}
        onChange={(v) => onChange({ blockSize: v })}
        options={[
          { value: 'market_cap_basic', label: 'Market cap' },
          { value: 'volume', label: 'Volume' },
          { value: 'number_of_employees', label: 'Employees' },
        ]}
      />
      <EnumField
        label="Cell color"
        value={config.blockColor ?? 'change'}
        onChange={(v) => onChange({ blockColor: v })}
        options={[
          { value: 'change', label: '% change (day)' },
          { value: 'Perf.W', label: '% change (week)' },
          { value: 'Perf.1M', label: '% change (month)' },
          { value: 'Volatility.W', label: 'Volatility' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<StockHeatmapConfig>({
  type: 'tv.stock-heatmap',
  title: 'Stock Heatmap',
  description: 'Sector-grouped treemap sized by market cap, colored by % change.',
  category: 'markets',
  icon: Map,
  component: StockHeatmapWidget,
  settingsComponent: StockHeatmapSettings,
  defaultConfig: { dataSource: 'SPX500', blockSize: 'market_cap_basic', blockColor: 'change' },
  defaultSize: { w: 12, h: 22 },
  minSize: { w: 6, h: 12 },
  maxSize: { w: 12, h: 40 },
  source: 'tradingview',
});
