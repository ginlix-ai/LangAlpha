import { Target } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { SymbolField } from '../../framework/settings/SymbolField';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface SymbolSpotlightConfig {
  symbol: string;
  range: string;
}

function SymbolSpotlightWidget({ instance }: WidgetRenderProps<SymbolSpotlightConfig>) {
  const { symbol, range } = instance.config;
  return (
    <TradingViewEmbed
      card
      scriptKey="symbol-overview"
      config={{
        symbols: [[symbol, `${symbol}|${range}`]],
        chartOnly: false,
        dateRange: range,
        trendLineColor: 'rgba(41, 98, 255, 1)',
        underLineColor: 'rgba(41, 98, 255, 0.3)',
        underLineBottomColor: 'rgba(41, 98, 255, 0)',
        hideDateRanges: false,
        hideMarketStatus: false,
        hideSymbolLogo: false,
        scalePosition: 'right',
        scaleMode: 'Normal',
        fontFamily: 'ui-sans-serif, system-ui',
        chartType: 'area',
      }}
    />
  );
}

function SymbolSpotlightSettings({ config, onChange, onClose }: WidgetSettingsProps<SymbolSpotlightConfig>) {
  return (
    <div className="space-y-4">
      <SymbolField
        label="Symbol"
        value={config.symbol ?? ''}
        onChange={(v) => onChange({ symbol: v })}
        placeholder="NASDAQ:NVDA"
      />
      <EnumField
        label="Range"
        value={config.range ?? '12M'}
        onChange={(v) => onChange({ range: v })}
        options={[
          { value: '1D', label: '1 day' },
          { value: '5D', label: '5 days' },
          { value: '1M', label: '1 month' },
          { value: '3M', label: '3 months' },
          { value: '12M', label: '12 months' },
          { value: '60M', label: '5 years' },
          { value: 'ALL', label: 'All time' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<SymbolSpotlightConfig>({
  type: 'tv.symbol-spotlight',
  title: 'Symbol Spotlight',
  description: 'Price + chart overview with key stats for a single symbol.',
  category: 'markets',
  icon: Target,
  component: SymbolSpotlightWidget,
  settingsComponent: SymbolSpotlightSettings,
  defaultConfig: { symbol: 'NASDAQ:NVDA', range: '12M' },
  defaultSize: { w: 6, h: 22 },
  minSize: { w: 4, h: 14 },
  maxSize: { w: 12, h: 32 },
  source: 'tradingview',
});
