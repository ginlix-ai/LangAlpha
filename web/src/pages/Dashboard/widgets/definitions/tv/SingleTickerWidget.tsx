import { Tag } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { SymbolField } from '../../framework/settings/SymbolField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface SingleTickerConfig {
  symbol: string;
}

function SingleTickerWidget({ instance }: WidgetRenderProps<SingleTickerConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="single-quote"
      config={{ symbol: instance.config.symbol }}
    />
  );
}

function SingleTickerSettings({ config, onChange, onClose }: WidgetSettingsProps<SingleTickerConfig>) {
  return (
    <div className="space-y-4">
      <SymbolField
        label="Symbol"
        value={config.symbol ?? ''}
        onChange={(v) => onChange({ symbol: v })}
        placeholder="NASDAQ:NVDA"
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<SingleTickerConfig>({
  type: 'tv.single-ticker',
  title: 'Single Ticker',
  description: 'Compact live-price card for one symbol.',
  category: 'markets',
  icon: Tag,
  component: SingleTickerWidget,
  settingsComponent: SingleTickerSettings,
  defaultConfig: { symbol: 'NASDAQ:NVDA' },
  defaultSize: { w: 3, h: 4 },
  minSize: { w: 2, h: 3 },
  maxSize: { w: 6, h: 6 },
  source: 'tradingview',
});
