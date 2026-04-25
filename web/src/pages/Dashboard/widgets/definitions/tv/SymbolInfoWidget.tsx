import { Info } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { SymbolField } from '../../framework/settings/SymbolField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface SymbolInfoConfig {
  symbol: string;
}

function SymbolInfoWidget({ instance }: WidgetRenderProps<SymbolInfoConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="symbol-info"
      config={{ symbol: instance.config.symbol }}
    />
  );
}

function SymbolInfoSettings({ config, onChange, onClose }: WidgetSettingsProps<SymbolInfoConfig>) {
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

registerWidget<SymbolInfoConfig>({
  type: 'tv.symbol-info',
  title: 'Symbol Info',
  description: 'Compact summary card — price, day range, market cap, volume.',
  category: 'markets',
  icon: Info,
  component: SymbolInfoWidget,
  settingsComponent: SymbolInfoSettings,
  defaultConfig: { symbol: 'NASDAQ:NVDA' },
  defaultSize: { w: 6, h: 8 },
  minSize: { w: 4, h: 6 },
  maxSize: { w: 12, h: 12 },
  source: 'tradingview',
});
