import { Gauge } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { SymbolField } from '../../framework/settings/SymbolField';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface TechnicalsConfig {
  symbol: string;
  interval: string;
}

function TechnicalsWidget({ instance }: WidgetRenderProps<TechnicalsConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="technical-analysis"
      config={{
        symbol: instance.config.symbol,
        interval: instance.config.interval,
        showIntervalTabs: true,
        displayMode: 'single',
      }}
    />
  );
}

function TechnicalsSettings({ config, onChange, onClose }: WidgetSettingsProps<TechnicalsConfig>) {
  return (
    <div className="space-y-4">
      <SymbolField
        label="Symbol"
        value={config.symbol ?? ''}
        onChange={(v) => onChange({ symbol: v })}
        placeholder="NASDAQ:NVDA"
      />
      <EnumField
        label="Interval"
        value={config.interval ?? '1D'}
        onChange={(v) => onChange({ interval: v })}
        options={[
          { value: '1m', label: '1 minute' },
          { value: '5m', label: '5 minutes' },
          { value: '15m', label: '15 minutes' },
          { value: '1h', label: '1 hour' },
          { value: '4h', label: '4 hours' },
          { value: '1D', label: '1 day' },
          { value: '1W', label: '1 week' },
          { value: '1M', label: '1 month' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<TechnicalsConfig>({
  type: 'tv.technicals',
  title: 'Technical Analysis',
  description: 'Oscillator + moving-average consensus gauge with buy/sell/neutral verdict.',
  category: 'markets',
  icon: Gauge,
  component: TechnicalsWidget,
  settingsComponent: TechnicalsSettings,
  defaultConfig: { symbol: 'NASDAQ:NVDA', interval: '1D' },
  defaultSize: { w: 6, h: 22 },
  // Min raised to 22 (= default) so the gauge + pills + labels always
  // render fully. At h<22 the edit-mode body (~cell-64px for chrome) drops
  // below TV's natural ~450px render height and the gauge clips at the
  // bottom. Users can still enlarge up to h=32.
  minSize: { w: 4, h: 22 },
  maxSize: { w: 12, h: 32 },
  source: 'tradingview',
});
