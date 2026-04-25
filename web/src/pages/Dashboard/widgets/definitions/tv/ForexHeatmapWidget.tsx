import { DollarSign } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface ForexHeatmapConfig {
  currencies: string[];
}

const DEFAULT_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'CNY'];

function ForexHeatmapWidget({ instance }: WidgetRenderProps<ForexHeatmapConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="forex-cross-rates"
      config={{
        currencies: instance.config.currencies?.length ? instance.config.currencies : DEFAULT_CURRENCIES,
      }}
    />
  );
}

function ForexHeatmapSettings({ config: _config, onChange: _onChange, onClose }: WidgetSettingsProps<ForexHeatmapConfig>) {
  return (
    <div className="space-y-4">
      <div className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
        Shows cross-rates for major currencies. The currency set is fixed by TradingView in this widget.
      </div>
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<ForexHeatmapConfig>({
  type: 'tv.forex-heatmap',
  title: 'Forex Cross Rates',
  description: 'Major-currency cross-rate grid with live spreads.',
  category: 'markets',
  icon: DollarSign,
  component: ForexHeatmapWidget,
  settingsComponent: ForexHeatmapSettings,
  defaultConfig: { currencies: DEFAULT_CURRENCIES },
  defaultSize: { w: 12, h: 18 },
  minSize: { w: 6, h: 10 },
  maxSize: { w: 12, h: 32 },
  source: 'tradingview',
});
