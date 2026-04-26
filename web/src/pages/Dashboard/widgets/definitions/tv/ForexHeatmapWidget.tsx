import { useTranslation } from 'react-i18next';
import { DollarSign } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { ForexHeatmapConfigSchema, FOREX_DEFAULT_CURRENCIES } from '../../framework/configSchemas';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface ForexHeatmapConfig {
  currencies: string[];
}

// Single source of truth lives in configSchemas.ts so the schema's whole-array
// catch-fallback can't drift below the widget's expected breadth.
const DEFAULT_CURRENCIES = [...FOREX_DEFAULT_CURRENCIES];

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
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <div className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
        {t('dashboard.widgets.forexHeatmap.info')}
      </div>
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<ForexHeatmapConfig>({
  type: 'tv.forex-heatmap',
  titleKey: 'dashboard.widgets.forexHeatmap.title',
  descriptionKey: 'dashboard.widgets.forexHeatmap.description',
  category: 'markets',
  icon: DollarSign,
  component: ForexHeatmapWidget,
  settingsComponent: ForexHeatmapSettings,
  defaultConfig: { currencies: DEFAULT_CURRENCIES },
  configSchema: ForexHeatmapConfigSchema,
  defaultSize: { w: 12, h: 18 },
  minSize: { w: 6, h: 10 },
  maxSize: { w: 12, h: 32 },
  source: 'tradingview',
});
