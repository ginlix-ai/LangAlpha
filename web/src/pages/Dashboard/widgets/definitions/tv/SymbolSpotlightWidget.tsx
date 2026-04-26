import { useTranslation } from 'react-i18next';
import { Target } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { SymbolSpotlightConfigSchema } from '../../framework/configSchemas';
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
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <SymbolField
        label={t('dashboard.widgets.symbolSpotlight.symbol')}
        value={config.symbol ?? ''}
        onChange={(v) => onChange({ symbol: v })}
        placeholder="NASDAQ:NVDA"
      />
      <EnumField
        label={t('dashboard.widgets.symbolSpotlight.range')}
        value={config.range ?? '12M'}
        onChange={(v) => onChange({ range: v })}
        options={[
          { value: '1D', label: t('dashboard.widgets.symbolSpotlight.range_1d') },
          { value: '5D', label: t('dashboard.widgets.symbolSpotlight.range_5d') },
          { value: '1M', label: t('dashboard.widgets.symbolSpotlight.range_1m') },
          { value: '3M', label: t('dashboard.widgets.symbolSpotlight.range_3m') },
          { value: '12M', label: t('dashboard.widgets.symbolSpotlight.range_12m') },
          { value: '60M', label: t('dashboard.widgets.symbolSpotlight.range_5y') },
          { value: 'ALL', label: t('dashboard.widgets.symbolSpotlight.range_all') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<SymbolSpotlightConfig>({
  type: 'tv.symbol-spotlight',
  titleKey: 'dashboard.widgets.symbolSpotlight.title',
  descriptionKey: 'dashboard.widgets.symbolSpotlight.description',
  category: 'markets',
  icon: Target,
  component: SymbolSpotlightWidget,
  settingsComponent: SymbolSpotlightSettings,
  defaultConfig: { symbol: 'NASDAQ:NVDA', range: '12M' },
  configSchema: SymbolSpotlightConfigSchema,
  defaultSize: { w: 6, h: 22 },
  minSize: { w: 4, h: 14 },
  maxSize: { w: 12, h: 32 },
  source: 'tradingview',
});
