import { useTranslation } from 'react-i18next';
import { Gauge } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { TechnicalsConfigSchema } from '../../framework/configSchemas';
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
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <SymbolField
        label={t('dashboard.widgets.technicals.symbol')}
        value={config.symbol ?? ''}
        onChange={(v) => onChange({ symbol: v })}
        placeholder="NASDAQ:NVDA"
      />
      <EnumField
        label={t('dashboard.widgets.technicals.interval')}
        value={config.interval ?? '1D'}
        onChange={(v) => onChange({ interval: v })}
        options={[
          { value: '1m', label: t('dashboard.widgets.technicals.interval_1m') },
          { value: '5m', label: t('dashboard.widgets.technicals.interval_5m') },
          { value: '15m', label: t('dashboard.widgets.technicals.interval_15m') },
          { value: '1h', label: t('dashboard.widgets.technicals.interval_1h') },
          { value: '4h', label: t('dashboard.widgets.technicals.interval_4h') },
          { value: '1D', label: t('dashboard.widgets.technicals.interval_1d') },
          { value: '1W', label: t('dashboard.widgets.technicals.interval_1w') },
          { value: '1M', label: t('dashboard.widgets.technicals.interval_1M') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<TechnicalsConfig>({
  type: 'tv.technicals',
  titleKey: 'dashboard.widgets.technicals.title',
  descriptionKey: 'dashboard.widgets.technicals.description',
  category: 'markets',
  icon: Gauge,
  component: TechnicalsWidget,
  settingsComponent: TechnicalsSettings,
  defaultConfig: { symbol: 'NASDAQ:NVDA', interval: '1D' },
  configSchema: TechnicalsConfigSchema,
  defaultSize: { w: 6, h: 22 },
  // Min raised to 22 (= default) so the gauge + pills + labels always
  // render fully. At h<22 the edit-mode body (~cell-64px for chrome) drops
  // below TV's natural ~450px render height and the gauge clips at the
  // bottom. Users can still enlarge up to h=32.
  minSize: { w: 4, h: 22 },
  maxSize: { w: 12, h: 32 },
  source: 'tradingview',
});
