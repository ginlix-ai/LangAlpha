import { useTranslation } from 'react-i18next';
import { TrendingUp } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { MoversConfigSchema } from '../../framework/configSchemas';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface MoversConfig {
  exchange: string;
  dataSource: string;
}

function MoversWidget({ instance }: WidgetRenderProps<MoversConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="hotlists"
      config={{
        exchange: instance.config.exchange,
        dataSource: instance.config.dataSource,
        // showChart drives both the chart preview strip and TV's "selected
        // row" highlight (the heavy blue stripe). Off keeps a clean ranked
        // list with no row dominating the layout.
        showChart: false,
      }}
    />
  );
}

function MoversSettings({ config, onChange, onClose }: WidgetSettingsProps<MoversConfig>) {
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <EnumField
        label={t('dashboard.widgets.movers.exchange')}
        value={config.exchange ?? 'US'}
        onChange={(v) => onChange({ exchange: v })}
        options={[
          { value: 'US', label: t('dashboard.widgets.movers.exchange_us') },
          { value: 'NASDAQ', label: t('dashboard.widgets.movers.exchange_nasdaq') },
          { value: 'NYSE', label: t('dashboard.widgets.movers.exchange_nyse') },
          { value: 'LSE', label: t('dashboard.widgets.movers.exchange_lse') },
          { value: 'HKEX', label: t('dashboard.widgets.movers.exchange_hkex') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.movers.category')}
        value={config.dataSource ?? 'AllUSA'}
        onChange={(v) => onChange({ dataSource: v })}
        options={[
          { value: 'AllUSA', label: t('dashboard.widgets.movers.category_all') },
          { value: 'TopGainersUSA', label: t('dashboard.widgets.movers.category_gainers') },
          { value: 'TopLosersUSA', label: t('dashboard.widgets.movers.category_losers') },
          { value: 'MostActiveUSA', label: t('dashboard.widgets.movers.category_active') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<MoversConfig>({
  type: 'tv.movers',
  titleKey: 'dashboard.widgets.movers.title',
  descriptionKey: 'dashboard.widgets.movers.description',
  category: 'markets',
  icon: TrendingUp,
  component: MoversWidget,
  settingsComponent: MoversSettings,
  defaultConfig: { exchange: 'US', dataSource: 'AllUSA' },
  configSchema: MoversConfigSchema,
  defaultSize: { w: 6, h: 22 },
  minSize: { w: 4, h: 14 },
  maxSize: { w: 12, h: 40 },
  source: 'tradingview',
});
