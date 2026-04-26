import { useTranslation } from 'react-i18next';
import { Layers } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { ETFHeatmapConfigSchema } from '../../framework/configSchemas';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface ETFHeatmapConfig {
  dataSource: string;
  blockSize: string;
  blockColor: string;
  grouping: string;
}

function ETFHeatmapWidget({ instance }: WidgetRenderProps<ETFHeatmapConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="etf-heatmap"
      config={{
        dataSource: instance.config.dataSource,
        blockSize: instance.config.blockSize,
        blockColor: instance.config.blockColor,
        grouping: instance.config.grouping,
      }}
    />
  );
}

function ETFHeatmapSettings({ config, onChange, onClose }: WidgetSettingsProps<ETFHeatmapConfig>) {
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <EnumField
        label={t('dashboard.widgets.etfHeatmap.universe')}
        value={config.dataSource ?? 'AllUSEtf'}
        onChange={(v) => onChange({ dataSource: v })}
        options={[
          { value: 'AllUSEtf', label: t('dashboard.widgets.etfHeatmap.universe_allUS') },
          { value: 'EquityEtf', label: t('dashboard.widgets.etfHeatmap.universe_equity') },
          { value: 'BondEtf', label: t('dashboard.widgets.etfHeatmap.universe_bond') },
          { value: 'CommodityEtf', label: t('dashboard.widgets.etfHeatmap.universe_commodity') },
          { value: 'CurrencyEtf', label: t('dashboard.widgets.etfHeatmap.universe_currency') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.etfHeatmap.blockSize')}
        value={config.blockSize ?? 'aum'}
        onChange={(v) => onChange({ blockSize: v })}
        options={[
          { value: 'aum', label: t('dashboard.widgets.etfHeatmap.blockSize_aum') },
          { value: 'volume', label: t('dashboard.widgets.etfHeatmap.blockSize_volume') },
          { value: 'no_filter', label: t('dashboard.widgets.etfHeatmap.blockSize_equal') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.etfHeatmap.color')}
        value={config.blockColor ?? 'change'}
        onChange={(v) => onChange({ blockColor: v })}
        options={[
          { value: 'change', label: t('dashboard.widgets.etfHeatmap.color_change') },
          { value: 'Perf.W', label: t('dashboard.widgets.etfHeatmap.color_1w') },
          { value: 'Perf.1M', label: t('dashboard.widgets.etfHeatmap.color_1m') },
          { value: 'Perf.YTD', label: t('dashboard.widgets.etfHeatmap.color_ytd') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.etfHeatmap.groupBy')}
        value={config.grouping ?? 'asset_class'}
        onChange={(v) => onChange({ grouping: v })}
        options={[
          { value: 'asset_class', label: t('dashboard.widgets.etfHeatmap.groupBy_assetClass') },
          { value: 'no_group', label: t('dashboard.widgets.etfHeatmap.groupBy_none') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<ETFHeatmapConfig>({
  type: 'tv.etf-heatmap',
  titleKey: 'dashboard.widgets.etfHeatmap.title',
  descriptionKey: 'dashboard.widgets.etfHeatmap.description',
  category: 'markets',
  icon: Layers,
  component: ETFHeatmapWidget,
  settingsComponent: ETFHeatmapSettings,
  defaultConfig: { dataSource: 'AllUSEtf', blockSize: 'aum', blockColor: 'change', grouping: 'asset_class' },
  configSchema: ETFHeatmapConfigSchema,
  defaultSize: { w: 12, h: 20 },
  minSize: { w: 6, h: 12 },
  maxSize: { w: 12, h: 40 },
  source: 'tradingview',
});
