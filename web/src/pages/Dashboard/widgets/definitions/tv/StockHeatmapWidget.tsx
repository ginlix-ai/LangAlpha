import { useTranslation } from 'react-i18next';
import { Map } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { StockHeatmapConfigSchema } from '../../framework/configSchemas';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface StockHeatmapConfig {
  dataSource: string;
  blockSize: string;
  blockColor: string;
}

function StockHeatmapWidget({ instance }: WidgetRenderProps<StockHeatmapConfig>) {
  const { dataSource, blockSize, blockColor } = instance.config;
  return (
    <TradingViewEmbed
      card
      scriptKey="stock-heatmap"
      config={{
        dataSource,
        blockSize,
        blockColor,
        grouping: 'sector',
        hasTopBar: false,
        isDataSetEnabled: false,
        isZoomEnabled: true,
        hasSymbolTooltip: true,
      }}
    />
  );
}

function StockHeatmapSettings({ config, onChange, onClose }: WidgetSettingsProps<StockHeatmapConfig>) {
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <EnumField
        label={t('dashboard.widgets.stockHeatmap.market')}
        value={config.dataSource ?? 'SPX500'}
        onChange={(v) => onChange({ dataSource: v })}
        options={[
          { value: 'SPX500', label: t('dashboard.widgets.stockHeatmap.market_sp500') },
          { value: 'NASDAQ100', label: t('dashboard.widgets.stockHeatmap.market_ndx') },
          { value: 'DJI', label: t('dashboard.widgets.stockHeatmap.market_dji') },
          { value: 'FTSE100', label: t('dashboard.widgets.stockHeatmap.market_ftse') },
          { value: 'HSI', label: t('dashboard.widgets.stockHeatmap.market_hsi') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.stockHeatmap.cellSize')}
        value={config.blockSize ?? 'market_cap_basic'}
        onChange={(v) => onChange({ blockSize: v })}
        options={[
          { value: 'market_cap_basic', label: t('dashboard.widgets.stockHeatmap.cellSize_marketCap') },
          { value: 'volume', label: t('dashboard.widgets.stockHeatmap.cellSize_volume') },
          { value: 'number_of_employees', label: t('dashboard.widgets.stockHeatmap.cellSize_employees') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.stockHeatmap.cellColor')}
        value={config.blockColor ?? 'change'}
        onChange={(v) => onChange({ blockColor: v })}
        options={[
          { value: 'change', label: t('dashboard.widgets.stockHeatmap.cellColor_changeDay') },
          { value: 'Perf.W', label: t('dashboard.widgets.stockHeatmap.cellColor_changeWeek') },
          { value: 'Perf.1M', label: t('dashboard.widgets.stockHeatmap.cellColor_changeMonth') },
          { value: 'Volatility.W', label: t('dashboard.widgets.stockHeatmap.cellColor_volatility') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<StockHeatmapConfig>({
  type: 'tv.stock-heatmap',
  titleKey: 'dashboard.widgets.stockHeatmap.title',
  descriptionKey: 'dashboard.widgets.stockHeatmap.description',
  category: 'markets',
  icon: Map,
  component: StockHeatmapWidget,
  settingsComponent: StockHeatmapSettings,
  defaultConfig: { dataSource: 'SPX500', blockSize: 'market_cap_basic', blockColor: 'change' },
  configSchema: StockHeatmapConfigSchema,
  defaultSize: { w: 12, h: 22 },
  minSize: { w: 6, h: 12 },
  maxSize: { w: 12, h: 40 },
  source: 'tradingview',
});
