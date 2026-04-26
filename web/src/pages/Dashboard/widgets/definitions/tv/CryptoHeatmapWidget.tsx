import { useTranslation } from 'react-i18next';
import { Bitcoin } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { CryptoHeatmapConfigSchema } from '../../framework/configSchemas';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface CryptoHeatmapConfig {
  dataSource: string;
  blockSize: string;
  blockColor: string;
}

function CryptoHeatmapWidget({ instance }: WidgetRenderProps<CryptoHeatmapConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="crypto-coins-heatmap"
      config={{
        ...instance.config,
        hasTopBar: false,
        isDataSetEnabled: false,
        isZoomEnabled: true,
        hasSymbolTooltip: true,
      }}
    />
  );
}

function CryptoHeatmapSettings({ config, onChange, onClose }: WidgetSettingsProps<CryptoHeatmapConfig>) {
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <EnumField
        label={t('dashboard.widgets.cryptoHeatmap.dataSource')}
        value={config.dataSource ?? 'Crypto'}
        onChange={(v) => onChange({ dataSource: v })}
        options={[
          { value: 'Crypto', label: t('dashboard.widgets.cryptoHeatmap.dataSource_all') },
          { value: 'CryptoMarkets', label: t('dashboard.widgets.cryptoHeatmap.dataSource_markets') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.cryptoHeatmap.cellSize')}
        value={config.blockSize ?? 'market_cap_calc'}
        onChange={(v) => onChange({ blockSize: v })}
        options={[
          { value: 'market_cap_calc', label: t('dashboard.widgets.cryptoHeatmap.cellSize_marketCap') },
          { value: '24h_vol_cmc', label: t('dashboard.widgets.cryptoHeatmap.cellSize_volume24') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.cryptoHeatmap.cellColor')}
        value={config.blockColor ?? '24h_close_change|5'}
        onChange={(v) => onChange({ blockColor: v })}
        options={[
          { value: '24h_close_change|5', label: t('dashboard.widgets.cryptoHeatmap.cellColor_change24') },
          { value: 'Perf.W', label: t('dashboard.widgets.cryptoHeatmap.cellColor_change1w') },
          { value: 'Perf.1M', label: t('dashboard.widgets.cryptoHeatmap.cellColor_change1m') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<CryptoHeatmapConfig>({
  type: 'tv.crypto-heatmap',
  titleKey: 'dashboard.widgets.cryptoHeatmap.title',
  descriptionKey: 'dashboard.widgets.cryptoHeatmap.description',
  category: 'markets',
  icon: Bitcoin,
  component: CryptoHeatmapWidget,
  settingsComponent: CryptoHeatmapSettings,
  defaultConfig: { dataSource: 'Crypto', blockSize: 'market_cap_calc', blockColor: '24h_close_change|5' },
  configSchema: CryptoHeatmapConfigSchema,
  defaultSize: { w: 12, h: 20 },
  minSize: { w: 6, h: 12 },
  maxSize: { w: 12, h: 40 },
  source: 'tradingview',
});
