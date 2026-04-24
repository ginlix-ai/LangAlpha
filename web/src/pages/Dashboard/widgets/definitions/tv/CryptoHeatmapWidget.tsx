import { Bitcoin } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
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
  return (
    <div className="space-y-4">
      <EnumField
        label="Data source"
        value={config.dataSource ?? 'Crypto'}
        onChange={(v) => onChange({ dataSource: v })}
        options={[
          { value: 'Crypto', label: 'All crypto' },
          { value: 'CryptoMarkets', label: 'Crypto markets' },
        ]}
      />
      <EnumField
        label="Cell size"
        value={config.blockSize ?? 'market_cap_calc'}
        onChange={(v) => onChange({ blockSize: v })}
        options={[
          { value: 'market_cap_calc', label: 'Market cap' },
          { value: '24h_vol_cmc', label: '24h volume' },
        ]}
      />
      <EnumField
        label="Cell color"
        value={config.blockColor ?? '24h_close_change|5'}
        onChange={(v) => onChange({ blockColor: v })}
        options={[
          { value: '24h_close_change|5', label: '24h change' },
          { value: 'Perf.W', label: '1W change' },
          { value: 'Perf.1M', label: '1M change' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<CryptoHeatmapConfig>({
  type: 'tv.crypto-heatmap',
  title: 'Crypto Heatmap',
  description: 'Crypto treemap sized by market cap, colored by performance.',
  category: 'markets',
  icon: Bitcoin,
  component: CryptoHeatmapWidget,
  settingsComponent: CryptoHeatmapSettings,
  defaultConfig: { dataSource: 'Crypto', blockSize: 'market_cap_calc', blockColor: '24h_close_change|5' },
  defaultSize: { w: 12, h: 20 },
  minSize: { w: 6, h: 12 },
  maxSize: { w: 12, h: 40 },
  source: 'tradingview',
});
