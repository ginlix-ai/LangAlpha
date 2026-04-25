import { Layers } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
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
  return (
    <div className="space-y-4">
      <EnumField
        label="Universe"
        value={config.dataSource ?? 'AllUSEtf'}
        onChange={(v) => onChange({ dataSource: v })}
        options={[
          { value: 'AllUSEtf', label: 'All US ETFs' },
          { value: 'EquityEtf', label: 'Equity ETFs' },
          { value: 'BondEtf', label: 'Bond ETFs' },
          { value: 'CommodityEtf', label: 'Commodity ETFs' },
          { value: 'CurrencyEtf', label: 'Currency ETFs' },
        ]}
      />
      <EnumField
        label="Block size by"
        value={config.blockSize ?? 'aum'}
        onChange={(v) => onChange({ blockSize: v })}
        options={[
          { value: 'aum', label: 'AUM' },
          { value: 'volume', label: 'Volume' },
          { value: 'no_filter', label: 'Equal size' },
        ]}
      />
      <EnumField
        label="Color by"
        value={config.blockColor ?? 'change'}
        onChange={(v) => onChange({ blockColor: v })}
        options={[
          { value: 'change', label: '% change' },
          { value: 'Perf.W', label: '1 week performance' },
          { value: 'Perf.1M', label: '1 month performance' },
          { value: 'Perf.YTD', label: 'YTD performance' },
        ]}
      />
      <EnumField
        label="Group by"
        value={config.grouping ?? 'asset_class'}
        onChange={(v) => onChange({ grouping: v })}
        options={[
          { value: 'asset_class', label: 'Asset class' },
          { value: 'no_group', label: 'None' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<ETFHeatmapConfig>({
  type: 'tv.etf-heatmap',
  title: 'ETF Heatmap',
  description: 'Treemap of ETFs sized by AUM, colored by performance.',
  category: 'markets',
  icon: Layers,
  component: ETFHeatmapWidget,
  settingsComponent: ETFHeatmapSettings,
  defaultConfig: { dataSource: 'AllUSEtf', blockSize: 'aum', blockColor: 'change', grouping: 'asset_class' },
  defaultSize: { w: 12, h: 20 },
  minSize: { w: 6, h: 12 },
  maxSize: { w: 12, h: 40 },
  source: 'tradingview',
});
