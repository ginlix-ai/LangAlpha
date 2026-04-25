import { TrendingUp } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
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
  return (
    <div className="space-y-4">
      <EnumField
        label="Exchange"
        value={config.exchange ?? 'US'}
        onChange={(v) => onChange({ exchange: v })}
        options={[
          { value: 'US', label: 'US (NYSE + NASDAQ)' },
          { value: 'NASDAQ', label: 'NASDAQ' },
          { value: 'NYSE', label: 'NYSE' },
          { value: 'LSE', label: 'London (LSE)' },
          { value: 'HKEX', label: 'Hong Kong (HKEX)' },
        ]}
      />
      <EnumField
        label="Category"
        value={config.dataSource ?? 'AllUSA'}
        onChange={(v) => onChange({ dataSource: v })}
        options={[
          { value: 'AllUSA', label: 'All (gainers/losers/active)' },
          { value: 'TopGainersUSA', label: 'Top gainers' },
          { value: 'TopLosersUSA', label: 'Top losers' },
          { value: 'MostActiveUSA', label: 'Most active' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<MoversConfig>({
  type: 'tv.movers',
  title: 'Market Movers',
  description: 'Top gainers / losers / most-active across US exchanges.',
  category: 'markets',
  icon: TrendingUp,
  component: MoversWidget,
  settingsComponent: MoversSettings,
  defaultConfig: { exchange: 'US', dataSource: 'AllUSA' },
  defaultSize: { w: 6, h: 22 },
  minSize: { w: 4, h: 14 },
  maxSize: { w: 12, h: 40 },
  source: 'tradingview',
});
