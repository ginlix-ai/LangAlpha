import { Filter } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface StockScreenerConfig {
  market: string;
  defaultColumn: string;
  defaultScreen: string;
}

function StockScreenerWidget({ instance }: WidgetRenderProps<StockScreenerConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="screener"
      config={{
        screener_type: 'stock',
        market: instance.config.market,
        defaultColumn: instance.config.defaultColumn,
        defaultScreen: instance.config.defaultScreen,
        showToolbar: true,
      }}
    />
  );
}

function StockScreenerSettings({ config, onChange, onClose }: WidgetSettingsProps<StockScreenerConfig>) {
  return (
    <div className="space-y-4">
      <EnumField
        label="Market"
        value={config.market ?? 'america'}
        onChange={(v) => onChange({ market: v })}
        options={[
          { value: 'america', label: 'USA (NYSE + NASDAQ + AMEX)' },
          { value: 'uk', label: 'United Kingdom (LSE)' },
          { value: 'germany', label: 'Germany (XETR)' },
          { value: 'japan', label: 'Japan' },
          { value: 'china', label: 'China (SSE)' },
          { value: 'hongkong', label: 'Hong Kong (HKEX)' },
        ]}
      />
      <EnumField
        label="Default column set"
        value={config.defaultColumn ?? 'overview'}
        onChange={(v) => onChange({ defaultColumn: v })}
        options={[
          { value: 'overview', label: 'Overview' },
          { value: 'performance', label: 'Performance' },
          { value: 'valuation', label: 'Valuation' },
          { value: 'dividends', label: 'Dividends' },
          { value: 'profitability', label: 'Profitability' },
          { value: 'technicals', label: 'Technicals' },
        ]}
      />
      <EnumField
        label="Default screen"
        value={config.defaultScreen ?? 'general'}
        onChange={(v) => onChange({ defaultScreen: v })}
        options={[
          { value: 'general', label: 'General' },
          { value: 'most_capitalized', label: 'Most capitalized' },
          { value: 'top_gainers', label: 'Top gainers' },
          { value: 'top_losers', label: 'Top losers' },
          { value: 'most_active', label: 'Most active' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<StockScreenerConfig>({
  type: 'tv.screener',
  title: 'Stock Screener',
  description: 'Filter the full stock universe by price, market cap, technicals, fundamentals.',
  category: 'markets',
  icon: Filter,
  component: StockScreenerWidget,
  settingsComponent: StockScreenerSettings,
  defaultConfig: { market: 'america', defaultColumn: 'overview', defaultScreen: 'general' },
  defaultSize: { w: 12, h: 24 },
  minSize: { w: 6, h: 14 },
  maxSize: { w: 12, h: 48 },
  source: 'tradingview',
});
