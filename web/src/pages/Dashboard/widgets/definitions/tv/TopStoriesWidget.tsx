import { Newspaper } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { SymbolField } from '../../framework/settings/SymbolField';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface TopStoriesConfig {
  feedMode: 'all_symbols' | 'market' | 'symbol';
  market: 'stock' | 'crypto' | 'forex' | 'index' | 'futures' | 'bond' | 'economic';
  symbol: string;
  displayMode: 'regular' | 'compact';
}

function TopStoriesWidget({ instance }: WidgetRenderProps<TopStoriesConfig>) {
  const { feedMode, market, symbol, displayMode } = instance.config;
  // TV's timeline widget accepts either `market` OR `symbol` depending on
  // feedMode. Passing the right key prevents the widget from falling back
  // to an empty state.
  const cfg: Record<string, unknown> = { feedMode, displayMode };
  if (feedMode === 'market') cfg.market = market;
  if (feedMode === 'symbol') cfg.symbol = symbol;
  return <TradingViewEmbed card scriptKey="timeline" config={cfg} />;
}

function TopStoriesSettings({ config, onChange, onClose }: WidgetSettingsProps<TopStoriesConfig>) {
  const feedMode = config.feedMode ?? 'market';
  return (
    <div className="space-y-4">
      <EnumField
        label="Feed"
        value={feedMode}
        onChange={(v) => onChange({ feedMode: v as TopStoriesConfig['feedMode'] })}
        options={[
          { value: 'all_symbols', label: 'All markets' },
          { value: 'market', label: 'Specific market' },
          { value: 'symbol', label: 'Specific symbol' },
        ]}
      />
      {feedMode === 'market' && (
        <EnumField
          label="Market"
          value={config.market ?? 'stock'}
          onChange={(v) => onChange({ market: v as TopStoriesConfig['market'] })}
          options={[
            { value: 'stock', label: 'Stocks' },
            { value: 'crypto', label: 'Crypto' },
            { value: 'forex', label: 'Forex' },
            { value: 'index', label: 'Indices' },
            { value: 'futures', label: 'Futures' },
            { value: 'bond', label: 'Bonds' },
            { value: 'economic', label: 'Economic' },
          ]}
        />
      )}
      {feedMode === 'symbol' && (
        <SymbolField
          label="Symbol"
          value={config.symbol ?? 'NASDAQ:NVDA'}
          onChange={(v) => onChange({ symbol: v })}
        />
      )}
      <EnumField
        label="Density"
        value={config.displayMode ?? 'regular'}
        onChange={(v) => onChange({ displayMode: v as TopStoriesConfig['displayMode'] })}
        options={[
          { value: 'regular', label: 'Regular (headlines + snippets)' },
          { value: 'compact', label: 'Compact (headlines only)' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<TopStoriesConfig>({
  type: 'tv.top-stories',
  title: 'Top Stories',
  description: 'Market headlines from TradingView — by market or pinned to a single symbol.',
  category: 'markets',
  icon: Newspaper,
  component: TopStoriesWidget,
  settingsComponent: TopStoriesSettings,
  defaultConfig: { feedMode: 'market', market: 'stock', symbol: 'NASDAQ:NVDA', displayMode: 'regular' },
  defaultSize: { w: 6, h: 22 },
  minSize: { w: 4, h: 14 },
  maxSize: { w: 12, h: 40 },
  source: 'tradingview',
});
