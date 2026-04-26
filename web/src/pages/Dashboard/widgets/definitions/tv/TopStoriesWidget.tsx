import { useTranslation } from 'react-i18next';
import { Newspaper } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { TopStoriesConfigSchema } from '../../framework/configSchemas';
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
  const { t } = useTranslation();
  const feedMode = config.feedMode ?? 'market';
  return (
    <div className="space-y-4">
      <EnumField
        label={t('dashboard.widgets.topStories.feed')}
        value={feedMode}
        onChange={(v) => onChange({ feedMode: v as TopStoriesConfig['feedMode'] })}
        options={[
          { value: 'all_symbols', label: t('dashboard.widgets.topStories.feed_all') },
          { value: 'market', label: t('dashboard.widgets.topStories.feed_market') },
          { value: 'symbol', label: t('dashboard.widgets.topStories.feed_symbol') },
        ]}
      />
      {feedMode === 'market' && (
        <EnumField
          label={t('dashboard.widgets.topStories.market')}
          value={config.market ?? 'stock'}
          onChange={(v) => onChange({ market: v as TopStoriesConfig['market'] })}
          options={[
            { value: 'stock', label: t('dashboard.widgets.topStories.market_stocks') },
            { value: 'crypto', label: t('dashboard.widgets.topStories.market_crypto') },
            { value: 'forex', label: t('dashboard.widgets.topStories.market_forex') },
            { value: 'index', label: t('dashboard.widgets.topStories.market_indices') },
            { value: 'futures', label: t('dashboard.widgets.topStories.market_futures') },
            { value: 'bond', label: t('dashboard.widgets.topStories.market_bonds') },
            { value: 'economic', label: t('dashboard.widgets.topStories.market_economic') },
          ]}
        />
      )}
      {feedMode === 'symbol' && (
        <SymbolField
          label={t('dashboard.widgets.topStories.symbol')}
          value={config.symbol ?? 'NASDAQ:NVDA'}
          onChange={(v) => onChange({ symbol: v })}
        />
      )}
      <EnumField
        label={t('dashboard.widgets.topStories.density')}
        value={config.displayMode ?? 'regular'}
        onChange={(v) => onChange({ displayMode: v as TopStoriesConfig['displayMode'] })}
        options={[
          { value: 'regular', label: t('dashboard.widgets.topStories.density_regular') },
          { value: 'compact', label: t('dashboard.widgets.topStories.density_compact') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<TopStoriesConfig>({
  type: 'tv.top-stories',
  titleKey: 'dashboard.widgets.topStories.title',
  descriptionKey: 'dashboard.widgets.topStories.description',
  category: 'markets',
  icon: Newspaper,
  component: TopStoriesWidget,
  settingsComponent: TopStoriesSettings,
  defaultConfig: { feedMode: 'market', market: 'stock', symbol: 'NASDAQ:NVDA', displayMode: 'regular' },
  configSchema: TopStoriesConfigSchema,
  defaultSize: { w: 6, h: 22 },
  minSize: { w: 4, h: 14 },
  maxSize: { w: 12, h: 40 },
  source: 'tradingview',
});
