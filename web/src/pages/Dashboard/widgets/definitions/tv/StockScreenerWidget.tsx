import { useTranslation } from 'react-i18next';
import { Filter } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { StockScreenerConfigSchema } from '../../framework/configSchemas';
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
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <EnumField
        label={t('dashboard.widgets.stockScreener.market')}
        value={config.market ?? 'america'}
        onChange={(v) => onChange({ market: v })}
        options={[
          { value: 'america', label: t('dashboard.widgets.stockScreener.market_us') },
          { value: 'uk', label: t('dashboard.widgets.stockScreener.market_uk') },
          { value: 'germany', label: t('dashboard.widgets.stockScreener.market_de') },
          { value: 'japan', label: t('dashboard.widgets.stockScreener.market_jp') },
          { value: 'china', label: t('dashboard.widgets.stockScreener.market_cn') },
          { value: 'hongkong', label: t('dashboard.widgets.stockScreener.market_hk') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.stockScreener.columns')}
        value={config.defaultColumn ?? 'overview'}
        onChange={(v) => onChange({ defaultColumn: v })}
        options={[
          { value: 'overview', label: t('dashboard.widgets.stockScreener.columns_overview') },
          { value: 'performance', label: t('dashboard.widgets.stockScreener.columns_performance') },
          { value: 'valuation', label: t('dashboard.widgets.stockScreener.columns_valuation') },
          { value: 'dividends', label: t('dashboard.widgets.stockScreener.columns_dividends') },
          { value: 'profitability', label: t('dashboard.widgets.stockScreener.columns_profitability') },
          { value: 'technicals', label: t('dashboard.widgets.stockScreener.columns_technicals') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.stockScreener.screen')}
        value={config.defaultScreen ?? 'general'}
        onChange={(v) => onChange({ defaultScreen: v })}
        options={[
          { value: 'general', label: t('dashboard.widgets.stockScreener.screen_general') },
          { value: 'most_capitalized', label: t('dashboard.widgets.stockScreener.screen_mostCap') },
          { value: 'top_gainers', label: t('dashboard.widgets.stockScreener.screen_topGainers') },
          { value: 'top_losers', label: t('dashboard.widgets.stockScreener.screen_topLosers') },
          { value: 'most_active', label: t('dashboard.widgets.stockScreener.screen_active') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<StockScreenerConfig>({
  type: 'tv.screener',
  titleKey: 'dashboard.widgets.stockScreener.title',
  descriptionKey: 'dashboard.widgets.stockScreener.description',
  category: 'markets',
  icon: Filter,
  component: StockScreenerWidget,
  settingsComponent: StockScreenerSettings,
  defaultConfig: { market: 'america', defaultColumn: 'overview', defaultScreen: 'general' },
  configSchema: StockScreenerConfigSchema,
  defaultSize: { w: 12, h: 24 },
  minSize: { w: 6, h: 14 },
  maxSize: { w: 12, h: 48 },
  source: 'tradingview',
});
