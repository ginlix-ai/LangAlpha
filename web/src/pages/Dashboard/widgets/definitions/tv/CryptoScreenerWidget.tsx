import { useTranslation } from 'react-i18next';
import { Bitcoin } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { CryptoScreenerConfigSchema } from '../../framework/configSchemas';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface CryptoScreenerConfig {
  defaultColumn: string;
  defaultScreen: string;
}

// Crypto screener is the same `embed-widget-screener.js` iframe as the Stock
// Screener, flipped to `screener_type: 'crypto_mkt'` (verified from TV's
// /widget-docs/widgets/screeners/crypto-mkt-screener/ demo config). Splitting
// into two widgets keeps the gallery presets clear instead of making users
// toggle a market-type dropdown.
function CryptoScreenerWidget({ instance }: WidgetRenderProps<CryptoScreenerConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="screener"
      config={{
        screener_type: 'crypto_mkt',
        market: 'crypto',
        defaultColumn: instance.config.defaultColumn,
        defaultScreen: instance.config.defaultScreen,
        showToolbar: true,
      }}
    />
  );
}

function CryptoScreenerSettings({ config, onChange, onClose }: WidgetSettingsProps<CryptoScreenerConfig>) {
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <EnumField
        label={t('dashboard.widgets.cryptoScreener.columns')}
        value={config.defaultColumn ?? 'overview'}
        onChange={(v) => onChange({ defaultColumn: v })}
        options={[
          { value: 'overview', label: t('dashboard.widgets.cryptoScreener.columns_overview') },
          { value: 'performance', label: t('dashboard.widgets.cryptoScreener.columns_performance') },
          { value: 'oscillators', label: t('dashboard.widgets.cryptoScreener.columns_oscillators') },
          { value: 'moving_averages', label: t('dashboard.widgets.cryptoScreener.columns_movingAverages') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.cryptoScreener.screen')}
        value={config.defaultScreen ?? 'general'}
        onChange={(v) => onChange({ defaultScreen: v })}
        options={[
          { value: 'general', label: t('dashboard.widgets.cryptoScreener.screen_general') },
          { value: 'most_capitalized', label: t('dashboard.widgets.cryptoScreener.screen_mostCap') },
          { value: 'top_gainers', label: t('dashboard.widgets.cryptoScreener.screen_topGainers') },
          { value: 'top_losers', label: t('dashboard.widgets.cryptoScreener.screen_topLosers') },
          { value: 'most_active', label: t('dashboard.widgets.cryptoScreener.screen_active') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<CryptoScreenerConfig>({
  type: 'tv.crypto-screener',
  titleKey: 'dashboard.widgets.cryptoScreener.title',
  descriptionKey: 'dashboard.widgets.cryptoScreener.description',
  category: 'markets',
  icon: Bitcoin,
  component: CryptoScreenerWidget,
  settingsComponent: CryptoScreenerSettings,
  defaultConfig: { defaultColumn: 'overview', defaultScreen: 'general' },
  configSchema: CryptoScreenerConfigSchema,
  defaultSize: { w: 12, h: 24 },
  minSize: { w: 6, h: 14 },
  maxSize: { w: 12, h: 48 },
  source: 'tradingview',
});
