import { Bitcoin } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
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
  return (
    <div className="space-y-4">
      <EnumField
        label="Default column set"
        value={config.defaultColumn ?? 'overview'}
        onChange={(v) => onChange({ defaultColumn: v })}
        options={[
          { value: 'overview', label: 'Overview' },
          { value: 'performance', label: 'Performance' },
          { value: 'oscillators', label: 'Oscillators' },
          { value: 'moving_averages', label: 'Moving averages' },
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

registerWidget<CryptoScreenerConfig>({
  type: 'tv.crypto-screener',
  title: 'Crypto Screener',
  description: 'Filter the crypto universe by market cap, performance, technicals.',
  category: 'markets',
  icon: Bitcoin,
  component: CryptoScreenerWidget,
  settingsComponent: CryptoScreenerSettings,
  defaultConfig: { defaultColumn: 'overview', defaultScreen: 'general' },
  defaultSize: { w: 12, h: 24 },
  minSize: { w: 6, h: 14 },
  maxSize: { w: 12, h: 48 },
  source: 'tradingview',
});
