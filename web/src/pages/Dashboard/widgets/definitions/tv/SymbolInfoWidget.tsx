import { useTranslation } from 'react-i18next';
import { Info } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { SymbolInfoConfigSchema } from '../../framework/configSchemas';
import { SymbolField } from '../../framework/settings/SymbolField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface SymbolInfoConfig {
  symbol: string;
}

function SymbolInfoWidget({ instance }: WidgetRenderProps<SymbolInfoConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="symbol-info"
      config={{ symbol: instance.config.symbol }}
    />
  );
}

function SymbolInfoSettings({ config, onChange, onClose }: WidgetSettingsProps<SymbolInfoConfig>) {
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <SymbolField
        label={t('dashboard.widgets.symbolInfo.symbol')}
        value={config.symbol ?? ''}
        onChange={(v) => onChange({ symbol: v })}
        placeholder="NASDAQ:NVDA"
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<SymbolInfoConfig>({
  type: 'tv.symbol-info',
  titleKey: 'dashboard.widgets.symbolInfo.title',
  descriptionKey: 'dashboard.widgets.symbolInfo.description',
  category: 'markets',
  icon: Info,
  component: SymbolInfoWidget,
  settingsComponent: SymbolInfoSettings,
  defaultConfig: { symbol: 'NASDAQ:NVDA' },
  configSchema: SymbolInfoConfigSchema,
  defaultSize: { w: 6, h: 8 },
  minSize: { w: 4, h: 6 },
  maxSize: { w: 12, h: 12 },
  source: 'tradingview',
});
