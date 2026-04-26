import { useTranslation } from 'react-i18next';
import { Building2 } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { CompanyProfileConfigSchema } from '../../framework/configSchemas';
import { SymbolField } from '../../framework/settings/SymbolField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface CompanyProfileConfig {
  symbol: string;
}

function CompanyProfileWidget({ instance }: WidgetRenderProps<CompanyProfileConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="symbol-profile"
      config={{ symbol: instance.config.symbol }}
    />
  );
}

function CompanyProfileSettings({ config, onChange, onClose }: WidgetSettingsProps<CompanyProfileConfig>) {
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <SymbolField
        label={t('dashboard.widgets.companyProfile.symbol')}
        value={config.symbol ?? ''}
        onChange={(v) => onChange({ symbol: v })}
        placeholder="NASDAQ:NVDA"
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<CompanyProfileConfig>({
  type: 'tv.company-profile',
  titleKey: 'dashboard.widgets.companyProfile.title',
  descriptionKey: 'dashboard.widgets.companyProfile.description',
  category: 'markets',
  icon: Building2,
  component: CompanyProfileWidget,
  settingsComponent: CompanyProfileSettings,
  defaultConfig: { symbol: 'NASDAQ:NVDA' },
  configSchema: CompanyProfileConfigSchema,
  defaultSize: { w: 6, h: 18 },
  minSize: { w: 4, h: 12 },
  maxSize: { w: 12, h: 32 },
  source: 'tradingview',
});
