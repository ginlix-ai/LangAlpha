import { useTranslation } from 'react-i18next';
import { BarChart3 } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { CompanyFinancialsConfigSchema } from '../../framework/configSchemas';
import { SymbolField } from '../../framework/settings/SymbolField';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface CompanyFinancialsConfig {
  symbol: string;
  displayMode: 'regular' | 'compact' | 'adaptive';
}

function CompanyFinancialsWidget({ instance }: WidgetRenderProps<CompanyFinancialsConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="financials"
      config={{
        symbol: instance.config.symbol,
        displayMode: instance.config.displayMode,
      }}
    />
  );
}

function CompanyFinancialsSettings({ config, onChange, onClose }: WidgetSettingsProps<CompanyFinancialsConfig>) {
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <SymbolField
        label={t('dashboard.widgets.companyFinancials.symbol')}
        value={config.symbol ?? ''}
        onChange={(v) => onChange({ symbol: v })}
        placeholder="NASDAQ:NVDA"
      />
      <EnumField
        label={t('dashboard.widgets.companyFinancials.layout')}
        value={config.displayMode ?? 'regular'}
        onChange={(v) => onChange({ displayMode: v as CompanyFinancialsConfig['displayMode'] })}
        options={[
          { value: 'regular', label: t('dashboard.widgets.companyFinancials.layout_regular') },
          { value: 'compact', label: t('dashboard.widgets.companyFinancials.layout_compact') },
          { value: 'adaptive', label: t('dashboard.widgets.companyFinancials.layout_adaptive') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<CompanyFinancialsConfig>({
  type: 'tv.company-financials',
  titleKey: 'dashboard.widgets.companyFinancials.title',
  descriptionKey: 'dashboard.widgets.companyFinancials.description',
  category: 'markets',
  icon: BarChart3,
  component: CompanyFinancialsWidget,
  settingsComponent: CompanyFinancialsSettings,
  defaultConfig: { symbol: 'NASDAQ:NVDA', displayMode: 'regular' },
  configSchema: CompanyFinancialsConfigSchema,
  defaultSize: { w: 6, h: 24 },
  minSize: { w: 4, h: 14 },
  maxSize: { w: 12, h: 48 },
  source: 'tradingview',
});
