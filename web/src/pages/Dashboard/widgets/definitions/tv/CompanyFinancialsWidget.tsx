import { BarChart3 } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
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
  return (
    <div className="space-y-4">
      <SymbolField
        label="Symbol"
        value={config.symbol ?? ''}
        onChange={(v) => onChange({ symbol: v })}
        placeholder="NASDAQ:NVDA"
      />
      <EnumField
        label="Layout"
        value={config.displayMode ?? 'regular'}
        onChange={(v) => onChange({ displayMode: v as CompanyFinancialsConfig['displayMode'] })}
        options={[
          { value: 'regular', label: 'Regular' },
          { value: 'compact', label: 'Compact' },
          { value: 'adaptive', label: 'Adaptive' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<CompanyFinancialsConfig>({
  type: 'tv.company-financials',
  title: 'Company Financials',
  description: 'Fundamentals — income statement, balance sheet, cash flow, key ratios.',
  category: 'markets',
  icon: BarChart3,
  component: CompanyFinancialsWidget,
  settingsComponent: CompanyFinancialsSettings,
  defaultConfig: { symbol: 'NASDAQ:NVDA', displayMode: 'regular' },
  defaultSize: { w: 6, h: 24 },
  minSize: { w: 4, h: 14 },
  maxSize: { w: 12, h: 48 },
  source: 'tradingview',
});
