import { CalendarClock } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface EconomicEventsConfig {
  importanceFilter: string; // '-1,0,1'
  countryFilter: string;
}

function EconomicEventsWidget({ instance }: WidgetRenderProps<EconomicEventsConfig>) {
  return (
    <TradingViewEmbed
      card
      scriptKey="events"
      config={{
        importanceFilter: instance.config.importanceFilter,
        countryFilter: instance.config.countryFilter,
      }}
    />
  );
}

function EconomicEventsSettings({ config, onChange, onClose }: WidgetSettingsProps<EconomicEventsConfig>) {
  return (
    <div className="space-y-4">
      <EnumField
        label="Importance"
        value={config.importanceFilter ?? '-1,0,1'}
        onChange={(v) => onChange({ importanceFilter: v })}
        options={[
          { value: '-1,0,1', label: 'All (low, medium, high)' },
          { value: '0,1', label: 'Medium + high' },
          { value: '1', label: 'High only' },
        ]}
      />
      <EnumField
        label="Countries"
        value={config.countryFilter ?? 'us,eu,jp,gb,cn'}
        onChange={(v) => onChange({ countryFilter: v })}
        options={[
          { value: 'us,eu,jp,gb,cn', label: 'Major (US, EU, JP, GB, CN)' },
          { value: 'us', label: 'US only' },
          { value: 'us,eu,gb', label: 'US + EU + GB' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<EconomicEventsConfig>({
  type: 'tv.economic-events',
  title: 'Economic Calendar',
  description: 'Upcoming releases (CPI, NFP, FOMC, etc.) filtered by importance.',
  category: 'markets',
  icon: CalendarClock,
  component: EconomicEventsWidget,
  settingsComponent: EconomicEventsSettings,
  defaultConfig: { importanceFilter: '-1,0,1', countryFilter: 'us,eu,jp,gb,cn' },
  defaultSize: { w: 6, h: 24 },
  minSize: { w: 4, h: 14 },
  maxSize: { w: 12, h: 48 },
  source: 'tradingview',
});
