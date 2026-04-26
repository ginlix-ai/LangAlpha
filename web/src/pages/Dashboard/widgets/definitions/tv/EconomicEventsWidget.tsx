import { useTranslation } from 'react-i18next';
import { CalendarClock } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { EconomicEventsConfigSchema } from '../../framework/configSchemas';
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
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      <EnumField
        label={t('dashboard.widgets.economicEvents.importance')}
        value={config.importanceFilter ?? '-1,0,1'}
        onChange={(v) => onChange({ importanceFilter: v })}
        options={[
          { value: '-1,0,1', label: t('dashboard.widgets.economicEvents.importance_all') },
          { value: '0,1', label: t('dashboard.widgets.economicEvents.importance_mediumHigh') },
          { value: '1', label: t('dashboard.widgets.economicEvents.importance_high') },
        ]}
      />
      <EnumField
        label={t('dashboard.widgets.economicEvents.countries')}
        value={config.countryFilter ?? 'us,eu,jp,gb,cn'}
        onChange={(v) => onChange({ countryFilter: v })}
        options={[
          { value: 'us,eu,jp,gb,cn', label: t('dashboard.widgets.economicEvents.countries_major') },
          { value: 'us', label: t('dashboard.widgets.economicEvents.countries_us') },
          { value: 'us,eu,gb', label: t('dashboard.widgets.economicEvents.countries_usEuGb') },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<EconomicEventsConfig>({
  type: 'tv.economic-events',
  titleKey: 'dashboard.widgets.economicEvents.title',
  descriptionKey: 'dashboard.widgets.economicEvents.description',
  category: 'markets',
  icon: CalendarClock,
  component: EconomicEventsWidget,
  settingsComponent: EconomicEventsSettings,
  defaultConfig: { importanceFilter: '-1,0,1', countryFilter: 'us,eu,jp,gb,cn' },
  configSchema: EconomicEventsConfigSchema,
  defaultSize: { w: 6, h: 24 },
  minSize: { w: 4, h: 14 },
  maxSize: { w: 12, h: 48 },
  source: 'tradingview',
});
