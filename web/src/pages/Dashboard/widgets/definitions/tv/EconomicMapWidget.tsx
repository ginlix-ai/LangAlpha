import { Globe } from 'lucide-react';
import { TradingViewWebComponent } from '../../framework/TradingViewWebComponent';
import { registerWidget } from '../../framework/WidgetRegistry';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface EconomicMapConfig {
  region: 'global' | 'africa' | 'asia' | 'europe' | 'north-america' | 'oceania' | 'south-america';
  metric: 'gdp' | 'ur' | 'gdg' | 'intr' | 'iryy';
  hideLegend: boolean;
}

// Attribute shape verified against the TV wizard module
// (widgets.tradingview-widget.com/wizards/tv-economic-map-wizard.js):
//   defaults = { region: "global", metric: "gdp", hideLegend: false, metrics: [] }
//   metric enum (label):
//     gdp  → GDP
//     ur   → Unemployment Rate
//     gdg  → Government Debt to GDP
//     intr → Interest Rate
//     iryy → Inflation Rate
//   region enum: global, africa, asia, europe, north-america, oceania, south-america
function EconomicMapWidget({ instance }: WidgetRenderProps<EconomicMapConfig>) {
  return (
    <TradingViewWebComponent
      card
      element="tv-economic-map"
      config={{
        region: instance.config.region,
        metric: instance.config.metric,
        hideLegend: instance.config.hideLegend,
      }}
    />
  );
}

function EconomicMapSettings({ config, onChange, onClose }: WidgetSettingsProps<EconomicMapConfig>) {
  return (
    <div className="space-y-4">
      <EnumField
        label="Region"
        value={config.region ?? 'global'}
        onChange={(v) => onChange({ region: v as EconomicMapConfig['region'] })}
        options={[
          { value: 'global', label: 'Global' },
          { value: 'north-america', label: 'North America' },
          { value: 'south-america', label: 'South America' },
          { value: 'europe', label: 'Europe' },
          { value: 'asia', label: 'Asia' },
          { value: 'africa', label: 'Africa' },
          { value: 'oceania', label: 'Oceania' },
        ]}
      />
      <EnumField
        label="Metric"
        value={config.metric ?? 'gdp'}
        onChange={(v) => onChange({ metric: v as EconomicMapConfig['metric'] })}
        options={[
          { value: 'gdp', label: 'GDP' },
          { value: 'iryy', label: 'Inflation Rate' },
          { value: 'intr', label: 'Interest Rate' },
          { value: 'ur', label: 'Unemployment Rate' },
          { value: 'gdg', label: 'Government Debt to GDP' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<EconomicMapConfig>({
  type: 'tv.economic-map',
  title: 'Economic Map',
  description: 'World map of economic indicators (GDP, inflation, interest rates) by country.',
  category: 'markets',
  icon: Globe,
  component: EconomicMapWidget,
  settingsComponent: EconomicMapSettings,
  defaultConfig: { region: 'global', metric: 'gdp', hideLegend: false },
  // Wizard reports natural size 750w × 475h. h=18 (~416px content) hugs that
  // aspect at w=12 so the map + legend strip render without letterboxing.
  // Min raised to 18 (= default) so country labels and the legend always
  // fit; at smaller h the world map shrinks below legibility.
  defaultSize: { w: 12, h: 18 },
  minSize: { w: 6, h: 18 },
  maxSize: { w: 12, h: 32 },
  source: 'tradingview',
});
