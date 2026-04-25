import type { ComponentType } from 'react';
import type { LucideIcon } from 'lucide-react';
import type { DashboardDataContextValue } from './framework/DashboardDataContext';

export type WidgetCategory = 'markets' | 'intel' | 'personal' | 'agent' | 'workspace';

export interface WidgetSize {
  w: number;
  h: number;
}

export interface WidgetRenderProps<C = unknown> {
  instance: WidgetInstance<C>;
  updateConfig: (patch: Partial<C>) => void;
}

export interface WidgetSettingsProps<C = unknown> {
  config: C;
  onChange: (patch: Partial<C>) => void;
  onClose: () => void;
}

export interface WidgetDefinition<C = unknown> {
  type: string;
  title: string;
  description?: string;
  category: WidgetCategory;
  icon: LucideIcon;
  component: ComponentType<WidgetRenderProps<C>>;
  defaultConfig: C;
  defaultSize: WidgetSize;
  minSize: WidgetSize;
  maxSize?: WidgetSize;
  settingsComponent?: ComponentType<WidgetSettingsProps<C>>;
  singleton?: boolean;
  /**
   * When true, the widget opts out of manual vertical resize: its cell height
   * is driven entirely by whatever it reports to `fitHeight`. The grid locks
   * `minH === maxH === measured-rows` so the resize grip can't stretch the
   * cell beyond the content. Width resize still works.
   */
  fitToContent?: boolean;
  /**
   * Gallery shows a provider badge and widget components should render
   * attribution when this is set. Currently only TradingView uses this flag.
   */
  source?: 'tradingview';
  /**
   * Optional factory that returns the initial config for a newly added
   * instance, given runtime dashboard context (watchlist, portfolio). Called
   * by the add-widget path instead of shallow-copying `defaultConfig`. Falls
   * back to `defaultConfig` when undefined.
   */
  initConfig?: (ctx: DashboardDataContextValue) => C;
}

export interface WidgetInstance<C = unknown> {
  id: string;
  type: string;
  config: C;
}

export interface RGLItem {
  i: string;
  x: number;
  y: number;
  w: number;
  h: number;
  minW?: number;
  minH?: number;
  maxW?: number;
  maxH?: number;
  static?: boolean;
}

export type BreakpointKey = 'lg' | 'md';

export interface DashboardPrefs {
  version: 1;
  mode: 'classic' | 'custom';
  widgets: WidgetInstance[];
  layouts: Partial<Record<BreakpointKey, RGLItem[]>>;
  lastBreakpoint?: BreakpointKey;
  history?: Array<{ widgets: WidgetInstance[]; layouts: Partial<Record<BreakpointKey, RGLItem[]>> }>;
}

export const DASHBOARD_PREFS_VERSION = 1 as const;
