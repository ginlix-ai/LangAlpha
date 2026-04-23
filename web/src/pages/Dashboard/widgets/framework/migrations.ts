import { DASHBOARD_PREFS_VERSION, type DashboardPrefs, type WidgetInstance } from '../types';

// Widget types that have been renamed. Stored prefs are silently upgraded on load.
const TYPE_RENAMES: Record<string, string> = {
  'agent.input': 'agent.conversation',
};

function renameWidgetTypes(widgets: WidgetInstance[]): WidgetInstance[] {
  return widgets.map((w) => {
    const renamed = TYPE_RENAMES[w.type];
    return renamed ? { ...w, type: renamed } : w;
  });
}

/**
 * Bring any stored dashboard prefs up to the current schema shape.
 * Absent / malformed input → null so callers can fall back to defaults.
 */
export function migrateDashboardPrefs(raw: unknown): DashboardPrefs | null {
  if (!raw || typeof raw !== 'object') return null;
  const src = raw as Partial<DashboardPrefs>;

  const mode = src.mode === 'custom' ? 'custom' : 'classic';
  const rawWidgets = Array.isArray(src.widgets) ? src.widgets : [];
  const widgets = renameWidgetTypes(rawWidgets);
  const layouts =
    src.layouts && typeof src.layouts === 'object' && !Array.isArray(src.layouts)
      ? src.layouts
      : {};

  return {
    version: DASHBOARD_PREFS_VERSION,
    mode,
    widgets,
    layouts,
    lastBreakpoint: src.lastBreakpoint,
    history: Array.isArray(src.history) ? src.history : undefined,
  };
}
