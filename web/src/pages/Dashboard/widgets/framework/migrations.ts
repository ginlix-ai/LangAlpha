import { getWidget } from './WidgetRegistry';
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
 * Tighter membership check than `Array.isArray(widgets)`. Drops any entry that
 * is null/string/number/missing-id/missing-type/missing-config so a malformed
 * persisted blob can't crash the renderer downstream.
 */
function isValidWidgetInstance(w: unknown): w is WidgetInstance {
  if (!w || typeof w !== 'object') return false;
  const obj = w as Partial<WidgetInstance>;
  return (
    typeof obj.id === 'string' &&
    typeof obj.type === 'string' &&
    typeof obj.config === 'object' &&
    obj.config !== null
  );
}

/**
 * Run the widget's optional Zod schema over its stored config. Per-field
 * `.catch()` clauses recover individual bad values; a fully malformed config
 * falls back to the widget's `defaultConfig`. Widgets without a schema pass
 * through unchanged.
 *
 * Bootstrapping note: this calls `getWidget(type)`, which only returns a
 * value after `widgets/index.ts` has executed (it's the side-effect import
 * that fills the registry). Production callers reach `migrations.ts` via
 * `useDashboardPrefs`, which is mounted from a Dashboard component that
 * transitively imports `widgets/index`. Tests that import this module
 * directly MUST also `import 'widgets/index'` at the top, otherwise
 * `getWidget` returns undefined and `sanitizeConfig` is silently a no-op.
 */
function sanitizeConfig(w: WidgetInstance): WidgetInstance {
  const def = getWidget(w.type);
  if (!def?.configSchema) return w;
  const parsed = def.configSchema.safeParse(w.config);
  if (parsed.success) return { ...w, config: parsed.data };
  // Worst case: schema parse failed even after per-field .catch() — usually
  // means the stored value was the wrong top-level type entirely. Reset to
  // the widget's default so the user gets a working widget back, with a
  // diagnostic log to find the offender in dev.
  if (import.meta.env?.DEV) {
    console.warn('[dashboard-prefs] sanitizeConfig fell back to defaultConfig for', w.type, parsed.error);
  }
  return { ...w, config: { ...(def.defaultConfig as object) } };
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
  // Filter shape-invalid widgets BEFORE rename + sanitize. A stored blob
  // with `widgets: [null, "garbage", { id: 'x' }]` previously passed
  // straight through and crashed the renderer.
  const validWidgets = rawWidgets.filter(isValidWidgetInstance);
  // Order matters: rename FIRST so sanitize sees the current type and can
  // look up the right widget definition + schema.
  const widgets = renameWidgetTypes(validWidgets).map(sanitizeConfig);
  // Per-breakpoint shape filter: `{ layouts: { lg: { foo: 'bar' } } }` would
  // pass the top-level "object && not array" check but later crash
  // reconcileLayouts (it `.filter()`s each breakpoint as if it were an array).
  // Drop any breakpoint whose value isn't a real array.
  const layouts: Record<string, unknown[]> = {};
  if (src.layouts && typeof src.layouts === 'object' && !Array.isArray(src.layouts)) {
    for (const [bp, items] of Object.entries(src.layouts)) {
      if (Array.isArray(items)) layouts[bp] = items;
    }
  }

  return {
    version: DASHBOARD_PREFS_VERSION,
    mode,
    widgets,
    layouts,
    lastBreakpoint: src.lastBreakpoint,
    history: Array.isArray(src.history) ? src.history : undefined,
  };
}
