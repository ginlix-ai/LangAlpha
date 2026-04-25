import { useCallback } from 'react';
import { useIsMobile } from '@/hooks/useIsMobile';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import Dashboard from './Dashboard';
import DashboardCustom from './DashboardCustom';
import { migrateDashboardPrefs } from './widgets/framework/migrations';
import { getPreset } from './widgets/presets';
import type { DashboardPrefs } from './widgets/types';

// Side-effect: ensure widget registry is populated before any preset factory runs.
import './widgets/index';

/**
 * Thin dispatcher between the untouched Classic Dashboard and the new Custom widget dashboard.
 *
 * Rules:
 * - Mobile (`<md`) ALWAYS renders Classic. The toggle is hidden.
 * - On desktop, prefs.dashboard.mode decides. Absent/legacy prefs → Classic (zero-regression).
 */
export default function DashboardRouter() {
  const isMobile = useIsMobile();
  const { preferences } = usePreferences();
  const updatePrefs = useUpdatePreferences();

  const rawOther = (preferences as { other_preference?: { dashboard?: unknown } } | null)
    ?.other_preference;
  const parsed: DashboardPrefs | null = migrateDashboardPrefs(rawOther?.dashboard ?? null);
  const mode: 'classic' | 'custom' = parsed?.mode ?? 'classic';

  const onModeChange = useCallback(
    (next: 'classic' | 'custom') => {
      const prevOther = (rawOther ?? {}) as Record<string, unknown>;
      // Spread the migrated/normalized form (parsed) instead of the raw
      // stored blob so malformed legacy keys can't round-trip back into
      // the write. parsed comes from migrateDashboardPrefs which coerces
      // bad widgets/layouts/history values into safe defaults.
      const baseDashboard = parsed ?? ({} as Partial<DashboardPrefs>);
      const firstFlipToCustom = next === 'custom' && (!baseDashboard.widgets || baseDashboard.widgets.length === 0);
      const seed = firstFlipToCustom ? getPreset('morning-brief') : null;
      updatePrefs.mutate({
        other_preference: {
          ...prevOther,
          dashboard: {
            ...baseDashboard,
            version: 1,
            mode: next,
            ...(seed ? { widgets: seed.widgets, layouts: seed.layouts } : {}),
          },
        },
      });
    },
    [rawOther, parsed, updatePrefs]
  );

  if (isMobile) {
    // Mobile: Classic always. Toggle is not surfaced.
    return <Dashboard />;
  }

  if (mode === 'custom') {
    return <DashboardCustom mode={mode} onModeChange={onModeChange} />;
  }

  return <Dashboard layoutToggle={{ mode, onModeChange }} />;
}
