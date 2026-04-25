import { useCallback } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useIsMobile } from '@/hooks/useIsMobile';
import { usePreferences } from '@/hooks/usePreferences';
import { queryKeys } from '@/lib/queryKeys';
import type { UserPreferences } from '@/types/api';
import Dashboard from './Dashboard';
import DashboardCustom from './DashboardCustom';
import NetworkBanner from './components/NetworkBanner';
import { useDashboardPrefsWriter } from './widgets/framework/dashboardPrefsWriter';
import { migrateDashboardPrefs } from './widgets/framework/migrations';
import { getPreset } from './widgets/presets';
import { DASHBOARD_PREFS_VERSION, type DashboardPrefs } from './widgets/types';

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
  const { preferences, isLoading } = usePreferences();
  const { writeDashboardPrefs } = useDashboardPrefsWriter();
  const queryClient = useQueryClient();

  const rawOther = (preferences as { other_preference?: { dashboard?: unknown } } | null)
    ?.other_preference;
  const parsed: DashboardPrefs | null = migrateDashboardPrefs(rawOther?.dashboard ?? null);
  const mode: 'classic' | 'custom' = parsed?.mode ?? 'classic';

  const onModeChange = useCallback(
    (next: 'classic' | 'custom') => {
      // Cold-cache gate: refuse the toggle until prefs load so we don't PUT
      // `{ other_preference: { dashboard: {...} } }` and clobber sibling
      // server-side keys (theme, locale). The toggle is disabled in the UI
      // while isLoading, so this branch is defense-in-depth.
      if (isLoading) return;
      // Replay-aware: re-read the freshest cache so a cross-tab edit (or
      // pending debounce in this tab) that already updated the dashboard
      // sub-object isn't replaced with the render-time snapshot. Without
      // this, `firstFlipToCustom` could mis-trigger the morning-brief seed
      // because `parsed` (render-time) saw an empty widget list while the
      // cache already has widgets. The writer also reads cache for sibling
      // preservation; this read is for the dashboard sub-object only.
      const fresh = queryClient.getQueryData<UserPreferences>(queryKeys.user.preferences());
      const freshOther = (fresh?.other_preference as Record<string, unknown> | undefined) ?? rawOther;
      const freshDashboardRaw = (freshOther?.dashboard as unknown) ?? null;
      const baseDashboard: Partial<DashboardPrefs> =
        migrateDashboardPrefs(freshDashboardRaw) ?? parsed ?? {};
      const firstFlipToCustom = next === 'custom' && (!baseDashboard.widgets || baseDashboard.widgets.length === 0);
      const seed = firstFlipToCustom ? getPreset('morning-brief') : null;
      const dashboard: DashboardPrefs = {
        version: DASHBOARD_PREFS_VERSION,
        mode: next,
        widgets: seed ? seed.widgets : (baseDashboard.widgets ?? []),
        layouts: seed ? seed.layouts : (baseDashboard.layouts ?? {}),
        lastBreakpoint: baseDashboard.lastBreakpoint,
        history: baseDashboard.history,
      };
      writeDashboardPrefs(dashboard, {
        // undefined = cold (writer refuses); null = warm w/ empty siblings.
        fallbackOther: preferences === null
          ? undefined
          : ((rawOther as Record<string, unknown> | undefined) ?? null),
      });
    },
    [writeDashboardPrefs, parsed, preferences, rawOther, isLoading, queryClient]
  );

  if (isMobile) {
    // Mobile: Classic always. Toggle is not surfaced. Banner still mounts so
    // tablet/phone users get the offline warning too — TV iframes silently
    // serve stale data on mobile just like desktop.
    return (
      <>
        <NetworkBanner />
        <Dashboard />
      </>
    );
  }

  if (mode === 'custom') {
    return (
      <>
        <NetworkBanner />
        <DashboardCustom mode={mode} onModeChange={onModeChange} />
      </>
    );
  }

  return (
    <>
      <NetworkBanner />
      <Dashboard layoutToggle={{ mode, onModeChange }} />
    </>
  );
}
