import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useToast } from '@/components/ui/use-toast';
import { migrateDashboardPrefs } from './migrations';
import { getPreset, type PresetId } from '../presets';
import { DASHBOARD_PREFS_VERSION, type DashboardPrefs } from '../types';

const HISTORY_CAP = 3;
const DEBOUNCE_MS = 800;

function emptyPrefs(): DashboardPrefs {
  return {
    version: DASHBOARD_PREFS_VERSION,
    mode: 'classic',
    widgets: [],
    layouts: {},
  };
}

/** Dashboard prefs live inside `other_preference.dashboard` to fit the backend's 4-column schema. */
function readDashboardPrefs(preferences: unknown): Partial<DashboardPrefs> | null {
  const prefs = preferences as { other_preference?: { dashboard?: Partial<DashboardPrefs> } } | null;
  return prefs?.other_preference?.dashboard ?? null;
}

export function useDashboardPrefs() {
  const { preferences, isLoading } = usePreferences();
  const updatePrefs = useUpdatePreferences();
  const { toast } = useToast();

  const raw = readDashboardPrefs(preferences);
  const stored = useMemo<DashboardPrefs>(() => migrateDashboardPrefs(raw) ?? emptyPrefs(), [raw]);

  const [local, setLocal] = useState<DashboardPrefs>(stored);
  const storedRef = useRef<DashboardPrefs>(stored);
  const skipNextSyncRef = useRef(false);

  // Ref-backed so the debounced flush always reads the current `other_preference`
  // snapshot at fire time. Without this, a concurrent write to another preference
  // key (theme, language) during the 800ms debounce would be overwritten when
  // the flush spreads a stale snapshot.
  const preferencesRef = useRef(preferences);
  preferencesRef.current = preferences;

  useEffect(() => {
    storedRef.current = stored;
    if (skipNextSyncRef.current) {
      skipNextSyncRef.current = false;
      return;
    }
    setLocal(stored);
  }, [stored]);

  const pendingTimer = useRef<number | null>(null);

  const flush = useCallback(
    (next: DashboardPrefs) => {
      skipNextSyncRef.current = true;
      const prevOther = ((preferencesRef.current as { other_preference?: Record<string, unknown> } | null | undefined)
        ?.other_preference) ?? {};
      updatePrefs.mutate(
        {
          other_preference: { ...prevOther, dashboard: next },
        },
        {
          onError: () => {
            // Server rejected the write. Clear the sync guard so the invalidate
            // → refetch in useUpdatePreferences can reconcile local state back to
            // the server copy, and tell the user their change didn't stick.
            skipNextSyncRef.current = false;
            toast({
              variant: 'destructive',
              title: 'Couldn’t save dashboard',
              description: 'Your latest change didn’t sync. We restored the last saved layout.',
            });
          },
        }
      );
    },
    [updatePrefs, toast]
  );

  const update = useCallback(
    (patch: Partial<DashboardPrefs> | ((prev: DashboardPrefs) => DashboardPrefs), opts?: { immediate?: boolean }) => {
      setLocal((prev) => {
        const next = typeof patch === 'function' ? patch(prev) : { ...prev, ...patch };
        if (pendingTimer.current) window.clearTimeout(pendingTimer.current);
        if (opts?.immediate) {
          flush(next);
        } else {
          pendingTimer.current = window.setTimeout(() => flush(next), DEBOUNCE_MS);
        }
        return next;
      });
    },
    [flush]
  );

  useEffect(
    () => () => {
      if (pendingTimer.current) window.clearTimeout(pendingTimer.current);
    },
    []
  );

  const setMode = useCallback(
    (mode: 'classic' | 'custom') => {
      update((prev) => {
        // First flip to custom with no widgets → seed Morning Brief
        if (mode === 'custom' && prev.widgets.length === 0) {
          const preset = getPreset('morning-brief');
          return { ...prev, mode, widgets: preset.widgets, layouts: preset.layouts };
        }
        return { ...prev, mode };
      }, { immediate: true });
    },
    [update]
  );

  const applyPreset = useCallback(
    (presetId: PresetId) => {
      const preset = getPreset(presetId);
      update((prev) => {
        const history = [
          { widgets: prev.widgets, layouts: prev.layouts },
          ...(prev.history ?? []),
        ].slice(0, HISTORY_CAP);
        return {
          ...prev,
          mode: 'custom',
          widgets: preset.widgets,
          layouts: preset.layouts,
          history,
        };
      }, { immediate: true });
    },
    [update]
  );

  const resetToDefault = useCallback(() => {
    applyPreset('morning-brief');
  }, [applyPreset]);

  return {
    prefs: local,
    stored: storedRef.current,
    isLoading,
    setMode,
    update,
    applyPreset,
    resetToDefault,
  };
}
