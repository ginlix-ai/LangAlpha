import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { usePreferences } from '@/hooks/usePreferences';
import { useToast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import { BROADCAST_CHANNEL, useDashboardPrefsWriter } from './dashboardPrefsWriter';
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
  const { writeDashboardPrefs, isPending } = useDashboardPrefsWriter();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const raw = readDashboardPrefs(preferences);
  const stored = useMemo<DashboardPrefs>(() => migrateDashboardPrefs(raw) ?? emptyPrefs(), [raw]);

  const [local, setLocal] = useState<DashboardPrefs>(stored);
  const storedRef = useRef<DashboardPrefs>(stored);
  const ownWriteInFlightRef = useRef(false);

  useEffect(() => {
    storedRef.current = stored;
    if (ownWriteInFlightRef.current) {
      ownWriteInFlightRef.current = false;
      return;
    }
    setLocal(stored);
  }, [stored]);

  const pendingTimer = useRef<number | null>(null);
  const bcRef = useRef<BroadcastChannel | null>(null);
  // isPending changes mid-effect-cycle; mirror via ref so the
  // BroadcastChannel onmessage handler reads the latest value without
  // tearing down + rebuilding the channel on every mutation transition.
  const isMutatingRef = useRef(false);
  isMutatingRef.current = isPending;
  // Deferred-replay: a broadcast that arrives during a pending edit can't be
  // applied immediately (refetching mid-edit would race the response). Set
  // this flag instead and run the invalidate after the current edit settles
  // so cross-tab changes still land — they just wait their turn.
  const replayPendingRef = useRef(false);

  const runReplay = useCallback(() => {
    if (!replayPendingRef.current) return;
    replayPendingRef.current = false;
    queryClient.invalidateQueries({ queryKey: queryKeys.user.preferences() });
  }, [queryClient]);

  // After an in-flight mutation finishes, drain any deferred broadcast.
  useEffect(() => {
    if (!isPending) runReplay();
  }, [isPending, runReplay]);

  // Cross-tab notification: broadcast on flush success so other tabs invalidate
  // their preferences cache and pull the fresh write. Falls back silently in
  // browsers without BroadcastChannel (Safari < 15.4) — staleTime: 0 +
  // refetchOnWindowFocus already covers the alt-tab case for those.
  useEffect(() => {
    if (typeof BroadcastChannel === 'undefined') return;
    const chan = new BroadcastChannel(BROADCAST_CHANNEL);
    bcRef.current = chan;
    chan.onmessage = (e: MessageEvent) => {
      if ((e.data as { type?: string } | null)?.type !== 'updated') return;
      if (pendingTimer.current === null && !isMutatingRef.current) {
        queryClient.invalidateQueries({ queryKey: queryKeys.user.preferences() });
      } else {
        // Defer until the current edit settles — runs from either the flush
        // timer's onSuccess/onError or the isPending effect above.
        replayPendingRef.current = true;
      }
    };
    return () => {
      chan.close();
      bcRef.current = null;
    };
  }, [queryClient]);

  const flush = useCallback(
    (next: DashboardPrefs) => {
      ownWriteInFlightRef.current = true;
      // Dev-only size trap: catches widget configs that bloat the prefs
      // blob before we ship them. The prefs are PATCHed as one payload,
      // so a widget with a 50-symbol array full of objects can balloon
      // the blob quickly. 20KB is generous for normal use.
      if (import.meta.env?.DEV) {
        try {
          const bytes = new Blob([JSON.stringify(next)]).size;
          if (bytes > 20_000) {
            console.warn(
              `[dashboard-prefs] blob ${bytes}B exceeds 20KB dev trap — check widget configs`,
              next,
            );
          }
        } catch {
          /* ignore sizing errors in dev */
        }
      }
      const accepted = writeDashboardPrefs(next, {
        // undefined = cold (writer refuses); null = warm w/ empty siblings.
        fallbackOther: preferences === null
          ? undefined
          : ((preferences as { other_preference?: Record<string, unknown> }).other_preference ?? null),
        onSuccess: runReplay,
        onError: () => {
          // Server rejected the write. Clear the sync guard so the invalidate
          // → refetch in useUpdatePreferences can reconcile local state back to
          // the server copy, and tell the user their change didn't stick.
          ownWriteInFlightRef.current = false;
          toast({
            variant: 'destructive',
            title: 'Couldn’t save dashboard',
            description: 'Your latest change didn’t sync. We restored the last saved layout.',
          });
          runReplay();
        },
      });
      if (!accepted) {
        // Cold-cache refusal — undo the sync-skip guard so the next render
        // can reconcile local back to the server copy when the GET resolves.
        ownWriteInFlightRef.current = false;
      }
    },
    [writeDashboardPrefs, preferences, runReplay, toast]
  );

  const update = useCallback(
    (patch: Partial<DashboardPrefs> | ((prev: DashboardPrefs) => DashboardPrefs), opts?: { immediate?: boolean }) => {
      // Cold-cache gate: drop edits before the initial GET resolves so we
      // don't construct a payload from `{}` and clobber server-side
      // sibling other_preference keys (theme, locale, etc.).
      if (isLoading) return;
      setLocal((prev) => {
        const next = typeof patch === 'function' ? patch(prev) : { ...prev, ...patch };
        if (pendingTimer.current) window.clearTimeout(pendingTimer.current);
        if (opts?.immediate) {
          // Treat immediate writes as "no debounce queued" so a follow-up
          // cross-tab broadcast can invalidate normally.
          pendingTimer.current = null;
          flush(next);
        } else {
          // Reset the timer ref to null AFTER flush so the cross-tab onmessage
          // handler sees an empty queue and runs the invalidate path. Without
          // this reset the gate stays closed forever after the first edit.
          pendingTimer.current = window.setTimeout(() => {
            flush(next);
            pendingTimer.current = null;
          }, DEBOUNCE_MS);
        }
        return next;
      });
    },
    [flush, isLoading]
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
