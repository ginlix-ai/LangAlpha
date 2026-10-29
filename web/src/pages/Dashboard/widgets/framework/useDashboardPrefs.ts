import { useCallback, useEffect, useEffectEvent, useMemo, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { usePreferences } from '@/hooks/usePreferences';
import { PREFERENCE_MUTATION_KEY } from '@/hooks/useUpdatePreferences';
import { useToast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import { BROADCAST_CHANNEL, useDashboardPrefsWriter } from './dashboardPrefsWriter';
import { migrateDashboardPrefs } from './migrations';
import { defaultLayout, getPreset, type PresetId } from '../presets';
import type { DashboardPrefs } from '../types';

const HISTORY_CAP = 3;
const DEBOUNCE_MS = 800;


/** Dashboard prefs live inside `other_preference.dashboard` to fit the backend's 4-column schema. */
function readDashboardPrefs(preferences: unknown): Partial<DashboardPrefs> | null {
  const prefs = preferences as { other_preference?: { dashboard?: Partial<DashboardPrefs> } } | null;
  return prefs?.other_preference?.dashboard ?? null;
}

/** Whether a preferences write carries the dashboard, as its variables show. */
function isDashboardWrite(variables: unknown): boolean {
  return readDashboardPrefs(variables) !== null;
}

export function useDashboardPrefs() {
  const { preferences, isLoading, isLoaded } = usePreferences();
  const { writeDashboardPrefs, isPending } = useDashboardPrefsWriter();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const raw = readDashboardPrefs(preferences);
  const saved = useMemo(() => migrateDashboardPrefs(raw), [raw]);
  const stored = useMemo<DashboardPrefs>(() => saved ?? defaultLayout(), [saved]);

  const [local, setLocal] = useState<DashboardPrefs>(stored);
  const storedRef = useRef<DashboardPrefs>(stored);
  const pendingTimer = useRef<number | null>(null);
  // An adjustment made while a write is saving, kept on the page until that
  // write lands (see `update`).
  const heldAdjust = useRef(false);
  const bcRef = useRef<BroadcastChannel | null>(null);
  // Whether a dashboard write is saving. The observer's `isPending` reaches a
  // render a beat after the write starts (TanStack batches its notifications),
  // and the grid can adjust a layout in between; the mutation cache knows at
  // once. Only dashboard writes count: another preference saving says nothing
  // about this layout, and holding for it would save a default only viewed.
  const writing = useCallback(
    () =>
      isPending ||
      queryClient.isMutating({
        mutationKey: PREFERENCE_MUTATION_KEY,
        predicate: (mutation) => isDashboardWrite(mutation.state.variables),
      }) > 0,
    [isPending, queryClient],
  );
  // Read by the BroadcastChannel handler through an effect event, so it sees
  // the latest answer without the channel being rebuilt as writes come and go.
  const writingNow = useEffectEvent(() => writing());

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
      if (pendingTimer.current === null && !writingNow()) {
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
      writeDashboardPrefs(next, {
        // undefined = cold (writer refuses); null = warm, no saved prefs yet.
        fallbackOther: preferences === null
          ? undefined
          : ((preferences as { other_preference?: Record<string, unknown> }).other_preference ?? null),
        onSuccess: runReplay,
        onError: () => {
          // Server rejected the write. The refetch after it settles brings the
          // server copy back over local; tell the user their change didn't stick.
          // An adjustment held for it goes too: the grid makes it again on
          // the layout it is given back.
          heldAdjust.current = false;
          toast({
            variant: 'destructive',
            title: 'Couldn’t save dashboard',
            description: 'Your latest change didn’t sync. We restored the last saved layout.',
          });
          runReplay();
        },
      });
    },
    [writeDashboardPrefs, preferences, runReplay, toast]
  );

  // While an edit waits on its debounce or its write, local is ahead of the
  // server copy, and every write echoes back twice (the optimistic patch, then
  // the refetch after it settles). Adopting an echo then would roll back an
  // edit made since: a fitted widget's height snapped back to the height an
  // earlier write carried. Once nothing is queued, the next copy is current,
  // and it is taken again as a write settles: a rejected one rolls the cache
  // back while it still counts as pending, and its refetch, holding the same
  // copy, need not change `stored` again.
  const saveLocal = useEffectEvent(() => flush(local));
  useEffect(() => {
    storedRef.current = stored;
    if (pendingTimer.current !== null || writing()) return;
    // A held adjustment still standing means the write landed (a rejected one
    // drops it), so it is saved now, on top of that write.
    if (heldAdjust.current) {
      heldAdjust.current = false;
      saveLocal();
      return;
    }
    setLocal(stored);
  }, [stored, writing]);

  /**
   * `adjust` marks a change the page makes on its own (a layout clamped to
   * its widgets' bounds, a cell fitted to its content) rather than one the
   * reader made. On a saved layout it saves like any other; on the unsaved
   * default it stays on the page, so viewing the default never writes it, and
   * the reader's first edit saves it whole.
   */
  const update = useCallback(
    (
      patch: Partial<DashboardPrefs> | ((prev: DashboardPrefs) => DashboardPrefs),
      opts?: { immediate?: boolean; adjust?: boolean },
    ) => {
      const apply = (prev: DashboardPrefs) => (typeof patch === 'function' ? patch(prev) : { ...prev, ...patch });
      // Nothing is saved until the saved copy has been read: the page shows
      // the default while it loads and after a read that failed, and a write
      // built on that would replace the saved dashboard once a later read
      // lands (the server replaces the dashboard key wholesale). The page
      // still answers (a widget's tab switches), and the saved copy replaces
      // it when a read lands.
      if (!isLoaded) {
        setLocal(apply);
        return;
      }
      // An adjustment joins an edit waiting to save. With none waiting, it
      // is not saved on its own while a write is saving: it waits for that
      // write and is saved if it lands, so a rejected write is never sent
      // again under it. On the unsaved default it stays on the page.
      if (opts?.adjust && pendingTimer.current === null) {
        const held = writing();
        if (held) heldAdjust.current = true;
        if (held || !saved) {
          setLocal(apply);
          return;
        }
      }
      // What is saved next is the page as it stands, held adjustment included.
      heldAdjust.current = false;
      setLocal((prev) => {
        const next = apply(prev);
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
    [flush, isLoaded, saved, writing]
  );

  // An edit still waiting on its debounce is saved when the page is left, not
  // dropped: a switch to another page unmounts this one, and the write goes
  // on without it. The page is the edit at that point, as nothing else
  // changes it while one waits.
  useEffect(
    () => () => {
      if (pendingTimer.current === null) return;
      window.clearTimeout(pendingTimer.current);
      pendingTimer.current = null;
      saveLocal();
    },
    []
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
    /** Whether the saved copy has been read, so an edit can be saved. */
    editable: isLoaded,
    update,
    applyPreset,
    resetToDefault,
  };
}
