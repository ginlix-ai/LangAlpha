import { useCallback, useEffect, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useFeatureEnabled } from '@/hooks/useFeatures';
import { onPageReturn } from '@/lib/pageVisibility';
import { queryKeys } from '@/lib/queryKeys';
import type { WakeupList, WakeupsResponse } from '@/types/wakeups';
import { cancelWakeups, fetchWakeups } from '../utils/api';
import { isValidUuid } from '../utils/uuid';
import { patchListingAfterMutation } from './utils/patchListing';
import { nextWakeupLapse, wakeupsAt } from '../components/wakeupLabels';

// setTimeout keeps its delay in a signed 32-bit count and fires at once past
// it; a later lapse is re-armed from the wake this cap ends in.
const MAX_TIMER_MS = 2 ** 31 - 1;

/** The wall clock as a render input that moves only when a price or order
 *  wakeup lapses, the one instant what reads armed changes. */
function useLapseClock(list: WakeupList | null): number {
  const [now, setNow] = useState(Date.now);
  const next = nextWakeupLapse(list?.items, now);
  useEffect(() => {
    if (next === null) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // Checked when it fires rather than trusted: a timer can fire early, and
    // one capped short of a far lapse ends early by design.
    const wake = () => {
      const at = Date.now();
      if (at >= next) setNow(at);
      else timer = setTimeout(wake, Math.min(next - at, MAX_TIMER_MS));
    };
    timer = setTimeout(wake, Math.min(Math.max(next - Date.now(), 0), MAX_TIMER_MS));
    // A machine asleep holds timers back, so a page shown again checks at once.
    const offReturn = onPageReturn(() => {
      clearTimeout(timer);
      wake();
    });
    return () => {
      clearTimeout(timer);
      offReturn();
    };
  }, [next]);
  return now;
}

// The sweep fires a due wakeup within 30 s. A fire that starts no turn this
// view follows (held for usage, parked behind a turn elsewhere) sends it no
// event, so the listing is read again once each fire should have settled,
// and once more for a slow one: twice per fire, never in a loop.
const FIRE_SETTLE_MS = [45_000, 180_000];

/** When to read the listing again for a due fire it may not show yet:
 *  the first settle point of an armed wakeup's next fire after `readAt`. */
function nextFireRead(list: WakeupList | null, readAt: number): number | null {
  let next: number | null = null;
  for (const w of list?.items ?? []) {
    if (w.state !== 'armed' || !w.next_fire_at) continue;
    const due = Date.parse(w.next_fire_at);
    if (Number.isNaN(due)) continue;
    const at = FIRE_SETTLE_MS.map((ms) => due + ms).find((ms) => ms > readAt);
    if (at !== undefined && (next === null || at < next)) next = at;
  }
  return next;
}

/**
 * The thread's wakeups, for the tray chip, the Status panel and the tab that
 * follows them, each reading the one cached listing. A wakeup's state changes
 * only on the server, so the list is never stale for long: every read starts
 * fresh, a page shown again reads once (a wakeup can fire while no turn
 * runs), a due fire reads it again once the fire should have settled, and
 * what the stream sees move says so through `invalidateThreadWakeups`.
 *
 * `active` gates the reads only. A chat kept warm behind another one goes on
 * showing what it had, so the Status tab's follow sees no change on its
 * return, and reads afresh once it is in front again. Null with both features
 * off, for an id that is not a thread yet, and with wakeups off once the
 * thread has no price target from before them left.
 */
export function useThreadWakeups(threadId: string | null | undefined, active = true): WakeupList | null {
  const wakeupsOn = useFeatureEnabled('wakeups');
  // With wakeups off the server still lists the thread's price targets from
  // before them, which keep running until they drain and only the panel can
  // cancel, so the list is read for either flag and holds only those rows
  // when wakeups are off.
  const legacyOn = useFeatureEnabled('market_watch');
  const thread = isValidUuid(threadId) ? threadId : null;
  const readable = thread !== null && (wakeupsOn || legacyOn);
  const select = useCallback((data: WakeupsResponse): WakeupList => ({
    items: wakeupsOn ? data.wakeups : data.wakeups.filter((w) => w.legacy),
    userTimezone: data.user_timezone,
  }), [wakeupsOn]);
  const { data, dataUpdatedAt, refetch } = useQuery({
    queryKey: queryKeys.threads.wakeups(thread ?? ''),
    queryFn: () => fetchWakeups(thread as string),
    enabled: readable && active,
    staleTime: 0,
    // Every open chat reads it, and the next event or page return reads it
    // again, so a failure leaves what was shown rather than retrying.
    retry: false,
    select,
  });
  // With wakeups off the panel has nothing to say beyond legacy rows.
  const list = readable && data && (wakeupsOn || data.items.length > 0) ? data : null;

  const readAgainAt = active ? nextFireRead(list, dataUpdatedAt) : null;
  useEffect(() => {
    if (readAgainAt === null) return;
    const timer = setTimeout(() => void refetch(), Math.min(Math.max(readAgainAt - Date.now(), 0), MAX_TIMER_MS));
    return () => clearTimeout(timer);
  }, [readAgainAt, refetch]);

  // A wakeup past its window reads lapsed before the server's sweep settles it.
  const now = useLapseClock(list);
  return useMemo(() => {
    if (!list) return null;
    const items = wakeupsAt(list.items, now);
    return items === list.items ? list : { ...list, items };
  }, [list, now]);
}

/** Re-read the thread's wakeups wherever they are shown, after something the
 *  view saw may have moved one: a delivery, a `schedule_wakeup` result, a
 *  reload that caught up on turns. */
export function invalidateThreadWakeups(queryClient: QueryClient, threadId: string | null | undefined): void {
  if (!isValidUuid(threadId)) return;
  void queryClient.invalidateQueries({ queryKey: queryKeys.threads.wakeups(threadId) });
}

/**
 * Cancels the named wakeups, or every one with `'all'`, and shows the listing
 * the answer carries, which keeps a `too_late` one open, or reads it again when
 * the answer has none. Rejects when the request fails, for the caller to say so.
 */
export function useCancelWakeups(threadId: string | null | undefined) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (which: readonly string[] | 'all') => {
      if (!isValidUuid(threadId)) return Promise.reject(new Error('No thread to cancel wakeups in'));
      return cancelWakeups(threadId, which);
    },
    // The answer has no zone, so the cached one is kept.
    onSuccess: ({ wakeups }) => patchListingAfterMutation<WakeupsResponse>(
      queryClient,
      queryKeys.threads.wakeups(threadId ?? ''),
      wakeups ? (prev) => ({ ...prev, wakeups }) : null,
    ),
  });
}
