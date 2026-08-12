import { useCallback, useEffect, useRef, useState } from 'react';
import type { Dispatch, MutableRefObject, SetStateAction } from 'react';
import { useFeatureEnabled } from '@/hooks/useFeatures';
import { fetchMarketWatch } from '../utils/api';
import { coerceSymbols, type MarketWatchState } from './utils/streamEventHandlers';

/**
 * Owns the persistent market-watch chip lifecycle for a thread:
 *   (a) seed / re-seed from GET /market-watch on thread load or switch,
 *   (c) refetch once when a streaming turn completes.
 * Live mid-turn overwrites (b) arrive via `market_watch_update` SSE events —
 * the caller forwards the returned `setMarketWatch` into `handleMarketWatchUpdate`.
 *
 * `threadIdRef` (the caller's latest-thread ref) is read by (c) so the refetch
 * targets the real thread id even when the `threadId` prop is still `__default__`.
 */
export function useMarketWatch(
  threadId: string,
  isLoading: boolean,
  threadIdRef: MutableRefObject<string>,
): {
  marketWatch: MarketWatchState | null;
  setMarketWatch: Dispatch<SetStateAction<MarketWatchState | null>>;
} {
  // Latest market-watch snapshot for this thread — seeded from the GET
  // /market-watch endpoint on thread load, overwritten live by
  // `market_watch_update` SSE events, and refetched on turn completion. Drives
  // the persistent "Watching …" chip (null/empty = watch off).
  const [marketWatch, setMarketWatch] = useState<MarketWatchState | null>(null);

  // Feature-gated: when off, skip both seed + refetch so no market-watch reads
  // fire. The chip self-hides on empty symbols (state stays null).
  const marketWatchEnabled = useFeatureEnabled('market_watch');

  // --- Market watch chip: seed (a) + turn-completion refetch (c) -----------
  // Live SSE overwrites (b) land in processEvent's `market_watch_update` case.
  const applyMarketWatchList = useCallback((data: { symbols?: unknown } | null) => {
    const symbols = coerceSymbols(data?.symbols);
    // Merge, don't replace: the turn-completion refetch (c) only knows the
    // symbol list, so it must preserve the `content`/`timestamp` that a live
    // `market_watch_update` (b) just streamed — otherwise the Status panel
    // blanks at turn end. Empty symbols still means "watch off" (null).
    setMarketWatch((prev) => (symbols.length ? { ...prev, symbols } : null));
  }, []);

  // (a) Seed / re-seed from the thread's Redis watch list on load or switch so
  //     the chip survives a page reload and reflects the true watch state.
  useEffect(() => {
    // Clear eagerly on every thread change — an in-place real-thread switch
    // must not flash the previous thread's symbols while the fetch is in
    // flight. The chip reappears when the new thread's list arrives.
    setMarketWatch(null);
    if (!threadId || threadId === '__default__' || !marketWatchEnabled) return;
    let cancelled = false;
    fetchMarketWatch(threadId)
      .then((data) => { if (!cancelled) applyMarketWatchList(data); })
      .catch(() => { /* best-effort — leave prior state untouched */ });
    return () => { cancelled = true; };
  }, [threadId, applyMarketWatchList, marketWatchEnabled]);

  // (c) Refetch once when a streaming turn completes (isLoading true -> false).
  //     Makes the chip disappear promptly after a `watch_market` unwatch and
  //     appear after a watch that never produced a live stamp (market closed).
  const prevIsLoadingRef = useRef(isLoading);
  useEffect(() => {
    const wasLoading = prevIsLoadingRef.current;
    prevIsLoadingRef.current = isLoading;
    if (!wasLoading || isLoading || !marketWatchEnabled) return;
    const tid = threadIdRef.current;
    if (!tid || tid === '__default__') return;
    let cancelled = false;
    fetchMarketWatch(tid)
      .then((data) => { if (!cancelled) applyMarketWatchList(data); })
      .catch(() => { /* best-effort */ });
    return () => { cancelled = true; };
    // threadIdRef is a stable ref (identity never changes) — listed only to
    // satisfy exhaustive-deps; the effect still fires solely on isLoading flips.
  }, [isLoading, applyMarketWatchList, threadIdRef, marketWatchEnabled]);

  return { marketWatch, setMarketWatch };
}
