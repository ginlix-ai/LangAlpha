import { useCallback, useEffect, useEffectEvent, useRef } from 'react';
import type { RefObject } from 'react';
import {
  advanceWatermark,
  dedupeMergeByTime,
  fetchBarsDelta,
  shouldSkipPollWhileWsHealthy,
} from './chartDataLoaders';
import { DELTA_POLL_CADENCE_MS, INTERVAL_SECONDS } from './chartConstants';
import type { ChartBar, LoaderMeta } from './marketProtocol';
import type { Freshness } from '@/types/market';

/** Currency/watermark surfaced by a delta poll, forwarded to `onMeta`. */
export interface LiveBarsMeta {
  currency?: string;
  displayDecimals?: number;
  watermark?: number | null;
  marketPhase?: string | null;
  nextChangeAt?: number | null;
  freshness?: Freshness | null;
}

/** What a served series is on: a new revision or adjustment basis means the
 *  server rebuilt its history rather than extending it. */
interface SeriesBasis {
  revision?: number;
  priceTreatment?: string;
}

function basisOf(meta: Partial<LoaderMeta>): SeriesBasis {
  return { revision: meta.revision, priceTreatment: meta.priceTreatment };
}

function differs<T>(a: T | undefined, b: T | undefined): boolean {
  return a != null && b != null && a !== b;
}

/** A side that does not say proves nothing: an older backend's load carries no revision. */
function isRebuilt(known: SeriesBasis, served: SeriesBasis): boolean {
  return differs(known.revision, served.revision) || differs(known.priceTreatment, served.priceTreatment);
}

/** *primary* where it says, *fallback* where it does not. */
function fillBasis(primary: SeriesBasis, fallback: SeriesBasis): SeriesBasis {
  return {
    revision: primary.revision ?? fallback.revision,
    priceTreatment: primary.priceTreatment ?? fallback.priceTreatment,
  };
}

export interface UseLiveBarsOptions {
  /** Whether the poll is armed. MarketChart: `effectiveChartMode === 'custom'`; ChartWidget: `true`. */
  enabled: boolean;
  /**
   * Component-owned full-bar array. The hook reads and writes `dataRef.current`
   * in place — it never clones or owns the storage. The component's initial
   * loader and WS fold effect write the same ref.
   */
  dataRef: RefObject<ChartBar[]>;
  /**
   * Wall-clock ms of the last applied WS tick, written by the component's fold
   * effect. The hook reads it to skip polls while a healthy WS feed drives the
   * forming bar.
   */
  lastWsTickRef: RefObject<number>;
  /**
   * Called only when the timeline actually moved — a new bar was appended or the
   * forming head's OHLCV was revised. `dataRef.current` is already updated to
   * `merged`. `headChanged` is true when the newest bar's OHLCV differs from the
   * prior newest bar (a head revision, or an append landing a different bar). The
   * component redraws (full `updateSeriesData` or a surgical head update).
   */
  onBars: (merged: ChartBar[], info: { headChanged: boolean }) => void;
  /**
   * Currency/decimals upgrade from a delta poll. Wire to
   * `useCurrencyDisplay`'s `onCurrencyMeta`.
   */
  onMeta?: (meta: LiveBarsMeta) => void;
  /**
   * Venue market phase (`pre|open|post|closed`) from the loader seed or a
   * delta poll — calendar-derived server-side. Fires only when a payload
   * carries one; drives closed-market presentation (price-line label,
   * header badge).
   */
  onPhase?: (phase: string) => void;
  /**
   * Measured freshness of the served series from the loader seed or a delta
   * poll — which provider filled these bars and how far behind they are.
   * Travels the same path as `onPhase`; drives the header's Chart tooltip line.
   * Null on a symbol or interval change: the last label measured another series.
   */
  onFreshness?: (freshness: Freshness | null) => void;
  /**
   * A poll found the series rebuilt server-side (a new revision or adjustment
   * basis), so the bars on screen and the ones it brought are on different
   * bases and must not be spliced. Reload the full history through the
   * initial loader; polls hold until its `seedMeta`, and a reload reported
   * through `loadFailed` is asked for again on the next tick.
   */
  onRebuilt: () => void;
}

export interface LiveBarsController {
  /**
   * Seed the delta-poll watermark, currency and series basis from the initial
   * loader's metadata. No-op on `undefined`; only overwrites the watermark when
   * the meta carries one, and forwards currency to `onMeta` whenever meta is
   * present.
   */
  seedMeta: (meta: LoaderMeta | LiveBarsMeta | null | undefined) => void;
  /**
   * The initial loader came back without bars, from an error or an empty
   * answer alike: a provider miss mid-rebuild can be either. The reload a
   * rebuild asked for cleared the chart, so the polls would hold for good on
   * a blank one; instead the next tick asks for it again. A no-op when no
   * rebuild is pending.
   */
  loadFailed: () => void;
}

/**
 * Shared live-bars delta-poll controller for the lightweight-charts panels.
 *
 * Every interval polls on a tiered cadence (DELTA_POLL_CADENCE_MS); each poll
 * re-serves the current forming head bar (the server returns it even when its
 * ts == the watermark while the market is open), so the last candle stays live
 * even without a WS feed. While WS drives the forming bar (1min + fold
 * intervals) the poll skips — except the periodic ≤60s reconcile
 * (shouldSkipPollWhileWsHealthy), the authoritative correction for fold volume
 * drift, buckets missed during a tab suspend (WS gap-fill only covers 1min),
 * server-side revisions, and MA/RSI.
 *
 * The hook owns the `watermark` + `lastReconcile` cursors and resets the
 * watermark when the symbol/interval changes; the component owns bar storage
 * (`dataRef`) and the WS tick clock (`lastWsTickRef`).
 */
export function useLiveBars(
  symbol: string,
  interval: string,
  { enabled, dataRef, lastWsTickRef, onBars, onMeta, onPhase, onFreshness, onRebuilt }: UseLiveBarsOptions,
): LiveBarsController {
  // Backend epoch-ms high-water mark for delta polls (`after=`).
  const watermarkRef = useRef<number | null>(null);
  // Wall-clock ms of the last delta poll that actually ran (authoritative REST
  // sync) — drives the periodic reconcile through the WS-healthy skip.
  const lastReconcileRef = useRef(0);
  // Epoch ms of the venue's next phase boundary (server calendar). A one-shot
  // timer polls right past it so presentation flips at the bell, not on the
  // next cadence tick. The arm closure lives on a ref because seedMeta (called
  // from the loader, outside the poll effect) must be able to re-arm it.
  const nextChangeAtRef = useRef<number | null>(null);
  const armBoundaryRef = useRef<(() => void) | null>(null);

  // Symbol/interval as of the last commit, for the post-await staleness
  // re-check (see the poll body).
  const liveSymbol = useEffectEvent(() => symbol);
  const liveInterval = useEffectEvent(() => interval);

  // Callbacks read through effect events and refs so a new closure each render
  // doesn't tear down and re-arm the poll interval. onMeta and onPhase stay on
  // refs: seedMeta calls them from the loader, outside any effect.
  const emitBars = useEffectEvent(onBars);
  const onMetaRef = useRef(onMeta);
  onMetaRef.current = onMeta;
  const onPhaseRef = useRef(onPhase);
  onPhaseRef.current = onPhase;
  const onFreshnessRef = useRef(onFreshness);
  onFreshnessRef.current = onFreshness;
  const emitRebuilt = useEffectEvent(onRebuilt);
  // The basis the bars on screen were served on, and the one a poll found the
  // series rebuilt to while the component reloads; polls hold while it is set.
  const basisRef = useRef<SeriesBasis>({});
  const rebuiltToRef = useRef<SeriesBasis | null>(null);
  // The basis the last reload was sent for, kept until a poll agrees with the
  // screen again.
  const reloadedForRef = useRef<SeriesBasis | null>(null);
  // That reload came back without bars; the next tick sends it again.
  const reloadFailedRef = useRef(false);

  // Reset the delta cursor when the symbol/interval changes — independent of
  // `enabled` so a chart-mode toggle neither strands nor resets it. The initial
  // loader re-seeds via `seedMeta` after its fetch, which always lands
  // post-await (after this synchronous reset).
  useEffect(() => {
    watermarkRef.current = null;
    nextChangeAtRef.current = null;
    basisRef.current = {};
    rebuiltToRef.current = null;
    reloadedForRef.current = null;
    reloadFailedRef.current = false;
    onFreshnessRef.current?.(null);
  }, [symbol, interval]);

  const seedMeta = useCallback((meta: LoaderMeta | LiveBarsMeta | null | undefined) => {
    if (!meta) return;
    if (meta.watermark != null) watermarkRef.current = meta.watermark;
    // The basis is what the bars on screen were loaded on, so the load's own
    // word wins; the basis a poll saw only fills what the load does not say
    // (the legacy route omits a revision the cached series never recorded).
    const seeded = basisOf(meta);
    basisRef.current = rebuiltToRef.current ? fillBasis(seeded, rebuiltToRef.current) : seeded;
    rebuiltToRef.current = null;
    reloadFailedRef.current = false;
    onMetaRef.current?.({
      currency: meta.currency,
      displayDecimals: meta.displayDecimals,
      watermark: meta.watermark,
    });
    if (meta.marketPhase) onPhaseRef.current?.(meta.marketPhase);
    if (meta.freshness) onFreshnessRef.current?.(meta.freshness);
    if (meta.nextChangeAt != null) {
      nextChangeAtRef.current = meta.nextChangeAt;
      armBoundaryRef.current?.();
    }
  }, []);

  const loadFailed = useCallback(() => {
    if (rebuiltToRef.current) reloadFailedRef.current = true;
  }, []);

  useEffect(() => {
    if (!enabled) return;
    let aborted = false;

    const poll = async () => {
      if (aborted) return;
      // The reload after a rebuild owns the next render, so the poll holds
      // while it is out. One that failed left the chart blank, and nothing
      // else would send it again: send it on this tick, ahead of the WS skip,
      // since a feed folding into no bars covers nothing.
      if (rebuiltToRef.current) {
        if (reloadFailedRef.current) {
          reloadFailedRef.current = false;
          emitRebuilt();
        }
        return;
      }
      const now = Date.now();
      // Skip while WS drives the forming bar — except the periodic reconcile
      // (WS_RECONCILE_POLL_MS), which always gets through as the authoritative
      // correction.
      if (shouldSkipPollWhileWsHealthy(lastWsTickRef.current, lastReconcileRef.current, now)) return;
      // The initial loader owns first render; the poll never seeds from an
      // empty series. Bail BEFORE stamping the reconcile so a skipped tick
      // doesn't consume its budget.
      if (dataRef.current.length === 0) return;
      lastReconcileRef.current = now;

      const sym = liveSymbol();
      const iv = liveInterval();
      try {
        // Delta-poll: records newer than the stored watermark (server-side
        // `after=`) PLUS the re-served forming head bar, falling back to a full
        // re-fetch when the protocol endpoint is absent. Merged by bar time.
        const delta = await fetchBarsDelta(sym, iv, watermarkRef.current);
        if (aborted) return;
        // Post-await staleness (belt and braces): the effect re-runs on
        // symbol/interval change (deps) which aborts in-flight polls via the
        // cleanup, AND we re-check the live values here to drop a response that
        // resolved in the window between the render committing a new
        // symbol/interval and that cleanup running.
        if (liveSymbol() !== sym || liveInterval() !== iv) return;

        // A rebuilt series answers `after=` with its new tail only; merged, it
        // would sit next to history on the old basis and corrupt returns and
        // indicators. Hand it to a full reload instead.
        const served = basisOf(delta.meta);
        if (isRebuilt(basisRef.current, served)) {
          // A reload for this very basis came back on another: the loader and
          // the poll read different series. Reloading again would loop and
          // merging would mix the two, so the polls hold until either moves.
          if (reloadedForRef.current && !isRebuilt(reloadedForRef.current, served)) return;
          rebuiltToRef.current = served;
          reloadedForRef.current = served;
          watermarkRef.current = null;
          emitRebuilt();
          return;
        }
        reloadedForRef.current = null;
        basisRef.current = fillBasis(basisRef.current, served);

        // Forward-only against jitter, but a watermark a full bucket older means
        // the server envelope was rebuilt — adopt it or the cursor strands past
        // every server bar (see advanceWatermark).
        watermarkRef.current = advanceWatermark(
          watermarkRef.current, delta.meta.watermark, INTERVAL_SECONDS[iv] ?? 60,
        );
        if (delta.meta.currency) {
          onMetaRef.current?.({
            currency: delta.meta.currency,
            displayDecimals: delta.meta.displayDecimals,
            watermark: delta.meta.watermark,
          });
        }
        if (delta.meta.marketPhase) onPhaseRef.current?.(delta.meta.marketPhase);
        if (delta.meta.freshness) onFreshnessRef.current?.(delta.meta.freshness);
        if (delta.meta.nextChangeAt != null) {
          nextChangeAtRef.current = delta.meta.nextChangeAt;
          armBoundaryRef.current?.();
        }

        const bars = delta.bars;
        if (bars.length === 0) return;

        const current = dataRef.current;
        const prevLast = current[current.length - 1];
        const lastKnown = prevLast?.time ?? 0;
        // time > lastKnown appends a new bar; time == lastKnown is the re-served
        // forming head bar (its OHLCV moved). dedupeMergeByTime prefers the
        // incoming bar on a tie, so it replaces the stale head in place and
        // appends new bars — without trim-and-reappend's failure mode of
        // dropping the head bar when the delta doesn't re-include it.
        const newer = bars.filter((b) => b.time >= lastKnown);
        if (newer.length === 0) return;
        const { merged } = dedupeMergeByTime(current, newer);

        // Redraw only when something actually moved — a forming bar re-served
        // unchanged is common, and skipping the redraw avoids a full setData
        // (and its overlay/annotation recompute) every poll tick.
        const head = merged[merged.length - 1];
        const headChanged = !prevLast ||
          head.open !== prevLast.open || head.high !== prevLast.high ||
          head.low !== prevLast.low || head.close !== prevLast.close ||
          head.volume !== prevLast.volume;
        const changed = merged.length !== current.length || headChanged;
        if (!changed) return;

        dataRef.current = merged;
        emitBars(merged, { headChanged });
      } catch (err) {
        const e = err as { name?: string };
        if (e?.name === 'AbortError' || e?.name === 'CanceledError') return;
        console.debug('[useLiveBars] delta poll failed:', err);
      }
    };

    // Tiered cadence: 1min→15s, 5min–4hour→30s, 1day→60s.
    const pollMs = DELTA_POLL_CADENCE_MS[interval] ?? 30000;
    const timer = setInterval(poll, pollMs);

    // One-shot poll just past the venue's next phase boundary, so the phase
    // (and everything derived from it) flips at the bell instead of lagging a
    // cadence tick. Bypasses the WS-healthy skip like the visibility handler.
    let boundaryTimer: ReturnType<typeof setTimeout> | null = null;
    const armBoundary = () => {
      if (boundaryTimer != null) clearTimeout(boundaryTimer);
      boundaryTimer = null;
      const at = nextChangeAtRef.current;
      if (at == null) return;
      const delay = at - Date.now() + 1_000; // land 1s past the boundary
      // Stale boundary → the cadence covers it. Far boundary (>36h, e.g. over
      // a weekend) → don't hold a long timer; a later poll re-arms anyway.
      if (delay <= 0 || delay > 36 * 3_600_000) return;
      boundaryTimer = setTimeout(() => {
        lastReconcileRef.current = 0;
        poll();
      }, delay);
    };
    armBoundaryRef.current = armBoundary;
    armBoundary();

    // A tab returning from suspension polls immediately: WS resumes ticking
    // right away (masking the hole from the skip check), and waiting out the
    // reconcile window would leave missed buckets on screen for up to a minute.
    const onVisibility = () => {
      if (document.visibilityState !== 'visible') return;
      lastReconcileRef.current = 0;
      poll();
    };
    document.addEventListener('visibilitychange', onVisibility);

    return () => {
      aborted = true;
      clearInterval(timer);
      if (boundaryTimer != null) clearTimeout(boundaryTimer);
      armBoundaryRef.current = null;
      document.removeEventListener('visibilitychange', onVisibility);
    };
  }, [symbol, interval, enabled, dataRef, lastWsTickRef]);

  return { seedMeta, loadFailed };
}
