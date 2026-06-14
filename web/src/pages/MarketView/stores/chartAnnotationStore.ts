/**
 * Chart annotation store — module-level store keyed by chart instance.
 *
 * A chart instance is ``(workspace_id, chart_id)`` where
 * ``chart_id = "{SYMBOL}:{timeframe}"`` — the same identity the backend and
 * the agent use. A line drawn on ``NVDA:1day`` does not appear on
 * ``NVDA:1hour``; a different workspace has its own charts.
 *
 * Annotations flow into the store from two sources:
 * 1. SSE `artifact` events with `artifact_type === 'chart_annotation'` —
 *    these arrive live during a chat turn and carry `workspace_id` +
 *    `chart_id` in the payload.
 * 2. The persistence fetcher (`useChartAnnotationSync`) that pulls
 *    `GET /api/v1/workspaces/{workspace_id}/chart-annotations?symbol=X`
 *    on mount and symbol change.
 *
 * Consumers subscribe via `useAnnotationsForView(workspaceId, symbol,
 * timeframe)` and the chart rendering hook (`useAgentAnnotations`) translates
 * the store shape into LWC primitives.
 *
 * Implementation: a singleton store built on `useSyncExternalStore`. Snapshots
 * for a given chart instance are referentially stable between unrelated writes
 * so consumers don't re-render when another instance's bucket mutates.
 */

import { useMemo, useSyncExternalStore } from 'react';

export type AnnotationType =
  | 'price_line'
  | 'trendline'
  | 'marker'
  | 'vertical_line'
  | 'rectangle'
  | 'text'
  | 'event'
  | 'fib_retracement';

export interface TimePricePoint {
  time: string;
  price: number;
}

export interface BaseAnnotation {
  annotation_id: string;
  symbol: string;
  /** Chart interval this annotation belongs to (e.g. '1day'). */
  timeframe?: string;
  /** Disclosed instance key, `{SYMBOL}:{timeframe}`. */
  chart_id?: string;
  type: AnnotationType;
}

export interface PriceLineAnnotation extends BaseAnnotation {
  type: 'price_line';
  price: number;
  label?: string | null;
  color?: string | null;
  style?: 'solid' | 'dashed' | 'dotted';
}

export interface TrendlineAnnotation extends BaseAnnotation {
  type: 'trendline';
  point1: TimePricePoint;
  point2: TimePricePoint;
  label?: string | null;
  color?: string | null;
}

export interface MarkerAnnotation extends BaseAnnotation {
  type: 'marker';
  time: string;
  shape: 'arrowUp' | 'arrowDown' | 'circle' | 'square';
  position?: 'aboveBar' | 'belowBar' | 'inBar';
  text?: string | null;
  color?: string | null;
}

export interface VerticalLineAnnotation extends BaseAnnotation {
  type: 'vertical_line';
  time: string;
  label?: string | null;
  color?: string | null;
  style?: 'solid' | 'dashed' | 'dotted';
}

export interface RectangleAnnotation extends BaseAnnotation {
  type: 'rectangle';
  point1: TimePricePoint;
  point2: TimePricePoint;
  label?: string | null;
  color?: string | null;
}

export interface TextAnnotation extends BaseAnnotation {
  type: 'text';
  time: string;
  price: number;
  text: string;
  color?: string | null;
}

export interface FibRetracementAnnotation extends BaseAnnotation {
  type: 'fib_retracement';
  point1: TimePricePoint;
  point2: TimePricePoint;
  label?: string | null;
  color?: string | null;
}

export interface EventAnnotation extends BaseAnnotation {
  type: 'event';
  /** ISO8601 datetime anchoring the badge horizontally. */
  time: string;
  /** Price (y-axis value) anchoring the badge vertically. */
  price: number;
  /** Short headline shown on the always-visible badge. */
  title: string;
  /** A few sentences revealed on hover/click. */
  detail: string;
  color?: string | null;
}

export type StoredAnnotation =
  | PriceLineAnnotation
  | TrendlineAnnotation
  | MarkerAnnotation
  | VerticalLineAnnotation
  | RectangleAnnotation
  | TextAnnotation
  | EventAnnotation
  | FibRetracementAnnotation;

/** One server-side chart instance, as returned by the list endpoint. */
export interface ChartInstance {
  chart_id: string;
  symbol: string;
  timeframe: string;
  annotations: StoredAnnotation[];
}

// Timeframes the agent can draw on (mirrors the backend `Timeframe` enum).
// The chart's `interval` may be a superset (e.g. '1s'); intervals outside this
// set simply never match an agent-drawn chart instance.
export const VALID_TIMEFRAMES: ReadonlySet<string> = new Set([
  '1min',
  '5min',
  '15min',
  '30min',
  '1hour',
  '4hour',
  '1day',
]);

export const DEFAULT_TIMEFRAME = '1day';

/** Coerce a chart interval to a timeframe the agent understands. */
export function normalizeTimeframe(interval: string | null | undefined): string {
  const tf = (interval ?? '').trim();
  return VALID_TIMEFRAMES.has(tf) ? tf : DEFAULT_TIMEFRAME;
}

/** Disclosed instance key: `{SYMBOL}:{timeframe}` (uppercased ticker). */
export function makeChartId(symbol: string, timeframe: string): string {
  return `${symbol.trim().toUpperCase()}:${timeframe.trim()}`;
}

type Bucket = Record<string, StoredAnnotation>;
type State = { byChart: Record<string, Bucket> };

const EMPTY_BUCKET: Bucket = Object.freeze({}) as Bucket;
const SEP = '||';

let state: State = { byChart: {} };

// Display-cleared chart instances. The "Clear" affordance removes a drawing from
// the chart while keeping its data in `byChart` — re-opening the annotation
// artifact in chat (or a new live draw) restores it. Ephemeral by design: a
// reload re-syncs from the server and shows everything again.
let clearedKeys: ReadonlySet<string> = new Set();

const listeners = new Set<() => void>();

function emit(): void {
  for (const listener of listeners) {
    listener();
  }
}

function toUpper(symbol: string): string {
  return symbol.trim().toUpperCase();
}

/** Composite key for one chart instance bucket. */
function storeKey(workspaceId: string, chartId: string): string {
  return `${workspaceId}${SEP}${chartId}`;
}

/**
 * Module-level store API. Safe to call from anywhere — SSE handlers,
 * effect hooks, event handlers. Every mutator is scoped to one chart
 * instance `(workspaceId, chartId)`.
 */
export const chartAnnotationStore = {
  getState(): State {
    return state;
  },

  subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  },

  /** Upsert a single annotation by its id into a chart instance. Idempotent. */
  add(workspaceId: string, chartId: string, annotation: StoredAnnotation): void {
    if (!workspaceId || !chartId) return;
    const key = storeKey(workspaceId, chartId);
    const prevBucket = state.byChart[key] ?? {};
    const nextBucket: Bucket = {
      ...prevBucket,
      [annotation.annotation_id]: annotation,
    };
    state = { byChart: { ...state.byChart, [key]: nextBucket } };
    emit();
  },

  /** Remove specific ids from a chart instance's bucket. */
  remove(workspaceId: string, chartId: string, ids: string[]): void {
    if (!workspaceId || !chartId || !ids?.length) return;
    const key = storeKey(workspaceId, chartId);
    const prev = state.byChart[key];
    if (!prev) return;
    const next: Bucket = { ...prev };
    let changed = false;
    for (const id of ids) {
      if (id in next) {
        delete next[id];
        changed = true;
      }
    }
    if (!changed) return;
    state = { byChart: { ...state.byChart, [key]: next } };
    emit();
  },

  /** True when the user cleared this instance from the chart display. */
  isDisplayCleared(
    workspaceId: string | null | undefined,
    chartId: string | null | undefined,
  ): boolean {
    if (!workspaceId || !chartId) return false;
    return clearedKeys.has(storeKey(workspaceId, chartId));
  },

  /**
   * Remove a chart instance from the display without deleting its data. The
   * data stays in `byChart`; `restoreDisplay` (or a new live add) brings it
   * back.
   */
  clearDisplay(workspaceId: string, chartId: string): void {
    if (!workspaceId || !chartId) return;
    const key = storeKey(workspaceId, chartId);
    if (clearedKeys.has(key)) return;
    const next = new Set(clearedKeys);
    next.add(key);
    clearedKeys = next;
    emit();
  },

  /** Re-show a previously cleared chart instance. No-op if not cleared. */
  restoreDisplay(workspaceId: string, chartId: string): void {
    if (!workspaceId || !chartId) return;
    const key = storeKey(workspaceId, chartId);
    if (!clearedKeys.has(key)) return;
    const next = new Set(clearedKeys);
    next.delete(key);
    clearedKeys = next;
    emit();
  },

  /** Drop every annotation in a chart instance. */
  clear(workspaceId: string, chartId: string): void {
    if (!workspaceId || !chartId) return;
    const key = storeKey(workspaceId, chartId);
    if (!(key in state.byChart)) return;
    const nextByChart = { ...state.byChart };
    delete nextByChart[key];
    state = { byChart: nextByChart };
    emit();
  },

  /**
   * Replace one chart instance's bucket with exactly these annotations.
   * Leaves other instances untouched.
   */
  setAll(
    workspaceId: string,
    chartId: string,
    annotations: StoredAnnotation[],
  ): void {
    if (!workspaceId || !chartId) return;
    const key = storeKey(workspaceId, chartId);
    const bucket: Bucket = {};
    for (const ann of annotations) {
      bucket[ann.annotation_id] = ann;
    }
    state = { byChart: { ...state.byChart, [key]: bucket } };
    emit();
  },

  /**
   * Reconcile every chart instance for a `(workspace, symbol)` against the
   * server's view: replace all timeframes for that symbol with `charts`,
   * dropping any local instance the server no longer has. Used by the
   * persistence sync so a reload is the source of truth for that symbol
   * without nuking other workspaces' or symbols' instances.
   */
  setChartsForSymbol(
    workspaceId: string,
    symbol: string,
    charts: ChartInstance[],
  ): void {
    if (!workspaceId) return;
    const sym = toUpper(symbol);
    const prefix = `${workspaceId}${SEP}${sym}:`;
    const nextByChart: Record<string, Bucket> = {};
    // Keep everything that isn't this (workspace, symbol).
    for (const [k, v] of Object.entries(state.byChart)) {
      if (!k.startsWith(prefix)) nextByChart[k] = v;
    }
    // Install the server's instances for this (workspace, symbol).
    for (const chart of charts) {
      const bucket: Bucket = {};
      for (const ann of chart.annotations ?? []) {
        bucket[ann.annotation_id] = ann;
      }
      nextByChart[storeKey(workspaceId, chart.chart_id)] = bucket;
    }
    state = { byChart: nextByChart };
    emit();
  },

  /** Test-only: wipe every chart instance. */
  _resetForTesting(): void {
    state = { byChart: {} };
    clearedKeys = new Set();
    emit();
  },
};

const KNOWN_ANNOTATION_TYPES: ReadonlySet<string> = new Set<AnnotationType>([
  'price_line',
  'trendline',
  'marker',
  'vertical_line',
  'rectangle',
  'text',
  'event',
  'fib_retracement',
]);

/** Resolve the chart_id from a payload, deriving it if only symbol+tf given. */
function resolveChartId(payload: Record<string, unknown>): string | null {
  const chartId = payload.chart_id as string | undefined;
  if (chartId) return chartId;
  const symbol = payload.symbol as string | undefined;
  if (!symbol) return null;
  return makeChartId(symbol, normalizeTimeframe(payload.timeframe as string | undefined));
}

/**
 * Apply one SSE ``chart_annotation`` artifact event to the store.
 *
 * Shared by both chat engines (MarketView's flash ``useMarketChat`` and
 * ChatAgent's ``useChatMessages``) so the live add/remove/clear logic + shape
 * validation lives in one place. The payload carries ``workspace_id`` +
 * ``chart_id`` (or ``symbol`` + ``timeframe`` to derive it). The store's
 * consumers (LWC primitives) throw on a malformed ``type`` / ``annotation_id``,
 * so we validate before writing. No-op for any other artifact type, a missing
 * payload, or a payload without a workspace to key on.
 */
export function applyAnnotationArtifact(
  artifactType: string | undefined,
  payload: Record<string, unknown> | undefined,
): void {
  if (artifactType !== 'chart_annotation' || !payload) return;
  const workspaceId = payload.workspace_id as string | undefined;
  const chartId = resolveChartId(payload);
  if (!workspaceId) return;
  if (!chartId) return;

  const op = payload.op as string | undefined;
  if (op === 'add') {
    const raw = payload.annotation as Record<string, unknown> | undefined;
    if (
      raw &&
      typeof raw.annotation_id === 'string' &&
      typeof raw.symbol === 'string' &&
      typeof raw.type === 'string' &&
      KNOWN_ANNOTATION_TYPES.has(raw.type)
    ) {
      chartAnnotationStore.add(workspaceId, chartId, raw as unknown as StoredAnnotation);
      // A fresh draw un-clears the instance so the new annotation is visible.
      chartAnnotationStore.restoreDisplay(workspaceId, chartId);
    }
  } else if (op === 'remove') {
    const ids = payload.ids as string[] | undefined;
    if (Array.isArray(ids) && ids.length > 0) {
      chartAnnotationStore.remove(workspaceId, chartId, ids);
    }
  } else if (op === 'clear') {
    chartAnnotationStore.clear(workspaceId, chartId);
  }
}

function getBucket(
  workspaceId: string | null | undefined,
  chartId: string | null | undefined,
): Bucket {
  if (!workspaceId || !chartId) return EMPTY_BUCKET;
  return state.byChart[storeKey(workspaceId, chartId)] ?? EMPTY_BUCKET;
}

/**
 * Subscribe to annotations for one chart instance — the active workspace's
 * `symbol:timeframe`. Returns a referentially stable array so React consumers
 * only re-render when that instance's bucket actually changes.
 */
export function useAnnotationsForView(
  workspaceId: string | null | undefined,
  symbol: string | null | undefined,
  timeframe: string | null | undefined,
): StoredAnnotation[] {
  const chartId =
    symbol && timeframe ? makeChartId(symbol, timeframe) : null;
  const bucket = useSyncExternalStore(
    chartAnnotationStore.subscribe,
    () => getBucket(workspaceId, chartId),
    () => EMPTY_BUCKET, // SSR: nothing
  );
  return useMemo(() => Object.values(bucket), [bucket]);
}

/**
 * Subscribe to whether one chart instance is currently cleared from the
 * display. `true` means the data is still in the store but the user dismissed
 * it from the chart.
 */
export function useDisplayCleared(
  workspaceId: string | null | undefined,
  symbol: string | null | undefined,
  timeframe: string | null | undefined,
): boolean {
  const chartId = symbol && timeframe ? makeChartId(symbol, timeframe) : null;
  return useSyncExternalStore(
    chartAnnotationStore.subscribe,
    () => chartAnnotationStore.isDisplayCleared(workspaceId, chartId),
    () => false, // SSR: nothing cleared
  );
}
