/**
 * Widget context snapshot registry.
 *
 * Each widget definition that opts in calls `useWidgetContextExport(instanceId,
 * exporters)` from inside its render component. The hook registers a function
 * that returns a `WidgetContextSnapshot` synchronously, reading whatever JS
 * state is already in scope (OHLCV bars, news headlines, watchlist quotes).
 *
 * The "+" button in `WidgetFrame` calls `getWidgetContextSnapshot(instanceId)`
 * to produce the snapshot, then publishes it on `ContextBus`.
 *
 * List-shaped widgets (news, watchlist, threads, calendar, insight, portfolio)
 * also register a `rows(rowId)` exporter for per-row attach via
 * `RowAttachButton`.
 *
 * Most exporters are synchronous (read JS state already in scope). The `rows`
 * exporter may also return a Promise — used by widgets that fetch the full
 * row payload on attach (e.g., news detail body that lives behind an API call).
 */

import { useEffect, useSyncExternalStore } from 'react';

export interface WidgetContextSnapshot {
  widget_type: string;
  widget_id: string;
  /** Human-readable label for the chip (e.g., "NVDA · 1d Chart"). */
  label: string;
  /** Optional caption / freshness note. */
  description?: string;
  /** ISO timestamp captured at snapshot time. */
  captured_at: string;
  /** Pre-rendered <widget-context>...</widget-context> markdown for the agent. */
  text: string;
  /** Structured raw payload — persisted to query_metadata.widget_contexts for replay. */
  data: Record<string, unknown>;
  /**
   * Optional JPEG data URL for chart-bearing widgets. Backend frontend code
   * splits this into a separate MultimodalContext(type='image') item so the
   * existing modality gate handles vision-vs-text-only routing.
   */
  image_jpeg_data_url?: string;
}

export interface WidgetContextExporters {
  /** Snapshot the entire widget. Always required. */
  full: () => WidgetContextSnapshot;
  /**
   * Snapshot a single row by id. List widgets only. Returns null if rowId not
   * found. May return a Promise — useful when the full row payload requires a
   * network fetch (e.g., news detail body).
   */
  rows?: (
    rowId: string,
  ) => WidgetContextSnapshot | null | Promise<WidgetContextSnapshot | null>;
}

const registry = new Map<string, WidgetContextExporters>();
const listeners = new Set<() => void>();

function notify() {
  for (const fn of listeners) {
    try {
      fn();
    } catch (err) {
      console.error('[widgetContext] listener threw', err);
    }
  }
}

function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => {
    listeners.delete(fn);
  };
}

/**
 * Register snapshot exporters for a widget instance. Call from inside the
 * widget's render component; the hook re-registers if the instance id ever
 * changes (rare — usually only when the user duplicates a widget).
 *
 * Cleanup runs on unmount or when `instanceId` changes; this is what keeps
 * stale exporters from outliving their widgets and capturing stale closures.
 */
export function useWidgetContextExport(
  instanceId: string,
  exporters: WidgetContextExporters,
): void {
  // Stash the exporters in the registry so the (latest) closure is what runs
  // when the user clicks "+". Re-running this effect on every render keeps
  // the closures fresh without forcing each widget to memoize its serializer.
  // Perf tradeoff (intentional): callers pass `exporters` inline, so its
  // identity changes every render; the effect runs and `notify()` fires twice
  // (cleanup + setup) per widget per dashboard render. `useSyncExternalStore`
  // snapshot equality (`registry.has(instanceId)` stays true) prevents downstream
  // re-renders. Don't `useMemo` this in callers — that would break the
  // closure-freshness guarantee.
  useEffect(() => {
    registry.set(instanceId, exporters);
    notify();
    return () => {
      // Only delete if the slot still points at us — guards against unmount
      // running *after* a remount with the same id has registered new
      // exporters (rare, but happens with React StrictMode + key reuse).
      if (registry.get(instanceId) === exporters) {
        registry.delete(instanceId);
        notify();
      }
    };
  }, [instanceId, exporters]);
}

/**
 * Returns the registered widget snapshot. Full-widget exporters are always
 * sync; row exporters may be async (the row exporter's signature allows it),
 * so callers should `await` the result. Sync exporters resolve in a
 * micro-task — the await cost is negligible.
 */
export async function getWidgetContextSnapshot(
  instanceId: string,
  rowId?: string,
): Promise<WidgetContextSnapshot | null> {
  const entry = registry.get(instanceId);
  if (!entry) return null;
  if (rowId !== undefined) {
    if (!entry.rows) return null;
    try {
      return await entry.rows(rowId);
    } catch (err) {
      console.error('[widgetContext] row exporter threw', err);
      return null;
    }
  }
  try {
    return entry.full();
  } catch (err) {
    console.error('[widgetContext] full exporter threw', err);
    return null;
  }
}

/** Whether a widget instance has registered a snapshot exporter. */
export function hasWidgetContextExporter(instanceId: string): boolean {
  return registry.has(instanceId);
}

/**
 * Reactive variant — subscribes to registry changes so the caller re-renders
 * when an exporter is registered or removed. Use this in `WidgetFrame` so the
 * "+" button enables the moment a child widget registers its exporter (which
 * happens in a `useEffect` *after* the frame's first render).
 */
export function useHasWidgetContextExporter(instanceId: string): boolean {
  return useSyncExternalStore(
    subscribe,
    () => registry.has(instanceId),
    () => registry.has(instanceId),
  );
}

/** Test-only — reset the registry between cases. */
export function __resetWidgetContextRegistryForTests(): void {
  registry.clear();
  notify();
}
