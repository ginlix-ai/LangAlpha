/**
 * The viewer's own state about agent chart layers, which the server never
 * sees: which layers this viewer hid, and the one pending request to show
 * some layers. The layers themselves are server state (`useChartLayerQuery`).
 *
 * Hiding lasts the session: a reload shows everything again.
 *
 * A show request is what a fresh publish or a layer card's "Open chart" makes:
 * it un-hides the layers, MarketView switches to their chart when it is in the
 * request's workspace, and the chart rendering the key frames them once it
 * has them and its bars, then clears the request. One request at a time, a
 * newer one replacing it; one no chart takes within `SHOW_REQUEST_TTL_MS` is
 * dropped, so a chart opened much later opens on its default view.
 */

import { useSyncExternalStore } from 'react';
import { registerAuthReset } from '@/lib/authResets';
import type { ChartKey } from '../utils/chartInstanceKey';

export interface ShowLayersRequest {
  key: ChartKey;
  /** The frame covers them all. */
  layerIds: readonly string[];
  /** `Date.now()` at the request. */
  at: number;
  seq: number;
}

export const SHOW_REQUEST_TTL_MS = 60_000;

export function showRequestExpired(req: ShowLayersRequest, now = Date.now()): boolean {
  return now - req.at > SHOW_REQUEST_TTL_MS;
}

// A long session must not grow the hidden marks without bound: past the cap
// the chart hidden on longest ago shows everything again.
const MAX_HIDDEN_CHARTS = 200;

const NO_IDS: ReadonlySet<string> = new Set();

interface State {
  hidden: ReadonlyMap<ChartKey, ReadonlySet<string>>;
  show: ShowLayersRequest | null;
  seq: number;
}

const initial = (): State => ({ hidden: new Map(), show: null, seq: 0 });

let state: State = initial();
const listeners = new Set<() => void>();

function set(next: State): void {
  state = next;
  for (const listener of listeners) listener();
}

/** Hear every change to the viewer's layer state. Returns the unsubscribe. */
export function subscribeLayerView(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** `hidden` with `key`'s ids replaced, the oldest chart evicted past the cap. */
function withHidden(
  hidden: ReadonlyMap<ChartKey, ReadonlySet<string>>,
  key: ChartKey,
  ids: ReadonlySet<string>,
): ReadonlyMap<ChartKey, ReadonlySet<string>> {
  const next = new Map(hidden);
  next.delete(key); // re-inserted last, so the oldest is first out
  if (ids.size === 0) return next;
  next.set(key, ids);
  if (next.size > MAX_HIDDEN_CHARTS) next.delete(next.keys().next().value!);
  return next;
}

/** Hide or show a layer, or set it outright with `hidden`. */
export function setLayerHidden(key: ChartKey, layerId: string, hidden?: boolean): void {
  const current = state.hidden.get(key) ?? NO_IDS;
  const want = hidden ?? !current.has(layerId);
  if (want === current.has(layerId)) return;
  const ids = new Set(current);
  if (want) ids.add(layerId);
  else ids.delete(layerId);
  set({ ...state, hidden: withHidden(state.hidden, key, ids) });
}

/** Show these layers of a chart: un-hide them, then switch to and frame them. */
export function requestShowLayers(key: ChartKey, layerIds: readonly string[]): void {
  const ids = layerIds.filter(Boolean);
  if (ids.length === 0) return;
  let { hidden } = state;
  const held = hidden.get(key);
  if (held && ids.some((id) => held.has(id))) {
    const left = new Set(held);
    for (const id of ids) left.delete(id);
    hidden = withHidden(hidden, key, left);
  }
  const seq = state.seq + 1;
  set({ hidden, seq, show: { key, layerIds: ids, at: Date.now(), seq } });
}

/** Clear the request once taken; a newer one (another `seq`) stays. */
export function clearShowRequest(seq: number): void {
  if (state.show?.seq !== seq) return;
  set({ ...state, show: null });
}

/** The ids hidden on this chart; the set keeps its identity until they change. */
export function getHiddenLayers(key: ChartKey | null): ReadonlySet<string> {
  return (key && state.hidden.get(key)) || NO_IDS;
}

export function getShowLayersRequest(): ShowLayersRequest | null {
  return state.show;
}

export function useHiddenLayers(key: ChartKey | null): ReadonlySet<string> {
  return useSyncExternalStore(subscribeLayerView, () => getHiddenLayers(key), () => NO_IDS);
}

export function useShowLayersRequest(): ShowLayersRequest | null {
  return useSyncExternalStore(subscribeLayerView, getShowLayersRequest, () => null);
}

/** Test-only. */
export function resetLayerViewForTesting(): void {
  set(initial());
}

// What the last account hid and asked to see goes with it.
registerAuthReset(() => set(initial()));
