/**
 * The query cache side of agent chart layers: the entry a chart reads, the
 * pure update one `chart_layer` stream event makes to it, and how the stream
 * applies one. Free of React Query's runtime and of the API client, since the
 * stream processor imports it in node tests, which have no `window`.
 *
 * A live event is applied to the cache in place, so a chart on screen shows
 * the layer the moment the agent stores it. A replayed event (a reconnect's
 * backlog) is not: it may predate what the viewer did since, such as removing
 * the layer it adds, so it only marks the entry stale and the server's list,
 * which already holds every event the backlog carries, replaces it.
 */

import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';

import { requestShowLayers } from '../stores/layerViewStore';
import type { ChartLayerArtifactPayload, ChartLayerRef, StoredChartLayer } from '../types/chartLayer';
import { chartKeyFor, chartKeyParts, type ChartKey } from './chartInstanceKey';

/**
 * The query entry holding every layer of the chart's `(workspace, symbol)`;
 * the query itself is `chartLayersQuery`.
 */
export function chartLayersKey(key: ChartKey) {
  const { workspaceId, symbol } = chartKeyParts(key);
  return queryKeys.chartLayers.bySymbol(workspaceId, symbol);
}

/** The chart a stored row or an event names. */
export const chartKeyOf = (ref: ChartLayerRef): ChartKey | null =>
  chartKeyFor(ref.workspace_id, ref.symbol, ref.timeframe);

/**
 * Whether the renderer can draw this row. The server validates every layer
 * before storing it; a version this build does not know is left out.
 */
const drawable = (row: StoredChartLayer): boolean => row.layer.version === 1;

/** The drawable rows of one chart, in the order the server lists them. */
export function rowsOfChart(rows: readonly StoredChartLayer[], key: ChartKey): StoredChartLayer[] {
  return rows.filter((row) => drawable(row) && chartKeyOf(row) === key);
}

/**
 * The cache update for one event. An add replaces the row with its id on its
 * chart in place, or appends it; with nothing cached it does nothing, since
 * the next read fetches the server's list, which already has it.
 */
export function applyLayerOp(
  op: ChartLayerArtifactPayload,
): (rows: StoredChartLayer[] | undefined) => StoredChartLayer[] | undefined {
  const key = chartKeyOf(op);
  return (rows) => {
    if (!rows || !key) return rows;
    if (op.op === 'add') {
      // A body-less add names a layer the cache cannot hold; it is read.
      if (!op.layer) return rows;
      const { workspace_id, chart_id, symbol, timeframe, layer_id, layer } = op;
      const row: StoredChartLayer = { workspace_id, chart_id, symbol, timeframe, layer_id, layer };
      const at = rows.findIndex((r) => r.layer_id === layer_id && chartKeyOf(r) === key);
      return at < 0 ? [...rows, row] : rows.with(at, row);
    }
    const ids = op.op === 'remove' ? new Set(op.ids) : null;
    return rows.filter((r) => chartKeyOf(r) !== key || (ids !== null && !ids.has(r.layer_id)));
  };
}

/**
 * A background command published layers with no stream to carry them, and its
 * report names them. Each affected `(workspace, symbol)` entry is marked stale
 * so a chart already open reads them; an entry nothing observes is left to its
 * next mount.
 */
export function invalidateReportedLayers(
  queryClient: QueryClient,
  layers: readonly ChartLayerRef[] | null | undefined,
): void {
  // One entry holds every timeframe of a symbol, so several charts share it.
  const seen = new Set<string>();
  for (const layer of layers ?? []) {
    const key = chartKeyOf(layer);
    if (!key) continue;
    const queryKey = chartLayersKey(key);
    const id = JSON.stringify(queryKey);
    if (seen.has(id)) continue;
    seen.add(id);
    void queryClient.invalidateQueries({ queryKey });
  }
}

/**
 * Apply one `chart_layer` stream event. A live add also asks to show the
 * layer (`requestShowLayers`), so a chart on screen jumps to where it landed.
 */
export async function applyChartLayerEvent(
  queryClient: QueryClient,
  op: ChartLayerArtifactPayload | undefined,
  { replay = false }: { replay?: boolean } = {},
): Promise<void> {
  const key = op ? chartKeyOf(op) : null;
  if (!op || !key) return;
  const queryKey = chartLayersKey(key);
  // A body-less add is read like a replay: the server dropped the layer from
  // the event to protect the stream (or a subagent forwarded it), and its list
  // has it. Live, it is still shown: framing waits for the list to hold it.
  if (replay || (op.op === 'add' && !op.layer)) {
    await queryClient.invalidateQueries({ queryKey });
    if (!replay && op.op === 'add') requestShowLayers(key, [op.layer_id]);
    return;
  }
  // A read in flight may predate this event. Cancelled, it would put back what
  // it held on landing, so the write waits for the cancel; and since the entry
  // carries every timeframe, a read cancelled here is read again.
  const wasFetching = queryClient.getQueryState(queryKey)?.fetchStatus === 'fetching';
  await queryClient.cancelQueries({ queryKey });
  queryClient.setQueryData(queryKey, applyLayerOp(op));
  if (wasFetching) void queryClient.invalidateQueries({ queryKey });
  if (op.op === 'add') requestShowLayers(key, [op.layer_id]);
}
