/**
 * `window.__chartLayers`, for drawing a layer on the chart by hand in a dev
 * build, without an agent turn. A publish goes through the same path the
 * stream's does; nothing reaches the server, so a reload drops it.
 */

import type { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';

import type { ChartLayer, StoredChartLayer } from '../types/chartLayer';
import { applyChartLayerEvent } from './chartLayerCache';
import { chartKeyFor, chartKeyParts } from './chartInstanceKey';

interface ChartLayersDevHelper {
  /** Publish `layer` as the agent would, in `workspaceId` (default: the last one read). */
  inject(layer: ChartLayer, workspaceId?: string): void;
  /** Drop every layer of a chart. */
  clear(symbol: string, timeframe: string, workspaceId?: string): void;
  list(): StoredChartLayer[];
}

declare global {
  interface Window {
    __chartLayers?: ChartLayersDevHelper;
  }
}

/** Dev builds only; a no-op once installed. */
export function installChartLayersDevHelper(queryClient: QueryClient): void {
  if (!import.meta.env.DEV || window.__chartLayers) return;
  const cached = () => queryClient.getQueriesData<StoredChartLayer[]>({ queryKey: queryKeys.chartLayers.all });
  const workspaceOf = (workspaceId?: string): string => {
    const ws = workspaceId ?? (cached().at(-1)?.[0][1] as string | undefined);
    if (!ws) throw new Error('__chartLayers: no workspace yet, pass one');
    return ws;
  };
  const refOf = (workspaceId: string, symbol: string, timeframe: string) => {
    const key = chartKeyFor(workspaceId, symbol, timeframe);
    if (!key) throw new Error('__chartLayers: need a symbol and a timeframe');
    const parts = chartKeyParts(key);
    return { workspace_id: workspaceId, chart_id: `${parts.symbol}:${parts.timeframe}`, symbol: parts.symbol, timeframe: parts.timeframe };
  };
  window.__chartLayers = {
    inject(layer, workspaceId) {
      const ref = refOf(workspaceOf(workspaceId), layer.symbol, layer.timeframe);
      void applyChartLayerEvent(queryClient, { op: 'add', ...ref, layer_id: layer.id, layer });
    },
    clear(symbol, timeframe, workspaceId) {
      void applyChartLayerEvent(queryClient, { op: 'clear', ...refOf(workspaceOf(workspaceId), symbol, timeframe) });
    },
    list() {
      return cached().flatMap(([, rows]) => rows ?? []);
    },
  };
}
