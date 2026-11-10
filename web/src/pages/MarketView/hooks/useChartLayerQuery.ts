/**
 * A chart's agent layers, read from the query cache, and the viewer's remove.
 */

import { useCallback } from 'react';
import { queryOptions, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import type { StoredChartLayer } from '../types/chartLayer';
import { deleteChartLayer, fetchChartLayers } from '../utils/api';
import { applyLayerOp, chartKeyOf, chartLayersKey, rowsOfChart } from '../utils/chartLayerCache';
import { chartKeyParts, type ChartKey } from '../utils/chartInstanceKey';

const NO_ROWS: StoredChartLayer[] = [];

/** The query holding every layer of the chart's `(workspace, symbol)`, all timeframes. */
export function chartLayersQuery(key: ChartKey) {
  const { workspaceId, symbol } = chartKeyParts(key);
  return queryOptions({
    queryKey: chartLayersKey(key),
    queryFn: ({ signal }) => fetchChartLayers(workspaceId, symbol, signal),
    // Read again on every mount: another tab or thread may have published.
    staleTime: 0,
  });
}

/** Every layer of this chart, hidden ones included, oldest first. */
export function useChartLayerRows(key: ChartKey | null): StoredChartLayer[] {
  const select = useCallback((rows: StoredChartLayer[]) => (key ? rowsOfChart(rows, key) : NO_ROWS), [key]);
  const { data } = useQuery({
    // A null key reads nothing; the placeholder key is never fetched.
    ...chartLayersQuery(key ?? ('' as ChartKey)),
    enabled: key !== null,
    select,
  });
  return data ?? NO_ROWS;
}

/**
 * Remove a stored layer. It goes from the chart at once and comes back in its
 * place if the server refuses, unless the agent has published it again since;
 * either way the server's list is read again after.
 */
export function useRemoveChartLayer(onFailed: () => void) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (row: StoredChartLayer) =>
      deleteChartLayer(row.workspace_id, row.layer_id, row.symbol, row.timeframe),
    onMutate: async (row) => {
      const key = chartKeyOf(row);
      if (!key) return { at: -1 };
      const queryKey = chartLayersKey(key);
      await queryClient.cancelQueries({ queryKey });
      const at = queryClient.getQueryData<StoredChartLayer[]>(queryKey)?.indexOf(row) ?? -1;
      queryClient.setQueryData(queryKey, applyLayerOp({ ...row, op: 'remove', ids: [row.layer_id] }));
      return { at };
    },
    onError: (_err, row, context) => {
      const key = chartKeyOf(row);
      if (key) {
        queryClient.setQueryData<StoredChartLayer[]>(chartLayersKey(key), (rows) => {
          if (!rows || rows.some((r) => r.layer_id === row.layer_id && chartKeyOf(r) === key)) return rows;
          const at = context && context.at >= 0 ? Math.min(context.at, rows.length) : rows.length;
          return rows.toSpliced(at, 0, row);
        });
      }
      onFailed();
    },
    onSettled: (_data, _err, row) => {
      const key = chartKeyOf(row);
      if (key) void queryClient.invalidateQueries({ queryKey: chartLayersKey(key) });
    },
  });
}
