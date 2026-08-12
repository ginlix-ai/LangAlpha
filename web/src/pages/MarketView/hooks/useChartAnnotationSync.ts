/**
 * Persistence sync for chart annotations.
 *
 * On mount and whenever ``workspaceId`` or ``symbol`` changes, fetch
 * ``GET /api/v1/workspaces/{workspace_id}/chart-annotations?symbol=X`` for the
 * active workspace and reconcile every chart instance (all timeframes) for that
 * symbol into ``chartAnnotationStore``. The store keys by
 * ``(workspace_id, chart_id)``, so we fetch all timeframes up front and the
 * chart selects the one matching the current interval — switching the interval
 * needs no refetch.
 */

import { useEffect } from 'react';

import { api } from '@/api/client';

import {
  chartAnnotationStore,
  type ChartInstance,
} from '../stores/chartAnnotationStore';

interface ChartsResponse {
  workspace_id: string;
  charts: ChartInstance[];
}

/**
 * Fetch and reconcile annotations for the active workspace + symbol. Replaces
 * every locally-held instance for that `(workspace, symbol)` so a reload mirrors
 * server state, including instances cleared elsewhere.
 */
export function useChartAnnotationSync(
  workspaceId: string | null | undefined,
  symbol: string | null | undefined,
): void {
  useEffect(() => {
    if (!workspaceId || !symbol) return;

    const controller = new AbortController();
    let cancelled = false;
    // Capture the live-mutation seq before the fetch so a stale response can't
    // overwrite an instance a concurrent SSE add/remove/clear changed meanwhile.
    const seqAtStart = chartAnnotationStore.getMutationSeq();

    (async () => {
      try {
        const { data } = await api.get<ChartsResponse>(
          `/api/v1/workspaces/${encodeURIComponent(workspaceId)}/chart-annotations`,
          { params: { symbol }, signal: controller.signal },
        );
        if (cancelled) return;
        chartAnnotationStore.setChartsForSymbol(
          workspaceId,
          symbol,
          data?.charts ?? [],
          seqAtStart,
        );
      } catch (err: unknown) {
        const error = err as { name?: string };
        if (error?.name === 'CanceledError' || error?.name === 'AbortError') {
          return;
        }
        // Missing workspace / 403 / 500 — leave the store as-is; the user may
        // not own the selected workspace in some stale-selection edge cases.
        if (import.meta.env.DEV) {
          console.warn('[useChartAnnotationSync] sync failed', workspaceId, err);
        }
      }
    })();

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [workspaceId, symbol]);
}
