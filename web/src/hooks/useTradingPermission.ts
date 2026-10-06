import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import { getTradingPermission, updateTradingPermission } from '../api/tradingPermission';
import type { TradingPermission, TradingPermissionUpdate } from '../types/api';

/** Reread on every mount and focus: another tab or device may have raised the
 *  level, and a page still showing the level that asks would let the user
 *  believe they are already there, since picking the level shown sends
 *  nothing. */
export function useTradingPermission() {
  return useQuery({
    queryKey: queryKeys.user.tradingPermission(),
    queryFn: getTradingPermission,
    staleTime: 0,
    retry: false,
  });
}

/**
 * Change the trading permission. Not optimistic: raising it is a consent the
 * server has to accept, so nothing shows the new level until it has. A
 * success stores the server's answer as it is; rereading it at once would
 * only fetch the same row again.
 *
 * The MCP queries go stale with it because every catalog row carries the level
 * (`trading_permission`) and the server folds it into the row's
 * `order_approval` and each tool's `approval`, so a broker's switches and
 * badges read differently the moment this lands. They are reread
 * after a failure too, since a write whose answer was lost may still have
 * landed.
 */
export function useUpdateTradingPermission() {
  const queryClient = useQueryClient();
  const key = queryKeys.user.tradingPermission();
  return useMutation({
    mutationFn: (body: TradingPermissionUpdate) => updateTradingPermission(body),
    // A read already on its way left before the write and would land the
    // level from before over this answer, so it is dropped first.
    onSuccess: async (result: TradingPermission) => {
      await queryClient.cancelQueries({ queryKey: key });
      queryClient.setQueryData(key, result);
    },
    // A 422 can mean the server moved to a newer agreement than the one this
    // bundle shows. Rereading brings in its version, which turns the agreement
    // dialog into a reload prompt; the opt-in itself never sends that version.
    onError: () => {
      void queryClient.invalidateQueries({ queryKey: key });
    },
    onSettled: () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.mcp.all });
    },
  });
}
