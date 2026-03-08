import { useCallback, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useToast } from '@/components/ui/use-toast';
import {
  addWatchlistItem,
  deleteWatchlistItem,
  getStockPrices,
  listWatchlists,
  listWatchlistItems,
} from '../utils/api';

/**
 * Shared hook for watchlist data fetching and CRUD operations.
 * Used by both Dashboard and MarketView sidebar.
 * Refactored to use TanStack Query for optimal polling and caching.
 */
export function useWatchlistData() {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const [modalOpen, setModalOpen] = useState(false);

  const { data = { rows: [], currentWatchlistId: null }, isLoading: loading, refetch: fetchWatchlist } = useQuery({
    queryKey: ['watchlistData'],
    queryFn: async () => {
      const { watchlists } = await listWatchlists();
      const firstWatchlist = watchlists?.[0];
      const watchlistId = firstWatchlist?.watchlist_id || 'default';

      const { items } = await listWatchlistItems(watchlistId);
      const symbols = items?.length ? items.map((i) => i.symbol) : [];
      const prices = symbols.length > 0 ? await getStockPrices(symbols) : [];
      const bySym = Object.fromEntries((prices || []).map((p) => [p.symbol, p]));

      const combined = items?.length
        ? items.map((i) => {
          const sym = String(i.symbol || '').trim().toUpperCase();
          const p = bySym[sym] || {};
          return {
            watchlist_item_id: i.watchlist_item_id,
            symbol: sym,
            price: p.price ?? 0,
            change: p.change ?? 0,
            changePercent: p.changePercent ?? 0,
            isPositive: p.isPositive ?? true,
            earlyTradingChangePercent: p.earlyTradingChangePercent ?? null,
            lateTradingChangePercent: p.lateTradingChangePercent ?? null,
          };
        })
        : [];

      return { rows: combined, currentWatchlistId: watchlistId };
    },
    refetchInterval: 60000,
    staleTime: 1000 * 30, // 30s fresh
  });

  const { rows, currentWatchlistId } = data;

  const handleAdd = useCallback(
    async (itemData, watchlistId) => {
      try {
        let targetWatchlistId = watchlistId || currentWatchlistId;
        if (!targetWatchlistId) {
          const { watchlists } = await listWatchlists();
          targetWatchlistId = watchlists?.[0]?.watchlist_id || 'default';
        }

        await addWatchlistItem(itemData, targetWatchlistId);
        setModalOpen(false);
        queryClient.invalidateQueries({ queryKey: ['watchlistData'] });

        toast({
          title: 'Stock added',
          description: `${itemData.symbol} has been added to your watchlist.`,
        });
      } catch (e) {
        console.error('Add watchlist item failed:', e?.response?.status, e?.response?.data, e?.message);

        const status = e?.response?.status;
        const msg = e?.response?.data?.detail || e?.response?.data?.message || '';

        if (status === 409 || msg.toLowerCase().includes('already exists')) {
          toast({
            variant: 'destructive',
            title: 'Already in watchlist',
            description: `${itemData.symbol} is already in your watchlist.`,
          });
        } else {
          toast({
            variant: 'destructive',
            title: 'Cannot add stock',
            description: msg || 'Failed to add to watchlist. Please try again.',
          });
        }
      }
    },
    [currentWatchlistId, queryClient, toast]
  );

  const handleDelete = useCallback(
    async (itemId) => {
      try {
        let watchlistId = currentWatchlistId;
        if (!watchlistId) {
          const { watchlists } = await listWatchlists();
          watchlistId = watchlists?.[0]?.watchlist_id || 'default';
        }

        await deleteWatchlistItem(itemId, watchlistId);
        queryClient.invalidateQueries({ queryKey: ['watchlistData'] });
      } catch (e) {
        console.error('Delete watchlist item failed:', e?.response?.status, e?.response?.data, e?.message);
      }
    },
    [currentWatchlistId, queryClient]
  );

  return {
    rows,
    loading,
    modalOpen,
    setModalOpen,
    currentWatchlistId,
    fetchWatchlist,
    handleAdd,
    handleDelete,
  };
}
