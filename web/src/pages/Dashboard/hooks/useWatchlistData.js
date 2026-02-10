import { useCallback, useEffect, useState } from 'react';
import { useToast } from '@/components/ui/use-toast';
import {
  addWatchlistItem,
  deleteWatchlistItem,
  getStockPrices,
  listWatchlists,
  listWatchlistItems,
} from '../utils/api';

// Module-level cache (survives navigation, clears on page refresh)
let watchlistCache = null; // { rows, watchlistId }

/**
 * Shared hook for watchlist data fetching and CRUD operations.
 * Used by both Dashboard and TradingCenter sidebar.
 */
export function useWatchlistData() {
  const { toast } = useToast();

  const [rows, setRows] = useState(() => watchlistCache?.rows || []);
  const [loading, setLoading] = useState(!watchlistCache);
  const [modalOpen, setModalOpen] = useState(false);
  const [currentWatchlistId, setCurrentWatchlistId] = useState(() => watchlistCache?.watchlistId || null);

  const fetchWatchlist = useCallback(async () => {
    if (!watchlistCache) setLoading(true);
    try {
      const { watchlists } = await listWatchlists();
      const firstWatchlist = watchlists?.[0];
      const watchlistId = firstWatchlist?.watchlist_id || 'default';
      setCurrentWatchlistId(watchlistId);

      const { items } = await listWatchlistItems(watchlistId);
      const symbols = items?.length ? items.map((i) => i.symbol) : [];
      const prices = symbols.length > 0 ? await getStockPrices(symbols) : [];
      const bySym = Object.fromEntries((prices || []).map((p) => [p.symbol, p]));

      const combined = items?.length
        ? items.map((i) => {
            const sym = String(i.symbol || '').trim().toUpperCase();
            const p = bySym[sym] || {};
            return {
              item_id: i.item_id,
              symbol: sym,
              price: p.price ?? 0,
              change: p.change ?? 0,
              changePercent: p.changePercent ?? 0,
              isPositive: p.isPositive ?? true,
            };
          })
        : [];
      setRows(combined);
      watchlistCache = { rows: combined, watchlistId };
    } catch {
      if (!watchlistCache) setRows([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchWatchlist();
    const intervalId = setInterval(() => {
      fetchWatchlist();
    }, 60000);
    return () => clearInterval(intervalId);
  }, [fetchWatchlist]);

  const handleAdd = useCallback(
    async (itemData, watchlistId) => {
      try {
        let targetWatchlistId = watchlistId || currentWatchlistId;
        if (!targetWatchlistId) {
          const { watchlists } = await listWatchlists();
          targetWatchlistId = watchlists?.[0]?.watchlist_id || 'default';
          setCurrentWatchlistId(targetWatchlistId);
        }

        await addWatchlistItem(itemData, targetWatchlistId);
        setModalOpen(false);
        watchlistCache = null;
        fetchWatchlist();

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
    [currentWatchlistId, fetchWatchlist, toast]
  );

  const handleDelete = useCallback(
    async (itemId) => {
      try {
        let watchlistId = currentWatchlistId;
        if (!watchlistId) {
          const { watchlists } = await listWatchlists();
          watchlistId = watchlists?.[0]?.watchlist_id || 'default';
          setCurrentWatchlistId(watchlistId);
        }

        await deleteWatchlistItem(itemId, watchlistId);
        watchlistCache = null;
        fetchWatchlist();
      } catch (e) {
        console.error('Delete watchlist item failed:', e?.response?.status, e?.response?.data, e?.message);
      }
    },
    [currentWatchlistId, fetchWatchlist]
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
