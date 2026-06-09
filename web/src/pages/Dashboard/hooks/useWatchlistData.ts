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
import type { StockPrice } from '@/types/market';

export interface WatchlistRow {
  watchlist_item_id: string;
  symbol: string;
  price: number;
  change: number;
  changePercent: number;
  isPositive: boolean;
  quoteAvailable?: boolean;
  previousClose: number | null;
  earlyTradingChangePercent: number | null;
  lateTradingChangePercent: number | null;
  [key: string]: unknown;
}

interface WatchlistQueryData {
  rows: WatchlistRow[];
  currentWatchlistId: string | null;
}

interface WatchlistItemData {
  symbol: string;
  [key: string]: unknown;
}

interface ApiError {
  response?: {
    status?: number;
    data?: {
      detail?: string;
      message?: string;
      [key: string]: unknown;
    };
  };
  message?: string;
}

/**
 * Shared hook for watchlist data fetching and CRUD operations.
 * Used by both Dashboard and MarketView sidebar.
 * Refactored to use TanStack Query for optimal polling and caching.
 */
export function useWatchlistData() {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const [modalOpen, setModalOpen] = useState(false);

  const { data = { rows: [], currentWatchlistId: null }, isLoading: loading, refetch: fetchWatchlist } = useQuery<WatchlistQueryData>({
    queryKey: ['watchlistData'],
    queryFn: async (): Promise<WatchlistQueryData> => {
      const { watchlists } = await listWatchlists() as { watchlists?: Array<{ watchlist_id: string; [key: string]: unknown }> };
      const firstWatchlist = watchlists?.[0];
      const watchlistId = firstWatchlist?.watchlist_id || 'default';

      const { items } = await listWatchlistItems(watchlistId) as { items?: Array<{ watchlist_item_id: string; symbol: string; [key: string]: unknown }> };
      const symbols = items?.length ? items.map((i) => i.symbol) : [];
      const prices: StockPrice[] = symbols.length > 0 ? await getStockPrices(symbols) : [];
      const bySym: Record<string, StockPrice> = Object.fromEntries((prices || []).map((p) => [p.symbol, p]));

      const combined: WatchlistRow[] = items?.length
        ? items.map((i) => {
          const sym = String(i.symbol || '').trim().toUpperCase();
          const p = bySym[sym] || {} as Partial<StockPrice>;
          const quoteAvailable = p.quoteAvailable !== false && p.price != null;
          return {
            watchlist_item_id: i.watchlist_item_id,
            symbol: sym,
            price: quoteAvailable ? p.price ?? 0 : 0,
            change: quoteAvailable ? p.change ?? 0 : 0,
            changePercent: quoteAvailable ? p.changePercent ?? 0 : 0,
            isPositive: quoteAvailable ? p.isPositive ?? true : true,
            quoteAvailable,
            previousClose: p.previousClose ?? null,
            earlyTradingChangePercent: p.earlyTradingChangePercent ?? null,
            lateTradingChangePercent: p.lateTradingChangePercent ?? null,
          };
        })
        : [];

      return { rows: combined, currentWatchlistId: watchlistId };
    },
    refetchInterval: 60000,
    refetchIntervalInBackground: false,
    staleTime: 1000 * 30, // 30s fresh
  });

  const { rows, currentWatchlistId } = data;

  const handleAdd = useCallback(
    async (itemData: WatchlistItemData, watchlistId?: string | null) => {
      try {
        let targetWatchlistId = watchlistId || currentWatchlistId;
        if (!targetWatchlistId) {
          const { watchlists } = await listWatchlists() as { watchlists?: Array<{ watchlist_id: string }> };
          targetWatchlistId = watchlists?.[0]?.watchlist_id || 'default';
        }

        await addWatchlistItem(itemData, targetWatchlistId);
        setModalOpen(false);
        queryClient.invalidateQueries({ queryKey: ['watchlistData'] });

        toast({
          title: 'Stock added',
          description: `${itemData.symbol} has been added to your watchlist.`,
        });
      } catch (e: unknown) {
        const err = e as ApiError;
        console.error('Add watchlist item failed:', err?.response?.status, err?.response?.data, err?.message);

        const status = err?.response?.status;
        const msg = err?.response?.data?.detail || err?.response?.data?.message || '';

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
    async (itemId: string) => {
      try {
        let watchlistId = currentWatchlistId;
        if (!watchlistId) {
          const { watchlists } = await listWatchlists() as { watchlists?: Array<{ watchlist_id: string }> };
          watchlistId = watchlists?.[0]?.watchlist_id || 'default';
        }

        await deleteWatchlistItem(itemId, watchlistId);
        queryClient.invalidateQueries({ queryKey: ['watchlistData'] });
      } catch (e: unknown) {
        const err = e as ApiError;
        console.error('Delete watchlist item failed:', err?.response?.status, err?.response?.data, err?.message);
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
