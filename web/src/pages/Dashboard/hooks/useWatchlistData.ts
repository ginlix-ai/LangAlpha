import { useCallback, useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useToast } from '@/components/ui/use-toast';
import {
  addWatchlistItem,
  deleteWatchlistItem,
  listWatchlists,
  listWatchlistItems,
} from '../utils/api';
import { useQuotes, snapshotToStockPrice } from '@/lib/quotes';
import { registerAuthReset } from '@/lib/authResets';
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

interface WatchlistItem {
  watchlist_item_id: string;
  symbol: string;
  [key: string]: unknown;
}

interface WatchlistItemsData {
  items: WatchlistItem[];
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

// Last resolved watchlist id — lets the next fetch request the membership in
// parallel with the watchlist list instead of serializing on it. Persisted so
// the first dashboard paint after a reload is parallel too. A stale id is
// harmless: the speculative call is discarded on mismatch or failure and the
// exact sequential path runs as before.
const LAST_WATCHLIST_ID_KEY = 'watchlist_last_id';

function readLastWatchlistId(): string | null {
  try {
    return localStorage.getItem(LAST_WATCHLIST_ID_KEY);
  } catch {
    return null;
  }
}

function storeLastWatchlistId(id: string): void {
  try {
    localStorage.setItem(LAST_WATCHLIST_ID_KEY, id);
  } catch {
    // localStorage unavailable — every fetch stays sequential, as before
  }
}

// The speculative id outlives React and is account-scoped: after an account
// switch it would aim the parallel prefetch at the previous user's watchlist.
registerAuthReset(() => {
  try {
    localStorage.removeItem(LAST_WATCHLIST_ID_KEY);
  } catch {
    // localStorage unavailable — nothing was persisted to clear
  }
});

/**
 * Shared hook for watchlist data fetching and CRUD operations.
 * Used by both Dashboard and MarketView sidebar.
 * Refactored to use TanStack Query for optimal polling and caching.
 */
export function useWatchlistData() {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const [modalOpen, setModalOpen] = useState(false);

  // Watchlist membership (items) is its own query so add/delete invalidation and
  // polling still key on ['watchlistData']; the per-symbol quotes come from the
  // shared quote layer so overlapping watchlists/portfolios share one fetch.
  const { data: itemsData = { items: [], currentWatchlistId: null }, isLoading: itemsLoading, refetch: refetchItems } = useQuery<WatchlistItemsData>({
    queryKey: ['watchlistData'],
    queryFn: async (): Promise<WatchlistItemsData> => {
      const speculativeId = readLastWatchlistId();
      const speculative = speculativeId
        ? (listWatchlistItems(speculativeId) as Promise<{ items?: WatchlistItem[] }>).catch(() => null)
        : null;

      const { watchlists } = await listWatchlists() as { watchlists?: Array<{ watchlist_id: string; [key: string]: unknown }> };
      const firstWatchlist = watchlists?.[0];
      const watchlistId = firstWatchlist?.watchlist_id || 'default';
      storeLastWatchlistId(watchlistId);

      let items: WatchlistItem[] | undefined;
      if (speculative && speculativeId === watchlistId) {
        items = (await speculative)?.items;
      }
      if (items === undefined) {
        ({ items } = await listWatchlistItems(watchlistId) as { items?: WatchlistItem[] });
      }
      return { items: items ?? [], currentWatchlistId: watchlistId };
    },
    refetchInterval: 60000,
    refetchIntervalInBackground: false,
    staleTime: 1000 * 30, // 30s fresh
  });

  const { items, currentWatchlistId } = itemsData;

  const symbols = useMemo(
    () => items.map((i) => String(i.symbol || '').trim().toUpperCase()).filter(Boolean),
    [items]
  );

  const { quotes, isLoading: quotesLoading, refetch: refetchQuotes } = useQuotes(symbols, {
    staleTime: 1000 * 30,
    refetchInterval: 60000,
  });

  const rows = useMemo<WatchlistRow[]>(() => {
    if (!items.length) return [];
    const bySym: Record<string, StockPrice> = Object.fromEntries(
      symbols.map((s) => [s, snapshotToStockPrice(s, quotes[s])])
    );
    return items.map((i) => {
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
    });
  }, [items, symbols, quotes]);

  // Keep the skeleton up until both membership and its quotes have resolved —
  // preserves the single-query loading semantics widgets relied on.
  const loading = itemsLoading || (symbols.length > 0 && quotesLoading);

  const fetchWatchlist = useCallback(async () => {
    await Promise.all([refetchItems(), Promise.resolve(refetchQuotes())]);
  }, [refetchItems, refetchQuotes]);

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
