import { useCallback, useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useToast } from '@/components/ui/use-toast';
import {
  addWatchlistItem,
  deleteWatchlistItem,
  listWatchlists,
} from '../utils/api';
import { watchlistItemsQuery } from '@/hooks/membershipQueries';
import { useQuotes, snapshotToStockPrice } from '@/lib/quotes';
import { quoteCurrency } from '@/lib/bars/exchanges';
import { quotePlaces } from '@/lib/bars/currencyDisplay';
import type { QuoteRow } from '../widgets/framework/snapshotSerializers';
import type { ExtendedSessionFields, StockPrice } from '@/types/market';

export interface WatchlistRow {
  watchlist_item_id: string;
  symbol: string;
  price: number;
  change: number;
  changePercent: number;
  isPositive: boolean;
  quoteAvailable?: boolean;
  /** ISO code the snapshot was quoted in; null on a payload that predates it. */
  currency: string | null;
  /** The snapshot's asset class: an `index` row prints a level, not a price. */
  assetClass: string | null;
  /** Places its prices print up to (lib/bars `quotePlaces`), from the quote's own currency and asset class. */
  places: number;
  session?: ExtendedSessionFields;
  [key: string]: unknown;
}

/** A row as the agent reads it: the figures the user sees, in the currency
 *  the user sees them in, so a CN¥ price never reaches the agent as dollars. */
export function watchlistQuoteRow(r: WatchlistRow): QuoteRow {
  const hasQuote = r.quoteAvailable !== false;
  return {
    symbol: r.symbol,
    price: hasQuote ? r.price : undefined,
    change: hasQuote ? r.change : undefined,
    changePercent: hasQuote ? r.changePercent : undefined,
    currency: quoteCurrency(r.currency, r.symbol, r.assetClass),
  };
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

  // Watchlist membership (items) is its own query so add/delete invalidation and
  // polling still key on ['watchlistData']; the per-symbol quotes come from the
  // shared quote layer so overlapping watchlists/portfolios share one fetch.
  const { data: itemsData = { items: [], currentWatchlistId: null }, isLoading: itemsLoading, isSuccess: loaded, refetch: refetchItems } = useQuery({
    ...watchlistItemsQuery,
    refetchInterval: 60000,
    refetchIntervalInBackground: false,
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
      const price = quoteAvailable ? p.price ?? 0 : 0;
      const currency = p.currency ?? null;
      const assetClass = quotes[sym]?.asset_class ?? null;
      return {
        watchlist_item_id: i.watchlist_item_id,
        symbol: sym,
        price,
        change: quoteAvailable ? p.change ?? 0 : 0,
        changePercent: quoteAvailable ? p.changePercent ?? 0 : 0,
        isPositive: quoteAvailable ? p.isPositive ?? true : true,
        quoteAvailable,
        session: p.session,
        currency,
        assetClass,
        places: p.places ?? quotePlaces(sym, {}),
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
    /** The items arrived, as opposed to a read that failed or never ran. */
    loaded,
    modalOpen,
    setModalOpen,
    currentWatchlistId,
    fetchWatchlist,
    handleAdd,
    handleDelete,
  };
}
