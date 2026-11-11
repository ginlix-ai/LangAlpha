/**
 * What is on the user's watchlist and in their holdings, without quotes: the
 * entries the Dashboard's hooks poll, kept apart from those hooks so a reader
 * that needs only the lists (the welcome flow, the command palette) loads
 * none of their quote and editing code.
 */
import { queryOptions } from '@tanstack/react-query';
import { registerAuthReset } from '@/lib/authResets';
import { getPortfolio, listWatchlistItems, listWatchlists } from '@/pages/Dashboard/utils/api';

export interface WatchlistItem {
  watchlist_item_id: string;
  symbol: string;
  name?: string | null;
  exchange?: string | null;
  [key: string]: unknown;
}

interface WatchlistItemsData {
  items: WatchlistItem[];
  currentWatchlistId: string | null;
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
 * The watchlist's membership alone, for a reader that needs what is on the
 * list but not its quotes (the welcome flow's holdings entry, the command
 * palette). The same entry useWatchlistData polls, so a symbol added anywhere
 * shows everywhere. The hook adds the polling; a reader that only needs the
 * list does not poll.
 */
export const watchlistItemsQuery = queryOptions({
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
  staleTime: 1000 * 30, // 30s fresh
});

export interface PortfolioHolding {
  user_portfolio_id: string;
  symbol: string;
  name?: string | null;
  exchange?: string | null;
  quantity?: number;
  average_cost?: number | null;
  currency?: string | null;
  notes?: string;
  [key: string]: unknown;
}

/**
 * The holdings list alone, for a reader that needs what is held but not its
 * quotes (the welcome flow's holdings entry, the command palette). The same
 * entry usePortfolioData polls, so a holding added anywhere shows everywhere.
 */
export const portfolioHoldingsQuery = queryOptions({
  queryKey: ['portfolioData'],
  queryFn: async (): Promise<PortfolioHolding[]> => {
    const { holdings: rawHoldings } = await getPortfolio() as { holdings?: PortfolioHolding[] };
    return rawHoldings ?? [];
  },
  staleTime: 1000 * 30, // 30s fresh cache
});
