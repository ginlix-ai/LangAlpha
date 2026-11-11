import { useQuery } from '@tanstack/react-query';
import { portfolioHoldingsQuery, watchlistItemsQuery, type PortfolioHolding, type WatchlistItem } from '@/hooks/membershipQueries';
import type { RecentTicker } from '@/lib/recentTickers';
import type { StockSearchHit } from '@/lib/marketUtils';

/**
 * How long the palette takes the lists the Dashboard keeps as current. Every
 * change made in the app invalidates them, so this only bounds how late a
 * change made elsewhere (another tab, the agent) shows here.
 */
const OWN_STALE_MS = 5 * 60_000;

const hitOf = ({ symbol, name, exchange }: PortfolioHolding | WatchlistItem): StockSearchHit => ({
  symbol,
  name: name ?? undefined,
  exchangeShortName: exchange ?? undefined,
});

/**
 * The user's own tickers, held first, then watched, then opened lately, for
 * the palette to put first among a search's hits. The holdings and watchlist
 * are the Dashboard's own entries, asked for only while a ticker is being
 * searched (`enabled`), and then at most every few minutes, so opening the
 * palette costs no request and a cached list answers at once.
 */
export function useOwnTickers(enabled: boolean, recent: readonly RecentTicker[]): StockSearchHit[] {
  const { data: holdings } = useQuery({ ...portfolioHoldingsQuery, enabled, staleTime: OWN_STALE_MS });
  const { data: watchlist } = useQuery({ ...watchlistItemsQuery, enabled, staleTime: OWN_STALE_MS });
  return [...(holdings ?? []).map(hitOf), ...(watchlist?.items ?? []).map(hitOf), ...recent.map(({ symbol }) => ({ symbol }))];
}
