import { useQuery } from '@tanstack/react-query';
import { marketStatusQuery, type MarketStatusData } from '@/lib/marketUtils';

/** The session changes a few times a day; a minute's poll catches each change in time. */
const MARKET_STATUS_POLL_MS = 60_000;

/**
 * The US session for quotes on screen (`useQuoteTiles`), which splits each
 * quote at it: read, and polled, only while `active`.
 */
export function useMarketStatus(active: boolean): MarketStatusData | null {
  const { data = null } = useQuery({
    ...marketStatusQuery(),
    enabled: active,
    refetchInterval: active ? MARKET_STATUS_POLL_MS : false,
  });
  return data;
}
