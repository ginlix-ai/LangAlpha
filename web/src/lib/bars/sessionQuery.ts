import { queryOptions } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { dateStrInTz } from '@/lib/utils';
import { displaySpelling, regularHoursForSymbol, timezoneForSymbol } from './exchanges';
import { sessionLine } from './intradaySession';
import { fetchStockData } from './legacyBars';

const DAY_MS = 86_400_000;
/**
 * Back far enough that the session before the latest is in the bars over the
 * longest closure a venue takes (Golden Week and the Spring Festival close
 * China's exchanges for nine days with their weekends).
 */
const LOOKBACK_DAYS = 14;

/**
 * A ticker's latest session as a line, from its five-minute bars. Read once a
 * view asks and kept 30 s; nothing polls it. Null when the bars hold no line.
 */
export const sessionLineQuery = (symbol: string) =>
  queryOptions({
    queryKey: queryKeys.intradaySession.detail(displaySpelling(symbol)),
    queryFn: async ({ signal }) => {
      const tz = timezoneForSymbol(symbol);
      const now = Date.now();
      const { data } = await fetchStockData(symbol, '5min', dateStrInTz(now - LOOKBACK_DAYS * DAY_MS, tz), dateStrInTz(now, tz), { signal });
      return sessionLine(data, regularHoursForSymbol(symbol));
    },
    staleTime: 30_000,
    refetchOnWindowFocus: false,
  });
