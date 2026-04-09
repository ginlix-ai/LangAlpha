import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getPortfolio, getStockPrices } from '../utils/api';
import type { StockPrice } from '@/types/market';

export interface PortfolioRow {
  user_portfolio_id?: string | number;
  symbol: string;
  quantity?: number | null;
  average_cost?: number | null;
  notes?: string;
  price: number;
  marketValue?: number;
  unrealizedPlPercent?: number | null;
  isPositive?: boolean;
  previousClose?: number | null;
  earlyTradingChangePercent?: number | null;
  lateTradingChangePercent?: number | null;
  [key: string]: unknown;
}

interface PortfolioQueryData {
  rows: PortfolioRow[];
  hasRealHoldings: boolean;
}

/**
 * Read-only portfolio data hook.
 * Fetches holdings from Sharesight via the backend proxy,
 * then enriches with real-time prices from market data tools.
 */
export function usePortfolioData() {
  const queryClient = useQueryClient();
  const [lastSyncedAt, setLastSyncedAt] = useState<Date | null>(null);

  const {
    data = { rows: [], hasRealHoldings: false },
    isLoading: loading,
    isFetching: isSyncing,
  } = useQuery<PortfolioQueryData>({
    queryKey: ['portfolioData'],
    queryFn: async (): Promise<PortfolioQueryData> => {
      const { holdings } = await getPortfolio() as {
        holdings?: Array<{
          user_portfolio_id: string;
          symbol: string;
          quantity?: number;
          average_cost?: number | null;
          notes?: string;
          [key: string]: unknown;
        }>;
      };
      const symbols = holdings?.length
        ? holdings.map((h) => String(h.symbol || '').trim().toUpperCase())
        : [];
      const prices: StockPrice[] = symbols.length > 0 ? await getStockPrices(symbols) : [];
      const bySym: Record<string, StockPrice> = Object.fromEntries(
        (prices || []).map((p) => [p.symbol, p])
      );

      if (holdings?.length) {
        const combined: PortfolioRow[] = holdings.map((h) => {
          const sym = String(h.symbol || '').trim().toUpperCase();
          const p = bySym[sym] || ({} as Partial<StockPrice>);
          const q = Number(h.quantity || 0);
          const ac = h.average_cost != null ? Number(h.average_cost) : null;
          const price = p.price ?? 0;
          const marketValue = q * price;
          const plPct = ac != null && ac > 0 ? ((price - ac) / ac) * 100 : null;
          return {
            user_portfolio_id: h.user_portfolio_id,
            symbol: sym,
            quantity: q,
            average_cost: ac,
            notes: h.notes ?? '',
            price,
            marketValue,
            unrealizedPlPercent: plPct,
            isPositive: plPct == null ? true : plPct >= 0,
            previousClose: p.previousClose ?? null,
            earlyTradingChangePercent: p.earlyTradingChangePercent ?? null,
            lateTradingChangePercent: p.lateTradingChangePercent ?? null,
          };
        });
        setLastSyncedAt(new Date());
        return { rows: combined, hasRealHoldings: true };
      }
      setLastSyncedAt(new Date());
      return { rows: [], hasRealHoldings: false };
    },
    refetchInterval: 60000,
    refetchIntervalInBackground: false,
    staleTime: 1000 * 30,
  });

  const { rows, hasRealHoldings } = data;

  const syncPortfolio = () => {
    queryClient.invalidateQueries({ queryKey: ['portfolioData'] });
  };

  return {
    rows,
    loading,
    hasRealHoldings,
    isSyncing,
    lastSyncedAt,
    syncPortfolio,
  };
}
