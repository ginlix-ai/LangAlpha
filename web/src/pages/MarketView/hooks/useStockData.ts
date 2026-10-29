import { useEffect, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { mapSnapshotToStockQuote, fetchCompanyOverview, fetchAnalystData } from '../utils/api';
import { useQuote } from '@/lib/quotes';
import { isIndexFamilySpelling, isUSEquity } from '@/lib/bars/exchanges';
import { fetchMarketStatus } from '@/lib/marketUtils';
import type { StockInfo, RealTimePrice, SnapshotData } from '@/types/market';
import type { CompanyOverviewArtifact } from '@/lib/quotes/overview';
import type { ConnectionStatus } from './useMarketDataWS';

/** Market status shape returned by fetchMarketStatus */
interface MarketStatusData {
    market?: string;
    afterHours?: boolean;
    earlyHours?: boolean;
    [key: string]: unknown;
}

interface UseStockDataOptions {
    selectedStock: string | null;
    wsStatus: ConnectionStatus;
    setPreviousClose?: (symbol: string, price: number) => void;
    setDayOpen?: (symbol: string, price: number) => void;
}

interface AnalystOverlayData {
    priceTargets: {
        targetHigh?: number;
        targetLow?: number;
        targetConsensus?: number;
        [key: string]: unknown;
    } | null;
    grades: Array<{
        date?: string;
        action?: string;
        [key: string]: unknown;
    }>;
}

export interface UseStockDataReturn {
    stockInfo: StockInfo | null;
    realTimePrice: RealTimePrice | null;
    snapshotData: SnapshotData | null;
    overviewData: CompanyOverviewArtifact | null;
    overviewLoading: boolean;
    overlayData: AnalystOverlayData | null;
    marketStatus: MarketStatusData | null;
}

/**
 * useStockData Hook
 *
 * Extracts data fetching logic out of MarketView to improve modularity.
 * Uses TanStack Query to automatically handle AbortControllers, background refetching,
 * polling intervals, and aggressive caching out-of-the-box.
 */
export function useStockData({
    selectedStock,
    wsStatus,
    setPreviousClose,
    setDayOpen
}: UseStockDataOptions): UseStockDataReturn {
    // 1. Stock Quote & Snapshot — sourced from the unified quote layer so this
    //    symbol shares one cache entry (and one poll) with the sidebar watchlist
    //    / portfolio showing it, and stays consistent with WS write-through.
    const { quote, isLoading: quoteLoading } = useQuote(selectedStock, {
        isIndex: isIndexFamilySpelling(selectedStock),
        // The socket streams US equities only, so it stands in for the poll
        // only there; a CN/HK listing or an index keeps polling every 60s.
        refetchInterval: wsStatus === 'connected' && isUSEquity(selectedStock) ? false : 60000,
        staleTime: 1000 * 10, // 10s fresh cache
    });

    // Derived from the cache entry of the symbol on screen, never mirrored
    // into state: a mirror held the previous symbol's price, currency and name
    // under the new ticker until its quote landed. Null while it is in flight,
    // so the header prints dashes rather than another company's figures.
    const quoteResponse = useMemo(() => {
        if (!selectedStock) return null;
        if (quote) return mapSnapshotToStockQuote(selectedStock, quote);
        if (!quoteLoading) return mapSnapshotToStockQuote(selectedStock, null);
        return null;
    }, [selectedStock, quote, quoteLoading]);

    // Seed the WS refs (previousClose / dayOpen) from the resolved snapshot.
    useEffect(() => {
        if (!selectedStock || !quote) return;
        if (quote.previous_close != null && setPreviousClose) {
            setPreviousClose(selectedStock, quote.previous_close);
        }
        if (quote.open != null && setDayOpen) {
            setDayOpen(selectedStock, quote.open);
        }
    }, [quote, selectedStock, setPreviousClose, setDayOpen]);

    // 2. Company Overview
    const { data: overviewData = null, isLoading: overviewLoading } = useQuery({
        queryKey: ['companyOverview', selectedStock],
        queryFn: ({ signal }) => fetchCompanyOverview(selectedStock!, { signal }),
        enabled: !!selectedStock,
        staleTime: 5 * 60 * 1000, // 5 minutes fresh
    });

    // 3. Analyst Data
    const { data: overlayData = null } = useQuery<AnalystOverlayData | null>({
        queryKey: ['analystData', selectedStock],
        queryFn: async ({ signal }) => {
            const analyst = await fetchAnalystData(selectedStock!, { signal }) as Record<string, unknown> | null;
            return analyst ? {
                priceTargets: (analyst.priceTargets as AnalystOverlayData['priceTargets']) || null,
                grades: (analyst.grades as AnalystOverlayData['grades']) || [],
            } : null;
        },
        enabled: !!selectedStock,
        staleTime: 5 * 60 * 1000, // 5 minutes fresh
    });

    // 4. Market Status
    const { data: marketStatus = null } = useQuery<MarketStatusData | null>({
        queryKey: ['dashboard', 'marketStatus'], // Matches cached value from useDashboardData
        queryFn: fetchMarketStatus,
        refetchInterval: 60000,
        refetchIntervalInBackground: false,
        staleTime: 30000,
    });

    // The quote row is the single source of realTimePrice — live WS ticks
    // override at display time (wsPrices in the consumer), never here; the
    // chart's head bar must not be lifted into it.
    return {
        stockInfo: quoteResponse?.stockInfo ?? null,
        realTimePrice: quoteResponse?.realTimePrice ?? null,
        snapshotData: quoteResponse?.snapshot ?? null,
        overviewData,
        overviewLoading,
        overlayData,
        marketStatus,
    };
}
