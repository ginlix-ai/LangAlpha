import { useState, useEffect, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { mapSnapshotToStockQuote, fetchCompanyOverview, fetchAnalystData } from '../utils/api';
import { useQuote } from '@/lib/quotes';
import { fetchMarketStatus } from '@/lib/marketUtils';
import type { StockInfo, RealTimePrice, SnapshotData } from '@/types/market';
import type { ConnectionStatus } from './useMarketDataWS';

type MapperSnapshot = Parameters<typeof mapSnapshotToStockQuote>[1];

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
    overviewData: unknown;
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
    const [stockInfo, setStockInfo] = useState<StockInfo | null>(null);
    const [realTimePrice, setRealTimePrice] = useState<RealTimePrice | null>(null);
    const [snapshotData, setSnapshotData] = useState<SnapshotData | null>(null);

    // 1. Stock Quote & Snapshot — sourced from the unified quote layer so this
    //    symbol shares one cache entry (and one poll) with the sidebar watchlist
    //    / portfolio showing it, and stays consistent with WS write-through.
    const isIndex = !!selectedStock && selectedStock.startsWith('^');
    const { quote, isLoading: quoteLoading } = useQuote(selectedStock, {
        isIndex,
        // Polling: disabled if WS is streaming real-time, otherwise poll every 60s.
        refetchInterval: wsStatus === 'connected' ? false : 60000,
        staleTime: 1000 * 10, // 10s fresh cache
    });

    // Undefined while the first fetch is still in flight so the sync effect below
    // keeps the prior UI state instead of flashing the fallback (matches the old
    // "leave state untouched until the query resolves" behavior).
    const quoteResponse = useMemo(() => {
        if (!selectedStock) return null;
        if (quote) return mapSnapshotToStockQuote(selectedStock, quote as MapperSnapshot);
        if (!quoteLoading) return mapSnapshotToStockQuote(selectedStock, null);
        return undefined;
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

    // The quote row is the single writer of realTimePrice — live WS ticks
    // override at display time (wsPrices in the consumer), never here. Keeping
    // one writer makes the header deterministic across refreshes; the chart's
    // head bar must not be lifted into this state.
    useEffect(() => {
        if (!selectedStock) {
            setStockInfo(null);
            setRealTimePrice(null);
            setSnapshotData(null);
        } else if (quoteResponse) {
            setStockInfo(quoteResponse.stockInfo);
            setRealTimePrice(quoteResponse.realTimePrice);
            setSnapshotData(quoteResponse.snapshot);
        }
    }, [quoteResponse, selectedStock]);

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

    return {
        stockInfo,
        realTimePrice,
        snapshotData,
        overviewData,
        overviewLoading,
        overlayData,
        marketStatus,
    };
}
