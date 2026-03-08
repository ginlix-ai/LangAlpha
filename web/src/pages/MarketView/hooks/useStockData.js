import { useState, useEffect, useRef, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import { fetchStockQuote, fetchCompanyOverview, fetchAnalystData } from '../utils/api';
import { fetchMarketStatus } from '@/lib/marketUtils';

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
}) {
    const [stockInfo, setStockInfo] = useState(null);
    const [realTimePrice, setRealTimePrice] = useState(null);
    const [snapshotData, setSnapshotData] = useState(null);

    // 1. Stock Quote & Snapshot
    const { data: quoteResponse } = useQuery({
        queryKey: ['stockQuote', selectedStock],
        queryFn: async ({ signal }) => {
            if (!selectedStock) return null;
            try {
                const data = await fetchStockQuote(selectedStock, { signal });

                // Side effects mapping for WS refs
                if (data.snapshot) {
                    if (data.snapshot.previous_close != null && setPreviousClose) {
                        setPreviousClose(selectedStock, data.snapshot.previous_close);
                    }
                    if (data.snapshot.open != null && setDayOpen) {
                        setDayOpen(selectedStock, data.snapshot.open);
                    }
                }
                return data;
            } catch (error) {
                // If the stock is bogus or fetch fails, fallback so UI has skeleton data
                return {
                    stockInfo: {
                        Symbol: selectedStock,
                        Name: `${selectedStock} Corp`,
                        Exchange: 'NASDAQ',
                    },
                    realTimePrice: null,
                    snapshot: null,
                };
            }
        },
        // Polling: disabled if WS is streaming real-time, otherwise poll every 60s
        refetchInterval: wsStatus === 'connected' ? false : 60000,
        enabled: !!selectedStock,
        staleTime: 1000 * 10, // 10s fresh cache 
    });

    // Isolate pure UI state for the realtime bar updates 
    // This allows WebSocket to update local state extremely fast 
    // without triggering React Query cache updates on every tick.
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
        queryFn: ({ signal }) => fetchCompanyOverview(selectedStock, { signal }),
        enabled: !!selectedStock,
        staleTime: 5 * 60 * 1000, // 5 minutes fresh
    });

    // 3. Analyst Data
    const { data: overlayData = null } = useQuery({
        queryKey: ['analystData', selectedStock],
        queryFn: async ({ signal }) => {
            const analyst = await fetchAnalystData(selectedStock, { signal });
            return analyst ? {
                priceTargets: analyst.priceTargets || null,
                grades: analyst.grades || [],
            } : null;
        },
        enabled: !!selectedStock,
        staleTime: 5 * 60 * 1000, // 5 minutes fresh
    });

    // 4. Market Status
    const { data: marketStatus = null } = useQuery({
        queryKey: ['dashboard', 'marketStatus'], // Matches cached value from useDashboardData
        queryFn: fetchMarketStatus,
        refetchInterval: 60000,
        staleTime: 30000,
    });

    // WebSocket Update Handler (mutates local realTimePrice state)
    const stockInfoRef = useRef(stockInfo);
    useEffect(() => { stockInfoRef.current = stockInfo; }, [stockInfo]);

    const handleLatestBar = useCallback((bar) => {
        if (!bar?.close) return;
        setRealTimePrice((prev) => {
            if (!prev || !prev.price) return prev;
            const updatedPrice = Math.round(bar.close * 100) / 100;
            // Use previousClose from snapshot if available, else derive from initial quote
            const previousClose = prev.previousClose ?? ((prev.price ?? 0) - (prev.change ?? 0));
            if (!previousClose) {
                // Still update price even without previousClose — just skip change% recalculation
                return { ...prev, price: updatedPrice, close: bar.close, timestamp: bar.time * 1000 };
            }
            const change = bar.close - previousClose;
            const changePct = parseFloat(((change / previousClose) * 100).toFixed(2));
            return {
                ...prev,
                price: updatedPrice,
                close: bar.close,
                change: Math.round(change * 100) / 100,
                changePercent: changePct,
                timestamp: bar.time * 1000,
            };
        });
    }, []);

    return {
        stockInfo,
        setStockInfo,
        realTimePrice,
        setRealTimePrice,
        snapshotData,
        setSnapshotData,
        overviewData,
        overviewLoading,
        overlayData,
        marketStatus,
        handleLatestBar
    };
}
