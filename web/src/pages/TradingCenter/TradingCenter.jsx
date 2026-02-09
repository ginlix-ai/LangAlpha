import React, { useState, useRef, useEffect, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useToast } from '@/components/ui/use-toast';
import './TradingCenter.css';
import DashboardHeader from '../Dashboard/components/DashboardHeader';
import StockHeader from './components/StockHeader';
import TradingChart from './components/TradingChart';
import TradingChatInput from './components/TradingChatInput';
import TradingPanel from './components/TradingPanel';
import TradingSidebarPanel from './components/TradingSidebarPanel';
import { getAuthUserId } from '@/api/client';
import { DEFAULT_USER_ID } from '@/api/client';
import { fetchStockQuote, fetchCompanyOverview, fetchAnalystData } from './utils/api';
import { useTradingChat } from './hooks/useTradingChat';
import { deleteFlashWorkspaces } from './utils/api';
import { findOrCreateDefaultWorkspace } from '../Dashboard/utils/workspace';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '../../components/ui/dialog';
import { Loader2, ArrowLeft } from 'lucide-react';
import CompanyOverviewPanel from './components/CompanyOverviewPanel';

function TradingCenter() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { toast } = useToast();
  const [selectedStock, setSelectedStock] = useState('GOOGL');
  const [selectedStockDisplay, setSelectedStockDisplay] = useState(null);
  const [stockInfo, setStockInfo] = useState(null);
  const [realTimePrice, setRealTimePrice] = useState(null);
  const [chartMeta, setChartMeta] = useState(null);
  const [selectedInterval, setSelectedInterval] = useState('1day');
  const chartRef = useRef();
  const [chartImage, setChartImage] = useState(null);       // base64 data URL
  const [chartImageDesc, setChartImageDesc] = useState(null); // text description for LLM
  const [isCreatingWorkspace, setIsCreatingWorkspace] = useState(false);
  const [showOverview, setShowOverview] = useState(false);
  const [overviewData, setOverviewData] = useState(null);
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [overlayData, setOverlayData] = useState(null);

  const userId = getAuthUserId() || DEFAULT_USER_ID;
  const { messages, isLoading, error, handleSendMessage: handleFastModeSend } = useTradingChat(userId);

  // Chat return path — captured from URL when navigating from chat DetailPanel
  const [chatReturnPath, setChatReturnPath] = useState(null);

  // Handle URL parameters (symbol + returnTo from chat context)
  useEffect(() => {
    const symbolParam = searchParams.get('symbol');
    const returnToParam = searchParams.get('returnTo');
    if (symbolParam) {
      const symbol = symbolParam.trim().toUpperCase();
      if (symbol && symbol !== selectedStock) {
        setSelectedStock(symbol);
        setSelectedStockDisplay(null);
        setChartMeta(null);
      }
    }
    if (returnToParam) {
      setChatReturnPath(returnToParam);
    }
    // Clear all URL parameters after applying them
    if (symbolParam || returnToParam) {
      setSearchParams({});
    }
  }, [searchParams, selectedStock, setSearchParams]);

  // Cleanup: Delete flash workspaces when component unmounts (navigation away or refresh)
  useEffect(() => {
    return () => {
      deleteFlashWorkspaces(userId).catch((err) => {
        console.warn('[TradingCenter] Error deleting flash workspaces on unmount:', err);
      });
    };
  }, [userId]);

  const handleStockSearch = useCallback((symbol, searchResult) => {
    setSelectedStock(symbol);
    setSelectedStockDisplay(
      searchResult
        ? {
            name: searchResult.name || searchResult.symbol,
            exchange: searchResult.exchangeShortName || searchResult.stockExchange || '',
          }
        : null
    );
    setChartMeta(null);
    setShowOverview(false);
  }, []);

  // Consolidated fetch: stockInfo + realTimePrice from a single API call
  // with AbortController and Page Visibility API
  useEffect(() => {
    if (!selectedStock) return;

    const abortController = new AbortController();

    const loadStockQuote = async () => {
      try {
        const { stockInfo: info, realTimePrice: price } = await fetchStockQuote(
          selectedStock,
          { signal: abortController.signal }
        );
        setStockInfo(info);
        if (price) setRealTimePrice(price);
      } catch (error) {
        if (error?.name === 'CanceledError' || error?.name === 'AbortError') return;
        console.error('Error loading stock quote:', error);
        setStockInfo({
          Symbol: selectedStock,
          Name: `${selectedStock} Corp`,
          Exchange: 'NASDAQ',
        });
      }
    };

    loadStockQuote();

    // Refresh price every 60s, but skip when tab is hidden (Page Visibility API)
    const priceInterval = setInterval(async () => {
      if (document.hidden) return; // Skip fetch when tab is not visible
      try {
        const { stockInfo: info, realTimePrice: price } = await fetchStockQuote(selectedStock);
        setStockInfo(info);
        if (price) setRealTimePrice(price);
      } catch (error) {
        console.error('Error refreshing stock quote:', error);
      }
    }, 60000);

    return () => {
      abortController.abort();
      clearInterval(priceInterval);
    };
  }, [selectedStock]);

  // Fetch company overview data (lifted from CompanyOverviewPanel)
  useEffect(() => {
    if (!selectedStock) return;
    const ac = new AbortController();
    setOverviewLoading(true);
    fetchCompanyOverview(selectedStock, { signal: ac.signal })
      .then((result) => {
        setOverviewData(result);
      })
      .catch((err) => {
        if (err?.name === 'CanceledError' || err?.name === 'AbortError') return;
        console.error('Error fetching company overview:', err);
        setOverviewData(null);
      })
      .finally(() => setOverviewLoading(false));
    return () => ac.abort();
  }, [selectedStock]);

  // Fetch analyst data (price targets + grades) for chart overlays
  useEffect(() => {
    if (!selectedStock) return;
    const ac = new AbortController();
    fetchAnalystData(selectedStock, { signal: ac.signal })
      .then((analyst) => {
        setOverlayData(analyst ? {
          priceTargets: analyst.priceTargets || null,
          grades: analyst.grades || [],
        } : null);
      })
      .catch((err) => {
        if (err?.name === 'CanceledError' || err?.name === 'AbortError') return;
        setOverlayData(null);
      });
    return () => ac.abort();
  }, [selectedStock]);

  const handleCaptureChart = useCallback(async () => {
    if (!chartRef.current) return;
    try {
      const blob = await chartRef.current.captureChart();
      if (blob) {
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${selectedStock}_chart_${new Date().getTime()}.png`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
      }
    } catch (error) {
      console.error('Chart capture failed:', error);
    }
  }, [selectedStock]);

  const handleCaptureChartForContext = useCallback(async () => {
    if (!chartRef.current) return;
    const dataUrl = await chartRef.current.captureChartAsDataUrl();
    if (!dataUrl) return;

    setChartImage(dataUrl);

    // Build rich description from available metadata
    const meta = chartRef.current.getChartMetadata?.();
    const intervalLabel = selectedInterval === '1day' ? 'Daily' : selectedInterval;
    const companyName = stockInfo?.Name || selectedStockDisplay?.name || selectedStock;
    const exchange = stockInfo?.Exchange || selectedStockDisplay?.exchange || '';

    const parts = [`Chart: ${selectedStock} (${companyName})${exchange ? ` — ${exchange}` : ''}`];
    parts.push(`Interval: ${intervalLabel}`);

    if (meta) {
      parts.push(`Date range: ${meta.dateRange.from} to ${meta.dateRange.to} (${meta.dataPoints} bars)`);

      if (meta.maDescription) {
        parts.push(`Moving Averages shown: ${meta.maDescription}`);
      }
      parts.push(`RSI(${meta.rsiPeriod}): ${meta.rsiValue ?? 'N/A'}`);

      const c = meta.lastCandle;
      parts.push(`Latest candle — O: ${c.open} H: ${c.high} L: ${c.low} C: ${c.close} Vol: ${c.volume?.toLocaleString()}`);
    }

    if (chartMeta) {
      if (chartMeta.fiftyTwoWeekHigh != null) parts.push(`52-week high: ${chartMeta.fiftyTwoWeekHigh}`);
      if (chartMeta.fiftyTwoWeekLow != null) parts.push(`52-week low: ${chartMeta.fiftyTwoWeekLow}`);
    }

    if (realTimePrice) {
      parts.push(`Real-time price: $${realTimePrice.price} (${realTimePrice.change >= 0 ? '+' : ''}${realTimePrice.change} / ${realTimePrice.changePercent})`);
    }

    setChartImageDesc(parts.join('\n'));
  }, [selectedStock, selectedInterval, stockInfo, selectedStockDisplay, chartMeta, realTimePrice]);

  const handleSendMessage = useCallback(async (message, mode, image) => {
    // Build additional_context with image + description bundled together
    const imageContext = image
      ? [{ type: 'image', data: image, description: chartImageDesc || undefined }]
      : null;

    if (mode === 'fast') {
      handleFastModeSend(message, imageContext);
    } else {
      // Deep mode: navigate to ChatAgent with initial message
      try {
        setIsCreatingWorkspace(true);

        const workspaceId = await findOrCreateDefaultWorkspace(
          () => {},
          () => {}
        );

        setIsCreatingWorkspace(false);

        navigate(`/chat/${workspaceId}/__default__`, {
          state: {
            initialMessage: message,
            planMode: false,
            additionalContext: imageContext,
          },
        });
      } catch (error) {
        console.error('Error setting up deep mode:', error);
        toast({
          variant: 'destructive',
          title: 'Error',
          description: 'Failed to set up deep mode. Please try again.',
        });
        setIsCreatingWorkspace(false);
      }
    }
    setChartImage(null);
    setChartImageDesc(null);
  }, [handleFastModeSend, navigate, toast, chartImageDesc]);

  const handleSidebarSymbolClick = useCallback((symbol) => {
    setSelectedStock(symbol);
    setSelectedStockDisplay(null);
    setChartMeta(null);
    setShowOverview(false);
  }, []);

  const handleIntervalChange = useCallback((interval) => {
    setSelectedInterval(interval);
  }, []);

  const handleStockMeta = useCallback((meta) => {
    setChartMeta(meta);
  }, []);

  return (
    <div className="trading-center-container">
      <DashboardHeader title="LangAlpha" onStockSearch={handleStockSearch} />
      <div className="trading-content-wrapper">
        <div className="trading-left-panel">
          <StockHeader
            symbol={selectedStock}
            stockInfo={stockInfo}
            realTimePrice={realTimePrice}
            chartMeta={chartMeta}
            displayOverride={selectedStockDisplay}
            onToggleOverview={() => setShowOverview(v => !v)}
          />
          <div className="trading-chart-area">
            {showOverview && (
              <CompanyOverviewPanel
                symbol={selectedStock}
                visible={showOverview}
                onClose={() => setShowOverview(false)}
                data={overviewData}
                loading={overviewLoading}
              />
            )}
            <TradingChart
              ref={chartRef}
              symbol={selectedStock}
              interval={selectedInterval}
              onIntervalChange={handleIntervalChange}
              onCapture={handleCaptureChart}
              onStockMeta={handleStockMeta}
              quoteData={overviewData?.quote || null}
              earningsData={overviewData?.earningsSurprises || null}
              overlayData={overlayData}
              stockMeta={chartMeta}
            />
          </div>
        </div>
        <TradingSidebarPanel
          activeSymbol={selectedStock}
          onSymbolClick={handleSidebarSymbolClick}
        />
        <div className="trading-right-panel">
          <div className="trading-right-panel-inner">
            <TradingChatInput
              onSend={handleSendMessage}
              isLoading={isLoading}
              onCaptureChart={handleCaptureChartForContext}
              chartImage={chartImage}
              onRemoveChartImage={() => { setChartImage(null); setChartImageDesc(null); }}
            />
            <TradingPanel
              messages={messages}
              isLoading={isLoading}
              error={error}
            />
          </div>
        </div>
      </div>
      {isCreatingWorkspace && (
        <Dialog open={isCreatingWorkspace} onOpenChange={() => {}}>
          <DialogContent className="sm:max-w-md text-white border" style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}>
            <DialogHeader>
              <DialogTitle className="dashboard-title-font" style={{ color: 'var(--color-text-primary)' }}>
                Setting up Deep Mode
              </DialogTitle>
              <DialogDescription style={{ color: 'var(--color-text-secondary)' }}>
                Please wait while we set up your workspace...
              </DialogDescription>
            </DialogHeader>
            <div className="flex items-center justify-center py-4">
              <Loader2 className="h-6 w-6 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
            </div>
          </DialogContent>
        </Dialog>
      )}

      {/* Floating "Return to Chat" card — shown when navigated from chat context */}
      {chatReturnPath && (
        <button
          onClick={() => navigate(chatReturnPath)}
          style={{
            position: 'fixed',
            bottom: 24,
            right: 416,
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            padding: '10px 16px',
            background: 'rgba(97, 85, 245, 0.15)',
            border: '1px solid rgba(97, 85, 245, 0.35)',
            borderRadius: 10,
            color: '#c4bfff',
            fontSize: 13,
            fontWeight: 500,
            cursor: 'pointer',
            backdropFilter: 'blur(12px)',
            boxShadow: '0 4px 20px rgba(0,0,0,0.3)',
            transition: 'background 0.15s, border-color 0.15s',
            zIndex: 50,
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = 'rgba(97, 85, 245, 0.25)';
            e.currentTarget.style.borderColor = 'rgba(97, 85, 245, 0.5)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = 'rgba(97, 85, 245, 0.15)';
            e.currentTarget.style.borderColor = 'rgba(97, 85, 245, 0.35)';
          }}
        >
          <ArrowLeft style={{ width: 14, height: 14 }} />
          Return to Chat
        </button>
      )}
    </div>
  );
}

export default TradingCenter;
