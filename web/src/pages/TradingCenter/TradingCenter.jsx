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
import { fetchStockQuote, fetchCompanyOverview, fetchAnalystData } from './utils/api';
import { useTradingChat } from './hooks/useTradingChat';
import { findOrCreateDefaultWorkspace } from '../Dashboard/utils/workspace';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '../../components/ui/dialog';
import { Loader2, ArrowLeft, RefreshCw } from 'lucide-react';
import CompanyOverviewPanel from './components/CompanyOverviewPanel';

const QUICK_QUERIES = [
  'Analyze the technical setup of {symbol}',
  'What are the key support and resistance levels for {symbol}?',
  'Summarize the trend and momentum indicators for {symbol}',
  'What signals are the moving averages showing for {symbol}?',
  'Analyze the RSI and volume patterns for {symbol}',
  'Identify any chart patterns forming on {symbol}',
  'How is {symbol} performing relative to its 52-week range?',
  "What's the MACD crossover status for {symbol}?",
];

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
  const [prefillMessage, setPrefillMessage] = useState('');

  const pickRandomQueries = useCallback((symbol) => {
    const shuffled = [...QUICK_QUERIES].sort(() => Math.random() - 0.5);
    return shuffled.slice(0, 2).map(q => q.replace('{symbol}', symbol));
  }, []);

  const [quickQueries, setQuickQueries] = useState(() => pickRandomQueries(selectedStock));

  useEffect(() => {
    setQuickQueries(pickRandomQueries(selectedStock));
  }, [selectedStock, pickRandomQueries]);

  const handleShuffleQueries = useCallback(() => {
    setQuickQueries(pickRandomQueries(selectedStock));
  }, [selectedStock, pickRandomQueries]);

  // Resizable chat panel
  const [chatPanelWidth, setChatPanelWidth] = useState(() =>
    parseInt(localStorage.getItem('trading-chat-width')) || 400
  );
  const isDragging = useRef(false);
  const dragStartX = useRef(0);
  const dragStartWidth = useRef(0);

  const { messages, isLoading, error, handleSendMessage: handleFastModeSend } = useTradingChat();

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
    if (meta?.chartMode) parts.push(`Chart mode: ${meta.chartMode}`);
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

  const handleQuickQuery = useCallback(async (queryText) => {
    await handleCaptureChartForContext();
    setPrefillMessage(queryText);
  }, [handleCaptureChartForContext]);

  const handleIntervalChange = useCallback((interval) => {
    setSelectedInterval(interval);
  }, []);

  const handleStockMeta = useCallback((meta) => {
    setChartMeta(meta);
  }, []);

  const handleDragStart = useCallback((e) => {
    e.preventDefault();
    isDragging.current = true;
    dragStartX.current = e.clientX;
    dragStartWidth.current = chatPanelWidth;
    document.body.classList.add('col-resizing');
  }, [chatPanelWidth]);

  useEffect(() => {
    const handleMouseMove = (e) => {
      if (!isDragging.current) return;
      const delta = dragStartX.current - e.clientX;
      const newWidth = Math.min(700, Math.max(300, dragStartWidth.current + delta));
      setChatPanelWidth(newWidth);
    };

    const handleMouseUp = () => {
      if (!isDragging.current) return;
      isDragging.current = false;
      document.body.classList.remove('col-resizing');
      localStorage.setItem('trading-chat-width', String(chatPanelWidth));
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [chatPanelWidth]);

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
        <div className="trading-resize-handle" onMouseDown={handleDragStart} />
        <div className="trading-right-panel" style={{ width: chatPanelWidth }}>
          <div className="trading-right-panel-inner">
            <TradingPanel
              messages={messages}
              isLoading={isLoading}
              error={error}
            />
            {messages.length === 0 && (
              <div className="trading-quick-queries">
                {quickQueries.map((q, i) => (
                  <button key={i} className="trading-quick-query-card" onClick={() => handleQuickQuery(q)}>
                    {q}
                  </button>
                ))}
                <button className="trading-quick-query-shuffle" onClick={handleShuffleQueries} title="Show different suggestions">
                  <RefreshCw size={13} />
                </button>
              </div>
            )}
            <TradingChatInput
              onSend={handleSendMessage}
              isLoading={isLoading}
              onCaptureChart={handleCaptureChartForContext}
              chartImage={chartImage}
              onRemoveChartImage={() => { setChartImage(null); setChartImageDesc(null); }}
              prefillMessage={prefillMessage}
              onClearPrefill={() => setPrefillMessage('')}
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
