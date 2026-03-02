import React, { useState, useRef, useEffect, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useToast } from '@/components/ui/use-toast';
import './MarketView.css';
import DashboardHeader from '../Dashboard/components/DashboardHeader';
import StockHeader from './components/StockHeader';
import MarketChart from './components/MarketChart';
import ChatInput from '../../components/ui/chat-input';
import MarketPanel from './components/MarketPanel';
import MarketSidebarPanel from './components/MarketSidebarPanel';
import { fetchStockQuote, fetchCompanyOverview, fetchAnalystData } from './utils/api';
import { useMarketChat } from './hooks/useMarketChat';
import { getWorkspaces } from '../ChatAgent/utils/api';
import { ArrowLeft, RefreshCw } from 'lucide-react';
import CompanyOverviewPanel from './components/CompanyOverviewPanel';
import { MarketDataWSProvider, useMarketDataWSContext } from './contexts/MarketDataWSContext';

// --- localStorage persistence helpers (shared key prefix with MarketChart) ---
const STORAGE_PREFIX = 'market-chart:';
function loadPref(key, fallback) {
  try {
    const raw = localStorage.getItem(STORAGE_PREFIX + key);
    return raw !== null ? JSON.parse(raw) : fallback;
  } catch { return fallback; }
}
function savePref(key, value) {
  try { localStorage.setItem(STORAGE_PREFIX + key, JSON.stringify(value)); } catch { /* noop */ }
}

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

function MarketViewInner() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { toast } = useToast();
  const { prices: wsPrices, connectionStatus: wsStatus, subscribe: wsSubscribe, unsubscribe: wsUnsubscribe } = useMarketDataWSContext();
  const [selectedStock, setSelectedStock] = useState(() => loadPref('symbol', 'GOOGL'));
  const [selectedStockDisplay, setSelectedStockDisplay] = useState(null);
  const [stockInfo, setStockInfo] = useState(null);
  const [realTimePrice, setRealTimePrice] = useState(null);
  const [chartMeta, setChartMeta] = useState(null);
  const [selectedInterval, setSelectedInterval] = useState(() => loadPref('interval', '1day'));
  const chartRef = useRef();
  const [chartImage, setChartImage] = useState(null);       // base64 data URL
  const [chartImageDesc, setChartImageDesc] = useState(null); // text description for LLM
  const [showOverview, setShowOverview] = useState(false);
  const [overviewData, setOverviewData] = useState(null);
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [overlayData, setOverlayData] = useState(null);
  const [prefillMessage, setPrefillMessage] = useState('');
  const [mode, setMode] = useState('fast');
  const [workspaces, setWorkspaces] = useState([]);
  const [selectedWorkspaceId, setSelectedWorkspaceId] = useState(null);

  const pickRandomQueries = useCallback((symbol) => {
    const shuffled = [...QUICK_QUERIES].sort(() => Math.random() - 0.5);
    return shuffled.slice(0, 2).map(q => q.replace('{symbol}', symbol));
  }, []);

  const [quickQueries, setQuickQueries] = useState(() => pickRandomQueries(selectedStock));

  // Persist user preferences to localStorage (dedicated effects — no other side effects)
  useEffect(() => { savePref('symbol', selectedStock); }, [selectedStock]);
  useEffect(() => { savePref('interval', selectedInterval); }, [selectedInterval]);

  useEffect(() => {
    setQuickQueries(pickRandomQueries(selectedStock));
  }, [selectedStock, pickRandomQueries]);

  const handleShuffleQueries = useCallback(() => {
    setQuickQueries(pickRandomQueries(selectedStock));
  }, [selectedStock, pickRandomQueries]);

  // Resizable chat panel
  const [chatPanelWidth, setChatPanelWidth] = useState(() =>
    parseInt(localStorage.getItem('market-chat-width')) || 400
  );
  const isDragging = useRef(false);
  const dragStartX = useRef(0);
  const dragStartWidth = useRef(0);

  const { messages, isLoading, error, handleSendMessage: handleFastModeSend } = useMarketChat();

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

  // Subscribe selected stock to WS feed
  useEffect(() => {
    if (!selectedStock) return;
    wsSubscribe([selectedStock]);
    return () => wsUnsubscribe([selectedStock]);
  }, [selectedStock, wsSubscribe, wsUnsubscribe]);

  // Wire WS price → realTimePrice state
  useEffect(() => {
    const wsPrice = wsPrices.get(selectedStock);
    if (wsPrice) setRealTimePrice(wsPrice);
  }, [wsPrices, selectedStock]);

  // Consolidated fetch: stockInfo + realTimePrice from a single API call
  // with AbortController and Page Visibility API.
  // When WS is connected, only fetch once (for stockInfo/name/exchange) — skip polling.
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

    // Suppress 60s polling when WS is connected — WS provides sub-second updates
    if (wsStatus === 'connected') {
      return () => abortController.abort();
    }

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
  }, [selectedStock, wsStatus]);

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

  // Fetch workspaces for the workspace selector (deep mode)
  useEffect(() => {
    let cancelled = false;
    getWorkspaces(50, 0)
      .then((data) => {
        if (cancelled) return;
        const list = (data.workspaces || []).filter((ws) => ws.status !== 'flash');
        setWorkspaces(list);
        if (list.length > 0 && !selectedWorkspaceId) {
          setSelectedWorkspaceId(list[0].workspace_id);
        }
      })
      .catch(() => {});
    return () => { cancelled = true; };
  }, []);

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

    if (overviewData?.quote) {
      if (overviewData.quote.yearHigh != null) parts.push(`52-week high: ${overviewData.quote.yearHigh}`);
      if (overviewData.quote.yearLow != null) parts.push(`52-week low: ${overviewData.quote.yearLow}`);
    }

    if (realTimePrice) {
      parts.push(`Real-time price: $${realTimePrice.price} (${realTimePrice.change >= 0 ? '+' : ''}${realTimePrice.change} / ${realTimePrice.changePercent})`);
    }

    setChartImageDesc(parts.join('\n'));
  }, [selectedStock, selectedInterval, stockInfo, selectedStockDisplay, overviewData, realTimePrice]);

  const handleSendMessage = useCallback(async (message, planMode, attachments = []) => {
    // Build additional_context from chart image + file attachments
    const contexts = [];
    if (chartImage) {
      contexts.push({ type: 'image', data: chartImage, description: chartImageDesc || undefined });
    }
    if (attachments && attachments.length > 0) {
      attachments.forEach((a) => {
        contexts.push({ type: 'image', data: a.dataUrl, description: a.file.name });
      });
    }
    const imageContext = contexts.length > 0 ? contexts : null;

    // Build attachment metadata for display in user message bubble
    const metaItems = [];
    if (chartImage) {
      metaItems.push({
        name: chartImageDesc || 'Chart',
        type: 'image',
        size: 0,
        preview: chartImage,
        dataUrl: chartImage,
      });
    }
    if (attachments && attachments.length > 0) {
      attachments.forEach((a) => {
        metaItems.push({
          name: a.file.name,
          type: a.type,
          size: a.file.size,
          preview: a.preview || null,
          dataUrl: a.dataUrl,
        });
      });
    }
    const attachmentMeta = metaItems.length > 0 ? metaItems : null;

    if (mode === 'fast') {
      handleFastModeSend(message, imageContext, attachmentMeta);
    } else {
      // Deep mode: use selected workspace or fall back to default
      try {
        let workspaceId = selectedWorkspaceId;
        if (!workspaceId) {
          toast({
            variant: 'destructive',
            title: 'No workspace selected',
            description: 'Please create a workspace first to use deep mode.',
          });
          return;
        }

        navigate(`/chat/${workspaceId}/__default__`, {
          state: {
            initialMessage: message,
            planMode: planMode || false,
            additionalContext: imageContext,
            ...(attachmentMeta ? { attachmentMeta } : {}),
          },
        });
      } catch (error) {
        console.error('Error setting up deep mode:', error);
        toast({
          variant: 'destructive',
          title: 'Error',
          description: 'Failed to set up deep mode. Please try again.',
        });
      }
    }
    setChartImage(null);
    setChartImageDesc(null);
  }, [handleFastModeSend, navigate, toast, chartImage, chartImageDesc, mode, selectedWorkspaceId]);

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
      localStorage.setItem('market-chat-width', String(chatPanelWidth));
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [chatPanelWidth]);

  return (
    <div className="market-center-container">
      <DashboardHeader title="LangAlpha" onStockSearch={handleStockSearch} />
      <div className="market-content-wrapper">
        <div className="market-left-panel">
          <StockHeader
            symbol={selectedStock}
            stockInfo={stockInfo}
            realTimePrice={realTimePrice}
            chartMeta={chartMeta}
            displayOverride={selectedStockDisplay}
            onToggleOverview={() => setShowOverview(v => !v)}
            wsStatus={wsStatus}
            quoteData={overviewData?.quote || null}
          />
          <div className="market-chart-area">
            {showOverview && (
              <CompanyOverviewPanel
                symbol={selectedStock}
                visible={showOverview}
                onClose={() => setShowOverview(false)}
                data={overviewData}
                loading={overviewLoading}
              />
            )}
            <MarketChart
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
              liveTick={wsPrices.get(selectedStock)?.barData || null}
              wsStatus={wsStatus}
            />
          </div>
        </div>
        <MarketSidebarPanel
          activeSymbol={selectedStock}
          onSymbolClick={handleSidebarSymbolClick}
        />
        <div className="market-resize-handle" onMouseDown={handleDragStart} />
        <div className="market-right-panel" style={{ width: chatPanelWidth }}>
          <div className="market-right-panel-inner">
            <MarketPanel
              messages={messages}
              isLoading={isLoading}
              error={error}
            />
            {messages.length === 0 && (
              <div className="market-quick-queries">
                {quickQueries.map((q, i) => (
                  <button key={i} className="market-quick-query-card" onClick={() => handleQuickQuery(q)}>
                    {q}
                  </button>
                ))}
                <button className="market-quick-query-shuffle" onClick={handleShuffleQueries} title="Show different suggestions">
                  <RefreshCw size={13} />
                </button>
              </div>
            )}
            <ChatInput
              onSend={handleSendMessage}
              isLoading={isLoading}
              mode={mode}
              onModeChange={setMode}
              workspaces={workspaces}
              selectedWorkspaceId={selectedWorkspaceId}
              onWorkspaceChange={setSelectedWorkspaceId}
              onCaptureChart={handleCaptureChartForContext}
              chartImage={chartImage}
              onRemoveChartImage={() => { setChartImage(null); setChartImageDesc(null); }}
              prefillMessage={prefillMessage}
              onClearPrefill={() => setPrefillMessage('')}
              placeholder="What would you like to know?"
            />
          </div>
        </div>
      </div>
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
            background: 'var(--color-accent-soft)',
            border: '1px solid var(--color-accent-overlay)',
            borderRadius: 10,
            color: 'var(--color-accent-light)',
            fontSize: 13,
            fontWeight: 500,
            cursor: 'pointer',
            backdropFilter: 'blur(12px)',
            boxShadow: '0 4px 20px rgba(0,0,0,0.3)',
            transition: 'background 0.15s, border-color 0.15s',
            zIndex: 50,
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = 'var(--color-accent-overlay)';
            e.currentTarget.style.borderColor = 'var(--color-accent-primary)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = 'var(--color-accent-soft)';
            e.currentTarget.style.borderColor = 'var(--color-accent-overlay)';
          }}
        >
          <ArrowLeft style={{ width: 14, height: 14 }} />
          Return to Chat
        </button>
      )}
    </div>
  );
}

function MarketView() {
  return (
    <MarketDataWSProvider>
      <MarketViewInner />
    </MarketDataWSProvider>
  );
}

export default MarketView;
