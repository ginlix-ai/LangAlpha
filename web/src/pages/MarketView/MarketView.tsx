import React, { useState, useRef, useEffect, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useToast } from '@/components/ui/use-toast';
import './MarketView.css';
import DashboardHeader from '../Dashboard/components/DashboardHeader';
import StockHeader from './components/StockHeader';
import MarketChart from './components/MarketChart';
import type { MarketChartHandle } from './components/MarketChart';
import ChatInput from '../../components/ui/chat-input';
import MarketPanel from './components/MarketPanel';
import MarketSidebarPanel from './components/MarketSidebarPanel';
import { supports1sInterval } from './utils/chartConstants';
import { useMarketChat } from './hooks/useMarketChat';
import { getWorkspaces } from '../ChatAgent/utils/api';
import { motion, AnimatePresence } from 'framer-motion';
import { ArrowLeft, RefreshCw } from 'lucide-react';
import CompanyOverviewPanel from './components/CompanyOverviewPanel';
import { MobileBottomSheet } from '../../components/ui/mobile-bottom-sheet';
import { MobileFabChat } from '../../components/ui/mobile-fab-chat';
import { MarketDataWSProvider, useMarketDataWSContext } from './contexts/MarketDataWSContext';

import { loadPref, savePref } from './utils/prefs';
import { useIsMobile } from '@/hooks/useIsMobile';

import { useStockData } from './hooks/useStockData';

interface SearchResult {
  name?: string;
  symbol?: string;
  exchangeShortName?: string;
  stockExchange?: string;
  [key: string]: unknown;
}

interface DisplayOverride {
  name: string;
  exchange: string;
}

interface AttachmentItem {
  dataUrl: string;
  file: { name: string; size: number };
  type: string;
  preview?: string | null;
}

interface Workspace {
  workspace_id: string;
  name?: string;
  status?: string;
  [key: string]: unknown;
}

// TODO: type properly once overview API response shape is formalized
interface OverviewData {
  symbol?: string;
  name?: string;
  quote?: {
    previousClose?: number;
    open?: number;
    yearHigh?: number;
    yearLow?: number;
    avgVolume?: number;
    [key: string]: unknown;
  };
  earningsSurprises?: unknown;
  [key: string]: unknown;
}

interface ChartMetadata {
  chartMode?: string;
  dateRange: { from: string; to: string };
  dataPoints: number;
  maDescription?: string;
  rsiPeriod: number;
  rsiValue?: string | null;
  lastCandle: {
    open: number;
    high: number;
    low: number;
    close: number;
    volume?: number;
  };
  [key: string]: unknown;
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
  const { prices: wsPrices, connectionStatus: wsStatus, dataLevel: wsDataLevel, ginlixDataEnabled, subscribe: wsSubscribe, unsubscribe: wsUnsubscribe, setPreviousClose, setDayOpen } = useMarketDataWSContext();
  const [selectedStock, setSelectedStock] = useState<string>(() => loadPref('symbol', 'GOOGL'));
  const [selectedStockDisplay, setSelectedStockDisplay] = useState<DisplayOverride | null>(null);

  const {
    stockInfo,
    realTimePrice,
    snapshotData,
    overviewData,
    overviewLoading,
    overlayData,
    marketStatus,
    handleLatestBar
  } = useStockData({
    selectedStock,
    wsStatus,
    setPreviousClose,
    setDayOpen
  });

  const [chartMeta, setChartMeta] = useState<Record<string, unknown> | null>(null);
  const [selectedInterval, setSelectedInterval] = useState<string>(() => loadPref('interval', '1day'));
  const chartRef = useRef<MarketChartHandle>(null);
  const [chartImage, setChartImage] = useState<string | null>(null);       // base64 data URL
  const [chartImageDesc, setChartImageDesc] = useState<string | null>(null); // text description for LLM
  const [showOverview, setShowOverview] = useState<boolean>(false);
  const [mobileTab, setMobileTab] = useState<'watchlist' | null>(null);
  const [chatExpanded, setChatExpanded] = useState(false);
  const isMobile = useIsMobile();

  const [prefillMessage, setPrefillMessage] = useState<string>('');
  const [mode, setMode] = useState<'fast' | 'deep'>('fast');
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [selectedWorkspaceId, setSelectedWorkspaceId] = useState<string | null>(null);

  const pickRandomQueries = useCallback((symbol: string): string[] => {
    const shuffled = [...QUICK_QUERIES].sort(() => Math.random() - 0.5);
    return shuffled.slice(0, 2).map(q => q.replace('{symbol}', symbol));
  }, []);

  const [quickQueries, setQuickQueries] = useState<string[]>(() => pickRandomQueries(selectedStock));

  // Persist user preferences to localStorage (dedicated effects — no other side effects)
  useEffect(() => { savePref('symbol', selectedStock); }, [selectedStock]);
  useEffect(() => { savePref('interval', selectedInterval); }, [selectedInterval]);

  // Auto-downgrade 1s → 1m when the current symbol doesn't support 1s
  useEffect(() => {
    if (selectedInterval === '1s' && !supports1sInterval(selectedStock)) {
      setSelectedInterval('1min');
    }
  }, [selectedStock, selectedInterval]);

  useEffect(() => {
    setQuickQueries(pickRandomQueries(selectedStock));
  }, [selectedStock, pickRandomQueries]);

  const handleShuffleQueries = useCallback(() => {
    setQuickQueries(pickRandomQueries(selectedStock));
  }, [selectedStock, pickRandomQueries]);

  // Resizable chat panel
  const [chatPanelWidth, setChatPanelWidth] = useState<number>(() =>
    parseInt(localStorage.getItem('market-chat-width') || '400') || 400
  );
  const isDragging = useRef<boolean>(false);
  const dragStartX = useRef<number>(0);
  const dragStartWidth = useRef<number>(0);

  const { messages, isLoading, error, handleSendMessage: handleFastModeSend } = useMarketChat();

  // Chat return path — captured from URL when navigating from chat DetailPanel
  const [chatReturnPath, setChatReturnPath] = useState<string | null>(null);

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

  const handleStockSearch = useCallback((symbol: string, searchResult?: SearchResult | null) => {
    setSelectedStock(symbol);
    setSelectedStockDisplay(
      searchResult
        ? {
          name: searchResult.name || searchResult.symbol || symbol,
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

  // Display price: prefer WS live data over REST. Only use realTimePrice if it
  // belongs to the current symbol (prevents stale data flash when switching tickers).
  const realTimePriceMatch = realTimePrice?.symbol === selectedStock ? realTimePrice : null;
  const displayPrice = wsPrices.get(selectedStock) || realTimePriceMatch;

  // Fetch workspaces for the workspace selector (deep mode)
  useEffect(() => {
    let cancelled = false;
    getWorkspaces(50, 0)
      .then((data: Record<string, unknown>) => {
        if (cancelled) return;
        const list = ((data.workspaces || []) as Workspace[]).filter((ws) => ws.status !== 'flash');
        setWorkspaces(list);
        if (list.length > 0 && !selectedWorkspaceId) {
          setSelectedWorkspaceId(list[0].workspace_id);
        }
      })
      .catch(() => { });
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
    const meta = chartRef.current.getChartMetadata?.() as ChartMetadata | null;
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

    const overview = overviewData as OverviewData | null;
    if (overview?.quote) {
      if (overview.quote.yearHigh != null) parts.push(`52-week high: ${overview.quote.yearHigh}`);
      if (overview.quote.yearLow != null) parts.push(`52-week low: ${overview.quote.yearLow}`);
    }

    if (displayPrice) {
      parts.push(`Real-time price: $${displayPrice.price} (${displayPrice.change >= 0 ? '+' : ''}${displayPrice.change} / ${displayPrice.changePercent.toFixed(2)}%)`);
    }

    setChartImageDesc(parts.join('\n'));
  }, [selectedStock, selectedInterval, stockInfo, selectedStockDisplay, overviewData, displayPrice]);

  const handleSendMessage = useCallback(async (message: string, planMode: boolean, attachments: AttachmentItem[] = [], _slashCommands: string[] = [], { model, reasoningEffort }: { model?: string; reasoningEffort?: string } = {}) => {
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

        navigate(`/chat/t/__default__`, {
          state: {
            workspaceId,
            initialMessage: message,
            planMode: planMode || false,
            additionalContext: imageContext,
            ...(attachmentMeta ? { attachmentMeta } : {}),
            ...(model ? { model } : {}),
            ...(reasoningEffort ? { reasoningEffort } : {}),
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

  const handleSidebarSymbolClick = useCallback((symbol: string) => {
    setSelectedStock(symbol);
    setSelectedStockDisplay(null);
    setChartMeta(null);
    setShowOverview(false);
  }, []);

  const handleQuickQuery = useCallback(async (queryText: string) => {
    await handleCaptureChartForContext();
    setPrefillMessage(queryText);
  }, [handleCaptureChartForContext]);

  const handleIntervalChange = useCallback((interval: string) => {
    setSelectedInterval(interval);
  }, []);

  const handleStockMeta = useCallback((meta: Record<string, unknown> | null) => {
    setChartMeta(meta);
  }, []);

  const handleDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDragging.current = true;
    dragStartX.current = e.clientX;
    dragStartWidth.current = chatPanelWidth;
    document.body.classList.add('col-resizing');
  }, [chatPanelWidth]);

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isDragging.current) return;
      const delta = dragStartX.current - e.clientX;
      const newWidth = Math.min(Math.min(700, window.innerWidth * 0.4), Math.max(300, dragStartWidth.current + delta));
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
      <DashboardHeader onStockSearch={handleStockSearch as any} />
      {isMobile ? (
        <div className="market-mobile-layout">
          <StockHeader
            symbol={selectedStock}
            stockInfo={stockInfo}
            realTimePrice={displayPrice}
            chartMeta={chartMeta}
            displayOverride={selectedStockDisplay}
            onToggleOverview={() => setShowOverview(v => !v)}
            onOpenWatchlist={() => setMobileTab('watchlist')}
            wsStatus={wsStatus}
            wsHasData={!!wsPrices.get(selectedStock)}
            wsDataLevel={wsDataLevel}
            ginlixDataEnabled={ginlixDataEnabled}
            quoteData={(overviewData as OverviewData | null)?.quote || null}
            marketStatus={marketStatus}
            snapshot={snapshotData}
          />

          {/* Chart fills remaining space */}
          <div className="market-chart-area" style={{ flex: 1, minHeight: 0 }}>
            <MarketChart
              ref={chartRef}
              symbol={selectedStock}
              interval={selectedInterval}
              onIntervalChange={handleIntervalChange}
              onCapture={handleCaptureChart}
              onStockMeta={handleStockMeta as any}
              onLatestBar={handleLatestBar}
              quoteData={(overviewData as OverviewData | null)?.quote || null}
              earningsData={(overviewData as OverviewData | null)?.earningsSurprises || null}
              overlayData={overlayData as Record<string, unknown> | null}
              stockMeta={chartMeta}
              snapshot={snapshotData}
              liveTick={wsPrices.get(selectedStock)?.barData || null}
              wsStatus={wsStatus}
              ginlixDataEnabled={ginlixDataEnabled}
              marketStatus={marketStatus}
            />
          </div>

          {/* Floating chat input — FAB on mobile, expands on tap */}
          <MobileFabChat
            expanded={chatExpanded}
            onExpand={() => setChatExpanded(true)}
            onCollapse={() => setChatExpanded(false)}
            className="market-mobile-chat-float"
          >
            <ChatInput
              onSend={(...args: any[]) => { (handleSendMessage as any)(...args); setChatExpanded(false); }}
              isLoading={isLoading}
              mode={mode}
              onModeChange={setMode as any}
              workspaces={workspaces as any}
              selectedWorkspaceId={selectedWorkspaceId}
              onWorkspaceChange={setSelectedWorkspaceId}
              onCaptureChart={handleCaptureChartForContext}
              chartImage={chartImage}
              onRemoveChartImage={() => { setChartImage(null); setChartImageDesc(null); }}
              prefillMessage={prefillMessage}
              onClearPrefill={() => setPrefillMessage('')}
              placeholder="Ask about this stock..."
            />
          </MobileFabChat>

          {/* Watchlist — left drawer overlay */}
          <AnimatePresence>
            {mobileTab === 'watchlist' && (
              <>
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.2 }}
                  className="fixed inset-0 z-40"
                  style={{ backgroundColor: 'var(--color-bg-overlay)' }}
                  onClick={() => setMobileTab(null)}
                />
                <motion.div
                  initial={{ x: '100%' }}
                  animate={{ x: 0 }}
                  exit={{ x: '100%' }}
                  transition={{ type: 'spring', damping: 30, stiffness: 300 }}
                  className="fixed top-0 right-0 bottom-0 z-50 border-l"
                  style={{
                    width: '80vw',
                    maxWidth: '320px',
                    backgroundColor: 'var(--color-bg-card)',
                    borderColor: 'var(--color-border-muted)',
                  }}
                >
                  <MarketSidebarPanel
                    activeSymbol={selectedStock}
                    onSymbolClick={(symbol) => {
                      handleSidebarSymbolClick(symbol);
                      setMobileTab(null);
                    }}
                    marketStatus={marketStatus}
                  />
                </motion.div>
              </>
            )}
          </AnimatePresence>

          {/* Company Overview — bottom drawer sheet */}
          <MobileBottomSheet
            open={showOverview}
            onClose={() => setShowOverview(false)}
            sizing="fixed"
            style={{ paddingBottom: 'calc(var(--bottom-tab-height, 0px) + 16px)' }}
          >
            <CompanyOverviewPanel
              symbol={selectedStock}
              visible={true}
              onClose={() => setShowOverview(false)}
              data={overviewData as OverviewData | null}
              loading={overviewLoading}
            />
          </MobileBottomSheet>
        </div>
      ) : (
        <>
          <div className="market-content-wrapper">
            <div className="market-left-panel">
              <StockHeader
                symbol={selectedStock}
                stockInfo={stockInfo}
                realTimePrice={displayPrice}
                chartMeta={chartMeta}
                displayOverride={selectedStockDisplay}
                onToggleOverview={() => setShowOverview(v => !v)}
                wsStatus={wsStatus}
                wsHasData={!!wsPrices.get(selectedStock)}
                wsDataLevel={wsDataLevel}
                ginlixDataEnabled={ginlixDataEnabled}
                quoteData={(overviewData as OverviewData | null)?.quote || null}
                marketStatus={marketStatus}
                snapshot={snapshotData}
              />
              <div className="market-chart-area">
                {showOverview && (
                  <CompanyOverviewPanel
                    symbol={selectedStock}
                    visible={showOverview}
                    onClose={() => setShowOverview(false)}
                    data={overviewData as OverviewData | null}
                    loading={overviewLoading}
                  />
                )}
                <MarketChart
                  ref={chartRef}
                  symbol={selectedStock}
                  interval={selectedInterval}
                  onIntervalChange={handleIntervalChange}
                  onCapture={handleCaptureChart}
                  onStockMeta={handleStockMeta as any}
                  onLatestBar={handleLatestBar}
                  quoteData={(overviewData as OverviewData | null)?.quote || null}
                  earningsData={(overviewData as OverviewData | null)?.earningsSurprises || null}
                  overlayData={overlayData as Record<string, unknown> | null}
                  stockMeta={chartMeta}
                  snapshot={snapshotData}
                  liveTick={wsPrices.get(selectedStock)?.barData || null}
                  wsStatus={wsStatus}
                  ginlixDataEnabled={ginlixDataEnabled}
                  marketStatus={marketStatus}
                />
              </div>
            </div>
            <MarketSidebarPanel
              activeSymbol={selectedStock}
              onSymbolClick={handleSidebarSymbolClick}
              marketStatus={marketStatus}
            />
            <div className="market-resize-handle" onMouseDown={handleDragStart} />
            <div className="market-right-panel" style={{ width: chatPanelWidth }}>
              <div className="market-right-panel-inner">
                <MarketPanel
                  messages={messages as any}
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
                  onSend={handleSendMessage as any}
                  isLoading={isLoading}
                  mode={mode}
                  onModeChange={setMode as any}
                  workspaces={workspaces as any}
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
              onMouseEnter={(e: React.MouseEvent<HTMLButtonElement>) => {
                e.currentTarget.style.background = 'var(--color-accent-overlay)';
                e.currentTarget.style.borderColor = 'var(--color-accent-primary)';
              }}
              onMouseLeave={(e: React.MouseEvent<HTMLButtonElement>) => {
                e.currentTarget.style.background = 'var(--color-accent-soft)';
                e.currentTarget.style.borderColor = 'var(--color-accent-overlay)';
              }}
            >
              <ArrowLeft style={{ width: 14, height: 14 }} />
              Return to Chat
            </button>
          )}
        </>
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
