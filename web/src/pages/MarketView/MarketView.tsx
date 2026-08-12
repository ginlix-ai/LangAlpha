import React, { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useToast } from '@/components/ui/use-toast';
import './MarketView.css';
import DashboardHeader from '../Dashboard/components/DashboardHeader';
import StockHeader from './components/StockHeader';
import MarketChart from './components/MarketChart';
import type { MarketChartHandle } from './components/MarketChart';
import ChatInput from '../../components/ui/chat-input';
import MarketChatPanel from './components/MarketChatPanel';
import MarketSidebarPanel from './components/MarketSidebarPanel';
import { INTERVALS } from './utils/chartConstants';
import { useMarketChat } from './hooks/useMarketChat';
import { getWorkspaces } from '../ChatAgent/utils/api';
import { attachmentsToContexts } from '../ChatAgent/utils/fileUpload';
import { motion, AnimatePresence } from 'framer-motion';
import CompanyOverviewPanel from './components/CompanyOverviewPanel';
import { MobileBottomSheet } from '../../components/ui/mobile-bottom-sheet';
import { MobileFabChat } from '../../components/ui/mobile-fab-chat';
import { MarketDataWSProvider, useMarketDataWSContext } from './contexts/MarketDataWSContext';

import { loadPref, savePref } from './utils/prefs';
import { useIsMobile } from '@/hooks/useIsMobile';

import { useStockData } from './hooks/useStockData';
import { useChartAnnotationSync } from './hooks/useChartAnnotationSync';
import { getOrFetchFlashWorkspaceId } from './utils/flashWorkspace';
import { marketViewAnnotationContext } from './constants/annotationPrompt';
import { normalizeTimeframe, subscribeLiveAnnotationAdd } from './stores/chartAnnotationStore';
import { chartSelectionStore, isConfirmedFor, useChartSelections } from './stores/chartSelectionStore';
import { buildChartSelectionSend } from './utils/selectionSend';

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
  } = useStockData({
    selectedStock,
    wsStatus,
    setPreviousClose,
    setDayOpen
  });

  const [chartMeta, setChartMeta] = useState<Record<string, unknown> | null>(null);
  // Venue market phase reported by the chart's bars responses (calendar-derived
  // server-side); drives the header's Closed badge. Null until the first load.
  const [marketPhase, setMarketPhase] = useState<string | null>(null);
  const [selectedInterval, setSelectedInterval] = useState<string>(() => {
    // Sanitize the stored pref: a since-removed interval (e.g. '1s') falls
    // back to the default instead of leaving the chart on an unknown key.
    const stored = loadPref('interval', '1day');
    return INTERVALS.some(({ key }) => key === stored) ? stored : '1day';
  });
  const chartRef = useRef<MarketChartHandle>(null);
  const [chartImage, setChartImage] = useState<string | null>(null);       // base64 data URL
  const [chartImageDesc, setChartImageDesc] = useState<string | null>(null); // text description for LLM
  const [showOverview, setShowOverview] = useState<boolean>(false);
  const [mobileTab, setMobileTab] = useState<'watchlist' | null>(null);
  const [chatExpanded, setChatExpanded] = useState(false);
  const isMobile = useIsMobile();

  const [prefillMessage, setPrefillMessage] = useState<string>('');
  const [mode, setMode] = useState<'fast' | 'ptc'>(() => {
    const stored = loadPref<string>('mode', 'fast');
    return stored === 'ptc' ? 'ptc' : 'fast';
  });
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [selectedWorkspaceId, setSelectedWorkspaceId] = useState<string | null>(
    () => loadPref<string | null>('selectedWorkspaceId', null),
  );

  useEffect(() => {
    savePref('mode', mode);
  }, [mode]);

  useEffect(() => {
    savePref('selectedWorkspaceId', selectedWorkspaceId);
  }, [selectedWorkspaceId]);

  const pickRandomQueries = useCallback((symbol: string): string[] => {
    const shuffled = [...QUICK_QUERIES].sort(() => Math.random() - 0.5);
    return shuffled.slice(0, 2).map(q => q.replace('{symbol}', symbol));
  }, []);

  const [quickQueries, setQuickQueries] = useState<string[]>(() => pickRandomQueries(selectedStock));

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
  const [chatPanelWidth, setChatPanelWidth] = useState<number>(() =>
    parseInt(localStorage.getItem('market-chat-width') || '400') || 400
  );
  const isDragging = useRef<boolean>(false);
  const dragStartX = useRef<number>(0);
  const dragStartWidth = useRef<number>(0);

  // Mobile FAB still uses the legacy useMarketChat (no persistence). Desktop
  // chat lives in MarketChatPanel which drives its own useChatMessages.
  const { isLoading, handleSendMessage: handleFastModeSend } = useMarketChat();

  // Resolve the user's flash workspace id once so we can scope chart
  // annotations to the workspace the chat is actually running in.
  const [flashWorkspaceId, setFlashWorkspaceId] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // A transient null (e.g. a failed first fetch) would otherwise leave
    // Fast-mode annotations unscoped for the whole session, so retry a few
    // times. getOrFetchFlashWorkspaceId clears its cache on failure, so each
    // call re-attempts the request.
    const attempt = (remaining: number) => {
      getOrFetchFlashWorkspaceId().then((id) => {
        if (cancelled) return;
        if (id) {
          setFlashWorkspaceId(id);
          return;
        }
        if (remaining > 0) {
          timer = setTimeout(() => attempt(remaining - 1), 1500);
        }
      });
    };
    attempt(3);
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, []);

  // The chart shows the agent-drawn instance for whichever workspace the chat
  // panel is using — flash workspace in Fast mode, the selected one in PTC.
  // Annotations are keyed by (workspace_id, chart_id), so this single id scopes
  // both the persistence sync and the live chart selection.
  const activeWorkspaceId = mode === 'fast' ? flashWorkspaceId : selectedWorkspaceId;
  useChartAnnotationSync(activeWorkspaceId, selectedStock);

  // Switch the chart to a given instance — used by the live-add auto-focus
  // below and by an annotation chip that jumps to a different ticker.
  const handleJumpToChart = useCallback((symbol: string, timeframe?: string | null) => {
    const sym = (symbol || '').trim().toUpperCase();
    if (!sym) return;
    if (sym !== selectedStock) {
      setSelectedStock(sym);
      setSelectedStockDisplay(null);
      setChartMeta(null);
      setShowOverview(false);
    }
    if (timeframe) {
      const tf = normalizeTimeframe(timeframe);
      setSelectedInterval((cur) => (cur === tf ? cur : tf));
    }
  }, [selectedStock]);

  // Auto-apply: when the agent draws an annotation from the chat panel on a
  // different instance than what's on screen (e.g. the user asks to mark GOOGL
  // while viewing AAPL), bring the chart to that symbol+timeframe so the new
  // drawing is actually visible instead of silently landing off-screen. Scoped
  // to the active workspace's live draws (the store's live-add channel only
  // fires for fresh SSE adds, not server re-sync).
  useEffect(() => {
    return subscribeLiveAnnotationAdd((add) => {
      if (!activeWorkspaceId || add.workspaceId !== activeWorkspaceId) return;
      handleJumpToChart(add.symbol, add.timeframe);
    });
  }, [activeWorkspaceId, handleJumpToChart]);

  // Chat return path — captured from URL when navigating from chat DetailPanel
  const [chatReturnPath, setChatReturnPath] = useState<string | null>(null);

  // Handle URL parameters (symbol + returnTo from chat context, and ws + mode
  // when expanding a chart-annotation preview from ChatAgent). Preserve
  // `?thread` so MarketChatPanel can pick it up to resume the right
  // conversation in the same workspace.
  useEffect(() => {
    const symbolParam = searchParams.get('symbol');
    const returnToParam = searchParams.get('returnTo');
    const wsParam = searchParams.get('ws');
    const modeParam = searchParams.get('mode');
    const tfParam = searchParams.get('tf');
    if (symbolParam) {
      const symbol = symbolParam.trim().toUpperCase();
      if (symbol && symbol !== selectedStock) {
        setSelectedStock(symbol);
        setSelectedStockDisplay(null);
        setChartMeta(null);
      }
    }
    // Land on the timeframe the expanded card was drawn for, so its annotations
    // (keyed by symbol:timeframe) show immediately. Validate against the chart's
    // own interval allowlist so a stale/hand-crafted ?tf= can't strand the chart
    // on an unknown interval (empty data + annotations keyed to a dead chart_id).
    if (tfParam && INTERVALS.some((i) => i.key === tfParam)) {
      setSelectedInterval(tfParam);
    }
    // Apply workspace before mode so MarketChatPanel resolves the right
    // (ptc) workspace when it mounts the forwarded thread.
    if (wsParam) {
      setSelectedWorkspaceId(wsParam);
    }
    if (modeParam === 'ptc' || modeParam === 'fast') {
      setMode(modeParam);
    }
    if (returnToParam) {
      setChatReturnPath(returnToParam);
    }
    if (symbolParam || returnToParam || wsParam || modeParam || tfParam) {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        next.delete('symbol');
        next.delete('returnTo');
        next.delete('ws');
        next.delete('mode');
        next.delete('tf');
        return next;
      }, { replace: true });
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

  // A confirmed chart selection for the live chart rides on send (even with an
  // empty box), so let the mobile input treat it as sendable content.
  const { selections: chartSelections } = useChartSelections();
  const hasChartSelectionForChart = useMemo(() => {
    const sym = (selectedStock || '').toUpperCase();
    const tf = normalizeTimeframe(selectedInterval);
    return chartSelections.some((s) => isConfirmedFor(s, sym, tf));
  }, [chartSelections, selectedStock, selectedInterval]);

  // Fetch workspaces for the workspace selector (PTC mode)
  useEffect(() => {
    let cancelled = false;
    getWorkspaces(50, 0)
      .then((data: Record<string, unknown>) => {
        if (cancelled) return;
        const list = ((data.workspaces || []) as Workspace[]).filter((ws) => ws.status !== 'flash');
        setWorkspaces(list);
        // Reconcile selectedWorkspaceId against the fresh list. If the stored
        // id is gone (workspace deleted between visits), drop back to a clean
        // default state — Flash mode + new chat — instead of silently picking
        // a different PTC workspace the user didn't ask for. Resolve both
        // decisions before calling either setter so neither updater runs
        // a side effect on the other piece of state.
        const storedId = loadPref<string | null>('selectedWorkspaceId', null);
        const stillValid = storedId && list.some((ws) => ws.workspace_id === storedId);
        if (storedId && !stillValid) setMode('fast');
        if (!stillValid) setSelectedWorkspaceId(list[0]?.workspace_id ?? null);
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
    // Build additional_context from chart image + file attachments.
    // Always preload the chart-annotation skill so the drawing tools are
    // available from turn 1 (the agent can also self-load it elsewhere, but
    // injecting here guarantees turn-1 availability on the chart surface).
    // Tell it which ticker + timeframe "the chart" is so it edits the instance
    // the user is actually viewing (chart_id = SYMBOL:timeframe).
    const sym = (selectedStock || '').toUpperCase();
    const tf = normalizeTimeframe(selectedInterval);
    const contexts: unknown[] = [
      {
        type: 'skills',
        name: 'chart-annotation',
        instruction: sym ? marketViewAnnotationContext(sym, tf) : undefined,
      },
    ];
    if (chartImage) {
      contexts.push({ type: 'image', data: chartImage, description: chartImageDesc || undefined });
    }
    if (attachments && attachments.length > 0) {
      contexts.push(...attachmentsToContexts(attachments as any));
    }
    // Append every confirmed chart selection (region/price level + note) for
    // the live (sym, tf); a stale one is dropped. The same set is snapshotted
    // for the sent message's cards, and a lone note becomes the message text
    // when the user typed nothing (so the bubble isn't empty).
    const {
      contexts: selectionContexts,
      snapshots: selectionSnapshots,
      attachments: selectionAttachments,
      outgoingMessage,
    } = buildChartSelectionSend(sym, tf, message);
    contexts.push(...selectionContexts);
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
    metaItems.push(...selectionAttachments);
    const attachmentMeta = metaItems.length > 0 ? metaItems : null;

    if (mode === 'fast') {
      handleFastModeSend(outgoingMessage, imageContext, attachmentMeta, model);
      chartSelectionStore.clearAll();
    } else {
      // PTC mode: use selected workspace or fall back to default
      try {
        let workspaceId = selectedWorkspaceId;
        if (!workspaceId) {
          toast({
            variant: 'destructive',
            title: 'No workspace selected',
            description: 'Please create a workspace first to use PTC mode.',
          });
          return;
        }

        navigate(`/chat/t/__default__`, {
          state: {
            workspaceId,
            initialMessage: outgoingMessage,
            planMode: planMode || false,
            additionalContext: imageContext,
            // PTC side needs the same skill activated so the chart tools
            // are available without the LLM having to call LoadSkill.
            skills: ['chart-annotation'],
            ...(selectionSnapshots.length > 0 ? { chartSelections: selectionSnapshots } : {}),
            ...(attachmentMeta ? { attachmentMeta } : {}),
            ...(model ? { model } : {}),
            ...(reasoningEffort ? { reasoningEffort } : {}),
          },
        });
        chartSelectionStore.clearAll();
      } catch (error) {
        console.error('Error setting up PTC mode:', error);
        toast({
          variant: 'destructive',
          title: 'Error',
          description: 'Failed to set up PTC mode. Please try again.',
        });
      }
    }
    setChartImage(null);
    setChartImageDesc(null);
  }, [handleFastModeSend, navigate, toast, chartImage, chartImageDesc, mode, selectedWorkspaceId, selectedStock, selectedInterval]);

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
            marketPhase={marketPhase}
          />

          {/* Chart fills remaining space */}
          <div className="market-chart-area" style={{ flex: 1, minHeight: 0 }}>
            <MarketChart
              ref={chartRef}
              symbol={selectedStock}
              interval={selectedInterval}
              workspaceId={activeWorkspaceId}
              onIntervalChange={handleIntervalChange}
              onCapture={handleCaptureChart}
              onStockMeta={handleStockMeta as any}
              onMarketPhase={setMarketPhase}
              quoteData={(overviewData as OverviewData | null)?.quote || null}
              earningsData={(overviewData as OverviewData | null)?.earningsSurprises || null}
              overlayData={overlayData as Record<string, unknown> | null}
              stockMeta={chartMeta}
              snapshot={snapshotData}
              liveTick={wsPrices.get(selectedStock)?.barData || null}
              wsStatus={wsStatus}
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
              hasExternalContext={hasChartSelectionForChart}
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
                marketPhase={marketPhase}
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
                  workspaceId={activeWorkspaceId}
                  onIntervalChange={handleIntervalChange}
                  onCapture={handleCaptureChart}
                  onStockMeta={handleStockMeta as any}
                  onMarketPhase={setMarketPhase}
                  quoteData={(overviewData as OverviewData | null)?.quote || null}
                  earningsData={(overviewData as OverviewData | null)?.earningsSurprises || null}
                  overlayData={overlayData as Record<string, unknown> | null}
                  stockMeta={chartMeta}
                  snapshot={snapshotData}
                  liveTick={wsPrices.get(selectedStock)?.barData || null}
                  wsStatus={wsStatus}
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
                <MarketChatPanel
                  symbol={selectedStock}
                  interval={selectedInterval}
                  mode={mode}
                  onModeChange={setMode}
                  workspaces={workspaces}
                  selectedWorkspaceId={selectedWorkspaceId}
                  onWorkspaceChange={setSelectedWorkspaceId}
                  chartImage={chartImage}
                  chartImageDesc={chartImageDesc}
                  onCaptureChart={handleCaptureChartForContext}
                  onClearChartImage={() => { setChartImage(null); setChartImageDesc(null); }}
                  prefillMessage={prefillMessage}
                  onClearPrefill={() => setPrefillMessage('')}
                  quickQueries={quickQueries}
                  onQuickQuery={handleQuickQuery}
                  onShuffleQueries={handleShuffleQueries}
                  onNavigateSubagent={(tid, taskId) => navigate(`/chat/t/${tid}/${taskId}`)}
                  placeholder="What would you like to know?"
                  onReturnToChat={chatReturnPath ? () => navigate(chatReturnPath) : undefined}
                  onJumpToChart={handleJumpToChart}
                />
              </div>
            </div>
          </div>
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
