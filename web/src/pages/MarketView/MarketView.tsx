import React, { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useToast } from '@/components/ui/use-toast';
import './MarketView.css';
import StockHeader from './components/StockHeader';
import MarketChart from './components/MarketChart';
import type { MarketChartHandle } from './components/MarketChart';
import ChatInput from '../../components/ui/chat-input';
import type { ComposerScope } from '../../components/ui/chat-input.types';
import MarketChatPanel from './components/MarketChatPanel';
import MarketSidebarPanel from './components/MarketSidebarPanel';
import { INTERVALS } from './utils/chartConstants';
import { MARKET_VIEW_ROUTE_PARAMS, readMarketViewRoute } from './utils/marketRoute';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { homeWorkspaceQuery, resolveChatWorkspace } from '@/hooks/useHomeWorkspace';
import type { Workspace } from '@/types/api';
import type { StockSearchHit } from '@/lib/marketUtils';
import type { Freshness } from '@/types/market';
import { attachmentsToContexts } from '../ChatAgent/utils/fileUpload';
import { useThreadModel } from '../ChatAgent/hooks/useThreadModel';
import { useThreadSubagents } from '../ChatAgent/hooks/useThreadSubagents';
import { useThreadTuning } from '../ChatAgent/hooks/useThreadTuning';
import { ThreadNotices } from '../ChatAgent/components/chatView/ThreadNotices';
import { motion, AnimatePresence } from '@/lib/framer';
import { SPRING_DRAG, TWEEN } from '@/lib/motion';
import CompanyOverviewPanel from './components/CompanyOverviewPanel';
import { MobileBottomSheet } from '../../components/ui/mobile-bottom-sheet';
import { MobileFabChat } from '../../components/ui/mobile-fab-chat';
import { MarketDataWSProvider, useMarketDataWSContext } from './contexts/MarketDataWSContext';

import { displaySpelling, formatMoney } from '@/lib/bars';
import { displayOverrideFromHit, type SymbolDisplayOverride } from '@/lib/displayName';
import { loadPref, savePref } from './utils/prefs';
import { useRestoredWorkspace } from './hooks/useRestoredWorkspace';
import { useIsMobile } from '@/hooks/useIsMobile';

import { useStockData } from './hooks/useStockData';
import { useStockQuoteModel } from './hooks/useStockQuoteModel';
import { useChartAnnotationSync } from './hooks/useChartAnnotationSync';
import { marketViewAnnotationContext } from './constants/annotationPrompt';
import { normalizeTimeframe, subscribeLiveAnnotationAdd } from './stores/chartAnnotationStore';
import { chartSelectionStore, isConfirmedFor, useChartSelections } from './stores/chartSelectionStore';
import { buildChartSelectionSend } from './utils/selectionSend';

interface AttachmentItem {
  dataUrl: string;
  file: { name: string; size: number };
  type: string;
  preview?: string | null;
}

// Stable identity for the pre-data render, so the reconcile effect's
// `workspaces` dependency doesn't change on every render before the fetch
// lands. React Query's structural sharing covers the loaded case.
const EMPTY_WORKSPACES: Workspace[] = [];

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

// Literal keys so the catalog key test sees every question.
const QUICK_QUERY_KEYS = [
  'marketView.quickQuery.q1',
  'marketView.quickQuery.q2',
  'marketView.quickQuery.q3',
  'marketView.quickQuery.q4',
  'marketView.quickQuery.q5',
  'marketView.quickQuery.q6',
  'marketView.quickQuery.q7',
  'marketView.quickQuery.q8',
] as const;

function pickQuickQueryIndices(): number[] {
  return QUICK_QUERY_KEYS.map((_, i) => i)
    .sort(() => Math.random() - 0.5)
    .slice(0, 2);
}

function MarketViewInner() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const { prices: wsPrices, connectionStatus: wsStatus, dataLevel: wsDataLevel, ginlixDataEnabled, subscribe: wsSubscribe, unsubscribe: wsUnsubscribe, setPreviousClose, setDayOpen } = useMarketDataWSContext();
  // A chart link is the page's opening state, not a correction applied after
  // it: MarketChatPanel reads a symbol or workspace that changes after its
  // mount as a scope change the user made, and drops the forwarded `?thread`
  // for a fresh chat. Seeded here, the effect below re-applies the same values
  // and React bails, so the link's conversation survives the landing.
  const [openingRoute] = useState(() => readMarketViewRoute(searchParams));
  // Every way in (link, saved pref, search, sidebar, chip) lands on the display
  // spelling, so a `600519.SS` link, its header, pref and chart keys all read
  // `600519.SH` from the first render.
  const [selectedStock, setSelectedStock] = useState<string>(
    () => displaySpelling(openingRoute.symbol || loadPref('symbol', 'GOOGL')) || 'GOOGL',
  );
  const [selectedStockDisplay, setSelectedStockDisplay] = useState<SymbolDisplayOverride | null>(null);

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
  // Measured freshness of the chart's own bars — a different surface from the
  // quote, and frequently a different provider; the header shows both.
  const [chartFreshness, setChartFreshness] = useState<Freshness | null>(null);
  const [selectedInterval, setSelectedInterval] = useState<string>(() => {
    // Sanitize the stored pref: a since-removed interval (e.g. '1s') falls
    // back to the default instead of leaving the chart on an unknown key.
    const routed = openingRoute.timeframe;
    if (routed && INTERVALS.some(({ key }) => key === routed)) return routed;
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
  // The composer's scope, stored under its old pref name and values so a
  // saved pick still reads: 'fast' is the Chief of Staff, 'ptc' the selected
  // workspace.
  const [scope, setScope] = useState<ComposerScope>(() => {
    if (openingRoute.scope) return openingRoute.scope;
    return loadPref<string>('mode', 'fast') === 'ptc' ? 'workspace' : 'all';
  });
  // Workspaces for the selector. The shared query, not a local
  // fetch — a rename elsewhere invalidates this key, and a copy in local state
  // would keep showing the old name until the page remounts. The list is
  // already the user's own workspaces: this key carries includeHome=false,
  // and the server leaves Home out of that response.
  // refetchOnMount 'always': the reconcile in useRestoredWorkspace only accepts
  // a fetch that happened during this mount, and the 30s staleTime would
  // otherwise serve a remount from cache with no request at all, so the check
  // would never run.
  const { data: workspacesData, isFetchedAfterMount, isSuccess } = useWorkspaces({
    limit: 50,
    refetchOnMount: 'always',
  });
  const workspaces = workspacesData?.workspaces ?? EMPTY_WORKSPACES;
  const {
    selectedWorkspaceId, pending: workspacePending, select: selectWorkspace, workspaces: selectableWorkspaces,
  } = useRestoredWorkspace({
    linkedId: openingRoute.workspaceId,
    workspaces,
    total: workspacesData?.total,
    isFetchedAfterMount,
    isSuccess,
    onDropped: () => setScope('all'),
  });

  useEffect(() => {
    savePref('mode', scope === 'workspace' ? 'ptc' : 'fast');
  }, [scope]);

  // The Chief of Staff is always there. A stored workspace side with no
  // workspace left to select would leave the chat panel with nothing to send
  // to, so it falls back.
  useEffect(() => {
    if (scope !== 'workspace' || workspacePending) return;
    if (isFetchedAfterMount && isSuccess && selectableWorkspaces.length === 0) setScope('all');
  }, [scope, workspacePending, isFetchedAfterMount, isSuccess, selectableWorkspaces.length]);

  // Indices, not text: the picks survive a language switch and only the words re-translate.
  const [quickQueryPicks, setQuickQueryPicks] = useState<number[]>(pickQuickQueryIndices);
  const quickQueries = useMemo(
    () => quickQueryPicks.map((i) => t(QUICK_QUERY_KEYS[i], { symbol: selectedStock })),
    [quickQueryPicks, selectedStock, t],
  );

  // Persist user preferences to localStorage (dedicated effects — no other side effects)
  useEffect(() => { savePref('symbol', selectedStock); }, [selectedStock]);
  useEffect(() => { savePref('interval', selectedInterval); }, [selectedInterval]);

  useEffect(() => {
    setQuickQueryPicks(pickQuickQueryIndices());
  }, [selectedStock]);

  const handleShuffleQueries = useCallback(() => {
    setQuickQueryPicks(pickQuickQueryIndices());
  }, []);

  // Resizable chat panel
  const [chatPanelWidth, setChatPanelWidth] = useState<number>(() =>
    parseInt(localStorage.getItem('market-chat-width') || '400') || 400
  );
  const isDragging = useRef<boolean>(false);
  const dragStartX = useRef<number>(0);
  const dragStartWidth = useRef<number>(0);

  // The mobile FAB hands each send to the chat view; the desktop chat lives in
  // MarketChatPanel, which drives its own useChatMessages.
  //
  // The FAB unmounts its composer after every send, so the model lives here,
  // which outlives the FAB. No thread is behind it: a send opens a new thread
  // in the chat view, and the pick goes with that navigation. The Chief of Staff
  // opens that thread in Home, which runs the default model as a workspace
  // does.
  const threadModel = useThreadModel({ threadId: null, isLoading: false });
  // The Subagents pick lives here too, so a fold keeps it. The send carries
  // it into the thread it opens.
  const subagents = useThreadSubagents({ threadId: null });
  // The thread's own effort and fast mode, held here for the same reason.
  const tuning = useThreadTuning({ threadId: null });

  // Home's id, which the Chief of Staff runs in. Retried, since
  // a failed first fetch would otherwise leave the Chief of Staff annotations
  // unscoped until something refetches it.
  const { data: homeWorkspace } = useQuery({ ...homeWorkspaceQuery(queryClient), retry: 3 });

  // The workspace the chat runs in: Home under the Chief of Staff, the selected
  // one otherwise. The chart shows the agent-drawn instance for it, and since
  // annotations are keyed by (workspace_id, chart_id), this single id scopes
  // both the persistence sync and the live chart selection.
  const activeWorkspaceId = scope === 'all' ? homeWorkspace?.workspace_id ?? null : selectedWorkspaceId;
  useChartAnnotationSync(activeWorkspaceId, selectedStock);

  // Switch the chart to a given instance — used by the live-add auto-focus
  // below and by an annotation chip that jumps to a different ticker.
  const handleJumpToChart = useCallback((symbol: string, timeframe?: string | null) => {
    const sym = displaySpelling(symbol || '');
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

  // Handle URL parameters (symbol + returnTo from chat context, and ws when
  // opening a chart-annotation card from ChatAgent). Preserve
  // `?thread` so MarketChatPanel can pick it up to resume the right
  // conversation in the same workspace. On the first render the state above is
  // already seeded from the same route, so this re-applies identical values;
  // it earns its keep for a link followed while the page is already mounted.
  useEffect(() => {
    const route = readMarketViewRoute(searchParams);
    if (route.symbol) {
      const symbol = displaySpelling(route.symbol);
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
    if (route.timeframe && INTERVALS.some((i) => i.key === route.timeframe)) {
      setSelectedInterval(route.timeframe);
    }
    // Apply workspace before scope so MarketChatPanel resolves the right
    // workspace when it mounts the forwarded thread.
    if (route.workspaceId) {
      selectWorkspace(route.workspaceId);
    }
    if (route.scope) {
      setScope(route.scope);
    }
    if (route.returnTo) {
      setChatReturnPath(route.returnTo);
    }
    // `thread` stays for MarketChatPanel, which consumes it itself.
    const consumed = MARKET_VIEW_ROUTE_PARAMS.filter((key) => key !== 'thread' && searchParams.has(key));
    if (consumed.length > 0) {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        for (const key of consumed) next.delete(key);
        return next;
      }, { replace: true });
    }
  }, [searchParams, selectedStock, setSearchParams, selectWorkspace]);

  const handleStockSearch = useCallback((symbol: string, searchResult?: StockSearchHit | null) => {
    setSelectedStock(displaySpelling(symbol));
    setSelectedStockDisplay(
      searchResult ? displayOverrideFromHit(searchResult) : null
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

  // Display price: prefer WS live data over REST. useStockData only returns
  // the rows of the symbol on screen, so a switch shows dashes, not the last one's.
  const displayPrice = wsPrices.get(selectedStock) || realTimePrice;
  const wsHasData = !!wsPrices.get(selectedStock);
  const quote = useStockQuoteModel({
    symbol: selectedStock,
    stockInfo,
    realTimePrice: displayPrice,
    quoteData: overviewData?.quote || null,
    snapshot: snapshotData,
    marketStatus,
    wsStatus,
    wsHasData,
    marketPhase,
    displayOverride: selectedStockDisplay,
  });

  // A confirmed chart selection for the live chart rides on send (even with an
  // empty box), so let the mobile input treat it as sendable content.
  const { selections: chartSelections } = useChartSelections();
  const hasChartSelectionForChart = useMemo(() => {
    const sym = (selectedStock || '').toUpperCase();
    const tf = normalizeTimeframe(selectedInterval);
    return chartSelections.some((s) => isConfirmedFor(s, sym, tf));
  }, [chartSelections, selectedStock, selectedInterval]);

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
      // Prompt text for the model, not display: pinned so it reads the same
      // digits whatever the browser's locale.
      parts.push(`Latest candle — O: ${c.open} H: ${c.high} L: ${c.low} C: ${c.close} Vol: ${c.volume?.toLocaleString('en-US')}`);
    }

    const overview = overviewData;
    if (overview?.quote) {
      if (overview.quote.yearHigh != null) parts.push(`52-week high: ${overview.quote.yearHigh}`);
      if (overview.quote.yearLow != null) parts.push(`52-week low: ${overview.quote.yearLow}`);
    }

    if (displayPrice) {
      // The header's own currency: the row's code, or none for an index level.
      const code = quote.currency;
      parts.push(`Real-time price: ${formatMoney(displayPrice.price, code, 'en-US')} (${formatMoney(displayPrice.change, code, 'en-US', { signed: true })} / ${displayPrice.changePercent.toFixed(2)}%)`);
    }

    setChartImageDesc(parts.join('\n'));
  }, [selectedStock, selectedInterval, stockInfo, selectedStockDisplay, overviewData, displayPrice, quote.currency]);

  const handleSendMessage = useCallback(async (message: string, attachments: AttachmentItem[] = [], _slashCommands: string[] = [], { model, reasoningEffort, fastMode }: { model?: string; reasoningEffort?: string | null; fastMode?: boolean | null } = {}) => {
    // Build additional_context from chart image + file attachments.
    // The drawing tools are always bound; the chart-annotation skill rides
    // every send for its drawing guide and to tell the agent which ticker +
    // timeframe "the chart" is, so it edits the instance the user is actually
    // viewing (chart_id = SYMBOL:timeframe).
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

    // The selected workspace, or, under the Chief of Staff, Home, which the server
    // runs on the full agent. A failed Home lookup is the setup toast below.
    try {
      const workspaceId = await resolveChatWorkspace(queryClient, scope, selectedWorkspaceId);
      if (!workspaceId) {
        toast({
          variant: 'destructive',
          title: t('marketView.chatHistory.noWorkspace'),
          description: t('agents.market.noWorkspaceToast'),
        });
        return;
      }

      navigate(`/chat/t/__default__`, {
        state: {
          workspaceId,
          initialMessage: outgoingMessage,
          ...(subagents.toSend !== undefined ? { subagentsAllowed: subagents.toSend } : {}),
          additionalContext: imageContext,
          // Carry the chart-annotation skill over so the agent starts with
          // the drawing guide (the tools themselves are always bound).
          skills: ['chart-annotation'],
          ...(selectionSnapshots.length > 0 ? { chartSelections: selectionSnapshots } : {}),
          ...(attachmentMeta ? { attachmentMeta } : {}),
          ...(model ? { model } : {}),
          ...(reasoningEffort ? { reasoningEffort } : {}),
          ...(typeof fastMode === 'boolean' ? { fastMode } : {}),
          ...tuning.toSend,
        },
      });
      chartSelectionStore.clearAll();
    } catch (error) {
      console.error('Error opening the chat:', error);
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('agents.market.setupFailed'),
      });
    }
    setChartImage(null);
    setChartImageDesc(null);
  }, [navigate, toast, t, queryClient, chartImage, chartImageDesc, scope, selectedWorkspaceId, selectedStock, selectedInterval, subagents.toSend, tuning.toSend]);

  const handleSidebarSymbolClick = useCallback((symbol: string) => {
    setSelectedStock(displaySpelling(symbol));
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
      {/* Doubles as the window titlebar in the desktop shell; inert elsewhere.
          Page-wide, so the chat and watchlist columns stay draggable too; it
          overlaps the header rather than pushing it down (App.css). */}
      <div className="chrome-drag-strip" aria-hidden="true" />
      {isMobile ? (
        <div className="market-mobile-layout">
          <StockHeader
            symbol={selectedStock}
            quote={quote}
            chartMeta={chartMeta}
            onToggleOverview={() => setShowOverview(v => !v)}
            onSwitchSymbol={handleStockSearch}
            onOpenWatchlist={() => setMobileTab('watchlist')}
            wsStatus={wsStatus}
            wsHasData={wsHasData}
            wsDataLevel={wsDataLevel}
            ginlixDataEnabled={ginlixDataEnabled}
            chartFreshness={chartFreshness}
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
              onChartFreshness={setChartFreshness}
              quoteData={overviewData?.quote || null}
              earningsData={overviewData?.earningsSurprises || null}
              overlayData={overlayData as Record<string, unknown> | null}
              stockMeta={chartMeta}
              snapshot={snapshotData}
              liveTick={wsPrices.get(selectedStock)?.barData || null}
              wsStatus={wsStatus}
              marketStatus={marketStatus}
              selectionTools
            />
          </div>

          {/* Floating chat input — FAB on mobile, expands on tap */}
          <MobileFabChat
            expanded={chatExpanded}
            onExpand={() => setChatExpanded(true)}
            onCollapse={() => setChatExpanded(false)}
            className="market-mobile-chat-float"
          >
            {(threadModel.retired || threadModel.lapsed || threadModel.offer || subagents.offer !== null) && (
              // A card of its own: the retired notice is transparent, and the
              // FAB floats over the chart.
              <div
                className="flex flex-col gap-1.5 mb-1.5 p-1.5 rounded-2xl border"
                style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-muted)' }}
              >
                <ThreadNotices model={threadModel} subagents={subagents} />
              </div>
            )}
            <ChatInput
              onSend={(...args: any[]) => { (handleSendMessage as any)(...args); setChatExpanded(false); }}
              scope={scope}
              onScopeChange={setScope}
              model={threadModel.model}
              onPickModel={threadModel.pickModel}
              subagentsAllowed={subagents.allowed}
              onToggleSubagents={subagents.setAllowed}
              tuning={tuning.tuning}
              onPickReasoningEffort={tuning.setReasoningEffort}
              onPickFastMode={tuning.setFastMode}
              workspaces={selectableWorkspaces}
              selectedWorkspaceId={selectedWorkspaceId}
              onWorkspaceChange={selectWorkspace}
              onCaptureChart={handleCaptureChartForContext}
              chartImage={chartImage}
              onRemoveChartImage={() => { setChartImage(null); setChartImageDesc(null); }}
              prefillMessage={prefillMessage}
              onClearPrefill={() => setPrefillMessage('')}
              hasExternalContext={hasChartSelectionForChart}
              placeholder={t('marketView.chatPanel.askStockPlaceholder')}
              // The desktop panel waits out the same check: until it confirms
              // the restored workspace there is none to send to, and
              // a send would clear the draft into the no-workspace toast.
              disabled={scope === 'workspace' && workspacePending}
            />
          </MobileFabChat>

          {/* Watchlist — left drawer overlay */}
          <AnimatePresence>
            {mobileTab === 'watchlist' && (
              <>
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0, transition: TWEEN.exit }}
                  transition={TWEEN.quick}
                  className="fixed inset-0 z-40"
                  style={{ backgroundColor: 'var(--color-bg-overlay)' }}
                  onClick={() => setMobileTab(null)}
                />
                <motion.div
                  initial={{ x: '100%' }}
                  animate={{ x: 0 }}
                  exit={{ x: '100%', transition: TWEEN.exit }}
                  transition={SPRING_DRAG}
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
              data={overviewData}
              loading={overviewLoading}
            />
          </MobileBottomSheet>
        </div>
      ) : (
        <>
          <div className="market-content-wrapper">
            <div className="market-left-panel">
              <div className="market-titlebar">
                <StockHeader
                  symbol={selectedStock}
                  quote={quote}
                  chartMeta={chartMeta}
                  onToggleOverview={() => setShowOverview(v => !v)}
                  onSwitchSymbol={handleStockSearch}
                  wsStatus={wsStatus}
                  wsHasData={wsHasData}
                  wsDataLevel={wsDataLevel}
                  ginlixDataEnabled={ginlixDataEnabled}
                  chartFreshness={chartFreshness}
                />
              </div>
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
                  workspaceId={activeWorkspaceId}
                  onIntervalChange={handleIntervalChange}
                  onCapture={handleCaptureChart}
                  onStockMeta={handleStockMeta as any}
                  onMarketPhase={setMarketPhase}
                  onChartFreshness={setChartFreshness}
                  quoteData={overviewData?.quote || null}
                  earningsData={overviewData?.earningsSurprises || null}
                  overlayData={overlayData as Record<string, unknown> | null}
                  stockMeta={chartMeta}
                  snapshot={snapshotData}
                  liveTick={wsPrices.get(selectedStock)?.barData || null}
                  wsStatus={wsStatus}
                  marketStatus={marketStatus}
                  selectionTools
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
                  scope={scope}
                  onScopeChange={setScope}
                  activeWorkspaceId={activeWorkspaceId}
                  workspaces={selectableWorkspaces}
                  selectedWorkspaceId={selectedWorkspaceId}
                  workspacePending={workspacePending}
                  onWorkspaceChange={selectWorkspace}
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
