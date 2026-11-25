import React, { useState, useRef, useEffect, useEffectEvent, useCallback, useMemo } from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { Info } from 'lucide-react';
import { useToast } from '@/components/ui/use-toast';
import './MarketView.css';
import StockHeader from './components/StockHeader';
import MarketChart from './components/MarketChart';
import { ChartToolButton } from './components/ChartToolButton';
import type { MarketChartHandle } from './components/MarketChart';
import ChatInput from '../../components/ui/chat-input';
import type { ComposerScope, ComposerSend, ReadyAttachment } from '../../components/ui/chat-input.types';
import MarketChatPanel from './components/MarketChatPanel';
import { QuoteList } from './components/quoteList/QuoteList';
import { QuoteListDrawer } from './components/quoteList/QuoteListDrawer';
import { INTERVALS } from './utils/chartConstants';
import { MARKET_VIEW_ROUTE_PARAMS, readMarketViewRoute } from './utils/marketRoute';
import { useEditSearchParams } from '@/hooks/useEditSearchParams';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { homeWorkspaceQuery, resolveChatWorkspace } from '@/hooks/useHomeWorkspace';
import type { Workspace } from '@/types/api';
import type { StockSearchHit } from '@/lib/marketUtils';
import type { Freshness } from '@/types/market';
import { attachmentSendParts, type AttachmentMeta } from '../ChatAgent/utils/fileUpload';
import { useThreadModel } from '../ChatAgent/hooks/useThreadModel';
import { useThreadSubagents } from '../ChatAgent/hooks/useThreadSubagents';
import { useThreadTuning } from '../ChatAgent/hooks/useThreadTuning';
import { RetiredModelNotice } from '../ChatAgent/components/chatView/RetiredModelNotice';
import { useComposerNotices } from '../ChatAgent/components/chatView/useComposerNotices';
import { motion, AnimatePresence } from '@/lib/framer';
import { SPRING_DRAG, TWEEN } from '@/lib/motion';
import CompanyOverviewPanel from './components/CompanyOverviewPanel';
import { MobileBottomSheet } from '../../components/ui/mobile-bottom-sheet';
import { MobileFabChat } from '../../components/ui/mobile-fab-chat';
import { MarketDataWSProvider, useMarketDataWSContext } from './contexts/MarketDataWSContext';

import { displaySpelling, formatMoney } from '@/lib/bars';
import { displayOverrideFromHit, type SymbolDisplayOverride } from '@/lib/displayName';
import { loadPref, savePref } from './utils/prefs';
import { recordRecentTicker } from '@/lib/recentTickers';
import { useRestoredWorkspace } from './hooks/useRestoredWorkspace';
import { useIsMobile } from '@/hooks/useIsMobile';
import { ChartComposerContext, createChartComposer, useChartAskHandler, useChartComposer } from '@/components/chart/chartComposer';

import { useStockData } from './hooks/useStockData';
import { useStockQuoteModel } from './hooks/useStockQuoteModel';
import { useChartLegendSlots } from './hooks/useChartLegendSlots';
import { chartIdFor, chartKeyParts, chartParts, normalizeTimeframe } from '@/lib/bars/chartId';
import { getShowLayersRequest, subscribeLayerView, type ShowLayersRequest } from './stores/layerViewStore';
import { buildMarketSendContexts } from './utils/marketSend';
import { useCommentUi } from '../ChatAgent/components/viewers/html/comments/useCommentUi';
import { ArtifactCommentSendContext } from '../ChatAgent/components/artifactComments/sendContext';
import type { ChatCommentSends } from '../ChatAgent/components/artifactComments/useChatCommentSends';
import type { ArtifactCommentRef } from '../ChatAgent/components/artifactComments/artifactCommentReply';
import { layerAskContext } from './utils/layerAsk';
import { draftKeys } from '@/lib/composerDrafts';

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
  'marketView:quickQuery.q1',
  'marketView:quickQuery.q2',
  'marketView:quickQuery.q3',
  'marketView:quickQuery.q4',
  'marketView:quickQuery.q5',
  'marketView:quickQuery.q6',
  'marketView:quickQuery.q7',
  'marketView:quickQuery.q8',
] as const;

function pickQuickQueryIndices(): number[] {
  return QUICK_QUERY_KEYS.map((_, i) => i)
    .sort(() => Math.random() - 0.5)
    .slice(0, 2);
}

function MarketViewInner() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const editSearchParams = useEditSearchParams();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const { prices: wsPrices, connectionStatus: wsStatus, dataLevel: wsDataLevel, subscribe: wsSubscribe, unsubscribe: wsUnsubscribe } = useMarketDataWSContext();
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
  const wsHasData = wsPrices.has(selectedStock);
  const streamsLive = wsStatus === 'connected' && wsHasData;

  const {
    stockInfo,
    realTimePrice,
    snapshotData,
    overviewData,
    overviewLoading,
    overlayData,
    marketStatus,
  } = useStockData({ selectedStock, streamsLive });

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
  const { collectViews } = useChartComposer();
  // The chart's comments. On a desktop they go to the chat panel beside it,
  // through its sends; a phone has no chat beside, and each send starts its
  // own turn in the workspace.
  const isMobile = useIsMobile();
  const [chatSends, setChatSends] = useState<Pick<ChatCommentSends, 'sender' | 'sends'> | null>(null);
  const commentUi = useCommentUi(null, chatSends?.sends);
  const [chartImage, setChartImage] = useState<string | null>(null);       // base64 data URL
  const [chartImageDesc, setChartImageDesc] = useState<string | null>(null); // text description for LLM
  const [showOverview, setShowOverview] = useState<boolean>(false);
  const [mobileTab, setMobileTab] = useState<'watchlist' | null>(null);
  const [chatExpanded, setChatExpanded] = useState(false);

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
  useEffect(() => { recordRecentTicker(selectedStock); }, [selectedStock]);
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
  // opens that thread in Home, whose turn may start on another Default than
  // the account's.
  const threadModel = useThreadModel({ threadId: null, isLoading: false, chiefOfStaff: scope === 'all' });
  // The Subagents pick lives here too, so a fold keeps it. The send carries
  // it into the thread it opens.
  const subagents = useThreadSubagents({ threadId: null });
  // The thread's own effort and fast mode, held here for the same reason.
  const tuning = useThreadTuning({ threadId: null });
  // What the FAB's composer says about its next send, as every thread host's does.
  const notices = useComposerNotices({
    threadId: null,
    threadModel,
    subagents,
    workspaceId: selectedWorkspaceId,
    chiefOfStaff: scope === 'all',
  });

  // Home's id, which the Chief of Staff runs in. Retried, since
  // a failed first fetch would otherwise leave the Chief of Staff's chart
  // layers unscoped until something refetches it.
  const { data: homeWorkspace } = useQuery({ ...homeWorkspaceQuery(queryClient), retry: 3 });

  // The workspace the chat runs in: Home under the Chief of Staff, the selected
  // one otherwise. The chart shows the agent's layers drawn in it.
  const activeWorkspaceId = scope === 'all' ? homeWorkspace?.workspace_id ?? null : selectedWorkspaceId;

  // The toolbar's pick of a workspace sets the workspace and then the side in
  // one press, and a link may name both: either way they land as one move.
  const moveScope = useCallback((move: Partial<ScopeMove>) => {
    if (move.workspaceId !== undefined) selectWorkspace(move.workspaceId);
    if (move.scope) setScope(move.scope);
  }, [selectWorkspace]);
  const pickScope = useCallback((next: ComposerScope) => moveScope({ scope: next }), [moveScope]);
  // Picking a workspace is picking its side too, as the toolbar does next.
  const pickWorkspace = useCallback((id: string) => moveScope({ workspaceId: id, scope: 'workspace' }), [moveScope]);
  const moveFromRoute = useEffectEvent(moveScope);

  // Switch the chart to a given instance: for a request to show layers below,
  // and a layer chip that jumps to a different ticker.
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

  // A request to show layers made while the page is open (the agent publishing
  // from the chat panel, a card's chip) brings the chart to their instance when
  // it is in this workspace, so a layer drawn on GOOGL while AAPL is on screen
  // does not land out of sight. The chart there frames them.
  const onShowLayers = useEffectEvent((req: ShowLayersRequest) => {
    const { workspaceId, symbol, timeframe } = chartKeyParts(req.key);
    if (workspaceId === activeWorkspaceId) handleJumpToChart(symbol, timeframe);
  });
  useEffect(() => {
    let seen = getShowLayersRequest();
    return subscribeLayerView(() => {
      const req = getShowLayersRequest();
      if (!req || req === seen) return;
      seen = req;
      onShowLayers(req);
    });
  }, []);

  // The mobile composer has no handle to add a context pill through, so the
  // chart's "ask about this layer" becomes its draft instead. The desktop chat
  // panel answers with a pill.
  useChartAskHandler(isMobile ? (req) => {
    setPrefillMessage(t('marketView:layers.askDraft', { label: layerAskContext(req).label }));
    setChatExpanded(true);
  } : null);

  // A comment on a chart of this workspace comes forward on it, the chart
  // moved to its ticker and timeframe first.
  const { requestFocus } = commentUi;
  const showChartComment = useCallback((path: string, commentId: string) => {
    const chart = chartParts(path);
    if (!chart) return;
    handleJumpToChart(chart.symbol, chart.timeframe);
    requestFocus({ kind: 'chart', path, commentId });
  }, [handleJumpToChart, requestFocus]);
  // As a line in the chat beside naming one does.
  const openChartComment = useCallback((comment: ArtifactCommentRef) => {
    if (comment.kind === 'chart' && comment.workspaceId === activeWorkspaceId) showChartComment(comment.path, comment.commentId);
  }, [activeWorkspaceId, showChartComment]);
  // And a row in the chart's list for one on another of its timeframes.
  const chartComments = useMemo(
    () => ({ ui: commentUi, tabId: CHART_COMMENTS_TAB, active: true, openComment: showChartComment }),
    [commentUi, showChartComment],
  );

  // Chat return path — captured from URL when navigating from chat DetailPanel
  const [chatReturnPath, setChatReturnPath] = useState<string | null>(null);

  // Handle URL parameters (symbol + returnTo from chat context, and ws when
  // opening a chart layer card from ChatAgent). Preserve
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
    // Land on the timeframe the expanded card was drawn for, so its layers
    // (keyed by symbol:timeframe) show immediately. Validate against the chart's
    // own interval allowlist so a stale/hand-crafted ?tf= can't strand the chart
    // on an unknown interval (empty data + layers keyed to a dead chart_id).
    if (route.timeframe && INTERVALS.some((i) => i.key === route.timeframe)) {
      setSelectedInterval(route.timeframe);
    }
    // Workspace and scope as one move, as the picker makes it.
    if (route.workspaceId || route.scope) {
      moveFromRoute({
        ...(route.workspaceId ? { workspaceId: route.workspaceId } : {}),
        ...(route.scope ? { scope: route.scope } : {}),
      });
    }
    if (route.returnTo) {
      setChatReturnPath(route.returnTo);
    }
    // A comment a phone opened from the chat comes forward on the chart the
    // route lands on, as one the Market chat names does.
    const commentChart = route.commentId ? chartIdFor(route.symbol, route.timeframe) : null;
    if (route.commentId && commentChart) requestFocus({ kind: 'chart', path: commentChart, commentId: route.commentId });
    // `thread` stays for MarketChatPanel, which consumes it itself.
    const consumed = MARKET_VIEW_ROUTE_PARAMS.filter((key) => key !== 'thread' && searchParams.has(key));
    if (consumed.length > 0) {
      editSearchParams((params) => {
        for (const key of consumed) params.delete(key);
      }, { replace: true });
    }
  }, [searchParams, selectedStock, editSearchParams, requestFocus]);

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

  // Desktop: the quote rides the chart's own toolbar, as in the chat's chart
  // panel, with the figures a wide page has room for in the row beneath.
  const legend = useChartLegendSlots({
    symbol: selectedStock,
    quote,
    onSwitchSymbol: handleStockSearch,
    source: { wsStatus, wsHasData, wsDataLevel, chartFreshness },
    detail: 'full',
  });
  const toggleOverview = useCallback(() => setShowOverview((v) => !v), []);


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
      const upTo = quote.places;
      const change = displayPrice.change != null && displayPrice.changePercent != null
        ? ` (${formatMoney(displayPrice.change, code, 'en-US', { signed: true, upTo })} / ${displayPrice.changePercent.toFixed(2)}%)`
        : '';
      parts.push(`Real-time price: ${formatMoney(displayPrice.price, code, 'en-US', { upTo })}${change}`);
    }

    setChartImageDesc(parts.join('\n'));
  }, [selectedStock, selectedInterval, stockInfo, selectedStockDisplay, overviewData, displayPrice, quote.currency, quote.places]);

  const handleSendMessage = useCallback(async (message: string, attachments: ReadyAttachment[] = [], _slashCommands: string[] = [], { model, reasoningEffort, fastMode }: { model?: string | null; reasoningEffort?: string | null; fastMode?: boolean | null } = {}) => {
    const send = buildMarketSendContexts(selectedStock || '', selectedInterval, collectViews());
    const contexts: unknown[] = [send.skill];
    if (chartImage) {
      contexts.push({ type: 'image', data: chartImage, description: chartImageDesc || undefined });
    }
    const attached = attachmentSendParts(attachments);
    contexts.push(...attached.contexts);
    const imageContext = contexts.length > 0 ? contexts : null;

    // Build attachment metadata for display in user message bubble
    const metaItems: AttachmentMeta[] = [];
    if (chartImage) {
      metaItems.push({ name: chartImageDesc || 'Chart', type: 'image', size: 0, dataUrl: chartImage });
    }
    metaItems.push(...attached.meta);
    const attachmentMeta = metaItems.length > 0 ? metaItems : null;

    // The selected workspace, or, under the Chief of Staff, Home, which the server
    // runs on the full agent. A failed Home lookup is the setup toast below.
    try {
      const workspaceId = await resolveChatWorkspace(queryClient, scope, selectedWorkspaceId);
      if (!workspaceId) {
        toast({
          variant: 'destructive',
          title: t('marketView:chatHistory.noWorkspace'),
          description: t('agents.market.noWorkspaceToast'),
        });
        return;
      }

      navigate(`/chat/t/__default__`, {
        state: {
          workspaceId,
          initialMessage: message,
          ...(subagents.toSend !== undefined ? { subagentsAllowed: subagents.toSend } : {}),
          additionalContext: imageContext,
          // Carry the chart-annotation skill over so the agent starts with
          // the drawing guide (the tools themselves are always bound).
          skills: ['chart-annotation'],
          ...(send.chartViews.length > 0 ? { chartViews: send.chartViews } : {}),
          ...(attachmentMeta ? { attachmentMeta } : {}),
          ...(model ? { model } : {}),
          ...(reasoningEffort ? { reasoningEffort } : {}),
          ...(typeof fastMode === 'boolean' ? { fastMode } : {}),
          ...tuning.toSend,
        },
      });
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
  }, [navigate, toast, t, queryClient, chartImage, chartImageDesc, scope, selectedWorkspaceId, selectedStock, selectedInterval, subagents.toSend, tuning.toSend, collectViews]);

  const handleListPick = useCallback((symbol: string) => {
    setSelectedStock(displaySpelling(symbol));
    setSelectedStockDisplay(null);
    setChartMeta(null);
    setShowOverview(false);
  }, []);

  // The chart toolbar's own end: the watchlist and holdings, then the company overview.
  const toolbarTrail = useMemo(() => (
    <>
      <QuoteListDrawer symbol={selectedStock} onPick={handleListPick} />
      <ChartToolButton
        onClick={toggleOverview}
        title={t('marketView:companyOverview')}
        aria-pressed={showOverview}
        className={showOverview ? 'chart-tool-btn-active' : undefined}
      >
        <Info size={14} />
      </ChartToolButton>
    </>
  ), [selectedStock, handleListPick, toggleOverview, showOverview, t]);

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
      <h1 className="sr-only">{t('marketView:pageHeading', { symbol: selectedStock })}</h1>
      {isMobile ? (
        <div className="market-mobile-layout">
          <StockHeader
            symbol={selectedStock}
            quote={quote}
            onToggleOverview={toggleOverview}
            onSwitchSymbol={handleStockSearch}
            onOpenWatchlist={() => setMobileTab('watchlist')}
            wsStatus={wsStatus}
            wsHasData={wsHasData}
            wsDataLevel={wsDataLevel}
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
              comments={chartComments}
            />
          </div>

          {/* Floating chat input — FAB on mobile, expands on tap */}
          <MobileFabChat
            expanded={chatExpanded}
            onExpand={() => setChatExpanded(true)}
            onCollapse={() => setChatExpanded(false)}
            className="market-mobile-chat-float"
          >
            {threadModel.retired && (
              // A card of its own: the retired notice is transparent, and the
              // FAB floats over the chart. What else the thread says about the
              // next send is on the composer's controls.
              <div
                className="flex flex-col gap-1.5 mb-1.5 p-1.5 rounded-2xl border"
                style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-muted)' }}
              >
                <RetiredModelNotice {...threadModel.retired} className="px-3" />
              </div>
            )}
            <ChatInput
              onSend={(send: ComposerSend) => { void handleSendMessage(send.message, send.files, [], send.modelOptions); setChatExpanded(false); }}
              draftKey={draftKeys.market}
              scope={scope}
              onScopeChange={pickScope}
              model={threadModel.model}
              sendModel={threadModel.sendModel}
              onPickModel={threadModel.pickModel}
              subagentsAllowed={subagents.allowed}
              onToggleSubagents={subagents.setAllowed}
              tuning={tuning.tuning}
              onPickReasoningEffort={tuning.setReasoningEffort}
              onPickFastMode={tuning.setFastMode}
              workspaces={selectableWorkspaces}
              selectedWorkspaceId={selectedWorkspaceId}
              onWorkspaceChange={pickWorkspace}
              onCaptureChart={handleCaptureChartForContext}
              chartImage={chartImage}
              onRemoveChartImage={() => { setChartImage(null); setChartImageDesc(null); }}
              prefillMessage={prefillMessage}
              onClearPrefill={() => setPrefillMessage('')}
              placeholder={t('marketView:chatPanel.askStockPlaceholder')}
              // The desktop panel waits out the same check: until it confirms
              // the restored workspace there is none to send to, and
              // a send would clear the draft into the no-workspace toast.
              disabled={scope === 'workspace' && workspacePending}
              notices={notices}
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
                  <QuoteList
                    activeSymbol={selectedStock}
                    onPick={(symbol) => {
                      handleListPick(symbol);
                      setMobileTab(null);
                    }}
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
                <ArtifactCommentSendContext value={chatSends?.sender ?? null}>
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
                  comments={chartComments}
                  toolbarLead={legend.toolbarLead}
                  toolbarLeadKey={legend.toolbarLeadKey}
                  measureToolbarLead={legend.measureToolbarLead}
                  toolbarTrail={toolbarTrail}
                  toolbarSubrow={legend.toolbarSubrow}
                />
                </ArtifactCommentSendContext>
              </div>
            </div>
            <div className="market-resize-handle" onMouseDown={handleDragStart} />
            <div className="market-right-panel" style={{ width: chatPanelWidth }}>
              <div className="market-right-panel-inner">
                <MarketChatPanel
                  symbol={selectedStock}
                  interval={selectedInterval}
                  scope={scope}
                  onScopeChange={pickScope}
                  activeWorkspaceId={activeWorkspaceId}
                  workspaces={selectableWorkspaces}
                  selectedWorkspaceId={selectedWorkspaceId}
                  workspacePending={workspacePending}
                  onWorkspaceChange={pickWorkspace}
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
                  onCommentSends={setChatSends}
                  onOpenChartComment={openChartComment}
                />
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

/** The chart's own id among the page's comment targets: it has one chart. */
const CHART_COMMENTS_TAB = 'market-chart';

/** Where the chat runs: the side, and the workspace the workspace side remembers. */
interface ScopeMove {
  scope: ComposerScope;
  workspaceId: string | null;
}

function MarketView() {
  // The page's chart reaches the composer through here: the desktop panel's
  // and the mobile one's.
  const [chartComposer] = useState(createChartComposer);
  return (
    <MarketDataWSProvider>
      <ChartComposerContext value={chartComposer}>
        <MarketViewInner />
      </ChartComposerContext>
    </MarketDataWSProvider>
  );
}

export default MarketView;
