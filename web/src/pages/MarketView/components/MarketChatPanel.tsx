import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { ArrowLeft, RefreshCw, MessageSquare, Loader2, ScrollText, X } from 'lucide-react';
import { queryKeys } from '@/lib/queryKeys';
import { ErrorBanner } from '@/components/ui/error-banner';
import LogoLoading from '@/components/ui/logo-loading';
import ChatInput, { type ChatInputHandle } from '@/components/ui/chat-input';
import { useNarrowContainer } from '@/hooks/useNarrowContainer';
import MessageList from '../../ChatAgent/components/MessageList';
import { SubagentTelemetryContext } from '../../ChatAgent/components/SubagentTelemetryContext';
import { ChartSurfaceContext, type ChartSurface } from '../../ChatAgent/contexts/ChartSurfaceContext';
import { WorkspaceProvider } from '../../ChatAgent/contexts/WorkspaceContext';
import { useChatMessages } from '../../ChatAgent/hooks/useChatMessages';
import { getFlashWorkspace, getPreviewUrl, summarizeThread, offloadThread } from '../../ChatAgent/utils/api';
import { attachmentsToContexts } from '../../ChatAgent/utils/fileUpload';
import {
  resolveSubagentTelemetry as resolveSubagentTelemetryPure,
  type SubagentHistoryLike,
} from '../../ChatAgent/utils/resolveSubagentTelemetry';
import type { ToolCallProcessRecord, SubagentInfo } from '../../ChatAgent/components/ToolCallDetailView';
import type { PreviewData } from '../../ChatAgent/hooks/utils/types';
import MarketChatHistoryButton from './MarketChatHistoryButton';
import MarketDetailDialog, { type DialogPayload } from './MarketDetailDialog';
import { getMarketThreadId, setMarketThreadId, clearMarketThreadId } from '../utils/threadPersistence';
import { normalizeTimeframe } from '../stores/chartAnnotationStore';
import { chartSelectionStore, useChartSelections, isConfirmedFor } from '../stores/chartSelectionStore';
import { buildChartSelectionSend } from '../utils/selectionSend';
import { marketViewAnnotationContext } from '../constants/annotationPrompt';
import './MarketPanel.css';

/** Compact status banner shown above the input (interrupt, compaction, etc.). */
function bannerStyle(background: string): React.CSSProperties {
  return {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '6px 10px',
    borderRadius: 6,
    background,
    color: 'var(--color-text-tertiary)',
    fontSize: 12,
  };
}

/** Append a URL path suffix (e.g. "/report.html") to a resolved signed URL. */
function appendPathSuffix(baseUrl: string, path?: string): string {
  if (!path) return baseUrl;
  try {
    const parsed = new URL(baseUrl);
    parsed.pathname = parsed.pathname.replace(/\/+$/, '') + path;
    return parsed.toString();
  } catch {
    return baseUrl;
  }
}

/** Slash-command shapes emitted by ChatInput (skill/subagent pills, action verbs). */
interface SlashCommandLike {
  type: string;
  name: string;
  skillName?: string;
}
interface ActionCommandLike {
  name: string;
  type?: string;
}
interface ModelOptionsLike {
  model?: string | null;
  reasoningEffort?: string | null;
  fastMode?: boolean | null;
}

interface Workspace {
  workspace_id: string;
  name?: string;
  status?: string;
  [key: string]: unknown;
}

interface AttachmentItem {
  dataUrl: string;
  file: { name: string; size: number };
  type: string;
  preview?: string | null;
}

interface MarketChatPanelProps {
  symbol: string;
  /** Current chart interval — tells the agent which timeframe to annotate. */
  interval: string;
  mode: 'fast' | 'ptc';
  onModeChange: (mode: 'fast' | 'ptc') => void;
  workspaces: Workspace[];
  selectedWorkspaceId: string | null;
  onWorkspaceChange: (id: string) => void;
  chartImage: string | null;
  chartImageDesc: string | null;
  onCaptureChart: () => Promise<void> | void;
  onClearChartImage: () => void;
  prefillMessage: string;
  onClearPrefill: () => void;
  quickQueries: string[];
  onQuickQuery: (q: string) => void;
  onShuffleQueries: () => void;
  onNavigateSubagent?: (threadId: string, taskId: string) => void;
  placeholder?: string;
  /** When set (navigated from chat context), shows a "Return to Chat" chip in the header. */
  onReturnToChat?: () => void;
  /** Switch the live chart to a symbol+timeframe (so a chip can jump to it). */
  onJumpToChart?: (symbol: string, timeframe: string) => void;
}

/**
 * Desktop chat panel for MarketView. Drives the message stream via
 * ChatAgent's `useChatMessages` so the rendering (tool calls, reasoning,
 * artifacts, widgets) stays in lockstep with the main chat page.
 */
export default function MarketChatPanel(props: MarketChatPanelProps): React.ReactElement {
  const {
    symbol,
    mode,
    workspaces,
    selectedWorkspaceId,
  } = props;
  const { t } = useTranslation();

  // Flash workspace: lazily fetched once, cached forever.
  const { data: flashWs } = useQuery({
    queryKey: queryKeys.workspaces.flash(),
    queryFn: getFlashWorkspace,
    staleTime: Infinity,
  });

  // Active workspace per mode. Flash mode uses the shared flash workspace.
  const activeWorkspaceId = mode === 'fast'
    ? (flashWs as { workspace_id?: string } | undefined)?.workspace_id ?? null
    : selectedWorkspaceId;

  // Initial thread resolution. URL `?thread=` wins, then localStorage keyed by
  // (workspace, symbol), then a new chat. This state determines which thread
  // mounts on the keyed `<ChatBody>` — changing it forces a remount so the SSE
  // engine can re-initialise with a different thread.
  const [searchParams, setSearchParams] = useSearchParams();
  const [activeThreadInit, setActiveThreadInit] = useState<string>(() => {
    const fromUrl = searchParams.get('thread');
    if (fromUrl) return fromUrl;
    return getMarketThreadId(activeWorkspaceId, symbol) ?? '__default__';
  });

  // Re-resolve thread when symbol changes — restore the last-seen thread for
  // the new symbol in the current workspace (or start a fresh chat if none).
  const lastSymbolRef = useRef(symbol);
  useEffect(() => {
    if (lastSymbolRef.current === symbol) return;
    lastSymbolRef.current = symbol;
    setActiveThreadInit(getMarketThreadId(activeWorkspaceId, symbol) ?? '__default__');
  }, [symbol, activeWorkspaceId]);

  // Reset to a fresh chat when scope (mode / workspace) changes. The user
  // explicitly chose a different scope — surface a clean slate. localStorage
  // is keyed by (workspace, symbol), so the previous scope's pointer stays
  // intact and is reachable via the history dropdown.
  const prevWorkspaceRef = useRef<string | null>(activeWorkspaceId);
  useEffect(() => {
    const prev = prevWorkspaceRef.current;
    if (prev === activeWorkspaceId) return;
    prevWorkspaceRef.current = activeWorkspaceId;
    if (prev === null || activeWorkspaceId === null) return;
    setActiveThreadInit(`__default__#${Date.now()}`);
    setSearchParams((p) => {
      const next = new URLSearchParams(p);
      if (next.has('thread')) next.delete('thread');
      return next;
    }, { replace: true });
  }, [activeWorkspaceId, setSearchParams]);

  const handleSelectThread = useCallback((threadId: string) => {
    setMarketThreadId(activeWorkspaceId, symbol, threadId);
    setActiveThreadInit(threadId);
  }, [activeWorkspaceId, symbol]);

  const handleStartNewChat = useCallback(() => {
    clearMarketThreadId(activeWorkspaceId, symbol);
    // Force a remount even if we were already on __default__ — append a
    // monotonic suffix that's stripped before passing to useChatMessages.
    setActiveThreadInit(`__default__#${Date.now()}`);
    // Clear `?thread=` from the URL too — otherwise a refresh on the
    // empty new-chat panel would restore the old thread from the URL,
    // beating the freshly-cleared localStorage entry.
    setSearchParams((p) => {
      const next = new URLSearchParams(p);
      if (next.has('thread')) next.delete('thread');
      return next;
    }, { replace: true });
  }, [activeWorkspaceId, symbol, setSearchParams]);

  if (mode === 'ptc' && !activeWorkspaceId) {
    return (
      <div className="market-panel" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1, padding: 16 }}>
        <span style={{ color: 'var(--color-text-tertiary)', fontSize: 14 }}>
          {t('marketView.chatPanel.noWorkspacePrompt')}
        </span>
      </div>
    );
  }

  if (!activeWorkspaceId) {
    return (
      <div className="market-panel" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1, padding: 16 }}>
        <LogoLoading size={36} color="var(--color-accent-overlay)" />
      </div>
    );
  }

  return (
    <WorkspaceProvider workspaceId={activeWorkspaceId} downloadFile={null}>
      <ChatBody
        key={`${activeWorkspaceId}:${activeThreadInit}`}
        {...props}
        activeWorkspaceId={activeWorkspaceId}
        initialThreadId={activeThreadInit.split('#')[0]}
        ptcWorkspaces={workspaces}
        onSelectThread={handleSelectThread}
        onStartNewChat={handleStartNewChat}
      />
    </WorkspaceProvider>
  );
}

interface ChatBodyProps extends MarketChatPanelProps {
  activeWorkspaceId: string;
  initialThreadId: string;
  ptcWorkspaces: Workspace[];
  onSelectThread: (threadId: string) => void;
  onStartNewChat: () => void;
}

function ChatBody(props: ChatBodyProps): React.ReactElement {
  const {
    symbol,
    interval,
    mode,
    onModeChange,
    ptcWorkspaces,
    selectedWorkspaceId,
    onWorkspaceChange,
    chartImage,
    chartImageDesc,
    onCaptureChart,
    onClearChartImage,
    prefillMessage,
    onClearPrefill,
    quickQueries,
    onQuickQuery,
    onShuffleQueries,
    onNavigateSubagent,
    placeholder,
    onReturnToChat,
    onJumpToChart,
    activeWorkspaceId,
    initialThreadId,
    onSelectThread,
    onStartNewChat,
  } = props;

  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [, setSearchParams] = useSearchParams();
  const [dialogPayload, setDialogPayload] = useState<DialogPayload | null>(null);
  // Port of the preview currently shown — guards against a late URL resolution
  // reopening a dialog the user already closed (or switched away from).
  const previewPortRef = useRef<number | null>(null);
  // ChatInput handle — lets edit/regenerate/retry read the current model picker.
  const chatInputRef = useRef<ChatInputHandle>(null);
  // Set when the user stops a running turn, so the input placeholder reflects it.
  const [wasStopped, setWasStopped] = useState(false);

  const messagesContainerRef = useRef<HTMLDivElement | null>(null);
  const isNarrowChat = useNarrowContainer(messagesContainerRef, 640);

  // The user's confirmed chart selections (region / price level). Render a chip
  // per selection that still matches the live chart instance — selections drawn
  // on another ticker/timeframe are stale and dropped on send anyway.
  const { selections } = useChartSelections();
  const liveSym = symbol ? symbol.toUpperCase() : '';
  const liveTf = normalizeTimeframe(interval);
  const chips = useMemo(
    () => selections.filter((s) => isConfirmedFor(s, liveSym, liveTf)),
    [selections, liveSym, liveTf],
  );

  // PTC zero-state — disable PTC option if user has no non-flash workspaces.
  const ptcDisabledReason = ptcWorkspaces.length === 0
    ? t('marketView.chatPanel.ptcDisabledReason')
    : null;

  const agentMode = mode === 'fast' ? 'flash' : 'ptc';

  // MarketView always has the live chart beside the chat, so inline
  // chart-annotation previews collapse to a confirmation chip. Tell the chip
  // which instance is on screen + how to switch the chart, so a chip for a
  // different ticker/timeframe can jump the chart to it.
  const chartSurface = useMemo<ChartSurface>(
    () => ({
      chartPresent: true,
      activeSymbol: symbol ? symbol.toUpperCase() : undefined,
      // Raw interval, not normalized: the chip compares this against an
      // annotation's timeframe to decide "shown vs jump". A view-only interval
      // like 1s (which collapses to 1day) must NOT read as active for a 1day
      // annotation that the 1s chart can't actually display.
      activeTimeframe: interval,
      onJumpToChart,
    }),
    [symbol, interval, onJumpToChart],
  );

  // Open the served-HTML preview. The SSE artifact carries only a port, so —
  // like ChatView — open immediately in a loading state, then resolve the
  // authenticated signed URL and swap it in (otherwise the viewer is blank).
  const handlePreview = useCallback((preview: PreviewData) => {
    previewPortRef.current = preview.port;
    setDialogPayload({ type: 'preview', preview: { ...preview, loading: true } });

    if (preview.url || !activeWorkspaceId) return;

    getPreviewUrl(activeWorkspaceId, preview.port, preview.command)
      .then((result: { url: string }) => {
        if (previewPortRef.current !== preview.port) return; // closed / superseded
        const url = appendPathSuffix(result.url, preview.path);
        setDialogPayload({ type: 'preview', preview: { ...preview, url, loading: false } });
      })
      .catch(() => {
        if (previewPortRef.current !== preview.port) return;
        setDialogPayload({
          type: 'preview',
          preview: { ...preview, url: '', loading: false, error: true },
        });
      });
  }, [activeWorkspaceId]);

  const handleCloseDialog = useCallback(() => {
    previewPortRef.current = null;
    setDialogPayload(null);
  }, []);

  // Origin tag — symbol uppercased so AAPL/aapl collapse. Server validates
  // `platform` against ^[a-z_]+(:[A-Z0-9][A-Z0-9.-]*)?$ with max_length 50.
  // Only tag the origin with the symbol when the uppercased symbol already
  // satisfies that suffix shape (CN/HK symbols like 002851.SZ are digit-first)
  // AND fits the length budget (`market_view:` is 12 chars, so <=38 left).
  // Anything else — stray chars, over-long — falls back to the bare namespace
  // rather than mangling the symbol into a wrong, colliding tag or overflowing
  // the cap and 422ing the send.
  const platformValue = useMemo(() => {
    const s = (symbol || '').trim().toUpperCase();
    return /^[A-Z0-9][A-Z0-9.-]*$/.test(s) && s.length <= 38
      ? `market_view:${s}`
      : 'market_view';
  }, [symbol]);

  const chat = useChatMessages(
    activeWorkspaceId,
    initialThreadId,
    null,                       // updateTodoListCard
    null,                       // updateSubagentCard
    null,                       // finalizePendingTodos
    null,                       // onOnboardingRelatedToolComplete
    null,                       // onFileArtifact
    handlePreview,              // onPreviewUrl
    agentMode,
    null,                       // clearSubagentCards
    null,                       // onWorkspaceCreated
    platformValue,
  );

  const {
    messages,
    isLoading,
    isLoadingHistory,
    messageError,
    threadId,
    threadModels,
    lastThreadModel,
    handleSendMessage,
    stopWorkflow,
    getSubagentHistory,
    // HITL handlers — plan approval, ask-user questions, workspace/PTC/secretary
    // proposals. Without these wired into MessageList the cards render but their
    // Accept/Decline buttons are dead.
    handleApproveInterrupt,
    handleRejectInterrupt,
    handleAnswerQuestion,
    handleSkipQuestion,
    handleApproveCreateWorkspace,
    handleRejectCreateWorkspace,
    handleApproveStartQuestion,
    handleRejectStartQuestion,
    handleApprovePTCAgent,
    handleRejectPTCAgent,
    handleApproveSecretaryAction,
    handleRejectSecretaryAction,
    // Turn/context state — drives the stop button, input gating, and the
    // interrupted / plan-feedback / compaction status banners.
    pendingInterrupt,
    pendingRejection,
    hasActiveSubagents,
    workspaceStarting,
    isCompacting,
    setIsCompacting,
    tokenUsage,
    insertNotification,
    // Message-level actions — edit, regenerate, retry, and feedback thumbs.
    handleEditMessage,
    handleRegenerate,
    handleRetry,
    handleThumbUp,
    handleThumbDown,
    getFeedbackForMessage,
  } = chat;

  // Subagent telemetry resolver — feeds ActivityBlock's live token counts.
  // MarketView has no floating cards layer, so we resolve through history only.
  const resolveSubagentTelemetry = useCallback((subagentId: string) => {
    const history = getSubagentHistory?.(subagentId) as SubagentHistoryLike | undefined;
    return resolveSubagentTelemetryPure(undefined, history);
  }, [getSubagentHistory]);

  // Persist thread id per (workspace, symbol) + sync to URL once the workflow
  // assigns a real id.
  useEffect(() => {
    if (!threadId || threadId === '__default__') return;
    setMarketThreadId(activeWorkspaceId, symbol, threadId);
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (next.get('thread') !== threadId) {
        next.set('thread', threadId);
      }
      return next;
    }, { replace: true });
    queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(activeWorkspaceId) });
  }, [threadId, symbol, activeWorkspaceId, setSearchParams, queryClient]);

  // Send: shape attachments + chart screenshot like ChatAgent does.
  const handleSend = useCallback(
    (
      message: string,
      planMode: boolean,
      attachments: AttachmentItem[] = [],
      slashCommands: SlashCommandLike[] = [],
      modelOptions: ModelOptionsLike = {},
    ) => {
      // Always activate the chart-annotation skill so the agent can draw on
      // the live chart from turn 1, and tell it which ticker + timeframe "the
      // chart" is (chart_id = SYMBOL:timeframe — drawing on the wrong timeframe
      // won't show on the view the user is looking at).
      const sym = symbol ? symbol.toUpperCase() : '';
      const tf = normalizeTimeframe(interval);
      const contexts: Record<string, unknown>[] = [
        {
          type: 'skills',
          name: 'chart-annotation',
          instruction: sym ? marketViewAnnotationContext(sym, tf) : undefined,
        },
      ];

      // Skill contexts from typed slash commands (mirrors ChatView). Skip a
      // second chart-annotation context — it's always injected above.
      for (const cmd of slashCommands) {
        if (cmd.type === 'skill' && cmd.skillName && cmd.skillName !== 'chart-annotation') {
          contexts.push({ type: 'skills', name: cmd.skillName });
        } else if (cmd.type === 'subagent') {
          contexts.push({ type: 'directive', content: 'User wishes you to complete this task using subagents.' });
        }
      }

      const metaItems: Record<string, unknown>[] = [];

      if (chartImage) {
        contexts.push({ type: 'image', data: chartImage, description: chartImageDesc || undefined });
        metaItems.push({
          name: chartImageDesc || 'Chart',
          type: 'image',
          size: 0,
          preview: chartImage,
          dataUrl: chartImage,
        });
      }
      if (attachments && attachments.length > 0) {
        contexts.push(...(attachmentsToContexts(attachments as never[]) as unknown as Record<string, unknown>[]));
        attachments.forEach((a) => {
          metaItems.push({
            name: a.file.name,
            type: a.type,
            size: a.file.size,
            preview: a.preview ?? null,
            dataUrl: a.dataUrl,
          });
        });
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
      metaItems.push(...selectionAttachments);

      const additionalContext = contexts.length > 0 ? contexts : null;
      const attachmentMeta = metaItems.length > 0 ? metaItems : null;

      handleSendMessage(outgoingMessage, planMode, additionalContext, attachmentMeta, {
        ...modelOptions,
        ...(selectionSnapshots.length > 0 ? { chartSelections: selectionSnapshots } : {}),
      });
      onClearChartImage();
      chartSelectionStore.clearAll();
    },
    [symbol, interval, chartImage, chartImageDesc, handleSendMessage, onClearChartImage],
  );

  // Stop the running turn (the input's Stop button). Mirrors ChatView: the hook's
  // stopWorkflow aborts the stream reader, finalizes the open message with a
  // "Stopped" chip, and hard-cancels the backend run; we flip the stopped marker.
  const handleStop = useCallback(() => {
    setWasStopped(true);
    void stopWorkflow();
  }, [stopWorkflow]);

  // Clear the stopped marker once a new turn starts.
  useEffect(() => {
    if (isLoading) setWasStopped(false);
  }, [isLoading]);

  // Action slash commands (/compact, /offload). Mirrors ChatView's handler so
  // the input's action verbs do the same thing here.
  const handleAction = useCallback((cmd: ActionCommandLike) => {
    if (!threadId || threadId === '__default__') return;

    const surfaceActionError = (err: unknown, fallbackKey: string, busyKey = 'chat.compactBusy') => {
      const resp = (err as { response?: { status?: number; data?: unknown } } | undefined)?.response;
      const detail = ((resp?.data ?? undefined) as { detail?: unknown } | undefined)?.detail;
      if (detail && typeof detail === 'object' && !Array.isArray(detail)) {
        const obj = detail as { code?: string; message?: string };
        // A user Stop cancels the backend call; the shared cancellation wrapper
        // returns 409 {code: "request_cancelled"} — report a clean stop, not an error.
        if (obj.code === 'request_cancelled') { insertNotification(t('chat.compactionStopped'), 'info'); return; }
        if (obj.code === 'workflow_active') { insertNotification(t(busyKey), 'warning'); return; }
        if (typeof obj.message === 'string' && obj.message.length > 0) { insertNotification(obj.message, 'warning'); return; }
        insertNotification(t(fallbackKey), 'warning');
        return;
      }
      if (typeof detail === 'string' && detail.length > 0) { insertNotification(detail, 'warning'); return; }
      insertNotification(t(fallbackKey), 'warning');
    };

    if (cmd.name === 'compact') {
      setIsCompacting?.('summarize');
      summarizeThread(threadId)
        .then((data: Record<string, unknown>) => {
          setIsCompacting?.(false);
          const detail = (data.summary_text as string | undefined) || undefined;
          insertNotification(t('chat.compactedNotification', { from: data.original_message_count }), 'info', detail);
        })
        .catch((err: unknown) => { surfaceActionError(err, 'chat.compactionError'); setIsCompacting?.(false); });
    } else if (cmd.name === 'offload') {
      setIsCompacting?.('offload');
      offloadThread(threadId)
        .then((data: Record<string, unknown>) => {
          setIsCompacting?.(false);
          insertNotification(t('chat.offloadedNotification', {
            args: (data.offloaded_args as number) || 0,
            reads: (data.offloaded_reads as number) || 0,
          }));
        })
        .catch((err: unknown) => { surfaceActionError(err, 'chat.compactionError', 'chat.offloadBusy'); setIsCompacting?.(false); });
    }
  }, [threadId, setIsCompacting, insertNotification, t]);

  // Track whether the user is currently parked near the bottom of the
  // message list. Auto-scroll only fires while this is true, so a user
  // who has scrolled up to read earlier content during an active stream
  // isn't yanked back down on every SSE chunk.
  const isNearBottomRef = useRef(true);
  useEffect(() => {
    const el = messagesContainerRef.current;
    if (!el) return;
    const onScroll = () => {
      const threshold = 120;
      isNearBottomRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < threshold;
    };
    el.addEventListener('scroll', onScroll, { passive: true });
    return () => el.removeEventListener('scroll', onScroll);
  }, []);

  // Auto-scroll to bottom on new messages / streaming — only when the
  // user is already near the bottom (see isNearBottomRef above).
  useEffect(() => {
    if (!isNearBottomRef.current) return;
    const el = messagesContainerRef.current;
    if (!el || messages.length === 0) return;
    const id = setTimeout(() => {
      el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' });
    }, 80);
    return () => clearTimeout(id);
  }, [messages]);

  // Subagent navigation — chips deep-link to ChatAgent for full subagent view.
  const handleOpenSubagentTask = useCallback((info: SubagentInfo) => {
    if (!info.subagentId || !threadId || threadId === '__default__') return;
    onNavigateSubagent?.(threadId, info.subagentId);
  }, [threadId, onNavigateSubagent]);

  const handleToolCallDetailClick = useCallback((proc: Record<string, unknown>) => {
    setDialogPayload({ type: 'toolcall', toolCallProcess: proc as ToolCallProcessRecord });
  }, []);

  const initialModel = lastThreadModel ?? null;

  // In fast mode, carry the source thread/workspace into a PTC-agent proposal so
  // its "open in chat" deep-link lands back here. Null in PTC mode.
  const flashContext = agentMode === 'flash' && threadId && threadId !== '__default__'
    ? { threadId, workspaceId: activeWorkspaceId }
    : null;

  const showQuickQueries = messages.length === 0 && !isLoading && !isLoadingHistory;

  // Continue the current conversation in the full ChatView page. `/chat/t/:id`
  // resolves the thread's workspace on its own; we also pass it via state to
  // skip the lookup. Available once a real thread exists.
  const canOpenInChat = !!threadId && threadId !== '__default__';
  const handleOpenInChat = useCallback(() => {
    if (!threadId || threadId === '__default__') return;
    navigate(`/chat/t/${threadId}`, { state: { workspaceId: activeWorkspaceId } });
  }, [navigate, threadId, activeWorkspaceId]);

  // Shared styling for the header's right-hand action chip.
  const headerBtnStyle: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    gap: 5,
    flexShrink: 0,
    padding: '4px 10px',
    background: 'var(--color-accent-soft)',
    border: '1px solid var(--color-accent-overlay)',
    borderRadius: 8,
    color: 'var(--color-accent-light)',
    fontSize: 12,
    fontWeight: 500,
    cursor: 'pointer',
    transition: 'background 0.15s, border-color 0.15s',
  };
  const headerBtnHover = (e: React.MouseEvent<HTMLButtonElement>) => {
    e.currentTarget.style.background = 'var(--color-accent-overlay)';
    e.currentTarget.style.borderColor = 'var(--color-accent-primary)';
  };
  const headerBtnLeave = (e: React.MouseEvent<HTMLButtonElement>) => {
    e.currentTarget.style.background = 'var(--color-accent-soft)';
    e.currentTarget.style.borderColor = 'var(--color-accent-overlay)';
  };

  // Session title: first user message text (truncated) or fallback to "New chat".
  const newChatLabel = t('marketView.chatHistory.newChat');
  const activeTitle = useMemo(() => {
    if (!threadId || threadId === '__default__') return newChatLabel;
    const firstUser = (messages as unknown as Array<Record<string, unknown>>).find(
      (m) => (m.role as string) === 'user',
    );
    const raw = (firstUser?.content as string) || '';
    const trimmed = raw.trim();
    if (!trimmed) return newChatLabel;
    return trimmed.length > 40 ? `${trimmed.slice(0, 40)}…` : trimmed;
  }, [messages, threadId, newChatLabel]);

  return (
    <div className="market-panel">
      {/* Header — session title (left) doubles as history dropdown trigger.
          When navigated from chat, a compact "Return to Chat" chip sits at the
          right so it never overlaps the message input. */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 8,
          padding: '6px 12px',
          borderBottom: '1px solid var(--color-border-muted)',
          flexShrink: 0,
        }}
      >
        <MarketChatHistoryButton
          workspaceId={activeWorkspaceId}
          activeThreadId={threadId === '__default__' ? null : threadId}
          activeTitle={activeTitle}
          onSelectThread={onSelectThread}
          onStartNewChat={onStartNewChat}
        />
        {canOpenInChat ? (
          <button
            type="button"
            onClick={handleOpenInChat}
            title={t('marketView.chatPanel.openInChat')}
            style={headerBtnStyle}
            onMouseEnter={headerBtnHover}
            onMouseLeave={headerBtnLeave}
          >
            <MessageSquare style={{ width: 13, height: 13 }} />
            {t('marketView.chatPanel.openInChat')}
          </button>
        ) : onReturnToChat ? (
          <button
            type="button"
            onClick={onReturnToChat}
            style={headerBtnStyle}
            onMouseEnter={headerBtnHover}
            onMouseLeave={headerBtnLeave}
          >
            <ArrowLeft style={{ width: 13, height: 13 }} />
            {t('marketView.chatPanel.returnToChat')}
          </button>
        ) : null}
      </div>

      {/* Messages */}
      <div
        ref={messagesContainerRef}
        style={{ flex: 1, minHeight: 0, overflowY: 'auto', overflowX: 'hidden' }}
      >
        {messages.length === 0 && !isLoading && !isLoadingHistory ? (
          <div className="market-chat-empty-state" style={{ height: '100%' }}>
            <LogoLoading size={60} color="var(--color-accent-overlay)" />
            <p className="market-chat-empty-text" style={{ marginTop: 16 }}>
              {t('marketView.chatPanel.startConversation')}
            </p>
            {messageError && (
              <div style={{ margin: '16px 24px 0', maxWidth: '100%', width: '100%' }}>
                <ErrorBanner error={messageError} />
              </div>
            )}
          </div>
        ) : (
          <div style={{ padding: '16px 24px', maxWidth: '100%' }}>
            <ChartSurfaceContext.Provider value={chartSurface}>
              <SubagentTelemetryContext.Provider value={resolveSubagentTelemetry}>
                <MessageList
                  messages={messages as never[]}
                  isLoading={isLoading}
                  isLoadingHistory={isLoadingHistory}
                  hideAvatar={isNarrowChat}
                  onOpenSubagentTask={handleOpenSubagentTask}
                  onToolCallDetailClick={handleToolCallDetailClick}
                  onApprovePlan={handleApproveInterrupt}
                  onRejectPlan={handleRejectInterrupt}
                  onAnswerQuestion={handleAnswerQuestion}
                  onSkipQuestion={handleSkipQuestion}
                  onApproveCreateWorkspace={handleApproveCreateWorkspace}
                  onRejectCreateWorkspace={handleRejectCreateWorkspace}
                  onApproveStartQuestion={handleApproveStartQuestion}
                  onRejectStartQuestion={handleRejectStartQuestion}
                  onApprovePTCAgent={handleApprovePTCAgent}
                  onRejectPTCAgent={handleRejectPTCAgent}
                  onApproveSecretaryAction={handleApproveSecretaryAction}
                  onRejectSecretaryAction={handleRejectSecretaryAction}
                  onEditMessage={(id: string, content: string) =>
                    handleEditMessage(id, content, chatInputRef.current?.getModelOptions?.())}
                  onRegenerate={(id: string) =>
                    handleRegenerate(id, chatInputRef.current?.getModelOptions?.())}
                  onRetry={() => handleRetry(chatInputRef.current?.getModelOptions?.())}
                  onThumbUp={handleThumbUp}
                  onThumbDown={handleThumbDown}
                  getFeedbackForMessage={getFeedbackForMessage}
                  onReportWithAgent={(instruction: string) =>
                    handleSendMessage(`/self-improve ${instruction}`, false, null, null, {})}
                  flashContext={flashContext}
                  onWidgetSendPrompt={(text: string) => handleSendMessage(text, false, null, null, {})}
                />
              </SubagentTelemetryContext.Provider>
            </ChartSurfaceContext.Provider>
            {messageError && (
              <div style={{ margin: '8px 0' }}>
                <ErrorBanner error={messageError} />
              </div>
            )}
          </div>
        )}
      </div>

      {/* Quick queries (empty state) */}
      {showQuickQueries && quickQueries.length > 0 && (
        <div className="market-quick-queries">
          {quickQueries.map((q, i) => (
            <button key={i} className="market-quick-query-card" onClick={() => onQuickQuery(q)}>
              {q}
            </button>
          ))}
          <button
            className="market-quick-query-shuffle"
            onClick={onShuffleQueries}
            title="Show different suggestions"
          >
            <RefreshCw size={13} />
          </button>
        </div>
      )}

      {/* Status banners — feedback for plan-rejection, interrupt, background
          subagents, workspace warming, and context compaction. Mirrors the
          indicators ChatView shows above its input. */}
      {(pendingRejection
        || (hasActiveSubagents && !isLoading)
        || workspaceStarting
        || isCompacting) && (
        <div style={{ padding: '0 12px', display: 'flex', flexDirection: 'column', gap: 6 }}>
          {pendingRejection && (
            <div style={bannerStyle('var(--color-accent-soft)')}>
              <ScrollText style={{ width: 14, height: 14, flexShrink: 0, color: 'var(--color-accent-primary)' }} />
              <span>{t('chat.planFeedbackHint')}</span>
            </div>
          )}
          {hasActiveSubagents && !isLoading && (
            <div style={bannerStyle('transparent')}>
              <span style={{ position: 'relative', display: 'flex', height: 8, width: 8 }}>
                <span style={{ position: 'absolute', display: 'inline-flex', height: '100%', width: '100%', borderRadius: '9999px', background: 'var(--color-accent-primary)', opacity: 0.6 }} className="animate-ping motion-reduce:animate-none" />
                <span style={{ position: 'relative', display: 'inline-flex', borderRadius: '9999px', height: 8, width: 8, background: 'var(--color-accent-primary)' }} />
              </span>
              <span>{t('chat.backgroundTasksRunning')}</span>
            </div>
          )}
          {workspaceStarting && (
            <div style={bannerStyle('transparent')}>
              <Loader2 style={{ width: 14, height: 14, flexShrink: 0, color: 'var(--color-accent-primary)' }} className="animate-spin" />
              <span>{t(workspaceStarting === 'archived' ? 'chat.workspaceRestoring' : 'chat.workspaceStarting')}</span>
            </div>
          )}
          {isCompacting && (
            <div style={bannerStyle('transparent')}>
              <Loader2 style={{ width: 14, height: 14, flexShrink: 0, color: 'var(--color-accent-primary)' }} className="animate-spin" />
              <span>{t(isCompacting === 'offload' ? 'chat.offloading' : 'chat.compacting')}</span>
            </div>
          )}
        </div>
      )}

      {/* Chart selection chips — the regions / price levels the user picked on
          the chart, each with its note, ready to attach to the next send. Click
          a chip to re-open its note editor on the chart; ✕ removes it. Sits
          directly above the input, matching the status-banner layout. */}
      {chips.length > 0 && (
        <div style={{ padding: '0 12px', marginBottom: 6, display: 'flex', flexWrap: 'wrap', gap: 6 }}>
          {chips.map((c) => {
            const baseLabel = c.selectionType === 'region'
              ? t('marketView.selection.chipRegion', { symbol: c.symbol, timeframe: c.timeframe })
              : t('marketView.selection.chipPriceLevel', {
                  price: Number.isFinite(c.priceLow) ? c.priceLow.toFixed(2) : '—',
                  symbol: c.symbol,
                  timeframe: c.timeframe,
                });
            const label = c.comment ? `${baseLabel} · "${c.comment}"` : baseLabel;
            return (
              <span
                key={c.id}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                  maxWidth: '100%',
                  padding: '4px 6px 4px 10px',
                  borderRadius: 6,
                  background: 'var(--color-accent-soft)',
                  border: '1px solid var(--color-accent-overlay)',
                  color: 'var(--color-text-secondary)',
                  fontSize: 12,
                }}
              >
                <button
                  type="button"
                  title={t('marketView.selection.editChip')}
                  onClick={() => chartSelectionStore.openEditor(c.id)}
                  style={{
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    maxWidth: 240,
                    border: 'none',
                    background: 'transparent',
                    color: 'inherit',
                    font: 'inherit',
                    padding: 0,
                    cursor: 'pointer',
                  }}
                >
                  {label}
                </button>
                <button
                  type="button"
                  aria-label={t('marketView.selection.removeChip')}
                  onClick={() => chartSelectionStore.remove(c.id)}
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    flexShrink: 0,
                    width: 16,
                    height: 16,
                    padding: 0,
                    border: 'none',
                    background: 'transparent',
                    color: 'var(--color-text-tertiary)',
                    cursor: 'pointer',
                  }}
                >
                  <X style={{ width: 12, height: 12 }} />
                </button>
              </span>
            );
          })}
        </div>
      )}

      {/* Input */}
      <ChatInput
        ref={chatInputRef}
        onSend={handleSend as never}
        disabled={isLoadingHistory || !!pendingInterrupt}
        onStop={handleStop}
        onAction={handleAction}
        isLoading={isLoading}
        mode={mode}
        onModeChange={onModeChange}
        ptcDisabledReason={ptcDisabledReason}
        workspaces={ptcWorkspaces as never}
        selectedWorkspaceId={selectedWorkspaceId}
        onWorkspaceChange={onWorkspaceChange}
        onCaptureChart={onCaptureChart}
        chartImage={chartImage}
        onRemoveChartImage={onClearChartImage}
        prefillMessage={prefillMessage}
        onClearPrefill={onClearPrefill}
        hasExternalContext={chips.length > 0}
        placeholder={
          wasStopped && !isLoading && !pendingInterrupt && !pendingRejection
            ? t('chat.placeholderStopped')
            : (placeholder ?? t('marketView.chatPanel.defaultPlaceholder'))
        }
        initialModel={initialModel}
        threadModels={threadModels}
        tokenUsage={tokenUsage}
      />

      <MarketDetailDialog payload={dialogPayload} onClose={handleCloseDialog} />
    </div>
  );
}
