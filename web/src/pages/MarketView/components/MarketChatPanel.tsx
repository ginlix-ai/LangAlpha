import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { ArrowLeft, RefreshCw, MessageSquare } from 'lucide-react';
import { queryKeys } from '@/lib/queryKeys';
import { ErrorBanner } from '@/components/ui/error-banner';
import { Loader } from '@/components/ui/loader';
import LogoLoading from '@/components/ui/logo-loading';
import ChatInput, { type ChatInputHandle } from '@/components/ui/chat-input';
import { useStableHandler } from '@/hooks/useStableHandler';
import { useWorkspace } from '@/hooks/useWorkspace';
import { usePreferences } from '@/hooks/usePreferences';
import { readTurnEndScroll } from '@/lib/turnEndScroll';
import { LiveMessageList } from '../../ChatAgent/components/MessageList';
import { MessageActionsProvider, type MessageActions } from '../../ChatAgent/components/messageList/MessageActionsContext';
import { SubagentTelemetryContext } from '../../ChatAgent/components/SubagentTelemetryContext';
import { ChartSurfaceContext, type ChartSurface } from '../../ChatAgent/contexts/ChartSurfaceContext';
import { WorkspaceProvider } from '../../ChatAgent/contexts/WorkspaceContext';
import { computerFolders } from '../../ChatAgent/utils/agentPaths';
import { useChatMessages } from '../../ChatAgent/hooks/useChatMessages';
import { useThreadModel } from '../../ChatAgent/hooks/useThreadModel';
import { useThreadSubagents } from '../../ChatAgent/hooks/useThreadSubagents';
import { ThreadNotices } from '../../ChatAgent/components/chatView/ThreadNotices';
import { DispatchStatusProvider } from '../../ChatAgent/hooks/usePTCDispatchStatus';
import { useStreamFollow } from '../../ChatAgent/components/chatView/streamFollow';
import { useTranscriptFollow } from '../../ChatAgent/components/chatView/useTranscriptFollow';
import { useActiveThreadPublisher } from '@/lib/threadLifecycle/useActiveThreadPublisher';
import { FLASH_ROUTE_STATE, flashWorkspaceQuery } from '@/hooks/useFlashWorkspace';
import { useAllWorkspacesAgent } from '@/hooks/useAllWorkspacesAgent';
import { appendPathSuffix, getPreviewUrl, summarizeThread, offloadThread } from '../../ChatAgent/utils/api';
import { attachmentsToContexts } from '../../ChatAgent/utils/fileUpload';
import {
  resolveSubagentTelemetry as resolveSubagentTelemetryPure,
  type SubagentHistoryLike,
} from '../../ChatAgent/session/subagents/resolveSubagentTelemetry';
import type { SubagentInfo } from '../../ChatAgent/components/ToolCallDetailView';
import { useToolCallLookup } from '../../ChatAgent/components/chatView/toolCallLookup';
import type { PreviewData } from '../../ChatAgent/hooks/utils/types';
import type { Workspace } from '@/types/api';
import MarketChatHistoryButton from './MarketChatHistoryButton';
import MarketDetailDialog, { type DialogPayload } from './MarketDetailDialog';
import { getMarketThreadId, setMarketThreadId, clearMarketThreadId } from '../utils/threadPersistence';
import { readMarketViewRoute } from '../utils/marketRoute';
import { normalizeTimeframe } from '../stores/chartAnnotationStore';
import { chartSelectionStore, useChartSelections, isConfirmedFor } from '../stores/chartSelectionStore';
import { buildChartSelectionSend } from '../utils/selectionSend';
import { composerModeProps } from '../utils/composerMode';
import { SelectionChips } from './SelectionChips';
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
    fontSize: '0.75rem',
  };
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
  /** A workspace was restored but not yet checked against the list, so
   *  `selectedWorkspaceId` is held back as null until it is. */
  workspacePending?: boolean;
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

function PanelLoading(): React.ReactElement {
  return (
    <div className="market-panel" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1, padding: 16 }}>
      <LogoLoading size={36} color="var(--color-accent-overlay)" />
    </div>
  );
}

/**
 * Desktop chat panel for MarketView. Drives the message stream via
 * ChatAgent's `useChatMessages` so the rendering (tool calls, reasoning,
 * artifacts, widgets) stays in lockstep with the main chat page.
 *
 * A PTC panel waits out a pending workspace rather than mounting without one:
 * the scope resolves its stored thread once, at mount, so mounting early would
 * start a fresh chat where the workspace's last one should have reopened.
 */
export default function MarketChatPanel(props: MarketChatPanelProps): React.ReactElement {
  if (props.mode === 'ptc' && props.workspacePending) return <PanelLoading />;
  return <MarketChatScope {...props} />;
}

function MarketChatScope(props: MarketChatPanelProps): React.ReactElement {
  const {
    symbol,
    mode,
    workspaces,
    selectedWorkspaceId,
  } = props;
  const { t } = useTranslation();
  const queryClient = useQueryClient();

  // Flash workspace: lazily fetched once, cached forever.
  const { data: flashWs } = useQuery(flashWorkspaceQuery(queryClient));

  // Active workspace per mode. Flash mode uses the shared flash workspace.
  const activeWorkspaceId = mode === 'fast'
    ? (flashWs as { workspace_id?: string } | undefined)?.workspace_id ?? null
    : selectedWorkspaceId;
  // The folder the workspace lives in on a shared computer, and any a rename
  // moved it out of, which the turn file deck needs to tell the workspace's
  // own notes file from a deliverable. Read through the detail query, which a
  // turn re-reads once the folder has settled. The page's list never does, nor
  // does the flash row, which can predate the turn that gave Home its folder.
  const { data: activeWorkspace } = useWorkspace(activeWorkspaceId);
  // No sibling list: this page opens no files, so a sibling's has nowhere to go.
  const folders = useMemo(() => computerFolders(activeWorkspace, null), [activeWorkspace]);

  // Initial thread resolution. URL `?thread=` wins, then localStorage keyed by
  // (workspace, symbol), then a new chat. This state determines which thread
  // mounts on the keyed `<ChatBody>` — changing it forces a remount so the SSE
  // engine can re-initialise with a different thread.
  const [searchParams, setSearchParams] = useSearchParams();
  const [activeThreadInit, setActiveThreadInit] = useState<string>(() => {
    const fromUrl = readMarketViewRoute(searchParams).threadId;
    if (fromUrl) return fromUrl;
    return getMarketThreadId(activeWorkspaceId, symbol) ?? '__default__';
  });

  // Re-resolve thread when symbol changes: restore the last-seen thread for
  // the new symbol in the current workspace, or start a fresh chat if none.
  // A fresh chat also drops `?thread` from the URL; the mirror below rewrites
  // it for a restored thread but never touches it for a default one, so a
  // reload would otherwise reopen the previous symbol's conversation and
  // save it as this symbol's.
  const lastSymbolRef = useRef(symbol);
  useEffect(() => {
    if (lastSymbolRef.current === symbol) return;
    lastSymbolRef.current = symbol;
    const restored = getMarketThreadId(activeWorkspaceId, symbol);
    setActiveThreadInit(restored ?? '__default__');
    if (restored) return;
    setSearchParams((p) => {
      const next = new URLSearchParams(p);
      if (next.has('thread')) next.delete('thread');
      return next;
    }, { replace: true });
  }, [symbol, activeWorkspaceId, setSearchParams]);

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
        <span style={{ color: 'var(--color-text-tertiary)', fontSize: '0.875rem' }}>
          {t('marketView.chatPanel.noWorkspacePrompt')}
        </span>
      </div>
    );
  }

  if (!activeWorkspaceId) return <PanelLoading />;

  return (
    <WorkspaceProvider workspaceId={activeWorkspaceId} downloadFile={null} folders={folders}>
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
  const { preferences } = usePreferences();
  const allWorkspaces = useAllWorkspacesAgent();
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
  // Under the all-workspaces agent nothing is disabled: the scope picker still
  // offers All workspaces, and says why the workspace list is empty.
  const noWorkspaces = ptcWorkspaces.length === 0;
  const ptcDisabledReason = noWorkspaces && !allWorkspaces ? t('marketView.chatPanel.ptcDisabledReason') : null;
  const emptyWorkspacesHint = noWorkspaces && allWorkspaces ? t('agents.market.workspaceDisabledReason') : null;

  // 'fast' is All workspaces under the all-workspaces agent: the flash row,
  // which the server runs as Home on the full agent.
  const agentMode = mode === 'fast' && !allWorkspaces ? 'flash' : 'ptc';

  // MarketView always has the live chart beside the chat, so inline
  // chart-annotation cards collapse to a confirmation chip. Tell the chip
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
    null,                       // onFileArtifact
    handlePreview,              // onPreviewUrl
    agentMode,
    null,                       // clearSubagentCards
    null,                       // onWorkspaceCreated
    platformValue,
  );

  const {
    messages,
    liveMessages,
    isLoading,
    isLoadingHistory,
    messageError,
    threadId,
    threadModels,
    stopWorkflow,
    getSubagentHistory,
    // HITL handlers: ask-user questions, workspace/PTC/secretary proposals,
    // credit pauses and tool approvals. Without these wired into MessageList the
    // cards render but their Accept/Decline buttons are dead.
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
    handleResumeCreditPause,
    handleApproveToolCall,
    handleRejectToolCall,
    // Turn/context state — drives the stop button, input gating, and the
    // interrupted / compaction status banners.
    pendingInterrupt,
    hasActiveSubagents,
    workspaceStarting,
    isCompacting,
    setIsCompacting,
    tokenUsage,
    insertNotification,
    // Feedback thumbs. Send, edit, regenerate and retry come from
    // useTranscriptFollow below.
    handleThumbUp,
    handleThumbDown,
    feedbackByTurn,
  } = chat;

  // The market panel's thread is what the user is looking at — same active
  // contract as ChatAgent (no unseen dot for watched finishes, seen on open).
  useActiveThreadPublisher(threadId);

  // All workspaces runs the default model, as the full agent does in a workspace.
  const modelMode = agentMode === 'flash' ? 'fast' : 'ptc';
  const threadModel = useThreadModel({ threadId, mode: modelMode, isLoading });
  const threadSubagents = useThreadSubagents({ threadId, mode: modelMode });
  const { toSend: subagentsToSend } = threadSubagents;

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

  const transcriptRef = useRef<HTMLDivElement | null>(null);
  const showTranscript = messages.length > 0 || isLoading || isLoadingHistory;
  const follow = useStreamFollow(messagesContainerRef, transcriptRef, showTranscript, isLoading || isLoadingHistory);
  const { handleSendMessage, handleEditMessage, handleRegenerate, handleRetry } = useTranscriptFollow(
    chat,
    follow,
    () => messagesContainerRef.current,
    readTurnEndScroll(preferences),
  );

  // Send: shape attachments + chart screenshot like ChatAgent does.
  const handleSend = useCallback(
    (
      message: string,
      attachments: AttachmentItem[] = [],
      slashCommands: SlashCommandLike[] = [],
      modelOptions: ModelOptionsLike = {},
    ) => {
      // The drawing tools are always bound; inject the chart-annotation skill
      // every send for its drawing guide and to tell the agent which ticker +
      // timeframe "the chart" is (chart_id = SYMBOL:timeframe — drawing on the
      // wrong timeframe won't show on the view the user is looking at).
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

      handleSendMessage(outgoingMessage, additionalContext, attachmentMeta, {
        ...modelOptions,
        ...(selectionSnapshots.length > 0 ? { chartSelections: selectionSnapshots } : {}),
        subagentsAllowed: subagentsToSend,
      });
      onClearChartImage();
      chartSelectionStore.clearAll();
    },
    [symbol, interval, chartImage, chartImageDesc, handleSendMessage, onClearChartImage, subagentsToSend],
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

  // Subagent navigation — chips deep-link to ChatAgent for full subagent view.
  const handleOpenSubagentTask = useCallback((info: SubagentInfo) => {
    if (!info.subagentId || !threadId || threadId === '__default__') return;
    onNavigateSubagent?.(threadId, info.subagentId);
  }, [threadId, onNavigateSubagent]);

  // Main transcript only: the panel keeps no subagent cards (it hands the
  // chat engine no card updater), so a subagent's own rows never render here
  // and there is no transcript of theirs to search.
  const getToolCallProcess = useToolCallLookup(messages);

  // A row whose record the transcript no longer holds still opens the dialog:
  // it says the call is gone, where a swallowed click reads as a dead row.
  const handleToolCallDetailClick = useCallback((toolCallId: string) => {
    setDialogPayload({ type: 'toolcall', toolCallId });
  }, []);

  // The panel's transcript action surface. Each member is useStableHandler'd
  // so the context value survives every streamed chunk — the chat engine
  // rebuilds its handlers per render, and a fresh value here would re-render
  // every settled bubble in the panel.
  const stableOpenSubagentTask = useStableHandler(handleOpenSubagentTask);
  const stableToolCallDetail = useStableHandler(handleToolCallDetailClick);
  const stableAnswerQuestion = useStableHandler(handleAnswerQuestion);
  const stableSkipQuestion = useStableHandler(handleSkipQuestion);
  const stableApproveCreateWorkspace = useStableHandler(handleApproveCreateWorkspace);
  const stableRejectCreateWorkspace = useStableHandler(handleRejectCreateWorkspace);
  const stableApproveStartQuestion = useStableHandler(handleApproveStartQuestion);
  const stableRejectStartQuestion = useStableHandler(handleRejectStartQuestion);
  const stableApprovePTCAgent = useStableHandler(handleApprovePTCAgent);
  const stableRejectPTCAgent = useStableHandler(handleRejectPTCAgent);
  const stableApproveSecretaryAction = useStableHandler(handleApproveSecretaryAction);
  const stableRejectSecretaryAction = useStableHandler(handleRejectSecretaryAction);
  const stableResumeCreditPause = useStableHandler(handleResumeCreditPause);
  const stableApproveToolCall = useStableHandler(handleApproveToolCall);
  const stableRejectToolCall = useStableHandler(handleRejectToolCall);
  const stableEditMessage = useStableHandler((id: string, content: string) =>
    handleEditMessage(id, content, chatInputRef.current?.getModelOptions?.()));
  const stableRegenerate = useStableHandler((id: string) =>
    handleRegenerate(id, chatInputRef.current?.getModelOptions?.()));
  const stableRetry = useStableHandler(() => handleRetry(chatInputRef.current?.getModelOptions?.()));
  const stableThumbUp = useStableHandler(handleThumbUp);
  const stableThumbDown = useStableHandler(handleThumbDown);
  const stableReportWithAgent = useStableHandler((instruction: string) => {
    handleSendMessage(`/self-improve ${instruction}`, null, null, {});
  });
  const stableWidgetSendPrompt = useStableHandler((text: string) => {
    handleSendMessage(text, null, null, {});
  });

  const messageActions = useMemo<MessageActions>(() => ({
    onOpenSubagentTask: stableOpenSubagentTask,
    onToolCallDetailClick: stableToolCallDetail,
    onAnswerQuestion: stableAnswerQuestion,
    onSkipQuestion: stableSkipQuestion,
    onApproveCreateWorkspace: stableApproveCreateWorkspace,
    onRejectCreateWorkspace: stableRejectCreateWorkspace,
    onApproveStartQuestion: stableApproveStartQuestion,
    onRejectStartQuestion: stableRejectStartQuestion,
    onApprovePTCAgent: stableApprovePTCAgent,
    onRejectPTCAgent: stableRejectPTCAgent,
    onApproveSecretaryAction: stableApproveSecretaryAction,
    onRejectSecretaryAction: stableRejectSecretaryAction,
    onResumeCreditPause: stableResumeCreditPause,
    onApproveToolCall: stableApproveToolCall,
    onRejectToolCall: stableRejectToolCall,
    onEditMessage: stableEditMessage,
    onRegenerate: stableRegenerate,
    onRetry: stableRetry,
    onThumbUp: stableThumbUp,
    onThumbDown: stableThumbDown,
    onReportWithAgent: stableReportWithAgent,
    onWidgetSendPrompt: stableWidgetSendPrompt,
  }), [
    stableOpenSubagentTask, stableToolCallDetail,
    stableAnswerQuestion, stableSkipQuestion, stableApproveCreateWorkspace,
    stableRejectCreateWorkspace, stableApproveStartQuestion, stableRejectStartQuestion,
    stableApprovePTCAgent, stableRejectPTCAgent, stableApproveSecretaryAction,
    stableRejectSecretaryAction, stableResumeCreditPause, stableApproveToolCall, stableRejectToolCall,
    stableEditMessage, stableRegenerate, stableRetry,
    stableThumbUp, stableThumbDown, stableReportWithAgent, stableWidgetSendPrompt,
  ]);

  // In fast mode, carry the source thread/workspace into a PTC-agent proposal so
  // its "open in chat" deep-link lands back here. Null in PTC mode. Keyed on the
  // mode, not the agent: All workspaces runs as 'ptc' but hands off like Flash.
  // Memoized: a fresh object per render would defeat the memoized bubbles downstream.
  const flashContext = useMemo(
    () =>
      mode === 'fast' && threadId && threadId !== '__default__'
        ? { threadId, workspaceId: activeWorkspaceId }
        : null,
    [mode, threadId, activeWorkspaceId],
  );

  const showQuickQueries = messages.length === 0 && !isLoading && !isLoadingHistory;

  // Continue the current conversation in the full ChatView page. `/chat/t/:id`
  // resolves the thread's workspace on its own; we also pass it via state to
  // skip the lookup. Available once a real thread exists.
  const canOpenInChat = !!threadId && threadId !== '__default__';
  const handleOpenInChat = useCallback(() => {
    if (!threadId || threadId === '__default__') return;
    navigate(`/chat/t/${threadId}`, {
      state: { workspaceId: activeWorkspaceId, ...(mode === 'fast' ? FLASH_ROUTE_STATE : {}) },
    });
  }, [navigate, threadId, activeWorkspaceId, mode]);

  // Shared styling for the header's right-hand action chip.
  const headerBtnStyle: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    gap: 5,
    flexShrink: 0,
    padding: '4px 10px',
    background: 'transparent',
    border: '1px solid var(--color-border-default)',
    borderRadius: 8,
    color: 'var(--color-text-secondary)',
    fontSize: '0.75rem',
    fontWeight: 500,
    cursor: 'pointer',
    transition: 'background 0.15s, border-color 0.15s, color 0.15s',
  };
  const headerBtnHover = (e: React.MouseEvent<HTMLButtonElement>) => {
    e.currentTarget.style.background = 'var(--color-bg-hover)';
    e.currentTarget.style.borderColor = 'var(--color-border-elevated)';
    e.currentTarget.style.color = 'var(--color-text-primary)';
  };
  const headerBtnLeave = (e: React.MouseEvent<HTMLButtonElement>) => {
    e.currentTarget.style.background = 'transparent';
    e.currentTarget.style.borderColor = 'var(--color-border-default)';
    e.currentTarget.style.color = 'var(--color-text-secondary)';
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
        {!showTranscript ? (
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
          <div ref={transcriptRef} style={{ padding: '16px 24px', maxWidth: '100%' }}>
            <ChartSurfaceContext value={chartSurface}>
              <SubagentTelemetryContext value={resolveSubagentTelemetry}>
                <MessageActionsProvider actions={messageActions}>
                  <DispatchStatusProvider>
                    <LiveMessageList
                      store={liveMessages}
                      isLoading={isLoading}
                      isLoadingHistory={isLoadingHistory}
                      feedbackByTurn={feedbackByTurn}
                      flashContext={flashContext}
                    />
                  </DispatchStatusProvider>
                </MessageActionsProvider>
              </SubagentTelemetryContext>
            </ChartSurfaceContext>
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

      {/* Status banners: background subagents, workspace warming, and context
          compaction. Mirrors the indicators ChatView shows above its input. */}
      {((hasActiveSubagents && !isLoading)
        || workspaceStarting
        || isCompacting
        || threadModel.retired
        || threadModel.offer
        || threadSubagents.offer !== null) && (
        <div style={{ padding: '0 12px', display: 'flex', flexDirection: 'column', gap: 6 }}>
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
              <span aria-hidden="true" style={{ flexShrink: 0 }}>
                <Loader size={14} className="text-(--color-accent-primary)" />
              </span>
              <span>{t(workspaceStarting === 'archived' ? 'chat.workspaceRestoring' : 'chat.workspaceStarting')}</span>
            </div>
          )}
          {isCompacting && (
            <div style={bannerStyle('transparent')}>
              <span aria-hidden="true" style={{ flexShrink: 0 }}>
                <Loader size={14} className="text-(--color-accent-primary)" />
              </span>
              <span>{t(isCompacting === 'offload' ? 'chat.offloading' : 'chat.compacting')}</span>
            </div>
          )}
          <ThreadNotices model={threadModel} subagents={threadSubagents} mode={modelMode} />
        </div>
      )}

      {/* Chart selection chips, directly above the input like the status banners. */}
      <SelectionChips chips={chips} />

      {/* Input */}
      <ChatInput
        ref={chatInputRef}
        onSend={handleSend as never}
        disabled={isLoadingHistory || !!pendingInterrupt}
        onStop={handleStop}
        onAction={handleAction}
        isLoading={isLoading}
        {...composerModeProps(allWorkspaces, mode, onModeChange)}
        model={threadModel.model}
        onPickModel={threadModel.pickModel}
        subagentsAllowed={threadSubagents.allowed}
        onToggleSubagents={threadSubagents.setAllowed}
        ptcDisabledReason={ptcDisabledReason}
        emptyWorkspacesHint={emptyWorkspacesHint}
        workspaces={ptcWorkspaces}
        selectedWorkspaceId={selectedWorkspaceId}
        onWorkspaceChange={onWorkspaceChange}
        onCaptureChart={onCaptureChart}
        chartImage={chartImage}
        onRemoveChartImage={onClearChartImage}
        prefillMessage={prefillMessage}
        onClearPrefill={onClearPrefill}
        hasExternalContext={chips.length > 0}
        placeholder={
          wasStopped && !isLoading && !pendingInterrupt
            ? t('chat.placeholderStopped')
            : (placeholder ?? t('marketView.chatPanel.defaultPlaceholder'))
        }
        threadModels={threadModels}
        tokenUsage={tokenUsage}
      />

      <MarketDetailDialog payload={dialogPayload} onClose={handleCloseDialog} getToolCallProcess={getToolCallProcess} />
    </div>
  );
}
