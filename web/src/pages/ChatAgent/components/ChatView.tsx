import React, { Suspense, useEffect, useRef, useState, useCallback, useMemo } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { ArrowLeft, FolderOpen, ScrollText, Loader2, TextSelect, Minus, PanelLeftOpen, Menu, Info, Pin, PinOff, Clock } from 'lucide-react';
import { HoverCard, HoverCardTrigger, HoverCardContent } from '@/components/ui/hover-card';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useNarrowContainer } from '@/hooks/useNarrowContainer';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useFeatureEnabled } from '@/hooks/useFeatures';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { updateCurrentUser } from '../../Dashboard/utils/api';
import { getWorkspace, summarizeThread, offloadThread, getThreadShareStatus, updateThreadSharing } from '../utils/api';
import { buildSharedServeUrl, buildWsfilesUrl } from './viewers/html/wsfilesUrl';
import ShareReportLinkModal from './ShareReportLinkModal';
import { toast } from '@/components/ui/use-toast';
import { mergeWarmingDisplay } from '../utils/warmWorkspace';
import { useChatMessages } from '../hooks/useChatMessages';
import { saveChatSession, getChatSession, clearChatSession } from '../hooks/utils/chatSessionRestore';
import type { PreviewData } from '../hooks/utils/types';
import { useCardState } from '../hooks/useCardState';
import { useWorkspaceFiles } from '../hooks/useWorkspaceFiles';
import { classifyAgentPath } from '../utils/agentPaths';
import {
  routeStopAction,
  compactionErrorCode,
  isUserStoppedCompaction,
  shouldClearCompactingFlag,
  isManualCompactionInFlight,
} from '../utils/compactionControl';
import './FilePanel.css';
import ChatInput, { type ChatInputHandle } from '../../../components/ui/chat-input';
import { attachmentsToContexts, widgetSnapshotsToContexts, type Attachment } from '../utils/fileUpload';
import MessageList, { normalizeSubagentText } from './MessageList';
import { SubagentTelemetryContext } from './SubagentTelemetryContext';
import Markdown from './Markdown';
import NavigationPanel from './NavigationPanel';
import NavDisplayOptions from './NavDisplayOptions';
import ChatMinimap from './ChatMinimap';
import JumpToLatestPill from './JumpToLatestPill';
import { useNavigationData } from '../hooks/useNavigationData';
import ShareButton from './ShareButton';
import { WorkspaceProvider } from '../contexts/WorkspaceContext';
import SubagentStatusBar from './SubagentStatusBar';
import TodoDrawer from './TodoDrawer';
import MarketWatchChip from './MarketWatchChip';
import PulseDot from '@/components/ui/pulse-dot';
import { ErrorBanner } from '@/components/ui/error-banner';
import { motion, AnimatePresence, type PanInfo } from 'framer-motion';
import { MobileBottomSheet } from '@/components/ui/mobile-bottom-sheet';



const RightPanel = React.lazy(() => import('./RightPanel'));
const DetailPanel = React.lazy(() => import('./DetailPanel'));
const PreviewViewer = React.lazy(() => import('./viewers/PreviewViewer'));

import {
  type MessageRecord, type LocationState,
  type SubagentMessage, type SlashCommand, type ModelOptions, type ActionCommand,
  type MsgSelectionTooltipData, type WorkspaceRecord, type ChatViewProps,
} from './chatView/types';
import SubagentStatusIndicator from './chatView/SubagentStatusIndicator';
import { ModelStatusPill } from './chatView/ModelStatusPill';
import { FallbackSuggestionPill } from './chatView/FallbackSuggestionPill';
import { useToolCallAnnouncer } from './chatView/useToolCallAnnouncer';
import { useNavPanel } from './chatView/useNavPanel';
import { useChatScroll } from './chatView/useChatScroll';
import { useSubagentTabs } from './chatView/useSubagentTabs';
import { useRightPanel } from './chatView/useRightPanel';


function ChatView({ workspaceId, threadId, initialTaskId, onBack, workspaceName: initialWorkspaceName, isActive = true, onThreadResolved, warmingState = false }: ChatViewProps): React.ReactElement | null {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const containerRef = useRef<HTMLDivElement>(null);
  const chatInputRef = useRef<ChatInputHandle>(null);
  const location = useLocation();
  const navigate = useNavigate();
  const { preferences } = usePreferences();
  const { mutateAsync: updatePreferencesAsync } = useUpdatePreferences();
  const marketWatchEnabled = useFeatureEnabled('market_watch');
  const queryClient = useQueryClient();
  const initialMessageSentRef = useRef(false);
  // Determine agent mode: flash workspaces use flash mode, otherwise ptc
  const state = location.state as LocationState | null;
  const [agentMode, setAgentMode] = useState(state?.agentMode || 'ptc');
  const isFlashMode = agentMode === 'flash' || state?.workspaceStatus === 'flash';

  // The mode's currently-configured model — fallback initializer for the
  // suggestion pill's nextSendModel, mirroring ChatInput's own modePreferredModel.
  const otherPreference = (preferences as Record<string, Record<string, unknown>> | null | undefined)?.other_preference;
  const activePreferredModel = isFlashMode
    ? ((otherPreference?.preferred_flash_model as string | undefined) || (otherPreference?.preferred_model as string | undefined) || null)
    : ((otherPreference?.preferred_model as string | undefined) || null);
  // Live model selection reported by ChatInput (null until it reports in).
  const [inputModel, setInputModel] = useState<string | null>(null);
  const [workspaceName, setWorkspaceName] = useState(initialWorkspaceName || '');
  // Cross-workspace file panel: in flash mode, files live in PTC workspaces.
  // This tracks which workspace the file panel should fetch from.
  const [filePanelWorkspaceId, setFilePanelWorkspaceId] = useState<string | null>(null);



  // Active agent in main view (default: 'main', or from URL taskId)
  const [activeAgentId, setActiveAgentId] = useState(
    initialTaskId ? `task:${initialTaskId}` : 'main'
  );
  // Show system files in FilePanel (.agents/, code/, tools/, etc.)
  const [showSystemFiles, setShowSystemFiles] = useState(
    () => localStorage.getItem('filePanel.showSystemFiles') === 'true'
  );
  // Track whether the user hard-stopped the current turn (drives the
  // "⏹ Stopped" marker + placeholder). Cleared on the next send.
  const [wasStopped, setWasStopped] = useState(false);
  // Track intentional back navigation (skip session save on unmount)
  const intentionalExitRef = useRef(false);
  // Ref mirrors isActive prop for use in unmount cleanup closures (R1)
  const isActiveRef = useRef(isActive);
  isActiveRef.current = isActive;

  // Nav-panel controller (hover/pin/minimize, shared across instances).
  const {
    navPanelVisible,
    navPinned,
    contentNarrow,
    contentAreaRef,
    navPanelVisibleRef,
    skipNavAnimRef,
    handleNavEnter,
    handleNavLeave,
    handleNavMinimize,
    handleTogglePin,
    handleNavExpand,
    inheritNavOnActivate,
  } = useNavPanel({ isMobile, isActiveRef });



  // Ref for resolved thread ID — updated after useChatMessages, used in switchAgent
  // to avoid referencing currentThreadId (defined later) in useCallback closure.
  const resolvedThreadIdRef = useRef(threadId);



  // Direct URL navigation fallback: detect flash workspace and resolve name from API
  const wsFetchedRef = useRef<string | null>(null); // tracks workspaceId we already fetched for
  useEffect(() => {
    if (!workspaceId) return;
    if (state?.agentMode && workspaceName) return;
    if (wsFetchedRef.current === workspaceId) return;
    wsFetchedRef.current = workspaceId;
    let cancelled = false;
    getWorkspace(workspaceId).then((ws: WorkspaceRecord) => {
      if (cancelled) return;
      if (ws?.status === 'flash' && !state?.agentMode) setAgentMode('flash');
      if (ws?.name && !workspaceName) setWorkspaceName(ws.name);
    }).catch(() => {});
    return () => { cancelled = true; };
  }, [workspaceId, state?.agentMode]); // eslint-disable-line react-hooks/exhaustive-deps

  // Floating cards management - extracted to custom hook for better encapsulation
  // Must be called before useChatMessages since updateTodoListCard and updateSubagentCard are passed to it
  const {
    cards,
    updateTodoListCard,
    updateSubagentCard,
    finalizePendingTodos,
    clearSubagentCards,
  } = useCardState();

  // Sync onboarding_completed via PUT when ChatAgent completes onboarding (risk_preference + stocks)
  const handleOnboardingRelatedToolComplete = useCallback(async () => {
    try {
      await updateCurrentUser({ onboarding_completed: true });
      await queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
    } catch (e) {
      console.warn('[ChatView] Failed to sync onboarding_completed:', e);
    }
  }, [queryClient]);

  // Navigate to a newly created workspace with an optional starter question
  // Always PTC mode — start_question creates a sandbox-backed workspace
  const handleWorkspaceCreated = useCallback(({ workspaceId: newWsId, question }: { workspaceId?: string; question?: string }) => {
    if (!newWsId) return;
    const path = `/chat/t/__default__`;
    const navState = { workspaceId: newWsId, agentMode: 'ptc', ...(question ? { initialMessage: question } : {}) };
    navigate(path, { state: navState });
  }, [navigate]);

  // Workspace files - shared between FilePanel and ChatInput
  // Must be declared before useChatMessages so refreshFiles can be passed as onFileArtifact
  // For flash mode: use filePanelWorkspaceId (a PTC workspace) when set via cross-workspace file links.
  // For PTC mode: always use the current workspaceId.
  const effectiveFileWorkspaceId = isFlashMode ? filePanelWorkspaceId : workspaceId;
  const {
    files: workspaceFiles,
    loading: filesLoading,
    error: filesError,
    refresh: refreshFiles,
  } = useWorkspaceFiles(effectiveFileWorkspaceId, { includeSystem: showSystemFiles });

  // When the agent writes to a memory- or memo-tier path, invalidate the
  // matching queries so the Memory / Memo tab reflects the new content
  // without a manual refresh. classifyAgentPath is the single source of
  // truth — same logic the chat row click routing uses.
  const handleFileArtifact = useCallback((event: { payload?: Record<string, unknown> }) => {
    refreshFiles();
    const filePath = (event?.payload?.file_path as string | undefined) ?? '';
    if (!filePath) return;
    const info = classifyAgentPath(filePath);
    if (info.kind === 'memory') {
      if (info.tier === 'user') {
        queryClient.invalidateQueries({ queryKey: queryKeys.memory.user() });
      } else if (effectiveFileWorkspaceId) {
        queryClient.invalidateQueries({
          queryKey: queryKeys.memory.workspace(effectiveFileWorkspaceId),
        });
      }
    } else if (info.kind === 'memo') {
      queryClient.invalidateQueries({ queryKey: queryKeys.memo.all });
    }
  }, [refreshFiles, queryClient, effectiveFileWorkspaceId]);

  // Navigation panel data — workspaces + threads for the overlay sidebar
  const {
    workspaces: navWorkspaces,
    workspaceThreads: navWorkspaceThreads,
    expandWorkspace: navExpandWorkspace,
    hasMore: navHasMore,
    loadAll: navLoadAll,
    loadMoreThreads: navLoadMoreThreads,
    reorderWorkspace: navReorderWorkspace,
    canReorderWorkspaces: navCanReorderWorkspaces,
    pinWorkspace: navPinWorkspace,
    renameWorkspace: navRenameWorkspace,
  } = useNavigationData(workspaceId);

  // Navigate to a different thread from the navigation panel
  const handleNavigateThread = useCallback((wsId: string, tid: string) => {
    // Find workspace name from nav data for route state
    const ws = (navWorkspaces as Record<string, unknown>[]).find((w) => (w as Record<string, unknown>).workspace_id === wsId) as Record<string, unknown> | undefined;
    navigate(`/chat/t/${tid}`, {
      state: {
        workspaceId: wsId,
        workspaceName: (ws?.name as string) || workspaceName || '',
        workspaceStatus: (ws?.status as string) || null,
        ...(ws?.status === 'flash' ? { agentMode: 'flash' } : {}),
      },
    });
  }, [navigate, navWorkspaces, workspaceName]);

  // Open a fresh thread in a workspace from the nav panel. `__default__` + a
  // workspaceId in route state resolves to a brand-new thread (ChatAgent only
  // restores a stored session for the bare /chat route), mirroring the new-
  // workspace navigation path.
  const handleNewThread = useCallback((wsId: string) => {
    const ws = (navWorkspaces as Record<string, unknown>[]).find((w) => (w as Record<string, unknown>).workspace_id === wsId) as Record<string, unknown> | undefined;
    const status = (ws?.status as string) || null;
    navigate('/chat/t/__default__', {
      state: {
        workspaceId: wsId,
        workspaceName: (ws?.name as string) || '',
        workspaceStatus: status,
        agentMode: status === 'flash' ? 'flash' : 'ptc',
      },
    });
  }, [navigate, navWorkspaces]);

  // Stable ref-based callback for opening preview URLs from SSE events.
  // Defined here so it can be passed to useChatMessages; assigned after
  // clampPanelWidth/pushPanelHistory are defined further down.
  const openPreviewRef = useRef<(data: PreviewData) => void>(() => {});
  const handleOpenPreviewFromStream = useCallback((data: PreviewData) => {
    openPreviewRef.current(data);
  }, []);

  // Chat messages management - receives updateTodoListCard and updateSubagentCard from floating cards hook
  const {
    messages,
    isLoading,
    hasActiveSubagents,
    awaitingReportBack,
    workspaceStarting,
    isCompacting,
    setIsCompacting,
    queuedSend,
    isLoadingHistory,
    isReconnecting,
    modelStatus,
    fallbackSuggestion,
    clearFallbackSuggestion,
    messageError,
    returnedSteering,
    clearReturnedSteering,
    handleSendMessage,
    stopWorkflow,
    stopCompaction,
    pendingInterrupt,
    pendingRejection,
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
    tokenUsage,
    threadId: currentThreadId,
    threadModels,
    lastThreadModel,
    marketWatch,
    isShared: threadIsShared,
    insertNotification,
    handleEditMessage,
    handleRegenerate,
    handleRetry,
    handleThumbUp,
    handleThumbDown,
    getFeedbackForMessage,
    reconnectIfStaleRun,
    getSubagentHistory,
    resolveSubagentIdToAgentId,
  } = useChatMessages(workspaceId, threadId, updateTodoListCard as (todoData: Record<string, unknown>) => void, updateSubagentCard, finalizePendingTodos, handleOnboardingRelatedToolComplete, handleFileArtifact, handleOpenPreviewFromStream, agentMode, clearSubagentCards, handleWorkspaceCreated, 'web');

  // Fallback-suggestion pill action: adopt the model that actually answered —
  // immediately for this thread's next send (chat input selection) and
  // durably in preferences under the mode-appropriate key.
  const handleSwitchModel = useCallback(async (model: string) => {
    chatInputRef.current?.setModel(model);
    try {
      await updatePreferencesAsync({
        other_preference: isFlashMode ? { preferred_flash_model: model } : { preferred_model: model },
      });
      clearFallbackSuggestion();
      toast({ description: t('chat.modelSwitched', { model }) });
    } catch {
      toast({ description: t('chat.modelSwitchFailed'), variant: 'destructive' });
    }
  }, [isFlashMode, updatePreferencesAsync, clearFallbackSuggestion, t]);

  // Spinner state merges the in-conversation signal (chat SSE `workspace_status`
  // events, set when this client's message owns the start) with the entry-time
  // warming signal (the /events stream, which sees the start even when a
  // background warm owns it). 'archived' from either source wins so the slow-
  // restore copy survives a plain 'starting' from the other.
  const displayWorkspaceStarting = mergeWarmingDisplay(
    workspaceStarting,
    warmingState,
  );

  const chatPlaceholder = useMemo(() => {
    if (pendingRejection) return t('chat.placeholderPendingRejection');
    if (wasStopped && !isLoading && !pendingInterrupt && !pendingRejection)
      return t('chat.placeholderStopped');
    if (isLoading) return t('chat.placeholderLoading');
    if (hasActiveSubagents) return t('chat.placeholderSubagentsRunning');
    return t('chat.placeholderDefault');
  }, [pendingRejection, wasStopped, isLoading, pendingInterrupt, hasActiveSubagents, t]);

  // Status-row visibility, hoisted so the wrapper condition and its two children
  // share one source of truth. The chip self-hides on empty symbols; the tail
  // shows when the main turn ended but a dispatched subagent is still running.
  const showWatchChip = (marketWatch?.symbols?.length ?? 0) > 0;
  const showBackgroundTail = hasActiveSubagents && !isLoading;

  // Restore steering text to input when agent finishes without consuming it
  useEffect(() => {
    if (returnedSteering) {
      chatInputRef.current?.setValue(returnedSteering);
      clearReturnedSteering();
    }
  }, [returnedSteering, clearReturnedSteering]);

  // Ref to avoid stale closure in unmount cleanup
  const currentThreadIdRef = useRef(currentThreadId);
  currentThreadIdRef.current = currentThreadId;
  // Keep resolvedThreadIdRef in sync with the resolved thread ID from useChatMessages
  resolvedThreadIdRef.current = currentThreadId || threadId;

  // Chat transcript scroll controller + tab scroll memory (chatView/useChatScroll).
  const {
    scrollAreaRef,
    subagentScrollAreaRef,
    getScrollContainer,
    withProgrammaticScroll,
    pinToBottom,
    saveScrollPosition,
    jumpPill,
    userMsgCount,
    scrollPositionsRef,
    skipSubagentAutoScrollRef,
    activeAgentIdRef,
    isNearBottomRef,
    isSubagentNearBottomRef,
    restoredForThreadRef,
  } = useChatScroll({
    activeAgentId,
    messages,
    isActive,
    isActiveRef,
    isLoadingHistory,
    currentThreadId,
    threadId,
  });

  // Subagent tab registry + card refresh (chatView/useSubagentTabs).
  const {
    agents,
    activeAgent,
    switchAgent,
    handleSelectAgent,
    handleOpenSubagentTask,
    handleRemoveAgent,
    handleSubagentInstruction,
    resolveSubagentTelemetry,
  } = useSubagentTabs({
    threadId,
    workspaceId,
    initialTaskId,
    isLoadingHistory,
    activeAgentId,
    setActiveAgentId,
    cards,
    updateSubagentCard,
    getSubagentHistory,
    resolveSubagentIdToAgentId,
    saveScrollPosition,
    scrollPositionsRef,
    skipSubagentAutoScrollRef,
    activeAgentIdRef,
    resolvedThreadIdRef,
  });

  // Copy-a-link to an HTML report opens a consent chooser; the actual copy runs
  // in one of the two handlers below depending on the user's pick.
  const [shareLinkFile, setShareLinkFile] = useState<string | null>(null);

  const handleCopyShareLink = useCallback((filePath: string) => {
    setShareLinkFile(filePath);
  }, []);

  // Shareable link: public, revocable, token-scoped. Enables thread sharing
  // with allow_files on first use (always fetching live status first, so
  // spreading the current permissions preserves any existing allow_download
  // rather than clearing it), then copies the public serve URL. Throws on
  // failure so the chooser stays open.
  const copyShareableReportLink = useCallback(async () => {
    const filePath = shareLinkFile;
    const tid = currentThreadIdRef.current;
    if (!filePath || !tid) return;
    try {
      let status = await getThreadShareStatus(tid);
      if (!status?.is_shared || !status?.share_token) {
        status = await updateThreadSharing(tid, {
          is_shared: true,
          permissions: { ...(status?.permissions || {}), allow_files: true },
        });
      } else if (!status.permissions?.allow_files) {
        status = await updateThreadSharing(tid, {
          is_shared: true,
          permissions: { ...status.permissions, allow_files: true },
        });
      }
      const token = status?.share_token;
      if (!token) throw new Error('No share token');
      // buildSharedServeUrl encodes each path segment but preserves slashes, so
      // relative subresources still resolve. It's relative when the API base is
      // same-origin (the nginx case); make it absolute for a copyable link.
      const served = buildSharedServeUrl(token, filePath);
      const url = /^https?:\/\//i.test(served) ? served : `${window.location.origin}${served}`;
      await navigator.clipboard.writeText(url);
      toast({ description: t('filePanel.shareLinkCopied') });
    } catch (e) {
      console.error('[ChatView] Copy shareable link failed:', e);
      toast({ description: t('filePanel.shareLinkFailed'), variant: 'destructive' });
      throw e;
    }
  }, [shareLinkFile, t]);

  // Direct link: the raw wsfiles URL (workspace UUID is the credential). Renders
  // the file full screen. No sharing is enabled, but the link is not revocable
  // and reaches the whole workspace. Throws on failure so the chooser stays open.
  const copyDirectReportLink = useCallback(async () => {
    const filePath = shareLinkFile;
    if (!filePath) return;
    try {
      const served = buildWsfilesUrl(workspaceId, filePath);
      const url = /^https?:\/\//i.test(served) ? served : `${window.location.origin}${served}`;
      await navigator.clipboard.writeText(url);
      toast({ description: t('filePanel.directLinkCopied') });
    } catch (e) {
      console.error('[ChatView] Copy direct link failed:', e);
      toast({ description: t('filePanel.shareLinkFailed'), variant: 'destructive' });
      throw e;
    }
  }, [shareLinkFile, workspaceId, t]);

  // Save chat session on unmount for cross-tab restoration (workspace + thread only).
  // Only the active view saves — evicted hidden views must not overwrite (R1).
  useEffect(() => {
    return () => {
      if (!isActiveRef.current) return;
      if (intentionalExitRef.current) {
        saveChatSession({ workspaceId });
        return;
      }
      saveChatSession({
        workspaceId,
        threadId: currentThreadIdRef.current,
      });
    };
  }, [workspaceId]);

  // Consume saved session on mount so it doesn't interfere with future navigations.
  // One-shot: fires once per instance, never re-fires on isActive changes (R5).
  const sessionConsumedRef = useRef(false);
  useEffect(() => {
    if (sessionConsumedRef.current) return;
    sessionConsumedRef.current = true;
    const session = getChatSession();
    if (session && session.workspaceId === workspaceId) {
      clearChatSession();
    }
  }, [workspaceId]);

  // Hard-stop handler: terminates the current turn immediately (main agent +
  // all subagents) while preserving state. The hook's stopWorkflow aborts the
  // client reader, finalizes the open message, and POSTs /cancel; we flip the
  // "⏹ Stopped" marker here.
  const handleStop = useCallback(() => {
    setWasStopped(true);
    void stopWorkflow();
  }, [stopWorkflow]);

  // Set when the user stops a MANUAL compaction so handleAction's .catch
  // (the summarize/offload request rejects once the backend cancels it) shows a
  // "stopped" notice instead of an error banner. Reset at the start of each new
  // compaction in handleAction.
  const userStoppedCompactionRef = useRef(false);

  // Monotonic token: each manual compaction trigger bumps it, so a late
  // resolution/rejection from a superseded compaction can detect it is stale
  // and skip flipping isCompacting (RT#2). Without this, a rapid
  // /compact→Stop→/compact lets the first request's late .catch clear the flag
  // and unmask the input while the second compaction is still running.
  const compactionGenerationRef = useRef(0);

  // Single stop control reused by the chat-input Stop button. A manual
  // compaction has isLoading=false (no streaming turn) so it routes to
  // stopCompaction; otherwise (a running turn, including an auto Tier-2
  // summarize) it tears down the turn via stopWorkflow.
  const handleStopButton = useCallback(() => {
    if (routeStopAction({ isCompacting, isLoading }) === 'compaction') {
      userStoppedCompactionRef.current = true;
      void stopCompaction();
    } else {
      handleStop();
    }
  }, [isCompacting, isLoading, stopCompaction, handleStop]);

  // Wrapper: converts ChatInput's (message, planMode, attachments, slashCommands) into
  // handleSendMessage(message, planMode, additionalContext, attachmentMeta)
  const handleSendWithAttachments = useCallback((message: string, planMode: boolean, attachments: Attachment[] = [], slashCommands: SlashCommand[] = [], modelOptions: ModelOptions = {}) => {
    const contexts: Record<string, unknown>[] = [];
    let attachmentMeta: Record<string, unknown>[] | null = null;

    // Image/PDF contexts from attachments
    if (attachments && attachments.length > 0) {
      contexts.push(...(attachmentsToContexts(attachments) as unknown as Record<string, unknown>[]));
      attachmentMeta = attachments.map((a) => ({
        name: a.file.name,
        type: a.type,
        size: a.file.size,
        preview: null,
        dataUrl: a.dataUrl,
      }));
    }

    // Skill contexts from slash commands
    for (const cmd of slashCommands) {
      if (cmd.type === 'skill') {
        contexts.push({ type: 'skills', name: cmd.skillName });
      } else if (cmd.type === 'subagent') {
        contexts.push({ type: 'directive', content: 'User wishes you to complete this task using subagents.' });
      }
    }

    // Watch toggle activates the market-watch skill (re-sent every turn while
    // on, like MarketView's chart-annotation). Dedup against a manually-typed
    // /market-watch pill added by the loop above (SkillsMiddleware also dedups
    // the body, but this keeps the context list clean).
    if (modelOptions.marketWatch && marketWatchEnabled && !contexts.some((c) => c.type === 'skills' && c.name === 'market-watch')) {
      contexts.push({
        type: 'skills',
        name: 'market-watch',
        instruction: 'Market watch mode is on for this message. If the central tickers are not yet registered, register them with watch_market.',
      });
    }

    // Widget context snapshots from the deck rail. Each snapshot becomes one
    // `{type:"widget"}` item plus an optional sibling `{type:"image"}` item
    // (the existing MultimodalContext channel handles vision-vs-text-only
    // routing). The same snapshots are also forwarded to handleSendMessage so
    // the user message renders chip cards inline below its bubble.
    if (modelOptions.widgetSnapshots && modelOptions.widgetSnapshots.length > 0) {
      const items = widgetSnapshotsToContexts(modelOptions.widgetSnapshots);
      contexts.push(...(items as unknown as Record<string, unknown>[]));
    }

    const additionalContext = contexts.length > 0 ? contexts : null;
    handleSendMessage(message, planMode, additionalContext, attachmentMeta, modelOptions);
  }, [handleSendMessage, marketWatchEnabled]);

  // Handle action-type slash commands (e.g. /compact, /compaction, /offload)
  const handleAction = useCallback((cmd: ActionCommand) => {
    const tid = currentThreadId || threadId;
    if (!tid || tid === '__default__') return;

    // Surface backend errors from /compact + /offload. Backend may return
    // detail as a structured object ({code, verb, message}) — the 409
    // "workflow_active" case comes through this path when the user fires
    // /compact mid-stream, and we upgrade it to a warning banner.
    const surfaceActionError = (err: unknown, fallbackKey: string) => {
      const resp = (err as { response?: { status?: number; data?: unknown } } | undefined)?.response;
      const data = (resp?.data ?? undefined) as { detail?: unknown } | undefined;
      const detail = data?.detail;
      // `typeof null === 'object'` and arrays are objects in JS, so guard both.
      if (detail && typeof detail === 'object' && !Array.isArray(detail)) {
        const obj = detail as { code?: string; message?: string };
        if (obj.code === 'workflow_active') {
          insertNotification(t('chat.compactBusy'), 'warning');
          return;
        }
        if (typeof obj.message === 'string' && obj.message.length > 0) {
          insertNotification(obj.message, 'warning');
          return;
        }
        insertNotification(t(fallbackKey), 'warning');
        return;
      }
      if (typeof detail === 'string' && detail.length > 0) {
        insertNotification(detail, 'warning');
        return;
      }
      insertNotification(t(fallbackKey), 'warning');
    };

    // A user Stop while this compaction runs cancels the backend call, which
    // rejects the request below. Treat that as a clean stop (not an error).
    // The backend's shared cancellation wrapper tags any user-cancelled request
    // with a structured detail (409 {code: "request_cancelled"}); honor that
    // even when the local ref was already consumed — a rapid stop→retrigger
    // resets the ref before this rejection lands, which would otherwise mislabel
    // the stop as a failure.
    const handleActionError = (err: unknown) => {
      const code = compactionErrorCode(err);
      if (isUserStoppedCompaction({ userStopped: userStoppedCompactionRef.current, errorCode: code })) {
        userStoppedCompactionRef.current = false;
        insertNotification(t('chat.compactionStopped'), 'info');
        return;
      }
      surfaceActionError(err, 'chat.compactionError');
    };

    // Snapshot the generation BEFORE the await so a superseded compaction's late
    // settlement leaves the active one's isCompacting flag alone (RT#2).
    const clearIfCurrent = (myGeneration: number) => {
      if (shouldClearCompactingFlag(myGeneration, compactionGenerationRef.current)) {
        setIsCompacting(false);
      }
    };

    // Refuse a duplicate /compact or /offload while a manual compaction is
    // already running (#1). The duplicate would 409 ("compaction_in_progress")
    // on the backend, but it first bumps the generation token — which would
    // strand isCompacting, since the real (earlier-generation) compaction's
    // completion could then no longer clear the flag. Block it before it enters
    // the generation protocol. (An auto Tier-2 summarize has isLoading=true, so
    // this guard does not fire there.)
    if (
      (cmd.name === 'compact' || cmd.name === 'offload') &&
      isManualCompactionInFlight({ isCompacting, isLoading })
    ) {
      insertNotification(t('chat.compactBusy'), 'warning');
      return;
    }

    if (cmd.name === 'compact') {
      // SSE wire action value "summarize" is preserved as a protocol contract.
      userStoppedCompactionRef.current = false;
      const myGeneration = ++compactionGenerationRef.current;
      setIsCompacting('summarize');
      summarizeThread(tid)
        .then((data: Record<string, unknown>) => {
          clearIfCurrent(myGeneration);
          const detail = (data.summary_text as string | undefined) || undefined;
          insertNotification(
            t('chat.compactedNotification', { from: data.original_message_count }),
            'info',
            detail,
          );
        })
        .catch((err: unknown) => {
          console.error('[ChatView] Compaction failed:', err);
          handleActionError(err);
          clearIfCurrent(myGeneration);
        });
    } else if (cmd.name === 'offload') {
      userStoppedCompactionRef.current = false;
      const myGeneration = ++compactionGenerationRef.current;
      setIsCompacting('offload');
      offloadThread(tid)
        .then((data: Record<string, unknown>) => {
          clearIfCurrent(myGeneration);
          insertNotification(
            t('chat.offloadedNotification', {
              args: (data.offloaded_args as number) || 0,
              reads: (data.offloaded_reads as number) || 0,
            }),
          );
        })
        .catch((err: unknown) => {
          console.error('[ChatView] Offload failed:', err);
          handleActionError(err);
          clearIfCurrent(myGeneration);
        });
    }
  }, [currentThreadId, threadId, insertNotification, setIsCompacting, isCompacting, isLoading, t]);

  // Show sidebar at the start of each backend response (streaming)
  // Auto-refresh workspace files when agent finishes (isLoading transitions true→false)
  const prevLoadingRef = useRef(false);
  useEffect(() => {
    const wasLoading = prevLoadingRef.current;
    prevLoadingRef.current = isLoading;
    if (isLoading && !wasLoading) {
      setWasStopped(false);
    }
    if (!isLoading && wasLoading) {
      refreshFiles();
    }
  }, [isLoading, refreshFiles]);









  // Right-panel controller (chatView/useRightPanel).
  const {
    panelTarget,
    handleTargetFileHandled,
    handleTargetDirHandled,
    handleTargetMemoryHandled,
    handleTargetMemoHandled,
    rightPanelType,
    setRightPanelType,
    rightPanelWidth,
    previewData,
    panelWrapperRef,
    isDragging,
    dragJustEndedRef,
    handleDividerMouseDown,
    popPanelHistory,
    handleOpenFileFromChat,
    handleOpenSourcesFromChat,
    handleOpenStatusFromChat,
    handleOpenDirFromChat,
    handleToolCallDetailClick,
    handlePlanDetailClick,
    handleCloseDetailPanel,
    handleClosePreview,
    handleRefreshPreview,
    handleToggleFilePanel,
    handleOpenPreview,
    detailToolCall,
    detailPlanData,
    sourcesRecords,
    allSourcesRecords,
  } = useRightPanel({
    isMobile,
    workspaceId,
    isActive,
    containerRef,
    setFilePanelWorkspaceId,
    messages,
  });

  // Keep the ref in sync so SSE events (via handleOpenPreviewFromStream) use the latest closure
  openPreviewRef.current = handleOpenPreview;


  // Open a file in the right panel from chat tool calls
  // --- Mobile back-button integration for panels ---






















  // Add context from FilePanel or message selection to ChatInput
  const handleAddContext = useCallback((ctx: any) => { // TODO: type properly
    chatInputRef.current?.addContext(ctx);
  }, []);

  // Message text selection → "Add to context" tooltip
  const [msgSelectionTooltip, setMsgSelectionTooltip] = useState<MsgSelectionTooltipData | null>(null);
  const msgAreaRef = useRef<HTMLDivElement>(null);
  // Collapse avatars when the messages column is too narrow to comfortably
  // accommodate them (mobile, side panels, etc.). 640px matches the visual
  // breakpoint where avatar gutters start crowding the message bubble.
  const isNarrowChat = useNarrowContainer(msgAreaRef, 640);

  const handleMessageMouseUp = useCallback(() => {
    // Small delay to let the browser finalize the selection
    setTimeout(() => {
      const sel = window.getSelection();
      if (!sel || !sel.toString().trim()) {
        setMsgSelectionTooltip(null);
        return;
      }
      const text = sel.toString();
      const range = sel.getRangeAt(0);
      const rect = range.getBoundingClientRect();
      const area = msgAreaRef.current;
      const areaRect = area?.getBoundingClientRect();
      if (!areaRect) return;

      setMsgSelectionTooltip({
        x: rect.left - areaRect.left + rect.width / 2,
        y: rect.top - areaRect.top - 8,
        text,
      });
    }, 10);
  }, []);

  const handleAddMessageContext = useCallback(() => {
    if (!msgSelectionTooltip) return;
    const text = msgSelectionTooltip.text;
    const lineCount = (text.match(/\n/g) || []).length + 1;
    // Label: show line count for multi-line, or truncated text for single-line
    const label = lineCount > 1
      ? `chat: ${lineCount} lines`
      : (text.length > 30 ? text.slice(0, 27).trim() + '...' : text);
    chatInputRef.current?.addContext({
      snippet: text,
      label,
      lineCount,
      source: 'chat',
    });
    setMsgSelectionTooltip(null);
    window.getSelection()?.removeAllRanges();
  }, [msgSelectionTooltip]);

  // Clear tooltip on mousedown (unless clicking the tooltip itself)
  useEffect(() => {
    if (!msgSelectionTooltip) return;
    const handler = (e: MouseEvent) => {
      if ((e.target as HTMLElement)?.closest?.('.chat-selection-tooltip')) return;
      setTimeout(() => {
        const sel = window.getSelection();
        if (!sel || !sel.toString().trim()) setMsgSelectionTooltip(null);
      }, 10);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [msgSelectionTooltip]);


  // Mobile: tap top bar to scroll chat to top
  const handleTopBarTap = useCallback((e: React.MouseEvent) => {
    if (!isMobile) return;
    if ((e.target as HTMLElement).closest('button, a')) return;
    const ref = activeAgentId === 'main' ? scrollAreaRef : subagentScrollAreaRef;
    const container = getScrollContainer(ref);
    if (container) withProgrammaticScroll(() => container.scrollTo({ top: 0, behavior: 'smooth' }), 'smooth');
  }, [isMobile, activeAgentId, getScrollContainer, withProgrammaticScroll, scrollAreaRef, subagentScrollAreaRef]);









  // Update URL when thread ID changes (e.g., when __default__ becomes actual thread ID)
  // Hidden views notify parent via onThreadResolved but skip URL navigate.
  useEffect(() => {
    if (currentThreadId && currentThreadId !== '__default__' && currentThreadId !== threadId && workspaceId) {
      // Notify parent so the cache key updates in-place (preserves instanceId)
      onThreadResolved?.(threadId, currentThreadId);
      if (isActive) {
        const activeTid = activeAgentIdRef.current !== 'main'
          ? activeAgentIdRef.current.replace('task:', '')
          : null;
        const path = activeTid
          ? `/chat/t/${currentThreadId}/${activeTid}`
          : `/chat/t/${currentThreadId}`;
        navigate(path, { replace: true, state: { workspaceId } });
      }
      // Invalidate thread cache so navigation panel picks up the new thread
      queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) });
    }
  }, [currentThreadId, threadId, workspaceId, navigate, queryClient, isActive, onThreadResolved, activeAgentIdRef]);

  // Auto-send initial message from navigation state (e.g., from Dashboard)
  useEffect(() => {
    // Hidden views must not send initial messages (R7 — all views share useLocation)
    if (!isActive) return;
    // Only proceed if we have the required IDs
    if (!workspaceId || !threadId) {
      return;
    }

    // Handle personalization / onboarding flow (isPersonalizing is the new flag;
    // isOnboarding is kept for backward compatibility)
    if ((location.state?.isPersonalizing || location.state?.isOnboarding) && !initialMessageSentRef.current && !isLoading && !isLoadingHistory) {
      initialMessageSentRef.current = true;
      // Clear navigation state to prevent re-sending on re-renders
      navigate(location.pathname, { replace: true, state: {} });
      // Small delay to ensure component is fully mounted
      setTimeout(() => {
        const personalizationMessage = "I'd like to set up my investment profile";
        const additionalContext = [
          {
            type: "skills",
            name: "onboarding",
            instruction: "Help the user set up their investment profile — watchlists, risk preferences, and alerts.",
          }
        ];
        handleSendMessage(personalizationMessage, false, additionalContext);
      }, 100);
      return;
    }

    // Handle modify preferences flow (from settings panel)
    if (location.state?.isModifyingPreferences && !initialMessageSentRef.current && !isLoading && !isLoadingHistory) {
      initialMessageSentRef.current = true;
      navigate(location.pathname, { replace: true, state: {} });
      setTimeout(() => {
        const modifyMessage = "I'd like to review and update my preferences.";
        const additionalContext = [
          {
            type: "skills",
            name: "user-profile",
            instruction: "The user wants to review and update their existing preferences. Start by fetching their current preferences with get_user_data(entity='preferences'), show them what's currently set, then ask what they'd like to change. Use AskUserQuestion to offer options. Only update the fields they want to change.",
          }
        ];
        handleSendMessage(modifyMessage, false, additionalContext);
      }, 100);
      return;
    }

    // Handle regular message flow
    if (location.state?.initialMessage && !initialMessageSentRef.current) {
      // Merge state.skills (names) into additionalContext as skill entries,
      // so hidden skills preloaded upstream (e.g. chart-annotation from
      // MarketView) stay active on the PTC side.
      const mergeSkills = (
        context: Record<string, unknown>[] | null | undefined,
        skills: unknown,
      ): Record<string, unknown>[] | null => {
        const base = Array.isArray(context) ? [...context] : [];
        if (Array.isArray(skills)) {
          for (const name of skills) {
            if (typeof name !== 'string' || !name) continue;
            if (base.some((c) => c?.type === 'skills' && c?.name === name)) continue;
            base.push({ type: 'skills', name });
          }
        }
        return base.length > 0 ? base : null;
      };

      // For new threads (__default__), send immediately without waiting for history
      // For existing threads, wait for history to finish loading
      if (threadId === '__default__') {
        // New thread - send immediately
        initialMessageSentRef.current = true;
        // Capture state values before clearing (navigate may update location ref)
        const { initialMessage, planMode, additionalContext, attachmentMeta, model, reasoningEffort, widgetSnapshots, chartSelections, skills } = location.state;
        const mergedContext = mergeSkills(additionalContext, skills);
        // Clear navigation state to prevent re-sending on re-renders
        navigate(location.pathname, { replace: true, state: {} });
        // Small delay to ensure component is fully mounted
        setTimeout(() => {
          handleSendMessage(initialMessage, planMode || false, mergedContext, attachmentMeta || null, { model, reasoningEffort, widgetSnapshots, chartSelections });
        }, 100);
      } else if (!isLoadingHistory && !isLoading) {
        // Existing thread - wait for history to load, then send
        // This ensures we don't send duplicate messages
        initialMessageSentRef.current = true;
        // Capture state values before clearing (navigate may update location ref)
        const { initialMessage, planMode, additionalContext, attachmentMeta, model, reasoningEffort, widgetSnapshots, chartSelections, skills } = location.state;
        const mergedContext = mergeSkills(additionalContext, skills);
        // Clear navigation state to prevent re-sending on re-renders
        navigate(location.pathname, { replace: true, state: {} });
        // Small delay to ensure component is fully mounted
        setTimeout(() => {
          handleSendMessage(initialMessage, planMode || false, mergedContext, attachmentMeta || null, { model, reasoningEffort, widgetSnapshots, chartSelections });
        }, 100);
      }
    }
  }, [location.state, workspaceId, threadId, isLoading, isLoadingHistory, handleSendMessage, navigate, location.pathname, isActive]);

  // Re-seed the widget context deck from navigation state when there's no
  // initialMessage (the auto-send branch above already consumes them inline).
  // Used by the ContextOverflowPill click handoff: dashboard → /chat with
  // queued widget cards but no auto-send.
  const widgetSnapshotReseedRef = useRef(false);
  useEffect(() => {
    if (widgetSnapshotReseedRef.current) return;
    const navState = location.state as LocationState | null;
    const snaps = navState?.widgetSnapshots;
    if (!snaps?.length || navState?.initialMessage) return;
    widgetSnapshotReseedRef.current = true;
    snaps.forEach((s) => chatInputRef.current?.addWidgetSnapshot(s));
    navigate(location.pathname, { replace: true, state: { ...navState, widgetSnapshots: undefined } });
  }, [location.state, location.pathname, navigate]);





  // Screen-reader announcements for tool-call completions (polite live region).
  const recentlyCompletedAnnouncement = useToolCallAnnouncer(messages);

  // Auto-scroll subagent view when active subagent's messages change
  // Uses the same smart-scroll logic: only scroll if user is near the bottom
  // Skipped when restoring a saved scroll position after tab switch
  useEffect(() => {
    if (skipSubagentAutoScrollRef.current) {
      skipSubagentAutoScrollRef.current = false;
      return;
    }
    if (!isSubagentNearBottomRef.current) return;
    if (!activeAgent || !subagentScrollAreaRef.current) return;
    const scrollContainer = subagentScrollAreaRef.current.querySelector('[data-radix-scroll-area-viewport]') ||
                           subagentScrollAreaRef.current.querySelector('.overflow-auto') ||
                           subagentScrollAreaRef.current;
    if (scrollContainer) {
      setTimeout(() => {
        scrollContainer.scrollTo({ top: scrollContainer.scrollHeight, behavior: 'smooth' });
      }, 0);
    }
  }, [activeAgent?.messages]);

  // When this view becomes active (thread switch or new thread):
  // 1. Inherit nav panel state from the shared signal so it stays open across switches
  // 2. Scroll to bottom — while hidden (display:none) auto-scroll is a no-op
  const prevIsActiveRef = useRef(false);
  // Keep the latest reconnect-on-reactivate fn in a ref so the become-active
  // effect can fire it without listing an unstable closure in its deps (which
  // would re-run the nav/scroll restore on every render).
  const reconnectIfStaleRunRef = useRef(reconnectIfStaleRun);
  reconnectIfStaleRunRef.current = reconnectIfStaleRun;
  useEffect(() => {
    if (isActive && !prevIsActiveRef.current) {
      const wantNavVisible = inheritNavOnActivate();

      const tidNow = currentThreadId || threadId;
      requestAnimationFrame(() => {
        if (wantNavVisible) skipNavAnimRef.current = false;
        // First-mount restore is owned by the entry-restore effect. Here we only
        // catch up a cached re-entry to the bottom if the user left it at bottom;
        // otherwise the DOM scroll position preserved under display:none stands.
        if (restoredForThreadRef.current === tidNow && isNearBottomRef.current) {
          pinToBottom('auto');
        }
      });

      // Cached views stay mounted (useChatViewCache), so a run that started on
      // this thread while it was hidden won't have re-fired the thread-load
      // effect. Reconnect to the live run on reactivation — otherwise the view
      // shows the prior, completed turn (e.g. a second-round PTC dispatch into
      // an already-visited thread) until a full refresh.
      void reconnectIfStaleRunRef.current();
    }
    prevIsActiveRef.current = isActive;
  }, [isActive, getScrollContainer, currentThreadId, threadId, pinToBottom, inheritNavOnActivate, skipNavAnimRef, isNearBottomRef, restoredForThreadRef]);

  // Early return if workspaceId or threadId is missing
  if (!workspaceId || !threadId) {
    return (
      <div className="flex items-center justify-center h-full" style={{ backgroundColor: 'var(--color-bg-page)' }}>
        <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('chat.missingWorkspaceOrThread')}
        </p>
      </div>
    );
  }

  return (
    <WorkspaceProvider workspaceId={workspaceId} downloadFile={null}>
    <motion.div
      ref={containerRef}
      initial={navPanelVisibleRef.current ? false : { y: 10 }}
      animate={{ y: 0 }}
      transition={{ duration: 0.35, ease: [0.22, 1, 0.36, 1] }}
      className={`flex w-full overflow-hidden ${isMobile ? 'h-full' : 'h-screen'}`}
      style={{
        backgroundColor: 'var(--color-bg-page)',
      }}
    >
      {/* Polite aria-live region for screen-reader announcements when tool
          calls reach a terminal state. Visually hidden via sr-only. */}
      <div aria-live="polite" aria-atomic="false" className="sr-only">
        {recentlyCompletedAnnouncement}
      </div>
      <ShareReportLinkModal
        open={shareLinkFile !== null}
        fileName={shareLinkFile?.split('/').pop() || ''}
        onCopyShareable={copyShareableReportLink}
        onCopyDirect={copyDirectReportLink}
        onClose={() => setShareLinkFile(null)}
      />
      {/* Left Side: Topbar + Sidebar + Chat Window */}
      <div className="flex flex-col flex-1 min-w-0">
        {/* Top bar */}
        <div className="flex items-center justify-between px-4 py-2 border-b min-w-0 flex-shrink-0" style={{ borderColor: 'var(--color-border-muted)', cursor: isMobile ? 'pointer' : undefined }} onClick={handleTopBarTap}>
          <div className="flex items-center gap-4 min-w-0 flex-shrink">
            <button
              onClick={() => {
                if (activeAgentId !== 'main') {
                  switchAgent('main');
                } else if (state?.fromThreadId) {
                  // Navigate back to the flash thread that dispatched this PTC thread
                  intentionalExitRef.current = true;
                  navigate(`/chat/t/${state.fromThreadId}`, {
                    state: {
                      workspaceId: state.fromWorkspaceId,
                      agentMode: 'flash',
                      workspaceStatus: 'flash',
                    },
                  });
                } else {
                  intentionalExitRef.current = true;
                  onBack();
                }
              }}
              className="p-2 rounded-md transition-colors flex-shrink-0"
              style={{ color: 'var(--color-text-primary)' }}
              title={activeAgentId !== 'main' ? t('chat.backToMain', 'Back to main') : state?.fromThreadId ? t('chat.backToFlash', 'Back to Flash') : t('workspace.backToThreads')}
              onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'; }}
              onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = ''; }}
            >
              <ArrowLeft className="h-5 w-5" />
            </button>
            {isMobile && (
              <button
                onClick={handleNavExpand}
                className="p-2 rounded-md transition-colors flex-shrink-0"
                style={{ color: 'var(--color-text-primary)' }}
                title="Menu"
              >
                <Menu className="h-5 w-5" />
              </button>
            )}
            <h1 className="text-base font-semibold whitespace-nowrap title-font truncate" style={{ color: 'var(--color-text-primary)' }}>
              {workspaceName || t('thread.workspace')}
            </h1>
            {isLoadingHistory ? (
              <span className="text-xs whitespace-nowrap" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('chat.loadingHistory')}
              </span>
            ) : null}
          </div>

          <div className="flex items-center gap-2">
            {currentThreadId && currentThreadId !== '__default__' && (
              <ShareButton threadId={currentThreadId} initialIsShared={threadIsShared} />
            )}
            {(!isFlashMode || filePanelWorkspaceId) && (
              <button
                onClick={handleToggleFilePanel}
                className="p-2 rounded-md transition-colors"
                style={{ color: 'var(--color-text-primary)', backgroundColor: rightPanelType === 'file' ? 'var(--color-border-muted)' : undefined }}
                title={t('chat.workspaceFiles')}
                onMouseEnter={(e) => { if (rightPanelType !== 'file') e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'; }}
                onMouseLeave={(e) => { if (rightPanelType !== 'file') e.currentTarget.style.backgroundColor = ''; }}
              >
                <FolderOpen className="h-5 w-5" />
              </button>
            )}
          </div>
        </div>

        {/* Content area: Navigation Panel Overlay + Chat Window */}
        <div ref={contentAreaRef} className="flex-1 flex overflow-hidden" style={{ position: 'relative', containerType: 'inline-size' }}>
          {/* Navigation trigger strip — hover zone (desktop only) */}
          {!isMobile && (
            <div
              style={{
                position: 'absolute',
                left: 0,
                top: 0,
                bottom: 0,
                width: 'clamp(24px, calc((100% - 768px) / 2), 80px)',
                zIndex: 41,
                pointerEvents: navPanelVisible ? 'none' : 'auto',
              }}
              onMouseEnter={handleNavEnter}
            />
          )}
          {/* Expand tab — desktop only, visible when panel is hidden */}
          {!isMobile && !navPanelVisible && (
            <button
              onClick={handleNavExpand}
              className="nav-panel-dismiss-btn"
              style={{
                position: 'absolute',
                left: 0,
                top: '50%',
                transform: 'translateY(-50%)',
                zIndex: 42,
                padding: '6px 2px',
                background: 'var(--color-bg-elevated)',
                border: '1px solid var(--color-border-muted)',
                borderLeft: 'none',
                cursor: 'pointer',
                borderRadius: '0 6px 6px 0',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
              }}
              title="Open navigation panel"
            >
              <PanelLeftOpen className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
            </button>
          )}
          {/* Mobile backdrop — dimmed overlay behind nav drawer */}
          {isMobile && navPanelVisible && (
            <div
              style={{
                position: 'absolute',
                inset: 0,
                zIndex: 39,
                backgroundColor: 'rgba(0, 0, 0, 0.5)',
              }}
              onClick={handleNavMinimize}
            />
          )}
          {/* Navigation panel area — responsive width, interactive only when visible */}
          <div
            style={{
              position: 'absolute',
              left: 0,
              top: 0,
              bottom: 0,
              width: 'min(320px, calc(100% - 48px))',
              zIndex: 40,
              pointerEvents: navPanelVisible ? 'auto' : 'none',
            }}
            onMouseEnter={!isMobile ? handleNavEnter : undefined}
            onMouseLeave={!isMobile ? handleNavLeave : undefined}
          >
            <AnimatePresence>
              {navPanelVisible && (
                <motion.div
                  initial={skipNavAnimRef.current ? false : { x: '-100%', opacity: 0 }}
                  animate={{ x: 0, opacity: 1 }}
                  exit={{ x: '-100%', opacity: 0 }}
                  transition={{ duration: 0.2, ease: [0.22, 1, 0.36, 1] }}
                  {...(isMobile ? {
                    drag: 'x' as const,
                    dragConstraints: { left: -320, right: 0 },
                    dragElastic: { left: 0.3, right: 0 },
                    onDragEnd: (_: unknown, info: PanInfo) => {
                      if (info.velocity.x < -300 || info.offset.x < -100) handleNavMinimize();
                    },
                  } : {})}
                  style={{ width: '100%', height: '100%', position: 'absolute', left: 0, top: 0 }}
                >
                  <NavigationPanel
                    headerActions={
                      <>
                        {/* Sidebar display options (workspace/thread visibility) —
                            pinned to the left edge; margin-right:auto pushes the pin +
                            minimize controls to the right of the header row. */}
                        <div style={{ marginRight: 'auto', display: 'flex', alignItems: 'center' }}>
                          <NavDisplayOptions />
                        </div>
                        {/* Pin toggle — desktop only, next to the minimize button */}
                        {!isMobile && (
                          <button
                            onClick={handleTogglePin}
                            className="nav-panel-dismiss-btn"
                            aria-pressed={navPinned}
                            style={{
                              padding: 4,
                              background: 'transparent',
                              border: 'none',
                              cursor: 'pointer',
                              borderRadius: 4,
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'center',
                            }}
                            title={navPinned ? t('nav.unpin') : t('nav.pin')}
                            aria-label={navPinned ? t('nav.unpin') : t('nav.pin')}
                          >
                            {navPinned
                              ? <PinOff className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
                              : <Pin className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />}
                          </button>
                        )}
                        {/* Minimize button — while pinned it unpins (un-docks) */}
                        <button
                          onClick={!isMobile && navPinned ? handleTogglePin : handleNavMinimize}
                          className="nav-panel-dismiss-btn"
                          style={{
                            padding: 4,
                            background: 'transparent',
                            border: 'none',
                            cursor: 'pointer',
                            borderRadius: 4,
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                          }}
                          title={!isMobile && navPinned ? t('nav.unpin') : t('nav.minimize')}
                          aria-label={!isMobile && navPinned ? t('nav.unpin') : t('nav.minimize')}
                        >
                          <Minus className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
                        </button>
                      </>
                    }
                    isActive={isActive}
                    workspaces={navWorkspaces}
                    workspaceThreads={navWorkspaceThreads}
                    currentWorkspaceId={workspaceId}
                    currentThreadId={currentThreadId || threadId}
                    agents={agents}
                    activeAgentId={activeAgentId}
                    expandWorkspace={navExpandWorkspace}
                    onSelectAgent={handleSelectAgent}
                    onRemoveAgent={handleRemoveAgent}
                    onNavigateThread={handleNavigateThread}
                    hasMore={navHasMore}
                    onLoadMore={navLoadAll}
                    onLoadMoreThreads={navLoadMoreThreads}
                    onReorderWorkspace={navCanReorderWorkspaces ? navReorderWorkspace : undefined}
                    onPinWorkspace={navPinWorkspace}
                    onRenameWorkspace={navRenameWorkspace}
                    onNewThread={handleNewThread}
                  />
                </motion.div>
              )}
            </AnimatePresence>
          </div>

          {/* Chat Window — nudge right when nav panel is open so content clears the overlay.
              Pinned + narrow content (e.g. right panel open): keep the panel visible but
              drop the push so chat isn't crushed — the panel overlays instead. */}
          <div
            className="flex-1 flex flex-col overflow-hidden min-w-0"
            style={{
              paddingLeft: !isMobile && navPanelVisible && !(navPinned && contentNarrow)
                ? 'min(320px, max(0px, calc(1424px - 100%)))'
                : 0,
              transition: 'padding-left 0.2s cubic-bezier(0.22, 1, 0.36, 1)',
            }}
          >
            {/* Messages Area - Fixed height, scrollable */}
            {/* Subscribe inline subagent cards directly to live telemetry. The
                resolver identity changes on every SSE token (cards is a dep),
                but only context consumers re-render — MessageBubble /
                MessageContentSegments stay React.memo'd. */}
            <SubagentTelemetryContext.Provider value={resolveSubagentTelemetry}>
            <div
              ref={msgAreaRef}
              className="flex-1 overflow-hidden"
              style={{
                minHeight: 0,
                height: 0, // Force flex-1 to work properly
                position: 'relative',
              }}
              onMouseUp={handleMessageMouseUp}
            >
              {/* Message selection tooltip */}
              {msgSelectionTooltip && (() => {
                const lines = (msgSelectionTooltip.text.match(/\n/g) || []).length + 1;
                return (
                  <div
                    className="chat-selection-tooltip file-panel-selection-tooltip"
                    style={{
                      left: Math.max(8, msgSelectionTooltip.x - 60),
                      top: Math.max(4, msgSelectionTooltip.y - 32),
                    }}
                    onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); handleAddMessageContext(); }}
                  >
                    <TextSelect className="h-3.5 w-3.5" style={{ color: 'var(--color-accent-primary)' }} />
                    {lines > 1 ? t('context.addNLinesToContext', { count: lines }) : t('context.addToContext')}
                  </div>
                );
              })()}
              {activeAgentId === 'main' ? (
                <ScrollArea ref={scrollAreaRef} className={`h-full w-full${!isMobile && !rightPanelType ? ' chat-scroll-hide-scrollbar' : ''}`}>
                  <div className={`${isMobile ? 'px-3 py-3' : 'px-6 py-4'} flex justify-center`}>
                    <div className="w-full max-w-3xl overflow-x-hidden">
                      <MessageList
                        messages={messages as unknown as MessageRecord[]}
                        isLoading={isLoading}
                        isLoadingHistory={isLoadingHistory}
                        hideAvatar={isNarrowChat}
                        onOpenFile={handleOpenFileFromChat}
                        onOpenSources={handleOpenSourcesFromChat}
                        onOpenDir={handleOpenDirFromChat}
                        onToolCallDetailClick={handleToolCallDetailClick}
                        onOpenSubagentTask={handleOpenSubagentTask}
                        onApprovePlan={handleApproveInterrupt}
                        onRejectPlan={handleRejectInterrupt}
                        onPlanDetailClick={handlePlanDetailClick}
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
                        flashContext={isFlashMode && currentThreadId ? { threadId: currentThreadId, workspaceId } : null}
                        onEditMessage={(id, content) => handleEditMessage(id, content, chatInputRef.current?.getModelOptions?.())}
                        onRegenerate={(id) => handleRegenerate(id, chatInputRef.current?.getModelOptions?.())}
                        onRetry={() => handleRetry(chatInputRef.current?.getModelOptions?.())}
                        onThumbUp={handleThumbUp}
                        onThumbDown={handleThumbDown}
                        getFeedbackForMessage={getFeedbackForMessage}
                        onReportWithAgent={(instruction) => {
                          handleSendMessage(`/self-improve ${instruction}`);
                        }}
                        onWidgetSendPrompt={handleSendMessage}
                      />
                    </div>
                  </div>
                </ScrollArea>
              ) : activeAgent ? (
                <ScrollArea ref={subagentScrollAreaRef} className="h-full w-full">
                  <div className={`${isMobile ? 'px-3 py-3' : 'px-6 py-4'} flex justify-center`}>
                    <div className="w-full max-w-3xl space-y-2.5">
                      {/* Task description as header */}
                      {activeAgent.description && (
                        <div style={{ color: 'var(--color-text-secondary)', fontSize: 13, fontWeight: 500 }}>
                          {activeAgent.description}
                        </div>
                      )}
                      {/* Prompt as user message bubble — matches MessageBubble user style.
                          Only until the transcript's own epoch-opening user bubble
                          arrives: new task streams open with the run's instruction as
                          a user_message, so rendering both would duplicate it. Legacy
                          tasks (no opener) keep the static bubble. */}
                      {activeAgent.prompt && activeAgent.messages?.[0]?.role !== 'user' && (
                        <div className="flex justify-end">
                          <div
                            className={`max-w-[80%] rounded-lg rounded-tr-none ${isMobile ? 'px-3 py-2' : 'px-4 py-3'} overflow-hidden`}
                            style={{
                              backgroundColor: 'var(--color-bg-elevated)',
                              color: 'var(--color-text-primary)',
                            }}
                          >
                            <Markdown
                              variant="chat"
                              content={normalizeSubagentText(activeAgent.prompt)}
                              className="text-sm leading-relaxed"
                            />
                          </div>
                        </div>
                      )}
                      {/* Status indicator */}
                      <SubagentStatusIndicator
                        status={activeAgent.status}
                        currentTool={activeAgent.currentTool}
                        toolCalls={activeAgent.toolCalls}
                        messages={(activeAgent.messages || []) as SubagentMessage[]}
                      />
                      {/* Messages — reuse MessageList */}
                      {(activeAgent.messages?.length ?? 0) > 0 && (
                        <div style={{ borderTop: '0.5px solid var(--color-border-muted)', paddingTop: '8px' }}>
                          <MessageList
                            messages={activeAgent.messages as MessageRecord[]}
                            isSubagentView={true}
                            hideAvatar={true}
                            onOpenFile={handleOpenFileFromChat}
                            onToolCallDetailClick={handleToolCallDetailClick}
                          />
                        </div>
                      )}
                    </div>
                  </div>
                </ScrollArea>
              ) : (
                // Active agent not found (may have been removed) - fallback
                <div className="flex items-center justify-center h-full">
                  <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('chat.agentNotFound')}
                  </p>
                </div>
              )}
              {/* Minimap TOC — desktop only, when no right panel open */}
              {!isMobile && !rightPanelType && activeAgentId === 'main' && (
                <ChatMinimap
                  messages={messages as unknown as MessageRecord[]}
                  scrollAreaRef={scrollAreaRef}
                />
              )}
              {/* Jump-to-latest pill — shown only when the minimap isn't (mobile,
                  right panel open, or <2 user messages); the minimap's Bottom
                  button covers the desktop case so exactly one affordance shows. */}
              {activeAgentId === 'main' && (isMobile || !!rightPanelType || userMsgCount < 2) && (
                <JumpToLatestPill
                  visible={jumpPill.visible}
                  hasNew={jumpPill.hasNew}
                  newCount={jumpPill.newCount}
                  onJump={() => pinToBottom('smooth')}
                />
              )}
            </div>
            </SubagentTelemetryContext.Provider>

            {/* Input Area */}
            <div className={`flex-shrink-0 ${isMobile ? 'p-3' : 'p-4'} flex justify-center`}>
              <div className="w-full max-w-3xl space-y-3">
                {activeAgentId === 'main' ? (
                  <>
                    <TodoDrawer todoData={cards['todo-list-card']?.todoData ?? null} />
                    {/* Watch chip + background-tasks notice share one line, chip
                        first. Both are presentational; either may be absent (the
                        chip self-hides when the watch list is empty). Matched pill
                        height (py-1) keeps them vertically aligned. */}
                    {(showWatchChip || showBackgroundTail) && (
                      <div className="flex items-center gap-2 flex-wrap">
                        <MarketWatchChip symbols={marketWatch?.symbols} lastUpdate={marketWatch?.timestamp} onClick={handleOpenStatusFromChat} />
                        {/* Tail mode: main turn finished but a dispatched subagent is
                            still running in the backend. Independent of stop. */}
                        {showBackgroundTail && (
                          <div className="flex items-center gap-2 px-3 py-1 text-xs text-muted-foreground">
                            <PulseDot color="hsl(var(--primary))" />
                            {t('chat.backgroundTasksRunning')}
                          </div>
                        )}
                      </div>
                    )}
                    {pendingRejection && (
                      <div
                        className="flex items-center gap-2 px-3 py-2 rounded-md text-sm"
                        style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-text-tertiary)', border: '1px solid var(--color-accent-soft)' }}
                      >
                        <ScrollText className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                        <span>{t('chat.planFeedbackHint')}</span>
                      </div>
                    )}
                    {messageError && !isLoading && (
                      <ErrorBanner error={messageError} />
                    )}
                    {isReconnecting && (
                      <div className="flex items-center gap-2 px-3 py-1.5 text-xs"
                        role="status" aria-live="polite"
                        style={{ color: 'var(--color-text-tertiary)' }}>
                        <Loader2 aria-hidden="true" className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
                        {t('chat.reconnecting', 'Reconnecting…')}
                      </div>
                    )}
                    <ModelStatusPill modelStatus={modelStatus} isLoading={isLoading} />
                    <FallbackSuggestionPill
                      fallbackSuggestion={fallbackSuggestion}
                      isLoading={isLoading}
                      inputModel={inputModel}
                      lastThreadModel={lastThreadModel}
                      activePreferredModel={activePreferredModel}
                      onSwitchModel={handleSwitchModel}
                      onDismiss={clearFallbackSuggestion}
                    />
                    {/* Report-back pending: a follow-up turn will land here —
                        a flash summary of dispatched PTC thread(s), or a PTC
                        notification for an unseen subagent result. Suppressed
                        while the tail chip above already covers running
                        subagents (they overlap only on PTC threads). */}
                    {awaitingReportBack && !isLoading && !hasActiveSubagents && (
                      <div className="flex items-center gap-2 px-3 py-1.5 text-xs text-muted-foreground"
                        role="status" aria-live="polite">
                        <span aria-hidden="true" className="relative flex h-2 w-2">
                          <span className="motion-safe:animate-ping absolute inline-flex h-full w-full rounded-full bg-primary/60 opacity-75" />
                          <span className="relative inline-flex rounded-full h-2 w-2 bg-primary/80" />
                        </span>
                        {t(isFlashMode ? 'chat.reportBackPending' : 'chat.taskReportBackPending')}
                      </div>
                    )}
                    {displayWorkspaceStarting && (
                      <div className="flex items-center gap-2 px-3 py-1.5 text-xs"
                        style={{ color: 'var(--color-text-tertiary)' }}>
                        <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
                        <span>{t(displayWorkspaceStarting === 'archived' ? 'chat.workspaceRestoring' : 'chat.workspaceStarting')}</span>
                        <HoverCard openDelay={150} closeDelay={100}>
                          <HoverCardTrigger asChild>
                            <button
                              type="button"
                              aria-label={t('chat.workspaceStateHelp')}
                              className="inline-flex items-center justify-center rounded-full p-0.5 hover:opacity-80 focus:outline-none focus-visible:ring-1 focus-visible:ring-current"
                              style={{ color: 'var(--color-text-quaternary)' }}
                            >
                              <Info className="h-3 w-3" />
                            </button>
                          </HoverCardTrigger>
                          <HoverCardContent side="top" align="start" className="w-80 text-xs leading-relaxed">
                            <div className="font-medium mb-1" style={{ color: 'var(--color-text-primary)' }}>
                              {t(displayWorkspaceStarting === 'archived' ? 'chat.workspaceStateArchivedTitle' : 'chat.workspaceStateStartingTitle')}
                            </div>
                            <p style={{ color: 'var(--color-text-secondary)' }}>
                              {t(displayWorkspaceStarting === 'archived' ? 'chat.workspaceStateArchivedBody' : 'chat.workspaceStateStartingBody')}
                            </p>
                            {displayWorkspaceStarting === 'archived' && (
                              <p className="mt-2" style={{ color: 'var(--color-text-tertiary)' }}>
                                {t('chat.workspaceStateArchivedFootnote')}
                              </p>
                            )}
                          </HoverCardContent>
                        </HoverCard>
                      </div>
                    )}
                    {isCompacting && (
                      <div className="flex items-center gap-2 px-3 py-1.5 text-xs"
                        role="status" aria-live="polite"
                        style={{ color: 'var(--color-text-tertiary)' }}>
                        <Loader2 aria-hidden="true" className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
                        {t(isCompacting === 'offload' ? 'chat.offloading' : 'chat.compacting')}
                      </div>
                    )}
                    {queuedSend && (
                      <div className="flex items-center gap-2 px-3 py-1.5 text-xs"
                        role="status" aria-live="polite"
                        style={{ color: 'var(--color-text-tertiary)' }}
                        title={queuedSend === '…' ? undefined : queuedSend}>
                        <Clock aria-hidden="true" className="h-3.5 w-3.5" style={{ color: 'var(--color-accent-primary)' }} />
                        {t('chat.queuedSend')}
                      </div>
                    )}
                    <ChatInput
                      ref={chatInputRef}
                      onSend={handleSendWithAttachments}
                      disabled={isLoadingHistory || !workspaceId || !!pendingInterrupt}
                      onStop={handleStopButton}
                      isLoading={isLoading}
                      isCompacting={!!isCompacting}
                      placeholder={chatPlaceholder}
                      files={workspaceFiles}
                      tokenUsage={tokenUsage}
                      onAction={handleAction}
                      initialModel={lastThreadModel}
                      onModelChange={setInputModel}
                      threadModels={threadModels}
                      mode={isFlashMode ? 'fast' : 'ptc'}
                    />
                  </>
                ) : activeAgent ? (
                  <SubagentStatusBar agent={activeAgent} threadId={threadId} onInstructionSent={handleSubagentInstruction} />
                ) : null}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Mobile detail bottom sheet — always rendered so exit animation works */}
      {isMobile && (
        <MobileBottomSheet
          open={rightPanelType === 'detail' && !!(detailToolCall || detailPlanData)}
          onClose={handleCloseDetailPanel}
          sizing="fixed"
          style={{ paddingBottom: 'calc(var(--bottom-tab-height, 0px) + 16px)' }}
        >
          <Suspense fallback={null}>
            <DetailPanel
              toolCallProcess={detailToolCall}
              planData={detailPlanData}
              onClose={handleCloseDetailPanel}
              onOpenFile={handleOpenFileFromChat}
              onOpenSubagentTask={handleOpenSubagentTask}
            />
          </Suspense>
        </MobileBottomSheet>
      )}

      {/* Mobile preview bottom sheet */}
      {isMobile && (
        <MobileBottomSheet
          open={rightPanelType === 'preview' && !!previewData}
          onClose={handleClosePreview}
          sizing="fixed"
          height="75vh"
          className="!px-0 !overflow-hidden"
        >
          <Suspense fallback={null}>
            <PreviewViewer
              url={previewData?.url ?? ''}
              port={previewData?.port ?? 0}
              title={previewData?.title}
              loading={previewData?.loading}
              error={previewData?.error}
              onClose={handleClosePreview}
              onRefresh={handleRefreshPreview}
              reloadToken={previewData?.reloadToken}
            />
          </Suspense>
        </MobileBottomSheet>
      )}

      {/* Right Side: File panel (mobile overlay) or split panel (desktop) */}
      {isMobile ? (
        /* Mobile: no AnimatePresence — avoids exit animation restart when React Router
           re-renders mid-exit (popstate triggers RR location change during framer-motion
           exit, causing the panel to briefly re-appear and slide out again).
           Entry animation + drag-to-dismiss still work via motion.div. */
        rightPanelType === 'file' && (
          <motion.div
            key="file"
            initial={{ x: '100%' }}
            animate={{ x: 0 }}
            transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
            drag="x"
            dragConstraints={{ left: 0, right: 0 }}
            dragElastic={{ left: 0, right: 0.5 }}
            onDragEnd={(_: unknown, info: PanInfo) => {
              if (info.velocity.x > 300 || info.offset.x > 120) {
                setRightPanelType(null);
                popPanelHistory();
              }
            }}
            className="flex overflow-hidden mobile-panel-overlay"
            style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, zIndex: 30, backgroundColor: 'var(--color-bg-page)' }}
          >
            <div className="flex-shrink-0 h-full" style={{ width: '100%' }}>
              <Suspense fallback={null}>
                <WorkspaceProvider workspaceId={effectiveFileWorkspaceId || workspaceId} downloadFile={null}>
                <RightPanel
                  workspaceId={effectiveFileWorkspaceId || workspaceId}
                  onClose={() => { setRightPanelType(null); popPanelHistory(); }}
                  panelTarget={panelTarget}
                  onTargetFileHandled={handleTargetFileHandled}
                  onTargetDirHandled={handleTargetDirHandled}
                  onTargetMemoryHandled={handleTargetMemoryHandled}
                  onTargetMemoHandled={handleTargetMemoHandled}
                  sourcesRecords={sourcesRecords}
                  allSourcesRecords={allSourcesRecords}
                  marketWatch={marketWatch}
                  onOpenFile={handleOpenFileFromChat}
                  files={workspaceFiles}
                  filesLoading={filesLoading}
                  filesError={filesError}
                  onRefreshFiles={refreshFiles}
                  onAddContext={handleAddContext}
                  showSystemFiles={showSystemFiles}
                  onToggleSystemFiles={() => {
                    setShowSystemFiles((v) => {
                      localStorage.setItem('filePanel.showSystemFiles', String(!v));
                      return !v;
                    });
                  }}
                  readOnly={isFlashMode}
                  singleFileMode={isFlashMode && !!filePanelWorkspaceId}
                  onCopyShareLink={isFlashMode ? null : handleCopyShareLink}
                />
                </WorkspaceProvider>
              </Suspense>
            </div>
          </motion.div>
        )
      ) : (
        <>
        {/* Resize divider — outside overflow-hidden panel so its wide hover zone isn't clipped */}
        {rightPanelType && (
          <div
            className={`chat-split-divider${isDragging ? ' dragging' : ''}`}
            onMouseDown={handleDividerMouseDown}
          />
        )}
        <AnimatePresence>
          {rightPanelType && (
            <motion.div
              ref={panelWrapperRef}
              initial={{ width: 0, opacity: 0 }}
              animate={{ width: rightPanelWidth, opacity: 1 }}
              exit={{ width: 0, opacity: 0 }}
              transition={(isDragging || dragJustEndedRef.current)
                ? { duration: 0 }
                : { duration: 0.25, ease: [0.22, 1, 0.36, 1] }
              }
              className="flex flex-shrink-0 overflow-hidden"
            >
              <div data-panel-inner className="flex-shrink-0 h-full" style={{ width: rightPanelWidth }}>
                <Suspense fallback={null}>
                  {rightPanelType === 'file' ? (
                    <WorkspaceProvider workspaceId={effectiveFileWorkspaceId || workspaceId} downloadFile={null}>
                    <RightPanel
                      workspaceId={effectiveFileWorkspaceId || workspaceId}
                      onClose={() => { setRightPanelType(null); popPanelHistory(); }}
                      panelTarget={panelTarget}
                      onTargetFileHandled={handleTargetFileHandled}
                      onTargetDirHandled={handleTargetDirHandled}
                      onTargetMemoryHandled={handleTargetMemoryHandled}
                      onTargetMemoHandled={handleTargetMemoHandled}
                      sourcesRecords={sourcesRecords}
                      allSourcesRecords={allSourcesRecords}
                      marketWatch={marketWatch}
                      onOpenFile={handleOpenFileFromChat}
                      files={workspaceFiles}
                      filesLoading={filesLoading}
                      filesError={filesError}
                      onRefreshFiles={refreshFiles}
                      onAddContext={handleAddContext}
                      showSystemFiles={showSystemFiles}
                      onToggleSystemFiles={() => {
                        setShowSystemFiles((v) => {
                          localStorage.setItem('filePanel.showSystemFiles', String(!v));
                          return !v;
                        });
                      }}
                      readOnly={isFlashMode}
                      singleFileMode={isFlashMode && !!filePanelWorkspaceId}
                      onCopyShareLink={isFlashMode ? null : handleCopyShareLink}
                    />
                    </WorkspaceProvider>
                  ) : rightPanelType === 'detail' && (detailToolCall || detailPlanData) ? (
                    <DetailPanel
                      toolCallProcess={detailToolCall}
                      planData={detailPlanData}
                      onClose={handleCloseDetailPanel}
                      onOpenFile={handleOpenFileFromChat}
                      onOpenSubagentTask={handleOpenSubagentTask}
                    />
                  ) : rightPanelType === 'preview' && previewData ? (
                    <PreviewViewer
                      url={previewData.url}
                      port={previewData.port}
                      title={previewData.title}
                      loading={previewData.loading}
                      error={previewData.error}
                      onClose={handleClosePreview}
                      onRefresh={handleRefreshPreview}
                      isDragging={isDragging}
                      reloadToken={previewData.reloadToken}
                    />
                  ) : null}
                </Suspense>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
        </>
      )}

    </motion.div>
    </WorkspaceProvider>
  );
}

export default ChatView;
