import React, { Suspense, useEffect, useRef, useState, useCallback, useMemo } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { ArrowLeft, FolderOpen, StopCircle, ScrollText, AlertTriangle, CheckCircle2, Circle, Loader2, TextSelect, Minus, PanelLeftOpen, Menu } from 'lucide-react';
import { useIsMobile, getIsMobileSnapshot } from '@/hooks/useIsMobile';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { usePreferences } from '@/hooks/usePreferences';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { updateCurrentUser } from '../../Dashboard/utils/api';
import { softInterruptWorkflow, getWorkspace, summarizeThread, offloadThread, getPreviewUrl } from '../utils/api';
import { useChatMessages } from '../hooks/useChatMessages';
import { saveChatSession, getChatSession, clearChatSession } from '../hooks/utils/chatSessionRestore';
import type { PreviewData } from '../hooks/utils/types';
import { clampPanelWidth as clampPanelWidthUtil } from '@/lib/panelUtils';
import { useCardState } from '../hooks/useCardState';
import { useWorkspaceFiles } from '../hooks/useWorkspaceFiles';
import './FilePanel.css';
import ChatInput, { type ChatInputHandle } from '../../../components/ui/chat-input';
import { attachmentsToImageContexts, type Attachment } from '../utils/fileUpload';
import MessageList, { normalizeSubagentText } from './MessageList';
import Markdown from './Markdown';
import NavigationPanel from './NavigationPanel';
import ChatMinimap from './ChatMinimap';
import { useNavigationData } from '../hooks/useNavigationData';
import ShareButton from './ShareButton';
import { WorkspaceProvider } from '../contexts/WorkspaceContext';
import SubagentStatusBar from './SubagentStatusBar';
import TodoDrawer from './TodoDrawer';
import { parseErrorMessage } from '../utils/parseErrorMessage';
import { motion, AnimatePresence, type PanInfo } from 'framer-motion';
import { MobileBottomSheet } from '@/components/ui/mobile-bottom-sheet';


const FilePanel = React.lazy(() => import('./FilePanel'));
const DetailPanel = React.lazy(() => import('./DetailPanel'));
const PreviewViewer = React.lazy(() => import('./viewers/PreviewViewer'));

// --- Types ---

type MessageRecord = Record<string, unknown>;

interface LocationState {
  agentMode?: string;
  workspaceStatus?: string | null;
  initialMessage?: string;
  planMode?: boolean;
  additionalContext?: Record<string, unknown>[] | null;
  attachmentMeta?: Record<string, unknown>[] | null;
  model?: string;
  reasoningEffort?: string;
  isOnboarding?: boolean;
  isModifyingPreferences?: boolean;
  workspaceId?: string;
  workspaceName?: string;
  [key: string]: unknown;
}

interface ToolCallProcessRecord {
  toolName?: string;
  toolCallResult?: { artifact?: { type?: string } };
  [key: string]: unknown;
}

interface PlanData {
  [key: string]: unknown;
}

/** Subagent message shape (matches useCardState's SubagentMessage) */
interface SubagentMessage {
  role: string;
  isStreaming?: boolean;
  toolCallProcesses?: Record<string, { isInProgress?: boolean; toolName?: string; [key: string]: unknown }>;
  [key: string]: unknown;
}

interface AgentInfo {
  id: string;
  name: string;
  displayName?: string;
  taskId: string;
  description: string;
  prompt?: string;
  type: string;
  status: string;
  toolCalls: number;
  currentTool: string;
  messages: SubagentMessage[];
  isActive: boolean;
  isMainAgent: boolean;
  [key: string]: unknown;
}

/** Subagent card update data passed to updateSubagentCard */
interface SubagentUpdateData {
  agentId: string;
  taskId: string;
  description: string;
  prompt: string;
  type: string;
  isHistory: boolean;
  isActive: boolean;
  status?: string;
  toolCalls?: number;
  currentTool?: string;
  messages?: SubagentMessage[];
  [key: string]: unknown;
}

interface SubagentInfo {
  subagentId: string;
  description?: string;
  prompt?: string;
  type?: string;
  status?: string;
}

interface SlashCommand {
  type: string;
  name: string;
  skillName?: string;
}

interface ModelOptions {
  model?: string | null;
  reasoningEffort?: string | null;
}

interface ActionCommand {
  name: string;
  type?: string;
  skillName?: string;
  description?: string;
  aliases?: string[];
}

interface MsgSelectionTooltipData {
  x: number;
  y: number;
  text: string;
}

interface WorkspaceRecord {
  status?: string;
  name?: string;
  [key: string]: unknown;
}

interface ChatViewProps {
  workspaceId: string;
  threadId: string;
  initialTaskId?: string;
  onBack: () => void;
  workspaceName?: string;
  isActive?: boolean;
  onThreadResolved?: (oldThreadId: string, newThreadId: string) => void;
}

interface SubagentStatusIndicatorProps {
  status: string;
  currentTool: string;
  toolCalls?: number;
  messages?: SubagentMessage[];
}

// Shared nav panel state across ChatView instances — when switching threads,
// the newly active instance inherits this so the panel stays open.
const _sharedNav = { visible: false, locked: false };

// Static main agent object — never changes, so defined once at module level
const MAIN_AGENT: AgentInfo = {
  id: 'main',
  name: 'Boss',
  displayName: 'LangAlpha',
  taskId: '',
  description: '',
  type: 'main',
  status: 'active',
  toolCalls: 0,
  currentTool: '',
  messages: [],
  isActive: true,
  isMainAgent: true,
};

/**
 * SubagentStatusIndicator — inline status line for subagent view.
 */
function SubagentStatusIndicator({ status, currentTool, toolCalls = 0, messages = [] }: SubagentStatusIndicatorProps): React.ReactElement {
  const { t } = useTranslation();
  // Derive streaming state from messages (self-sufficient, no subagent_status dependency)
  const lastAssistant = [...messages].reverse().find(m => m.role === 'assistant');
  const isMessageStreaming = lastAssistant?.isStreaming === true;

  // Derive current tool from message state
  const derivedCurrentTool = (() => {
    if (currentTool) return currentTool;
    if (!lastAssistant?.toolCallProcesses) return '';
    const inProgress = Object.values(lastAssistant.toolCallProcesses).find(p => p.isInProgress);
    return (inProgress?.toolName as string) || '';
  })();

  // Effective status: only trust the authoritative card status for 'completed'
  // (set by openSubagentStream.finally when the per-task SSE closes).
  // Never derive 'completed' from message streaming gaps — those are transient,
  // especially after update/resume actions where there's a natural pause between
  // the old response ending and the new one starting.
  const effectiveStatus = status === 'completed'
    ? 'completed'
    : messages.length === 0
      ? 'initializing'
      : isMessageStreaming || derivedCurrentTool
        ? 'active'
        : status;

  const getIcon = (): React.ReactElement => {
    if (derivedCurrentTool) {
      return <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    if (effectiveStatus === 'active') {
      return <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    if (effectiveStatus === 'completed') {
      return <CheckCircle2 className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    return <Circle className="h-3.5 w-3.5" style={{ color: 'var(--color-icon-muted)' }} />;
  };

  const getText = (): string => {
    if (derivedCurrentTool) return t('chat.running', { tool: derivedCurrentTool });
    if (effectiveStatus === 'completed') {
      return toolCalls > 0 ? t('chat.completedWithCalls', { count: toolCalls }) : t('chat.completed');
    }
    if (effectiveStatus === 'active') {
      return t('chat.runningStatus');
    }
    return t('chat.initializing');
  };

  return (
    <div className="flex items-center gap-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
      {getIcon()}
      <span>{getText()}</span>
    </div>
  );
}

/**
 * ChatView Component
 *
 * Displays the chat interface for a specific workspace and thread.
 * Handles:
 * - Message display and streaming
 * - Auto-scrolling
 * - Navigation back to thread gallery
 * - Auto-sending initial message from navigation state
 *
 * @param {string} workspaceId - The workspace ID to chat in
 * @param {string} threadId - The thread ID to chat in
 * @param {Function} onBack - Callback to navigate back to thread gallery
 */
function ChatView({ workspaceId, threadId, initialTaskId, onBack, workspaceName: initialWorkspaceName, isActive = true, onThreadResolved }: ChatViewProps): React.ReactElement | null {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const containerRef = useRef<HTMLDivElement>(null);
  const scrollAreaRef = useRef<HTMLDivElement>(null);
  const subagentScrollAreaRef = useRef<HTMLDivElement>(null);
  const chatInputRef = useRef<ChatInputHandle>(null);
  const location = useLocation();
  const navigate = useNavigate();
  const { preferences } = usePreferences();
  const queryClient = useQueryClient();
  const preferredModel = (preferences as Record<string, unknown> | null)?.other_preference
    ? ((preferences as Record<string, unknown>).other_preference as Record<string, unknown>)?.preferred_model as string | null
    : null;
  const initialMessageSentRef = useRef(false);
  // Determine agent mode: flash workspaces use flash mode, otherwise ptc
  const state = location.state as LocationState | null;
  const [agentMode, setAgentMode] = useState(state?.agentMode || 'ptc');
  const isFlashMode = agentMode === 'flash' || state?.workspaceStatus === 'flash';
  const [workspaceName, setWorkspaceName] = useState(initialWorkspaceName || '');
  const [filePanelTargetFile, setFilePanelTargetFile] = useState<string | null>(null);
  const [filePanelTargetDir, setFilePanelTargetDir] = useState<string | null>(null);
  const isDraggingRef = useRef(false);
  const [isDragging, setIsDragging] = useState(false);
  // True for exactly one render after drag ends — forces transition duration:0
  // so Framer Motion jumps to the final width instead of animating from pre-drag.
  const dragJustEndedRef = useRef(false);

  // Right panel management - can show 'file', 'detail', 'preview', or null (closed)
  const [rightPanelType, setRightPanelType] = useState<'file' | 'detail' | 'preview' | null>(null);
  const [rightPanelWidth, setRightPanelWidth] = useState(750);
  const [previewData, setPreviewData] = useState<PreviewData | null>(null);
  const panelWrapperRef = useRef<HTMLDivElement>(null);

  // Clear the drag-just-ended flag after each render so future transitions animate normally.
  useEffect(() => { dragJustEndedRef.current = false; });
  // Active agent in main view (default: 'main', or from URL taskId)
  const [activeAgentId, setActiveAgentId] = useState(
    initialTaskId ? `task:${initialTaskId}` : 'main'
  );
  // Navigation panel visibility (hover-triggered overlay)
  // Initialize from shared state so thread switches inherit the panel's open/closed state.
  const [navPanelVisible, setNavPanelVisible] = useState(_sharedNav.visible);
  const navPanelVisibleRef = useRef(_sharedNav.visible);
  const navHideTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const navLockedRef = useRef(_sharedNav.locked);
  const contentAreaRef = useRef<HTMLDivElement>(null);
  const contentAreaWidthRef = useRef<number>(0);
  // Skip nav panel slide-in on mount if already open (inherited from previous thread).
  const skipNavAnimRef = useRef(_sharedNav.visible);
  useEffect(() => { skipNavAnimRef.current = false; return () => { if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current); }; }, []);
  // Auto-close nav panel when content area shrinks below threshold (e.g., right panel opens)
  useEffect(() => {
    const container = contentAreaRef.current;
    if (!container) return;
    const observer = new ResizeObserver((entries: ResizeObserverEntry[]) => {
      const width = entries[0].contentRect.width;
      contentAreaWidthRef.current = width;
      // Skip auto-hide on mobile — hamburger controls nav drawer
      if (getIsMobileSnapshot()) return;
      // Skip when view is hidden (display:none reports width 0) to avoid
      // corrupting _sharedNav for the incoming active view.
      if (!isActiveRef.current) return;
      if (width < 1100 && navPanelVisibleRef.current) {
        if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
        navPanelVisibleRef.current = false;
        _sharedNav.visible = false;
        setNavPanelVisible(false);
      }
    });
    observer.observe(container);
    return () => observer.disconnect();
  }, []);
  // Tool call detail panel state
  const [detailToolCall, setDetailToolCall] = useState<ToolCallProcessRecord | null>(null);
  // Plan detail panel state
  const [detailPlanData, setDetailPlanData] = useState<PlanData | null>(null);
  // Track hidden agents (removed from sidebar, but not from state)
  const [hiddenAgentIds, setHiddenAgentIds] = useState<Set<string>>(new Set());
  // Show system files in FilePanel (.agent/, code/, tools/, etc.)
  const [showSystemFiles, setShowSystemFiles] = useState(
    () => localStorage.getItem('filePanel.showSystemFiles') === 'true'
  );
  // Track whether the agent was soft-interrupted
  const [wasInterrupted, setWasInterrupted] = useState(false);
  // Track intentional back navigation (skip session save on unmount)
  const intentionalExitRef = useRef(false);
  // Ref mirrors isActive prop for use in unmount cleanup closures (R1)
  const isActiveRef = useRef(isActive);
  isActiveRef.current = isActive;

  // --- Scroll position memory for tab switching ---
  // Stores scrollTop per agentId so switching tabs preserves position
  const scrollPositionsRef = useRef<Record<string, number>>({});
  const activeAgentIdRef = useRef(activeAgentId);
  activeAgentIdRef.current = activeAgentId;
  // Flag to skip subagent auto-scroll when restoring a saved position
  const skipSubagentAutoScrollRef = useRef(false);

  // Helper: get the scrollable container from a ScrollArea ref
  const getScrollContainer = useCallback((ref: React.RefObject<HTMLDivElement | null>): HTMLElement | null => {
    if (!ref?.current) return null;
    return ref.current.querySelector('[data-radix-scroll-area-viewport]') ||
           ref.current.querySelector('.overflow-auto') ||
           ref.current;
  }, []);

  // Save scroll position of the currently active tab
  const saveScrollPosition = useCallback(() => {
    const currentId = activeAgentIdRef.current;
    const ref = currentId === 'main' ? scrollAreaRef : subagentScrollAreaRef;
    const container = getScrollContainer(ref);
    if (container) {
      scrollPositionsRef.current[currentId] = container.scrollTop;
    }
  }, [getScrollContainer]);

  // Ref for resolved thread ID — updated after useChatMessages, used in switchAgent
  // to avoid referencing currentThreadId (defined later) in useCallback closure.
  const resolvedThreadIdRef = useRef(threadId);

  // Switch agent tab with scroll position preservation
  const switchAgent = useCallback((newAgentId: string) => {
    if (newAgentId === activeAgentIdRef.current) return;
    const wasMain = activeAgentIdRef.current === 'main';
    saveScrollPosition();
    // If destination has a saved position, skip auto-scroll so restore wins
    if (scrollPositionsRef.current[newAgentId] != null) {
      skipSubagentAutoScrollRef.current = true;
    }
    setActiveAgentId(newAgentId);

    // Sync URL with active agent
    const tid = resolvedThreadIdRef.current || threadId;
    if (newAgentId === 'main') {
      // Replace: removes the subagent entry so browser back goes to thread gallery
      navigate(`/chat/t/${tid}`, { replace: true, state: { workspaceId } });
    } else {
      const taskSlug = newAgentId.replace('task:', '');
      // Push from main → subagent (back returns to main)
      // Replace from subagent → subagent (back still returns to main)
      navigate(`/chat/t/${tid}/${taskSlug}`, { replace: !wasMain, state: { workspaceId } });
    }
  }, [saveScrollPosition, threadId, workspaceId, navigate]);

  // Restore scroll position after the new tab mounts
  useEffect(() => {
    const savedPosition = scrollPositionsRef.current[activeAgentId];
    if (savedPosition == null) return;

    // requestAnimationFrame waits for DOM commit + layout
    requestAnimationFrame(() => {
      const ref = activeAgentId === 'main' ? scrollAreaRef : subagentScrollAreaRef;
      const container = getScrollContainer(ref);
      if (container) {
        container.scrollTop = savedPosition;
      }
    });
  }, [activeAgentId, getScrollContainer]);

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
    inactivateAllSubagents,
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
  // Skip for flash mode — no sandbox
  const {
    files: workspaceFiles,
    loading: filesLoading,
    error: filesError,
    refresh: refreshFiles,
  } = useWorkspaceFiles(isFlashMode ? null : workspaceId, { includeSystem: showSystemFiles });

  // Navigation panel data — workspaces + threads for the overlay sidebar
  const {
    workspaces: navWorkspaces,
    workspaceThreads: navWorkspaceThreads,
    expandWorkspace: navExpandWorkspace,
    hasMore: navHasMore,
    loadAll: navLoadAll,
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
    workspaceStarting,
    isCompacting,
    setIsCompacting,
    isLoadingHistory,
    isReconnecting: _isReconnecting,
    messageError,
    returnedSteering,
    clearReturnedSteering,
    handleSendMessage,
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
    tokenUsage,
    threadId: currentThreadId,
    threadModels,
    isShared: threadIsShared,
    insertNotification,
    handleEditMessage,
    handleRegenerate,
    handleRetry,
    handleThumbUp,
    handleThumbDown,
    getFeedbackForMessage,
    getSubagentHistory,
    resolveSubagentIdToAgentId,
  } = useChatMessages(workspaceId, threadId, updateTodoListCard as (todoData: Record<string, unknown>) => void, updateSubagentCard, inactivateAllSubagents, finalizePendingTodos, handleOnboardingRelatedToolComplete, refreshFiles, handleOpenPreviewFromStream, agentMode, clearSubagentCards, handleWorkspaceCreated);

  const chatPlaceholder = useMemo(() => {
    if (pendingRejection) return t('chat.placeholderPendingRejection');
    if (wasInterrupted && !isLoading && !pendingInterrupt && !pendingRejection)
      return t('chat.placeholderInterrupted');
    if (isLoading) return t('chat.placeholderLoading');
    if (hasActiveSubagents) return t('chat.placeholderSubagentsRunning');
    return t('chat.placeholderDefault');
  }, [pendingRejection, wasInterrupted, isLoading, pendingInterrupt, hasActiveSubagents, t]);

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

  // Soft-interrupt handler: pauses main agent while keeping subagents running
  const handleSoftInterrupt = useCallback(async () => {
    const tid = currentThreadId || threadId;
    if (!tid || tid === '__default__') return;
    try {
      await softInterruptWorkflow(tid);
      setWasInterrupted(true);
    } catch (e) {
      console.warn('[ChatView] Failed to soft-interrupt workflow:', e);
    }
  }, [currentThreadId, threadId]);

  // Wrapper: converts ChatInput's (message, planMode, attachments, slashCommands) into
  // handleSendMessage(message, planMode, additionalContext, attachmentMeta)
  const handleSendWithAttachments = useCallback((message: string, planMode: boolean, attachments: Attachment[] = [], slashCommands: SlashCommand[] = [], modelOptions: ModelOptions = {}) => {
    const contexts: Record<string, unknown>[] = [];
    let attachmentMeta: Record<string, unknown>[] | null = null;

    // Image/PDF contexts from attachments
    if (attachments && attachments.length > 0) {
      contexts.push(...(attachmentsToImageContexts(attachments) as unknown as Record<string, unknown>[]));
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

    const additionalContext = contexts.length > 0 ? contexts : null;
    handleSendMessage(message, planMode, additionalContext, attachmentMeta, modelOptions);
  }, [handleSendMessage]);

  // Handle action-type slash commands (e.g. /summarize, /compaction, /offload)
  const handleAction = useCallback((cmd: ActionCommand) => {
    const tid = currentThreadId || threadId;
    if (!tid || tid === '__default__') return;

    if (cmd.name === 'summarize') {
      setIsCompacting('summarize');
      summarizeThread(tid)
        .then((data: Record<string, unknown>) => {
          setIsCompacting(false);
          insertNotification(
            t('chat.summarizedNotification', { from: data.original_message_count }),
          );
        })
        .catch((err: unknown) => {
          console.error('[ChatView] Summarization failed:', err);
          const detail = (err as Record<string, unknown>)?.response
            ? ((err as Record<string, unknown>).response as Record<string, unknown>)?.data
              ? (((err as Record<string, unknown>).response as Record<string, unknown>).data as Record<string, unknown>)?.detail as string | undefined
              : undefined
            : undefined;
          insertNotification(detail || t('chat.compactionError'));
          setIsCompacting(false);
        });
    } else if (cmd.name === 'offload') {
      setIsCompacting('offload');
      offloadThread(tid)
        .then((data: Record<string, unknown>) => {
          setIsCompacting(false);
          insertNotification(
            t('chat.offloadedNotification', {
              args: (data.offloaded_args as number) || 0,
              reads: (data.offloaded_reads as number) || 0,
            }),
          );
        })
        .catch((err: unknown) => {
          console.error('[ChatView] Offload failed:', err);
          const detail = (err as Record<string, unknown>)?.response
            ? ((err as Record<string, unknown>).response as Record<string, unknown>)?.data
              ? (((err as Record<string, unknown>).response as Record<string, unknown>).data as Record<string, unknown>)?.detail as string | undefined
              : undefined
            : undefined;
          insertNotification(detail || t('chat.compactionError'));
          setIsCompacting(false);
        });
    }
  }, [currentThreadId, threadId, insertNotification, setIsCompacting, t]);

  // Show sidebar at the start of each backend response (streaming)
  // Auto-refresh workspace files when agent finishes (isLoading transitions true→false)
  const prevLoadingRef = useRef(false);
  useEffect(() => {
    const wasLoading = prevLoadingRef.current;
    prevLoadingRef.current = isLoading;
    if (isLoading && !wasLoading) {
      setWasInterrupted(false);
    }
    if (!isLoading && wasLoading) {
      refreshFiles();
    }
  }, [isLoading, refreshFiles]);

  // Ensure new active agents are visible (remove from hidden list)
  useEffect(() => {
    Object.entries(cards).forEach(([cardId, card]) => {
      if (cardId.startsWith('subagent-')) {
        const agentId = cardId.replace('subagent-', '');
        const isNewActiveAgent = card.subagentData?.isActive !== false && !card.subagentData?.isHistory;

        // If this is a new active agent, remove it from hidden list
        if (isNewActiveAgent && hiddenAgentIds.has(agentId)) {
          setHiddenAgentIds((prev) => {
            const newSet = new Set(prev);
            newSet.delete(agentId);
            return newSet;
          });
        }
      }
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cards]);

  // Convert cards to agents array for sidebar (memoized to avoid re-renders)
  const { subagentAgents, excessSubagents } = useMemo(() => {
    const maxSubagents = 11;
    const all = Object.entries(cards)
      .filter(([cardId]) => cardId.startsWith('subagent-'))
      .map(([cardId, card]): AgentInfo => {
        const sd = card.subagentData as Record<string, unknown> | undefined;
        return {
          id: cardId.replace('subagent-', ''),
          name: (sd?.displayId as string) || t('chat.worker'),
          taskId: (sd?.taskId as string) || (sd?.agentId as string) || '',
          description: (sd?.description as string) || '',
          prompt: (sd?.prompt as string) || '',
          type: (sd?.type as string) || 'general-purpose',
          status: (sd?.status as string) || 'active',
          toolCalls: (sd?.toolCalls as number) || 0,
          currentTool: (sd?.currentTool as string) || '',
          messages: (sd?.messages as SubagentMessage[]) || [],
          isActive: sd?.isActive !== false,
          isMainAgent: false,
        };
      })
      .reverse();
    const visible = all.filter(agent => !hiddenAgentIds.has(agent.id));
    return {
      subagentAgents: visible.slice(0, maxSubagents),
      excessSubagents: visible.slice(maxSubagents),
    };
  }, [cards, hiddenAgentIds, t]);

  // Auto-hide excess agents (beyond 11 subagents)
  const excessIds = useMemo(() => excessSubagents.map(a => a.id).join(','), [excessSubagents]);
  useEffect(() => {
    if (excessSubagents.length > 0) {
      setHiddenAgentIds((prev) => {
        const newSet = new Set(prev);
        excessSubagents.forEach(agent => {
          newSet.add(agent.id);
        });
        return newSet;
      });
    }
  }, [excessSubagents.length, excessIds]); // eslint-disable-line react-hooks/exhaustive-deps

  // Combine: main agent first, then visible subagents (limited to 11)
  const agents = useMemo((): AgentInfo[] => [MAIN_AGENT, ...subagentAgents], [subagentAgents]);

  // Find the active agent object for subagent view
  const activeAgent: AgentInfo | null = activeAgentId !== 'main'
    ? agents.find(a => a.id === activeAgentId) || null
    : null;

  // Callback: user sent an instruction to the active subagent via the status bar.
  // Immediately insert a pending user message (breathing animation) into the card.
  const handleSubagentInstruction = useCallback((content: string) => {
    if (!activeAgent) return;
    const agentId = activeAgent.id;
    const cardId = `subagent-${agentId}`;
    const card = cards[cardId];
    const existingMessages = card?.subagentData?.messages || [];

    const pendingMessage = {
      id: `pending-instruction-${Date.now()}`,
      role: 'user',
      content,
      contentSegments: [{ type: 'text', content, order: 0 }],
      reasoningProcesses: {},
      toolCallProcesses: {},
      isPending: true,
    };

    updateSubagentCard(agentId, {
      messages: [...existingMessages, pendingMessage],
    });
  }, [activeAgent, cards, updateSubagentCard]);


  const clampPanelWidth = useCallback(
    (desired: number) => clampPanelWidthUtil(desired, containerRef.current?.offsetWidth || window.innerWidth),
    [],
  );

  // Handle drag panel width — direct DOM manipulation for smooth, jank-free resize.
  // React state is only updated once on mouseup; during drag we bypass React/Framer.
  const PREVIEW_MAX_RATIO = 0.92;
  const handleDividerMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDraggingRef.current = true;
    setIsDragging(true);
    const startX = e.clientX;
    const startWidth = rightPanelWidth;
    const containerW = containerRef.current?.offsetWidth || window.innerWidth;
    const maxRatio = rightPanelType === 'preview' ? PREVIEW_MAX_RATIO : undefined;

    // Immediately disable pointer events on iframes to prevent them from
    // capturing mouse events during resize (can't wait for React re-render).
    const iframes = containerRef.current?.querySelectorAll('iframe');
    iframes?.forEach(iframe => { (iframe as HTMLIFrameElement).style.pointerEvents = 'none'; });

    // Grab DOM elements for direct manipulation (no React re-renders during drag)
    const wrapperEl = panelWrapperRef.current;
    const innerEl = wrapperEl?.querySelector<HTMLElement>('[data-panel-inner]');
    let currentWidth = startWidth;

    const onMouseMove = (moveEvent: MouseEvent) => {
      if (!isDraggingRef.current) return;
      const delta = startX - moveEvent.clientX;
      currentWidth = clampPanelWidthUtil(startWidth + delta, containerW, maxRatio);
      if (wrapperEl) wrapperEl.style.width = `${currentWidth}px`;
      if (innerEl) innerEl.style.width = `${currentWidth}px`;
    };

    const onMouseUp = () => {
      isDraggingRef.current = false;
      // Flag ensures the next render uses duration:0 so Framer doesn't
      // animate from the stale pre-drag width to the final width.
      dragJustEndedRef.current = true;
      setIsDragging(false);
      setRightPanelWidth(currentWidth);
      iframes?.forEach(iframe => { (iframe as HTMLIFrameElement).style.pointerEvents = ''; });
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, [rightPanelWidth, rightPanelType]);

  // Open a file in the right panel from chat tool calls
  // --- Mobile back-button integration for panels ---
  // Push a sentinel history entry when a panel opens so that the browser back
  // gesture closes the panel instead of navigating away from ChatView.
  //
  // Key: we use raw pushState (not React Router's navigate) and CLONE the
  // current history.state so React Router's idx/key tracking stays intact.
  // When the sentinel is popped, RR sees delta=0 and bails out — no re-render,
  // no route change, no flicker. Only our popstate handler fires to close the panel.
  //
  // Programmatic history.back() (explicit close) does NOT trigger iOS's visual
  // page transition — only the edge swipe gesture does.
  const panelHistoryPushedRef = useRef(false);

  const pushPanelHistory = useCallback(() => {
    if (!isMobile || panelHistoryPushedRef.current) return;
    panelHistoryPushedRef.current = true;
    window.history.pushState(
      { ...window.history.state, _panelSentinel: true },
      '',
      window.location.href,
    );
  }, [isMobile]);

  const popPanelHistory = useCallback(() => {
    if (!isMobile || !panelHistoryPushedRef.current) return;
    panelHistoryPushedRef.current = false;
    window.history.back();
  }, [isMobile]);

  // Listen for popstate — close panel if our sentinel was popped by back gesture
  useEffect(() => {
    if (!isMobile) return;
    const onPopState = () => {
      if (panelHistoryPushedRef.current) {
        panelHistoryPushedRef.current = false;
        setRightPanelType(null);
        setDetailToolCall(null);
        setDetailPlanData(null);
        setPreviewData(null);
      }
    };
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, [isMobile]);

  // Clean up sentinel on unmount (e.g. navigating away with panel still open).
  // Use replaceState to silently neutralize the sentinel instead of history.back(),
  // which would fire a popstate after our listener is already cleaned up and could
  // cause React Router to navigate backward unexpectedly.
  useEffect(() => {
    return () => {
      if (panelHistoryPushedRef.current) {
        panelHistoryPushedRef.current = false;
        const state = window.history.state;
        if (state?._panelSentinel) {
          window.history.replaceState(
            { ...state, _panelSentinel: undefined },
            '',
            window.location.href,
          );
        }
      }
    };
  }, []);

  const handleOpenFileFromChat = useCallback((filePath: string) => {
    setRightPanelWidth(clampPanelWidth(850));
    setRightPanelType('file');
    setFilePanelTargetDir(null);
    setFilePanelTargetFile(filePath);
    pushPanelHistory();
  }, [clampPanelWidth, pushPanelHistory]);

  // Open file panel filtered to a specific directory
  const handleOpenDirFromChat = useCallback((dirPath: string) => {
    setRightPanelWidth(clampPanelWidth(850));
    setRightPanelType('file');
    setFilePanelTargetFile(null);
    setFilePanelTargetDir(dirPath);
    pushPanelHistory();
  }, [clampPanelWidth, pushPanelHistory]);

  // Determine detail panel width based on content type
  const getDetailPanelWidth = useCallback((toolCallProcess: ToolCallProcessRecord | null) => {
    let desired = 650;
    if (!toolCallProcess) { desired = 550; }
    else {
      const toolName = toolCallProcess.toolName || '';
      const artifactType = toolCallProcess.toolCallResult?.artifact?.type;

      // Wide: file reading, SEC filings, subagent results
      if (artifactType === 'sec_filing') desired = 850;
      else if (toolName === 'Read' || toolName === 'read_file') desired = 850;
      else if (toolName === 'Task' || toolName === 'task') desired = 750;
      // Medium: charts, search results, default markdown
      else if (artifactType === 'stock_prices' || artifactType === 'market_indices' || artifactType === 'sector_performance') desired = 650;
      else if (toolName === 'WebSearch' || toolName === 'web_search') desired = 650;
      // Slim: compact data cards
      else if (artifactType === 'company_overview') desired = 480;
      else if (artifactType === 'automations') desired = 480;
    }
    return clampPanelWidth(desired);
  }, [clampPanelWidth]);

  // Resolve preview URL: get signed URL from backend, restart server on 503.
  const resolvePreviewUrl = useCallback(async (wid: string, port: number, command?: string): Promise<string> => {
    try {
      const result = await getPreviewUrl(wid, port, undefined);
      return result.url;
    } catch (err: unknown) {
      const status = (err as { response?: { status?: number } })?.response?.status;
      if (status === 503 && command) {
        const result = await getPreviewUrl(wid, port, command);
        return result.url;
      }
      throw err;
    }
  }, []);

  // Resolve a preview URL and update previewData state with the result
  const resolveAndSetPreview = useCallback((wid: string, port: number, command?: string) => {
    resolvePreviewUrl(wid, port, command)
      .then((url: string) => {
        setPreviewData(prev => prev?.port === port
          ? { ...prev, url, loading: false, error: undefined }
          : prev);
      })
      .catch(() => {
        setPreviewData(prev => prev?.port === port
          ? { ...prev, url: '', loading: false, error: true }
          : prev);
      });
  }, [resolvePreviewUrl]);

  // Open preview URL in right panel
  const handleOpenPreview = useCallback((data: PreviewData) => {
    setPreviewData(data);
    setRightPanelType('preview');
    const containerW = containerRef.current?.offsetWidth || window.innerWidth;
    setRightPanelWidth(clampPanelWidthUtil(850, containerW, PREVIEW_MAX_RATIO));
    pushPanelHistory();
    // If opened with loading state (no URL yet), resolve via authenticated endpoint
    if (data.loading && !data.url && workspaceId) {
      resolveAndSetPreview(workspaceId, data.port, data.command);
    }
  }, [pushPanelHistory, workspaceId, resolveAndSetPreview]);

  // Keep the ref in sync so SSE events (via handleOpenPreviewFromStream) use the latest closure
  openPreviewRef.current = handleOpenPreview;

  // Open tool call detail in right panel (or preview panel for preview_url artifacts)
  const handleToolCallDetailClick = useCallback((toolCallProcess: ToolCallProcessRecord) => {
    const artifact = toolCallProcess.toolCallResult?.artifact as Record<string, unknown> | undefined;
    if (artifact?.type === 'preview_url' && artifact.port && workspaceId) {
      const port = artifact.port as number;
      const title = artifact.title as string | undefined;
      const command = artifact.command as string | undefined;
      // Show cached URL instantly, then verify in background (handles sandbox recreation + dead server)
      if (previewData?.port === port && previewData.url) {
        handleOpenPreview({ ...previewData, loading: false, error: undefined });
        resolveAndSetPreview(workspaceId, port, command);
        return;
      }
      // No cache — resolve (restarts server if needed via 503 fallback)
      // handleOpenPreview will trigger resolution since loading=true and url=''
      handleOpenPreview({ url: '', port, title, command, loading: true });
      return;
    }
    setDetailToolCall(toolCallProcess);
    setDetailPlanData(null);
    setRightPanelWidth(getDetailPanelWidth(toolCallProcess));
    setRightPanelType('detail');
    pushPanelHistory();
  }, [getDetailPanelWidth, pushPanelHistory, workspaceId, handleOpenPreview, previewData, resolveAndSetPreview]);

  // Open plan detail in right panel
  const handlePlanDetailClick = useCallback((planData: PlanData) => {
    setDetailPlanData(planData);
    setDetailToolCall(null);
    setRightPanelWidth(clampPanelWidth(550));
    setRightPanelType('detail');
    pushPanelHistory();
  }, [clampPanelWidth, pushPanelHistory]);

  // Close detail panel (shared by MobileBottomSheet + DetailPanel onClose)
  const handleCloseDetailPanel = useCallback(() => {
    setRightPanelType(null);
    setDetailToolCall(null);
    setDetailPlanData(null);
    popPanelHistory();
  }, [popPanelHistory]);

  // Close preview panel (keep previewData cached for instant reopen)
  const handleClosePreview = useCallback(() => {
    setRightPanelType(null);
    popPanelHistory();
  }, [popPanelHistory]);

  // Refresh preview: restart process + resolve fresh signed URL (force bypasses cache)
  const handleRefreshPreview = useCallback(async () => {
    if (!previewData || !workspaceId) return;
    setPreviewData(prev => prev ? { ...prev, loading: true, error: undefined } : null);
    try {
      const result = await getPreviewUrl(workspaceId, previewData.port, previewData.command, true);
      setPreviewData(prev => prev ? { ...prev, url: result.url, loading: false } : null);
    } catch (e) {
      console.error('Failed to refresh preview:', e);
      setPreviewData(prev => prev ? { ...prev, loading: false, error: true } : null);
    }
  }, [previewData, workspaceId]);

  // Toggle file panel
  const handleToggleFilePanel = useCallback(() => {
    if (rightPanelType === 'file') {
      setRightPanelType(null);
      popPanelHistory();
    } else {
      setRightPanelWidth(clampPanelWidth(850));
      setRightPanelType('file');
      pushPanelHistory();
    }
  }, [rightPanelType, clampPanelWidth, pushPanelHistory, popPanelHistory]);

  // Add context from FilePanel or message selection to ChatInput
  const handleAddContext = useCallback((ctx: any) => { // TODO: type properly
    chatInputRef.current?.addContext(ctx);
  }, []);

  // Message text selection → "Add to context" tooltip
  const [msgSelectionTooltip, setMsgSelectionTooltip] = useState<MsgSelectionTooltipData | null>(null);
  const msgAreaRef = useRef<HTMLDivElement>(null);

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

  // Navigation panel hover handlers with 30s hide delay
  const handleNavEnter = useCallback(() => {
    if (navLockedRef.current) return; // locked after explicit minimize
    // Don't open if content area is too narrow (e.g., right panel consuming space)
    if ((contentAreaRef.current?.offsetWidth ?? Infinity) < 1100) return;
    if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
    navPanelVisibleRef.current = true;
    _sharedNav.visible = true;
    setNavPanelVisible(true);
  }, []);

  const handleNavLeave = useCallback(() => {
    if (navLockedRef.current) return;
    navHideTimerRef.current = setTimeout(() => {
      if (!isActiveRef.current) return;
      navPanelVisibleRef.current = false;
      _sharedNav.visible = false;
      setNavPanelVisible(false);
    }, 30000);
  }, []);

  const handleNavMinimize = useCallback(() => {
    if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
    navLockedRef.current = true;
    navPanelVisibleRef.current = false;
    _sharedNav.visible = false;
    _sharedNav.locked = true;
    setNavPanelVisible(false);
  }, []);

  // Mobile: tap top bar to scroll chat to top
  const handleTopBarTap = useCallback((e: React.MouseEvent) => {
    if (!isMobile) return;
    if ((e.target as HTMLElement).closest('button, a')) return;
    const ref = activeAgentId === 'main' ? scrollAreaRef : subagentScrollAreaRef;
    const container = getScrollContainer(ref);
    container?.scrollTo({ top: 0, behavior: 'smooth' });
  }, [isMobile, activeAgentId, getScrollContainer]);

  // Expand button explicitly unlocks and opens the panel
  const handleNavExpand = useCallback(() => {
    navLockedRef.current = false;
    _sharedNav.locked = false;
    if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
    navPanelVisibleRef.current = true;
    _sharedNav.visible = true;
    setNavPanelVisible(true);
  }, []);

  // Refresh subagent card with latest data from history or inline status.
  // Ensures status/currentTool are accurate regardless of stale streaming data.
  // agentId: stable agent_id (already resolved from toolCallId if needed)
  // overrides: optional { description, type, status } from inline card click
  const refreshSubagentCard = useCallback((agentId: string, overrides: Partial<SubagentInfo> = {}) => {
    if (!updateSubagentCard || !agentId) return;

    const history = getSubagentHistory ? getSubagentHistory(agentId) : null;
    // Preserve existing card description/type. Priority:
    // 1. History description (most authoritative — from replay)
    // 2. Existing card description (set during spawn — must not be overwritten
    //    by follow-up/resume inline cards whose description is the instruction)
    // 3. Override description (from inline card click — only used when card has
    //    no description yet, e.g., first open of a newly spawned task)
    const cardId = `subagent-${agentId}`;
    const existingDescription = cards[cardId]?.subagentData?.description;
    const existingPrompt = cards[cardId]?.subagentData?.prompt;
    const existingType = cards[cardId]?.subagentData?.type;
    const finalDescription = history?.description || existingDescription || overrides.description || '';
    const finalPrompt = history?.prompt || existingPrompt || overrides.prompt || '';
    const finalType = history?.type || existingType || overrides.type || 'general-purpose';
    const finalStatus = history?.status || overrides.status || 'completed';

    // Check if card is currently live (active with an open stream)
    const existingCard = cards[cardId]?.subagentData;
    const isLive = existingCard?.isActive && !history;

    const updateData: SubagentUpdateData = {
      agentId,
      taskId: agentId,
      description: finalDescription,
      prompt: finalPrompt,
      type: finalType,
      isHistory: !!history,
      // isActive: true bypasses the inactive-card guard so stale fields get cleared.
      // For history cards this will be immediately overridden to false by the
      // isHistory check inside updateSubagentCard.
      isActive: !history,
    };
    if (isLive) {
      // Card is actively streaming — preserve its current status, toolCalls, and currentTool.
      // Overwriting these causes a brief "completed" flash in the SubagentStatusBar.
    } else {
      updateData.status = finalStatus;
      updateData.toolCalls = 0;
      updateData.currentTool = '';
    }
    if (history) {
      updateData.messages = (history.messages || []) as SubagentMessage[];
    }

    updateSubagentCard(agentId, updateData);
  }, [updateSubagentCard, getSubagentHistory, cards]);

  // Handle sidebar agent selection — refresh card data, then switch tab
  const handleSelectAgent = useCallback((agentId: string) => {
    if (agentId !== 'main') {
      refreshSubagentCard(agentId);
    }
    switchAgent(agentId);
  }, [refreshSubagentCard, switchAgent]);

  // Open subagent task (navigate to subagent tab) - shared between MessageList and DetailPanel
  const handleOpenSubagentTask = useCallback((subagentInfo: SubagentInfo) => {
    const { subagentId, description, prompt, type, status } = subagentInfo;
    // Resolve subagentId (may be toolCallId from segment) to stable agent_id for card operations
    const agentId = resolveSubagentIdToAgentId
      ? resolveSubagentIdToAgentId(subagentId)
      : subagentId;

    if (!updateSubagentCard) {
      console.error('[ChatView] updateSubagentCard is not defined!');
      return;
    }

    refreshSubagentCard(agentId, { description, prompt, type, status });

    switchAgent(agentId);
  }, [resolveSubagentIdToAgentId, updateSubagentCard, refreshSubagentCard, switchAgent]);

  // Handle removing an agent from sidebar (just hide from display, don't affect state)
  const handleRemoveAgent = useCallback((agentId: string) => {
    // Add to hidden set
    setHiddenAgentIds((prev) => {
      const newSet = new Set(prev);
      newSet.add(agentId);
      return newSet;
    });

    // If the removed agent was active, fallback to main (preserving main's scroll position)
    if (activeAgentIdRef.current === agentId) {
      switchAgent('main');
    }
  }, [switchAgent]);

  // Sync activeAgentId with URL-derived initialTaskId (browser back/forward)
  useEffect(() => {
    const urlAgentId = initialTaskId ? `task:${initialTaskId}` : 'main';
    if (urlAgentId !== activeAgentIdRef.current) {
      saveScrollPosition();
      if (scrollPositionsRef.current[urlAgentId] != null) {
        skipSubagentAutoScrollRef.current = true;
      }
      setActiveAgentId(urlAgentId);
    }
  }, [initialTaskId, saveScrollPosition]);

  // Refresh subagent card data on deep link / browser forward (guarded to run once per taskId)
  const lastRefreshedTaskRef = useRef<string | undefined>(undefined);
  useEffect(() => {
    if (!initialTaskId || isLoadingHistory) {
      lastRefreshedTaskRef.current = undefined;
      return;
    }
    if (lastRefreshedTaskRef.current === initialTaskId) return;
    lastRefreshedTaskRef.current = initialTaskId;
    refreshSubagentCard(`task:${initialTaskId}`);
  }, [initialTaskId, isLoadingHistory, refreshSubagentCard]);

  // Update URL when thread ID changes (e.g., when __default__ becomes actual thread ID)
  // Hidden views notify parent via onThreadResolved but skip URL navigate.
  useEffect(() => {
    if (currentThreadId && currentThreadId !== '__default__' && currentThreadId !== threadId && workspaceId) {
      console.log('[ChatView] Thread ID changed from', threadId, 'to', currentThreadId, '- updating URL');
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
  }, [currentThreadId, threadId, workspaceId, navigate, queryClient, isActive, onThreadResolved]);

  // Auto-send initial message from navigation state (e.g., from Dashboard)
  useEffect(() => {
    // Hidden views must not send initial messages (R7 — all views share useLocation)
    if (!isActive) return;
    // Only proceed if we have the required IDs
    if (!workspaceId || !threadId) {
      return;
    }

    // Handle onboarding flow
    if (location.state?.isOnboarding && !initialMessageSentRef.current && !isLoading && !isLoadingHistory) {
      initialMessageSentRef.current = true;
      // Clear navigation state to prevent re-sending on re-renders
      navigate(location.pathname, { replace: true, state: {} });
      // Small delay to ensure component is fully mounted
      setTimeout(() => {
        const onboardingMessage = "Hi! I am new here and would like to set up my profile.";
        const additionalContext = [
          {
            type: "skills",
            name: "onboarding",
            instruction: "Help the user with first-time onboarding to set up their investment profile.",
          }
        ];
        handleSendMessage(onboardingMessage, false, additionalContext);
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
      // For new threads (__default__), send immediately without waiting for history
      // For existing threads, wait for history to finish loading
      if (threadId === '__default__') {
        // New thread - send immediately
        initialMessageSentRef.current = true;
        // Capture state values before clearing (navigate may update location ref)
        const { initialMessage, planMode, additionalContext, attachmentMeta, model, reasoningEffort } = location.state;
        // Clear navigation state to prevent re-sending on re-renders
        navigate(location.pathname, { replace: true, state: {} });
        // Small delay to ensure component is fully mounted
        setTimeout(() => {
          handleSendMessage(initialMessage, planMode || false, additionalContext || null, attachmentMeta || null, { model, reasoningEffort });
        }, 100);
      } else if (!isLoadingHistory && !isLoading) {
        // Existing thread - wait for history to load, then send
        // This ensures we don't send duplicate messages
        initialMessageSentRef.current = true;
        // Capture state values before clearing (navigate may update location ref)
        const { initialMessage, planMode, additionalContext, attachmentMeta, model, reasoningEffort } = location.state;
        // Clear navigation state to prevent re-sending on re-renders
        navigate(location.pathname, { replace: true, state: {} });
        // Small delay to ensure component is fully mounted
        setTimeout(() => {
          handleSendMessage(initialMessage, planMode || false, additionalContext || null, attachmentMeta || null, { model, reasoningEffort });
        }, 100);
      }
    }
  }, [location.state, workspaceId, threadId, isLoading, isLoadingHistory, handleSendMessage, navigate, location.pathname, isActive]);

  // Smart auto-scroll: only scroll to bottom when user is already near the bottom
  const isNearBottomRef = useRef(true);
  const isSubagentNearBottomRef = useRef(true);

  // Attach scroll listener to detect user scroll position
  // Re-attaches when activeAgentId changes (ScrollArea remounts on tab switch)
  useEffect(() => {
    const isMain = activeAgentId === 'main';
    const ref = isMain ? scrollAreaRef : subagentScrollAreaRef;
    const nearBottomRef = isMain ? isNearBottomRef : isSubagentNearBottomRef;

    if (!ref.current) return;
    const scrollContainer = ref.current.querySelector('[data-radix-scroll-area-viewport]') ||
                           ref.current.querySelector('.overflow-auto') ||
                           ref.current;
    if (!scrollContainer) return;

    // Reset to near-bottom when switching tabs
    nearBottomRef.current = true;

    const handleScroll = () => {
      const threshold = 120;
      const { scrollTop, scrollHeight, clientHeight } = scrollContainer;
      nearBottomRef.current = scrollHeight - scrollTop - clientHeight < threshold;
    };

    scrollContainer.addEventListener('scroll', handleScroll, { passive: true });
    return () => scrollContainer.removeEventListener('scroll', handleScroll);
  }, [activeAgentId]);

  // Auto-scroll main chat to bottom when messages change, but only if user is near the bottom
  useEffect(() => {
    if (!isNearBottomRef.current) return;
    if (!scrollAreaRef.current) return;

    const scrollContainer = scrollAreaRef.current.querySelector('[data-radix-scroll-area-viewport]') ||
                           scrollAreaRef.current.querySelector('.overflow-auto') ||
                           scrollAreaRef.current;
    if (scrollContainer) {
      setTimeout(() => {
        scrollContainer.scrollTo({ top: scrollContainer.scrollHeight, behavior: 'smooth' });
      }, 0);
    }
  }, [messages]);

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
  useEffect(() => {
    if (isActive && !prevIsActiveRef.current) {
      // Clear stale nav-hide timer from a previous activation period
      if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
      // Reset nav lock — matches the old per-thread-change reset so the
      // hover trigger zone works again after a minimize + thread switch.
      navLockedRef.current = false;
      _sharedNav.locked = false;
      // Sync nav panel from shared state
      navPanelVisibleRef.current = _sharedNav.visible;
      setNavPanelVisible(_sharedNav.visible);
      // Skip slide-in animation if inheriting open state
      if (_sharedNav.visible) skipNavAnimRef.current = true;

      requestAnimationFrame(() => {
        if (_sharedNav.visible) skipNavAnimRef.current = false;
        const container = getScrollContainer(scrollAreaRef);
        if (container) {
          container.scrollTo({ top: container.scrollHeight });
        }
      });
    }
    prevIsActiveRef.current = isActive;
  }, [isActive, getScrollContainer]);

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
      {/* Left Side: Topbar + Sidebar + Chat Window */}
      <div className="flex flex-col flex-1 min-w-0">
        {/* Top bar */}
        <div className="flex items-center justify-between px-4 py-2 border-b min-w-0 flex-shrink-0" style={{ borderColor: 'var(--color-border-muted)', cursor: isMobile ? 'pointer' : undefined }} onClick={handleTopBarTap}>
          <div className="flex items-center gap-4 min-w-0 flex-shrink">
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
            <button
              onClick={() => {
                if (activeAgentId !== 'main') {
                  switchAgent('main');
                } else {
                  intentionalExitRef.current = true;
                  onBack();
                }
              }}
              className="p-2 rounded-md transition-colors flex-shrink-0"
              style={{ color: 'var(--color-text-primary)' }}
              title={activeAgentId !== 'main' ? t('chat.backToMain', 'Back to main') : t('workspace.backToThreads')}
              onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'; }}
              onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = ''; }}
            >
              <ArrowLeft className="h-5 w-5" />
            </button>
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
            {!isFlashMode && (
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
                  {/* Minimize button — top right corner */}
                  <button
                    onClick={handleNavMinimize}
                    className="nav-panel-dismiss-btn"
                    style={{
                      position: 'absolute',
                      top: 8,
                      right: 8,
                      zIndex: 2,
                      padding: 4,
                      background: 'transparent',
                      border: 'none',
                      cursor: 'pointer',
                      borderRadius: 4,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                    }}
                    title="Minimize panel"
                  >
                    <Minus className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
                  </button>
                  <NavigationPanel
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
                  />
                </motion.div>
              )}
            </AnimatePresence>
          </div>

          {/* Chat Window — nudge right when nav panel is open so content clears the overlay */}
          <div
            className="flex-1 flex flex-col overflow-hidden min-w-0"
            style={{
              paddingLeft: !isMobile && navPanelVisible ? 'min(320px, max(0px, calc(1424px - 100%)))' : 0,
              transition: 'padding-left 0.2s cubic-bezier(0.22, 1, 0.36, 1)',
            }}
          >
            {/* Messages Area - Fixed height, scrollable */}
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
                        onOpenFile={handleOpenFileFromChat}
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
                      {/* Prompt as user message bubble — matches MessageBubble user style */}
                      {activeAgent.prompt && (
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
            </div>

            {/* Input Area */}
            <div className={`flex-shrink-0 ${isMobile ? 'p-3' : 'p-4'} flex justify-center`}>
              <div className="w-full max-w-3xl space-y-3">
                {activeAgentId === 'main' ? (
                  <>
                    <TodoDrawer todoData={cards['todo-list-card']?.todoData ?? null} />
                    {pendingRejection && (
                      <div
                        className="flex items-center gap-2 px-3 py-2 rounded-md text-sm"
                        style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-text-tertiary)', border: '1px solid var(--color-accent-soft)' }}
                      >
                        <ScrollText className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                        <span>{t('chat.planFeedbackHint')}</span>
                      </div>
                    )}
                    {wasInterrupted && !isLoading && !pendingInterrupt && !pendingRejection && (
                      <div
                        className="flex items-center gap-2 px-3 py-2 rounded-md text-sm"
                        style={{ backgroundColor: 'var(--color-loss-soft)', color: 'var(--color-text-tertiary)' }}
                      >
                        <StopCircle className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-loss)' }} />
                        <span>{t('chat.interruptedHint')}</span>
                      </div>
                    )}
                    {messageError && !isLoading && (() => {
                      const parsed = parseErrorMessage(messageError);
                      return (
                        <div
                          className="flex items-center gap-2 px-3 py-2 rounded-md text-sm"
                          style={{ backgroundColor: 'var(--color-loss-soft)', color: 'var(--color-loss)' }}
                        >
                          <AlertTriangle className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-loss)' }} />
                          <span>{parsed.detail ? `${parsed.title}: ${parsed.detail}` : parsed.title}</span>
                        </div>
                      );
                    })()}
                    {hasActiveSubagents && !isLoading && (
                      <div className="flex items-center gap-2 px-3 py-1.5 text-xs text-muted-foreground">
                        <span className="relative flex h-2 w-2">
                          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary/60 opacity-75" />
                          <span className="relative inline-flex rounded-full h-2 w-2 bg-primary/80" />
                        </span>
                        {t('chat.backgroundTasksRunning')}
                      </div>
                    )}
                    {workspaceStarting && (
                      <div className="flex items-center gap-2 px-3 py-1.5 text-xs"
                        style={{ color: 'var(--color-text-tertiary)' }}>
                        <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
                        {t('chat.workspaceStarting')}
                      </div>
                    )}
                    {isCompacting && (
                      <div className="flex items-center gap-2 px-3 py-1.5 text-xs"
                        style={{ color: 'var(--color-text-tertiary)' }}>
                        <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
                        {t(isCompacting === 'offload' ? 'chat.offloading' : 'chat.summarizing')}
                      </div>
                    )}
                    <ChatInput
                      ref={chatInputRef}
                      onSend={handleSendWithAttachments}
                      disabled={isLoadingHistory || !workspaceId || !!pendingInterrupt}
                      onStop={handleSoftInterrupt}
                      isLoading={isLoading}
                      placeholder={chatPlaceholder}
                      files={workspaceFiles}
                      tokenUsage={tokenUsage}
                      onAction={handleAction}
                      initialModel={threadModels[0] || preferredModel}
                      threadModels={threadModels}
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
                <FilePanel
                  workspaceId={workspaceId}
                  onClose={() => { setRightPanelType(null); popPanelHistory(); }}
                  targetFile={filePanelTargetFile}
                  onTargetFileHandled={() => setFilePanelTargetFile(null)}
                  targetDirectory={filePanelTargetDir}
                  onTargetDirHandled={() => setFilePanelTargetDir(null)}
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
                />
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
                    <FilePanel
                      workspaceId={workspaceId}
                      onClose={() => { setRightPanelType(null); popPanelHistory(); }}
                      targetFile={filePanelTargetFile}
                      onTargetFileHandled={() => setFilePanelTargetFile(null)}
                      targetDirectory={filePanelTargetDir}
                      onTargetDirHandled={() => setFilePanelTargetDir(null)}
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
                    />
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
