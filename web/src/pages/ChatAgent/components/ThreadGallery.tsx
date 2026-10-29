import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { ArrowLeft, Folder, FileText, Archive } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { SwapCell } from '@/components/ui/crossfade';
import { SkeletonBar } from '@/components/ui/skeleton';
import { cn } from '@/lib/utils';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { HomeRowIcon } from '@/hooks/useHomeWorkspace';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import { useNavigate, useParams, useLocation } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useInfiniteQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../../../lib/queryKeys';
import { patchThreadRows, rollbackThreadRows } from '@/lib/threadRowActions';
import { patchThreadTitle } from '@/lib/threadListCache';
import { threadGalleryQuery } from '../utils/threadGalleryQuery';
import { useWorkspace } from '../../../hooks/useWorkspace';
import { useScrollMemory } from '@/lib/scrollMemory';
import ThreadCard from './ThreadCard';
import RenameThreadModal from './RenameThreadModal';
import { useArchiveThreadConfirm } from './threadArchiveAction';
import { useThreadDeleteConfirm } from './threadDeleteAction';
import { deleteThreadRow } from '../hooks/deleteThreadRow';
import ChatInput from '../../../components/ui/chat-input';
import type { ChatInputHandle } from '../../../components/ui/chat-input';
import { attachmentsToContexts } from '../utils/fileUpload';
import FilePanel, { SYSTEM_DIR_PREFIXES } from './FilePanel';
import { clampPanelWidth as clampPanelWidthUtil } from '@/lib/panelUtils';
import SandboxSettingsPanel from './SandboxSettingsPanel';
import { updateThreadTitle, updateThread } from '../utils/api';
import { isValidUuid } from '../utils/uuid';
import { WorkspaceProvider } from '../contexts/WorkspaceContext';
import { useComputerFolders } from '../hooks/useComputerFolders';
import { usePanelFiles } from './chatView/usePanelFiles';
import { usePanelAsks, usePanelLinkOpen } from './chatView/usePanelLinks';
import { saveChatSession } from '../hooks/utils/chatSessionRestore';
import iconComputerLight from '../../../assets/img/icon-computer.svg';
import iconComputerDark from '../../../assets/img/icon-computer-dark.svg';
import { useTheme } from '../../../contexts/ThemeContext';
import { PresenceFold } from '@/components/ui/Disclosure';
import { motion, AnimatePresence } from '@/lib/framer';
import { DURATION, EASE_OUT, TWEEN } from '@/lib/motion';

/** Placeholder rows while the list is read: the first screen's worth. */
const THREAD_SKELETON_ROWS = 5;

/** A thread card's place while the list is read, in the card's box and with
 *  its two lines, spaced as the list spaces its cards. */
function ThreadCardSkeleton({ index }: { index: number }) {
  return (
    <div className="pb-2" aria-hidden="true" data-testid="thread-card-skeleton">
      <div className="rounded-lg px-4 py-3 flex items-center gap-3" style={{ borderBottom: '1px solid var(--color-border-muted)' }}>
        <div className="w-2 h-2 rounded-full shrink-0 animate-pulse" style={{ backgroundColor: 'var(--color-border-default)' }} />
        <div className="flex-1 min-w-0">
          <div className="text-sm"><SkeletonBar style={{ width: `${40 + (index % 3) * 15}%` }} /></div>
          <div className="text-xs mt-0.5"><SkeletonBar className="w-12" /></div>
        </div>
      </div>
    </div>
  );
}

interface ThreadRecord {
  thread_id: string;
  title?: string;
  thread_index?: number;
  current_status?: string;
  updated_at?: string;
  is_shared?: boolean;
  first_query_content?: string;
  [key: string]: unknown;
}

interface RenameModalState {
  isOpen: boolean;
  thread: ThreadRecord | null;
}

interface ThreadGalleryProps {
  workspaceId: string;
  onBack: () => void;
  onThreadSelect: (workspaceId: string, threadId: string) => void;
}

/**
 * ThreadGallery Component
 *
 * Displays a gallery of threads for a specific workspace.
 */
function ThreadGallery({ workspaceId, onBack, onThreadSelect }: ThreadGalleryProps) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const location = useLocation();
  const queryClient = useQueryClient();
  const { theme } = useTheme();
  const iconComputer = theme === 'light' ? iconComputerDark : iconComputerLight;

  // Workspace detail via React Query (useWorkspace)
  const { data: wsData, error: wsError } = useWorkspace(workspaceId);
  // Keep location.state values as instant display fallbacks during navigation
  const locationState = location.state as Record<string, unknown> | null;
  const label = useWorkspaceLabel();
  const workspaceName = (label(wsData) || locationState?.workspaceName || '') as string;
  // Home runs the full agent with a folder of its own, so its files show. It
  // still has no machine controls of its own.
  const isHome = isHomeWorkspace(wsData);

  // Archived view toggle. Both views live under the byWorkspace prefix
  // (queryKeys.threads.gallery), so prefix invalidations refresh both — and
  // the shared row patcher reaches this list's pages the same way it reaches
  // the sidebar's finite pages.
  const [showArchived, setShowArchived] = useState(false);
  // Whether the reader has switched views yet, in this workspace. The list the
  // gallery opens on arrives through the skeleton's crossfade; only a view the
  // reader picked fades in on its own.
  const [switchedIn, setSwitchedIn] = useState<string | null>(null);
  const viewSwitched = switchedIn === workspaceId;

  // Paged thread list. React Query owns the pages, the total, and the
  // has-more arithmetic; the rows are rendered straight from the cache so a
  // patch landing from anywhere (title generation, seen cursor, optimistic
  // create) paints here without a local copy to re-sync.
  const {
    data: threadPages,
    isLoading: isThreadsLoading,
    isPlaceholderData,
    error: threadError,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteQuery({
    ...threadGalleryQuery(workspaceId, showArchived),
    enabled: !!workspaceId,
    // Keep the outgoing list on screen (dimmed) while the archived toggle
    // fetches its view, instead of flashing the full-screen loader. Scoped to
    // the same workspace — switching workspaces must NOT show the previous
    // workspace's threads, so the placeholder only carries across the
    // archived-flag flip.
    placeholderData: (prev, prevQuery) => {
      const prevKey = prevQuery?.queryKey as unknown[] | undefined;
      return prevKey?.[2] === workspaceId ? prev : undefined;
    },
    retry: (failureCount, error) => {
      // Don't retry 403/404 — access denied or workspace not found won't resolve on retry
      const status = (error as { response?: { status?: number } })?.response?.status;
      if (status === 403 || status === 404) return false;
      return failureCount < 3;
    },
  });

  const threads = useMemo(
    () => (threadPages?.pages ?? []).flatMap((page) => page.threads || []),
    [threadPages],
  );
  // Row-order signature for framer-motion's layoutDependency: cards re-measure
  // only on genuine reorders, not on every gallery render (e.g. the file-panel
  // divider drag, which re-renders at pointer rate).
  const threadOrderSignature = useMemo(
    () => threads.map((th) => th.thread_id).join('|'),
    [threads],
  );
  // Every page echoes the server count (and the row patcher keeps all copies in
  // step), so the freshest page carries the current total.
  const totalThreads = threadPages?.pages[threadPages.pages.length - 1]?.total ?? null;

  // Detect 403 or 404 from either workspace or thread queries
  const accessDenied =
    (threadError as { response?: { status?: number } } | null)?.response?.status === 403 ||
    (wsError as { response?: { status?: number } } | null)?.response?.status === 403;
  const wsNotFound =
    (threadError as { response?: { status?: number } } | null)?.response?.status === 404 ||
    (wsError as { response?: { status?: number } } | null)?.response?.status === 404;

  // An empty outgoing list held while the other view loads says nothing about
  // that view: it loads as placeholders, not as "no threads".
  const isLoading = isThreadsLoading || (isPlaceholderData && threads.length === 0);
  const error = threadError && !accessDenied && !wsNotFound ? t('thread.failedLoadThreads') : null;
  const [renameModal, setRenameModal] = useState<RenameModalState>({ isOpen: false, thread: null });
  const [isRenaming, setIsRenaming] = useState(false);
  const [renameError, setRenameError] = useState<string | null>(null);
  const [isSendingMessage, setIsSendingMessage] = useState(false);
  const [showFilePanel, setShowFilePanel] = useState(false);
  const [showSandboxPanel, setShowSandboxPanel] = useState(false);
  const [filePanelWidth, setFilePanelWidth] = useState(850);
  // Expanded, the panel lies over the whole gallery, as it does over a chat.
  // Closing the panel ends it, so the next open starts beside the cards.
  const [panelExpanded, setPanelExpanded] = useState(false);
  if (panelExpanded && (!showFilePanel || isMobile)) setPanelExpanded(false);
  const handleTogglePanelExpanded = useCallback(() => setPanelExpanded((v) => !v), []);
  // Show system files in FilePanel (.agents/, code/, tools/, etc.)
  const [showSystemFiles, setShowSystemFiles] = useState(
    () => localStorage.getItem('filePanel.showSystemFiles') === 'true'
  );
  const [files, setFiles] = useState<string[]>([]);
  const isDraggingRef = useRef(false);
  const [isDragging, setIsDragging] = useState(false);
  // Armed for the duration of a divider drag; unmount mid-drag would otherwise
  // strand document listeners and app-wide col-resize/no-select body styles.
  const dragCleanupRef = useRef<(() => void) | null>(null);
  useEffect(() => () => dragCleanupRef.current?.(), []);
  const containerRef = useRef<HTMLDivElement>(null);
  const containerWidthRef = useRef<number>(0);
  const DIVIDER_WIDTH = 4; // px -- matches w-[4px] divider
  const chatInputRef = useRef<ChatInputHandle>(null);
  const handleAddContext = useCallback((ctx: Record<string, unknown>) => {
    chatInputRef.current?.addContext(ctx as any); // TODO: type properly
  }, []);

  const scrollContainerRef = useRef<HTMLDivElement>(null);
  // Keyed per view: the active and archived lists have unrelated heights, so
  // a saved offset from one must not restore into the other.
  useScrollMemory(scrollContainerRef, `threads:${workspaceId}:${showArchived ? 'archived' : 'active'}`);
  // Sentinel below the last card: intersecting it (within rootMargin) pulls the
  // next page. Also covers the short-list case a scroll listener can't — with
  // no overflow the sentinel is simply already on screen.
  const loadMoreSentinelRef = useRef<HTMLDivElement>(null);

  // Shared workspace files for the FilePanel.
  // A link in one of them can open a sibling's files read-only in their
  // place; the composer and the Files card keep listing the workspace's own.
  const {
    override: panelOverride,
    setOverride: setPanelOverride,
    shownWorkspaceId,
    files: panelFiles,
    loading: panelFilesLoading,
    error: panelFilesError,
    refresh: refreshPanelFiles,
    mentionFiles: ownFiles,
    panelAccess,
  } = usePanelFiles({
    isHome,
    workspaceId,
    workspaceName,
    includeSystem: showSystemFiles,
  });
  // A path names a sibling by its folder from the workspace it was written
  // in: the gallery's own for the link router, the shown one's for the
  // documents the panel renders.
  const folders = useComputerFolders(workspaceId);
  const panelFolders = useComputerFolders(shownWorkspaceId);
  const {
    panelTarget,
    askPanel,
    handleTargetHandled,
    handleTargetMemoryHandled,
    handleTargetMemoHandled,
    handleFilesLeaveGuardChange,
    leaveFiles,
  } = usePanelAsks();
  const handleOpenFile = usePanelLinkOpen({
    workspaceId,
    folders,
    override: panelOverride,
    setOverride: setPanelOverride,
    land: askPanel,
    leave: leaveFiles,
  });
  // The gallery stays mounted from one workspace to the next, and a sibling
  // the last one's panel showed need not be a sibling of this one.
  useEffect(() => {
    setPanelOverride(null);
  }, [workspaceId, setPanelOverride]);

  const navigate = useNavigate();
  const { threadId: currentThreadId } = useParams();

  // Redirect to workspace gallery when workspace is not found or access is denied
  useEffect(() => {
    if (wsNotFound || accessDenied) {
      navigate('/chat', { replace: true });
    }
  }, [wsNotFound, accessDenied, navigate]);

  // Sort helper for file list display
  const sortFiles = useCallback((fileList: string[]) => {
    const dirPriority = (fp: string) => {
      if (!fp.includes('/')) return 0;
      const dir = fp.slice(0, fp.indexOf('/'));
      if (dir === 'results') return 1;
      if (dir === 'data') return 2;
      return 3;
    };
    return [...fileList].sort((a, b) => {
      const pa = dirPriority(a);
      const pb = dirPriority(b);
      if (pa !== pb) return pa - pb;
      return a.localeCompare(b);
    });
  }, []);

  const clampPanelWidth = useCallback(
    (desired: number) => clampPanelWidthUtil(desired, containerWidthRef.current),
    [],
  );

  // Track container width via ResizeObserver
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const observer = new ResizeObserver((entries: ResizeObserverEntry[]) => {
      containerWidthRef.current = entries[0].contentRect.width;
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  // Derive sorted file list from hook data
  useEffect(() => {
    if (ownFiles.length > 0) {
      const sorted = sortFiles(ownFiles);
      setFiles(sorted);
    }
  }, [ownFiles, sortFiles]);

  // Save workspace-level session on unmount so tab switching restores to this workspace
  useEffect(() => {
    return () => {
      if (workspaceId) {
        saveChatSession({ workspaceId });
      }
    };
  }, [workspaceId]);

  // Infinite scroll: observe the sentinel, pull the next page while it's in
  // view. Re-runs after each page so a still-visible sentinel keeps filling a
  // tall container; React Query dedupes concurrent fetches for the same page.
  useEffect(() => {
    const sentinel = loadMoreSentinelRef.current;
    const root = scrollContainerRef.current;
    if (!sentinel || !root || !hasNextPage || isFetchingNextPage) return;
    const observer = new IntersectionObserver(
      (entries) => { if (entries[0]?.isIntersecting) void fetchNextPage(); },
      { root, rootMargin: '300px' },
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [hasNextPage, isFetchingNextPage, fetchNextPage, threads.length]);

  // Card callbacks are useCallback-stable: ThreadCard is memoized, so a fresh
  // closure per render would defeat the memo for the whole list.
  const handleThreadClick = useCallback((thread: Record<string, unknown>) => {
    if (onThreadSelect) {
      onThreadSelect(workspaceId, thread.thread_id as string);
    }
  }, [onThreadSelect, workspaceId]);

  /**
   * Handles delete icon click - opens confirmation modal
   */
  const { requestDelete, dialog: deleteConfirmDialog } = useThreadDeleteConfirm();
  const handleDeleteClick = useCallback((thread: Record<string, unknown>) => {
    const { thread_id: threadId, title, thread_index: index } = thread as ThreadRecord;
    if (!threadId) return;
    requestDelete(title || `Thread ${index !== undefined ? index + 1 : ''}`, async () => {
      await deleteThreadRow(queryClient, workspaceId, threadId);
      // If the deleted thread is currently active, navigate back to thread gallery
      if (currentThreadId === threadId) {
        navigate(isValidUuid(workspaceId) ? `/chat/${workspaceId}` : '/chat');
      }
    });
  }, [requestDelete, queryClient, workspaceId, currentThreadId, navigate]);

  /**
   * Handles rename icon click - opens rename modal
   */
  const handleRenameClick = useCallback((thread: Record<string, unknown>) => {
    setRenameModal({ isOpen: true, thread: thread as ThreadRecord });
    setRenameError(null);
  }, []);

  // Archive (from the active view) or unarchive (from the archived view).
  // Either way the row leaves the CURRENT list. Optimistic so the card's exit
  // animation IS the feedback; the prefix invalidation then refreshes both
  // views plus the sidebar's page caches, and a failure puts the row back.
  const handleArchiveToggle = useCallback(async (thread: Record<string, unknown>, archived: boolean) => {
    const threadId = thread.thread_id as string | undefined;
    if (!threadId) return;
    const snapshot = patchThreadRows(queryClient, queryKeys.threads.byWorkspace(workspaceId), (rows) =>
      rows.some((th) => th.thread_id === threadId) ? rows.filter((th) => th.thread_id !== threadId) : rows,
    );
    try {
      await updateThread(threadId, { archived });
      queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) });
    } catch (err) {
      rollbackThreadRows(queryClient, snapshot);
      console.error('Error updating thread archive state:', err);
    }
  }, [workspaceId, queryClient]);

  // Stable per-direction wrappers — inline `(th) => handleArchiveToggle(...)`
  // closures at the call site would break the ThreadCard memo every render.
  // Archiving goes through the shared confirm (live runs ask first); unarchive
  // is never gated.
  const { requestArchive, dialog: archiveConfirmDialog } = useArchiveThreadConfirm();
  const handleArchive = useCallback(
    (thread: Record<string, unknown>) => {
      const threadId = thread.thread_id as string | undefined;
      if (!threadId) return;
      requestArchive(threadId, () => { void handleArchiveToggle(thread, true); });
    },
    [requestArchive, handleArchiveToggle],
  );
  const handleUnarchive = useCallback(
    (thread: Record<string, unknown>) => handleArchiveToggle(thread, false),
    [handleArchiveToggle],
  );

  /**
   * Handles confirmed thread rename
   */
  const handleConfirmRename = async (newTitle: string) => {
    if (!renameModal.thread) return;

    const threadToRename = renameModal.thread;
    const threadId = threadToRename.thread_id;

    if (!threadId) {
      console.error('No thread ID found in thread object:', threadToRename);
      setRenameError(t('thread.invalidThread'));
      return;
    }

    setIsRenaming(true);
    setRenameError(null);

    try {
      const updatedThread = await updateThreadTitle(threadId, newTitle) as ThreadRecord;

      // Same writer the lifecycle feed uses for generated titles — every cached
      // list holding the row, plus its detail entry. The response's updated_at
      // versions the patch so a slower generated-title event can't undo it.
      patchThreadTitle(
        queryClient,
        threadId,
        (updatedThread.title as string | undefined) ?? newTitle,
        updatedThread.updated_at as string | undefined,
      );
      queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) });

      // Close modal
      setRenameModal({ isOpen: false, thread: null });
    } catch (err: any) { // TODO: type properly
      console.error('Error renaming thread:', err);
      const errorMessage = err.response?.data?.detail || err.message || t('thread.failedRenameThread');
      setRenameError(errorMessage);
      // Keep modal open so user can see the error
    } finally {
      setIsRenaming(false);
    }
  };

  /**
   * Handles canceling rename
   */
  const handleCancelRename = () => {
    setRenameModal({ isOpen: false, thread: null });
    setRenameError(null);
  };

  /**
   * Handles sending a message from ChatInput
   * Creates a new thread and navigates to it with the message
   */
  const handleSendMessage = async (
    message: string,
    attachments: Array<{ file: File; type: string; preview: string | null; dataUrl: string | null }> = [],
    slashCommands: Array<{ type: string; skillName?: string; name?: string }> = [],
    {
      model, reasoningEffort, fastMode, subagentsAllowed, reasoningEffortOverride, fastModeOverride,
    }: {
      model?: string | null;
      reasoningEffort?: string | null;
      fastMode?: boolean | null;
      subagentsAllowed?: boolean;
      reasoningEffortOverride?: string;
      fastModeOverride?: boolean;
    } = {},
  ) => {
    if ((!message.trim() && (!attachments || attachments.length === 0)) || isSendingMessage || !workspaceId) {
      return;
    }

    setIsSendingMessage(true);
    try {
      const contexts: Array<Record<string, unknown>> = [];
      let attachmentMeta: Array<Record<string, unknown>> | null = null;
      if (attachments && attachments.length > 0) {
        contexts.push(...attachmentsToContexts(attachments as any) as unknown as Array<Record<string, unknown>>); // TODO: type properly — attachment shapes differ
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

      navigate(`/chat/t/__default__`, {
        state: {
          workspaceId,
          initialMessage: message.trim(),
          ...(subagentsAllowed !== undefined ? { subagentsAllowed } : {}),
          ...(additionalContext ? { additionalContext } : {}),
          ...(attachmentMeta ? { attachmentMeta } : {}),
          ...(model ? { model } : {}),
          ...(reasoningEffort ? { reasoningEffort } : {}),
          ...(typeof fastMode === 'boolean' ? { fastMode } : {}),
          ...(reasoningEffortOverride !== undefined ? { reasoningEffortOverride } : {}),
          ...(fastModeOverride !== undefined ? { fastModeOverride } : {}),
        },
      });
    } catch (error) {
      console.error('Error navigating to thread:', error);
    } finally {
      setIsSendingMessage(false);
    }
  };

  /**
   * Toggle file panel visibility
   */
  const handleToggleFilePanel = useCallback(() => {
    if (showFilePanel) {
      setShowFilePanel(false);
    } else {
      // The card is the workspace's own files; a sibling shown before the
      // panel closed was reached through a link.
      setPanelOverride(null);
      setFilePanelWidth(clampPanelWidth(850));
      setShowFilePanel(true);
    }
  }, [showFilePanel, clampPanelWidth, setPanelOverride]);

  /**
   * Handle drag panel width
   */
  const handleDividerMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDraggingRef.current = true;
    setIsDragging(true);
    const startX = e.clientX;
    const startWidth = filePanelWidth;

    const onMouseMove = (moveEvent: MouseEvent) => {
      if (!isDraggingRef.current) return;
      const delta = startX - moveEvent.clientX;
      const containerW = containerWidthRef.current > 0 ? containerWidthRef.current : window.innerWidth;
      setFilePanelWidth(clampPanelWidthUtil(startWidth + delta, containerW));
    };

    // Hoisted declarations: teardown and onMouseUp reference each other.
    function teardown() {
      dragCleanupRef.current = null;
      isDraggingRef.current = false;
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    }

    function onMouseUp() {
      setIsDragging(false);
      teardown();
    }

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
    dragCleanupRef.current = teardown;
  }, [filePanelWidth]);

  if (error) {
    return (
      <div className="h-full flex flex-col">
        <div className="chrome-drag-strip" aria-hidden="true" />
        <div className="flex-1 min-h-0 flex items-center justify-center">
          <div className="flex flex-col items-center gap-4 max-w-md text-center px-4">
            <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
              {error}
            </p>
            <button
              onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) })}
              className="px-4 py-2 rounded-md text-sm font-medium transition-opacity hover:opacity-90"
              style={{
                backgroundColor: 'var(--color-btn-primary-bg)',
                color: 'var(--color-btn-primary-text)',
              }}
            >
              {t('common.retry')}
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className="h-full flex overflow-hidden"
      style={{
        position: 'relative',
        backgroundColor: 'var(--color-bg-page)',
      }}
    >
      {/* Main Content Area. Inert and isolated under an expanded panel, as
          the chat is in ChatView. */}
      <div className={cn('flex-1 flex flex-col overflow-hidden', panelExpanded && 'isolate')} inert={panelExpanded}>
        {/* Back Button - Fixed at top left. Doubles as this route's drag region
            in the desktop shell: it is the top bar the content column already
            has, so it costs no layout, and the back button wins its own clicks
            back through the `no-drag` list in chrome.css. */}
        <div className="shrink-0 px-6 py-4" data-chrome="drag" style={{ paddingTop: 'calc(var(--page-top) - 4px)' }}>
          <button
            onClick={onBack}
            className="p-2 rounded-md transition-colors"
            style={{ color: 'var(--color-text-primary)' }}
            title={t('thread.backToWorkspaces')}
            onMouseEnter={!isMobile ? (e) => { e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'; } : undefined}
            onMouseLeave={!isMobile ? (e) => { e.currentTarget.style.backgroundColor = ''; } : undefined}
          >
            <ArrowLeft className="h-5 w-5" />
          </button>
        </div>

        {/* Main Content - Centered with max width */}
        <div ref={scrollContainerRef} className="flex-1 flex flex-col min-h-0 w-full px-4 overflow-auto">
          <div className="w-full max-w-[768px] mx-auto flex flex-col gap-8">

            {/* Workspace Header */}
            <div className="w-full flex flex-col items-center mt-2 md:mt-[8vh]">
              {/* Which icon, and whether it opens machine settings, waits on
                  the workspace row: until then Home reads as any workspace,
                  whose settings it does not have. A row that failed to load
                  (a 403 or 404 redirects instead) draws the plain icon, inert,
                  rather than loading for good. */}
              <div
                className={cn('flex items-center justify-center transition-colors', wsData && !isHome && 'cursor-pointer')}
                onClick={wsData && !isHome ? () => setShowSandboxPanel(true) : undefined}
              >
                {!wsData && !wsError ? (
                  <SkeletonBar className="w-10 h-10 rounded-lg" />
                ) : isHome ? (
                  <HomeRowIcon className="w-10 h-10" style={{ color: 'var(--color-accent-primary)' }} />
                ) : (
                  <img src={iconComputer} alt="Workspace" className="w-10 h-10" />
                )}
              </div>
              <h1
                className="text-xl font-medium mt-3 text-center title-font"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {workspaceName || (wsError ? '\u00a0' : <SkeletonBar className="w-40" />)}
              </h1>
              <div className="flex items-center gap-2 mt-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                <span>{t('thread.workspace')}</span>
                <div className="size-[3px] rounded-full bg-current opacity-50"></div>
                {isLoading ? (
                  <SkeletonBar className="w-14" />
                ) : (
                  <span>{totalThreads ?? threads.length} {(totalThreads ?? threads.length) === 1 ? t('thread.thread') : t('thread.threads')}</span>
                )}
              </div>
            </div>

            {/* Chat Input */}
            <div className="w-full relative z-20">
              <ChatInput
                ref={chatInputRef}
                onSend={handleSendMessage as any} // TODO: type properly — ChatInput expects strict ReadyAttachment[]
                disabled={isSendingMessage || !workspaceId}
                files={ownFiles}
                dropdownDirection="down"
                // The turn this composer sends lands in this workspace, so the
                // slash menu has to be scoped to it too: without this it lists
                // the account-level skills, hiding the workspace's own and
                // still offering ones it has disabled.
                selectedWorkspaceId={workspaceId}
                minRows={2}
              />
            </div>

            {/* Files Card */}
            <div className="w-full">
              <div
                className="flex-1 min-w-0 flex flex-col ps-[16px] pt-[12px] pb-[14px] pe-[20px] rounded-[12px] border cursor-pointer hover:bg-foreground/5 transition-colors"
                style={{
                  borderColor: 'var(--color-bg-card-border, var(--color-border-muted))',
                  backgroundColor: 'var(--color-bg-card-gradient, var(--color-border-muted))',
                  backdropFilter: 'blur(8px)',
                  WebkitBackdropFilter: 'blur(8px)',
                }}
                onClick={handleToggleFilePanel}
              >
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2.5">
                    <Folder className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
                    <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('workspace.files')}</span>
                  </div>
                  <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                    {showFilePanel ? t('common.close') : t('thread.viewAll')}
                  </div>
                </div>
                {/* Show first two user file names -- system dirs (.agents/, code/, etc.) are excluded */}
                {files.length > 0 && (
                  <div className="flex flex-col gap-0.5">
                    {files.filter((fp) => {
                      const top = fp.split('/')[0];
                      return !SYSTEM_DIR_PREFIXES.includes(top);
                    }).slice(0, 2).map((filePath, index) => {
                      const fileName = filePath.split('/').pop();
                      return (
                        <div
                          key={index}
                          className="flex items-center gap-2 text-[0.8125rem] rounded-md px-1 py-1 -mx-1 transition-colors hover:bg-foreground/5"
                          style={{ color: 'var(--color-text-tertiary)' }}
                          onClick={(e) => {
                            e.stopPropagation();
                            // The card lists the workspace's own files, so a
                            // sibling's on show gives way to them.
                            setPanelOverride(null);
                            askPanel({ kind: 'file', path: filePath });
                            setFilePanelWidth(clampPanelWidth(850));
                            setShowFilePanel(true);
                          }}
                        >
                          <FileText className="h-3.5 w-3.5 shrink-0" />
                          <span className="truncate">{fileName}</span>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </div>

            {/* Threads Section */}
            <div className="w-full flex flex-col gap-4 pb-8">
              <div className="flex items-center justify-between">
                <h2 className="text-base font-medium" style={{ color: 'var(--color-text-primary)' }}>
                  {showArchived ? t('thread.archivedTasks') : t('thread.tasks')}
                </h2>
                <button
                  type="button"
                  onClick={() => {
                    setShowArchived((v) => !v);
                    setSwitchedIn(workspaceId);
                  }}
                  className="flex items-center gap-1.5 text-xs px-2 py-1 rounded-md transition-colors hover:bg-foreground/5"
                  style={{ color: showArchived ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)' }}
                  aria-pressed={showArchived}
                >
                  <Archive className="h-3.5 w-3.5" />
                  {t('thread.archived')}
                </button>
              </div>

              {/* The rows take the placeholders' places in one crossfade. */}
              <SwapCell as="div" swapKey={isLoading ? 'loading' : 'list'}>
                {isLoading ? (
                  <div className="flex flex-col" aria-busy="true">
                    <span className="sr-only">{t('thread.loadingThreads')}</span>
                    {Array.from({ length: THREAD_SKELETON_ROWS }, (_, i) => <ThreadCardSkeleton key={i} index={i} />)}
                  </div>
                ) : threads.length === 0 ? (
                  // Empty state
                  <div className="flex flex-col items-center justify-center py-12">
                    <p className="text-sm mb-2" style={{ color: 'var(--color-text-tertiary)' }}>
                      {showArchived ? t('thread.noArchivedThreads') : t('thread.noThreadsYet')}
                    </p>
                    {!showArchived && (
                      <p className="text-xs text-center max-w-md" style={{ color: 'var(--color-text-tertiary)' }}>
                        {t('thread.startConversation')}
                      </p>
                    )}
                  </div>
                ) : (
                  // Thread list. The container is keyed per view (workspace +
                  // archived flag) so a view the reader switches to remounts it
                  // with a single fade in — a fresh inner Presence with
                  // initial={false} keeps the bulk swap from playing 20
                  // concurrent card animations. It dims while showing placeholder (outgoing)
                  // rows during the archived-toggle fetch. Per-card fold +
                  // layout animate the single-card case: archive/unarchive
                  // folds the card shut while neighbors glide up. Card spacing
                  // lives INSIDE the fold (pb-2, not container gap) so the
                  // collapse swallows the gap too.
                  <motion.div
                    key={`${workspaceId}:${showArchived}`}
                    className="flex flex-col"
                    initial={viewSwitched ? { opacity: 0 } : false}
                    animate={{ opacity: isPlaceholderData ? 0.45 : 1 }}
                    transition={TWEEN.quick}
                  >
                    <AnimatePresence initial={false}>
                      {threads.map((thread) => (
                        <motion.div
                          key={thread.thread_id}
                          layout="position"
                          layoutDependency={threadOrderSignature}
                          transition={{ layout: TWEEN.fold }}
                        >
                          <PresenceFold className="pb-2">
                            {/* Archive affordances derive from the ROW, not the
                                view toggle: while isPlaceholderData shows the
                                previous view's rows the toggle has already
                                flipped, so the view flag would offer Archive on
                                an archived row. */}
                            <ThreadCard
                              thread={thread}
                              onClick={handleThreadClick}
                              onDelete={handleDeleteClick}
                              onRename={handleRenameClick}
                              onArchive={!thread.archived_at ? handleArchive : undefined}
                              onUnarchive={thread.archived_at ? handleUnarchive : undefined}
                            />
                          </PresenceFold>
                        </motion.div>
                      ))}
                    </AnimatePresence>
                    {/* Infinite-scroll sentinel + its spinner */}
                    {hasNextPage && (
                      <div ref={loadMoreSentinelRef} className="flex items-center justify-center py-4">
                        {isFetchingNextPage && (
                          <Loader size={20} className="text-(--color-accent-primary)" />
                        )}
                      </div>
                    )}
                  </motion.div>
                )}
              </SwapCell>
            </div>
          </div>
        </div>
      </div>

      {/* Right Side: File Panel */}
      <AnimatePresence>
        {showFilePanel && (
          <motion.div
            initial={isMobile ? { x: '100%' } : { width: 0, opacity: 0 }}
            animate={isMobile ? { x: 0 } : { width: filePanelWidth + DIVIDER_WIDTH, opacity: 1 }}
            exit={isMobile ? { x: '100%' } : { width: 0, opacity: 0 }}
            transition={{ duration: isDragging ? 0 : DURATION.fold, ease: EASE_OUT }}
            className={isMobile ? 'flex overflow-hidden' : 'flex shrink-0 overflow-hidden'}
            style={isMobile ? { position: 'absolute', inset: 0, zIndex: 30 } : undefined}
          >
            {!isMobile && !panelExpanded && (
              <div
                className="w-[4px] bg-transparent hover:bg-foreground/20 cursor-col-resize shrink-0 transition-colors"
                onMouseDown={handleDividerMouseDown}
              />
            )}
            {/* Expanded, the panel lies over the gallery while the wrapper
                keeps its width in the row, so the panel stays the same
                element and a running app's iframe does not reload. */}
            <div
              className={panelExpanded ? 'absolute inset-0 z-10' : 'shrink-0'}
              style={panelExpanded ? undefined : { width: isMobile ? '100%' : filePanelWidth }}
            >
              <WorkspaceProvider workspaceId={shownWorkspaceId} downloadFile={null} folders={panelFolders}>
              <FilePanel
                workspaceId={shownWorkspaceId ?? workspaceId}
                onClose={() => setShowFilePanel(false)}
                expanded={panelExpanded}
                onToggleExpanded={isMobile ? null : handleTogglePanelExpanded}
                onLeaveGuardChange={handleFilesLeaveGuardChange}
                target={panelTarget}
                onTargetHandled={handleTargetHandled}
                onTargetMemoryHandled={handleTargetMemoryHandled}
                onTargetMemoHandled={handleTargetMemoHandled}
                onOpenFile={handleOpenFile}
                persistTabs={false}
                files={panelFiles}
                filesLoading={panelFilesLoading}
                filesError={panelFilesError}
                onRefreshFiles={refreshPanelFiles}
                onAddContext={handleAddContext as any} // TODO: type properly
                showSystemFiles={showSystemFiles}
                onToggleSystemFiles={() => {
                  setShowSystemFiles((v) => {
                    localStorage.setItem('filePanel.showSystemFiles', String(!v));
                    return !v;
                  });
                }}
                readOnly={panelAccess.readOnly}
                singleFileMode={panelAccess.singleFileMode}
                onReturnHome={panelAccess.onReturnHome}
                returnLabel={panelAccess.returnLabel}
              />
              </WorkspaceProvider>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {deleteConfirmDialog}

      {/* Archive-while-running confirmation (opens only for a live thread) */}
      {archiveConfirmDialog}

      {/* Rename Thread Modal */}
      <RenameThreadModal
        isOpen={renameModal.isOpen}
        currentTitle={renameModal.thread?.title || ''}
        onConfirm={handleConfirmRename}
        onCancel={handleCancelRename}
        isRenaming={isRenaming}
        error={renameError}
      />

      {/* Sandbox Settings Panel */}
      {showSandboxPanel && (
        <SandboxSettingsPanel
          onClose={() => setShowSandboxPanel(false)}
          workspaceId={workspaceId}
        />
      )}
    </div>
  );
}

export default ThreadGallery;
