import React, { useState, useEffect, useRef, useCallback } from 'react';
import { ArrowLeft, Loader2, Folder, FileText, Zap } from 'lucide-react';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useNavigate, useParams, useLocation } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../../../lib/queryKeys';
import { useWorkspace } from '../../../hooks/useWorkspace';
import ThreadCard from './ThreadCard';
import DeleteConfirmModal from './DeleteConfirmModal';
import RenameThreadModal from './RenameThreadModal';
import ChatInput from '../../../components/ui/chat-input';
import type { ChatInputHandle } from '../../../components/ui/chat-input';
import { attachmentsToContexts } from '../utils/fileUpload';
import { SYSTEM_DIR_PREFIXES } from './FilePanel';
import RightPanel from './RightPanel';
import { clampPanelWidth as clampPanelWidthUtil } from '@/lib/panelUtils';
import SandboxSettingsPanel from './SandboxSettingsPanel';
import { getWorkspaceThreads, deleteThread, updateThreadTitle } from '../utils/api';
import { useWorkspaceFiles } from '../hooks/useWorkspaceFiles';
import { removeStoredThreadId } from '../hooks/utils/threadStorage';
import { saveChatSession } from '../hooks/utils/chatSessionRestore';
import iconComputerLight from '../../../assets/img/icon-computer.svg';
import iconComputerDark from '../../../assets/img/icon-computer-dark.svg';
import { useTheme } from '../../../contexts/ThemeContext';
import { motion, AnimatePresence } from 'framer-motion';

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

interface ThreadsResponse {
  threads: ThreadRecord[];
  total: number;
}

interface DeleteModalState {
  isOpen: boolean;
  thread: ThreadRecord | null;
}

interface RenameModalState {
  isOpen: boolean;
  thread: ThreadRecord | null;
}

interface ThreadGalleryProps {
  workspaceId: string;
  onBack: () => void;
  onThreadSelect: (workspaceId: string, threadId: string, agentMode?: string | null) => void;
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
  const [threads, setThreads] = useState<ThreadRecord[]>([]);

  // Workspace detail via React Query (useWorkspace)
  const { data: wsData, error: wsError } = useWorkspace(workspaceId);
  // Keep location.state values as instant display fallbacks during navigation
  const locationState = location.state as Record<string, unknown> | null;
  const workspaceName = (wsData?.name || locationState?.workspaceName || '') as string;
  const workspaceStatus = (wsData?.status || locationState?.workspaceStatus || null) as string | null;
  const isFlash = workspaceStatus === 'flash';

  // Thread loading via React Query
  const { data: threadData, isLoading: isThreadsLoading, error: threadError } = useQuery({
    queryKey: queryKeys.threads.byWorkspace(workspaceId),
    queryFn: () => getWorkspaceThreads(workspaceId),
    enabled: !!workspaceId,
    staleTime: 30_000,
    retry: (failureCount, error) => {
      // Don't retry 403/404 — access denied or workspace not found won't resolve on retry
      const status = (error as { response?: { status?: number } })?.response?.status;
      if (status === 403 || status === 404) return false;
      return failureCount < 3;
    },
  });

  // Detect 403 or 404 from either workspace or thread queries
  const accessDenied =
    (threadError as { response?: { status?: number } } | null)?.response?.status === 403 ||
    (wsError as { response?: { status?: number } } | null)?.response?.status === 403;
  const wsNotFound =
    (threadError as { response?: { status?: number } } | null)?.response?.status === 404 ||
    (wsError as { response?: { status?: number } } | null)?.response?.status === 404;

  const isLoading = isThreadsLoading;
  const error = threadError && !accessDenied && !wsNotFound ? t('thread.failedLoadThreads') : null;
  const [deleteModal, setDeleteModal] = useState<DeleteModalState>({ isOpen: false, thread: null });
  const [isDeleting, setIsDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [renameModal, setRenameModal] = useState<RenameModalState>({ isOpen: false, thread: null });
  const [isRenaming, setIsRenaming] = useState(false);
  const [renameError, setRenameError] = useState<string | null>(null);
  const [isSendingMessage, setIsSendingMessage] = useState(false);
  const [showFilePanel, setShowFilePanel] = useState(false);
  const [showSandboxPanel, setShowSandboxPanel] = useState(false);
  const [filePanelWidth, setFilePanelWidth] = useState(850);
  const [filePanelTargetFile, setFilePanelTargetFile] = useState<string | null>(null);
  // Show system files in FilePanel (.agents/, code/, tools/, etc.)
  const [showSystemFiles, setShowSystemFiles] = useState(
    () => localStorage.getItem('filePanel.showSystemFiles') === 'true'
  );
  const [files, setFiles] = useState<string[]>([]);
  const isDraggingRef = useRef(false);
  const [isDragging, setIsDragging] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const containerWidthRef = useRef<number>(0);
  const DIVIDER_WIDTH = 4; // px -- matches w-[4px] divider
  const chatInputRef = useRef<ChatInputHandle>(null);
  const handleAddContext = useCallback((ctx: Record<string, unknown>) => {
    chatInputRef.current?.addContext(ctx as any); // TODO: type properly
  }, []);

  // Infinite scroll pagination state
  const [totalThreads, setTotalThreads] = useState<number | null>(null);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [hasMore, setHasMore] = useState(false);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  // Refs to avoid stale closures in IntersectionObserver callback
  const isLoadingMoreRef = useRef(false);
  const hasMoreRef = useRef(false);
  const threadsLengthRef = useRef(0);

  // Shared workspace files for the FilePanel (skip for flash workspaces -- no sandbox)
  const {
    files: panelFiles,
    loading: panelFilesLoading,
    error: panelFilesError,
    refresh: refreshPanelFiles,
  } = useWorkspaceFiles(isFlash ? null : workspaceId, { includeSystem: showSystemFiles });

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
    if (panelFiles.length > 0) {
      const sorted = sortFiles(panelFiles);
      setFiles(sorted);
    }
  }, [panelFiles, sortFiles]);

  // Save workspace-level session on unmount so tab switching restores to this workspace
  useEffect(() => {
    return () => {
      if (workspaceId) {
        saveChatSession({ workspaceId });
      }
    };
  }, [workspaceId]);

  // Sync threads state from React Query data
  useEffect(() => {
    if (threadData) {
      const data = threadData as ThreadsResponse;
      setThreads(data.threads || []);
      setTotalThreads(data.total || 0);
      setHasMore((data.threads?.length || 0) < (data.total || 0));
    }
  }, [threadData]);

  // Keep refs in sync with state for IntersectionObserver callback
  useEffect(() => { isLoadingMoreRef.current = isLoadingMore; }, [isLoadingMore]);
  useEffect(() => { hasMoreRef.current = hasMore; }, [hasMore]);
  useEffect(() => { threadsLengthRef.current = threads.length; }, [threads.length]);

  /**
   * Load more threads for infinite scroll.
   * Uses refs to avoid stale closures -- safe to call from IntersectionObserver.
   */
  const loadMoreThreads = useCallback(async () => {
    if (isLoadingMoreRef.current || !hasMoreRef.current) return;
    isLoadingMoreRef.current = true;
    setIsLoadingMore(true);
    try {
      const offset = threadsLengthRef.current;
      const moreData = await getWorkspaceThreads(workspaceId, 20, offset) as ThreadsResponse;
      const moreThreads = moreData.threads || [];
      const updatedTotal = moreData.total ?? 0;
      setThreads((prev) => [...prev, ...moreThreads]);
      setTotalThreads(updatedTotal);
      const newHasMore = offset + moreThreads.length < updatedTotal;
      setHasMore(newHasMore);
    } catch (err) {
      console.error('Error loading more threads:', err);
    } finally {
      isLoadingMoreRef.current = false;
      setIsLoadingMore(false);
    }
  }, [workspaceId]);

  // Scroll-based infinite loading: trigger when near bottom of scroll container
  useEffect(() => {
    const el = scrollContainerRef.current;
    if (!el) return;
    const onScroll = () => {
      if (isLoadingMoreRef.current || !hasMoreRef.current) return;
      const { scrollTop, scrollHeight, clientHeight } = el;
      if (scrollHeight - scrollTop - clientHeight < 300) {
        loadMoreThreads();
      }
    };
    el.addEventListener('scroll', onScroll, { passive: true });
    return () => el.removeEventListener('scroll', onScroll);
  }, [loadMoreThreads]);

  // Auto-fill: if content doesn't overflow the container, keep loading until it does
  useEffect(() => {
    const el = scrollContainerRef.current;
    if (!el || !hasMore || isLoadingMore) return;
    // Use rAF to ensure layout is computed after render
    const raf = requestAnimationFrame(() => {
      if (el.scrollHeight <= el.clientHeight) {
        loadMoreThreads();
      }
    });
    return () => cancelAnimationFrame(raf);
  }, [hasMore, isLoadingMore, threads.length, loadMoreThreads]);

  /**
   * Handles thread selection
   */
  const handleThreadClick = (thread: ThreadRecord) => {
    if (onThreadSelect) {
      onThreadSelect(workspaceId, thread.thread_id, isFlash ? 'flash' : null);
    }
  };

  /**
   * Handles delete icon click - opens confirmation modal
   */
  const handleDeleteClick = (thread: Record<string, unknown>) => {
    setDeleteModal({ isOpen: true, thread: thread as ThreadRecord });
    setDeleteError(null);
  };

  /**
   * Handles confirmed thread deletion
   */
  const handleConfirmDelete = async () => {
    if (!deleteModal.thread) return;

    const threadToDelete = deleteModal.thread;
    const threadId = threadToDelete.thread_id;

    if (!threadId) {
      console.error('No thread ID found in thread object:', threadToDelete);
      setDeleteError(t('thread.invalidThread'));
      return;
    }

    setIsDeleting(true);
    setDeleteError(null);

    try {
      await deleteThread(threadId);

      // Clean up localStorage: remove thread ID for deleted thread
      if (workspaceId) {
        // Check if the deleted thread is the currently stored thread for this workspace
        const storedThreadId = localStorage.getItem(`workspace_thread_id_${workspaceId}`);
        if (storedThreadId === threadId) {
          removeStoredThreadId(workspaceId);
        }
      }

      // Remove thread from list and adjust total
      setThreads((prev) =>
        prev.filter((t) => t.thread_id !== threadId)
      );
      setTotalThreads((prev) => (prev != null ? prev - 1 : prev));

      // Invalidate thread query cache
      queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) });

      // If the deleted thread is currently active, navigate back to thread gallery
      if (currentThreadId === threadId) {
        navigate(`/chat/${workspaceId}`);
      }

      // Close modal
      setDeleteModal({ isOpen: false, thread: null });
    } catch (err: any) { // TODO: type properly
      console.error('Error deleting thread:', err);
      const errorMessage = err.response?.data?.detail || err.message || t('thread.failedDeleteThread');
      setDeleteError(errorMessage);
      // Keep modal open so user can see the error
    } finally {
      setIsDeleting(false);
    }
  };

  /**
   * Handles canceling deletion
   */
  const handleCancelDelete = () => {
    setDeleteModal({ isOpen: false, thread: null });
    setDeleteError(null);
  };

  /**
   * Handles rename icon click - opens rename modal
   */
  const handleRenameClick = (thread: Record<string, unknown>) => {
    setRenameModal({ isOpen: true, thread: thread as ThreadRecord });
    setRenameError(null);
  };

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

      // Update thread in list
      setThreads((prev) =>
        prev.map((t) =>
          t.thread_id === threadId
            ? { ...t, title: updatedThread.title, updated_at: updatedThread.updated_at }
            : t
        )
      );

      // Invalidate thread query cache
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
    planMode = false,
    attachments: Array<{ file: File; type: string; preview: string | null; dataUrl: string | null }> = [],
    slashCommands: Array<{ type: string; skillName?: string; name?: string }> = [],
    { model, reasoningEffort }: { model?: string; reasoningEffort?: string } = {},
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
          planMode: planMode,
          ...(isFlash ? { agentMode: 'flash' } : {}),
          ...(additionalContext ? { additionalContext } : {}),
          ...(attachmentMeta ? { attachmentMeta } : {}),
          ...(model ? { model } : {}),
          ...(reasoningEffort ? { reasoningEffort } : {}),
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
      setFilePanelWidth(clampPanelWidth(850));
      setShowFilePanel(true);
    }
  }, [showFilePanel, clampPanelWidth]);

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

    const onMouseUp = () => {
      isDraggingRef.current = false;
      setIsDragging(false);
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, [filePanelWidth]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="h-8 w-8 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
          <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('thread.loadingThreads')}
          </p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4 max-w-md text-center px-4">
          <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
            {error}
          </p>
          <button
            onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(workspaceId) })}
            className="px-4 py-2 rounded-md text-sm font-medium transition-colors"
            style={{
              backgroundColor: 'var(--color-accent-primary)',
              color: 'var(--color-text-on-accent)',
            }}
          >
            {t('common.retry')}
          </button>
        </div>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className={`${isMobile ? 'h-full' : 'h-screen'} flex overflow-hidden`}
      style={{
        position: 'relative',
        backgroundColor: 'var(--color-bg-page)',
        backgroundImage: 'radial-gradient(circle at center, var(--color-dot-grid) 0.75px, transparent 0.75px)',
        backgroundSize: '18px 18px',
        backgroundPosition: '0 0'
      }}
    >
      {/* Main Content Area */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Back Button - Fixed at top left */}
        <div className="flex-shrink-0 px-6 py-4 enter-fade-up">
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
            <div className="w-full flex flex-col items-center mt-2 md:mt-[8vh] enter-fade-up enter-fade-up-d1">
              <div
                className="flex items-center justify-center transition-colors cursor-pointer"
                onClick={!isFlash ? () => setShowSandboxPanel(true) : undefined}
              >
                {isFlash ? (
                  <Zap className="w-10 h-10" style={{ color: 'var(--color-accent-primary)' }} />
                ) : (
                  <img src={iconComputer} alt="Workspace" className="w-10 h-10" />
                )}
              </div>
              <h1
                className="text-xl font-medium mt-3 text-center title-font"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {workspaceName}
              </h1>
              <div className="flex items-center gap-2 mt-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                <span>{t('thread.workspace')}</span>
                <div className="size-[3px] rounded-full bg-current opacity-50"></div>
                <span>{totalThreads ?? threads.length} {(totalThreads ?? threads.length) === 1 ? t('thread.thread') : t('thread.threads')}</span>
              </div>
            </div>

            {/* Chat Input */}
            <div className="w-full enter-fade-up enter-fade-up-d2 relative z-20">
              <ChatInput
                ref={chatInputRef}
                onSend={handleSendMessage as any} // TODO: type properly — ChatInput expects strict ReadyAttachment[]
                disabled={isSendingMessage || !workspaceId}
                files={panelFiles}
                dropdownDirection="down"
                mode={isFlash ? 'fast' : 'deep'}
              />
            </div>

            {/* Files Card -- hidden for flash workspaces (no sandbox) */}
            {!isFlash && <div className="w-full enter-fade-up enter-fade-up-d3">
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
                          className="flex items-center gap-2 text-[13px] rounded-md px-1 py-1 -mx-1 transition-colors hover:bg-foreground/5"
                          style={{ color: 'var(--color-text-tertiary)' }}
                          onClick={(e) => {
                            e.stopPropagation();
                            setFilePanelTargetFile(filePath);
                            setFilePanelWidth(clampPanelWidth(850));
                            setShowFilePanel(true);
                          }}
                        >
                          <FileText className="h-3.5 w-3.5 flex-shrink-0" />
                          <span className="truncate">{fileName}</span>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </div>}

            {/* Threads Section */}
            <div className="w-full flex flex-col gap-4 pb-8 enter-fade-up enter-fade-up-d4">
              <div className="flex items-center justify-between">
                <h2 className="text-base font-medium" style={{ color: 'var(--color-text-primary)' }}>
                  {t('thread.tasks')}
                </h2>
              </div>

              {threads.length === 0 ? (
                // Empty state
                <div className="flex flex-col items-center justify-center py-12">
                  <p className="text-sm mb-2" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('thread.noThreadsYet')}
                  </p>
                  <p className="text-xs text-center max-w-md" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('thread.startConversation')}
                  </p>
                </div>
              ) : (
                // Thread list
                <div className="flex flex-col gap-2">
                  {threads.map((thread) => (
                    <ThreadCard
                      key={thread.thread_id}
                      thread={thread}
                      onClick={() => handleThreadClick(thread)}
                      onDelete={handleDeleteClick}
                      onRename={handleRenameClick}
                    />
                  ))}
                  {/* Loading spinner for infinite scroll */}
                  {isLoadingMore && (
                    <div className="flex items-center justify-center py-4">
                      <Loader2 className="h-5 w-5 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Right Side: File Panel -- hidden for flash workspaces */}
      <AnimatePresence>
        {showFilePanel && !isFlash && (
          <motion.div
            initial={isMobile ? { x: '100%' } : { width: 0, opacity: 0 }}
            animate={isMobile ? { x: 0 } : { width: filePanelWidth + DIVIDER_WIDTH, opacity: 1 }}
            exit={isMobile ? { x: '100%' } : { width: 0, opacity: 0 }}
            transition={{ duration: isDragging ? 0 : 0.25, ease: [0.22, 1, 0.36, 1] }}
            className={isMobile ? 'flex overflow-hidden' : 'flex flex-shrink-0 overflow-hidden'}
            style={isMobile ? { position: 'absolute', inset: 0, zIndex: 30 } : undefined}
          >
            {!isMobile && (
              <div
                className="w-[4px] bg-transparent hover:bg-foreground/20 cursor-col-resize flex-shrink-0 transition-colors"
                onMouseDown={handleDividerMouseDown}
              />
            )}
            <div className="flex-shrink-0" style={{ width: isMobile ? '100%' : filePanelWidth }}>
              <RightPanel
                workspaceId={workspaceId}
                onClose={() => setShowFilePanel(false)}
                targetFile={filePanelTargetFile}
                onTargetFileHandled={() => setFilePanelTargetFile(null)}
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
              />
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Delete Confirmation Modal */}
      <DeleteConfirmModal
        isOpen={deleteModal.isOpen}
        workspaceName={deleteModal.thread?.title || `Thread ${deleteModal.thread?.thread_index !== undefined ? (deleteModal.thread.thread_index as number) + 1 : ''}`}
        onConfirm={handleConfirmDelete}
        onCancel={handleCancelDelete}
        isDeleting={isDeleting}
        error={deleteError}
        itemType="thread"
      />

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
