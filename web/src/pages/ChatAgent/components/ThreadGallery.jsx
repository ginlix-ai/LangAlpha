import React, { useState, useEffect, useRef, useCallback } from 'react';
import { ArrowLeft, Loader2, Folder, FileText, Zap } from 'lucide-react';
import { useNavigate, useParams, useLocation } from 'react-router-dom';
import ThreadCard from './ThreadCard';
import DeleteConfirmModal from './DeleteConfirmModal';
import RenameThreadModal from './RenameThreadModal';
import ChatInputWithMentions from './ChatInputWithMentions';
import FilePanel from './FilePanel';
import { getWorkspaceThreads, getWorkspace, deleteThread, updateThreadTitle } from '../utils/api';
import { useWorkspaceFiles } from '../hooks/useWorkspaceFiles';
import { removeStoredThreadId } from '../hooks/utils/threadStorage';
import { saveChatSession } from '../hooks/utils/chatSessionRestore';
import iconComputer from '../../../assets/img/icon-computer.svg';
import '../../Dashboard/Dashboard.css';

/**
 * ThreadGallery Component
 * 
 * Displays a gallery of threads for a specific workspace.
 * Features:
 * - Lists all threads for the workspace
 * - Shows workspace name in header
 * - Back button to return to workspace gallery
 * - Empty state when no threads exist
 * 
 * @param {string} workspaceId - The workspace ID to show threads for
 * @param {Function} onBack - Callback to navigate back to workspace gallery
 * @param {Function} onThreadSelect - Callback when a thread is selected (receives workspaceId and threadId)
 */
function ThreadGallery({ workspaceId, onBack, onThreadSelect, cache }) {
  const location = useLocation();
  const [threads, setThreads] = useState([]);
  const [workspaceName, setWorkspaceName] = useState(
    location.state?.workspaceName || ''
  );
  const [workspaceStatus, setWorkspaceStatus] = useState(
    location.state?.workspaceStatus || null
  );
  const isFlash = workspaceStatus === 'flash';
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [deleteModal, setDeleteModal] = useState({ isOpen: false, thread: null });
  const [isDeleting, setIsDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState(null);
  const [renameModal, setRenameModal] = useState({ isOpen: false, thread: null });
  const [isRenaming, setIsRenaming] = useState(false);
  const [renameError, setRenameError] = useState(null);
  const [isSendingMessage, setIsSendingMessage] = useState(false);
  const [showFilePanel, setShowFilePanel] = useState(false);
  const [filePanelWidth, setFilePanelWidth] = useState(420);
  const [filePanelTargetFile, setFilePanelTargetFile] = useState(null);
  // Initialize files from cache for instant display on return navigation
  const [files, setFiles] = useState(() => cache?.current?.[workspaceId]?.files || []);
  const isDraggingRef = useRef(false);

  // Shared workspace files for the FilePanel (skip for flash workspaces — no sandbox)
  const {
    files: panelFiles,
    loading: panelFilesLoading,
    error: panelFilesError,
    refresh: refreshPanelFiles,
  } = useWorkspaceFiles(isFlash ? null : workspaceId);

  const navigate = useNavigate();
  const { threadId: currentThreadId } = useParams();
  const loadingRef = useRef(false);

  // Sort helper for file list display
  const sortFiles = useCallback((fileList) => {
    const dirPriority = (fp) => {
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

  // Derive sorted file list from hook data and save to cache
  useEffect(() => {
    if (panelFiles.length > 0) {
      const sorted = sortFiles(panelFiles);
      setFiles(sorted);
      // Save to cache so files show instantly on return navigation
      if (cache?.current?.[workspaceId]) {
        cache.current[workspaceId].files = sorted;
      }
    }
  }, [panelFiles, sortFiles, workspaceId, cache]);

  // Save workspace-level session on unmount so tab switching restores to this workspace
  useEffect(() => {
    return () => {
      if (workspaceId) {
        saveChatSession({ workspaceId });
      }
    };
  }, [workspaceId]);

  // Load threads on mount, using cache for instant display on return navigation
  useEffect(() => {
    if (!workspaceId) return;

    // Guard: Prevent duplicate calls
    if (loadingRef.current) {
      return;
    }

    // Show cached data instantly (stale-while-revalidate)
    const cached = cache?.current?.[workspaceId];
    if (cached?.threads) {
      setThreads(cached.threads);
      if (cached.workspaceName) setWorkspaceName(cached.workspaceName);
      setIsLoading(false);
    }

    loadingRef.current = true;
    loadData().finally(() => {
      loadingRef.current = false;
    });
  }, [workspaceId]);

  /**
   * Fetches threads (and workspace name if not yet known) from the API.
   * File listing is handled separately by useWorkspaceFiles hook.
   */
  const loadData = async () => {
    try {
      // Only show spinner if we have nothing cached
      const hasCached = cache?.current?.[workspaceId]?.threads;
      if (!hasCached) setIsLoading(true);
      setError(null);

      // If we don't have workspace name yet, fetch it in parallel with threads
      const needsName = !workspaceName;
      const promises = [getWorkspaceThreads(workspaceId)];
      if (needsName) {
        promises.push(getWorkspace(workspaceId).catch(() => null));
      }

      const [threadsData, workspaceData] = await Promise.all(promises);

      const freshThreads = threadsData.threads || [];
      setThreads(freshThreads);

      const freshName = workspaceData?.name || workspaceName || 'Workspace';
      if (needsName && workspaceData?.name) {
        setWorkspaceName(freshName);
      }

      // Detect workspace status from API if not provided via navigation state
      if (!workspaceStatus && workspaceData?.status) {
        setWorkspaceStatus(workspaceData.status);
      }

      // Update cache for stale-while-revalidate on return navigation
      // Spread existing entry to preserve cached files
      if (cache?.current) {
        cache.current[workspaceId] = {
          ...cache.current[workspaceId],
          threads: freshThreads,
          workspaceName: freshName,
          fetchedAt: Date.now(),
        };
      }
    } catch (err) {
      console.error('Error loading threads:', err);
      setError('Failed to load threads. Please refresh the page.');
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * Handles thread selection
   * @param {Object} thread - The selected thread
   */
  const handleThreadClick = (thread) => {
    if (onThreadSelect) {
      onThreadSelect(workspaceId, thread.thread_id, isFlash ? 'flash' : null);
    }
  };

  /**
   * Handles delete icon click - opens confirmation modal
   * @param {Object} thread - The thread to delete
   */
  const handleDeleteClick = (thread) => {
    setDeleteModal({ isOpen: true, thread });
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
      setDeleteError('Invalid thread. Please try again.');
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

      // Remove thread from list
      setThreads((prev) =>
        prev.filter((t) => t.thread_id !== threadId)
      );

      // If the deleted thread is currently active, navigate back to thread gallery
      if (currentThreadId === threadId) {
        navigate(`/chat/${workspaceId}`);
      }

      // Close modal
      setDeleteModal({ isOpen: false, thread: null });
    } catch (err) {
      console.error('Error deleting thread:', err);
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to delete thread. Please try again.';
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
   * @param {Object} thread - The thread to rename
   */
  const handleRenameClick = (thread) => {
    setRenameModal({ isOpen: true, thread });
    setRenameError(null);
  };

  /**
   * Handles confirmed thread rename
   * @param {string} newTitle - New thread title
   */
  const handleConfirmRename = async (newTitle) => {
    if (!renameModal.thread) return;

    const threadToRename = renameModal.thread;
    const threadId = threadToRename.thread_id;

    if (!threadId) {
      console.error('No thread ID found in thread object:', threadToRename);
      setRenameError('Invalid thread. Please try again.');
      return;
    }

    setIsRenaming(true);
    setRenameError(null);

    try {
      const updatedThread = await updateThreadTitle(threadId, newTitle);

      // Update thread in list
      setThreads((prev) =>
        prev.map((t) =>
          t.thread_id === threadId
            ? { ...t, title: updatedThread.title, updated_at: updatedThread.updated_at }
            : t
        )
      );

      // Close modal
      setRenameModal({ isOpen: false, thread: null });
    } catch (err) {
      console.error('Error renaming thread:', err);
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to rename thread. Please try again.';
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
   * @param {string} message - The message to send
   * @param {boolean} planMode - Plan mode flag (not used, always false)
   */
  const handleSendMessage = async (message, planMode = false) => {
    if (!message.trim() || isSendingMessage || !workspaceId) {
      return;
    }

    setIsSendingMessage(true);
    try {
      // Navigate to ChatAgent page with workspace, new thread, and message in state
      // Use '__default__' as threadId to create a new thread
      navigate(`/chat/${workspaceId}/__default__`, {
        state: {
          initialMessage: message.trim(),
          planMode: planMode,
          ...(isFlash ? { agentMode: 'flash' } : {}),
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
    setShowFilePanel(!showFilePanel);
  }, [showFilePanel]);

  /**
   * Handle drag panel width
   */
  const handleDividerMouseDown = useCallback((e) => {
    e.preventDefault();
    isDraggingRef.current = true;
    const startX = e.clientX;
    const startWidth = filePanelWidth;

    const onMouseMove = (moveEvent) => {
      if (!isDraggingRef.current) return;
      const delta = startX - moveEvent.clientX;
      const newWidth = Math.max(280, Math.min(startWidth + delta, window.innerWidth * 0.6));
      setFilePanelWidth(newWidth);
    };

    const onMouseUp = () => {
      isDraggingRef.current = false;
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
          <Loader2 className="h-8 w-8 animate-spin" style={{ color: '#6155F5' }} />
          <p className="text-sm" style={{ color: '#FFFFFF', opacity: 0.65 }}>
            Loading threads...
          </p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4 max-w-md text-center px-4">
          <p className="text-sm" style={{ color: '#FF383C' }}>
            {error}
          </p>
          <button
            onClick={loadData}
            className="px-4 py-2 rounded-md text-sm font-medium transition-colors"
            style={{
              backgroundColor: '#6155F5',
              color: '#FFFFFF',
            }}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div
      className="h-full flex overflow-hidden"
      style={{
        backgroundColor: 'var(--color-bg-page)',
        backgroundImage: 'radial-gradient(circle at center, var(--color-dot-grid) 0.75px, transparent 0.75px)',
        backgroundSize: '18px 18px',
        backgroundPosition: '0 0'
      }}
    >
      {/* Main Content Area */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Back Button - Fixed at top left */}
        <div className="flex-shrink-0 px-6 py-4">
          <button
            onClick={onBack}
            className="p-2 rounded-md transition-colors hover:bg-white/10"
            style={{ color: '#FFFFFF' }}
            title="Back to workspaces"
          >
            <ArrowLeft className="h-5 w-5" />
          </button>
        </div>

        {/* Main Content - Centered with max width */}
        <div className="flex-1 flex flex-col min-h-0 w-full px-4 overflow-auto">
          <div className="w-full max-w-[768px] mx-auto flex flex-col gap-8">

            {/* Workspace Header */}
            <div className="w-full flex flex-col items-center mt-[8vh]">
              <div className="flex items-center justify-center transition-colors cursor-pointer">
                {isFlash ? (
                  <Zap className="w-10 h-10" style={{ color: '#6155F5' }} />
                ) : (
                  <img src={iconComputer} alt="Workspace" className="w-10 h-10" />
                )}
              </div>
              <h1
                className="text-xl font-medium mt-3 text-center dashboard-title-font"
                style={{ color: '#FFFFFF' }}
              >
                {workspaceName}
              </h1>
              <div className="flex items-center gap-2 mt-2 text-xs" style={{ color: '#FFFFFF', opacity: 0.5 }}>
                <span>Workspace</span>
                <div className="size-[3px] rounded-full bg-current opacity-50"></div>
                <span>{threads.length} {threads.length === 1 ? 'thread' : 'threads'}</span>
              </div>
            </div>

            {/* Chat Input */}
            <div className="w-full">
              <ChatInputWithMentions
                onSend={handleSendMessage}
                disabled={isSendingMessage || !workspaceId}
                files={panelFiles}
              />
            </div>

            {/* Files Card — hidden for flash workspaces (no sandbox) */}
            {!isFlash && <div className="w-full">
              <div
                className="flex-1 min-w-0 flex flex-col ps-[16px] pt-[12px] pb-[14px] pe-[20px] rounded-[12px] border cursor-pointer hover:bg-white/5 transition-colors"
                style={{
                  borderColor: 'rgba(255, 255, 255, 0.06)',
                  backgroundColor: 'rgba(255, 255, 255, 0.03)'
                }}
                onClick={handleToggleFilePanel}
              >
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2.5">
                    <Folder className="h-4 w-4" style={{ color: '#6155F5' }} />
                    <span className="text-sm font-medium" style={{ color: '#FFFFFF' }}>Files</span>
                  </div>
                  <div className="text-xs" style={{ color: '#FFFFFF', opacity: 0.5 }}>
                    {showFilePanel ? 'Close' : 'View all'}
                  </div>
                </div>
                {/* Show first two file names — clicking a name opens that file directly */}
                {files.length > 0 && (
                  <div className="flex flex-col gap-0.5">
                    {files.slice(0, 2).map((filePath, index) => {
                      const fileName = filePath.split('/').pop();
                      return (
                        <div
                          key={index}
                          className="flex items-center gap-2 text-[13px] rounded-md px-1 py-1 -mx-1 transition-colors hover:bg-white/5"
                          style={{ color: '#FFFFFF', opacity: 0.7 }}
                          onClick={(e) => {
                            e.stopPropagation();
                            setFilePanelTargetFile(filePath);
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
            <div className="w-full flex flex-col gap-4 pb-8">
              <div className="flex items-center justify-between">
                <h2 className="text-base font-medium" style={{ color: '#FFFFFF' }}>
                  Tasks
                </h2>
              </div>

              {threads.length === 0 ? (
                // Empty state
                <div className="flex flex-col items-center justify-center py-12">
                  <p className="text-sm mb-2" style={{ color: '#FFFFFF', opacity: 0.65 }}>
                    No threads yet
                  </p>
                  <p className="text-xs text-center max-w-md" style={{ color: '#FFFFFF', opacity: 0.45 }}>
                    Start a conversation to create your first thread
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
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Right Side: File Panel — hidden for flash workspaces */}
      {showFilePanel && !isFlash && (
        <>
          <div
            className="w-[4px] bg-transparent hover:bg-white/20 cursor-col-resize flex-shrink-0 transition-colors"
            onMouseDown={handleDividerMouseDown}
          />
          <div className="flex-shrink-0" style={{ width: filePanelWidth }}>
            <FilePanel
              workspaceId={workspaceId}
              onClose={() => setShowFilePanel(false)}
              targetFile={filePanelTargetFile}
              onTargetFileHandled={() => setFilePanelTargetFile(null)}
              files={panelFiles}
              filesLoading={panelFilesLoading}
              filesError={panelFilesError}
              onRefreshFiles={refreshPanelFiles}
            />
          </div>
        </>
      )}

      {/* Delete Confirmation Modal */}
      <DeleteConfirmModal
        isOpen={deleteModal.isOpen}
        workspaceName={deleteModal.thread?.title || `Thread ${deleteModal.thread?.thread_index !== undefined ? deleteModal.thread.thread_index + 1 : ''}`}
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
    </div>
  );
}

export default ThreadGallery;
