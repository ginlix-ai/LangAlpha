import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Plus, Loader2, Search, ArrowDownUp, MoreHorizontal, Zap, MessageSquareText, Pin, Trash2, GripVertical, Check } from 'lucide-react';
import { useNavigate, useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { DndContext, closestCenter, PointerSensor, useSensor, useSensors } from '@dnd-kit/core';
import { SortableContext, useSortable, arrayMove, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import CreateWorkspaceModal from './CreateWorkspaceModal';
import DeleteConfirmModal from './DeleteConfirmModal';
import MorphingPageDots from '../../../components/ui/morphing-page-dots';
import { getWorkspaces, createWorkspace, deleteWorkspace, getFlashWorkspace, updateWorkspace, reorderWorkspaces } from '../utils/api';
import { removeStoredThreadId } from '../hooks/useChatMessages';
import { clearChatSession } from '../hooks/utils/chatSessionRestore';

const PAGE_SIZE = 8;

const hoverHandlers = (bgVar) => ({
  onMouseEnter: (e) => { e.currentTarget.style.backgroundColor = `var(${bgVar})`; },
  onMouseLeave: (e) => { e.currentTarget.style.backgroundColor = ''; },
});

const slideVariants = {
  enter: (direction) => ({
    x: direction > 0 ? 80 : -80,
    opacity: 0,
  }),
  center: {
    x: 0,
    opacity: 1,
  },
  exit: (direction) => ({
    x: direction > 0 ? -80 : 80,
    opacity: 0,
  }),
};

const slideTransition = {
  x: { type: 'spring', stiffness: 400, damping: 35 },
  opacity: { duration: 0.15 },
};

/**
 * Card menu dropdown (Pin / Delete)
 */
function CardMenu({ workspace, onTogglePin, onDelete }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const menuRef = useRef(null);

  useEffect(() => {
    if (!open) return;
    const handleClickOutside = (e) => {
      if (menuRef.current && !menuRef.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [open]);

  return (
    <div ref={menuRef} className="relative">
      <button
        onClick={(e) => {
          e.stopPropagation();
          setOpen((v) => !v);
        }}
        className="h-8 w-8 rounded-md transition-colors flex items-center justify-center"
        style={{ color: 'var(--color-text-tertiary)' }}
        {...hoverHandlers('--color-border-muted')}
      >
        <MoreHorizontal className="h-5 w-5" />
      </button>

      {open && (
        <div
          className="absolute right-0 top-9 z-50 min-w-[150px] rounded-lg border py-1 shadow-lg"
          style={{
            backgroundColor: 'var(--color-bg-elevated, var(--color-bg-card))',
            borderColor: 'var(--color-border-muted)',
          }}
        >
          <button
            onClick={(e) => {
              e.stopPropagation();
              onTogglePin(workspace);
              setOpen(false);
            }}
            className="flex w-full items-center gap-2 px-3 py-2 text-sm transition-colors"
            style={{ color: 'var(--color-text-secondary)' }}
            {...hoverHandlers('--color-bg-subtle')}
          >
            <Pin className="h-4 w-4" />
            {workspace.is_pinned ? t('workspace.unpin') : t('workspace.pinToTop')}
          </button>
          <button
            onClick={(e) => {
              e.stopPropagation();
              onDelete(workspace);
              setOpen(false);
            }}
            className="flex w-full items-center gap-2 px-3 py-2 text-sm transition-colors"
            style={{ color: 'var(--color-loss)' }}
            {...hoverHandlers('--color-bg-subtle')}
          >
            <Trash2 className="h-4 w-4" />
            {t('common.delete', 'Delete')}
          </button>
        </div>
      )}
    </div>
  );
}

/**
 * Sortable row for reorder mode — compact single-column list item
 */
function SortableReorderRow({ workspace }) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: workspace.workspace_id, disabled: workspace.status === 'flash' });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    zIndex: isDragging ? 50 : undefined,
  };

  const isFlash = workspace.status === 'flash';

  return (
    <div
      ref={setNodeRef}
      className="flex items-center gap-3 px-4 py-3 rounded-xl border mb-2"
      style={{
        ...style,
        background: isFlash
          ? 'linear-gradient(to right, var(--color-accent-soft), var(--color-bg-subtle))'
          : 'var(--color-bg-card-gradient, var(--color-border-muted))',
        borderColor: isFlash ? 'var(--color-accent-overlay)' : 'var(--color-bg-card-border, var(--color-border-muted))',
      }}
    >
      {!isFlash ? (
        <button
          {...listeners}
          {...attributes}
          className="flex-shrink-0 cursor-grab active:cursor-grabbing p-1 rounded"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <GripVertical className="h-5 w-5" />
        </button>
      ) : (
        <div className="flex-shrink-0 p-1">
          <Zap className="h-5 w-5" style={{ color: 'var(--color-accent-primary)' }} />
        </div>
      )}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          {!isFlash && workspace.is_pinned && (
            <Pin className="h-3.5 w-3.5 flex-shrink-0 rotate-45" style={{ color: 'var(--color-text-tertiary)' }} />
          )}
          <span className="font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
            {workspace.name}
          </span>
        </div>
      </div>
    </div>
  );
}

/**
 * Workspace card for the normal gallery grid (no DnD)
 */
function WorkspaceCard({ workspace, onSelect, onTogglePin, onDelete, prefetchThreads, index }) {
  const { t, i18n } = useTranslation();
  const isFlash = workspace.status === 'flash';

  return (
    <div
      className="h-40 enter-fade-up"
      style={{ animationDelay: `${(index || 0) * 50}ms` }}
    >
      <div
        className="relative group h-full"
        onMouseEnter={() => prefetchThreads?.(workspace.workspace_id)}
      >
        <div
          onClick={() => onSelect(workspace.workspace_id, workspace.name, workspace.status)}
          className="relative flex cursor-pointer flex-col overflow-hidden rounded-xl py-4 pl-5 pr-4 transition-all ease-in-out hover:shadow-sm active:scale-[0.98] h-full w-full"
          style={{
            background: isFlash
              ? 'linear-gradient(to bottom, var(--color-accent-soft), var(--color-bg-subtle))'
              : 'var(--color-bg-card-gradient, linear-gradient(to bottom, var(--color-border-muted), var(--color-border-muted)))',
            border: isFlash
              ? '0.5px solid var(--color-accent-overlay)'
              : '0.5px solid var(--color-bg-card-border, var(--color-border-muted))',
            backdropFilter: 'blur(8px)',
            WebkitBackdropFilter: 'blur(8px)',
          }}
        >
          <div className="flex flex-col flex-grow gap-4">
            <div className="flex items-center pr-10 overflow-hidden gap-2">
              {isFlash && (
                <Zap className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
              )}
              {!isFlash && workspace.is_pinned && (
                <Pin className="h-3.5 w-3.5 flex-shrink-0 rotate-45" style={{ color: 'var(--color-text-tertiary)' }} />
              )}
              <div className="font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
                {workspace.name}
              </div>
            </div>
            <div className="text-sm line-clamp-2 flex-grow" style={{ color: 'var(--color-text-tertiary)' }}>
              {workspace.description || ''}
            </div>
            <div className="text-xs mt-auto pt-3 flex justify-between" style={{ color: 'var(--color-text-tertiary)' }}>
              <span>
                {t('workspace.updated', { time: workspace.updated_at ? new Date(workspace.updated_at).toLocaleDateString(i18n.language, { month: 'short', day: 'numeric' }) : t('workspace.recently') })}
              </span>
            </div>
          </div>
        </div>

        {/* Menu (no drag handle in normal mode) */}
        {!isFlash && (
          <div className="absolute top-3 right-3 z-10 transition-opacity opacity-0 group-focus-within:opacity-100 group-hover:opacity-100">
            <CardMenu workspace={workspace} onTogglePin={onTogglePin} onDelete={onDelete} />
          </div>
        )}
      </div>
    </div>
  );
}

/**
 * WorkspaceGallery Component
 *
 * Displays a gallery of workspaces as cards.
 * Features:
 * - Lists all workspaces for the user
 * - "Create Workspace" button that opens a modal
 * - Empty state when no workspaces exist
 * - Handles workspace creation
 *
 * @param {Function} onWorkspaceSelect - Callback when a workspace is selected (receives workspaceId)
 */
function WorkspaceGallery({ onWorkspaceSelect, cache, prefetchThreads }) {
  const { t } = useTranslation();
  const [workspaces, setWorkspaces] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [deleteModal, setDeleteModal] = useState({ isOpen: false, workspace: null });
  const [isDeleting, setIsDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [sortBy, setSortBy] = useState('activity'); // 'activity' | 'name' | 'custom'
  const sortByRef = useRef(sortBy);
  sortByRef.current = sortBy;
  const [totalWorkspaces, setTotalWorkspaces] = useState(0);
  const [currentPage, setCurrentPage] = useState(0);
  const [isReorderMode, setIsReorderMode] = useState(false);
  const [allWorkspaces, setAllWorkspaces] = useState([]); // full list for reorder mode
  const navigate = useNavigate();
  const { workspaceId: currentWorkspaceId } = useParams();
  const loadingRef = useRef(false);
  const searchTimerRef = useRef(null);
  const scrollContainerRef = useRef(null);
  const slideDirectionRef = useRef(0); // 1 = forward, -1 = back
  const skipInitialAnimRef = useRef(true); // skip slide animation on first render
  const gridHeightRef = useRef(null); // locked grid height for consistent dot placement
  const preSortByRef = useRef(sortBy); // sort mode before entering reorder
  const didReorderRef = useRef(false); // whether a drag occurred in reorder mode
  const isSearching = searchQuery.length > 0;
  const totalPages = Math.ceil((totalWorkspaces + 1) / PAGE_SIZE);

  // DnD sensors — require 8px drag distance before activating
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 8 } }),
  );

  const goToPage = useCallback((page) => {
    gridHeightRef.current = null;
    setCurrentPage((prev) => {
      slideDirectionRef.current = page > prev ? 1 : -1;
      return page;
    });
  }, []);

  // Clear saved chat session so tab-switching returns to workspace gallery
  useEffect(() => {
    clearChatSession();
  }, []);

  // Load workspaces on mount, using cache for instant display on return navigation
  useEffect(() => {
    // Guard: Prevent duplicate calls
    if (loadingRef.current) {
      return;
    }

    // Show cached data instantly (stale-while-revalidate)
    if (cache?.current?.workspaces) {
      setWorkspaces(cache.current.workspaces);
      if (cache.current.total != null) {
        setTotalWorkspaces(cache.current.total);
      }
      setIsLoading(false);
    }

    loadingRef.current = true;
    loadWorkspaces(0).finally(() => {
      loadingRef.current = false;
    });
  }, []);

  // Re-fetch when page changes (skip initial mount which is handled above)
  const initialMountRef = useRef(true);
  useEffect(() => {
    if (initialMountRef.current) {
      initialMountRef.current = false;
      return;
    }
    if (!isSearching && !isReorderMode) {
      loadWorkspaces(currentPage);
    }
    scrollContainerRef.current?.scrollTo({ top: 0, behavior: 'smooth' });
  }, [currentPage, isSearching, isReorderMode, sortBy]);

  /**
   * Fetches workspaces from the API with pagination.
   * @param {number} page - 0-indexed page number
   */
  const loadWorkspaces = async (page = currentPage) => {
    try {
      const hasCached = cache?.current?.workspaces;
      if (!hasCached) setIsLoading(true);
      setError(null);
      // Reserve one slot on page 0 for the flash workspace
      const isFirstPage = (page || 0) === 0;
      const limit = isFirstPage ? PAGE_SIZE - 1 : PAGE_SIZE;
      const offset = isFirstPage ? 0 : (PAGE_SIZE - 1) + (page - 1) * PAGE_SIZE;
      const [data, flashWs] = await Promise.all([
        getWorkspaces(limit, offset, sortByRef.current),
        isFirstPage ? getFlashWorkspace().catch(() => null) : Promise.resolve(null),
      ]);
      let workspaceList = data.workspaces || [];
      const total = data.total ?? workspaceList.length;
      // Prepend flash workspace on first page (server excludes it from listings)
      if (flashWs && isFirstPage) {
        workspaceList = [flashWs, ...workspaceList];
      }
      setWorkspaces(workspaceList);
      setTotalWorkspaces(total);

      // Update cache
      if (cache?.current) {
        cache.current.workspaces = workspaceList;
        cache.current.total = total;
        cache.current.fetchedAt = Date.now();
      }
    } catch (err) {
      console.error('Error loading workspaces:', err);
      setError(t('workspace.failedLoadWorkspaces'));
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * Debounced search: fetch all workspaces matching query
   */
  const handleSearchChange = useCallback((value) => {
    setSearchQuery(value);
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);

    if (value.length > 0) {
      searchTimerRef.current = setTimeout(async () => {
        try {
          const data = await getWorkspaces(100, 0, sortByRef.current);
          setWorkspaces(data.workspaces || []);
        } catch (err) {
          console.error('Error searching workspaces:', err);
        }
      }, 300);
    } else {
      // Search cleared — return to paginated view
      loadWorkspaces(currentPage);
    }
  }, [currentPage]);

  // Cleanup timers on unmount
  useEffect(() => {
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    };
  }, []);

  /**
   * Handles workspace creation
   * @param {Object} workspaceData - Object with name and description
   */
  const handleCreateWorkspace = async (workspaceData) => {
    try {
      const newWorkspace = await createWorkspace(
        workspaceData.name,
        workspaceData.description,
      );
      // Add new workspace to the list
      setWorkspaces((prev) => [newWorkspace, ...prev]);
      setTotalWorkspaces((prev) => prev + 1);
      // Return workspace so modal can use workspace_id for file uploads
      return newWorkspace;
    } catch (err) {
      console.error('Error creating workspace:', err);
      throw err; // Let modal handle the error display
    }
  };

  /**
   * Handles delete icon click - opens confirmation modal
   * @param {Object} workspace - The workspace to delete
   */
  const handleDeleteClick = (workspace) => {
    setDeleteModal({ isOpen: true, workspace });
    setDeleteError(null);
  };

  /**
   * Handles confirmed workspace deletion
   */
  const handleConfirmDelete = async () => {
    if (!deleteModal.workspace) return;

    const workspaceToDelete = deleteModal.workspace;
    const workspaceId = workspaceToDelete.workspace_id;

    if (!workspaceId) {
      console.error('No workspace ID found in workspace object:', workspaceToDelete);
      setDeleteError(t('workspace.invalidWorkspace'));
      return;
    }

    setIsDeleting(true);
    setDeleteError(null);

    try {
      await deleteWorkspace(workspaceId);

      // Clean up localStorage: remove thread ID for deleted workspace
      removeStoredThreadId(workspaceId);

      // Remove workspace from list and adjust total
      setWorkspaces((prev) => {
        const updated = prev.filter((ws) => ws.workspace_id !== workspaceId);
        // If page is now empty, go to previous page
        if (updated.length === 0 && currentPage > 0) {
          slideDirectionRef.current = -1;
          setCurrentPage((p) => p - 1);
        }
        return updated;
      });
      setTotalWorkspaces((prev) => Math.max(0, prev - 1));

      // If the deleted workspace is currently active, navigate back to gallery
      if (currentWorkspaceId === workspaceId) {
        navigate('/chat');
      }

      // Close modal
      setDeleteModal({ isOpen: false, workspace: null });
    } catch (err) {
      console.error('Error deleting workspace:', err);
      const errorMessage = err.message || t('workspace.failedDeleteWorkspace');
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
    setDeleteModal({ isOpen: false, workspace: null });
    setDeleteError(null);
  };

  /**
   * Toggle pin state — optimistic update
   */
  const handleTogglePin = async (workspace) => {
    const newPinned = !workspace.is_pinned;
    const wsId = workspace.workspace_id;

    // Optimistic update
    setWorkspaces((prev) =>
      prev.map((ws) =>
        ws.workspace_id === wsId ? { ...ws, is_pinned: newPinned } : ws
      )
    );

    try {
      await updateWorkspace(wsId, { is_pinned: newPinned });
      // Refetch from server — pinning changes global sort order
      slideDirectionRef.current = -1;
      gridHeightRef.current = null;
      if (currentPage === 0) {
        await loadWorkspaces(0);
      } else {
        setCurrentPage(0); // useEffect handles the fetch
      }
    } catch (err) {
      console.error('Error toggling pin:', err);
      // Rollback
      setWorkspaces((prev) =>
        prev.map((ws) =>
          ws.workspace_id === wsId ? { ...ws, is_pinned: !newPinned } : ws
        )
      );
    }
  };

  /**
   * Enter reorder mode — fetch all workspaces
   */
  const enterReorderMode = async () => {
    try {
      preSortByRef.current = sortBy;
      didReorderRef.current = false;
      const [data, flashWs] = await Promise.all([
        getWorkspaces(100, 0),
        getFlashWorkspace().catch(() => null),
      ]);
      const list = data.workspaces || [];
      setAllWorkspaces(flashWs ? [flashWs, ...list] : list);
      setIsReorderMode(true);
    } catch (err) {
      console.error('Error loading workspaces for reorder:', err);
    }
  };

  /**
   * Exit reorder mode — return to paginated gallery
   */
  const exitReorderMode = () => {
    setIsReorderMode(false);
    setSortBy(didReorderRef.current ? 'custom' : preSortByRef.current);
    gridHeightRef.current = null;
    setCurrentPage(0);
    // useEffect will re-fetch with correct sortBy after re-render
  };

  /**
   * Handle drag end in reorder mode
   */
  const handleReorderDragEnd = async (event) => {
    const { active, over } = event;
    if (!over || active.id === over.id) return;

    const sorted = reorderSortedList;
    const oldIndex = sorted.findIndex((ws) => ws.workspace_id === active.id);
    const newIndex = sorted.findIndex((ws) => ws.workspace_id === over.id);
    if (oldIndex === -1 || newIndex === -1) return;

    const draggedWs = sorted[oldIndex];
    const targetWs = sorted[newIndex];

    // Prevent crossing pin/unpin boundary
    if (draggedWs.is_pinned !== targetWs.is_pinned) return;
    // Prevent moving flash workspaces
    if (draggedWs.status === 'flash' || targetWs.status === 'flash') return;

    const reordered = arrayMove(sorted, oldIndex, newIndex);

    // Assign sequential sort_order
    const items = [];
    reordered.forEach((ws, i) => {
      if (ws.status === 'flash') return;
      items.push({ workspace_id: ws.workspace_id, sort_order: i });
    });

    // Optimistic update
    const snapshot = allWorkspaces;
    const updated = allWorkspaces.map((ws) => {
      const item = items.find((it) => it.workspace_id === ws.workspace_id);
      return item ? { ...ws, sort_order: item.sort_order } : ws;
    });
    setAllWorkspaces(updated);

    try {
      await reorderWorkspaces(items);
      didReorderRef.current = true;
    } catch (err) {
      console.error('Error reordering workspaces:', err);
      setAllWorkspaces(snapshot); // rollback
    }
  };

  /**
   * Filter and sort workspaces
   */
  // Server handles sort order; client only filters by search and keeps flash on top
  const filteredAndSortedWorkspaces = workspaces
    .filter((workspace) =>
      workspace.name.toLowerCase().includes(searchQuery.toLowerCase())
    )
    .sort((a, b) => {
      // Keep flash workspace pinned to top
      const aFlash = a.status === 'flash' ? 1 : 0;
      const bFlash = b.status === 'flash' ? 1 : 0;
      if (aFlash !== bFlash) return bFlash - aFlash;
      return 0; // preserve server order
    });

  const visibleWorkspaces = filteredAndSortedWorkspaces;

  // Sorted list for reorder mode (flash first, then pinned, then unpinned — by sort_order)
  const reorderSortedList = [...allWorkspaces].sort((a, b) => {
    const aFlash = a.status === 'flash' ? 1 : 0;
    const bFlash = b.status === 'flash' ? 1 : 0;
    if (aFlash !== bFlash) return bFlash - aFlash;
    const aPinned = a.is_pinned ? 1 : 0;
    const bPinned = b.is_pinned ? 1 : 0;
    if (aPinned !== bPinned) return bPinned - aPinned;
    if (a.sort_order !== b.sort_order) return a.sort_order - b.sort_order;
    return new Date(b.updated_at || 0) - new Date(a.updated_at || 0);
  });
  const reorderSortedIds = reorderSortedList.map((ws) => ws.workspace_id);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="h-8 w-8 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
          <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('workspace.loadingWorkspaces')}
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
            onClick={() => loadWorkspaces(0)}
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

  const hasWorkspaces = workspaces.length > 0;

  const renderGrid = () => {
    const skipAnim = skipInitialAnimRef.current;
    if (skipAnim) skipInitialAnimRef.current = false;
    return (
    <AnimatePresence mode="wait" custom={slideDirectionRef.current}>
    {visibleWorkspaces.length === 0 ? (
      // Empty state
      <motion.div
        key="empty"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.15 }}
        className="flex flex-col items-center justify-center py-16"
      >
        {searchQuery ? (
          <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('workspace.noWorkspacesFound')}
          </p>
        ) : (
          <>
            <p className="text-lg font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
              {t('workspace.welcomeTitle')}
            </p>
            <p className="text-sm mb-8" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('workspace.welcomeDesc')}
            </p>
            <div className="flex flex-col sm:flex-row items-center gap-3">
              <button
                onClick={async () => {
                  try {
                    const flashWs = await getFlashWorkspace();
                    navigate(`/chat/${flashWs.workspace_id}/__default__`, {
                      state: {
                        isOnboarding: true,
                        agentMode: 'flash',
                        workspaceStatus: 'flash',
                      },
                    });
                  } catch (err) {
                    console.error('Error starting onboarding:', err);
                  }
                }}
                className="flex items-center gap-2 px-6 py-3 rounded-lg transition-all hover:scale-[1.01] active:scale-[0.985]"
                style={{
                  backgroundColor: 'var(--color-accent-primary)',
                  color: 'var(--color-text-on-accent)',
                }}
              >
                <MessageSquareText className="h-5 w-5" />
                <span className="font-medium">{t('settings.startOnboarding')}</span>
              </button>
              <button
                onClick={() => setIsModalOpen(true)}
                className="flex items-center gap-2 px-6 py-3 rounded-lg border transition-all hover:bg-foreground/5 hover:scale-[1.01] active:scale-[0.985]"
                style={{
                  borderColor: 'var(--color-border-muted)',
                  color: 'var(--color-text-primary)',
                }}
              >
                <Plus className="h-5 w-5" />
                <span className="font-medium">{t('workspace.createWorkspace')}</span>
              </button>
            </div>
          </>
        )}
      </motion.div>
    ) : (
      <div
        style={{ height: gridHeightRef.current || undefined, overflow: 'hidden' }}
        ref={(el) => {
          if (el && visibleWorkspaces.length >= PAGE_SIZE) {
            const h = el.scrollHeight;
            if (!gridHeightRef.current || h > gridHeightRef.current) {
              gridHeightRef.current = h;
              el.style.height = h + 'px';
            }
          }
        }}
      >
        <motion.div
          key={`page-${currentPage}`}
          custom={slideDirectionRef.current}
          variants={slideVariants}
          initial={skipAnim ? false : "enter"}
          animate="center"
          exit="exit"
          transition={slideTransition}
          className="grid gap-3 md:grid-cols-2 md:gap-6 grid-cols-1 mb-3 md:mb-6"
        >
          {visibleWorkspaces.map((workspace, index) => (
            <WorkspaceCard
              key={workspace.workspace_id}
              workspace={workspace}
              index={index}
              onSelect={onWorkspaceSelect}
              onTogglePin={handleTogglePin}
              onDelete={handleDeleteClick}
              prefetchThreads={prefetchThreads}
            />
          ))}
        </motion.div>
      </div>
    )}
    </AnimatePresence>
  );
  };

  return (
    <div
      className="h-full flex flex-col overflow-hidden"
      style={{
        backgroundColor: 'var(--color-bg-page)',
        backgroundImage: 'radial-gradient(circle at center, var(--color-dot-grid) 0.75px, transparent 0.75px)',
        backgroundSize: '18px 18px',
        backgroundPosition: '0 0'
      }}
    >
      {/* Header */}
      <header className="flex w-full h-16 md:h-24 md:items-end mx-auto max-w-4xl flex-shrink-0 px-4 md:px-8 enter-fade-up">
        <div className="flex w-full items-center justify-between gap-4">
          <h1 className="text-2xl font-semibold hidden md:block title-font" style={{ color: 'var(--color-text-primary)' }}>
            {t('workspace.workspaces')}
          </h1>
          <div></div>
          {hasWorkspaces && (
            <button
              onClick={() => setIsModalOpen(true)}
              className="flex items-center gap-1.5 px-4 py-2 h-9 rounded-lg transition-all hover:scale-[1.01] active:scale-[0.985]"
              style={{
                backgroundColor: 'var(--color-accent-primary)',
                color: 'var(--color-text-on-accent)',
              }}
            >
              <Plus className="h-4 w-4" />
              <span className="text-sm font-medium">{t('workspace.newWorkspace')}</span>
            </button>
          )}
        </div>
      </header>

      {/* Main Content */}
      <main className="mx-auto mt-4 w-full flex-1 min-h-0 px-4 md:px-8 lg:mt-6 max-w-4xl flex flex-col pb-0">
        <h1 className="text-xl font-semibold mb-4 md:hidden title-font" style={{ color: 'var(--color-text-primary)' }}>
          {t('workspace.workspaces')}
        </h1>

        {hasWorkspaces && !isReorderMode && (
        <div className="flex-shrink-0 flex flex-col gap-4 pb-4 md:pb-6 px-1 enter-fade-up enter-fade-up-d1">
          {/* Search Bar */}
          <div className="w-full">
            <div
              className="flex items-center gap-2 h-11 px-3 rounded-xl border transition-colors"
              style={{
                backgroundColor: 'var(--color-bg-input)',
                borderColor: 'var(--color-border-muted)',
              }}
            >
              <Search className="h-5 w-5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              <input
                className="w-full bg-transparent outline-none text-sm"
                style={{ color: 'var(--color-text-primary)' }}
                placeholder={t('workspace.searchWorkspaces')}
                value={searchQuery}
                onChange={(e) => handleSearchChange(e.target.value)}
              />
            </div>
          </div>

          {/* Sort By + Reorder */}
          <div className="flex w-full gap-4 justify-between items-center">
            <div></div>
            <div className="flex items-center gap-2.5">
              <span className="text-sm hidden md:inline" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('workspace.sortBy')}
              </span>
              <button
                onClick={() => {
                  setSortBy((s) => s === 'activity' ? 'name' : s === 'name' ? 'custom' : 'activity');
                  setCurrentPage(0);
                }}
                className="flex items-center gap-1 md:gap-1.5 px-2 md:px-3 py-1 h-9 rounded-lg border transition-colors hover:bg-foreground/5"
                style={{ borderColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
              >
                <ArrowDownUp className="h-4 w-4 md:hidden" />
                <span className="text-sm">
                  {sortBy === 'activity' ? t('workspace.activity') : sortBy === 'name' ? t('common.name') : t('workspace.custom')}
                </span>
              </button>
              <button
                onClick={enterReorderMode}
                className="flex items-center gap-1.5 px-2 md:px-3 py-1 h-9 rounded-lg border transition-colors hover:bg-foreground/5"
                style={{ borderColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
              >
                <GripVertical className="h-4 w-4" />
                <span className="text-sm hidden md:inline">{t('workspace.reorder')}</span>
              </button>
            </div>
          </div>
        </div>
        )}

        {isReorderMode ? (
          /* ── Reorder Mode: vertical scrollable list with DnD ── */
          <div className="flex-1 min-h-0 flex flex-col">
            <div className="flex items-center justify-between px-1 pb-3 flex-shrink-0">
              <span className="text-sm font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                {t('workspace.dragToReorder')}
              </span>
              <button
                onClick={exitReorderMode}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors"
                style={{
                  backgroundColor: 'var(--color-accent-primary)',
                  color: 'var(--color-text-on-accent)',
                }}
              >
                <Check className="h-4 w-4" />
                {t('common.done')}
              </button>
            </div>
            <div className="flex-1 min-h-0 overflow-y-auto px-1 pb-4">
              <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleReorderDragEnd}>
                <SortableContext items={reorderSortedIds} strategy={verticalListSortingStrategy}>
                  {reorderSortedList.map((ws) => (
                    <SortableReorderRow key={ws.workspace_id} workspace={ws} />
                  ))}
                </SortableContext>
              </DndContext>
            </div>
          </div>
        ) : (
          /* ── Normal Mode: paginated grid ── */
          <>
            <div ref={scrollContainerRef} className="overflow-y-auto overflow-x-hidden px-1">
              {renderGrid()}
            </div>

            {/* Pagination dots — pinned below scroll area */}
            {!isSearching && totalPages > 1 && (
              <div className="flex-shrink-0 py-3">
                <MorphingPageDots
                  totalPages={totalPages}
                  activeIndex={currentPage}
                  onChange={goToPage}
                />
              </div>
            )}
          </>
        )}
      </main>

      {/* Create Workspace Modal */}
      <CreateWorkspaceModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onCreate={handleCreateWorkspace}
        onComplete={(wsId) => onWorkspaceSelect(wsId)}
      />

      {/* Delete Confirmation Modal */}
      <DeleteConfirmModal
        isOpen={deleteModal.isOpen}
        workspaceName={deleteModal.workspace?.name || ''}
        onConfirm={handleConfirmDelete}
        onCancel={handleCancelDelete}
        isDeleting={isDeleting}
        error={deleteError}
      />
    </div>
  );
}

export default WorkspaceGallery;
