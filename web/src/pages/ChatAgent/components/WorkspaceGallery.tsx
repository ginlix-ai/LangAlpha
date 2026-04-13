import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { Plus, Loader2, Search, ArrowDownUp, MoreHorizontal, Zap, MessageSquareText, Pin, Trash2, GripVertical, Check } from 'lucide-react';
import { useNavigate, useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { DndContext, closestCenter, PointerSensor, useSensor, useSensors } from '@dnd-kit/core';
import type { DragEndEvent } from '@dnd-kit/core';
import { SortableContext, useSortable, arrayMove, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import CreateWorkspaceModal from './CreateWorkspaceModal';
import DeleteConfirmModal from './DeleteConfirmModal';
import MorphingPageDots from '../../../components/ui/morphing-page-dots';
import { useIsMobile, getIsMobileSnapshot } from '@/hooks/useIsMobile';
import { useWorkspaces } from '../../../hooks/useWorkspaces';
import { queryKeys } from '../../../lib/queryKeys';
import { createWorkspace, deleteWorkspace, getFlashWorkspace, updateWorkspace, reorderWorkspaces } from '../utils/api';
import { removeStoredThreadId } from '../hooks/useChatMessages';
import { clearChatSession } from '../hooks/utils/chatSessionRestore';

const DEFAULT_PAGE_SIZE = 8;

interface WorkspaceRecord {
  workspace_id: string;
  name: string;
  description?: string;
  status?: string;
  is_pinned?: boolean;
  sort_order?: number;
  updated_at?: string;
  [key: string]: unknown;
}

interface DeleteModalState {
  isOpen: boolean;
  workspace: WorkspaceRecord | null;
}

const slideVariants = {
  enter: (direction: number) => ({
    x: direction > 0 ? 80 : -80,
    opacity: 0,
  }),
  center: {
    x: 0,
    opacity: 1,
  },
  exit: (direction: number) => ({
    x: direction > 0 ? -80 : 80,
    opacity: 0,
  }),
};

const slideTransition = {
  x: { type: 'spring' as const, stiffness: 400, damping: 35 },
  opacity: { duration: 0.15 },
};

/**
 * Card menu dropdown (Pin / Delete)
 */

interface CardMenuProps {
  workspace: WorkspaceRecord;
  onTogglePin: (workspace: WorkspaceRecord) => void;
  onDelete: (workspace: WorkspaceRecord) => void;
}

function CardMenu({ workspace, onTogglePin, onDelete }: CardMenuProps) {
  const { t } = useTranslation();

  return (
    <DropdownMenu modal={false}>
      <DropdownMenuTrigger asChild>
        <button
          onPointerDown={(e) => e.stopPropagation()}
          className="h-8 w-8 rounded-md transition-colors flex items-center justify-center hover:bg-[var(--color-border-muted)]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <MoreHorizontal className="h-5 w-5" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" sideOffset={4}>
        <DropdownMenuItem onSelect={() => onTogglePin(workspace)}>
          <Pin className="h-4 w-4" />
          {workspace.is_pinned ? t('workspace.unpin') : t('workspace.pinToTop')}
        </DropdownMenuItem>
        <DropdownMenuItem variant="destructive" onSelect={() => onDelete(workspace)}>
          <Trash2 className="h-4 w-4" />
          {t('common.delete', 'Delete')}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/**
 * Sortable row for reorder mode -- compact single-column list item
 */
interface SortableReorderRowProps {
  workspace: WorkspaceRecord;
}

function SortableReorderRow({ workspace }: SortableReorderRowProps) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: workspace.workspace_id, disabled: workspace.status === 'flash' });

  const style: React.CSSProperties = {
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
interface WorkspaceCardProps {
  workspace: WorkspaceRecord;
  onSelect: (wsId: string, name?: string, status?: string) => void;
  onTogglePin: (workspace: WorkspaceRecord) => void;
  onDelete: (workspace: WorkspaceRecord) => void;
  prefetchThreads?: (wsId: string) => void;
  index?: number;
}

function WorkspaceCard({ workspace, onSelect, onTogglePin, onDelete, prefetchThreads, index }: WorkspaceCardProps) {
  const { t, i18n } = useTranslation();
  const isMobile = useIsMobile();
  const isFlash = workspace.status === 'flash';

  return (
    <div
      className="h-40 enter-fade-up"
      style={{ animationDelay: `${(index || 0) * 50}ms` }}
    >
      <div
        className="relative group h-full"
        onMouseEnter={!isMobile ? () => prefetchThreads?.(workspace.workspace_id) : undefined}
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
          <div className={`absolute top-3 right-3 z-10 transition-opacity ${isMobile ? 'opacity-60' : 'opacity-0 group-focus-within:opacity-100 group-hover:opacity-100'}`}>
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
 */

interface WorkspaceGalleryProps {
  onWorkspaceSelect: (wsId: string, name?: string, status?: string) => void;
  prefetchThreads?: (wsId: string) => void;
}

function WorkspaceGallery({ onWorkspaceSelect, prefetchThreads }: WorkspaceGalleryProps) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [deleteModal, setDeleteModal] = useState<DeleteModalState>({ isOpen: false, workspace: null });
  const [isDeleting, setIsDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [sortBy, setSortBy] = useState<'activity' | 'name' | 'custom'>('activity');
  const [currentPage, setCurrentPage] = useState(0);
  const [isReorderMode, setIsReorderMode] = useState(false);
  const [allWorkspaces, setAllWorkspaces] = useState<WorkspaceRecord[]>([]);
  const navigate = useNavigate();
  const { workspaceId: currentWorkspaceId } = useParams();
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const slideDirectionRef = useRef(0); // 1 = forward, -1 = back
  const skipInitialAnimRef = useRef(true); // skip slide animation on first render
  const gridHeightRef = useRef<number | null>(null); // locked grid height for consistent dot placement
  const touchStartRef = useRef<{ x: number; y: number; t: number } | null>(null); // swipe gesture tracking
  const preSortByRef = useRef(sortBy); // sort mode before entering reorder
  const didReorderRef = useRef(false); // whether a drag occurred in reorder mode
  const isSearching = debouncedSearch.length > 0;
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);

  // Pagination: reserve one slot on page 0 for the flash workspace
  const isFirstPage = currentPage === 0;
  const wsLimit = isSearching ? 100 : isFirstPage ? pageSize - 1 : pageSize;
  const wsOffset = isSearching ? 0 : isFirstPage ? 0 : (pageSize - 1) + (currentPage - 1) * pageSize;

  // Main workspace list query
  const {
    data: wsData,
    isLoading: isWsLoading,
    error: wsError,
  } = useWorkspaces({
    limit: wsLimit,
    offset: wsOffset,
    sortBy,
    enabled: !isReorderMode,
  });

  // Flash workspace query (idempotent POST -- creates if not exists)
  const { data: flashWs } = useQuery({
    queryKey: queryKeys.workspaces.flash(),
    queryFn: getFlashWorkspace,
    staleTime: 5 * 60_000,
  });

  // Reorder mode: fetch all workspaces
  const { data: allWsData } = useWorkspaces({
    limit: 100,
    offset: 0,
    sortBy: 'custom',
    enabled: isReorderMode,
  });

  // Derive workspace list from query data
  const workspaces = useMemo((): WorkspaceRecord[] => {
    const list = (wsData as any)?.workspaces || []; // TODO: type properly
    // Prepend flash workspace on first page when not searching
    if (flashWs && isFirstPage && !isSearching) {
      return [flashWs as WorkspaceRecord, ...list];
    }
    return list;
  }, [wsData, flashWs, isFirstPage, isSearching]);

  const totalWorkspaces = (wsData as any)?.total || 0; // TODO: type properly
  const totalPages = Math.ceil((totalWorkspaces + 1) / pageSize);

  // Sync allWorkspaces state from query data when in reorder mode
  useEffect(() => {
    if (isReorderMode && (allWsData as any)?.workspaces) {
      const list = (allWsData as any).workspaces;
      setAllWorkspaces(flashWs ? [flashWs as WorkspaceRecord, ...list] : list);
    }
  }, [isReorderMode, allWsData, flashWs]);

  // DnD sensors -- require 8px drag distance before activating
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 8 } }),
  );

  const goToPage = useCallback((page: number) => {
    gridHeightRef.current = null;
    setCurrentPage((prev) => {
      slideDirectionRef.current = page > prev ? 1 : -1;
      return page;
    });
  }, []);

  // Swipe gesture handlers for mobile pagination
  const handleTouchStart = useCallback((e: React.TouchEvent) => {
    const touch = e.touches[0];
    touchStartRef.current = { x: touch.clientX, y: touch.clientY, t: Date.now() };
  }, []);

  const handleTouchEnd = useCallback((e: React.TouchEvent) => {
    if (!touchStartRef.current || isSearching || totalPages <= 1) return;
    const touch = e.changedTouches[0];
    const dx = touch.clientX - touchStartRef.current.x;
    const dy = touch.clientY - touchStartRef.current.y;
    const dt = Date.now() - touchStartRef.current.t;
    touchStartRef.current = null;

    // Require: horizontal distance > 50px, more horizontal than vertical, within 500ms
    if (Math.abs(dx) > 50 && Math.abs(dx) > Math.abs(dy) * 1.5 && dt < 500) {
      if (dx < 0 && currentPage < totalPages - 1) {
        goToPage(currentPage + 1);
      } else if (dx > 0 && currentPage > 0) {
        goToPage(currentPage - 1);
      }
    }
  }, [isSearching, totalPages, currentPage, goToPage]);

  // Clear saved chat session so tab-switching returns to workspace gallery
  useEffect(() => {
    clearChatSession();
  }, []);

  // Scroll to top when page changes
  useEffect(() => {
    scrollContainerRef.current?.scrollTo({ top: 0, behavior: 'smooth' });
  }, [currentPage]);

  // Dynamic page size: compute how many cards fit in the scroll container.
  // Pagination container is always rendered (visibility:hidden when unused)
  // so the scroll container height is stable and no paginationReserve is needed.
  const computePageSizeFromHeight = useCallback((height: number) => {
    const isMobile = getIsMobileSnapshot();
    const columns = isMobile ? 1 : 2;
    const gap = isMobile ? 12 : 24;
    const cardHeight = 160;
    const gridBottomMargin = isMobile ? 12 : 24;
    const available = height - gridBottomMargin;
    const rows = Math.max(1, Math.floor((available + gap) / (cardHeight + gap)));
    return Math.max(2, columns * rows);
  }, []);

  useEffect(() => {
    if (isReorderMode || isWsLoading) return;
    const el = scrollContainerRef.current;
    if (!el) return;

    let debounceTimer: ReturnType<typeof setTimeout> | null = null;

    const handleResize = () => {
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        const newSize = computePageSizeFromHeight(el.clientHeight);
        setPageSize(prev => prev === newSize ? prev : newSize);
      }, 200);
    };

    // Measure after a frame to ensure layout is settled
    requestAnimationFrame(() => {
      const newSize = computePageSizeFromHeight(el.clientHeight);
      setPageSize(newSize);
    });

    const observer = new ResizeObserver(handleResize);
    observer.observe(el);

    return () => {
      observer.disconnect();
      if (debounceTimer) clearTimeout(debounceTimer);
    };
  }, [isReorderMode, isWsLoading, computePageSizeFromHeight]);

  // Reset page and grid height when page size changes
  const prevPageSizeRef = useRef(DEFAULT_PAGE_SIZE);
  useEffect(() => {
    if (prevPageSizeRef.current !== pageSize) {
      prevPageSizeRef.current = pageSize;
      gridHeightRef.current = null;
      setCurrentPage(0);
    }
  }, [pageSize]);

  /**
   * Debounced search: update debouncedSearch after 300ms
   */
  const handleSearchChange = useCallback((value: string) => {
    setSearchQuery(value);
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);

    if (value.length > 0) {
      searchTimerRef.current = setTimeout(() => {
        setDebouncedSearch(value);
      }, 300);
    } else {
      setDebouncedSearch('');
    }
  }, []);

  // Cleanup timers on unmount
  useEffect(() => {
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    };
  }, []);

  /**
   * Handles workspace creation
   */
  const handleCreateWorkspace = async (workspaceData: { name: string; description: string }) => {
    try {
      const newWorkspace = await createWorkspace(
        workspaceData.name,
        workspaceData.description,
      );
      // Invalidate workspace list cache so the new workspace appears
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
      // Return workspace so modal can use workspace_id for file uploads
      return newWorkspace;
    } catch (err) {
      console.error('Error creating workspace:', err);
      throw err; // Let modal handle the error display
    }
  };

  /**
   * Handles delete icon click - opens confirmation modal
   */
  const handleDeleteClick = (workspace: WorkspaceRecord) => {
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

      // Invalidate workspace list cache
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });

      // If page would be empty after deletion, go to previous page
      const remainingOnPage = workspaces.filter((ws) => ws.workspace_id !== workspaceId).length;
      if (remainingOnPage === 0 && currentPage > 0) {
        slideDirectionRef.current = -1;
        setCurrentPage((p) => p - 1);
      }

      // If the deleted workspace is currently active, navigate back to gallery
      if (currentWorkspaceId === workspaceId) {
        navigate('/chat');
      }

      // Close modal
      setDeleteModal({ isOpen: false, workspace: null });
    } catch (err: any) { // TODO: type properly
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
   * Toggle pin state with optimistic update and rollback on error.
   */
  const handleTogglePin = async (workspace: WorkspaceRecord) => {
    const newPinned = !workspace.is_pinned;
    const wsId = workspace.workspace_id;

    // Snapshot + optimistic flip across all cached workspace lists
    const previous = queryClient.getQueriesData({ queryKey: queryKeys.workspaces.lists() });
    previous.forEach(([key, data]: [unknown, any]) => {
      if (data?.workspaces) {
        queryClient.setQueryData(key as any, {
          ...data,
          workspaces: data.workspaces.map((ws: WorkspaceRecord) =>
            ws.workspace_id === wsId ? { ...ws, is_pinned: newPinned } : ws
          ),
        });
      }
    });

    try {
      await updateWorkspace(wsId, { is_pinned: newPinned });
      // Refetch from server -- pinning changes global sort order
      slideDirectionRef.current = -1;
      gridHeightRef.current = null;
      if (currentPage !== 0) {
        setCurrentPage(0);
      }
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
    } catch (err) {
      // Rollback optimistic update
      previous.forEach(([key, data]: [unknown, any]) => queryClient.setQueryData(key as any, data));
      console.error('Error toggling pin:', err);
    }
  };

  /**
   * Enter reorder mode -- fetch all workspaces
   */
  const enterReorderMode = () => {
    preSortByRef.current = sortBy;
    didReorderRef.current = false;
    setIsReorderMode(true);
  };

  /**
   * Exit reorder mode -- return to paginated gallery
   */
  const exitReorderMode = () => {
    setIsReorderMode(false);
    const newSortBy = didReorderRef.current ? 'custom' : preSortByRef.current;
    setSortBy(newSortBy);
    gridHeightRef.current = null;
    setCurrentPage(0);
    // Invalidate so paginated view refetches with correct sort order
    queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
  };

  /**
   * Handle drag end in reorder mode
   */
  const handleReorderDragEnd = async (event: DragEndEvent) => {
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
    const items: { workspace_id: string; sort_order: number }[] = [];
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
      await reorderWorkspaces(items.map(it => ({ workspace_id: it.workspace_id, position: it.sort_order })));
      didReorderRef.current = true;
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
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

  // Sorted list for reorder mode (flash first, then pinned, then unpinned -- by sort_order)
  const reorderSortedList = [...allWorkspaces].sort((a, b) => {
    const aFlash = a.status === 'flash' ? 1 : 0;
    const bFlash = b.status === 'flash' ? 1 : 0;
    if (aFlash !== bFlash) return bFlash - aFlash;
    const aPinned = a.is_pinned ? 1 : 0;
    const bPinned = b.is_pinned ? 1 : 0;
    if (aPinned !== bPinned) return bPinned - aPinned;
    if ((a.sort_order ?? 0) !== (b.sort_order ?? 0)) return (a.sort_order ?? 0) - (b.sort_order ?? 0);
    return new Date(b.updated_at || 0).getTime() - new Date(a.updated_at || 0).getTime();
  });
  const reorderSortedIds = reorderSortedList.map((ws) => ws.workspace_id);

  if (isWsLoading) {
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

  if (wsError) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4 max-w-md text-center px-4">
          <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
            {t('workspace.failedLoadWorkspaces')}
          </p>
          <button
            onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() })}
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
                    const flashWsData = await getFlashWorkspace();
                    navigate(`/chat/t/__default__`, {
                      state: {
                        workspaceId: (flashWsData as WorkspaceRecord).workspace_id,
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
          if (el && visibleWorkspaces.length >= pageSize) {
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
      {/* Header (desktop only) */}
      <header className="hidden md:flex w-full h-24 items-end mx-auto max-w-4xl flex-shrink-0 px-8 enter-fade-up">
        <div className="flex w-full items-center justify-between gap-4">
          <h1 className="text-2xl font-semibold title-font" style={{ color: 'var(--color-text-primary)' }}>
            {t('workspace.workspaces')}
          </h1>
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
        <div className="flex items-center justify-between mb-4 md:hidden">
          <h1 className="text-xl font-semibold title-font" style={{ color: 'var(--color-text-primary)' }}>
            {t('workspace.workspaces')}
          </h1>
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
                className="w-full bg-transparent outline-none text-base sm:text-sm"
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
          /* -- Reorder Mode: vertical scrollable list with DnD -- */
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
          /* -- Normal Mode: paginated grid -- */
          <>
            <div
              ref={scrollContainerRef}
              className="flex-1 min-h-0 overflow-hidden px-1"
              onTouchStart={handleTouchStart}
              onTouchEnd={handleTouchEnd}
            >
              {renderGrid()}
            </div>

            {/* Pagination dots -- always rendered to keep scroll container height stable;
                hidden via visibility when not needed to prevent layout oscillation */}
            <div
              className="flex-shrink-0 py-3"
              style={{
                visibility: (!isSearching && totalPages > 1) ? 'visible' : 'hidden',
                pointerEvents: (!isSearching && totalPages > 1) ? 'auto' : 'none',
              }}
            >
              <MorphingPageDots
                totalPages={totalPages}
                activeIndex={currentPage}
                onChange={goToPage}
              />
            </div>
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
