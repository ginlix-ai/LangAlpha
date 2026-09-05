import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { Plus, Search, ArrowDownUp, MoreHorizontal, Zap, MessageSquareText, Pin, GripVertical, Check, Cpu, Infinity as InfinityIcon } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useNavigate, useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { DndContext, closestCenter, PointerSensor, useSensor, useSensors } from '@dnd-kit/core';
import type { DragEndEvent } from '@dnd-kit/core';
import { SortableContext, useSortable, arrayMove, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import type { Workspace } from '@/types/api';
import CreateWorkspaceModal from './CreateWorkspaceModal';
import { normalizeTier, tierLabel } from './ChangeSpecDialog';
import RenameWorkspaceDialog from './RenameWorkspaceDialog';
import MorphingPageDots from '../../../components/ui/morphing-page-dots';
import { useIsMobile, getIsMobileSnapshot } from '@/hooks/useIsMobile';
import { useWorkspaces } from '../../../hooks/useWorkspaces';
import { queryKeys } from '../../../lib/queryKeys';
import {
  createWorkspace,
  getFlashWorkspace,
  reorderWorkspaces,
  renameWorkspace,
} from '../utils/api';
import { WorkspaceMenuItems, useWorkspaceActions } from './workspaceActions';
import { isEffectivelyPinned } from '../hooks/useNavigationData';
import { pinWorkspaceRow } from '../hooks/workspaceRowActions';
import { clearChatSession } from '../hooks/utils/chatSessionRestore';
import { useWorkspaceMutation } from '../hooks/useWorkspaceMutation';

const DEFAULT_PAGE_SIZE = 8;

// Gallery view of a workspace: the shared DTO plus manual-order metadata.
interface WorkspaceRecord extends Workspace {
  is_pinned?: boolean;
  sort_order?: number;
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
  onRename: (workspace: WorkspaceRecord) => void;
  onUpgrade: (workspace: WorkspaceRecord) => void;
  onToggleAlwaysOn: (workspace: WorkspaceRecord) => void;
  onDuplicate: (workspace: WorkspaceRecord) => void;
  onDelete: (workspace: WorkspaceRecord) => void;
}

function CardMenu({ workspace, onTogglePin, onRename, onUpgrade, onToggleAlwaysOn, onDuplicate, onDelete }: CardMenuProps) {
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
        <WorkspaceMenuItems
          workspace={workspace}
          onTogglePin={onTogglePin}
          onRename={onRename}
          onUpgrade={onUpgrade}
          onToggleAlwaysOn={onToggleAlwaysOn}
          onDuplicate={onDuplicate}
          onDelete={onDelete}
        />
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/**
 * Sortable row for reorder mode -- compact single-column list item
 */
interface SortableReorderRowProps {
  workspace: WorkspaceRecord;
  disabled: boolean | { draggable: boolean; droppable: boolean };
}

function SortableReorderRow({ workspace, disabled }: SortableReorderRowProps) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({
    id: workspace.workspace_id,
    // dnd-kit back-compat trap: a boolean `disabled` normalizes to
    // {draggable, droppable: false} — the row would stay an active drop
    // target. Spell out both aspects so `true` really means fully disabled.
    disabled: typeof disabled === 'boolean' ? { draggable: disabled, droppable: disabled } : disabled,
  });

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
        // Same material split as the gallery card: flash rides an elevated
        // surface, user rows keep the card wash; the Zap glyph is the accent.
        background: isFlash
          ? 'var(--color-bg-elevated)'
          : 'var(--color-bg-card-gradient, var(--color-border-muted))',
        borderColor: isFlash ? 'var(--color-border-default)' : 'var(--color-bg-card-border, var(--color-border-muted))',
      }}
    >
      {/* Flash drags too (within the pinned block) — its Zap identity glyph
          doubles as the grab handle where user rows show the grip. */}
      <button
        {...listeners}
        {...attributes}
        className="flex-shrink-0 cursor-grab active:cursor-grabbing p-1 rounded"
        style={{ color: isFlash ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)' }}
      >
        {isFlash ? <Zap className="h-5 w-5" /> : <GripVertical className="h-5 w-5" />}
      </button>
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
  onRenameStart: (workspace: WorkspaceRecord) => void;
  onUpgrade: (workspace: WorkspaceRecord) => void;
  onToggleAlwaysOn: (workspace: WorkspaceRecord) => void;
  onDuplicate: (workspace: WorkspaceRecord) => void;
  onDelete: (workspace: WorkspaceRecord) => void;
  prefetchThreads?: (wsId: string) => void;
  index?: number;
}

function WorkspaceCard({ workspace, onSelect, onTogglePin, onRenameStart, onUpgrade, onToggleAlwaysOn, onDuplicate, onDelete, prefetchThreads, index }: WorkspaceCardProps) {
  const { t, i18n } = useTranslation();
  const isMobile = useIsMobile();
  const isFlash = workspace.status === 'flash';

  const tier = normalizeTier(workspace.resource_tier);
  const showTierBadge = !isFlash && tier !== 'standard';
  const showAlwaysOn = !isFlash && workspace.is_always_on === true;

  return (
    <div
      className="h-40 enter-fade-up"
      style={{ animationDelay: `${(index || 0) * 50}ms` }}
    >
      <div
        className="relative group h-full"
        data-testid="workspace-card"
        onMouseEnter={!isMobile ? () => prefetchThreads?.(workspace.workspace_id) : undefined}
      >
        <div
          onClick={() => onSelect(workspace.workspace_id, workspace.name, workspace.status)}
          className="relative flex cursor-pointer flex-col overflow-hidden rounded-xl py-4 pl-5 pr-4 transition-all ease-in-out hover:shadow-sm active:scale-[0.98] h-full w-full"
          style={{
            // Flash is a system card: flat elevated surface + crisp hairline,
            // a different material from user cards; the amber Zap glyph is the
            // only accent.
            background: isFlash
              ? 'var(--color-bg-elevated)'
              : 'var(--color-bg-card-gradient, linear-gradient(to bottom, var(--color-border-muted), var(--color-border-muted)))',
            border: isFlash
              ? '1px solid var(--color-border-default)'
              : '1px solid var(--color-bg-card-border, var(--color-border-muted))',
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
            <div className="text-xs mt-auto pt-3 flex items-center justify-between gap-2" style={{ color: 'var(--color-text-tertiary)' }}>
              <span className="truncate">
                {t('workspace.updated', { time: workspace.updated_at ? new Date(workspace.updated_at).toLocaleDateString(i18n.language, { month: 'short', day: 'numeric' }) : t('workspace.recently') })}
              </span>
              {(showTierBadge || showAlwaysOn) && (
                <div className="flex items-center gap-1.5 flex-shrink-0">
                  {showTierBadge && (
                    <span
                      className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[0.625rem] font-medium"
                      style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
                      title={t('workspace.tierBadgeTitle', { tier: tierLabel(t, tier) })}
                    >
                      <Cpu className="h-3 w-3" />
                      {tierLabel(t, tier)}
                    </span>
                  )}
                  {showAlwaysOn && (
                    <span
                      className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[0.625rem] font-medium"
                      style={{ backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
                      title={t('workspace.alwaysOnBadgeTitle', 'Always-on — sandbox stays running')}
                    >
                      <InfinityIcon className="h-3 w-3" />
                      {t('workspace.alwaysOnBadge', 'Always-on')}
                    </span>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Menu (no drag handle in normal mode) */}
        {!isFlash && (
          <div className={`absolute top-3 right-3 z-10 transition-opacity ${isMobile ? 'opacity-60' : 'opacity-0 group-focus-within:opacity-100 group-hover:opacity-100'}`}>
            <CardMenu
              workspace={workspace}
              onTogglePin={onTogglePin}
              onRename={onRenameStart}
              onUpgrade={onUpgrade}
              onToggleAlwaysOn={onToggleAlwaysOn}
              onDuplicate={onDuplicate}
              onDelete={onDelete}
            />
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
  const [searchQuery, setSearchQuery] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  // Default to the manual ('custom') order so the gallery matches the in-chat
  // nav panel, which always shows the user's drag order. With no manual reorder
  // the server's custom sort falls back to updated_at DESC, so this looks
  // identical to 'activity' until the user actually reorders. Activity/Name
  // remain available via the Sort-by toggle.
  const [sortBy, setSortBy] = useState<'activity' | 'name' | 'custom'>('custom');
  const [currentPage, setCurrentPage] = useState(0);
  const [isReorderMode, setIsReorderMode] = useState(false);
  // Lifted row during a reorder drag — rows across the pin boundary from it
  // stop being drop targets so the preview never shows a refused arrangement.
  const [reorderActiveId, setReorderActiveId] = useState<string | null>(null);
  const [allWorkspaces, setAllWorkspaces] = useState<WorkspaceRecord[]>([]);
  // Rename is the gallery's own dialog (the tree renames inline); change-spec,
  // always-on, duplicate and delete all come from useWorkspaceActions below.
  const [renameTarget, setRenameTarget] = useState<WorkspaceRecord | null>(null);
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
    const list = wsData?.workspaces || [];
    // Prepend flash workspace on first page when not searching
    if (flashWs && isFirstPage && !isSearching) {
      return [flashWs as WorkspaceRecord, ...list];
    }
    return list;
  }, [wsData, flashWs, isFirstPage, isSearching]);

  const totalWorkspaces = wsData?.total || 0;
  const totalPages = Math.ceil((totalWorkspaces + 1) / pageSize);

  // Sync allWorkspaces state from query data when in reorder mode
  useEffect(() => {
    if (isReorderMode && allWsData?.workspaces) {
      const list = allWsData.workspaces;
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

  // Reveal a mutation whose result lands at the top of the list (pin, duplicate):
  // slide backwards to page 0 and release the locked grid height.
  const snapToFirstPage = useCallback(() => {
    slideDirectionRef.current = -1;
    gridHeightRef.current = null;
    setCurrentPage(0);
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
   * Pin/unpin from the card menu. The canonical row action owns the
   * refetch-then-unfreeze sequence (shared with the nav tree, so an unpin here
   * releases the sidebar's session freeze too); the gallery only adds its own
   * presentation reaction — pinning re-sorts the list, so snap back to page 0.
   */
  const handleTogglePin = (workspace: WorkspaceRecord) => {
    void pinWorkspaceRow(queryClient, workspace.workspace_id, !workspace.is_pinned, {
      onAfterPin: snapToFirstPage,
    });
  };

  // Rename is the gallery's own flow (dialog + optimistic patch); the tree
  // renames inline through the same row action.
  const renameMutation = useWorkspaceMutation<string>({
    mutationFn: (wsId, name) => renameWorkspace(wsId, name),
    optimisticPatch: (name) => ({ name }),
    errorTitleKey: 'workspace.renameFailed',
  });

  /** Open the rename dialog (the card menu's Rename action). */
  const handleRenameStart = (workspace: WorkspaceRecord) => {
    setRenameTarget(workspace);
  };

  /** Commit the rename dialog; close on success. */
  const handleRenameSubmit = async (name: string) => {
    if (!renameTarget) return;
    const trimmed = name.trim();
    if (!trimmed || trimmed === renameTarget.name) {
      setRenameTarget(null);
      return;
    }
    const ok = await renameMutation.run(renameTarget.workspace_id, trimmed);
    if (ok) setRenameTarget(null);
  };

  // Change-spec / always-on / duplicate / delete — one implementation, shared
  // with the nav tree. Only the paging reactions are gallery-specific: a
  // duplicate lands at the top, and a delete that empties the page steps back.
  const wsActions = useWorkspaceActions({
    currentWorkspaceId,
    onAfterMutate: (op) => { if (op === 'duplicate') snapToFirstPage(); },
    onAfterDelete: (wsId) => {
      const remainingOnPage = workspaces.filter((ws) => ws.workspace_id !== wsId).length;
      if (remainingOnPage === 0 && currentPage > 0) {
        slideDirectionRef.current = -1;
        setCurrentPage((p) => p - 1);
      }
    },
  });

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
    setReorderActiveId(null);
    const { active, over } = event;
    if (!over || active.id === over.id) return;

    const sorted = reorderSortedList;
    const oldIndex = sorted.findIndex((ws) => ws.workspace_id === active.id);
    const newIndex = sorted.findIndex((ws) => ws.workspace_id === over.id);
    if (oldIndex === -1 || newIndex === -1) return;

    const draggedWs = sorted[oldIndex];
    const targetWs = sorted[newIndex];

    // Prevent crossing the pin boundary. No flash special case: it counts as
    // pinned, so this both contains it in the pinned block and keeps
    // unpinned rows out.
    if (isEffectivelyPinned(draggedWs) !== isEffectivelyPinned(targetWs)) return;

    const reordered = arrayMove(sorted, oldIndex, newIndex);

    // Assign sequential sort_order. Flash is included: it's DB-pinned with a
    // real sort_order, and writing its slot is what makes "pinned workspace
    // above/below Flash" stick — omitting it leaves a sort_order tie decided
    // by updated_at, so the pinned block would reshuffle whenever Flash is used.
    const items: { workspace_id: string; sort_order: number }[] = [];
    reordered.forEach((ws, i) => {
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
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
    } catch (err) {
      console.error('Error reordering workspaces:', err);
      setAllWorkspaces(snapshot); // rollback
    }
  };

  /**
   * Filter and sort workspaces
   */
  // Server handles sort order; the client only filters by search and enforces
  // the ordering rule shared with the nav tree: the pinned block stays above
  // unpinned rows, and Flash counts as always-pinned (it isn't guaranteed to
  // carry is_pinned in the DB). No flash-first hoist — within the pinned
  // block Flash competes on the server sort like any pinned workspace.
  const filteredAndSortedWorkspaces = workspaces
    .filter((workspace) =>
      workspace.name.toLowerCase().includes(searchQuery.toLowerCase())
    )
    .sort((a, b) => {
      const aPinned = isEffectivelyPinned(a) ? 1 : 0;
      const bPinned = isEffectivelyPinned(b) ? 1 : 0;
      return bPinned - aPinned; // stable sort: server order preserved within blocks
    });

  const visibleWorkspaces = filteredAndSortedWorkspaces;

  // Sorted list for reorder mode (pinned block first — Flash counts as
  // always-pinned — then sort_order, then recency). Sort keys are computed
  // once per item, not per comparison, and the whole sort re-runs only when
  // the list changes — not on every drag-position render.
  const reorderSortedList = useMemo(() => {
    const keyed = allWorkspaces.map((ws) => ({
      ws,
      pinned: isEffectivelyPinned(ws) ? 1 : 0,
      order: ws.sort_order ?? 0,
      updated: new Date(ws.updated_at || 0).getTime(),
    }));
    keyed.sort((a, b) => {
      if (a.pinned !== b.pinned) return b.pinned - a.pinned;
      if (a.order !== b.order) return a.order - b.order;
      return b.updated - a.updated;
    });
    return keyed.map((k) => k.ws);
  }, [allWorkspaces]);
  const reorderSortedIds = reorderSortedList.map((ws) => ws.workspace_id);
  const reorderActiveWs = reorderActiveId
    ? reorderSortedList.find((w) => w.workspace_id === reorderActiveId) ?? null
    : null;

  if (isWsLoading) {
    return (
      // A branch that replaces the whole route replaces its top bar too, so it
      // owes the window a titlebar of its own -- otherwise the column beside the
      // sidebar stops moving the window for as long as the fetch is in flight.
      <div className="h-full flex flex-col">
        <div className="chrome-drag-strip" aria-hidden="true" />
        <div className="flex-1 min-h-0 flex items-center justify-center">
          <div className="flex flex-col items-center gap-4">
            <span aria-hidden="true" className="flex-shrink-0">
              <Loader size={32} className="text-[color:var(--color-accent-primary)]" />
            </span>
            <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('workspace.loadingWorkspaces')}
            </p>
          </div>
        </div>
      </div>
    );
  }

  if (wsError) {
    return (
      <div className="h-full flex flex-col">
        <div className="chrome-drag-strip" aria-hidden="true" />
        <div className="flex-1 min-h-0 flex items-center justify-center">
          <div className="flex flex-col items-center gap-4 max-w-md text-center px-4">
            <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
              {t('workspace.failedLoadWorkspaces')}
            </p>
            <button
              onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() })}
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
                className="flex items-center gap-2 px-6 py-3 rounded-lg transition-all hover:opacity-90 active:scale-[0.985]"
                style={{
                  backgroundColor: 'var(--color-btn-primary-bg)',
                  color: 'var(--color-btn-primary-text)',
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
              onRenameStart={handleRenameStart}
              onUpgrade={wsActions.openUpgrade}
              onToggleAlwaysOn={wsActions.toggleAlwaysOn}
              onDuplicate={wsActions.openDuplicate}
              onDelete={wsActions.openDelete}
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
      style={{ backgroundColor: 'var(--color-bg-page)' }}
    >
      {/* Doubles as the window titlebar in the desktop shell; inert elsewhere.
          The header below is centred and holds a title, so it is not the bar to
          hand the window -- a drag region over prose is text you cannot select. */}
      <div className="chrome-drag-strip" aria-hidden="true" />
      {/* Header (desktop only) */}
      <header className="hidden md:flex w-full h-24 items-end mx-auto max-w-4xl flex-shrink-0 px-8 enter-fade-up">
        <div className="flex w-full items-center justify-between gap-4">
          <h1 className="text-2xl font-semibold title-font" style={{ color: 'var(--color-text-primary)' }}>
            {t('workspace.workspaces')}
          </h1>
          {hasWorkspaces && (
            <button
              onClick={() => setIsModalOpen(true)}
              className="flex items-center gap-1.5 px-4 py-2 h-9 rounded-lg transition-all hover:opacity-90 active:scale-[0.985]"
              style={{
                backgroundColor: 'var(--color-btn-primary-bg)',
                color: 'var(--color-btn-primary-text)',
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
              className="flex items-center gap-1.5 px-4 py-2 h-9 rounded-lg transition-all hover:opacity-90 active:scale-[0.985]"
              style={{
                backgroundColor: 'var(--color-btn-primary-bg)',
                color: 'var(--color-btn-primary-text)',
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
            {/* The pill rings for the field inside it: a ring drawn on the
                field alone would cut this border and leave the icon outside
                the indicator. `rings-within` in tokens.css owns the rule. */}
            <div
              className="rings-within flex items-center gap-2 h-11 px-3 rounded-xl border transition-colors"
              style={{
                backgroundColor: 'var(--color-bg-input)',
                borderColor: 'var(--color-border-muted)',
              }}
            >
              <Search className="h-5 w-5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              <input
                className="w-full bg-transparent text-base sm:text-sm"
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
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-opacity hover:opacity-90"
                style={{
                  backgroundColor: 'var(--color-btn-primary-bg)',
                  color: 'var(--color-btn-primary-text)',
                }}
              >
                <Check className="h-4 w-4" />
                {t('common.done')}
              </button>
            </div>
            <div className="flex-1 min-h-0 overflow-y-auto px-1 pb-4">
              <DndContext
                sensors={sensors}
                collisionDetection={closestCenter}
                onDragStart={(e) => setReorderActiveId(String(e.active.id))}
                onDragCancel={() => setReorderActiveId(null)}
                onDragEnd={handleReorderDragEnd}
              >
                <SortableContext items={reorderSortedIds} strategy={verticalListSortingStrategy}>
                  {reorderSortedList.map((ws) => {
                    const crossBlock = !!reorderActiveWs && ws.workspace_id !== reorderActiveId &&
                      isEffectivelyPinned(ws) !== isEffectivelyPinned(reorderActiveWs);
                    return (
                      <SortableReorderRow
                        key={ws.workspace_id}
                        workspace={ws}
                        disabled={crossBlock ? { draggable: false, droppable: true } : false}
                      />
                    );
                  })}
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

      {/* Rename Dialog — the gallery's own flow; the rest live in wsActions.dialogs */}
      <RenameWorkspaceDialog
        target={renameTarget}
        onClose={() => setRenameTarget(null)}
        onSubmit={(name) => void handleRenameSubmit(name)}
        busy={renameTarget ? renameMutation.busyIds.has(renameTarget.workspace_id) : false}
      />

      {/* Change-spec / always-on / duplicate / delete confirmations */}
      {wsActions.dialogs}
    </div>
  );
}

export default WorkspaceGallery;
