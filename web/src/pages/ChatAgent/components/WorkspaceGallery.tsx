import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { Search, ArrowDownUp, GripVertical } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import type { Computer } from '@/types/api';
import CreateWorkspaceModal from './CreateWorkspaceModal';
import ComputersDialog from './ComputersDialog';
import RenameWorkspaceDialog from './RenameWorkspaceDialog';
import MorphingPageDots from '../../../components/ui/morphing-page-dots';
import { useWorkspaces } from '../../../hooks/useWorkspaces';
import { queryKeys } from '../../../lib/queryKeys';
import {
  createWorkspace,
  getFlashWorkspace,
  renameWorkspace,
} from '../utils/api';
import { useWorkspaceActions } from './workspaceActions';
import { isEffectivelyPinned } from '../hooks/useNavigationData';
import { pinWorkspaceRow } from '../hooks/workspaceRowActions';
import { clearChatSession } from '../hooks/utils/chatSessionRestore';
import { useWorkspaceMutation } from '../hooks/useWorkspaceMutation';
import { useComputers } from '../hooks/useComputers';
import { GalleryActions } from './workspaceGallery/GalleryActions';
import { GalleryEmptyState } from './workspaceGallery/GalleryEmptyState';
import { ReorderList } from './workspaceGallery/ReorderList';
import { WorkspaceCard } from './workspaceGallery/WorkspaceCard';
import { useGalleryPaging } from './workspaceGallery/useGalleryPaging';
import type { WorkspaceRecord } from './workspaceGallery/types';

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
  const [isComputersOpen, setIsComputersOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  // Default to the manual ('custom') order so the gallery matches the in-chat
  // nav panel, which always shows the user's drag order. With no manual reorder
  // the server's custom sort falls back to updated_at DESC, so this looks
  // identical to 'activity' until the user actually reorders. Activity/Name
  // remain available via the Sort-by toggle.
  const [sortBy, setSortBy] = useState<'activity' | 'name' | 'custom'>('custom');
  const [isReorderMode, setIsReorderMode] = useState(false);
  // Rename is the gallery's own dialog (the tree renames inline); change-spec,
  // always-on, duplicate and delete all come from useWorkspaceActions below.
  const [renameTarget, setRenameTarget] = useState<WorkspaceRecord | null>(null);
  const { workspaceId: currentWorkspaceId } = useParams();
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const preSortByRef = useRef(sortBy); // sort mode before entering reorder
  const isSearching = debouncedSearch.length > 0;

  // Flash workspace query (idempotent POST -- creates if not exists)
  const { data: flashWs } = useQuery({
    queryKey: queryKeys.workspaces.flash(),
    queryFn: getFlashWorkspace,
    staleTime: 5 * 60_000,
  });

  // Paging owns the page size, the page index and the window the list query
  // reads. It holds no server state, so the page count is derived below from
  // the total the query returns.
  const paging = useGalleryPaging({ isSearching });

  // Main workspace list query
  const {
    data: wsData,
    isLoading: isWsLoading,
    error: wsError,
  } = useWorkspaces({
    limit: paging.limit,
    offset: paging.offset,
    sortBy,
    enabled: !isReorderMode,
  });

  // One slot on page 0 belongs to the flash card, hence the +1.
  const totalPages = Math.ceil(((wsData?.total ?? 0) + 1) / paging.pageSize);

  // The machines behind the cards. The status fan-out itself is mounted at the
  // page root (ChatAgent), because the sandbox panel can start a machine too.
  const { data: computerData } = useComputers();
  const computersById = useMemo(() => {
    const byId = new Map<string, Computer>();
    for (const c of computerData?.computers ?? []) byId.set(c.computer_id, c);
    return byId;
  }, [computerData]);

  // Derive workspace list from query data
  const workspaces = useMemo((): WorkspaceRecord[] => {
    const list = (wsData?.workspaces || []) as WorkspaceRecord[];
    // Prepend flash workspace on first page when not searching
    if (flashWs && paging.isFirstPage && !isSearching) {
      return [flashWs as WorkspaceRecord, ...list];
    }
    return list;
  }, [wsData, flashWs, paging.isFirstPage, isSearching]);

  // Clear saved chat session so tab-switching returns to workspace gallery
  useEffect(() => {
    clearChatSession();
  }, []);

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
      onAfterPin: paging.snapToFirstPage,
    });
  };

  // Rename is the gallery's own flow (dialog + optimistic patch); the tree
  // renames inline through the same row action.
  const renameMutation = useWorkspaceMutation<string>({
    mutationFn: (wsId, name) => renameWorkspace(wsId, name),
    optimisticPatch: (name) => ({ name }),
    errorTitleKey: 'workspace.renameFailed',
  });

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
    onAfterMutate: (op) => { if (op === 'duplicate') paging.snapToFirstPage(); },
    onAfterDelete: (wsId) => {
      const remainingOnPage = workspaces.filter((ws) => ws.workspace_id !== wsId).length;
      if (remainingOnPage === 0) paging.stepBackPage();
    },
  });

  /** Enter reorder mode: the list takes over from the paginated grid. */
  const enterReorderMode = () => {
    preSortByRef.current = sortBy;
    setIsReorderMode(true);
  };

  /** Leave reorder mode; a drag that landed pins the manual order. */
  const exitReorderMode = (didReorder: boolean) => {
    setIsReorderMode(false);
    setSortBy(didReorder ? 'custom' : preSortByRef.current);
    paging.resetToFirstPage();
    // Invalidate so paginated view refetches with correct sort order
    queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
  };

  // Server handles sort order; the client only filters by search and enforces
  // the ordering rule shared with the nav tree: the pinned block stays above
  // unpinned rows, and Flash counts as always-pinned (it isn't guaranteed to
  // carry is_pinned in the DB). No flash-first hoist — within the pinned
  // block Flash competes on the server sort like any pinned workspace.
  const visibleWorkspaces = workspaces
    .filter((workspace) =>
      workspace.name.toLowerCase().includes(searchQuery.toLowerCase())
    )
    .sort((a, b) => {
      const aPinned = isEffectivelyPinned(a) ? 1 : 0;
      const bPinned = isEffectivelyPinned(b) ? 1 : 0;
      return bPinned - aPinned; // stable sort: server order preserved within blocks
    });

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
    const skipAnim = paging.takeSkipInitialAnim();
    return (
    <AnimatePresence mode="wait" custom={paging.slideDirectionRef.current}>
    {visibleWorkspaces.length === 0 ? (
      <motion.div
        key="empty"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.15 }}
        className="flex flex-col items-center justify-center py-16"
      >
        <GalleryEmptyState
          isFiltered={!!searchQuery}
          onNewWorkspace={() => setIsModalOpen(true)}
        />
      </motion.div>
    ) : (
      <div
        style={{ height: paging.gridHeightRef.current || undefined, overflow: 'hidden' }}
        ref={(el) => {
          if (el && visibleWorkspaces.length >= paging.pageSize) {
            const h = el.scrollHeight;
            if (!paging.gridHeightRef.current || h > paging.gridHeightRef.current) {
              paging.gridHeightRef.current = h;
              el.style.height = h + 'px';
            }
          }
        }}
      >
        <motion.div
          key={`page-${paging.currentPage}`}
          custom={paging.slideDirectionRef.current}
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
              computer={workspace.computer_id ? computersById.get(workspace.computer_id) : null}
              index={index}
              onSelect={onWorkspaceSelect}
              onTogglePin={handleTogglePin}
              onRenameStart={setRenameTarget}
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
            <GalleryActions
              onNewWorkspace={() => setIsModalOpen(true)}
              onOpenComputers={() => setIsComputersOpen(true)}
            />
          )}
        </div>
      </header>

      {/* Main Content */}
      <main className="mx-auto mt-4 w-full flex-1 min-h-0 px-4 md:px-8 lg:mt-6 max-w-4xl flex flex-col pb-0">
        {/* On a phone the two labeled actions do not fit beside the title,
            so they take the row beneath it. */}
        <div className="flex flex-col gap-3 mb-4 md:hidden">
          <h1 className="text-xl font-semibold title-font" style={{ color: 'var(--color-text-primary)' }}>
            {t('workspace.workspaces')}
          </h1>
          {hasWorkspaces && (
            <GalleryActions
              stacked
              onNewWorkspace={() => setIsModalOpen(true)}
              onOpenComputers={() => setIsComputersOpen(true)}
            />
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
              className="rings-within owns-its-edge flex items-center gap-2 h-11 px-3 rounded-xl border transition-colors"
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
                  paging.resetToFirstPage();
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
          <ReorderList
            flashWorkspace={(flashWs as WorkspaceRecord) ?? null}
            onDone={exitReorderMode}
          />
        ) : (
          /* -- Normal Mode: paginated grid -- */
          <>
            <div
              ref={paging.setScrollContainer}
              className="flex-1 min-h-0 overflow-hidden px-1"
              {...paging.swipeHandlers(totalPages)}
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
                activeIndex={paging.currentPage}
                onChange={paging.goToPage}
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

      {/* Computers: the machines the cards live on. It reads its own list, so
          the count it shows is the machine's, not this page's. */}
      <ComputersDialog open={isComputersOpen} onOpenChange={setIsComputersOpen} />

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
