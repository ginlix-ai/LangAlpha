import { useState, useEffect, useMemo, useRef } from 'react';
import { SwapCell } from '@/components/ui/crossfade';
import { useLastShape } from '@/lib/lastShape';
import { useParams } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import type { Computer } from '@/types/api';
import CreateWorkspaceModal from './CreateWorkspaceModal';
import { DiskWarning, useDiskAlertComputer } from './DiskWarning';
import RenameWorkspaceDialog from './RenameWorkspaceDialog';
import { useAllWorkspaces } from '../../../hooks/useAllWorkspaces';
import { queryKeys } from '../../../lib/queryKeys';
import { renameWorkspace } from '../utils/api';
import { useWorkspaceActions } from './workspaceActions';
import { isEffectivelyPinned } from '../hooks/useNavigationData';
import { pinWorkspaceRow } from '../hooks/workspaceRowActions';
import { useCreateWorkspace } from '../hooks/useCreateWorkspace';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { useNow } from '@/hooks/useNow';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import { safeLocalStorage } from '@/lib/utils';
import { clearChatSession } from '../hooks/utils/chatSessionRestore';
import { useWorkspaceMutation } from '../hooks/useWorkspaceMutation';
import { useComputers } from '../hooks/useComputers';
import { useThisMac } from '../hooks/useRemoteControl';
import { openComputersPanel } from '../hooks/computerPanelStore';
import { liveComputers } from '../utils/computerStatus';
import { GalleryActions } from './workspaceGallery/GalleryActions';
import { GalleryEmptyState } from './workspaceGallery/GalleryEmptyState';
import { Kicker } from './workspaceGallery/Kicker';
import {
  ALL_COMPUTERS,
  ComputerControl,
  GALLERY_SORTS,
  GalleryToolbar,
  type ComputerControlProps,
  type GallerySort,
} from './workspaceGallery/GalleryToolbar';
import { ChiefOfStaffCard } from './workspaceGallery/ChiefOfStaffCard';
import { CardGrid } from './workspaceGallery/CardGrid';
import { GALLERY_COLUMN, GALLERY_FRAME, GALLERY_GRID, GALLERY_PAGE } from './workspaceGallery/layout';
import { useGalleryReorder } from './workspaceGallery/useGalleryReorder';
import { WorkspaceCard, WorkspaceCardSkeleton } from './workspaceGallery/WorkspaceCard';
import type { WorkspaceRecord } from './workspaceGallery/types';

/**
 * The order the user last picked, a per-browser convenience. Nothing stored,
 * or a value this build does not know, reads as Recent; blocked storage just
 * forgets on reload.
 */
const SORT_KEY = 'workspaceGallery.sort';

/** Placeholder cards while the list is read, before this device has seen it:
 *  two rows at a laptop's width. After that, as many as it last held. */
const SKELETON_CARDS = 6;
const MAX_SKELETON_CARDS = 12;

const readSort = (): GallerySort => {
  const stored = safeLocalStorage.getItem(SORT_KEY);
  return GALLERY_SORTS.find((sort) => sort === stored) ?? 'activity';
};

/**
 * WorkspaceGallery Component
 *
 * The Chief of Staff's card, then every workspace as a card in one scrolled
 * grid, each saying which machine it lives on and whether that machine is up.
 */

interface WorkspaceGalleryProps {
  onWorkspaceSelect: (wsId: string, name?: string, status?: string) => void;
  prefetchThreads?: (wsId: string) => void;
}

function WorkspaceGallery({ onWorkspaceSelect, prefetchThreads }: WorkspaceGalleryProps) {
  const { t } = useTranslation();
  const label = useWorkspaceLabel();
  const queryClient = useQueryClient();
  const now = useNow();
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [computerFilter, setComputerFilter] = useState<string>(ALL_COMPUTERS);
  // Most recent first until the user picks another order, and then theirs on
  // every visit: each card says when it was last used, the time Recent sorts
  // on. Custom is the order the cards are dragged into.
  const [sortBy, setSortBy] = useState<GallerySort>(readSort);
  // Rename is the gallery's own dialog (the tree renames inline); duplicate,
  // move and delete come from useWorkspaceActions below. Spec is the machine's
  // and lives in the Computers dialog.
  const [renameTarget, setRenameTarget] = useState<WorkspaceRecord | null>(null);
  const { workspaceId: currentWorkspaceId } = useParams();
  // The page scrolls as one below a laptop's width, and only the grid above it.
  const pageRef = useRef<HTMLDivElement | null>(null);
  const scrollRef = useRef<HTMLDivElement | null>(null);

  // Home's query (idempotent POST, creates it if missing). The Chief of Staff's card
  // shows the whole record, not just its id, so here it goes stale after five
  // minutes rather than never.
  const { data: homeWs, isLoading: isHomeLoading } = useQuery({
    ...homeWorkspaceQuery(queryClient),
    staleTime: 5 * 60_000,
  });

  // Every workspace, one scrolled list: search and the computer filter both
  // narrow it here, so neither needs a query of its own. Read beside Home, not
  // after it: the list only reads, and when the first Home read is the one
  // that creates it, the list has no row for it yet and the Home query's row
  // stands in (homeRow below).
  const {
    data: wsData,
    isLoading: isWsLoading,
    error: wsError,
    isFetching: isWsFetching,
    isPlaceholderData,
  } = useAllWorkspaces({ sortBy, includeHome: true });
  const reorder = useGalleryReorder();

  // The machines behind the cards. The status fan-out itself is mounted at the
  // page root (ChatAgent), because the sandbox panel can start a machine too.
  const { data: computerData } = useComputers();
  const computers = useMemo(() => liveComputers(computerData?.computers), [computerData]);
  const computersById = useMemo(() => {
    const byId = new Map<string, Computer>();
    for (const c of computerData?.computers ?? []) byId.set(c.computer_id, c);
    return byId;
  }, [computerData]);
  // The machine whose disk is worst, if any is past the warning line. One
  // banner, not one per machine: almost every user has a single computer.
  const alertComputer = useDiskAlertComputer(computerData?.computers);
  // Which row is the Mac this desktop app runs on; null everywhere else.
  const thisMac = useThisMac({ enabled: true });
  const isThisMac = (computer: Computer) => thisMac.of(computer) !== null;

  // A filter on a machine that has since gone, or with only one left, is no filter.
  const filterHolds = computers.length > 1 && computers.some((c) => c.computer_id === computerFilter);
  const activeFilter = filterHolds ? computerFilter : ALL_COMPUTERS;
  // And it is dropped once the list says so, rather than coming back if the
  // machine count grows again. An empty list is one not read yet, not an answer.
  if (computerFilter !== ALL_COMPUTERS && !filterHolds && computers.length > 0) {
    setComputerFilter(ALL_COMPUTERS);
  }

  // Clear saved chat session so tab-switching returns to workspace gallery
  useEffect(() => {
    clearChatSession();
  }, []);

  const scrollToTop = () => {
    for (const el of [pageRef.current, scrollRef.current]) el?.scrollTo?.({ top: 0, behavior: 'smooth' });
  };

  const createWorkspace = useCreateWorkspace();

  /** Switch the order and remember it for the next visit. */
  const pickSort = (sort: GallerySort) => {
    setSortBy(sort);
    safeLocalStorage.setItem(SORT_KEY, sort);
  };

  /**
   * Pin/unpin from the card menu. The canonical row action owns the
   * refetch-then-unfreeze sequence (shared with the nav tree, so an unpin here
   * releases the sidebar's session freeze too); the gallery only adds its own
   * presentation reaction: pinning re-sorts the list, so show its top.
   */
  const handleTogglePin = (workspace: WorkspaceRecord) => {
    void pinWorkspaceRow(queryClient, workspace.workspace_id, !workspace.is_pinned, {
      onAfterPin: scrollToTop,
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

  // Duplicate / move / delete: one implementation, shared with the nav tree.
  // A duplicate lands at the top, so the gallery shows the top.
  const wsActions = useWorkspaceActions({
    currentWorkspaceId,
    onAfterMutate: (op) => { if (op === 'duplicate') scrollToTop(); },
  });

  const rows = (wsData?.workspaces ?? []) as WorkspaceRecord[];
  // Home is the Chief of Staff's card, never one in the grid. The list carries
  // it when asked to; the Home query stands in until the list does.
  const homeRow = rows.find((ws) => isHomeWorkspace(ws)) ?? (homeWs as WorkspaceRecord | null | undefined) ?? null;
  const userRows = rows.filter((ws) => !isHomeWorkspace(ws));
  const hasUserWorkspaces = userRows.length > 0;
  // The list's shape last time here: that many placeholders, and a list that
  // was empty is drawn empty from the first frame rather than as a grid with a
  // toolbar that both go when the list arrives. Only a list that arrived
  // counts: a failed read is not an empty one.
  const lastCount = useLastShape('gallery.workspaces', wsData ? userRows.length : null);
  const showSkeleton = isWsLoading && lastCount !== 0;

  // Server handles sort order; the client only narrows by search and machine
  // and enforces the ordering rule shared with the nav tree: the pinned block
  // stays above unpinned rows. Matched on the name the card shows.
  const query = searchQuery.trim().toLowerCase();
  const matches = (ws: WorkspaceRecord) => !query || label(ws).toLowerCase().includes(query);
  const visibleWorkspaces = userRows
    .filter((ws) => matches(ws) && (activeFilter === ALL_COMPUTERS || ws.computer_id === activeFilter))
    .sort((a, b) => (isEffectivelyPinned(b) ? 1 : 0) - (isEffectivelyPinned(a) ? 1 : 0));
  // A search is for workspaces: the Chief of Staff steps aside while one runs,
  // rather than stand over the results as if it were one. With no workspaces
  // left there is no search box to clear a query, so it comes back.
  // While Home is read the card stands without it rather than arrive later
  // and push the grid down.
  const showChief = (!!homeRow || isHomeLoading) && (!query || !hasUserWorkspaces);
  const primary = computers.find((c) => c.is_primary) ?? null;
  // The machine Home is on: the primary, except while a make-primary is
  // still moving it there.
  const homeComputer = homeRow?.computer_id ? computersById.get(homeRow.computer_id) ?? null : primary;
  // A card names its machine only when it is not on the usual one: the
  // primary computer, or the one holding the most workspaces when the primary
  // holds none. Most cards then carry no line and the few that live elsewhere
  // stand out, where a line on every card would repeat one name down the grid.
  // Read off the whole list, so a search does not move it; a filter down to
  // one machine names it in the toolbar, so no card repeats it either.
  const held = new Map<string, number>();
  for (const ws of userRows) if (ws.computer_id) held.set(ws.computer_id, (held.get(ws.computer_id) ?? 0) + 1);
  // Ties go to the lower id, so a sort or new activity never moves it.
  const usualComputerId = primary && held.has(primary.computer_id)
    ? primary.computer_id
    : [...held].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1))[0]?.[0];
  const machineOf = (ws: WorkspaceRecord) =>
    activeFilter === ALL_COMPUTERS && ws.computer_id && ws.computer_id !== usualComputerId
      ? computersById.get(ws.computer_id) ?? null
      : null;
  const computerControl: ComputerControlProps | null = computerData
    ? {
        computers,
        isThisMac,
        computerFilter: activeFilter,
        onComputerFilterChange: setComputerFilter,
        onManageComputers: (computerId) => openComputersPanel({ view: 'list', computerId }),
        onNewComputer: () => openComputersPanel({ view: 'create' }),
      }
    : null;
  // Cards drag in the Custom order over the whole list: a search or a filter
  // leaves cards out, and an order read off part of the list has no place for
  // them. And they hold still while the list is read again, the last order's
  // list standing in for this one's included: a drop then would move from an
  // order the read is about to replace (a reorder just made in the sidebar).
  const canDrag = sortBy === 'custom' && !query && activeFilter === ALL_COMPUTERS;
  const refreshing = sortBy === 'custom' && (isWsFetching || isPlaceholderData);

  // A failed refetch keeps the list it had: the full-screen error is only for
  // a gallery with nothing to show.
  if (wsError && !wsData) {
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

  const renderList = () => {
    // A user with nothing but Home is welcomed, not shown a search box.
    if (!hasUserWorkspaces) {
      return (
        <GalleryEmptyState isFiltered={false} onNewWorkspace={() => setIsModalOpen(true)} />
      );
    }
    if (visibleWorkspaces.length === 0) {
      return (
        <div className="py-16">
          <GalleryEmptyState
            isFiltered
            query={searchQuery.trim()}
            onClearSearch={() => setSearchQuery('')}
            onNewWorkspace={() => setIsModalOpen(true)}
          />
        </div>
      );
    }
    return (
      <CardGrid
        workspaces={visibleWorkspaces}
        canDrag={canDrag}
        saving={reorder.isSaving}
        refreshing={refreshing}
        onMove={reorder.drop}
        label={label}
        renderCard={(workspace, _index, drag) => {
          const computer = machineOf(workspace);
          return (
            <WorkspaceCard
              workspace={workspace}
              computer={computer}
              isThisMac={!!computer && isThisMac(computer)}
              now={now}
              drag={drag}
              onSelect={onWorkspaceSelect}
              onTogglePin={handleTogglePin}
              onRenameStart={setRenameTarget}
              onOpenComputer={(computerId) => openComputersPanel({ view: 'list', computerId })}
              onDuplicate={wsActions.openDuplicate}
              onMove={wsActions.openMove}
              onDelete={wsActions.openDelete}
              prefetchThreads={prefetchThreads}
            />
          );
        }}
      />
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
      <div ref={pageRef} className={GALLERY_PAGE} data-testid="gallery-page">
        {/* Everything above the grid. From a laptop's width up it holds still
            and clips nothing (its bands are padded clear of their edges); the
            overflow is what lets it reserve the scrollbar's lane, so its
            column lines up with the grid's. */}
        <div className={`shrink-0 md:overflow-hidden ${GALLERY_FRAME}`}>
          {/* Header (desktop only) */}
          <header
            className={`hidden md:flex items-start pb-2 ${GALLERY_COLUMN}`}
            style={{ paddingTop: 'calc(var(--page-top) - 4px)' }}
          >
            <div className="flex w-full items-center justify-between gap-4" style={{ minHeight: 36 }}>
              <h1 className="text-2xl font-semibold title-font" style={{ color: 'var(--color-text-primary)' }}>
                {t('workspace.workspaces')}
              </h1>
              <GalleryActions onNewWorkspace={() => setIsModalOpen(true)} />
            </div>
          </header>

          <div className={`mt-4 ${GALLERY_COLUMN}`}>
            {/* Below a laptop's width the labeled action does not fit beside
                the title, so it takes the row beneath it. */}
            <div className="flex flex-col gap-3 mb-4 md:hidden">
              <h1 className="text-xl font-semibold title-font" style={{ color: 'var(--color-text-primary)' }}>
                {t('workspace.workspaces')}
              </h1>
              <GalleryActions stacked onNewWorkspace={() => setIsModalOpen(true)} />
            </div>

            {alertComputer && (
              <div className="pb-4 enter-fade-up">
                <DiskWarning computer={alertComputer} />
              </div>
            )}

            {/* Drawn while the list is read too, unless it was empty last time
                here: it goes only for a list that turns out to have none. With
                no workspaces only the computer control shows, the page's one
                way to the computers, once the list is read. */}
            {(hasUserWorkspaces || showSkeleton || computerControl) && (
              <div className="pb-3">
                {hasUserWorkspaces || showSkeleton ? (
                  <GalleryToolbar
                    search={searchQuery}
                    onSearchChange={setSearchQuery}
                    computerControl={computerControl}
                    sortBy={sortBy}
                    onSortChange={(sort) => {
                      pickSort(sort);
                      scrollToTop();
                    }}
                  />
                ) : (
                  computerControl && <ComputerControl {...computerControl} />
                )}
              </div>
            )}
          </div>
        </div>

        {/* From a laptop's width up, spans the full width so its scrollbar
            runs down the window's edge; the column is centred inside. Its top
            pad is the room a card rising under the pointer needs, which the
            toolbar's gap gives up, so the first row is not clipped. */}
        <div
          ref={scrollRef}
          className={`pt-1 pb-8 md:flex-1 md:min-h-0 md:overflow-y-auto ${GALLERY_FRAME}`}
          data-testid="workspace-grid"
        >
          <div className={GALLERY_COLUMN}>
            {showChief && (
              <ChiefOfStaffCard
                workspace={homeRow}
                computer={homeComputer}
                isThisMac={!!homeComputer && isThisMac(homeComputer)}
                onOpenComputer={(computerId) => openComputersPanel({ view: 'list', computerId })}
                now={now}
                onOpenThreads={onWorkspaceSelect}
                prefetchThreads={prefetchThreads}
              />
            )}
            {/* Under the Chief of Staff the workspaces take a labeled section of
                their own; alone, under a search, they need no label. */}
            <div className={showChief ? 'mt-6 md:mt-7' : undefined}>
              {showChief && (
                <Kicker className="mb-2.5">
                  {hasUserWorkspaces ? `${t('workspace.workspaces')} · ${visibleWorkspaces.length}` : t('workspace.workspaces')}
                </Kicker>
              )}
              {/* The cards take the placeholders' places in one crossfade. */}
              <SwapCell as="div" swapKey={showSkeleton ? 'loading' : 'list'}>
                {showSkeleton ? (
                  <div className={GALLERY_GRID} aria-busy="true">
                    <span className="sr-only">{t('workspace.loadingWorkspaces')}</span>
                    {Array.from({ length: Math.min(lastCount ?? SKELETON_CARDS, MAX_SKELETON_CARDS) }, (_, i) => <WorkspaceCardSkeleton key={i} index={i} />)}
                  </div>
                ) : (
                  renderList()
                )}
              </SwapCell>
            </div>
          </div>
        </div>
      </div>

      {/* Create Workspace Modal */}
      <CreateWorkspaceModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onCreate={createWorkspace}
        onComplete={(ws) => onWorkspaceSelect(ws.workspace_id, ws.name)}
      />

      {/* Rename Dialog — the gallery's own flow; the rest live in wsActions.dialogs */}
      <RenameWorkspaceDialog
        target={renameTarget}
        onClose={() => setRenameTarget(null)}
        onSubmit={(name) => void handleRenameSubmit(name)}
        busy={renameTarget ? renameMutation.busyIds.has(renameTarget.workspace_id) : false}
      />

      {/* Duplicate / delete confirmations. The Computers dialog mounts at the
          page root (ComputersDialogHost). */}
      {wsActions.dialogs}
    </div>
  );
}

export default WorkspaceGallery;
