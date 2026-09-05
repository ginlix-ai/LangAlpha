import React, { useState, useCallback, useMemo, useRef, useSyncExternalStore } from 'react';
import { createPortal } from 'react-dom';
import { useTranslation } from 'react-i18next';
import { DndContext, DragOverlay, closestCenter, PointerSensor, MeasuringStrategy, useSensor, useSensors } from '@dnd-kit/core';
import type { DragStartEvent, DragEndEvent, Modifier } from '@dnd-kit/core';
import { SortableContext, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { ChevronsDown } from 'lucide-react';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { useWorkspaceActions } from './workspaceActions';
import { isEffectivelyPinned } from '../hooks/useNavigationData';
import type { NavWorkspace } from '../hooks/useNavigationData';
import { useIsMobile } from '@/hooks/useIsMobile';
import {
  expandedWorkspaces,
  expandedThreads,
  notifyNavExpansion,
  subscribeNavExpansion,
  getNavExpansionVersion,
  toggleWorkspaceExpansion,
  toggleThreadExpansion,
} from './navExpansionStore';
import type { SidebarAgentRow } from '../session/subagents/subagentStatus';
import { WorkspaceTreeRow, WorkspaceDragChip } from './NavigationRows';
import type { ThreadsData } from './NavigationRows';
import { useArchiveThreadConfirm } from './threadArchiveAction';
import './NavigationPanel.css';

interface NavigationPanelProps {
  /** Whether this panel's host ChatView is the visible one. Up to 5 ChatViews
   *  stay mounted (display:none); only the active one should auto-page threads. */
  isActive?: boolean;
  workspaces: NavWorkspace[];
  workspaceThreads: Record<string, ThreadsData>;
  currentWorkspaceId?: string | null;
  currentThreadId?: string | null;
  /** Pre-derived nav rows (see toSidebarAgentRow) — status is already the
   *  display status, so this panel never re-derives it. */
  agents?: SidebarAgentRow[];
  activeAgentId?: string | null;
  expandWorkspace: (wsId: string) => void;
  onSelectAgent: (agentId: string) => void;
  onRemoveAgent?: (agentId: string) => void;
  onNavigateThread: (wsId: string, threadId: string) => void;
  hasMore?: boolean;
  onLoadMore?: () => void;
  /** Fetch the next page of threads for a workspace ("Show more" row). */
  onLoadMoreThreads?: (wsId: string) => void;
  /** Drag-reorder handler: persist `activeId` dropped onto `overId`'s slot. */
  onReorderWorkspace?: (activeId: string, overId: string) => void;
  /** Pin/unpin a workspace to the top of the list (options menu). */
  onPinWorkspace?: (wsId: string, pinned: boolean) => void;
  /** Persist a workspace rename (options menu → inline edit). */
  onRenameWorkspace?: (wsId: string, name: string) => void;
  /** Open a fresh thread in the given workspace. */
  onNewThread?: (wsId: string) => void;
  /** Pin/unpin a thread to the top of its workspace block (hover action). */
  onPinThread?: (wsId: string, threadId: string, pinned: boolean) => void;
  /** Archive a thread — removes it from the sidebar (hover action). */
  onArchiveThread?: (wsId: string, threadId: string) => void;
  /** Right-aligned controls (pin, minimize) rendered in a header row above the list. */
  headerActions?: React.ReactNode;
}

// Cap on how many extra thread pages the active-thread auto-reveal will fetch
// before giving up: covers a genuinely deep thread while bounding a stale or
// deleted id that would otherwise page through the whole workspace.
const MAX_REVEAL_PAGES = 20;

/**
 * NavigationPanel -- hover-triggered overlay sidebar showing
 * Workspace -> Thread -> Agent hierarchy.
 *
 * The rows themselves live in ./NavigationRows; this file owns the tree's
 * state: expansion, inline rename, drag orchestration, and paging.
 *
 * Expansion state lives in ./navExpansionStore (a globalThis-anchored module)
 * so it's shared across every cached panel instance and survives HMR. Keeping
 * it out of this file lets NavigationPanel export only its component, so Fast
 * Refresh hot-swaps cleanly instead of forcing full reloads that could split
 * the module state and make folders appear to auto-collapse on thread switch.
 */
function NavigationPanel({
  isActive = true,
  workspaces,
  workspaceThreads,
  currentWorkspaceId,
  currentThreadId,
  agents,
  activeAgentId,
  expandWorkspace,
  onSelectAgent,
  onRemoveAgent,
  onNavigateThread,
  hasMore,
  onLoadMore,
  onLoadMoreThreads,
  onReorderWorkspace,
  onPinWorkspace,
  onRenameWorkspace,
  onNewThread,
  onPinThread,
  onArchiveThread,
  headerActions,
}: NavigationPanelProps) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  // Change-spec / always-on / duplicate / delete are self-contained (mutations +
  // dialogs) so every host — sidebar tree, mobile drawer — gets the same menu
  // as the gallery card without extra wiring.
  const wsActions = useWorkspaceActions({ currentWorkspaceId });
  // Same reasoning for the thread archive confirm: this panel is the one tree
  // both hosts render, so gating here covers the sidebar and the mobile drawer
  // without either wiring a dialog of its own.
  const { requestArchive, dialog: archiveDialog } = useArchiveThreadConfirm();
  const handleArchiveThread = useCallback((wsId: string, threadId: string) => {
    requestArchive(threadId, () => onArchiveThread?.(wsId, threadId));
  }, [requestArchive, onArchiveThread]);
  // 8px activation distance (same as the gallery's reorder mode) keeps plain
  // clicks toggling expand/collapse instead of starting a drag.
  const dndSensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 8 } }));
  // Id of the workspace currently being dragged — drives the DragOverlay chip.
  const [activeDragId, setActiveDragId] = useState<string | null>(null);
  // Subscribe to the shared expansion store (navExpansionStore). One panel mounts
  // per cached ChatView instance; subscribing re-renders this panel whenever any
  // instance toggles a folder, so cached ChatViews never show stale folder state.
  // The render reads the sets directly (below), so only the subscription is
  // needed, not the returned version value.
  useSyncExternalStore(subscribeNavExpansion, getNavExpansionVersion);

  // Seed the current workspace/thread as expanded, then keep them expanded when
  // they change. A layout effect (not a render-phase mutation) runs before paint,
  // so the folder still renders open on first paint with no flicker — but it goes
  // through notifyNavExpansion, so the useSyncExternalStore version always tracks
  // the Set contents (no torn snapshot across cached panels under React 19).
  React.useLayoutEffect(() => {
    if (currentWorkspaceId) {
      if (!expandedWorkspaces.has(currentWorkspaceId)) {
        expandedWorkspaces.add(currentWorkspaceId);
        notifyNavExpansion();
      }
      expandWorkspace(currentWorkspaceId);
    }
  }, [currentWorkspaceId, expandWorkspace]);

  React.useLayoutEffect(() => {
    if (currentThreadId && currentThreadId !== '__default__' && !expandedThreads.has(currentThreadId)) {
      expandedThreads.add(currentThreadId);
      notifyNavExpansion();
    }
  }, [currentThreadId]);

  // Auto-reveal the active thread when it lives beyond the first page. A thread
  // you open can sit inside the collapsed "Show more" tail (it's older than the
  // first page of recents), which would leave it highlighted-but-hidden. Page in
  // successive batches until it surfaces so the open folder always holds the
  // current thread — re-runs as each page lands, walking the tail. Bounded by
  // MAX_REVEAL_PAGES.
  const revealAttemptRef = useRef<{ key: string; pages: number }>({ key: '', pages: 0 });
  React.useEffect(() => {
    // Only the visible ChatView's panel pages threads. Up to 5 panels stay
    // mounted under display:none; without this each could fire MAX_REVEAL_PAGES
    // fetches for its own (hidden) current thread.
    if (!isActive) return;
    if (!onLoadMoreThreads || !currentWorkspaceId) return;
    if (!currentThreadId || currentThreadId === '__default__') return;
    if (!expandedWorkspaces.has(currentWorkspaceId)) return;
    const currentWs = workspaces.find((ws) => ws.workspace_id === currentWorkspaceId);
    if (!currentWs) return;
    const data = workspaceThreads[currentWorkspaceId];
    if (!data || data.loading) return;
    const loaded = data.threads || [];
    if (loaded.some((thr) => thr.thread_id === currentThreadId)) return; // already visible
    if (typeof data.total !== 'number' || loaded.length >= data.total) return; // nothing more to page

    const key = `${currentWorkspaceId}:${currentThreadId}`;
    if (revealAttemptRef.current.key !== key) revealAttemptRef.current = { key, pages: 0 };
    if (revealAttemptRef.current.pages >= MAX_REVEAL_PAGES) return; // give up — leave "Show more" for manual paging
    revealAttemptRef.current.pages += 1;
    onLoadMoreThreads(currentWorkspaceId);
  }, [isActive, currentWorkspaceId, currentThreadId, workspaces, workspaceThreads, onLoadMoreThreads]);

  const toggleWorkspace = useCallback((wsId: string) => {
    // Capture pre-toggle state: only an *expand* should lazy-load threads.
    const wasExpanded = expandedWorkspaces.has(wsId);
    // Routed through the store so every collapse is traceable in one place.
    toggleWorkspaceExpansion(wsId);
    // Lazy-load threads only when opening. expandWorkspace is a no-op when data
    // is already cached, but the shared store is capped and can evict a list, so
    // calling it on a collapse would fire a needless re-fetch.
    if (!wasExpanded) expandWorkspace(wsId);
  }, [expandWorkspace]);

  const toggleThread = useCallback((threadId: string) => {
    toggleThreadExpansion(threadId);
  }, []);

  // Inline workspace rename — the name span becomes a text input while editing.
  // Refs shadow the editing state so the blur/Enter commit reads fresh values and
  // stays idempotent: Enter clears the id, the trailing blur then no-ops.
  const [renamingWsId, setRenamingWsId] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState('');
  const renamingWsIdRef = useRef<string | null>(null);
  const renameValueRef = useRef('');
  const renameOriginalRef = useRef('');
  const renameInputRef = useRef<HTMLInputElement>(null);

  const startRename = useCallback((wsId: string, currentName: string) => {
    renamingWsIdRef.current = wsId;
    renameOriginalRef.current = currentName;
    renameValueRef.current = currentName;
    setRenameValue(currentName);
    setRenamingWsId(wsId);
  }, []);

  const handleRenameChange = useCallback((value: string) => {
    renameValueRef.current = value;
    setRenameValue(value);
  }, []);

  const cancelRename = useCallback(() => {
    renamingWsIdRef.current = null;
    setRenamingWsId(null);
  }, []);

  const commitRename = useCallback(() => {
    const wsId = renamingWsIdRef.current;
    if (!wsId) return; // already committed — the trailing blur after Enter is a no-op
    renamingWsIdRef.current = null;
    setRenamingWsId(null);
    const name = renameValueRef.current.trim();
    if (name && name !== renameOriginalRef.current) onRenameWorkspace?.(wsId, name);
  }, [onRenameWorkspace]);

  // Two stable bundles instead of a per-row object: only the row whose id is
  // being renamed gets the active one, so the other rows' props stay
  // identity-stable across a rename.
  const inactiveRename = useMemo(() => ({
    active: false,
    value: '',
    inputRef: renameInputRef,
    onChange: handleRenameChange,
    onCommit: commitRename,
    onCancel: cancelRename,
    onStart: startRename,
    enabled: !!onRenameWorkspace,
  }), [handleRenameChange, commitRename, cancelRename, startRename, onRenameWorkspace]);

  const activeRename = useMemo(
    () => ({ ...inactiveRename, active: true, value: renameValue }),
    [inactiveRename, renameValue],
  );

  // Focus + select the rename input once it mounts. rAF defers past Radix's
  // focus-restore-to-trigger when the rename is launched from the options menu.
  React.useEffect(() => {
    if (!renamingWsId) return;
    const raf = requestAnimationFrame(() => {
      renameInputRef.current?.focus();
      renameInputRef.current?.select();
    });
    return () => cancelAnimationFrame(raf);
  }, [renamingWsId]);

  // Vertical extent of the lifted workspace's own pin block, measured at drag
  // start. The chip is clamped inside it so the GESTURE can't cross the pin
  // boundary — droppable gating already keeps the row previews honest, but an
  // unclamped chip floating over the other block still reads as a legal drop.
  // Rows above the lifted one never move mid-drag, so the boundary edge stays
  // exact even though the lifted section collapses after this measurement.
  const dragBlockBoundsRef = useRef<{ top: number; bottom: number } | null>(null);

  const handleWorkspaceDragStart = useCallback((event: DragStartEvent) => {
    const id = String(event.active.id);
    setActiveDragId(id);
    const active = workspaces.find((ws) => ws.workspace_id === id);
    if (!active) {
      dragBlockBoundsRef.current = null;
      return;
    }
    const activePinned = isEffectivelyPinned(active);
    const rects = workspaces
      .filter((ws) => isEffectivelyPinned(ws) === activePinned)
      .map((ws) => document.querySelector(`[data-ws-id="${ws.workspace_id}"]`)?.getBoundingClientRect())
      .filter((r): r is DOMRect => !!r);
    dragBlockBoundsRef.current = rects.length
      ? { top: Math.min(...rects.map((r) => r.top)), bottom: Math.max(...rects.map((r) => r.bottom)) }
      : null;
  }, [workspaces]);

  const clampChipToBlock = useCallback<Modifier>(({ transform, draggingNodeRect }) => {
    const bounds = dragBlockBoundsRef.current;
    if (!bounds || !draggingNodeRect) return transform;
    // The chip renders as a single header row even when the lifted section
    // had threads expanded — clamp that row height, not the section's.
    const chipHeight = Math.min(draggingNodeRect.height, 40);
    const minY = bounds.top - draggingNodeRect.top;
    const maxY = bounds.bottom - chipHeight - draggingNodeRect.top;
    if (maxY < minY) return transform;
    return { ...transform, y: Math.max(minY, Math.min(transform.y, maxY)) };
  }, []);

  const handleWorkspaceDragEnd = useCallback((event: DragEndEvent) => {
    setActiveDragId(null);
    dragBlockBoundsRef.current = null;
    const { active, over } = event;
    if (!over || active.id === over.id) return;
    onReorderWorkspace?.(String(active.id), String(over.id));
  }, [onReorderWorkspace]);

  const handleWorkspaceDragCancel = useCallback(() => {
    setActiveDragId(null);
    dragBlockBoundsRef.current = null;
  }, []);

  const activeDragWs = activeDragId ? workspaces.find((ws) => ws.workspace_id === activeDragId) : null;

  return (
    <div
      className="nav-panel h-full flex flex-col"
    >
      {headerActions && (
        <div className="nav-panel-header">
          {headerActions}
        </div>
      )}
      <ScrollArea className="flex-1">
        <div className="py-2">
          <DndContext
            sensors={dndSensors}
            collisionDetection={closestCenter}
            measuring={{ droppable: { strategy: MeasuringStrategy.Always } }}
            onDragStart={handleWorkspaceDragStart}
            onDragEnd={handleWorkspaceDragEnd}
            onDragCancel={handleWorkspaceDragCancel}
          >
          <SortableContext items={workspaces.map((ws) => ws.workspace_id)} strategy={verticalListSortingStrategy}>
          {workspaces.map((ws) => {
            const wsId = ws.workspace_id;
            // While a drag is live, rows across the pin boundary from the
            // lifted workspace (Flash counts as pinned) stop being drop
            // targets, so the preview can never show an arrangement the drop
            // handler refuses — releasing there used to silently snap back.
            const crossBlock = !!activeDragWs && activeDragId !== wsId &&
              isEffectivelyPinned(ws) !== isEffectivelyPinned(activeDragWs);
            const dragDisabled = isMobile || !onReorderWorkspace
              ? true
              : crossBlock
                ? { draggable: false, droppable: true }
                : false;

            return (
              <WorkspaceTreeRow
                key={wsId}
                ws={ws}
                isExpanded={expandedWorkspaces.has(wsId)}
                isCurrent={wsId === currentWorkspaceId}
                isMobile={isMobile}
                dragDisabled={dragDisabled}
                threadsData={workspaceThreads[wsId]}
                currentThreadId={currentThreadId}
                expandedThreadIds={expandedThreads}
                agents={agents}
                activeAgentId={activeAgentId}
                wsActions={wsActions}
                rename={renamingWsId === wsId ? activeRename : inactiveRename}
                onToggleWorkspace={toggleWorkspace}
                onToggleThread={toggleThread}
                onNavigateThread={onNavigateThread}
                onSelectAgent={onSelectAgent}
                onRemoveAgent={onRemoveAgent}
                onLoadMoreThreads={onLoadMoreThreads}
                onPinWorkspace={onPinWorkspace}
                onNewThread={onNewThread}
                onPinThread={onPinThread}
                onArchiveThread={onArchiveThread ? handleArchiveThread : undefined}
              />
            );
          })}
          </SortableContext>
          {/* Compact lift preview — a content-hugging header pill that follows
              the cursor, so the dragged section's real size never distorts.
              Portaled to <body>: the overlay is position:fixed, and any
              sidebar ancestor gaining a transform/filter/will-change would
              become its containing block and drift the chip off the cursor
              (the mount animation's fill mode did exactly that once). zIndex
              must clear the sidebar's 1000. */}
          {createPortal(
            <DragOverlay dropAnimation={null} zIndex={1100} modifiers={[clampChipToBlock]}>
              {activeDragWs ? (
                <WorkspaceDragChip ws={activeDragWs} expanded={expandedWorkspaces.has(activeDragWs.workspace_id)} />
              ) : null}
            </DragOverlay>,
            document.body,
          )}
          </DndContext>
          {hasMore && (
            <div
              className="nav-panel-row"
              style={{ paddingLeft: 10, justifyContent: 'center' }}
              onClick={onLoadMore}
            >
              <ChevronsDown className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('nav.loadAll')}
              </span>
            </div>
          )}
        </div>
      </ScrollArea>
      {wsActions.dialogs}
      {archiveDialog}
    </div>
  );
}

// Memoized: the desktop AppSidebar re-renders on route/theme/nav-data changes
// far more often than this tree's own inputs change, and every prop it passes
// is identity-stable between genuine updates.
export default React.memo(NavigationPanel);
