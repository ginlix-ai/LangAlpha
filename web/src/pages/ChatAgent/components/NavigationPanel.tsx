import React, { useState, useCallback, useRef, useSyncExternalStore } from 'react';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { DndContext, DragOverlay, closestCenter, PointerSensor, MeasuringStrategy, useSensor, useSensors } from '@dnd-kit/core';
import type { DragStartEvent, DragEndEvent } from '@dnd-kit/core';
import { SortableContext, useSortable, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import {
  ChevronRight, Folder, FolderOpen, Zap, Pin, MessageSquareText,
  Check, Circle, Loader2, X, ChevronsDown, MoreHorizontal, SquarePen, Pencil,
  AlertCircle, StopCircle,
} from 'lucide-react';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '../../../components/ui/dropdown-menu';
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
import { deriveSubagentStatus } from '../session/subagents/subagentStatus';
import './NavigationPanel.css';

interface WorkspaceEntry {
  workspace_id: string;
  name?: string;
  status?: string;
  is_pinned?: boolean;
  [key: string]: unknown;
}

interface ThreadEntry {
  thread_id: string;
  title?: string;
  first_query_content?: string;
  [key: string]: unknown;
}

interface ThreadsData {
  threads: ThreadEntry[];
  loading?: boolean;
  total?: number;
}

interface AgentMessage {
  role: string;
  isStreaming?: boolean;
  toolCallProcesses?: Record<string, { isInProgress?: boolean }>;
  [key: string]: unknown;
}

interface AgentEntry {
  id: string;
  name: string;
  description?: string;
  isMainAgent?: boolean;
  status?: string;
  messages?: AgentMessage[];
  [key: string]: unknown;
}

interface NavigationPanelProps {
  /** Whether this panel's host ChatView is the visible one. Up to 5 ChatViews
   *  stay mounted (display:none); only the active one should auto-page threads. */
  isActive?: boolean;
  workspaces: WorkspaceEntry[];
  workspaceThreads: Record<string, ThreadsData>;
  currentWorkspaceId?: string | null;
  currentThreadId?: string | null;
  agents?: AgentEntry[];
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
  /** Right-aligned controls (pin, minimize) rendered in a header row above the list. */
  headerActions?: React.ReactNode;
}

/**
 * Sortable wrapper for one workspace section (header row + thread sub-list).
 * The header row receives the drag listeners via the render prop, which also
 * gets `isDragging` so the section can collapse to header height while lifted.
 *
 * Translate-only (not Transform) so displaced siblings never pick up the
 * scaleX/scaleY that distorts variable-height rows; the lifted item itself is
 * hidden here and shown as a fixed-size DragOverlay chip instead.
 */
function SortableWorkspace({ wsId, disabled, children }: {
  wsId: string;
  disabled: boolean;
  children: (args: { dragHandleProps: Record<string, unknown>; isDragging: boolean }) => React.ReactNode;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id: wsId, disabled });
  const style: React.CSSProperties = {
    transform: CSS.Translate.toString(transform),
    transition,
    opacity: isDragging ? 0 : 1,
    position: 'relative',
    zIndex: isDragging ? 5 : undefined,
  };
  return (
    <div ref={setNodeRef} style={style}>
      {children({ dragHandleProps: disabled ? {} : { ...attributes, ...listeners }, isDragging })}
    </div>
  );
}

// Cap on how many extra thread pages the active-thread auto-reveal will fetch
// before giving up: covers a genuinely deep thread while bounding a stale or
// deleted id that would otherwise page through the whole workspace.
const MAX_REVEAL_PAGES = 20;

/**
 * NavigationPanel -- hover-triggered overlay sidebar showing
 * Workspace -> Thread -> Agent hierarchy.
 *
 * Follows the collapsible tree pattern from FilePanel's DirectoryNode:
 * ChevronRight/Down toggles, indented rows, Lucide icons throughout.
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
  headerActions,
}: NavigationPanelProps) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
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
  // current thread — re-runs as each page lands, walking the tail. Skipped for
  // flash (capped at 3, no "Show more") and bounded by MAX_REVEAL_PAGES.
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
    if (!currentWs || currentWs.status === 'flash') return;
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

  const handleWorkspaceDragStart = useCallback((event: DragStartEvent) => {
    setActiveDragId(String(event.active.id));
  }, []);

  const handleWorkspaceDragEnd = useCallback((event: DragEndEvent) => {
    setActiveDragId(null);
    const { active, over } = event;
    if (!over || active.id === over.id) return;
    onReorderWorkspace?.(String(active.id), String(over.id));
  }, [onReorderWorkspace]);

  const handleWorkspaceDragCancel = useCallback(() => setActiveDragId(null), []);

  const activeDragWs = activeDragId ? workspaces.find((ws) => ws.workspace_id === activeDragId) : null;

  // Derive agent status for display — shared with SubagentStatusBar so the
  // nav tree and the detail header can never disagree.
  const getAgentStatus = useCallback((agent: AgentEntry): string => {
    if (agent.isMainAgent) return 'active';
    return deriveSubagentStatus(agent);
  }, []);

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
            const isExpanded = expandedWorkspaces.has(wsId);
            const isFlash = ws.status === 'flash';
            const isPinned = ws.is_pinned;
            const isCurrent = wsId === currentWorkspaceId;
            const threadsData = workspaceThreads[wsId];
            const allThreads = threadsData?.threads || [];
            const threads = isFlash ? allThreads.slice(0, 3) : allThreads;
            const threadsLoading = threadsData?.loading || false;
            const isRenaming = renamingWsId === wsId;
            // Pin / rename / new-thread aren't offered on the flash workspace
            // (shared, immutable) — mirrors the gallery hiding its card menu there.
            const showWsActions = !isFlash && (onNewThread || onPinWorkspace || onRenameWorkspace);

            return (
              <SortableWorkspace key={wsId} wsId={wsId} disabled={isFlash || isMobile || !onReorderWorkspace}>
                {({ dragHandleProps, isDragging: isThisDragging }) => (<>
                {/* Workspace row — doubles as the drag handle for reordering.
                    While renaming, click-to-toggle and drag are suppressed so the
                    inline input owns the row. */}
                <div
                  className="nav-panel-row group"
                  style={{ paddingLeft: 10 }}
                  onClick={() => { if (!isRenaming) toggleWorkspace(wsId); }}
                  {...(isRenaming ? {} : dragHandleProps)}
                >
                  {isFlash
                    ? <Zap className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                    : isPinned
                      ? <Pin className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                      : isExpanded
                        ? <FolderOpen className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                        : <Folder className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                  }
                  {isRenaming ? (
                    <input
                      ref={renameInputRef}
                      className="text-sm font-medium bg-transparent outline-none border-b flex-1 min-w-0"
                      style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-muted)' }}
                      value={renameValue}
                      onChange={(e) => { setRenameValue(e.target.value); renameValueRef.current = e.target.value; }}
                      onClick={(e) => e.stopPropagation()}
                      onPointerDown={(e) => e.stopPropagation()}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter') { e.preventDefault(); commitRename(); }
                        else if (e.key === 'Escape') { e.preventDefault(); cancelRename(); }
                      }}
                      onBlur={commitRename}
                      aria-label={t('workspace.rename')}
                    />
                  ) : (
                    <>
                      <span
                        className="text-sm font-medium truncate"
                        style={{ color: isCurrent ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)' }}
                      >
                        {ws.name || 'Workspace'}
                      </span>
                      {/* initial={false}: thread switches remount the panel; the
                          chevron must render at its resting angle, not animate to it.
                          Hidden until the row is hovered (always visible on touch). */}
                      <motion.span
                        className={`flex-shrink-0 flex items-center ${isMobile ? '' : 'opacity-0 group-hover:opacity-100 transition-opacity'}`}
                        initial={false}
                        animate={{ rotate: isExpanded ? 90 : 0 }}
                        transition={{ duration: 0.15, ease: 'easeOut' }}
                      >
                        <ChevronRight className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
                      </motion.span>
                      {/* Right-aligned row actions: new thread + options (pin / rename).
                          Hover-revealed on desktop, always shown on touch. */}
                      {showWsActions && (
                        <div className={`flex items-center gap-0.5 ml-auto flex-shrink-0 ${isMobile ? '' : 'opacity-0 group-hover:opacity-100 transition-opacity'}`}>
                          {onNewThread && (
                            <button
                              type="button"
                              onPointerDown={(e) => e.stopPropagation()}
                              onClick={(e) => { e.stopPropagation(); onNewThread(wsId); }}
                              className="flex items-center justify-center p-0.5 rounded bg-transparent border-none cursor-pointer hover:bg-[var(--color-border-muted)]"
                              title={t('nav.newThread')}
                              aria-label={t('nav.newThread')}
                            >
                              <SquarePen className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                            </button>
                          )}
                          {(onPinWorkspace || onRenameWorkspace) && (
                            <DropdownMenu modal={false}>
                              <DropdownMenuTrigger asChild>
                                <button
                                  type="button"
                                  onPointerDown={(e) => e.stopPropagation()}
                                  onClick={(e) => e.stopPropagation()}
                                  className="flex items-center justify-center p-0.5 rounded bg-transparent border-none cursor-pointer hover:bg-[var(--color-border-muted)]"
                                  title={t('workspace.options')}
                                  aria-label={t('workspace.options')}
                                >
                                  <MoreHorizontal className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                                </button>
                              </DropdownMenuTrigger>
                              <DropdownMenuContent align="end" sideOffset={4} onClick={(e) => e.stopPropagation()}>
                                {onPinWorkspace && (
                                  <DropdownMenuItem onSelect={() => onPinWorkspace(wsId, !isPinned)}>
                                    <Pin className="h-4 w-4" />
                                    {isPinned ? t('workspace.unpin') : t('workspace.pinToTop')}
                                  </DropdownMenuItem>
                                )}
                                {onRenameWorkspace && (
                                  <DropdownMenuItem onSelect={() => startRename(wsId, ws.name || '')}>
                                    <Pencil className="h-4 w-4" />
                                    {t('workspace.rename')}
                                  </DropdownMenuItem>
                                )}
                              </DropdownMenuContent>
                            </DropdownMenu>
                          )}
                        </div>
                      )}
                      {threadsLoading && (
                        <Loader2 className={`h-3.5 w-3.5 animate-spin flex-shrink-0 ${showWsActions ? '' : 'ml-auto'}`} style={{ color: 'var(--color-text-tertiary)' }} />
                      )}
                    </>
                  )}
                </div>

                {/* Threads under this workspace — animated expand/collapse.
                    Hidden while this section is the one being dragged so the
                    lifted item shrinks to header height (clean gap), and the
                    DragOverlay chip below carries the visual instead. */}
                <AnimatePresence initial={false}>
                  {isExpanded && !isThisDragging && (
                    <motion.div
                      initial={{ height: 0, opacity: 0 }}
                      animate={{ height: 'auto', opacity: 1 }}
                      exit={{ height: 0, opacity: 0 }}
                      transition={{ duration: 0.2, ease: 'easeInOut' }}
                      style={{ overflow: 'hidden' }}
                    >
                    {!threadsLoading && threads.length === 0 && (
                      <div
                        className="text-xs px-2 py-1"
                        style={{ paddingLeft: 44, color: 'var(--color-icon-muted)' }}
                      >
                        No conversations yet
                      </div>
                    )}
                    {threads.map((thread) => {
                      const tid = thread.thread_id;
                      const isCurrentThread = tid === currentThreadId;
                      const isThreadExpanded = expandedThreads.has(tid);
                      const subagents = agents?.filter((a) => !a.isMainAgent) || [];
                      const hasSubagents = isCurrentThread && subagents.length > 0;
                      const title = thread.title || thread.first_query_content?.slice(0, 40) || 'Untitled';

                      return (
                        <div key={tid}>
                          {/* Thread row */}
                          <div
                            className={`nav-panel-row group ${isCurrentThread ? 'nav-panel-row-active' : ''}`}
                            style={{ paddingLeft: 28 }}
                            onClick={() => {
                              if (isCurrentThread) {
                                // Toggle agents expand for current thread
                                if (hasSubagents) toggleThread(tid);
                              } else {
                                onNavigateThread(wsId, tid);
                              }
                            }}
                          >
                            <MessageSquareText className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                            <span
                              className="text-sm truncate"
                              style={{ color: isCurrentThread ? 'var(--color-text-primary)' : 'var(--color-text-secondary)' }}
                              title={title}
                            >
                              {title}
                            </span>
                            {/* Expand-agents chevron — immediately right of the thread
                                name, mirroring the workspace row. Only on the current
                                thread when it has subagents. Hover-revealed (always shown
                                on touch); rotates 90° when expanded. initial={false}:
                                thread switches remount the panel, so the chevron renders
                                at its resting angle. */}
                            {hasSubagents && (
                              <motion.button
                                type="button"
                                onClick={(e) => { e.stopPropagation(); toggleThread(tid); }}
                                className={`flex-shrink-0 flex items-center p-0 bg-transparent border-none cursor-pointer ${isMobile ? '' : 'opacity-0 group-hover:opacity-100 transition-opacity'}`}
                                initial={false}
                                animate={{ rotate: isThreadExpanded ? 90 : 0 }}
                                transition={{ duration: 0.15, ease: 'easeOut' }}
                                aria-label={isThreadExpanded ? 'Collapse agents' : 'Expand agents'}
                              >
                                <ChevronRight className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
                              </motion.button>
                            )}
                          </div>

                          {/* Agent rows -- only when subagents exist, for current thread when expanded */}
                          {hasSubagents && isThreadExpanded && (
                            <div className="nav-panel-agent-group">
                              {agents!.map((agent) => {
                                const isMainAgent = agent.isMainAgent;
                                const isSelected = activeAgentId === agent.id;
                                const status = getAgentStatus(agent);
                                // Narrow agent-running flag; named distinctly from the
                                // component's isActive prop (panel visibility) to avoid shadowing.
                                const isAgentActive = status === 'active';
                                const isCompleted = status === 'completed';
                                const isError = status === 'error';
                                const isCancelled = status === 'cancelled';

                                const trimmedDescription = typeof agent.description === 'string' ? agent.description.trim() : '';
                                const rowLabel = !isMainAgent && trimmedDescription
                                  ? trimmedDescription
                                  : agent.name;

                                return (
                                  <div
                                    key={agent.id}
                                    data-testid="agent-row"
                                    data-agent-role={isMainAgent ? 'main' : 'sub'}
                                    className={`nav-panel-agent-row group ${isAgentActive && !isMainAgent ? 'nav-panel-agent-pulse' : ''}${isSelected ? ' is-selected' : ''}`}
                                    style={{
                                      backgroundColor: isSelected ? 'var(--color-border-muted)' : undefined,
                                    }}
                                    onClick={() => onSelectAgent(agent.id)}
                                  >
                                    {/* Hierarchy indicator: subagents render `└─` to descend visually under the main agent's text column */}
                                    {!isMainAgent && (
                                      <span aria-hidden="true" className="nav-panel-agent-glyph text-xs">
                                        └─
                                      </span>
                                    )}
                                    {/* Agent label: subagent description when available, else fallback name */}
                                    <span
                                      className="text-xs truncate"
                                      style={{ color: isSelected ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)' }}
                                      title={rowLabel}
                                    >
                                      {rowLabel}
                                    </span>
                                    {/* Status badge. Terminal outcomes read at a
                                        glance: error = red alert, cancelled = stop,
                                        completed = check; a genuinely running task
                                        spins, otherwise an idle circle. */}
                                    {!isMainAgent && (
                                      <span className="flex-shrink-0 ml-auto flex items-center">
                                        {isError ? (
                                          <AlertCircle className="h-3 w-3" style={{ color: 'var(--color-danger, #c43d3d)' }} />
                                        ) : isCancelled ? (
                                          <StopCircle className="h-3 w-3" style={{ color: 'var(--color-text-tertiary)' }} />
                                        ) : isCompleted ? (
                                          <Check className="h-3 w-3" style={{ color: 'var(--color-text-tertiary)' }} />
                                        ) : isAgentActive ? (
                                          <Loader2 className="h-3 w-3 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
                                        ) : (
                                          <Circle className="h-3 w-3" style={{ color: 'var(--color-icon-muted)' }} />
                                        )}
                                      </span>
                                    )}
                                    {/* Remove button -- non-main agents only, on hover */}
                                    {!isMainAgent && (
                                      <button
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          onRemoveAgent?.(agent.id);
                                        }}
                                        className={`flex-shrink-0 p-0 bg-transparent border-none cursor-pointer transition-opacity ${isMobile ? 'opacity-60' : 'opacity-0 group-hover:opacity-100'}`}
                                        title="Remove agent"
                                      >
                                        <X className="h-3 w-3" style={{ color: 'var(--color-text-tertiary)' }} />
                                      </button>
                                    )}
                                  </div>
                                );
                              })}
                            </div>
                          )}
                        </div>
                      );
                    })}
                    {/* Show more — next page of threads for this workspace */}
                    {!isFlash && onLoadMoreThreads && typeof threadsData?.total === 'number'
                      && allThreads.length < threadsData.total && !threadsLoading && (
                      <div
                        className="nav-panel-row"
                        style={{ paddingLeft: 44 }}
                        onClick={(e) => { e.stopPropagation(); onLoadMoreThreads(wsId); }}
                      >
                        <ChevronsDown className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                        <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                          {t('nav.showMore')}
                        </span>
                      </div>
                    )}
                    </motion.div>
                  )}
                </AnimatePresence>
                </>)}
              </SortableWorkspace>
            );
          })}
          </SortableContext>
          {/* Fixed-size lift preview — a clean header chip that follows the
              cursor, so the dragged section's real height never distorts. */}
          <DragOverlay dropAnimation={null}>
            {activeDragWs ? (
              <div className="nav-panel nav-panel-drag-chip">
                <div className="nav-panel-row" style={{ paddingLeft: 10 }}>
                  {activeDragWs.is_pinned
                    ? <Pin className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                    : <Folder className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                  }
                  <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
                    {activeDragWs.name || 'Workspace'}
                  </span>
                </div>
              </div>
            ) : null}
          </DragOverlay>
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
    </div>
  );
}

export default NavigationPanel;
