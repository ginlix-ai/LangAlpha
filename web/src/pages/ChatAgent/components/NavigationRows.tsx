/**
 * Row components for the nav tree (NavigationPanel): one workspace section,
 * one thread, one agent, plus the drag chip. Lifted out of the panel so each
 * row is a real component — hooks can live per row (title fade, per-thread
 * liveness), and the panel keeps only tree-level state and handlers.
 */
import React, { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import {
  ChevronRight, Folder, FolderOpen, Zap, Pin,
  X, ChevronsDown, MoreHorizontal, SquarePen, Archive,
} from 'lucide-react';
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger } from '../../../components/ui/dropdown-menu';
import { HoverCard, HoverCardContent, HoverCardTrigger } from '../../../components/ui/hover-card';
import { createDateFormatter, relativeTime } from '@/lib/format';
import { Loader } from '@/components/ui/loader';
import { useTitleFade } from '@/hooks/useTitleFade';
import { useThreadFlags } from '@/lib/threadLifecycle/store';
import type { SidebarAgentRow } from '../session/subagents/subagentStatus';
import { SubagentStatusIcon } from './taskStatusUi';
import type { NavWorkspace } from '../hooks/useNavigationData';
import { WorkspaceMenuItems } from './workspaceActions';
import type { WorkspaceActions } from './workspaceActions';

export interface ThreadEntry {
  thread_id: string;
  title?: string;
  first_query_content?: string;
  is_pinned?: boolean;
  created_at?: string;
  updated_at?: string;
  turn_count?: number;
  [key: string]: unknown;
}

export interface ThreadsData {
  threads: ThreadEntry[];
  loading?: boolean;
  total?: number;
}

/** The workspace row's leading glyph. Shared with the drag chip so a lifted
 *  section keeps the icon it had in the tree. */
function workspaceGlyph(ws: NavWorkspace, expanded: boolean) {
  const style = { color: 'var(--color-text-tertiary)' };
  const className = 'h-4 w-4 flex-shrink-0';
  if (ws.status === 'flash') return <Zap className={className} style={style} />;
  if (ws.is_pinned) return <Pin className={className} style={style} />;
  return expanded ? <FolderOpen className={className} style={style} /> : <Folder className={className} style={style} />;
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
  disabled: boolean | { draggable: boolean; droppable: boolean };
  children: (args: { dragHandleProps: Record<string, unknown>; isDragging: boolean }) => React.ReactNode;
}) {
  // dnd-kit back-compat trap: a boolean `disabled` normalizes to
  // {draggable, droppable: false} — the row would stay an active drop
  // target. Spell out both aspects so `true` really means fully disabled.
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: wsId,
    disabled: typeof disabled === 'boolean' ? { draggable: disabled, droppable: disabled } : disabled,
  });
  const dragDisabled = typeof disabled === 'boolean' ? disabled : disabled.draggable;
  const style: React.CSSProperties = {
    transform: CSS.Translate.toString(transform),
    transition,
    opacity: isDragging ? 0 : 1,
    position: 'relative',
    zIndex: isDragging ? 5 : undefined,
  };
  return (
    <div ref={setNodeRef} style={style} data-ws-id={wsId}>
      {children({ dragHandleProps: dragDisabled ? {} : { ...attributes, ...listeners }, isDragging })}
    </div>
  );
}

// Its own component so useTitleFade can run per row (hooks can't live in a map
// callback). Fades softly when a live title rewrite lands (auto-title,
// rename); static on first paint. No native `title` attr — the metadata
// hover card carries the full title (a browser tooltip would double up).
function ThreadRowTitle({ title, active }: { title: string; active: boolean }) {
  const fading = useTitleFade(title);
  return (
    <span
      className={`text-sm truncate${fading ? ' animate-fade-in' : ''}`}
      style={{ color: active ? 'var(--color-text-primary)' : 'var(--color-text-secondary)' }}
    >
      {title}
    </span>
  );
}

/* Run-state glyph in a thread row's indent gutter (absolute, under the
   workspace folder icon) so every title stays flush at the same x whether or
   not the row is flagged: ascii spinner = running (amber = live agent work),
   hollow ring = waiting on input (shown even on the current thread), dot =
   finished off-screen, unseen until opened. Subscribed per thread — a run
   event re-renders just this gutter span, never the panel tree. */
function ThreadRowGlyph({ tid, isCurrentThread }: { tid: string; isCurrentThread: boolean }) {
  const { t } = useTranslation();
  const { isRunning, needsInput, isUnseen } = useThreadFlags(tid);
  if (!isRunning && !needsInput && !(isUnseen && !isCurrentThread)) return null;
  return (
    <span className="absolute left-2 top-1/2 flex h-4 w-4 -translate-y-1/2 items-center justify-center">
      {isRunning ? (
        <Loader
          size={13}
          label={t('chat.taskCard.statusRunning')}
          className="text-[color:var(--color-accent-primary)]"
        />
      ) : needsInput ? (
        <span
          role="img"
          aria-label={t('nav.threadNeedsInput')}
          className="rounded-full"
          style={{
            width: 7,
            height: 7,
            border: '1.5px solid var(--color-accent-primary)',
          }}
        />
      ) : (
        <span
          role="img"
          aria-label={t('nav.threadUnseen')}
          className="rounded-full"
          style={{ width: 6, height: 6, background: 'var(--color-accent-primary)' }}
        />
      )}
    </span>
  );
}

const threadMetaDate = createDateFormatter({ dateStyle: 'medium' });

// Compact metadata card shown after hovering a thread row: full title plus
// the cheap facts the list row already carries. Metadata-only, so it never
// captures the pointer (pointer-events-none) — hovering through it onto
// neighboring rows keeps working.
function ThreadMetaCard({ thread, title, t }: {
  thread: ThreadEntry;
  title: string;
  t: (key: string) => string;
}) {
  const rows: Array<[string, string]> = [];
  if (thread.updated_at) rows.push([t('nav.threadUpdated'), relativeTime(thread.updated_at)]);
  if (thread.created_at) rows.push([t('nav.threadCreated'), threadMetaDate(new Date(thread.created_at))]);
  if (typeof thread.turn_count === 'number') rows.push([t('nav.threadTurns'), String(thread.turn_count)]);
  return (
    <HoverCardContent side="right" align="start" sideOffset={12} className="w-60 p-3 pointer-events-none select-none">
      <div className="text-[0.8125rem] font-medium leading-snug" style={{ color: 'var(--color-text-primary)' }}>
        {title}
      </div>
      {rows.length > 0 && (
        <div className="mt-2 flex flex-col gap-1">
          {rows.map(([label, value]) => (
            <div key={label} className="flex items-baseline justify-between gap-3 text-xs">
              <span style={{ color: 'var(--color-text-tertiary)' }}>{label}</span>
              <span className="text-right" style={{ color: 'var(--color-text-secondary)' }}>{value}</span>
            </div>
          ))}
        </div>
      )}
    </HoverCardContent>
  );
}

export interface AgentRowProps {
  agent: SidebarAgentRow;
  isSelected: boolean;
  isMobile: boolean;
  onSelectAgent: (agentId: string) => void;
  onRemoveAgent?: (agentId: string) => void;
}

export function AgentRow({ agent, isSelected, isMobile, onSelectAgent, onRemoveAgent }: AgentRowProps) {
  const { t } = useTranslation();
  const isMainAgent = agent.isMainAgent;
  const status = agent.status;
  // Narrow agent-running flag; named distinctly from the panel's isActive
  // prop (panel visibility) to avoid shadowing.
  const isAgentActive = status === 'active';

  const trimmedDescription = agent.description.trim();
  const rowLabel = !isMainAgent && trimmedDescription ? trimmedDescription : agent.name;

  return (
    <div
      data-testid="agent-row"
      data-agent-role={isMainAgent ? 'main' : 'sub'}
      className={`nav-panel-agent-row group ${isAgentActive && !isMainAgent ? 'nav-panel-agent-pulse' : ''}${isSelected ? ' is-selected' : ''}`}
      style={{ backgroundColor: isSelected ? 'var(--color-border-muted)' : undefined }}
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
      {/* Status badge — the shared SubagentStatusIcon table (taskStatusUi):
          exceptional outcomes read at a glance, a running task shows the ascii
          liveness glyph (amber = live agent work), a silent one an idle circle.
          Completed renders nothing — no glyph IS the done state. */}
      {!isMainAgent && (
        <span className="flex-shrink-0 ml-auto flex items-center">
          <SubagentStatusIcon status={status} className="h-3 w-3" />
        </span>
      )}
      {/* Remove button -- non-main agents only, on hover */}
      {!isMainAgent && (
        <button
          onClick={(e) => { e.stopPropagation(); onRemoveAgent?.(agent.id); }}
          className={`flex-shrink-0 p-0 bg-transparent border-none cursor-pointer transition-opacity ${isMobile ? 'opacity-60' : 'opacity-0 group-hover:opacity-100'}`}
          title={t('nav.removeAgent')}
        >
          <X className="h-3 w-3" style={{ color: 'var(--color-text-tertiary)' }} />
        </button>
      )}
    </div>
  );
}

export interface ThreadTreeRowProps {
  wsId: string;
  thread: ThreadEntry;
  isCurrentThread: boolean;
  isExpanded: boolean;
  isMobile: boolean;
  /** Subagent rows for the CURRENT thread only; absent on every other row. */
  agents?: SidebarAgentRow[];
  activeAgentId?: string | null;
  onToggleThread: (threadId: string) => void;
  onNavigateThread: (wsId: string, threadId: string) => void;
  onSelectAgent: (agentId: string) => void;
  onRemoveAgent?: (agentId: string) => void;
  onPinThread?: (wsId: string, threadId: string, pinned: boolean) => void;
  onArchiveThread?: (wsId: string, threadId: string) => void;
}

export function ThreadTreeRow({
  wsId,
  thread,
  isCurrentThread,
  isExpanded,
  isMobile,
  agents,
  activeAgentId,
  onToggleThread,
  onNavigateThread,
  onSelectAgent,
  onRemoveAgent,
  onPinThread,
  onArchiveThread,
}: ThreadTreeRowProps) {
  const { t } = useTranslation();
  const tid = thread.thread_id;
  const subagents = agents?.filter((a) => !a.isMainAgent) || [];
  const hasSubagents = isCurrentThread && subagents.length > 0;
  const title = thread.title || thread.first_query_content?.slice(0, 40) || t('nav.untitledThread');
  const isPinned = Boolean(thread.is_pinned);

  return (
    <>
      {/* Thread row */}
      <HoverCard openDelay={500} closeDelay={100}>
        <HoverCardTrigger asChild>
          <div
            className={`nav-panel-row group relative ${isCurrentThread ? 'nav-panel-row-active' : ''}`}
            style={{ paddingLeft: 28 }}
            onClick={() => {
              if (isCurrentThread) {
                // Toggle agents expand for current thread
                if (hasSubagents) onToggleThread(tid);
              } else {
                onNavigateThread(wsId, tid);
              }
            }}
          >
            <ThreadRowGlyph tid={tid} isCurrentThread={isCurrentThread} />
            <ThreadRowTitle title={title} active={isCurrentThread} />
            {/* Expand-agents chevron — immediately right of the thread name,
                mirroring the workspace row. Only on the current thread when it
                has subagents. Hover-revealed (always shown on touch); rotates
                90° when expanded. initial={false}: thread switches remount the
                panel, so the chevron renders at its resting angle. */}
            {hasSubagents && (
              <motion.button
                type="button"
                onClick={(e) => { e.stopPropagation(); onToggleThread(tid); }}
                className={`flex-shrink-0 flex items-center p-0 bg-transparent border-none cursor-pointer ${isMobile ? '' : 'opacity-0 group-hover:opacity-100 transition-opacity'}`}
                initial={false}
                animate={{ rotate: isExpanded ? 90 : 0 }}
                transition={{ duration: 0.15, ease: 'easeOut' }}
                aria-label={t(isExpanded ? 'nav.collapseAgents' : 'nav.expandAgents')}
              >
                <ChevronRight className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
              </motion.button>
            )}
            {/* Resting pinned marker — fades (never display-toggles, which
                would reflow the title) while the hover overlay carries the
                interactive pin (desktop; mobile shows the in-flow cluster,
                whose Pin fill carries state). */}
            {isPinned && !isMobile && (
              <Pin
                aria-hidden
                className="h-3 w-3 ml-auto flex-shrink-0 transition-opacity duration-150 group-hover:opacity-0"
                fill="currentColor"
                style={{ color: 'var(--color-text-quaternary)' }}
              />
            )}
            {(onPinThread || onArchiveThread) && (
              <div className={isMobile ? 'flex items-center gap-0.5 ml-auto flex-shrink-0' : 'nav-panel-row-actions'}>
                {onPinThread && (
                  <motion.button
                    type="button"
                    whileTap={{ scale: 0.85 }}
                    onClick={(e) => { e.stopPropagation(); onPinThread(wsId, tid, !isPinned); }}
                    className="flex items-center justify-center p-0.5 rounded bg-transparent border-none cursor-pointer hover:bg-[var(--color-bg-hover)]"
                    title={isPinned ? t('nav.unpinThread') : t('nav.pinThread')}
                    aria-label={isPinned ? t('nav.unpinThread') : t('nav.pinThread')}
                  >
                    {/* Keyed remount pops the glyph when pin state flips —
                        click acknowledgment before the row starts its glide. */}
                    <motion.span
                      key={isPinned ? 'pinned' : 'unpinned'}
                      className="flex"
                      initial={{ scale: 0.6 }}
                      animate={{ scale: 1 }}
                      transition={{ type: 'spring', stiffness: 480, damping: 26 }}
                    >
                      <Pin
                        className="h-3.5 w-3.5"
                        fill={isPinned ? 'currentColor' : 'none'}
                        style={{ color: 'var(--color-text-tertiary)' }}
                      />
                    </motion.span>
                  </motion.button>
                )}
                {onArchiveThread && (
                  <motion.button
                    type="button"
                    whileTap={{ scale: 0.85 }}
                    onClick={(e) => { e.stopPropagation(); onArchiveThread(wsId, tid); }}
                    className="flex items-center justify-center p-0.5 rounded bg-transparent border-none cursor-pointer hover:bg-[var(--color-bg-hover)]"
                    title={t('nav.archiveThread')}
                    aria-label={t('nav.archiveThread')}
                  >
                    <Archive className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                  </motion.button>
                )}
              </div>
            )}
          </div>
        </HoverCardTrigger>
        <ThreadMetaCard thread={thread} title={title} t={t} />
      </HoverCard>

      {/* Agent rows -- only when subagents exist, for current thread when expanded */}
      {hasSubagents && isExpanded && (
        <div className="nav-panel-agent-group">
          {agents!.map((agent) => (
            <AgentRow
              key={agent.id}
              agent={agent}
              isSelected={activeAgentId === agent.id}
              isMobile={isMobile}
              onSelectAgent={onSelectAgent}
              onRemoveAgent={onRemoveAgent}
            />
          ))}
        </div>
      )}
    </>
  );
}

export interface WorkspaceTreeRowProps {
  ws: NavWorkspace;
  isExpanded: boolean;
  isCurrent: boolean;
  isMobile: boolean;
  /** dnd-kit gating: `true` disables both aspects, the object form keeps the row droppable. */
  dragDisabled: boolean | { draggable: boolean; droppable: boolean };
  threadsData?: ThreadsData;
  currentThreadId?: string | null;
  expandedThreadIds: Set<string>;
  agents?: SidebarAgentRow[];
  activeAgentId?: string | null;
  /** The shared change-spec / always-on / duplicate / delete flows. */
  wsActions: WorkspaceActions;
  rename: {
    active: boolean;
    value: string;
    inputRef: React.RefObject<HTMLInputElement | null>;
    onChange: (value: string) => void;
    onCommit: () => void;
    onCancel: () => void;
    onStart: (wsId: string, currentName: string) => void;
    /** Absent when the host doesn't accept renames — the menu item is then hidden. */
    enabled: boolean;
  };
  onToggleWorkspace: (wsId: string) => void;
  onToggleThread: (threadId: string) => void;
  onNavigateThread: (wsId: string, threadId: string) => void;
  onSelectAgent: (agentId: string) => void;
  onRemoveAgent?: (agentId: string) => void;
  onLoadMoreThreads?: (wsId: string) => void;
  onPinWorkspace?: (wsId: string, pinned: boolean) => void;
  onNewThread?: (wsId: string) => void;
  onPinThread?: (wsId: string, threadId: string, pinned: boolean) => void;
  onArchiveThread?: (wsId: string, threadId: string) => void;
}

export function WorkspaceTreeRow({
  ws,
  isExpanded,
  isCurrent,
  isMobile,
  dragDisabled,
  threadsData,
  currentThreadId,
  expandedThreadIds,
  agents,
  activeAgentId,
  wsActions,
  rename,
  onToggleWorkspace,
  onToggleThread,
  onNavigateThread,
  onSelectAgent,
  onRemoveAgent,
  onLoadMoreThreads,
  onPinWorkspace,
  onNewThread,
  onPinThread,
  onArchiveThread,
}: WorkspaceTreeRowProps) {
  const { t } = useTranslation();
  const wsId = ws.workspace_id;
  const isFlash = ws.status === 'flash';
  const isPinned = Boolean(ws.is_pinned);
  const threads = threadsData?.threads || [];
  // Row-order signature for framer-motion's layoutDependency: rows re-measure
  // only when membership or order actually changes, not on every panel render.
  const threadOrderSignature = threads.map((th) => th.thread_id).join('|');
  const threadsLoading = threadsData?.loading || false;
  const isRenaming = rename.active;
  // The options menu isn't offered on the flash workspace (shared, immutable) —
  // mirrors the gallery hiding its card menu there. New-thread stays available
  // everywhere: Flash composes threads too.
  const showWsMenu = !isFlash;

  const handleRowClick = useCallback(() => {
    if (!isRenaming) onToggleWorkspace(wsId);
  }, [isRenaming, onToggleWorkspace, wsId]);

  return (
    <SortableWorkspace wsId={wsId} disabled={dragDisabled}>
      {({ dragHandleProps, isDragging }) => (<>
        {/* Workspace row — doubles as the drag handle for reordering. While
            renaming, click-to-toggle and drag are suppressed so the inline
            input owns the row. */}
        <div
          className="nav-panel-row group"
          style={{ paddingLeft: 10 }}
          onClick={handleRowClick}
          {...(isRenaming ? {} : dragHandleProps)}
        >
          {workspaceGlyph(ws, isExpanded)}
          {isRenaming ? (
            <input
              ref={rename.inputRef}
              className="text-sm font-medium bg-transparent outline-none border-b flex-1 min-w-0"
              style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-muted)' }}
              value={rename.value}
              onChange={(e) => rename.onChange(e.target.value)}
              onClick={(e) => e.stopPropagation()}
              onPointerDown={(e) => e.stopPropagation()}
              onKeyDown={(e) => {
                if (e.key === 'Enter') { e.preventDefault(); rename.onCommit(); }
                else if (e.key === 'Escape') { e.preventDefault(); rename.onCancel(); }
              }}
              onBlur={rename.onCommit}
              aria-label={t('workspace.rename')}
            />
          ) : (
            <>
              <span
                className="text-sm font-medium truncate"
                style={{ color: isCurrent ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)' }}
              >
                {ws.name || t('nav.workspaceFallback')}
              </span>
              {/* initial={false}: thread switches remount the panel; the chevron
                  must render at its resting angle, not animate to it. Hidden
                  until the row is hovered (always visible on touch). */}
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
              {(onNewThread || showWsMenu) && (
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
                  {showWsMenu && (
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
                      {/* Drops below the trigger, growing rightward past the
                          sidebar edge into the content area (Codex-style).
                          align=end would grow leftward over the tree since the
                          trigger hugs the sidebar edge. */}
                      <DropdownMenuContent align="start" sideOffset={4} onClick={(e) => e.stopPropagation()}>
                        <WorkspaceMenuItems
                          workspace={ws}
                          onTogglePin={onPinWorkspace ? () => onPinWorkspace(wsId, !isPinned) : undefined}
                          onRename={rename.enabled ? () => rename.onStart(wsId, ws.name || '') : undefined}
                          onUpgrade={wsActions.openUpgrade}
                          onToggleAlwaysOn={wsActions.toggleAlwaysOn}
                          onDuplicate={wsActions.openDuplicate}
                          onDelete={wsActions.openDelete}
                        />
                      </DropdownMenuContent>
                    </DropdownMenu>
                  )}
                </div>
              )}
              {threadsLoading && (
                <Loader
                 
                  size={14}
                 
                  className={`flex-shrink-0 text-[color:var(--color-text-tertiary)] ${onNewThread || showWsMenu ? '' : 'ml-auto'}`}
                />
              )}
            </>
          )}
        </div>

        {/* Threads under this workspace — animated expand/collapse. Hidden
            while this section is the one being dragged so the lifted item
            shrinks to header height (clean gap), and the DragOverlay chip
            carries the visual instead. */}
        <AnimatePresence initial={false}>
          {isExpanded && !isDragging && (
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
                  style={{ paddingLeft: 34, color: 'var(--color-icon-muted)' }}
                >
                  {t('nav.noConversations')}
                </div>
              )}
              <AnimatePresence initial={false}>
                {threads.map((thread) => (
                  // layout="position": pin/unpin repartitions and chat bumps
                  // glide to their new slot instead of teleporting; enter/exit
                  // collapse covers archive + unarchive/new rows.
                  // initial={false} on the Presence keeps the first paint of an
                  // expanded workspace static.
                  <motion.div
                    key={thread.thread_id}
                    layout="position"
                    // Without a layoutDependency every panel render re-measures
                    // every row (getBoundingClientRect + projection walk); the
                    // id signature scopes that to genuine reorders.
                    layoutDependency={threadOrderSignature}
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: 'auto', opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{
                      layout: { duration: 0.22, ease: [0.22, 1, 0.36, 1] },
                      height: { duration: 0.18, ease: 'easeInOut' },
                      opacity: { duration: 0.15, ease: 'easeInOut' },
                    }}
                    style={{ overflow: 'hidden' }}
                  >
                    <ThreadTreeRow
                      wsId={wsId}
                      thread={thread}
                      isCurrentThread={thread.thread_id === currentThreadId}
                      isExpanded={expandedThreadIds.has(thread.thread_id)}
                      isMobile={isMobile}
                      agents={thread.thread_id === currentThreadId ? agents : undefined}
                      activeAgentId={activeAgentId}
                      onToggleThread={onToggleThread}
                      onNavigateThread={onNavigateThread}
                      onSelectAgent={onSelectAgent}
                      onRemoveAgent={onRemoveAgent}
                      onPinThread={onPinThread}
                      onArchiveThread={onArchiveThread}
                    />
                  </motion.div>
                ))}
              </AnimatePresence>
              {/* Show more — next page of threads for this workspace */}
              {onLoadMoreThreads && typeof threadsData?.total === 'number'
                && threads.length < threadsData.total && !threadsLoading && (
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
}

/** Content-hugging lift preview for a dragged workspace section. */
export function WorkspaceDragChip({ ws, expanded }: { ws: NavWorkspace; expanded: boolean }) {
  const { t } = useTranslation();
  return (
    <div className="nav-panel nav-panel-drag-chip">
      <div className="nav-panel-row" style={{ paddingLeft: 10 }}>
        {workspaceGlyph(ws, expanded)}
        <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
          {ws.name || t('nav.workspaceFallback')}
        </span>
      </div>
    </div>
  );
}
