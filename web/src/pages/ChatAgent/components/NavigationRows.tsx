/**
 * Row components for the nav tree (NavigationPanel): one workspace section,
 * one thread, one agent, plus the drag chip. Lifted out of the panel so each
 * row is a real component — hooks can live per row (title fade, per-thread
 * liveness), and the panel keeps only tree-level state and handlers.
 *
 * Every element in a section that takes focus or runs a CSS animation carries
 * a `data-nav-key` unique within the section: the nav tree's kit swap
 * (./navTreeKit) finds its twin in the new tree by it, to carry focus and
 * animation clocks across. An element without one has no twin.
 */
import React, { memo, useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  ChevronRight, Folder, FolderOpen, Pin,
  X, ChevronsDown, MoreHorizontal, SquarePen, Archive,
  ExternalLink, Pencil, Share2, Link2, Trash2,
  type LucideIcon,
} from 'lucide-react';
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger } from '../../../components/ui/dropdown-menu';
import {
  ContextMenu, ContextMenuContent, ContextMenuItem, ContextMenuSeparator, ContextMenuTrigger,
} from '../../../components/ui/context-menu';
import { toast } from '@/components/ui/use-toast';
import { copyText } from '@/lib/clipboard';
import { isDesktopShell } from '@/lib/desktop';
import { isImeComposing } from '@/lib/ime';
import { HoverCard, HoverCardContent, HoverCardTrigger } from '../../../components/ui/hover-card';
import { mediumDate, relativeTime } from '@/lib/format';
import { Loader } from '@/components/ui/loader';
import { useTitleFade } from '@/hooks/useTitleFade';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { HomeRowIcon } from '@/hooks/useHomeWorkspace';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import { useThreadFlags } from '@/lib/threadLifecycle/store';
import { sidebarAgentRowLabel, type SidebarAgentRow } from '../session/subagents/subagentStatus';
import { SubagentStatusIcon } from './taskStatusUi';
import type { NavWorkspace } from '../hooks/useNavigationData';
import { WorkspaceMenuItems } from './workspaceActions';
import type { WorkspaceActions } from './workspaceActions';
import { clampWorkspaceName } from '../utils/workspaceName';
import { useNavTree, type DragGate } from './navTreeKit';
import { computerIdentity } from './ComputerBadge';
import { FolderCommand, FolderTerminal } from './computerFolderIcons';
import { useComputer } from '../hooks/useComputers';

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

interface RowMachine {
  icon: LucideIcon;
  title: string;
}

/**
 * The machine a workspace row lives on, when the row should say so: a Mac or
 * another machine reached over SSH, which is there only while that machine is
 * on and holds the files itself. Still a folder, with the machine's mark in
 * it: ⌘ for a Mac, >_ for SSH. A hosted sandbox is where workspaces live by
 * default, so it keeps the plain folder.
 */
function useRowMachine(ws: NavWorkspace): RowMachine | null {
  const { t } = useTranslation();
  const id = typeof ws.computer_id === 'string' && !isHomeWorkspace(ws) ? ws.computer_id : null;
  const { data: computer } = useComputer(id);
  if (!computer) return null;
  const identity = computerIdentity(t, computer, false);
  if (identity.kind === 'hosted') return null;
  return {
    icon: identity.kind === 'ssh' ? FolderTerminal : FolderCommand,
    title: t('nav.onComputer', { name: computer.name, kind: identity.label }),
  };
}

/** The workspace row's leading glyph. Shared with the drag chip so a lifted
 *  section keeps the icon it had in the tree. A pin outranks the machine: it
 *  says why the row sits on top, and the machine stays in the tooltip. */
function workspaceGlyph(ws: NavWorkspace, expanded: boolean, machine: RowMachine | null) {
  const style = { color: 'var(--color-text-tertiary)' };
  const className = 'h-4 w-4 shrink-0';
  if (isHomeWorkspace(ws)) return <HomeRowIcon className={className} style={style} />;
  const glyph = ws.is_pinned
    ? <Pin className={className} style={style} />
    : machine
      ? <machine.icon className={className} style={style} />
      : expanded ? <FolderOpen className={className} style={style} /> : <Folder className={className} style={style} />;
  // The tooltip is for a pointer; the hidden text says it to a screen reader.
  return machine ? (
    <span className="flex shrink-0" title={machine.title}>
      {glyph}
      <span className="sr-only">{machine.title}</span>
    </span>
  ) : glyph;
}

// The disclosure chevron turns a quarter when its row opens. A CSS transition,
// so a remount renders at the resting angle and only a toggle turns it.
const CHEVRON_CLASS = 'h-4 w-4 transition-transform duration-fold';

// Its own component so useTitleFade can run per row (hooks can't live in a map
// callback). Fades softly when a live title rewrite lands (auto-title,
// rename); static on first paint. No native `title` attr — the metadata
// hover card carries the full title (a browser tooltip would double up).
/* The row's keyboard stop: Enter opens the thread, and Shift+F10 or the Menu
   key opens the row's menu. The title takes it, not the row, so no control
   sits inside a button (the row holds the pin, archive and agents buttons). */
function ThreadRowTitle({ tid, title, active, onActivate }: {
  tid: string; title: string; active: boolean; onActivate: () => void;
}) {
  const fading = useTitleFade(title);
  return (
    <span
      data-nav-key={`thread:${tid}:title`}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key !== 'Enter' && e.key !== ' ') return;
        e.preventDefault();
        onActivate();
      }}
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
          className="text-(--color-accent-primary)"
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


// Compact metadata card shown after hovering a thread row: full title plus
// the cheap facts the list row already carries. Metadata-only, so it never
// captures the pointer (pointer-events-none) — hovering through it onto
// neighboring rows keeps working.
function ThreadMetaCard({ thread, title, t }: {
  thread: ThreadEntry;
  title: string;
  t: (key: string) => string;
}) {
  return (
    <HoverCardContent side="right" align="start" sideOffset={12} className="w-60 p-3 pointer-events-none select-none">
      <div className="text-[0.8125rem] font-medium leading-snug" style={{ color: 'var(--color-text-primary)' }}>
        {title}
      </div>
      <ThreadMetaRows thread={thread} t={t} />
    </HoverCardContent>
  );
}

// Mounted only while the card is open, so a closed row holds no clock.
function ThreadMetaRows({ thread, t }: { thread: ThreadEntry; t: (key: string) => string }) {
  const locale = useLocale();
  const now = useNow();
  const rows: Array<[string, string]> = [];
  if (thread.updated_at) rows.push([t('nav.threadUpdated'), relativeTime(thread.updated_at, locale, now)]);
  if (thread.created_at) rows.push([t('nav.threadCreated'), mediumDate(new Date(thread.created_at), locale)]);
  if (typeof thread.turn_count === 'number') rows.push([t('nav.threadTurns'), String(thread.turn_count)]);
  if (rows.length === 0) return null;
  return (
    <div className="mt-2 flex flex-col gap-1">
      {rows.map(([label, value]) => (
        <div key={label} className="flex items-baseline justify-between gap-3 text-xs">
          <span style={{ color: 'var(--color-text-tertiary)' }}>{label}</span>
          <span className="text-right" style={{ color: 'var(--color-text-secondary)' }}>{value}</span>
        </div>
      ))}
    </div>
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

  const rowLabel = sidebarAgentRowLabel(agent);

  return (
    <div
      data-testid="agent-row"
      data-nav-key={`agent:${agent.id}`}
      data-agent-role={isMainAgent ? 'main' : 'sub'}
      className={`nav-panel-agent-row group${isSelected ? ' is-selected' : ''}`}
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
        <span className="shrink-0 ml-auto flex items-center">
          <SubagentStatusIcon status={status} className="h-3 w-3" />
        </span>
      )}
      {/* Remove button -- non-main agents only, on hover */}
      {!isMainAgent && (
        <button
          data-nav-key={`agent:${agent.id}:remove`}
          onClick={(e) => { e.stopPropagation(); onRemoveAgent?.(agent.id); }}
          className={`shrink-0 p-0 bg-transparent border-none cursor-pointer transition-opacity ${isMobile ? 'opacity-60' : 'opacity-0 group-hover:opacity-100'}`}
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
  onRenameThread?: (wsId: string, threadId: string, title: string, previousTitle: string) => void;
  onShareThread?: (wsId: string, threadId: string) => void;
  onDeleteThread?: (wsId: string, threadId: string, title: string) => void;
}

/** The thread's own address in the app, for a new tab or the clipboard. */
function threadHref(threadId: string): string {
  return new URL(`/chat/t/${threadId}`, window.location.origin).href;
}

interface ThreadRowMenuProps {
  isPinned: boolean;
  onCloseAutoFocus: (event: Event) => void;
  /** Absent in the desktop app, which hands a new window of its own pages to the system browser. */
  onOpenInNewTab?: () => void;
  onCopyLink: () => void;
  onTogglePin?: () => void;
  onRename?: () => void;
  onShare?: () => void;
  onArchive?: () => void;
  onDelete?: () => void;
}

// A thread row's right-click menu. Mounted only while open.
function ThreadRowMenu({
  isPinned, onCloseAutoFocus, onOpenInNewTab, onCopyLink, onTogglePin, onRename, onShare, onArchive, onDelete,
}: ThreadRowMenuProps) {
  const { t } = useTranslation();
  return (
    <ContextMenuContent className="min-w-44" onCloseAutoFocus={onCloseAutoFocus}>
      {onOpenInNewTab && (
        <ContextMenuItem onSelect={onOpenInNewTab}>
          <ExternalLink className="h-4 w-4" />
          {t('nav.openInNewTab')}
        </ContextMenuItem>
      )}
      {onOpenInNewTab && (onTogglePin || onRename) && <ContextMenuSeparator />}
      {onTogglePin && (
        <ContextMenuItem onSelect={onTogglePin}>
          <Pin className="h-4 w-4" />
          {isPinned ? t('nav.unpinThread') : t('nav.pinToTop')}
        </ContextMenuItem>
      )}
      {onRename && (
        <ContextMenuItem onSelect={onRename}>
          <Pencil className="h-4 w-4" />
          {t('nav.renameThread')}
        </ContextMenuItem>
      )}
      {(onOpenInNewTab || onTogglePin || onRename) && <ContextMenuSeparator />}
      {onShare && (
        <ContextMenuItem onSelect={onShare}>
          <Share2 className="h-4 w-4" />
          {t('nav.shareThread')}
        </ContextMenuItem>
      )}
      <ContextMenuItem onSelect={onCopyLink}>
        <Link2 className="h-4 w-4" />
        {t('share.copyLink')}
      </ContextMenuItem>
      {(onArchive || onDelete) && <ContextMenuSeparator />}
      {onArchive && (
        <ContextMenuItem onSelect={onArchive}>
          <Archive className="h-4 w-4" />
          {t('nav.archiveThread')}
        </ContextMenuItem>
      )}
      {onDelete && (
        <ContextMenuItem variant="destructive" onSelect={onDelete}>
          <Trash2 className="h-4 w-4" />
          {t('common.delete')}
        </ContextMenuItem>
      )}
    </ContextMenuContent>
  );
}

function ThreadTreeRowImpl({
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
  onRenameThread,
  onShareThread,
  onDeleteThread,
}: ThreadTreeRowProps) {
  const { t } = useTranslation();
  const { kit } = useNavTree();
  const tid = thread.thread_id;
  const subagents = agents?.filter((a) => !a.isMainAgent) || [];
  const hasSubagents = isCurrentThread && subagents.length > 0;
  const seedTitle = thread.title || thread.first_query_content?.slice(0, 40) || '';
  const title = seedTitle || t('nav.untitledThread');
  const isPinned = Boolean(thread.is_pinned);

  // The metadata card is held shut while the right-click menu is up, so the
  // two never stack; it opens again on the next hover.
  const [menuOpen, setMenuOpen] = useState(false);
  const [cardOpen, setCardOpen] = useState(false);
  const handleMenuOpenChange = useCallback((open: boolean) => {
    setMenuOpen(open);
    if (open) setCardOpen(false);
  }, []);

  // Inline rename, as the workspace row does it: the title becomes a field.
  // The ref makes Enter and the blur that follows it commit once; the seed is
  // what the field opened with, so leaving it untouched writes nothing.
  // Rename starts only once the menu has gone: until then a pointer crossing
  // its items moves focus into the menu, which would blur and close the field.
  const [renaming, setRenaming] = useState(false);
  const [draft, setDraft] = useState('');
  const renameInputRef = useRef<HTMLInputElement>(null);
  const renameSeedRef = useRef('');
  const renameOpenRef = useRef(false);
  const renameAfterMenuRef = useRef(false);
  const startRename = () => {
    renameSeedRef.current = seedTitle;
    renameOpenRef.current = true;
    setDraft(seedTitle);
    setRenaming(true);
  };
  const endRename = (commit: boolean) => {
    if (!renameOpenRef.current) return;
    renameOpenRef.current = false;
    setRenaming(false);
    // The field's focus opened the card underneath, and a focused element
    // that unmounts sends no blur to close it.
    setCardOpen(false);
    const next = draft.trim();
    if (commit && next && next !== renameSeedRef.current) onRenameThread?.(wsId, tid, next, thread.title || '');
  };
  const handleMenuCloseAutoFocus = (event: Event) => {
    if (!renameAfterMenuRef.current) return;
    renameAfterMenuRef.current = false;
    event.preventDefault();
    startRename();
  };
  useEffect(() => {
    if (!renaming) return;
    const raf = requestAnimationFrame(() => {
      renameInputRef.current?.focus();
      renameInputRef.current?.select();
    });
    return () => cancelAnimationFrame(raf);
  }, [renaming]);

  const activateRow = () => {
    // A touch long-press opens the menu; the tap that ends it is not a navigation.
    if (renaming || menuOpen) return;
    if (isCurrentThread) {
      // Toggle agents expand for current thread
      if (hasSubagents) onToggleThread(tid);
    } else {
      onNavigateThread(wsId, tid);
    }
  };

  const copyLink = () => {
    copyText(threadHref(tid)).then(
      () => toast({ title: t('nav.linkCopied') }),
      () => toast({ variant: 'destructive', title: t('nav.copyLinkFailed') }),
    );
  };

  return (
    <>
      {/* Thread row */}
      <ContextMenu onOpenChange={handleMenuOpenChange}>
        <HoverCard openDelay={500} closeDelay={100} open={cardOpen && !menuOpen && !renaming} onOpenChange={setCardOpen}>
          <HoverCardTrigger asChild>
            <ContextMenuTrigger asChild disabled={renaming}>
              <div
                className={`nav-panel-row group relative ${isCurrentThread ? 'nav-panel-row-active' : ''}${menuOpen ? ' is-menu-open' : ''}`}
                style={{ paddingLeft: 28 }}
                onClick={activateRow}
              >
                <ThreadRowGlyph tid={tid} isCurrentThread={isCurrentThread} />
                {renaming ? (
                  <input
                    data-nav-key={`thread:${tid}:rename`}
                    ref={renameInputRef}
                    className="text-sm bg-transparent border-b flex-1 min-w-0"
                    style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-muted)' }}
                    value={draft}
                    maxLength={255}
                    onChange={(e) => setDraft(e.target.value)}
                    onClick={(e) => e.stopPropagation()}
                    onKeyDown={(e) => {
                      if (isImeComposing(e)) return;
                      if (e.key === 'Enter') { e.preventDefault(); endRename(true); }
                      else if (e.key === 'Escape') { e.preventDefault(); endRename(false); }
                    }}
                    onBlur={() => endRename(true)}
                    aria-label={t('nav.threadTitle')}
                  />
                ) : (<>
                  <ThreadRowTitle tid={tid} title={title} active={isCurrentThread} onActivate={activateRow} />
                  {/* Expand-agents chevron — immediately right of the thread name,
                      mirroring the workspace row. Only on the current thread when it
                      has subagents. Hover-revealed (always shown on touch); rotates
                      90° when expanded. */}
                  {hasSubagents && (
                    <button
                      type="button"
                      data-nav-key={`thread:${tid}:agents`}
                      onClick={(e) => { e.stopPropagation(); onToggleThread(tid); }}
                      className={`shrink-0 flex items-center p-0 bg-transparent border-none cursor-pointer ${isMobile ? '' : 'opacity-0 group-hover:opacity-100 transition-opacity'}`}
                      aria-label={t(isExpanded ? 'nav.collapseAgents' : 'nav.expandAgents')}
                    >
                      <ChevronRight
                        className={`${CHEVRON_CLASS}${isExpanded ? ' rotate-90' : ''}`}
                        style={{ color: 'var(--color-text-tertiary)' }}
                      />
                    </button>
                  )}
                  {/* Resting pinned marker — fades (never display-toggles, which
                      would reflow the title) while the hover overlay carries the
                      interactive pin (desktop; mobile shows the in-flow cluster,
                      whose Pin fill carries state). */}
                  {isPinned && !isMobile && (
                    <Pin
                      aria-hidden
                      className="h-3 w-3 ml-auto shrink-0 transition-opacity group-hover:opacity-0"
                      fill="currentColor"
                      style={{ color: 'var(--color-text-quaternary)' }}
                    />
                  )}
                  {(onPinThread || onArchiveThread) && (
                    <div className={isMobile ? 'flex items-center gap-0.5 ml-auto shrink-0' : 'nav-panel-row-actions'}>
                      {onPinThread && (
                        <button
                          type="button"
                          data-nav-key={`thread:${tid}:pin`}
                          onClick={(e) => { e.stopPropagation(); onPinThread(wsId, tid, !isPinned); }}
                          className="flex items-center justify-center p-0.5 rounded bg-transparent border-none cursor-pointer hover:bg-(--color-bg-hover) press"
                          title={isPinned ? t('nav.unpinThread') : t('nav.pinThread')}
                          aria-label={isPinned ? t('nav.unpinThread') : t('nav.pinThread')}
                        >
                          <kit.PinGlyph pinned={isPinned}>
                            <Pin
                              className="h-3.5 w-3.5"
                              fill={isPinned ? 'currentColor' : 'none'}
                              style={{ color: 'var(--color-text-tertiary)' }}
                            />
                          </kit.PinGlyph>
                        </button>
                      )}
                      {onArchiveThread && (
                        <button
                          type="button"
                          data-nav-key={`thread:${tid}:archive`}
                          onClick={(e) => { e.stopPropagation(); onArchiveThread(wsId, tid); }}
                          className="flex items-center justify-center p-0.5 rounded bg-transparent border-none cursor-pointer hover:bg-(--color-bg-hover) press"
                          title={t('nav.archiveThread')}
                          aria-label={t('nav.archiveThread')}
                        >
                          <Archive className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                        </button>
                      )}
                    </div>
                  )}
                </>)}
              </div>
            </ContextMenuTrigger>
          </HoverCardTrigger>
          <ThreadMetaCard thread={thread} title={title} t={t} />
        </HoverCard>
        <ThreadRowMenu
          isPinned={isPinned}
          onCloseAutoFocus={handleMenuCloseAutoFocus}
          onOpenInNewTab={isDesktopShell() ? undefined : () => window.open(threadHref(tid), '_blank', 'noopener')}
          onCopyLink={copyLink}
          onTogglePin={onPinThread ? () => onPinThread(wsId, tid, !isPinned) : undefined}
          onRename={onRenameThread ? () => { renameAfterMenuRef.current = true; } : undefined}
          onShare={onShareThread ? () => onShareThread(wsId, tid) : undefined}
          onArchive={onArchiveThread ? () => onArchiveThread(wsId, tid) : undefined}
          onDelete={onDeleteThread ? () => onDeleteThread(wsId, tid, title) : undefined}
        />
      </ContextMenu>

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
  dragDisabled: DragGate;
  threadsData?: ThreadsData;
  currentThreadId?: string | null;
  expandedThreadIds: Set<string>;
  agents?: SidebarAgentRow[];
  activeAgentId?: string | null;
  /** The shared duplicate / move / delete flows. */
  wsActions: Omit<WorkspaceActions, 'dialogs'>;
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
  onRenameThread?: (wsId: string, threadId: string, title: string, previousTitle: string) => void;
  onShareThread?: (wsId: string, threadId: string) => void;
  onDeleteThread?: (wsId: string, threadId: string, title: string) => void;
}

function WorkspaceTreeRowImpl({
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
  onRenameThread,
  onShareThread,
  onDeleteThread,
}: WorkspaceTreeRowProps) {
  const { t } = useTranslation();
  const label = useWorkspaceLabel();
  const { kit } = useNavTree();
  const wsId = ws.workspace_id;
  const isHome = isHomeWorkspace(ws);
  const isPinned = Boolean(ws.is_pinned);
  const machine = useRowMachine(ws);
  const threads = threadsData?.threads || [];
  // Row-order signature for the thread rows' glide: rows re-measure only when
  // membership or order actually changes, not on every panel render.
  const threadOrderSignature = threads.map((th) => th.thread_id).join('|');
  const threadsLoading = threadsData?.loading || false;
  const isRenaming = rename.active;
  // The options menu isn't offered on Home (shared, immutable), mirroring
  // the gallery hiding its card menu there. New-thread stays available
  // everywhere: Home composes threads too.
  const showWsMenu = !isHome;
  const [menuOpen, setMenuOpen] = useState(false);
  // Rename starts once the menu has gone, as on the thread row: a pointer
  // crossing a closing menu's items moves focus into it and ends the edit.
  const renameAfterMenuRef = useRef(false);
  const requestRename = rename.enabled ? () => { renameAfterMenuRef.current = true; } : undefined;
  const handleMenuCloseAutoFocus = (event: Event) => {
    if (!renameAfterMenuRef.current) return;
    renameAfterMenuRef.current = false;
    event.preventDefault();
    rename.onStart(wsId, ws.name || '');
  };

  // A touch long-press opens the menu; the tap that ends it is not a toggle.
  const handleRowClick = useCallback(() => {
    if (!isRenaming && !menuOpen) onToggleWorkspace(wsId);
  }, [isRenaming, menuOpen, onToggleWorkspace, wsId]);

  return (
    <kit.Section wsId={wsId} disabled={dragDisabled}>
      {({ dragHandleProps, isDragging }) => (<>
        {/* Workspace row — doubles as the drag handle for reordering. While
            renaming, click-to-toggle and drag are suppressed so the inline
            input owns the row. A right-click opens the options menu. */}
        <ContextMenu onOpenChange={setMenuOpen}>
          <ContextMenuTrigger asChild disabled={!showWsMenu || isRenaming}>
            <div
              data-nav-key="header"
              className={`nav-panel-row group${menuOpen ? ' is-menu-open' : ''}`}
              style={{ paddingLeft: 10, position: 'relative' }}
              onClick={handleRowClick}
              {...(isRenaming ? {} : dragHandleProps)}
            >
              {workspaceGlyph(ws, isExpanded, machine)}
              {isRenaming ? (
                <input
                  data-nav-key="rename"
                  ref={rename.inputRef}
                  className="text-sm font-medium bg-transparent border-b flex-1 min-w-0"
                  style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-muted)' }}
                  value={rename.value}
                  onChange={(e) => rename.onChange(clampWorkspaceName(e.target.value))}
                  onClick={(e) => e.stopPropagation()}
                  onPointerDown={(e) => e.stopPropagation()}
                  onKeyDown={(e) => {
                    if (isImeComposing(e)) return;
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
                    {label(ws) || t('nav.workspaceFallback')}
                  </span>
                  {/* Chevron + row actions float over the name's tail on hover
                      (as thread rows do), so the name keeps the full row width
                      instead of truncating around controls that are not showing. */}
                  <div className={isMobile ? 'flex items-center gap-0.5 ml-auto shrink-0' : 'nav-panel-row-actions'}>
                    <span className="shrink-0 flex items-center">
                      <ChevronRight
                        className={`${CHEVRON_CLASS}${isExpanded ? ' rotate-90' : ''}`}
                        style={{ color: 'var(--color-text-tertiary)' }}
                      />
                    </span>
                    {/* Right-aligned row actions: new thread + options (pin / rename).
                        Hover-revealed on desktop, always shown on touch. */}
                    {(onNewThread || showWsMenu) && (
                      <div className="flex items-center gap-0.5 shrink-0">
                        {onNewThread && (
                          <button
                            type="button"
                            data-nav-key="new-thread"
                            onPointerDown={(e) => e.stopPropagation()}
                            onClick={(e) => { e.stopPropagation(); onNewThread(wsId); }}
                            className="flex items-center justify-center p-0.5 rounded bg-transparent border-none cursor-pointer hover:bg-(--color-border-muted)"
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
                                data-nav-key="options"
                                onPointerDown={(e) => e.stopPropagation()}
                                onClick={(e) => e.stopPropagation()}
                                className="flex items-center justify-center p-0.5 rounded bg-transparent border-none cursor-pointer hover:bg-(--color-border-muted)"
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
                                onDuplicate={wsActions.openDuplicate}
                                onMove={wsActions.openMove}
                                onDelete={wsActions.openDelete}
                              />
                            </DropdownMenuContent>
                          </DropdownMenu>
                        )}
                      </div>
                    )}
                  </div>
                  {threadsLoading && (
                    <Loader
                 
                      size={14}
                 
                      className={`shrink-0 text-(--color-text-tertiary) ${onNewThread || showWsMenu ? '' : 'ml-auto'}`}
                    />
                  )}
                </>
              )}
            </div>
          </ContextMenuTrigger>
          {showWsMenu && (
            <ContextMenuContent onCloseAutoFocus={handleMenuCloseAutoFocus}>
              <WorkspaceMenuItems
                workspace={ws}
                menu="context"
                onTogglePin={onPinWorkspace ? () => onPinWorkspace(wsId, !isPinned) : undefined}
                onRename={requestRename}
                onDuplicate={wsActions.openDuplicate}
                onMove={wsActions.openMove}
                onDelete={wsActions.openDelete}
              />
            </ContextMenuContent>
          )}
        </ContextMenu>

        {/* Threads under this workspace — animated expand/collapse. Hidden
            while this section is the one being dragged so the lifted item
            shrinks to header height (clean gap), and the DragOverlay chip
            carries the visual instead. */}
        <kit.Collapse show={isExpanded && !isDragging}>
          {!threadsLoading && threads.length === 0 && (
            <div
              className="text-xs px-2 py-1"
              style={{ paddingLeft: 34, color: 'var(--color-icon-muted)' }}
            >
              {t('nav.noConversations')}
            </div>
          )}
          <kit.ThreadList>
            {threads.map((thread) => (
              <kit.ThreadItem key={thread.thread_id} order={threadOrderSignature}>
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
                  onRenameThread={onRenameThread}
                  onShareThread={onShareThread}
                  onDeleteThread={onDeleteThread}
                />
              </kit.ThreadItem>
            ))}
          </kit.ThreadList>
          {/* Show more: the next page of threads for this workspace */}
          {onLoadMoreThreads && typeof threadsData?.total === 'number'
            && threads.length < threadsData.total && !threadsLoading && (
            <div
              className="nav-panel-row"
              style={{ paddingLeft: 44 }}
              onClick={(e) => { e.stopPropagation(); onLoadMoreThreads(wsId); }}
            >
              <ChevronsDown className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('nav.showMore')}
              </span>
            </div>
          )}
        </kit.Collapse>
      </>)}
    </kit.Section>
  );
}

/** Content-hugging lift preview for a dragged workspace section. */
export function WorkspaceDragChip({ ws, expanded }: { ws: NavWorkspace; expanded: boolean }) {
  const { t } = useTranslation();
  const label = useWorkspaceLabel();
  const machine = useRowMachine(ws);
  return (
    <div className="nav-panel nav-panel-drag-chip">
      <div className="nav-panel-row" style={{ paddingLeft: 10 }}>
        {workspaceGlyph(ws, expanded, machine)}
        <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
          {label(ws) || t('nav.workspaceFallback')}
        </span>
      </div>
    </div>
  );
}

// Memoized: the sidebar is a child of the chat view, which commits on every
// streamed token and again when the thread list and workspace files refetch
// at turn end. Without this, every row rebuilt its elements on each of those
// commits, and the one at turn end landed as a 30ms frame in the middle of
// the fold closing. Every prop is a primitive, a cache-shared record, or a
// callback the panel holds stable, so unchanged rows now cost nothing.
export const ThreadTreeRow = memo(ThreadTreeRowImpl);
export const WorkspaceTreeRow = memo(WorkspaceTreeRowImpl);
