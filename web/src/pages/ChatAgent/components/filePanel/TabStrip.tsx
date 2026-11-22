import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Activity, ArrowLeft, BookMarked, BookOpen, CandlestickChart, ChevronLeft, ChevronRight, FolderOpen, FolderTree, Globe, LayoutDashboard, Maximize2, Minimize2, PanelRight, Plus, ScrollText, Settings, X, XCircle, type LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { ContextMenu, ContextMenuContent, ContextMenuItem, ContextMenuSeparator, ContextMenuTrigger } from '@/components/ui/context-menu';
import { fileGlyph } from './fileMeta';
import { isHtmlFile } from '../../utils/filePaths';
import { getCompletedRowTitle, getCompletedSummary, getToolIcon, isTaskTool } from '../toolDisplayConfig';
import { isOnLoan, type FileTab } from './useFileTabs';
import { isToolCallFailed } from './toolCallFailure';
import type { ToolCallProcessRecord } from '../ToolCallDetailView';
import { appFaviconGlyph } from '../messaging/appGlyph';
import { messagingAppName, platformOf, readMessageDelivery } from '../messaging/messageDelivery';
import { countDedupedSources, type ProvenanceRecord } from '@/types/chat';
import { useArtifactByPath } from '@/hooks/useArtifact';
import { AnimatePresence, motion } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import { useTranscriptReads, type TranscriptReader } from './useTranscript';
import { focusToggleWhenRendered } from './panelToggleFocus';
import { lastInputWasPointer } from '@/lib/inputModality';
import { useWorkspaceFolders, useWorkspaceId } from '../../contexts/WorkspaceContext';
import { useLaNames } from '../../hooks/useLaNames';
import type { LaNames } from '../laStepLabels';
import type { NewTabMenu, PanelLauncher } from './panelLaunchers';
import './TabStrip.css';

interface TabStripProps {
  tabs: FileTab[];
  activeId: string;
  onActivate: (id: string) => void;
  /** One tab from its X, or several from a tab's menu. */
  onClose: (ids: string[]) => void;
  onPin: (id: string) => void;
  /** Null where an empty tab would offer nothing: a read-only or single-file panel. */
  onNewTab: (() => void) | null;
  /** What a right click on the new tab button offers to open straight away. */
  newTabMenu?: NewTabMenu | null;
  /** The tab in front's own back and forward; null where nothing ever lands in a tab, a panel locked to one file. */
  history?: { onBack: (() => void) | null; onForward: (() => void) | null } | null;
  /** The file changed under this tab since it last read it: the amber dot. */
  hasChanged: (path: string) => boolean;
  /** What names a tool tab (its call's live record) and a sources tab (its turn's records). */
  transcript?: TranscriptReader | null;
  treeOpen: boolean;
  /** Null where the panel is locked to one file and has no tree to show. */
  onToggleTree: (() => void) | null;
  /** The reader's stores, offered here only where the tree that pins them
   *  never renders: a panel locked to one file has no other way to them. */
  onOpenMemory?: (() => void) | null;
  onOpenMemo?: (() => void) | null;
  /** Null where the host cannot lay the panel over what it sits beside. */
  onToggleExpanded?: (() => void) | null;
  expanded?: boolean;
  /** The panel's own close, where the surface around it does not own one. */
  /** `afterClose` runs once the panel is closing, which an unsaved edit can put off behind its question. */
  onPanelClose: ((afterClose?: () => void) => void) | null;
  /** Mobile leaves the panel by a back arrow rather than an X. */
  backArrow?: boolean;
  /** The close is the host's panel toggle, shown pressed: see FilePanel. */
  panelToggle?: boolean;
}

/** What a tool or sources tab is named from; the tab itself holds only an id. */
type TabSource = ToolCallProcessRecord | Record<string, ProvenanceRecord> | undefined;

function readTabSource(reader: TranscriptReader, tab: FileTab): TabSource {
  if (tab.kind === 'tool') return reader.toolCall(tab.toolCallId);
  if (tab.kind === 'sources') return reader.sources(tab.messageId);
  return undefined;
}

/** A tool tab's summary past this is cut with an ellipsis; the hover card carries the whole of it. */
const TOOL_SUMMARY_MAX = 28;

/**
 * How a tab reads on the strip. `name` is the pill; `detail` is what the pill
 * leaves out and the hover card gives back underneath in a quieter voice:
 * where the file lives, which port the app answers on, which interval the
 * chart is on. A running app with no title is named by its port, the only
 * thing that tells two of them apart; a chart is named by its ticker; a tool
 * tab is named the way its row is, and a sources tab by its count. `failed`
 * is the one status a tab carries: the tool behind it did not succeed.
 */
interface TabReading {
  name: string;
  Glyph: LucideIcon | React.ComponentType<{ className?: string }>;
  detail: string | null;
  failed?: boolean;
}

// A call's record is replaced, never edited, when the call changes, so a tool
// tab's reading is kept per record rather than worked out on every render.
const toolReadings = new WeakMap<ToolCallProcessRecord, { t: TFunction; root: string | null | undefined; names: LaNames | undefined; reading: TabReading }>();

function readToolCall(proc: ToolCallProcessRecord, t: TFunction, root: string | null | undefined, names: LaNames | undefined): TabReading {
  const toolName = proc.toolName || '';
  const call = proc.toolCall ? { ...proc.toolCall } : undefined;
  const artifact = proc.toolCallResult?.artifact;
  const title = isTaskTool(toolName) ? t('toolArtifact.subagentTask') : getCompletedRowTitle(toolName, call, t, { record: proc, root });
  if (toolName === 'send_message') {
    // The app's favicon names where it went; its name moves to the hint. A
    // send that did not go is marked as a failed call is.
    const delivery = readMessageDelivery(artifact as Record<string, unknown> | undefined, call?.args);
    const platform = delivery?.platform ?? platformOf(call?.args?.target);
    const glyph = appFaviconGlyph(platform);
    if (glyph) {
      return {
        name: title,
        Glyph: glyph,
        detail: messagingAppName(platform),
        failed: isToolCallFailed(proc) || delivery?.status === 'failed',
      };
    }
  }
  const summary = getCompletedSummary(toolName, call, t, { record: proc, root, names });
  const short = summary && summary.length > TOOL_SUMMARY_MAX ? `${summary.slice(0, TOOL_SUMMARY_MAX - 1)}…` : summary;
  return {
    name: short ? `${title} · ${short}` : title,
    Glyph: getToolIcon(toolName, call?.args, root),
    detail: summary && summary !== short ? summary : null,
    failed: isToolCallFailed(proc),
  };
}

function describe(tab: FileTab, t: TFunction, source: TabSource, root: string | null | undefined, names: LaNames | undefined): TabReading {
  switch (tab.kind) {
    case 'empty':
      return { name: t('filePanel.emptyTab'), Glyph: FolderOpen, detail: null };
    case 'file': {
      const slash = tab.path.lastIndexOf('/');
      return { name: tab.path.slice(slash + 1), Glyph: fileGlyph(tab.path), detail: slash > 0 ? tab.path.slice(0, slash) : null };
    }
    case 'settings':
      return { name: t('chat.workspaceSettings'), Glyph: Settings, detail: null };
    case 'memory':
      return { name: t('filePanel.tabs.memory'), Glyph: BookMarked, detail: null };
    case 'memo':
      return { name: t('filePanel.tabs.memo'), Glyph: ScrollText, detail: null };
    case 'status':
      return { name: t('filePanel.tabs.status'), Glyph: Activity, detail: null };
    case 'browser':
      return { name: t('filePanel.browserTab'), Glyph: Globe, detail: null };
    case 'preview':
      return { name: tab.title || `:${tab.port}`, Glyph: LayoutDashboard, detail: [`:${tab.port}`, tab.previewPath].filter(Boolean).join(' ') };
    case 'chart':
      return { name: tab.symbol, Glyph: CandlestickChart, detail: `${t('filePanel.chartTab')} · ${tab.timeframe}` };
    case 'tool': {
      // Named the way its row is, so the tab is found by what was clicked. A
      // record the transcript no longer holds leaves the tab with a plain name.
      const proc = source as ToolCallProcessRecord | undefined;
      if (!proc) return { name: t('toolArtifact.toolCall'), Glyph: getToolIcon('', undefined), detail: null };
      const kept = toolReadings.get(proc);
      if (kept?.t === t && kept.root === root && kept.names === names) return kept.reading;
      const reading = readToolCall(proc, t, root, names);
      toolReadings.set(proc, { t, root, names, reading });
      return reading;
    }
    case 'sources':
      return { name: t('filePanel.sourcesTab', { count: countDedupedSources(source as Record<string, ProvenanceRecord> | undefined) }), Glyph: BookOpen, detail: null };
  }
}

/**
 * An HTML file's tab carries the dot while anything the agent posted on the
 * file's comments is unread, so a reply that lands while another tab is in
 * front is found from the strip. Only the owner reads them, and a dot already
 * there when the record is read is drawn rather than played in.
 */
function ArtifactUnreadDot({ path }: { path: string }): React.ReactElement | null {
  const { t } = useTranslation();
  const workspaceId = useWorkspaceId();
  const root = useWorkspaceFolders()?.rootDir;
  const { data: artifact } = useArtifactByPath(workspaceId, path, { root });
  if (!artifact) return null;
  const label = t('filePanel.pageUnread', { count: artifact.unread });
  return (
    <AnimatePresence initial={false}>
      {artifact.unread > 0 && (
        <motion.span
          key="dot"
          className="file-panel-tab-dot is-unread"
          role="img"
          title={label}
          aria-label={label}
          initial={{ opacity: 0, scale: 0.6 }}
          animate={{ opacity: 1, scale: 1 }}
          exit={{ opacity: 0, scale: 0.6, transition: TWEEN.exit }}
          transition={TWEEN.quick}
        />
      )}
    </AnimatePresence>
  );
}

function launcherItem({ key, label, icon: Icon, open }: PanelLauncher): React.ReactElement {
  return (
    <ContextMenuItem key={key} onSelect={open}>
      <Icon className="h-3.5 w-3.5 shrink-0" />
      <span className="truncate">{label}</span>
    </ContextMenuItem>
  );
}

const tabIds = (tabs: readonly FileTab[]): string[] => tabs.map((tab) => tab.id);

interface TabMenuProps {
  tabs: FileTab[];
  at: number;
  onClose: (ids: string[]) => void;
  /** Where focus goes when the menu closes, the tab it opened on maybe gone with it. */
  onCloseAutoFocus: (event: Event) => void;
}

/** A tab's right-click menu: which tabs each close takes, counted from the tab clicked. */
function TabMenuContent({ tabs, at, onClose, onCloseAutoFocus }: TabMenuProps): React.ReactElement {
  const { t } = useTranslation();
  return (
    // Escape closes the menu and nothing else: the panel's own Escape would
    // leave the tree's selection.
    <ContextMenuContent className="max-w-64" onEscapeKeyDown={(event) => event.stopPropagation()} onCloseAutoFocus={onCloseAutoFocus}>
      <ContextMenuItem onSelect={() => onClose([tabs[at].id])}>{t('filePanel.tabMenu.close')}</ContextMenuItem>
      <ContextMenuItem disabled={tabs.length < 2} onSelect={() => onClose(tabIds(tabs.filter((_, i) => i !== at)))}>
        {t('filePanel.tabMenu.closeOthers')}
      </ContextMenuItem>
      <ContextMenuItem disabled={at === tabs.length - 1} onSelect={() => onClose(tabIds(tabs.slice(at + 1)))}>
        {t('filePanel.tabMenu.closeToRight')}
      </ContextMenuItem>
      <ContextMenuSeparator />
      <ContextMenuItem onSelect={() => onClose(tabIds(tabs))}>{t('filePanel.tabMenu.closeAll')}</ContextMenuItem>
    </ContextMenuContent>
  );
}

/** Where the close button sits over the glyph rather than at the tab's end, as TabStrip.css has it. */
const X_ON_GLYPH = '(hover: hover) and (pointer: fine)';

/** The width of the fade at an end of the tab row, in TabStrip.css. */
const TAB_ROW_FADE = 32;

/**
 * Whether a tab's name runs past the room its pill gives it. The room is the
 * name's box, padding included: a marked name's fade padding is room the text
 * gets back once unmarked, so a name that would fit unmarked reads as fitting
 * and the mark cannot stick. Compared as fractions, since clientWidth rounds
 * and a name that exactly fits would flip its mark on every pass.
 */
function nameOverflows(name: HTMLElement): boolean {
  const range = document.createRange();
  range.selectNodeContents(name);
  // jsdom lays nothing out and has no Range geometry.
  if (typeof range.getBoundingClientRect !== 'function') return false;
  return range.getBoundingClientRect().width > name.getBoundingClientRect().width + 0.5;
}

/**
 * Marks each pill whose name is cut, so TabStrip.css gives that one alone the
 * room its fade needs and a short name's pill stays as narrow as the name. A
 * name changes size by resizing its pill (a wider panel, a new cap) or by its
 * own text changing inside a pill held at the cap, and a pill is replaced
 * whenever its wrapper or key changes, so the pills' sizes and the row's
 * contents are both watched; the row's scroll never changes it.
 */
function useCappedTabs(listRef: React.RefObject<HTMLDivElement | null>): void {
  useEffect(() => {
    const list = listRef.current;
    if (!list) return;
    const mark = () => {
      for (const pill of list.querySelectorAll<HTMLElement>('[role="tab"]')) {
        const name = pill.querySelector<HTMLElement>('.file-panel-tab-name');
        pill.toggleAttribute('data-capped', !!name && nameOverflows(name));
      }
    };
    const sizes = new ResizeObserver(mark);
    const watchPills = () => {
      sizes.disconnect();
      for (const pill of list.children) sizes.observe(pill);
    };
    // Attributes are left out: the mark itself would wake the observer.
    const contents = new MutationObserver(() => { watchPills(); mark(); });
    contents.observe(list, { childList: true, subtree: true, characterData: true });
    watchPills();
    return () => { sizes.disconnect(); contents.disconnect(); };
  }, [listRef]);
}

/** Which ends of the tab row have tabs scrolled past them. */
interface Overflow { start: boolean; end: boolean }

/**
 * The tab row scrolls with its scrollbar hidden, so without help a tab past
 * the edge is cut mid-pill with nothing to say more follow, and a mouse has
 * no way to it. This fades the ends that have more behind them, turns the
 * wheel into a sideways scroll, and keeps the active tab in view.
 */
function useTabRowScroll(listRef: React.RefObject<HTMLDivElement | null>, activeId: string, rowKey: string): Overflow {
  const [overflow, setOverflow] = useState<Overflow>({ start: false, end: false });

  useEffect(() => {
    const list = listRef.current;
    if (!list) return;
    const measure = () => {
      const start = list.scrollLeft > 1;
      const end = list.scrollLeft + list.clientWidth < list.scrollWidth - 1;
      setOverflow((prev) => (prev.start === start && prev.end === end ? prev : { start, end }));
    };
    // A wheel is vertical; the row only moves sideways. Native and not
    // passive, so the page under the panel does not scroll along with it.
    const onWheel = (event: WheelEvent) => {
      if (list.scrollWidth <= list.clientWidth || Math.abs(event.deltaY) <= Math.abs(event.deltaX)) return;
      event.preventDefault();
      list.scrollLeft += event.deltaY;
    };
    // The tabs as well as the row: a tab coming or a name growing changes
    // what overflows without changing the row's own size. A pill is a new
    // element whenever `rowKey` moves (a new tab, a history step's new id,
    // an empty tab filled), so the observer starts over with it.
    const observer = new ResizeObserver(measure);
    observer.observe(list);
    for (const child of list.children) observer.observe(child);
    list.addEventListener('scroll', measure, { passive: true });
    list.addEventListener('wheel', onWheel, { passive: false });
    return () => {
      observer.disconnect();
      list.removeEventListener('scroll', measure);
      list.removeEventListener('wheel', onWheel);
    };
  }, [listRef, rowKey]);

  // Brought into view along the row only, since scrollIntoView would also
  // scroll whatever holds the panel, and clear of the fade: at the row's own
  // end the scroll stops short of the margin and that end has no fade.
  useEffect(() => {
    const list = listRef.current;
    const tab = list?.querySelector<HTMLElement>(`[data-tab-id="${CSS.escape(activeId)}"]`);
    if (!list || !tab) return;
    const left = tab.offsetLeft - list.offsetLeft - TAB_ROW_FADE;
    const right = tab.offsetLeft - list.offsetLeft + tab.offsetWidth + TAB_ROW_FADE;
    if (left < list.scrollLeft) list.scrollLeft = Math.max(0, left);
    else if (right > list.scrollLeft + list.clientWidth) list.scrollLeft = right - list.clientWidth;
  }, [listRef, activeId, rowKey]);

  return overflow;
}

/**
 * The strip of open files at the top of the panel.
 *
 * It carries `file-panel-header` because the height of that row is the one the
 * ChatView header is aligned against (web/AGENTS.md); the tab shape lives
 * inside it rather than setting the row's height itself. It is the panel's
 * top bar, so in the desktop shell it is a window drag band as the chat's is,
 * expanded over the chat or not; the tabs and buttons in it opt out through
 * the global no-drag rule.
 */
export function TabStrip({
  tabs,
  activeId,
  onActivate,
  onClose,
  onPin,
  onNewTab,
  newTabMenu = null,
  history = null,
  hasChanged,
  transcript = null,
  treeOpen,
  onToggleTree,
  onOpenMemory = null,
  onOpenMemo = null,
  onToggleExpanded = null,
  expanded = false,
  onPanelClose,
  backArrow = false,
  panelToggle = false,
}: TabStripProps): React.ReactElement {
  const { t } = useTranslation();
  // A tool tab is named the way its row is, and a row reads its paths under
  // the computer's root.
  const root = useWorkspaceFolders()?.rootDir;
  const listRef = useRef<HTMLDivElement>(null);
  // A long press opens the new tab menu and its release still clicks the
  // button, so a click while the menu is open is that release, not a new tab.
  const [newTabMenuOpen, setNewTabMenuOpen] = useState(false);
  // The same for a tab's menu, whose release would land on the tab (bringing
  // it forward) or on its close button, which under touch sits at its end.
  const [menuTab, setMenuTab] = useState<string | null>(null);
  // The row's width as it was before a close made with the pointer: see closeFrom.
  const [heldWidth, setHeldWidth] = useState<number | null>(null);
  const sources = useTranscriptReads(transcript, tabs, readTabSource);
  const laNames = useLaNames(
    tabs.flatMap((tab, i) => {
      const proc = tab.kind === 'tool' ? (sources[i] as ToolCallProcessRecord | undefined) : undefined;
      return proc ? [{ toolName: proc.toolName, args: proc.toolCall?.args }] : [];
    }),
  );
  const overflow = useTabRowScroll(listRef, activeId, tabs.map((tab) => `${tab.id}:${tab.kind}`).join(' '));
  useCappedTabs(listRef);

  /**
   * A tab closed from the keyboard hands the focus to the tab that takes its
   * place; a tab closed with the mouse does not, because the pointer is where
   * the reader already is. Same rule the card deck follows. The last tab
   * takes the panel with it, which leaves nothing in the strip to hand to.
   */
  const closeFrom = useCallback((id: string, keyboard: boolean) => {
    const index = tabs.findIndex((tab) => tab.id === id);
    // The tab to the right slides in under the pointer, its X where this one's
    // was, so closing a run of tabs is one click after another. Past the last
    // one the new tab button would slide in there too, so the row keeps its
    // width until the pointer leaves the strip. Only where the X sits on the
    // glyph (TabStrip.css): a finger's X sits at the tab's end, which no run
    // of closes lines up, and a touch leaves the strip before its click lands.
    if (!keyboard && listRef.current && window.matchMedia(X_ON_GLYPH).matches) {
      setHeldWidth(listRef.current.getBoundingClientRect().width);
    }
    onClose([id]);
    if (!keyboard || tabs.length <= 1) return;
    requestAnimationFrame(() => {
      const remaining = listRef.current?.querySelectorAll<HTMLElement>('[role="tab"]');
      if (!remaining?.length) return;
      remaining[Math.min(index, remaining.length - 1)].focus();
    });
  }, [tabs, onClose]);

  /**
   * A menu opened from the keyboard hands focus back to its tab, which a
   * close from the menu may have taken away; the tab now in front takes it
   * instead, as a keyboard close from the X hands it on. A pointer's menu
   * moves no focus at all (`closeAutoFocus`).
   */
  const focusAfterMenu = (event: Event, id: string) => {
    const list = listRef.current;
    if (lastInputWasPointer() || !list || list.querySelector(`[data-tab-id="${CSS.escape(id)}"]`)) return;
    event.preventDefault();
    list.querySelector<HTMLElement>('[role="tab"][tabindex="0"]')?.focus();
  };

  const onKeyDown = (event: React.KeyboardEvent, tab: FileTab) => {
    const step = event.key === 'ArrowRight' ? 1 : event.key === 'ArrowLeft' ? -1 : 0;
    if (step) {
      event.preventDefault();
      const all = Array.from(listRef.current?.querySelectorAll<HTMLElement>('[role="tab"]') ?? []);
      const at = all.findIndex((el) => el.dataset.tabId === tab.id);
      all[(at + step + all.length) % all.length]?.focus();
      return;
    }
    // Enter or Space on the close button bubbles here; it means close, not activate.
    if (event.target !== event.currentTarget) return;
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      onActivate(tab.id);
    }
  };

  return (
    <div className="file-panel-header file-panel-tabstrip" data-chrome="drag" onPointerLeave={() => setHeldWidth(null)}>
      {/* A hoverable hint closes only on the document's pointermove, and a
          page frame below the strip takes those, so a pointer leaving a tab
          into it left the hint up. The hint holds nothing to reach for. */}
      <TooltipProvider delayDuration={350} skipDelayDuration={600} disableHoverableContent>
      <div
        className="file-panel-tabs clips-focus-ring"
        role="tablist"
        aria-label={t('filePanel.openFiles')}
        ref={listRef}
        // A basis, not a floor: the row still yields when the strip has less
        // room than it held (the panel resized, a header button appeared), so
        // the buttons at the strip's end never leave the panel.
        style={heldWidth != null ? { flexBasis: heldWidth } : undefined}
        data-more-start={overflow.start || undefined}
        data-more-end={overflow.end || undefined}
      >
        {tabs.map((tab, i) => {
          const { name, Glyph, detail, failed } = describe(tab, t, sources[i], root, laNames);
          const hasHint = detail != null || tab.kind === 'file';
          const active = tab.id === activeId;
          const onLoan = isOnLoan(tab);
          const pill = (
            <div
              key={tab.id}
              role="tab"
              data-tab-id={tab.id}
              tabIndex={active ? 0 : -1}
              aria-selected={active}
              className={`file-panel-tab${onLoan ? ' is-preview' : ''}`}
              onClick={() => { if (menuTab !== tab.id) onActivate(tab.id); }}
              onDoubleClick={() => onPin(tab.id)}
              onAuxClick={(e) => { if (e.button === 1) { e.preventDefault(); closeFrom(tab.id, false); } }}
              onKeyDown={(e) => onKeyDown(e, tab)}
            >
              <Glyph className="h-3.5 w-3.5 shrink-0 file-panel-tab-glyph" />
              <span className="file-panel-tab-name">{name}</span>
              {failed && (
                <XCircle className="h-3.5 w-3.5 shrink-0 file-panel-tab-failed" role="img" aria-label={t('toolArtifact.a11y.toolCallFailed')} />
              )}
              {tab.kind === 'file' && hasChanged(tab.path) && (
                <span className="file-panel-tab-dot" title={t('filePanel.changedSinceRead')} aria-hidden="true" />
              )}
              {tab.kind === 'file' && isHtmlFile(tab.path) && <ArtifactUnreadDot path={tab.path} />}
              <button
                type="button"
                className="file-panel-tab-close"
                aria-label={t('filePanel.closeTab', { name })}
                onClick={(e) => { e.stopPropagation(); if (menuTab !== tab.id) closeFrom(tab.id, e.detail === 0); }}
                onMouseDown={(e) => e.stopPropagation()}
              >
                <X className="h-3 w-3" />
              </button>
            </div>
          );
          // The two triggers wrap the pill directly, since a Root renders no
          // element to carry the other's handlers. The hint is always mounted
          // and only held shut where there is nothing to say, so a tab gaining
          // a detail keeps its element (and the observers on it).
          return (
            <ContextMenu key={tab.id} onOpenChange={(open) => setMenuTab((was) => (open ? tab.id : was === tab.id ? null : was))}>
              <Tooltip open={hasHint ? undefined : false}>
                <TooltipTrigger asChild>
                  <ContextMenuTrigger asChild>{pill}</ContextMenuTrigger>
                </TooltipTrigger>
                {hasHint && (
                  <TooltipContent side="bottom" align="start" className="file-panel-tab-hint">
                    <div className="file-panel-tab-hint-name">{name}</div>
                    {detail && <div className="file-panel-tab-hint-detail">{detail}</div>}
                  </TooltipContent>
                )}
              </Tooltip>
              <TabMenuContent tabs={tabs} at={i} onClose={onClose} onCloseAutoFocus={(event) => focusAfterMenu(event, tab.id)} />
            </ContextMenu>
          );
        })}
      </div>
      </TooltipProvider>

      {/* Against the last tab, where the new one will appear; the list only
          grows to its tabs, so the button follows them along the strip. */}
      {onNewTab && (
        <ContextMenu onOpenChange={setNewTabMenuOpen}>
          <ContextMenuTrigger asChild disabled={!newTabMenu}>
            <button type="button" onClick={() => { if (!newTabMenuOpen) onNewTab(); }} className="file-panel-icon-btn shrink-0" title={t('filePanel.newTab')} aria-label={t('filePanel.newTab')}>
              <Plus className="h-4 w-4" />
            </button>
          </ContextMenuTrigger>
          {/* Escape closes the menu and nothing else: the panel's own Escape
              would leave the tree's selection. */}
          <ContextMenuContent className="max-w-64" onEscapeKeyDown={(event) => event.stopPropagation()}>
            {newTabMenu?.kinds.map(launcherItem)}
            {!!newTabMenu?.apps.length && <ContextMenuSeparator />}
            {newTabMenu?.apps.map(launcherItem)}
          </ContextMenuContent>
        </ContextMenu>
      )}

      <div className="file-panel-strip-right">
        {history && (
          <>
            <button type="button" onClick={history.onBack ?? undefined} disabled={!history.onBack} className="file-panel-icon-btn" title={t('filePanel.back')} aria-label={t('filePanel.back')}>
              <ChevronLeft className="h-4 w-4" />
            </button>
            <button type="button" onClick={history.onForward ?? undefined} disabled={!history.onForward} className="file-panel-icon-btn" title={t('filePanel.forward')} aria-label={t('filePanel.forward')}>
              <ChevronRight className="h-4 w-4" />
            </button>
          </>
        )}
        {onOpenMemory && (
          <button type="button" onClick={onOpenMemory} className="file-panel-icon-btn" title={t('filePanel.tabs.memory')} aria-label={t('filePanel.tabs.memory')}>
            <BookMarked className="h-4 w-4" />
          </button>
        )}
        {onOpenMemo && (
          <button type="button" onClick={onOpenMemo} className="file-panel-icon-btn" title={t('filePanel.tabs.memo')} aria-label={t('filePanel.tabs.memo')}>
            <ScrollText className="h-4 w-4" />
          </button>
        )}
        {onToggleExpanded && (
          <button
            type="button"
            onClick={onToggleExpanded}
            className="file-panel-icon-btn"
            title={expanded ? t('filePanel.collapsePanel') : t('filePanel.expandPanel')}
            aria-label={expanded ? t('filePanel.collapsePanel') : t('filePanel.expandPanel')}
          >
            {expanded ? <Minimize2 className="h-4 w-4" /> : <Maximize2 className="h-4 w-4" />}
          </button>
        )}
        {/* Next to the panel's toggle, as the tree is the panel's right-hand
            column. It comes and goes with the tab in front, so what sits
            before it shifts by a slot; the corner toggle never moves. */}
        {onToggleTree && (
          <button
            type="button"
            onClick={onToggleTree}
            aria-pressed={treeOpen}
            className="file-panel-icon-btn"
            title={t('filePanel.toggleTree')}
            aria-label={t('filePanel.toggleTree')}
          >
            <FolderTree className="h-4 w-4" />
          </button>
        )}
        {onPanelClose && (panelToggle && !backArrow ? (
          <button
            type="button"
            data-panel-toggle="panel"
            onClick={(event) => onPanelClose(event.detail === 0 ? () => focusToggleWhenRendered('header') : undefined)}
            aria-pressed
            className="file-panel-icon-btn"
            title={t('chat.workspaceFiles')}
            aria-label={t('chat.workspaceFiles')}
          >
            <PanelRight className="h-4 w-4" />
          </button>
        ) : (
          <button type="button" onClick={() => onPanelClose()} className="file-panel-icon-btn" title={t('filePanel.close')} aria-label={t('filePanel.close')}>
            {backArrow ? <ArrowLeft className="h-4 w-4" /> : <X className="h-4 w-4" />}
          </button>
        ))}
      </div>
    </div>
  );
}
