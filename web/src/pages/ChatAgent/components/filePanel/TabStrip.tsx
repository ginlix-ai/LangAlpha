import React, { useCallback, useRef } from 'react';
import { Activity, ArrowLeft, BookMarked, BookOpen, CandlestickChart, FolderOpen, LayoutDashboard, PanelRight, Plus, ScrollText, Settings, X, XCircle, type LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { fileGlyph } from './fileMeta';
import { getCompletedRowTitle, getCompletedSummary, getToolIcon, isTaskTool } from '../toolDisplayConfig';
import { isOnLoan, type FileTab } from './useFileTabs';
import { isToolCallFailed } from './toolCallFailure';
import type { ToolCallProcessRecord } from '../ToolCallDetailView';
import { appFaviconGlyph } from '../messaging/appGlyph';
import { messagingAppName, platformOf, readMessageDelivery } from '../messaging/messageDelivery';
import { countDedupedSources, type ProvenanceRecord } from '@/types/chat';
import { useTranscriptReads, type TranscriptReader } from './useTranscript';
import './TabStrip.css';

interface TabStripProps {
  tabs: FileTab[];
  activeId: string;
  onActivate: (id: string) => void;
  onClose: (id: string) => void;
  onPin: (id: string) => void;
  /** Null where an empty tab would offer nothing: a read-only or single-file panel. */
  onNewTab: (() => void) | null;
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
  /** The panel's own close, where the surface around it does not own one. */
  onPanelClose: (() => void) | null;
  /** Mobile leaves the panel by a back arrow rather than an X. */
  backArrow?: boolean;
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
const toolReadings = new WeakMap<ToolCallProcessRecord, { t: TFunction; reading: TabReading }>();

function readToolCall(proc: ToolCallProcessRecord, t: TFunction): TabReading {
  const toolName = proc.toolName || '';
  const call = proc.toolCall ? { ...proc.toolCall } : undefined;
  const artifact = proc.toolCallResult?.artifact;
  const title = isTaskTool(toolName) ? t('toolArtifact.subagentTask') : getCompletedRowTitle(toolName, call, t, artifact);
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
  const summary = getCompletedSummary(toolName, call, t);
  const short = summary && summary.length > TOOL_SUMMARY_MAX ? `${summary.slice(0, TOOL_SUMMARY_MAX - 1)}…` : summary;
  return {
    name: short ? `${title} · ${short}` : title,
    Glyph: getToolIcon(toolName, call?.args),
    detail: summary && summary !== short ? summary : null,
    failed: isToolCallFailed(proc),
  };
}

function describe(tab: FileTab, t: TFunction, source: TabSource): TabReading {
  switch (tab.kind) {
    case 'empty':
      return { name: t('filePanel.openFile'), Glyph: FolderOpen, detail: null };
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
      if (kept?.t === t) return kept.reading;
      const reading = readToolCall(proc, t);
      toolReadings.set(proc, { t, reading });
      return reading;
    }
    case 'sources':
      return { name: t('filePanel.sourcesTab', { count: countDedupedSources(source as Record<string, ProvenanceRecord> | undefined) }), Glyph: BookOpen, detail: null };
  }
}

/**
 * The strip of open files at the top of the panel.
 *
 * It carries `file-panel-header` because the height of that row is the one the
 * ChatView header is aligned against (web/AGENTS.md); the tab shape lives
 * inside it rather than setting the row's height itself.
 */
export function TabStrip({
  tabs,
  activeId,
  onActivate,
  onClose,
  onPin,
  onNewTab,
  hasChanged,
  transcript = null,
  treeOpen,
  onToggleTree,
  onOpenMemory = null,
  onOpenMemo = null,
  onPanelClose,
  backArrow = false,
}: TabStripProps): React.ReactElement {
  const { t } = useTranslation();
  const listRef = useRef<HTMLDivElement>(null);
  const sources = useTranscriptReads(transcript, tabs, readTabSource);

  /**
   * A tab closed from the keyboard hands the focus to the tab that takes its
   * place; a tab closed with the mouse does not, because the pointer is where
   * the reader already is. Same rule the card deck follows. The last tab
   * takes the panel with it, which leaves nothing in the strip to hand to.
   */
  const closeFrom = useCallback((id: string, keyboard: boolean) => {
    const index = tabs.findIndex((tab) => tab.id === id);
    onClose(id);
    if (!keyboard || tabs.length <= 1) return;
    requestAnimationFrame(() => {
      const remaining = listRef.current?.querySelectorAll<HTMLElement>('[role="tab"]');
      if (!remaining?.length) return;
      remaining[Math.min(index, remaining.length - 1)].focus();
    });
  }, [tabs, onClose]);

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
    <div className="file-panel-header file-panel-tabstrip">
      <TooltipProvider delayDuration={350} skipDelayDuration={600}>
      <div className="file-panel-tabs clips-focus-ring" role="tablist" aria-label={t('filePanel.openFiles')} ref={listRef}>
        {tabs.map((tab, i) => {
          const { name, Glyph, detail, failed } = describe(tab, t, sources[i]);
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
              onClick={() => onActivate(tab.id)}
              onDoubleClick={() => onPin(tab.id)}
              onAuxClick={(e) => { if (e.button === 1) { e.preventDefault(); closeFrom(tab.id, false); } }}
              onKeyDown={(e) => onKeyDown(e, tab)}
            >
              <Glyph className="h-3.5 w-3.5 shrink-0" />
              <span className="file-panel-tab-name">{name}</span>
              {failed && (
                <XCircle className="h-3.5 w-3.5 shrink-0 file-panel-tab-failed" role="img" aria-label={t('toolArtifact.a11y.toolCallFailed')} />
              )}
              {tab.kind === 'file' && hasChanged(tab.path) && (
                <span className="file-panel-tab-dot" title={t('filePanel.changedSinceRead')} aria-hidden="true" />
              )}
              <button
                type="button"
                className="file-panel-tab-close"
                aria-label={t('filePanel.closeTab', { name })}
                onClick={(e) => { e.stopPropagation(); closeFrom(tab.id, e.detail === 0); }}
                onMouseDown={(e) => e.stopPropagation()}
              >
                <X className="h-3 w-3" />
              </button>
            </div>
          );
          if (!hasHint) return pill;
          return (
            <Tooltip key={tab.id}>
              <TooltipTrigger asChild>{pill}</TooltipTrigger>
              <TooltipContent side="bottom" align="start" className="file-panel-tab-hint">
                <div className="file-panel-tab-hint-name">{name}</div>
                {detail && <div className="file-panel-tab-hint-detail">{detail}</div>}
              </TooltipContent>
            </Tooltip>
          );
        })}
      </div>
      </TooltipProvider>

      {onNewTab && (
        <button type="button" onClick={onNewTab} className="file-panel-icon-btn" title={t('filePanel.newTab')} aria-label={t('filePanel.newTab')}>
          <Plus className="h-4 w-4" />
        </button>
      )}

      <div className="file-panel-strip-right">
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
        {onToggleTree && (
          <button
            type="button"
            onClick={onToggleTree}
            aria-pressed={treeOpen}
            className="file-panel-icon-btn"
            title={t('filePanel.toggleTree')}
            aria-label={t('filePanel.toggleTree')}
          >
            <PanelRight className="h-4 w-4" />
          </button>
        )}
        {onPanelClose && (
          <button type="button" onClick={onPanelClose} className="file-panel-icon-btn" title={t('filePanel.close')} aria-label={t('filePanel.close')}>
            {backArrow ? <ArrowLeft className="h-4 w-4" /> : <X className="h-4 w-4" />}
          </button>
        )}
      </div>
    </div>
  );
}
