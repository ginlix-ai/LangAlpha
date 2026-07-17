import React, { Suspense, useMemo, useState } from 'react';
import { X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { AnimatedTabs } from '@/components/ui/animated-tabs';
import type { ContextPayload } from './FilePanel';
import type { MemoryTier } from '../utils/agentPaths';
import type { MarketWatchState } from '../hooks/utils/streamEventHandlers';
import type { ProvenanceRecord } from '@/types/chat';

const FilePanel = React.lazy(() => import('./FilePanel'));
const MemoryPanel = React.lazy(() => import('./MemoryPanel'));
const MemoPanel = React.lazy(() => import('./MemoPanel'));
const SourcesPanel = React.lazy(() => import('./SourcesPanel'));
const StatusPanel = React.lazy(() => import('./StatusPanel'));

export type RightPanelTab = 'files' | 'memory' | 'memo' | 'sources' | 'status';

/**
 * What the panel is currently pointed at — one discriminated value replacing the
 * former parallel `targetFile`/`…Dir`/`…MemoryKey`/`…MemoKey`/`…Sources`/`…Status`
 * props. The active tab, tab visibility, and snap-back all derive from `.kind`,
 * so exactly one target can be set at a time (no sibling-nulling dance).
 */
export type PanelTarget =
  | { kind: 'file'; path?: string | null; dir?: string | null }
  | { kind: 'memory'; key: string; tier: MemoryTier }
  | { kind: 'memo'; key: string }
  | { kind: 'sources'; messageId: string }
  | { kind: 'status' };

interface RightPanelProps {
  workspaceId: string;
  onClose: () => void;
  /** The panel's current target (file/memory/memo/sources/status), or null. */
  panelTarget?: PanelTarget | null;
  onTargetFileHandled?: () => void;
  onTargetDirHandled?: () => void;
  onTargetMemoryHandled?: () => void;
  onTargetMemoHandled?: () => void;
  /** Live provenance records for the targeted message (keyed by record id). */
  sourcesRecords?: Record<string, ProvenanceRecord>;
  /** Provenance records merged across every turn in the thread (keyed by record
   * id). Powers the Sources panel's "All sources" scope. */
  allSourcesRecords?: Record<string, ProvenanceRecord>;
  /** Live market-watch snapshot rendered by the Status tab. */
  marketWatch?: MarketWatchState | null;
  /** Routes a clicked file/memory/memo path through ChatView's path-aware
   * router. Lets in-panel markdown links (e.g., a sibling memory entry
   * referenced from memory.md) jump to the right tab + entry. */
  onOpenFile?: (path: string, workspaceId?: string) => void;
  files?: string[];
  filesLoading?: boolean;
  filesError?: string | null;
  onRefreshFiles?: () => void;
  onAddContext?: ((ctx: ContextPayload) => void) | null;
  showSystemFiles?: boolean;
  onToggleSystemFiles?: (() => void) | null;
  readOnly?: boolean;
  singleFileMode?: boolean;
  /** Initial tab — callers can deep-link into the Memory tab once it stabilizes. */
  initialTab?: RightPanelTab;
  /** Copy a shareable link to an HTML report (authenticated app only). */
  onCopyShareLink?: ((filePath: string) => void) | null;
}

export default function RightPanel({
  workspaceId,
  onClose,
  panelTarget = null,
  onTargetFileHandled,
  onTargetDirHandled,
  onTargetMemoryHandled,
  onTargetMemoHandled,
  sourcesRecords,
  allSourcesRecords,
  marketWatch,
  onOpenFile,
  files,
  filesLoading,
  filesError,
  onRefreshFiles,
  onAddContext,
  showSystemFiles,
  onToggleSystemFiles,
  readOnly,
  singleFileMode,
  initialTab = 'files',
  onCopyShareLink,
}: RightPanelProps): React.ReactElement {
  const { t } = useTranslation();
  const [tab, setTab] = useState<RightPanelTab>(initialTab);
  const watchSymbolCount = marketWatch?.symbols?.length ?? 0;

  // Fan the single target back out to the per-panel pre-select props. Exactly
  // one kind is ever set, so these are mutually exclusive by construction.
  const kind = panelTarget?.kind;
  const targetFile = panelTarget?.kind === 'file' ? panelTarget.path ?? null : null;
  const targetDirectory = panelTarget?.kind === 'file' ? panelTarget.dir ?? null : null;
  const targetMemoryKey = panelTarget?.kind === 'memory' ? panelTarget.key : null;
  const targetMemoryTier = panelTarget?.kind === 'memory' ? panelTarget.tier : null;
  const targetMemoKey = panelTarget?.kind === 'memo' ? panelTarget.key : null;

  const tabs = useMemo<{ id: RightPanelTab; label: string }[]>(
    () => {
      const base: { id: RightPanelTab; label: string }[] = [
        { id: 'files', label: t('rightPanel.tabs.files') },
        { id: 'memory', label: t('rightPanel.tabs.memory') },
        { id: 'memo', label: t('rightPanel.tabs.memo') },
      ];
      // The Status tab surfaces the live market watch. Unlike Sources it also
      // shows whenever a watch is active (symbols present) — so a user who
      // opened Files by hand can still reach it — as well as on an explicit
      // chip click (a 'status' target).
      if (kind === 'status' || watchSymbolCount > 0) {
        base.push({ id: 'status', label: t('rightPanel.tabs.status') });
      }
      // The Sources tab is per-turn — only surface it when a turn's provenance
      // is being shown, so the chrome stays unchanged for file/memory/memo flows.
      if (kind === 'sources') {
        base.push({ id: 'sources', label: t('rightPanel.tabs.sources') });
      }
      return base;
    },
    [t, kind, watchSymbolCount],
  );

  // Snap to the tab that owns the current target. Only one kind is ever set, so
  // a single switch replaces the former precedence ladder; a null target leaves
  // the tab where the user (or a prior snap) put it.
  React.useEffect(() => {
    switch (kind) {
      case 'status': setTab('status'); break;
      case 'sources': setTab('sources'); break;
      case 'memory': setTab('memory'); break;
      case 'memo': setTab('memo'); break;
      case 'file': setTab('files'); break;
    }
  }, [panelTarget, kind]);

  // The Status/Sources tabs are conditional (see `tabs`). If the current tab
  // disappears — Status when its target clears with no active watch, Sources
  // when its target clears — fall back to Files so `tab` always resolves.
  React.useEffect(() => {
    if (tab === 'status' && kind !== 'status' && watchSymbolCount === 0) setTab('files');
    else if (tab === 'sources' && kind !== 'sources') setTab('files');
  }, [tab, kind, watchSymbolCount]);

  return (
    <div
      className="flex flex-col h-full"
      style={{
        backgroundColor: 'var(--color-bg-page)',
        borderLeft: '1px solid var(--color-border-muted)',
      }}
    >
      {/* Tab chrome — shared across all three panels */}
      <div
        className="flex items-center justify-between px-3 py-2 border-b flex-shrink-0"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <AnimatedTabs
          tabs={tabs}
          value={tab}
          onChange={(id) => setTab(id as RightPanelTab)}
          layoutId="right-panel-tabs"
        />
        <button
          onClick={onClose}
          className="file-panel-icon-btn"
          title={t('rightPanel.close')}
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      {/* Tab body */}
      <div className="flex-1 min-h-0">
        <Suspense fallback={null}>
          {tab === 'files' && (
            <FilePanel
              workspaceId={workspaceId}
              onClose={onClose}
              targetFile={targetFile}
              onTargetFileHandled={onTargetFileHandled}
              targetDirectory={targetDirectory}
              onTargetDirHandled={onTargetDirHandled}
              files={files}
              filesLoading={filesLoading}
              filesError={filesError}
              onRefreshFiles={onRefreshFiles}
              onAddContext={onAddContext}
              showSystemFiles={showSystemFiles}
              onToggleSystemFiles={onToggleSystemFiles}
              readOnly={readOnly}
              singleFileMode={singleFileMode}
              hideClose
              onSwitchToMemoTab={() => setTab('memo')}
              onCopyShareLink={onCopyShareLink}
            />
          )}
          {tab === 'memory' && (
            <MemoryPanel
              workspaceId={workspaceId}
              targetKey={targetMemoryKey ?? null}
              targetTier={targetMemoryTier ?? null}
              onTargetHandled={onTargetMemoryHandled}
              onOpenFile={onOpenFile}
            />
          )}
          {tab === 'memo' && (
            <MemoPanel
              targetKey={targetMemoKey ?? null}
              onTargetHandled={onTargetMemoHandled}
              onOpenFile={onOpenFile}
            />
          )}
          {tab === 'status' && <StatusPanel marketWatch={marketWatch} />}
          {tab === 'sources' && (
            <SourcesPanel
              provenanceRecords={sourcesRecords}
              allRecords={allSourcesRecords}
              onOpenFile={onOpenFile}
            />
          )}
        </Suspense>
      </div>
    </div>
  );
}
