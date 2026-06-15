import React, { Suspense, useMemo, useState } from 'react';
import { X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { AnimatedTabs } from '@/components/ui/animated-tabs';
import type { ContextPayload } from './FilePanel';
import type { MemoryTier } from '../utils/agentPaths';
import type { ProvenanceRecord } from '@/types/chat';

const FilePanel = React.lazy(() => import('./FilePanel'));
const MemoryPanel = React.lazy(() => import('./MemoryPanel'));
const MemoPanel = React.lazy(() => import('./MemoPanel'));
const SourcesPanel = React.lazy(() => import('./SourcesPanel'));

export type RightPanelTab = 'files' | 'memory' | 'memo' | 'sources';

interface RightPanelProps {
  workspaceId: string;
  onClose: () => void;
  targetFile?: string | null;
  onTargetFileHandled?: () => void;
  targetDirectory?: string | null;
  onTargetDirHandled?: () => void;
  /** Memory entry to pre-select when the Memory tab opens. */
  targetMemoryKey?: string | null;
  targetMemoryTier?: MemoryTier | null;
  onTargetMemoryHandled?: () => void;
  /** Memo entry to pre-select when the Memo tab opens. */
  targetMemoKey?: string | null;
  onTargetMemoHandled?: () => void;
  /** Message id whose provenance to show; when set, snaps to the Sources tab. */
  targetSources?: string | null;
  /** Live provenance records for the targeted message (keyed by record id). */
  sourcesRecords?: Record<string, ProvenanceRecord>;
  /** Provenance records merged across every turn in the thread (keyed by record
   * id). Powers the Sources panel's "All sources" scope. */
  allSourcesRecords?: Record<string, ProvenanceRecord>;
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
  targetFile,
  onTargetFileHandled,
  targetDirectory,
  onTargetDirHandled,
  targetMemoryKey,
  targetMemoryTier,
  onTargetMemoryHandled,
  targetMemoKey,
  onTargetMemoHandled,
  targetSources,
  sourcesRecords,
  allSourcesRecords,
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

  const tabs = useMemo<{ id: RightPanelTab; label: string }[]>(
    () => {
      const base: { id: RightPanelTab; label: string }[] = [
        { id: 'files', label: t('rightPanel.tabs.files') },
        { id: 'memory', label: t('rightPanel.tabs.memory') },
        { id: 'memo', label: t('rightPanel.tabs.memo') },
      ];
      // The Sources tab is per-turn — only surface it when a turn's provenance
      // is being shown, so the chrome stays unchanged for file/memory/memo flows.
      if (targetSources != null) {
        base.push({ id: 'sources', label: t('rightPanel.tabs.sources') });
      }
      return base;
    },
    [t, targetSources],
  );

  // Snap-back precedence: sources > memory > memo > file. The parent (ChatView)
  // clears sibling targets before setting one, so in steady state only one
  // branch fires; this effect is the second line of defense if multiple are set
  // in the same render.
  React.useEffect(() => {
    if (targetSources != null) setTab('sources');
    else if (targetMemoryKey != null) setTab('memory');
    else if (targetMemoKey != null) setTab('memo');
    else if (targetFile || targetDirectory) setTab('files');
  }, [targetSources, targetMemoryKey, targetMemoKey, targetFile, targetDirectory]);

  // The 'sources' tab only exists in `tabs` while targetSources is set. If it
  // clears while Sources is open, `tab` would point at a tab no longer in the
  // array (no active highlight + empty body); fall back so it always resolves.
  React.useEffect(() => {
    if (tab === 'sources' && targetSources == null) setTab('files');
  }, [tab, targetSources]);

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
