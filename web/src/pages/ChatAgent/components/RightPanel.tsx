import React, { Suspense, useMemo, useState } from 'react';
import { X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { AnimatedTabs } from '@/components/ui/animated-tabs';
import type { ContextPayload } from './FilePanel';

const FilePanel = React.lazy(() => import('./FilePanel'));
const MemoryPanel = React.lazy(() => import('./MemoryPanel'));
const MemoPanel = React.lazy(() => import('./MemoPanel'));

export type RightPanelTab = 'files' | 'memory' | 'memo';

interface RightPanelProps {
  workspaceId: string;
  onClose: () => void;
  targetFile?: string | null;
  onTargetFileHandled?: () => void;
  targetDirectory?: string | null;
  onTargetDirHandled?: () => void;
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
}

export default function RightPanel({
  workspaceId,
  onClose,
  targetFile,
  onTargetFileHandled,
  targetDirectory,
  onTargetDirHandled,
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
}: RightPanelProps): React.ReactElement {
  const { t } = useTranslation();
  const [tab, setTab] = useState<RightPanelTab>(initialTab);

  const tabs = useMemo<{ id: RightPanelTab; label: string }[]>(
    () => [
      { id: 'files', label: t('rightPanel.tabs.files') },
      { id: 'memory', label: t('rightPanel.tabs.memory') },
      { id: 'memo', label: t('rightPanel.tabs.memo') },
    ],
    [t],
  );

  // If the caller navigates to a specific file while the Memory tab is active,
  // snap back to Files so the user sees what they asked for.
  React.useEffect(() => {
    if (targetFile || targetDirectory) setTab('files');
  }, [targetFile, targetDirectory]);

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
            />
          )}
          {tab === 'memory' && <MemoryPanel workspaceId={workspaceId} />}
          {tab === 'memo' && <MemoPanel />}
        </Suspense>
      </div>
    </div>
  );
}
