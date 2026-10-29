import React, { Suspense } from 'react';
import { useTranslation } from 'react-i18next';
import type { OpenFileHandler } from '../../utils/fileLocation';
import type { MarketWatchState } from '../../session/marketWatchEvents';
import { SandboxSettingsContent } from '../SandboxSettingsPanel';
import type { SubagentInfo } from '../ToolCallDetailView';
import type { ApiAdapter, ChartTabSpec, ContextPayload, PanelTarget } from './types';
import type { FileTab, FileTabsApi } from './useFileTabs';
import { ChartTab } from './ChartTab';
import { StoreTab } from './StoreTab';
import { FileViewer, type FileViewerProps } from './FileViewer';
import { EmptyTab } from './EmptyTab';
import { useTranscriptRead, type TranscriptReader } from './useTranscript';

// Tool results draw market-data cards, and those carry the chart stack, so
// like the chart tab they load when looked at rather than with the panel.
const DetailPanel = React.lazy(() => import('../DetailPanel'));
const SourcesPanel = React.lazy(() => import('../SourcesPanel'));

interface ActiveTabBodyProps {
  activeTab: FileTab;
  tabs: FileTabsApi;
  workspaceId: string;
  apiAdapter: ApiAdapter | null;
  target: PanelTarget | null;
  onTargetMemoryHandled?: (seq?: number) => void;
  onTargetMemoHandled?: (seq?: number) => void;
  marketWatch: MarketWatchState | null;
  threadId: string | null;
  isActive: boolean;
  onOpenFile: OpenFileHandler | null;
  onOpenSubagentTask: ((info: SubagentInfo) => void) | null;
  transcript: TranscriptReader | null;
  onAddContext: ((ctx: ContextPayload) => void) | null;
  onOpenInMarketView: ((spec: ChartTabSpec) => void) | null;
  /** What the file viewer needs beyond the tab's own path: the bytes, the edit and focus state, and the handlers. */
  file: Omit<FileViewerProps, 'path' | 'workspaceId' | 'servePrefix' | 'onPageCount'>;
  onPageCount: (path: string, pages: number) => void;
  /** Whether the empty tab invites a drop. */
  canUpload: boolean;
}

/** What fills the viewer slot for the active tab. Preview panes are rendered beside this, always mounted. */
export function ActiveTabBody({
  activeTab, tabs, workspaceId, apiAdapter, target, onTargetMemoryHandled, onTargetMemoHandled, marketWatch,
  threadId, isActive, onOpenFile, onOpenSubagentTask, transcript, onAddContext,
  onOpenInMarketView, file, onPageCount, canUpload,
}: ActiveTabBodyProps): React.ReactNode {
  switch (activeTab.kind) {
    case 'preview':
      return null;
    case 'chart':
      return (
        <ChartTab
          tab={activeTab}
          tabs={tabs}
          workspaceId={workspaceId}
          onAddContext={onAddContext}
          onOpenInMarketView={onOpenInMarketView}
        />
      );
    case 'settings':
      return (
        <div className="file-panel-settings">
          <SandboxSettingsContent workspaceId={workspaceId} />
        </div>
      );
    case 'tool':
      return (
        <ToolTabBody
          transcript={transcript}
          toolCallId={activeTab.toolCallId}
          onOpenFile={onOpenFile}
          onOpenSubagentTask={onOpenSubagentTask}
        />
      );
    case 'memory':
    case 'memo':
    case 'status':
      return (
        <StoreTab
          kind={activeTab.kind}
          workspaceId={workspaceId}
          target={target}
          onTargetMemoryHandled={onTargetMemoryHandled}
          onTargetMemoHandled={onTargetMemoHandled}
          onOpenFile={onOpenFile}
          marketWatch={marketWatch}
          threadId={threadId}
          isActive={isActive}
        />
      );
    case 'sources':
      return <SourcesTabBody transcript={transcript} messageId={activeTab.messageId} onOpenFile={onOpenFile} />;
    case 'file': {
      const path = activeTab.path;
      return (
        <FileViewer
          {...file}
          path={path}
          workspaceId={workspaceId}
          onPageCount={(pages) => onPageCount(path, pages)}
          servePrefix={apiAdapter?.servePrefix}
        />
      );
    }
    case 'empty':
      return <EmptyTab canUpload={canUpload} />;
    default:
      return activeTab satisfies never;
  }
}

/** A call's result, following its live record: a running call's result shows as it lands. */
function ToolTabBody({ transcript, toolCallId, onOpenFile, onOpenSubagentTask }: {
  transcript: TranscriptReader | null;
  toolCallId: string;
  onOpenFile: OpenFileHandler | null;
  onOpenSubagentTask: ((info: SubagentInfo) => void) | null;
}): React.ReactNode {
  const { t } = useTranslation();
  const toolCallProcess = useTranscriptRead(transcript, (reader) => reader.toolCall(toolCallId));
  // The tab holds only the id, so a record cleared from the chat (a
  // subagent card dismissed, say) leaves it nothing to draw.
  if (!toolCallProcess) {
    return (
      <p className="px-6 py-10 text-center text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('toolArtifact.toolCallGone')}
      </p>
    );
  }
  return (
    <Suspense fallback={null}>
      <DetailPanel
        key={toolCallId}
        toolCallProcess={toolCallProcess}
        onOpenFile={onOpenFile ?? undefined}
        onOpenSubagentTask={onOpenSubagentTask ?? undefined}
      />
    </Suspense>
  );
}

const readAllSources = (reader: TranscriptReader) => reader.allSources();

/** A turn's sources as they stream in, beside every turn's for the "All sources" scope. */
function SourcesTabBody({ transcript, messageId, onOpenFile }: {
  transcript: TranscriptReader | null;
  messageId: string;
  onOpenFile: OpenFileHandler | null;
}): React.ReactNode {
  const records = useTranscriptRead(transcript, (reader) => reader.sources(messageId));
  // Read only while this tab is up, so a thread streaming with a file in
  // front never pays for a merge nothing shows.
  const allRecords = useTranscriptRead(transcript, readAllSources);
  return (
    <Suspense fallback={null}>
      <SourcesPanel provenanceRecords={records} allRecords={allRecords} onOpenFile={onOpenFile ?? undefined} />
    </Suspense>
  );
}
