import React, { Suspense, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import type { OpenFileHandler } from '../../utils/fileLocation';
import { SandboxSettingsContent } from '../SandboxSettingsPanel';
import type { SubagentInfo } from '../ToolCallDetailView';
import type { ContextPayload } from '@/components/ui/chat-input.types';
import type { ApiAdapter, ChartTabSpec, PanelTarget } from './types';
import type { BrowserViewSession } from './browserStream';
import type { FileTab, FileTabsApi } from './useFileTabs';
import type { CommentUi } from '../viewers/html/comments/useCommentUi';
import { BrowserViewer } from '../viewers/BrowserViewer';
import { ChartTab } from './ChartTab';
import { StoreTab } from './StoreTab';
import { FileViewer, type FileViewerProps } from './FileViewer';
import { EmptyTab } from './EmptyTab';
import { useTranscriptRead, type TranscriptReader } from './useTranscript';
import { withCopy } from '@/i18n';
import { isFeatureEnabled, useFeatures } from '@/hooks/useFeatures';

// Tool results draw market-data cards, and those carry the chart stack, so
// like the chart tab they load when looked at rather than with the panel.
const DetailPanel = React.lazy(withCopy(() => import('../DetailPanel'), ['automation']));
const SourcesPanel = React.lazy(() => import('../SourcesPanel'));
// The Drive tab is the Drive page's own code, its viewers and editor included.
const DriveTab = React.lazy(withCopy(() => import('@/pages/Drive/DriveTab'), ['drive', 'mcp', 'pageViewer']));

interface ActiveTabBodyProps {
  activeTab: FileTab;
  tabs: FileTabsApi;
  workspaceId: string;
  apiAdapter: ApiAdapter | null;
  target: PanelTarget | null;
  onTargetMemoryHandled?: (seq?: number) => void;
  onTargetMemoHandled?: (seq?: number) => void;
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
  /** Null where the panel has no browser to show, as a share's does not. */
  browser: BrowserViewSession | null;
  /** The computer that browser runs on, named on its start page; null where
   *  the reader is not shown it. */
  browserComputer: string | null;
  /** The owner's comment controls; null where the reader keeps no comments. */
  commentUi: CommentUi | null;
  /** The panel is the one in front. */
  active: boolean;
}

/** What fills the viewer slot for the active tab. Preview panes are rendered beside this, kept mounted. */
export function ActiveTabBody({
  activeTab, tabs, workspaceId, apiAdapter, target, onTargetMemoryHandled, onTargetMemoHandled,
  onOpenFile, onOpenSubagentTask, transcript, onAddContext,
  onOpenInMarketView, file, onPageCount, canUpload, browser, browserComputer, commentUi, active,
}: ActiveTabBodyProps): React.ReactNode {
  switch (activeTab.kind) {
    case 'preview':
      return null;
    case 'browser':
      return browser && <BrowserViewer view={browser} computerName={browserComputer} />;
    case 'chart':
      return (
        <ChartTab
          tab={activeTab}
          tabs={tabs}
          workspaceId={workspaceId}
          onAddContext={onAddContext}
          onOpenInMarketView={onOpenInMarketView}
          commentUi={commentUi}
          active={active}
        />
      );
    case 'drive':
      return <DriveTabBody tab={activeTab} tabs={tabs} active={active} />;
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

/**
 * A place in the person's Drive, its moves kept as the tab's steps. Shown
 * only once a read of the flags says the Drive is on: a read that failed
 * keeps the tab, which a later read may show, without asking the Drive for
 * anything meanwhile.
 */
/**
 * Mounted only while its panel is the one in front: a chat put behind
 * another stays mounted, hidden, and the Drive would keep polling its change
 * feed there, while two hidden editors on one file would share its draft. The
 * tab keeps its place and the draft store its text, so it comes back as left.
 */
function DriveTabBody({ tab, tabs, active }: { tab: Extract<FileTab, { kind: 'drive' }>; tabs: FileTabsApi; active: boolean }): React.ReactNode {
  const { t } = useTranslation();
  const { data: features, isPending } = useFeatures();
  const { id } = tab;
  const { goDrive, trackDrive, pinTab } = tabs;
  const onPin = useCallback(() => pinTab(id), [pinTab, id]);
  const onTrack = useCallback((entryId: string) => trackDrive(id, entryId), [trackDrive, id]);
  if (!active) return null;
  if (!isFeatureEnabled(features, 'drive')) {
    return isPending ? null : (
      <p className="px-6 py-10 text-center text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('filePanel:driveUnavailable')}
      </p>
    );
  }
  return (
    <Suspense fallback={null}>
      <DriveTab tab={tab} onGo={(to, opts) => goDrive(id, to, opts)} onTrack={onTrack} onPin={onPin} />
    </Suspense>
  );
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
        {t('toolArtifact:toolCallGone')}
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
