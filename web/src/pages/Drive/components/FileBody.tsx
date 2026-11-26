import React, { Suspense } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import type { editor } from 'monaco-editor';
import { queryKeys } from '@/lib/queryKeys';
import { FileViewer } from '@/pages/ChatAgent/components/filePanel/FileViewer';
import { DocumentLoadingFallback } from '@/pages/ChatAgent/components/filePanel/fallbacks';
import type { FileBody as Body } from '@/pages/ChatAgent/components/filePanel/fileBody';
import type { FileError } from '@/pages/ChatAgent/components/filePanel/fileErrors';
import { WorkspaceProvider } from '@/pages/ChatAgent/contexts/WorkspaceContext';
import type { OpenFileHandler } from '@/pages/ChatAgent/utils/fileLocation';
import type { DriveEntry } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { DriveHtml } from './DriveHtml';
import { PaneProblem } from './PaneProblem';

const CodeEditor = React.lazy(() => import('@/pages/ChatAgent/components/viewers/CodeEditor'));

/**
 * What the pane shows of the file, decided by the pane: the editor (beside
 * theirs in a conflict), a served page, the viewer its bytes belong to, or
 * why none of them can show.
 */
export type BodyView =
  | {
    kind: 'editor';
    name: string;
    draft: string;
    /** What the draft is compared with: theirs in a conflict, else the content it was written over. */
    original: string;
    compare: boolean;
    modelPath: string;
    editorRef: React.RefObject<editor.IStandaloneCodeEditor | null>;
    onChange: (draft: string) => void;
  }
  | { kind: 'html'; name: string; content: string; source: boolean }
  | {
    kind: 'document';
    entry: DriveEntry;
    body: Body | null;
    loading: boolean;
    error: FileError | null;
    onRetry: () => void;
    /** The version shown in place of the head, which a download takes. */
    seq: number | null;
  }
  | { kind: 'loading' }
  | { kind: 'failure'; name: string; gone: boolean; onRetry: () => void }
  /** The version the URL names is not kept (expired or cleared). */
  | { kind: 'versionGone'; onBack: () => void };

function Editor({ name, value, original, compare, modelPath, editorRef, onChange }: {
  name: string;
  value: string;
  original: string;
  compare: boolean;
  modelPath: string;
  editorRef: React.RefObject<editor.IStandaloneCodeEditor | null>;
  onChange: (value: string) => void;
}) {
  return (
    <div className="drive-editor">
      <Suspense fallback={<DocumentLoadingFallback />}>
        <CodeEditor
          value={value}
          onChange={onChange}
          fileName={name}
          diffMode={compare}
          originalValue={original}
          editorRef={editorRef}
          modelPath={modelPath}
        />
      </Suspense>
    </div>
  );
}

/** The file as `view` says to show it. */
export function FileBody({ view, imageLoader, onOpenLink }: {
  view: BodyView;
  imageLoader: (path: string) => Promise<string>;
  onOpenLink: OpenFileHandler;
}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { space, actions } = useDriveContext();

  switch (view.kind) {
    case 'editor':
      return (
        <Editor
          name={view.name}
          value={view.draft}
          original={view.original}
          compare={view.compare}
          modelPath={view.modelPath}
          editorRef={view.editorRef}
          onChange={view.onChange}
        />
      );
    case 'html':
      return <DriveHtml content={view.content} name={view.name} source={view.source} />;
    case 'loading':
      return <DocumentLoadingFallback />;
    case 'failure':
      return <PaneProblem line={view.gone ? t('drive:errors.missing', { name: view.name }) : t('drive:errors.loadFailed')} onRetry={view.gone ? undefined : view.onRetry} />;
    case 'versionGone':
      return <PaneProblem line={t('drive:errors.gone')} onBack={view.onBack} />;
    case 'document': {
      const { entry } = view;
      const download = () => void actions.download(entry, view.seq);
      return (
        <div className="drive-viewer">
          <WorkspaceProvider workspaceId={null} downloadFile={imageLoader} imageScope={`drive:${space}:${entry.id}`}>
            <FileViewer
              path={entry.name}
              body={view.body}
              loading={view.loading}
              error={view.error}
              onRetry={view.onRetry}
              onDownload={download}
              onDownloadInFallback={download}
              onFileChanged={() => void queryClient.invalidateQueries({ queryKey: queryKeys.drive.entry(space ?? '', entry.path) })}
              onAnchorLink={(id) => document.getElementById(id)?.scrollIntoView({ block: 'start' })}
              onViewerLink={onOpenLink}
              proseClassName="drive-prose"
            />
          </WorkspaceProvider>
        </div>
      );
    }
  }
}
