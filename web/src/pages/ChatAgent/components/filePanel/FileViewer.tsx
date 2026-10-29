import React, { Suspense, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from '@/contexts/ThemeContext';
import { Loader } from '@/components/ui/loader';
import SyntaxHighlighter, { oneDark, oneLight } from '../SyntaxHighlighter';
import Markdown from '../Markdown';
import ImageLightbox from '../ImageLightbox';
import DocumentErrorBoundary from '../viewers/DocumentErrorBoundary';
import { stripLineNumbers } from '../toolDisplayConfig';
import type { OpenFileHandler } from '../../utils/fileLocation';
import type { ContextPayload, EditorTextSelectData } from './types';
import { EXT_TO_LANG, LARGE_TOOL_RESULTS_PREFIX, getFileExtension } from './fileMeta';
import { imageMime, type FileBody } from './fileBody';
import { FileErrorDisplay, type FileError } from './fileErrors';
import { DocumentErrorFallback, DocumentLoadingFallback } from './fallbacks';
import type { useFileFocus } from './useFileFocus';
import type { DownloadState } from '../../utils/downloadNotice';

const PdfViewer = React.lazy(() => import('../viewers/PdfViewer'));
const CsvViewer = React.lazy(() => import('../viewers/CsvViewer'));
const HtmlViewer = React.lazy(() => import('../viewers/HtmlViewer'));
const CodeEditor = React.lazy(() => import('../viewers/CodeEditor'));

const ExcelViewer = React.lazy(() => import('../viewers/ExcelViewer'));

/** Where a reference into the file lands, per viewer. */
type FileViewerFocus = Pick<ReturnType<typeof useFileFocus>, 'lineRange' | 'focusPage' | 'htmlAnchor' | 'focusCell' | 'seq'>;

const NO_FOCUS: FileViewerFocus = { lineRange: null, focusPage: null, htmlAnchor: null, focusCell: null, seq: null };

// The highlighter re-tokenizes the whole file on every render it gets, so
// its element is reused only while every prop, these included, is the same.
const CODE_STYLE: React.CSSProperties = { margin: 0, padding: 0, backgroundColor: 'transparent', fontSize: '0.75rem', lineHeight: '1.6' };
const CODE_TAG_PROPS = { style: { backgroundColor: 'transparent' } };
const LINE_NUMBER_STYLE: React.CSSProperties = {
  minWidth: '2.5em', paddingRight: '1em', color: 'var(--color-text-tertiary)', userSelect: 'none', fontSize: '0.6875rem', opacity: 0.5,
};

export interface FileViewerProps {
  path: string;
  body: FileBody | null;
  loading: boolean;
  error: FileError | null;
  onRetry: () => void;
  /** A large PDF lost a slice, to a change in the file or a failed read, named by the path it was drawn for. Left out, the viewer fails instead. */
  onFileChanged?: (path: string) => void;
  /** Offered where the viewer failed; a toast reports its own failure. */
  onDownloadInFallback?: () => void;
  onDownload?: () => void;
  /** A save of this file is being prepared. */
  downloadState?: DownloadState;

  workspaceId: string;
  onAnchorLink: (fragment: string) => void;

  // The panel's own surface. A viewer outside it (a shared file's page) has
  // no reference to land, no editor and no workspace to link into.
  focus?: FileViewerFocus;
  onPageCount?: (pages: number) => void;
  /** An HTML file's markup rather than its page, switched from the panel header. */
  htmlSource?: boolean;

  isEditing?: boolean;
  editContent?: string | null;
  originalContent?: string | null;
  showDiff?: boolean;
  editorRef?: React.RefObject<unknown>;
  /** The edit session's Monaco model, kept per file across tab switches. */
  editorModelPath?: string | null;
  onEditorChange?: (value: string) => void;
  onUndoRedoChange?: (state: { canUndo: boolean; canRedo: boolean }) => void;
  onEditorTextSelect?: (data: EditorTextSelectData | null) => void;

  onAddContext?: ((ctx: ContextPayload) => void) | null;
  onContentMouseUp?: () => void;
  /** Left out, a link to another workspace file renders as plain text. */
  onViewerLink?: OpenFileHandler;
  /** The serve prefix a share was handed; omitted, the owner's grant serves HTML. */
  servePrefix?: string;
}

/** The open file, rendered by whichever viewer its bytes belong to. */
export function FileViewer(props: FileViewerProps): React.ReactElement {
  const { path, body, loading, error, isEditing = false, focus = NO_FOCUS } = props;
  // Out of `focus` so the highlighter's line props follow the range alone, not every focus change.
  const { lineRange } = focus;
  const { t } = useTranslation();
  const { theme } = useTheme();
  const [lightboxOpen, setLightboxOpen] = useState(false);
  // Bumped by Retry on a large PDF that failed, so its viewer starts over.
  const [pdfAttempt, setPdfAttempt] = useState(0);
  const ext = getFileExtension(path);
  const fileName = path.split('/').pop() || path;

  // An image is cached as bytes, not as a blob URL: a URL minted into the
  // query cache has no owner left to revoke it, and the leak is the whole
  // image. Minted here instead, it dies with the view that showed it.
  const imageBuffer = body?.mime === 'image' ? body.buffer : undefined;
  const imageUrl = useMemo(
    () => (imageBuffer ? URL.createObjectURL(new Blob([imageBuffer], { type: imageMime(path) })) : null),
    [imageBuffer, path],
  );
  useEffect(() => () => { if (imageUrl) URL.revokeObjectURL(imageUrl); }, [imageUrl]);

  const editor = (
    <div className="file-panel-editor-container">
      <Suspense fallback={<DocumentLoadingFallback />}>
        <CodeEditor
          value={props.editContent ?? undefined}
          onChange={props.onEditorChange}
          fileName={path}
          diffMode={props.showDiff}
          originalValue={props.originalContent ?? undefined}
          editorRef={props.editorRef as React.RefObject<never>}
          modelPath={props.editorModelPath ?? undefined}
          onUndoRedoChange={props.onUndoRedoChange}
          onTextSelect={props.onAddContext ? props.onEditorTextSelect : undefined}
        />
      </Suspense>
    </div>
  );

  if (loading) {
    return (
      <div className="p-4">
        <div className="flex items-center justify-center py-12">
          <Loader size={20} className="text-(--color-text-tertiary)" />
        </div>
      </div>
    );
  }

  if (error) {
    return <FileErrorDisplay error={error} onRetry={props.onRetry} onDownload={props.onDownload} downloadState={props.downloadState} />;
  }

  if (body?.mime === 'pdf') {
    return (
      <Suspense fallback={<DocumentLoadingFallback />}>
        <DocumentErrorBoundary
          // A newer copy of the same file keeps the reader's place; another
          // file starts over, and a failed one does not fail the next.
          key={`${path}#${pdfAttempt}`}
          fallback={
            <DocumentErrorFallback
              // A large PDF fails on a slice it could not read, which a fresh
              // viewer reads again; whole bytes that failed would fail again.
              onRetry={body.ranged ? () => setPdfAttempt((n) => n + 1) : undefined}
              onDownload={props.onDownloadInFallback}
              downloadState={props.downloadState}
            />
          }>
          <PdfViewer
            data={body.ranged ?? body.buffer!}
            onFileChanged={props.onFileChanged && (() => props.onFileChanged!(path))}
            focusPage={focus.focusPage}
            focusSeq={focus.seq}
            onPageCount={props.onPageCount}
          />
        </DocumentErrorBoundary>
      </Suspense>
    );
  }

  if (body?.mime === 'excel') {
    return (
      <Suspense fallback={<DocumentLoadingFallback />}>
        <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={props.onDownloadInFallback} downloadState={props.downloadState} />}>
          <ExcelViewer
            // The parse is async and keeps the last workbook's cells until it
            // lands, so a second workbook gets its own instance rather than
            // the first one's grid under its own path.
            key={path}
            data={body.buffer!}
            filePath={path}
            onAddContext={props.onAddContext ?? undefined}
            focusCell={focus.focusCell}
            focusSeq={focus.seq ?? undefined}
            canDownload={!!props.onDownload}
          />
        </DocumentErrorBoundary>
      </Suspense>
    );
  }

  if (ext === 'csv') {
    return isEditing ? editor : (
      <Suspense fallback={<DocumentLoadingFallback />}>
        <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={props.onDownloadInFallback} downloadState={props.downloadState} />}>
          <CsvViewer content={body?.content ?? ''} />
        </DocumentErrorBoundary>
      </Suspense>
    );
  }

  if (ext === 'html' || ext === 'htm') {
    return (
      <Suspense fallback={<DocumentLoadingFallback />}>
        <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={props.onDownloadInFallback} downloadState={props.downloadState} />}>
          <HtmlViewer
            content={body?.content ?? ''}
            fileName={fileName}
            workspaceId={props.workspaceId}
            filePath={path}
            servePrefix={props.servePrefix}
            anchor={focus.htmlAnchor}
            anchorSeq={focus.seq}
            source={props.htmlSource}
          />
        </DocumentErrorBoundary>
      </Suspense>
    );
  }

  if (isEditing) return editor;

  return (
    <div className="p-4" onMouseUp={props.onContentMouseUp}>
      {body?.mime === 'image' && imageUrl ? (
        <>
          <img src={imageUrl} alt={fileName} className="max-w-full rounded cursor-pointer" onClick={() => setLightboxOpen(true)} />
          <ImageLightbox src={imageUrl} alt={fileName} open={lightboxOpen} onClose={() => setLightboxOpen(false)} />
        </>
      ) : path.startsWith(LARGE_TOOL_RESULTS_PREFIX) ? (
        <div className="markdown-print-content">
          <Markdown variant="panel" content={stripLineNumbers(body?.content ?? null) ?? ''} className="text-sm" />
        </div>
      ) : body?.mime?.includes('markdown') || ext === 'md' ? (
        <div className="markdown-print-content">
          <Markdown
            variant="panel"
            content={body?.content ?? ''}
            className="text-sm"
            onOpenFile={props.onViewerLink}
            onAnchorLink={props.onAnchorLink}
          />
        </div>
      ) : body?.content != null ? (
        <SyntaxHighlighter
          language={EXT_TO_LANG[ext] || 'text'}
          style={theme === 'light' ? oneLight : oneDark}
          customStyle={CODE_STYLE}
          codeTagProps={CODE_TAG_PROPS}
          showLineNumbers
          lineNumberStyle={LINE_NUMBER_STYLE}
          wrapLines
          lineProps={(lineNumber: number) => {
            const focused = !!lineRange && lineNumber >= lineRange[0] && lineNumber <= lineRange[1];
            return { 'data-line': lineNumber, ...(focused ? { className: 'file-focus-line' } : {}) } as React.HTMLProps<HTMLElement>;
          }}
          wrapLongLines
        >
          {body.content}
        </SyntaxHighlighter>
      ) : (
        <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>{t('filePanel.emptyFile')}</p>
      )}
    </div>
  );
}
