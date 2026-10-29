import React, { useCallback, useMemo, useRef, useState } from 'react';
import { Download, FileDown } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { cn } from '@/lib/utils';
import { bodyMode } from '../ChatAgent/components/filePanel/fileBody';
import { getFileExtension } from '../ChatAgent/components/filePanel/fileMeta';
import { FileViewer } from '../ChatAgent/components/filePanel/FileViewer';
import { useFileBody, useFileBodyCache } from '../ChatAgent/components/filePanel/useFileBody';
import { useHtmlSandbox } from '../ChatAgent/components/viewers/html/useHtmlSandbox';
import { exportServedPdf } from '../ChatAgent/components/viewers/html/useHtmlActions';
import { SERVED_HTML_SANDBOX } from '../ChatAgent/components/viewers/html/sandbox';
import { buildServeUrl } from '../ChatAgent/components/viewers/html/wsfilesUrl';
import { WorkspaceProvider } from '../ChatAgent/contexts/WorkspaceContext';
import { shareLinkHref } from '../ChatAgent/utils/api/shareLinks';
import { ShareTopBar, ShareTopBarAction } from './ShareTopBar';
import { downloadServedFile, servedObjectUrl, servedReaders, type SharedFileMetadata } from './api';
import './SharePage.css';

interface SharedFileViewProps {
  code: string;
  metadata: SharedFileMetadata;
}

/**
 * A served HTML document filling the page, themed like the app around it.
 * The in-panel viewer's toolbar has no place here: the top bar already
 * carries the page's one action.
 */
function HtmlFrame({ src, title }: { src: string; title: string }) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const { pushTheme } = useHtmlSandbox({ iframeRef, autoHeight: false });
  return (
    <iframe
      ref={iframeRef}
      src={src}
      sandbox={SERVED_HTML_SANDBOX}
      className="share-frame"
      title={title}
      onLoad={pushTheme}
    />
  );
}

/** Every non-HTML file, read and rendered the way the owner's panel does. */
function FileBody({ code, path, frameBase, onDownload, downloading }: {
  code: string;
  path: string;
  frameBase: string;
  onDownload: () => void;
  downloading: boolean;
}) {
  const readers = useMemo(() => servedReaders(frameBase), [frameBase]);
  // Scoped by the code, not the prefix: an owner's grant renews under a new
  // prefix, and the bytes on screen are still the same file's.
  const cache = useFileBodyCache({ scope: `share:${code}`, workspaceId: '', readers });
  const { body, loading, error, refetch } = useFileBody({ cache, path });
  // Markdown images are workspace paths, fetched under the same prefix as the file.
  const imageDownloader = useCallback((imagePath: string) => servedObjectUrl(frameBase, imagePath), [frameBase]);
  // With an anchor handler and no file opener, a link to another workspace
  // file is plain text: a visitor can open only the files listed.
  const scrollToAnchor = useCallback((id: string) => {
    document.getElementById(id)?.scrollIntoView({ block: 'start' });
  }, []);
  // A grid or a paged document uses the width; prose and code read in a column.
  const wide = bodyMode(path) === 'buffer' || getFileExtension(path) === 'csv';
  // A PDF scrolls inside its own viewer, which needs the page's height for it:
  // left to grow with its pages, it would draw every one of them at once.
  const fill = getFileExtension(path) === 'pdf';

  return (
    <div className="share-scroll">
      <div className={cn(!wide && 'share-document', fill && 'share-fill')}>
        <WorkspaceProvider workspaceId={null} downloadFile={imageDownloader}>
          <FileViewer
            path={path}
            body={body}
            loading={loading}
            error={error}
            onRetry={() => void refetch()}
            onDownload={onDownload}
            onDownloadInFallback={onDownload}
            downloadState={downloading ? 'preparing' : 'idle'}
            workspaceId=""
            onAnchorLink={scrollToAnchor}
          />
        </WorkspaceProvider>
      </div>
    </div>
  );
}

/**
 * A file behind its `/a/` link: one line of chrome, then the file. It renders
 * under the serve prefix the metadata named, so an HTML document's relative
 * references resolve exactly as they do in the owner's panel.
 */
export default function SharedFileView({ code, metadata }: SharedFileViewProps): React.ReactElement {
  const { t } = useTranslation();
  const { path, name, frame_base: frameBase } = metadata;
  const isHtml = bodyMode(path) === 'html';
  const servedUrl = buildServeUrl(frameBase, path);
  const [busy, setBusy] = useState(false);

  const download = async () => {
    setBusy(true);
    try {
      await downloadServedFile(servedUrl, name);
    } catch (err) {
      console.error('[SharedFileView] Download failed:', err);
      toast({ description: t('filePanel.downloadFailed'), variant: 'destructive' });
    } finally {
      setBusy(false);
    }
  };

  const savePdf = async () => {
    if (busy) return;
    setBusy(true);
    try {
      await exportServedPdf({
        filePath: path,
        servedUrl,
        // The print fallback opens this page again, never the served URL: for
        // the owner that URL carries the grant.
        openUrl: shareLinkHref(code),
        printHint: t('filePanel.pdfPrintHint'),
        generatingHint: t('filePanel.pdfGenerating'),
      });
    } finally {
      setBusy(false);
    }
  };

  const action = isHtml
    ? <ShareTopBarAction icon={FileDown} label={t('filePanel.saveAsPdf')} onClick={() => void savePdf()} disabled={busy} />
    : <ShareTopBarAction icon={Download} label={t('filePanel.download')} onClick={() => void download()} disabled={busy} />;

  return (
    <div className="share-page">
      <ShareTopBar name={name} actions={action} />
      <div className="share-content">
        {isHtml
          ? <HtmlFrame src={buildServeUrl(frameBase, path, { injectTheme: true })} title={name} />
          : <FileBody code={code} path={path} frameBase={frameBase} onDownload={() => void download()} downloading={busy} />}
      </div>
    </div>
  );
}
