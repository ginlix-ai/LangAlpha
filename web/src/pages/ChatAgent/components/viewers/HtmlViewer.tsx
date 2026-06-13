import { useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from '@/contexts/ThemeContext';
import SyntaxHighlighter, { oneDark, oneLight } from '../SyntaxHighlighter';
import { useHtmlSandbox } from './html/useHtmlSandbox';
import { useHtmlActions } from './html/useHtmlActions';
import { useDirectLinkGuard } from './html/useDirectLinkGuard';
import HtmlActionBar from './html/HtmlActionBar';
import HtmlFullscreenModal from './html/HtmlFullscreenModal';
import { buildWsfilesUrl } from './html/wsfilesUrl';
import './HtmlViewer.css';

interface HtmlViewerProps {
  /** Full source (read unlimited so Source isn't truncated). */
  content: string;
  fileName: string;
  workspaceId: string;
  /** Path within the workspace, e.g. "results/report.html". */
  filePath: string;
  /** Download the server's original bytes for this file. */
  onTriggerDownload: () => void;
  /** Override the served URL (e.g. the public share serve URL). When set, the
   *  preview iframe and HTML actions point here instead of the wsfiles route. */
  servedUrlOverride?: string;
  /** Copy a shareable link to this report (authenticated app only). When set,
   *  a link button appears in the toolbar. */
  onCopyShareLink?: (filePath: string) => void;
}

export default function HtmlViewer({
  content,
  fileName,
  workspaceId,
  filePath,
  onTriggerDownload,
  servedUrlOverride,
  onCopyShareLink,
}: HtmlViewerProps) {
  const { t } = useTranslation();
  const { theme } = useTheme();
  const [mode, setMode] = useState<'preview' | 'source'>('preview');
  const [fullscreen, setFullscreen] = useState(false);
  const iframeRef = useRef<HTMLIFrameElement>(null);

  const { pushTheme } = useHtmlSandbox({ iframeRef, autoHeight: false });

  const servedUrl = useMemo(
    () => servedUrlOverride ?? buildWsfilesUrl(workspaceId, filePath, { injectTheme: true }),
    [servedUrlOverride, workspaceId, filePath],
  );

  // Byte-faithful served URL (no ?inject=theme) for open-in-new-tab / PDF.
  const servedUrlPlain = useMemo(
    () => (servedUrlOverride ? servedUrlOverride.split('?')[0] : undefined),
    [servedUrlOverride],
  );

  const actions = useHtmlActions({
    mode: 'file',
    workspaceId,
    filePath,
    triggerDownload: () => Promise.resolve(onTriggerDownload()),
    servedUrl: servedUrlPlain,
  });

  // Owner view (no servedUrl override) opens the raw, non-revocable wsfiles URL
  // — confirm before exposing it. The public share serve URL is revocable.
  const { request: openInNewTab, dialog: directLinkDialog } = useDirectLinkGuard(
    actions.openInNewTab,
    !servedUrlOverride,
  );

  const isLight = theme === 'light';

  return (
    <div className="html-viewer">
      <div className="html-viewer-toolbar">
        <div className="html-viewer-tabs">
          <button
            className={`html-viewer-tab ${mode === 'preview' ? 'active' : ''}`}
            onClick={() => setMode('preview')}
          >
            {t('filePanel.htmlPreview')}
          </button>
          <button
            className={`html-viewer-tab ${mode === 'source' ? 'active' : ''}`}
            onClick={() => setMode('source')}
          >
            {t('filePanel.htmlSource')}
          </button>
        </div>
        {/* Download/Save-as-PDF live in the file panel header's download menu. */}
        <HtmlActionBar
          onFullscreen={() => setFullscreen(true)}
          onOpenInNewTab={openInNewTab}
          onCopyLink={onCopyShareLink ? () => onCopyShareLink(filePath) : undefined}
        />
      </div>
      {mode === 'preview' ? (
        <iframe
          ref={iframeRef}
          src={servedUrl}
          sandbox="allow-scripts"
          className="html-viewer-frame"
          title={fileName || 'HTML Preview'}
          onLoad={pushTheme}
        />
      ) : (
        <div className="html-viewer-source">
          <SyntaxHighlighter
            language="markup"
            style={isLight ? oneLight : oneDark}
            customStyle={{ margin: 0, padding: 0, backgroundColor: 'transparent', fontSize: '12px', lineHeight: '1.6' }}
            codeTagProps={{ style: { backgroundColor: 'transparent' } }}
            wrapLongLines
          >
            {content}
          </SyntaxHighlighter>
        </div>
      )}
      {fullscreen && (
        <HtmlFullscreenModal
          variant="file"
          open={fullscreen}
          onOpenChange={setFullscreen}
          title={fileName}
          workspaceId={workspaceId}
          filePath={filePath}
          servedUrl={servedUrlOverride}
          actions={actions}
        />
      )}
      {directLinkDialog}
    </div>
  );
}
