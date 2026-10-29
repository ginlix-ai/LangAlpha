import { useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle } from 'lucide-react';
import { useTheme } from '@/contexts/ThemeContext';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import SyntaxHighlighter, { oneDark, oneLight } from '../SyntaxHighlighter';
import { useHtmlSandbox } from './html/useHtmlSandbox';
import { useServedHtml } from './html/useServedHtml';
import { SERVED_HTML_SANDBOX } from './html/sandbox';
import './HtmlViewer.css';

interface HtmlViewerProps {
  /** Full source (read unlimited so Source isn't truncated). */
  content: string;
  fileName: string;
  workspaceId: string;
  /** Path within the workspace, e.g. "results/report.html". */
  filePath: string;
  /** The markup rather than the rendered page; the switch is in the panel header. */
  source?: boolean;
  /** The serve prefix a share's page was handed. Omitted, the owner's grant
   *  serves the file. */
  servePrefix?: string;
  /** Element id a reference pointed at; the iframe scrolls to it. */
  anchor?: string | null;
  /** Bumped by the panel on every reference open, so the same anchor asked for
   *  twice is two requests rather than one unchanged prop. Read only alongside
   *  `anchor`. */
  anchorSeq?: number | null;
}

export default function HtmlViewer({
  content,
  fileName,
  workspaceId,
  filePath,
  source = false,
  servePrefix,
  anchor = null,
  anchorSeq = null,
}: HtmlViewerProps) {
  const { t } = useTranslation();
  const { theme } = useTheme();
  const request = anchor ? `${anchorSeq ?? ''}\u0000${anchor}` : null;
  const iframeRef = useRef<HTMLIFrameElement>(null);

  const { pushTheme, scrollToAnchor } = useHtmlSandbox({ iframeRef, autoHeight: false });

  // The fragment in `src` lands the first open, and any open naming a different
  // section. It cannot land the same section twice, because that URL is the one
  // already loaded, so ask the document itself on every request.
  useEffect(() => {
    if (anchor) scrollToAnchor(anchor);
  }, [request, anchor, scrollToAnchor]);

  const served = useServedHtml(workspaceId, filePath, servePrefix);
  const servedUrl = served.themedUrl;

  const isLight = theme === 'light';

  const renderPreview = () => {
    if (servedUrl) {
      // src= loads: the served response's CSP `sandbox` header intersects
      // with this attribute, and serve.py owns the effective policy and the
      // link-click rationale (both must carry the popup tokens).
      return (
        <iframe
          ref={iframeRef}
          src={anchor ? `${servedUrl}#${encodeURIComponent(anchor)}` : servedUrl}
          sandbox={SERVED_HTML_SANDBOX}
          className="html-viewer-frame"
          title={fileName || 'HTML Preview'}
          onLoad={pushTheme}
        />
      );
    }
    if (served.error) {
      return (
        <div className="flex flex-col items-center justify-center gap-3 py-12 text-sm" role="alert">
          <span className="flex items-center gap-2" style={{ color: 'var(--color-text-secondary)' }}>
            <AlertCircle className="h-4 w-4 shrink-0" style={{ color: 'var(--color-icon-danger)' }} />
            {t('filePanel.htmlPreviewFailed')}
          </span>
          <Button variant="outline" size="sm" onClick={served.retry}>
            {t('common.retry')}
          </Button>
        </div>
      );
    }
    return (
      <div className="flex items-center justify-center py-12">
        <Loader size={20} className="text-(--color-text-tertiary)" />
      </div>
    );
  };

  return (
    <div className="html-viewer">
      {source ? (
        <div className="html-viewer-source">
          <SyntaxHighlighter
            language="markup"
            style={isLight ? oneLight : oneDark}
            customStyle={{ margin: 0, padding: 0, backgroundColor: 'transparent', fontSize: '0.75rem', lineHeight: '1.6' }}
            codeTagProps={{ style: { backgroundColor: 'transparent' } }}
            wrapLongLines
          >
            {content}
          </SyntaxHighlighter>
        </div>
      ) : renderPreview()}
    </div>
  );
}
