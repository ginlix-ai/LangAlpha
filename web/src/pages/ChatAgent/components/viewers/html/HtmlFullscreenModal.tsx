import { useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { Dialog, DialogContent, DialogTitle } from '@/components/ui/dialog';
import { useHtmlSandbox } from './useHtmlSandbox';
import { useDirectLinkGuard } from './useDirectLinkGuard';
import HtmlActionBar from './HtmlActionBar';
import type { HtmlActions } from './useHtmlActions';
import { buildWsfilesUrl } from './wsfilesUrl';
import './HtmlFullscreenModal.css';

interface BaseProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  actions: HtmlActions;
}

interface WidgetVariant extends BaseProps {
  variant: 'widget';
  /** widget-fullscreen srcDoc. */
  srcDoc: string;
}

interface FileVariant extends BaseProps {
  variant: 'file';
  workspaceId: string;
  filePath: string;
  /** Override the served iframe src (e.g. public share serve URL). Defaults to wsfiles. */
  servedUrl?: string;
}

type HtmlFullscreenModalProps = WidgetVariant | FileVariant;

/**
 * Fullscreen HTML preview in a centered Radix dialog (portaled to body, so it
 * sidesteps the FilePanel/DetailPanel layout). Hosts a served-URL iframe for
 * files or a widget-fullscreen srcDoc iframe for widgets.
 */
export default function HtmlFullscreenModal(props: HtmlFullscreenModalProps) {
  const { open, onOpenChange, title, actions } = props;
  const { t } = useTranslation();
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const { pushTheme } = useHtmlSandbox({ iframeRef, autoHeight: false });

  const servedUrl =
    props.variant === 'file'
      ? props.servedUrl ?? buildWsfilesUrl(props.workspaceId, props.filePath, { injectTheme: true })
      : null;

  // Owner-served files open the raw, non-revocable wsfiles URL — confirm first.
  // Widgets (blob) and public share serve URLs are exempt.
  const { request: openInNewTab, dialog: directLinkDialog } = useDirectLinkGuard(
    actions.openInNewTab,
    props.variant === 'file' && !props.servedUrl,
  );

  return (
    <>
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        variant="centered"
        className="html-fullscreen-modal !w-[95vw] !max-w-[1400px] !h-[90vh] !max-h-[90vh] !p-0 !overflow-hidden"
        aria-describedby={undefined}
      >
        <DialogTitle className="sr-only">{title}</DialogTitle>
        <div className="html-fullscreen-body">
          <div className="html-fullscreen-toolbar">
            <span className="html-fullscreen-title" title={title}>{title}</span>
            {/* No exit-fullscreen button here — the dialog's own close (×) is
                the canonical close, so a second one would overlap it. */}
            <HtmlActionBar
              onOpenInNewTab={openInNewTab}
              onDownload={actions.downloadHtml}
              onExportPdf={actions.exportPdf}
            />
          </div>
          {props.variant === 'file' ? (
            <iframe
              ref={iframeRef}
              src={servedUrl!}
              sandbox="allow-scripts"
              className="html-fullscreen-frame"
              title={title || t('filePanel.fullscreen')}
              onLoad={pushTheme}
            />
          ) : (
            <iframe
              ref={iframeRef}
              srcDoc={props.srcDoc}
              sandbox="allow-scripts"
              className="html-fullscreen-frame"
              title={title || t('filePanel.fullscreen')}
            />
          )}
        </div>
      </DialogContent>
    </Dialog>
    {directLinkDialog}
    </>
  );
}
