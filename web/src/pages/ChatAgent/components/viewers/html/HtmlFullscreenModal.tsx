import { useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { Dialog, DialogContent, DialogTitle } from '@/components/ui/dialog';
import { useHtmlSandbox } from './useHtmlSandbox';
import HtmlActionBar from './HtmlActionBar';
import type { HtmlActions } from './useHtmlActions';
import './HtmlFullscreenModal.css';

interface HtmlFullscreenModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  actions: HtmlActions;
  /**
   * widget-fullscreen srcDoc: display markup for this dialog only. `actions`
   * deliberately operates on the inline srcDoc, which is the widget as an
   * artifact — see InlineWidget.
   */
  srcDoc: string;
}

/**
 * Fullscreen widget preview in a centered Radix dialog (portaled to body, so
 * it sidesteps the chat's layout). A file has no dialog of its own: the panel
 * expands over the chat instead.
 */
export default function HtmlFullscreenModal(props: HtmlFullscreenModalProps) {
  const { open, onOpenChange, title, actions, srcDoc } = props;
  const { t } = useTranslation();
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const { pushTheme } = useHtmlSandbox({ iframeRef, autoHeight: false });

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        variant="centered"
        className="html-fullscreen-modal w-[95vw]! h-[90vh]! max-h-[90vh]! p-0! overflow-hidden! max-w-[940px]!"
        aria-describedby={undefined}
      >
        <DialogTitle className="sr-only">{title}</DialogTitle>
        <div className="html-fullscreen-body">
          <div className="html-fullscreen-toolbar">
            <span className="html-fullscreen-title" title={title}>{title}</span>
            {/* No exit-fullscreen button here — the dialog's own close (×) is
                the canonical close, so a second one would overlap it. */}
            <HtmlActionBar
              onOpenInNewTab={actions.openInNewTab}
              onDownload={actions.downloadHtml}
              onExportPdf={actions.exportPdf}
            />
          </div>
          {/* Popup tokens: agent-embedded links must open as REAL tabs (a
              sandbox-inheriting tab has no cookies — bot checks break);
              buildHtmlSrcDoc's click handler forces noopener. */}
          <div className="html-fullscreen-frame-pad">
            <iframe
              ref={iframeRef}
              srcDoc={srcDoc}
              sandbox="allow-scripts allow-popups allow-popups-to-escape-sandbox"
              className="html-fullscreen-frame"
              title={title || t('filePanel.fullscreen')}
              // A srcDoc bakes the theme in at build time, and this one is
              // built once when the widget mounts inline, while the dialog
              // creates a fresh document from it on every open. Between those
              // two moments the user can change theme: the inline frame is
              // patched live by the observer in useHtmlSandbox, but this frame
              // did not exist to be patched, so it opens wearing whatever theme
              // the thread was first rendered in.
              onLoad={pushTheme}
            />
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
