import { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { buildWsfilesUrl } from './wsfilesUrl';

interface WidgetModeOptions {
  mode: 'widget';
  /** Full srcDoc — opened/downloaded/printed via a blob URL. */
  srcDoc: string;
  /** Source copied to clipboard (defaults to srcDoc). */
  source?: string;
  fileName?: string;
}

interface FileModeOptions {
  mode: 'file';
  workspaceId: string;
  filePath: string;
  /** Full source content copied to clipboard. */
  content: string;
  /** Server-side download of the original bytes. */
  triggerDownload?: (workspaceId: string, filePath: string) => Promise<void>;
  /** Override the served URL (e.g. the public share serve URL). Byte-faithful
   *  — no ?inject=theme — so open/print match the original. Defaults to wsfiles. */
  servedUrl?: string;
}

export type UseHtmlActionsOptions = WidgetModeOptions | FileModeOptions;

export interface HtmlActions {
  openInNewTab: () => void;
  downloadHtml: () => void;
  exportPdf: () => void;
  copySource: () => void;
}

function fileNameFromPath(filePath: string): string {
  return filePath.split('/').pop() || 'download.html';
}

/**
 * Open/download/print/copy actions for an HTML surface.
 *
 * Widget mode operates on a blob built from the srcDoc; file mode points at the
 * served wsfiles URL (byte-faithful — no ?inject=theme so downloads match the
 * original) and downloads the server's original bytes.
 */
export function useHtmlActions(opts: UseHtmlActionsOptions): HtmlActions {
  const { t } = useTranslation();

  const openInNewTab = useCallback(() => {
    if (opts.mode === 'widget') {
      const blob = new Blob([opts.srcDoc], { type: 'text/html' });
      const url = URL.createObjectURL(blob);
      window.open(url, '_blank', 'noopener,noreferrer');
      // Revoke once the new tab has had a chance to load.
      setTimeout(() => URL.revokeObjectURL(url), 60_000);
    } else {
      const url = opts.servedUrl ?? buildWsfilesUrl(opts.workspaceId, opts.filePath);
      window.open(url, '_blank', 'noopener,noreferrer');
    }
  }, [opts]);

  const downloadHtml = useCallback(() => {
    if (opts.mode === 'widget') {
      const blob = new Blob([opts.srcDoc], { type: 'text/html' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = opts.fileName || 'widget.html';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } else {
      // Server original bytes, not the rendered content.
      opts.triggerDownload?.(opts.workspaceId, opts.filePath).catch((err: unknown) =>
        console.error('[useHtmlActions] Download failed:', err),
      );
    }
  }, [opts]);

  const exportPdf = useCallback(() => {
    if (opts.mode === 'widget') {
      const blob = new Blob([opts.srcDoc], { type: 'text/html' });
      const url = URL.createObjectURL(blob);
      const win = window.open(url, '_blank', 'noopener,noreferrer');
      // Same-origin blob tab: auto-print once it loads.
      if (win) {
        const triggerPrint = () => {
          try {
            win.print();
          } catch {
            /* user can print manually */
          }
        };
        win.addEventListener?.('load', triggerPrint);
        setTimeout(triggerPrint, 800);
      }
      setTimeout(() => URL.revokeObjectURL(url), 60_000);
    } else {
      const url = opts.servedUrl ?? buildWsfilesUrl(opts.workspaceId, opts.filePath);
      const win = window.open(url, '_blank', 'noopener,noreferrer');
      // Opaque cross-origin tab: auto-print is usually blocked — fall back to a hint.
      try {
        if (!win) throw new Error('popup blocked');
        win.print();
      } catch {
        toast({ description: t('filePanel.pdfPrintHint') });
      }
    }
  }, [opts, t]);

  const copySource = useCallback(() => {
    const text = opts.mode === 'widget' ? (opts.source ?? opts.srcDoc) : opts.content;
    navigator.clipboard
      .writeText(text)
      .then(() => toast({ description: t('filePanel.copiedToClipboard') }))
      .catch(() => toast({ description: t('filePanel.copyFailed'), variant: 'destructive' }));
  }, [opts, t]);

  return { openInNewTab, downloadHtml, exportPdf, copySource };
}

export { fileNameFromPath };
