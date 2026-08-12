import { useCallback, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { buildWsfilesUrl, pdfQuery } from './wsfilesUrl';

interface WidgetModeOptions {
  mode: 'widget';
  /** Full srcDoc — opened/downloaded/printed via a blob URL. */
  srcDoc: string;
  fileName?: string;
}

interface FileModeOptions {
  mode: 'file';
  workspaceId: string;
  filePath: string;
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
  exportPdf: () => void | Promise<void>;
}

function fileNameFromPath(filePath: string): string {
  return filePath.split('/').pop() || 'download.html';
}

/** `<file stem>.pdf` for the server-rendered download (e.g. report.html → report.pdf). */
function pdfNameFromPath(filePath: string): string {
  const base = fileNameFromPath(filePath);
  return `${base.replace(/\.[^.]+$/, '')}.pdf`;
}

/** Append a query param, respecting whether the URL already carries one. */
function appendQueryParam(url: string, param: string): string {
  return `${url}${url.includes('?') ? '&' : '?'}${param}`;
}

// Cap the PDF fetch above the server's 30s render budget so a transport-level
// hang (server never responds) can't leave the request in flight forever —
// without it the in-flight guard never clears and the user can't retry.
const PDF_FETCH_TIMEOUT_MS = 120_000;

export interface ExportServedPdfOptions {
  workspaceId: string;
  filePath: string;
  /** Byte-faithful served URL override (e.g. public share). Defaults to wsfiles. */
  servedUrl?: string;
  /** Toast text shown when the print-dialog fallback can't auto-print. */
  printHint: string;
  /** Toast shown while the server render is in flight (cleared when it settles). */
  generatingHint: string;
  /** Render scale (server clamps to 0.5–2). 1 = default, omitted from the URL. */
  scale?: number;
  /** Draw an 'N / total' footer in the page margin. */
  pageNumbers?: boolean;
  /** The "langalpha · <date>" footer. Server default is on; pass false to drop it. */
  branding?: boolean;
}

/**
 * Download the server-rendered PDF (?format=pdf) for a served HTML file,
 * falling back to opening the served HTML and driving the browser print
 * dialog on any non-OK response. Shared by the HTML surfaces' actions and
 * the file panel's header download menu.
 */
export async function exportServedPdf({
  workspaceId,
  filePath,
  servedUrl,
  printHint,
  generatingHint,
  scale,
  pageNumbers,
  branding,
}: ExportServedPdfOptions): Promise<void> {
  const servedHtmlUrl = servedUrl ?? buildWsfilesUrl(workspaceId, filePath);
  const pdfUrl = servedUrl
    ? appendQueryParam(servedUrl, pdfQuery(scale, pageNumbers, branding))
    : buildWsfilesUrl(workspaceId, filePath, {
        format: 'pdf',
        pdfScale: scale,
        pdfPageNumbers: pageNumbers,
        pdfBranding: branding,
      });

  const printFallback = () => {
    // Keep the handle (no noopener) so we can drive print on the new tab.
    const win = window.open(servedHtmlUrl, '_blank');
    try {
      if (!win) throw new Error('popup blocked');
      win.print();
    } catch {
      toast({ description: printHint });
    }
  };

  // Server renders take seconds — show a dismissible "generating" toast so the
  // wait isn't silent, and clear it once the request settles either way.
  const pending = toast({ description: generatingHint });
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), PDF_FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(pdfUrl, { signal: controller.signal });
    if (!res.ok) {
      printFallback();
      return;
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = pdfNameFromPath(filePath);
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  } catch {
    // Includes the AbortError from the timeout above — fall back to print.
    printFallback();
  } finally {
    clearTimeout(timeout);
    pending.dismiss();
  }
}

/**
 * Open/download/print actions for an HTML surface.
 *
 * Widget mode operates on a blob built from the srcDoc; file mode points at the
 * served wsfiles URL (byte-faithful — no ?inject=theme so downloads match the
 * original) and downloads the server's original bytes. exportPdf fetches the
 * server-rendered PDF (?format=pdf) and falls back to browser print on any
 * non-OK response.
 */
export function useHtmlActions(opts: UseHtmlActionsOptions): HtmlActions {
  const { t } = useTranslation();
  // Server PDF renders take seconds; ignore re-entry while one is in flight.
  const pdfInFlight = useRef(false);

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

  const exportPdf = useCallback(async () => {
    if (opts.mode === 'widget') {
      const blob = new Blob([opts.srcDoc], { type: 'text/html' });
      const url = URL.createObjectURL(blob);
      // Same-origin blob tab: keep the handle (no noopener) so auto-print fires.
      const win = window.open(url, '_blank');
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
      return;
    }

    if (pdfInFlight.current) return;
    pdfInFlight.current = true;
    try {
      await exportServedPdf({
        workspaceId: opts.workspaceId,
        filePath: opts.filePath,
        servedUrl: opts.servedUrl,
        printHint: t('filePanel.pdfPrintHint'),
        generatingHint: t('filePanel.pdfGenerating'),
      });
    } finally {
      pdfInFlight.current = false;
    }
  }, [opts, t]);

  return { openInNewTab, downloadHtml, exportPdf };
}
