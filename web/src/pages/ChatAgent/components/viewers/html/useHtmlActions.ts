import { useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useStableHandler } from '@/hooks/useStableHandler';
import { desktop } from '@/lib/desktop';
import { holdReload } from '@/lib/reloadHolds';
import { pdfQuery } from './wsfilesUrl';
import { saveWidgetPdf } from './widgetPdf';

interface WidgetModeOptions {
  mode: 'widget';
  /** Full srcDoc — opened/downloaded/printed via a blob URL. */
  srcDoc: string;
  fileName?: string;
}

interface FileModeOptions {
  mode: 'file';
  filePath: string;
  /** Server-side download of the original bytes. */
  triggerDownload?: () => Promise<void>;
  /**
   * The byte-faithful served URL (no ?inject=theme) the PDF render is fetched
   * from. Absent until the owner's grant or the share's base is known, and the
   * export says so rather than guessing a URL.
   */
  servedUrl?: string;
  /**
   * What "open in new tab" and the print fallback open. The owner's is the
   * item's `/a/<code>` page: a served URL carries a credential, and this is
   * the one place a credential would land in an address bar.
   */
  openUrl?: string;
}

export type UseHtmlActionsOptions = WidgetModeOptions | FileModeOptions;

export interface HtmlActions {
  /**
   * Absent where a second surface cannot be opened at all: a widget inside the
   * desktop shell opens a `blob:` URL, and the shell answers every
   * `window.open` by handing the URL to the OS browser instead, which takes
   * http/https/mailto and nothing else. There is no fallback to offer — a blob
   * belongs to the renderer that made it — so the action is withheld rather
   * than left as a button that does nothing. A file surface is withheld only
   * until its `openUrl` is known, so the click can open it synchronously.
   */
  openInNewTab?: () => void;
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
  filePath: string;
  /** Byte-faithful served URL the `?format=pdf` render is fetched from. */
  servedUrl: string;
  /** Opened for the browser-print fallback. Omitted, the fallback is the hint alone. */
  openUrl?: string;
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
 * falling back to opening `openUrl` and driving the browser print dialog on
 * any non-OK response. Shared by the HTML surfaces' actions, the file panel's
 * header download menu and the public file page.
 */
export async function exportServedPdf({
  filePath,
  servedUrl,
  openUrl,
  printHint,
  generatingHint,
  scale,
  pageNumbers,
  branding,
}: ExportServedPdfOptions): Promise<void> {
  const pdfUrl = appendQueryParam(servedUrl, pdfQuery(scale, pageNumbers, branding));

  const printFallback = () => {
    // Keep the handle (no noopener) so we can drive print on the new tab.
    // Never `about:blank` first: the desktop shell decides what to do with a
    // popup by its URL, and a blank one is the shape it holds for sign-in.
    const win = openUrl ? window.open(openUrl, '_blank') : null;
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
  // A render outlasts the page that asked for it, so the stale-build reload a
  // section switch takes waits until the file is handed to the browser.
  const release = holdReload();
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
    release();
    clearTimeout(timeout);
    pending.dismiss();
  }
}

/**
 * Open/download/print actions for an HTML surface.
 *
 * Widget mode operates on a blob built from the srcDoc; file mode fetches the
 * server-rendered PDF from the byte-faithful served URL and opens the item's
 * own page in a new tab. exportPdf falls back to browser print on any non-OK
 * response.
 */
export function useHtmlActions(opts: UseHtmlActionsOptions): HtmlActions {
  const { t } = useTranslation();
  // Server PDF renders take seconds; ignore re-entry while one is in flight.
  const pdfInFlight = useRef(false);

  // `useStableHandler`, not `useCallback`: every call site builds `opts` as a
  // fresh object literal, so listing it as a dependency memoizes nothing. These
  // are click handlers and never run during render, which is the hook's one
  // precondition.
  const openInNewTab = useStableHandler(() => {
    if (opts.mode === 'widget') {
      const blob = new Blob([opts.srcDoc], { type: 'text/html' });
      const url = URL.createObjectURL(blob);
      window.open(url, '_blank', 'noopener,noreferrer');
      // Revoke once the new tab has had a chance to load.
      setTimeout(() => URL.revokeObjectURL(url), 60_000);
    } else if (opts.openUrl) {
      window.open(opts.openUrl, '_blank', 'noopener,noreferrer');
    }
  });

  const downloadHtml = useStableHandler(() => {
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
      opts.triggerDownload?.().catch((err: unknown) =>
        console.error('[useHtmlActions] Download failed:', err),
      );
    }
  });

  const exportPdf = useStableHandler(async () => {
    if (pdfInFlight.current) return;
    pdfInFlight.current = true;
    try {
      if (opts.mode === 'widget') {
        // Measuring the widget and rendering it takes seconds, same as a server
        // render, so it gets the same dismissible toast rather than a button
        // that looks broken until a file dialog appears.
        const pending = toast({ description: t('filePanel.pdfGenerating') });
        let result;
        try {
          // The shell renders it directly. Not merely nicer than the blob tab
          // below — that tab is one the shell refuses to open, so this is the
          // only route a widget has there. A null answer means no shell at all,
          // which is the only case that falls through.
          result = await saveWidgetPdf(opts.srcDoc, opts.fileName || 'widget');
        } finally {
          pending.dismiss();
        }
        if (result) {
          if ('error' in result) toast({ description: t('filePanel.pdfFailed') });
          return;
        }
        // Null inside the shell does not mean "print in the browser instead", it
        // means this shell is too old to know the channel. The fallback below is
        // a blob: tab, which the shell hands to the OS browser and the OS browser
        // will not open, so falling through here is a button that does nothing at
        // all and says nothing either. Every install older than the one that
        // added savePdf lands on exactly this line.
        if (desktop) {
          toast({ description: t('filePanel.pdfFailed') });
          return;
        }
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

      if (!opts.servedUrl) {
        toast({ description: t('filePanel.pdfFailed') });
        return;
      }
      await exportServedPdf({
        filePath: opts.filePath,
        servedUrl: opts.servedUrl,
        openUrl: opts.openUrl,
        printHint: t('filePanel.pdfPrintHint'),
        generatingHint: t('filePanel.pdfGenerating'),
      });
    } finally {
      pdfInFlight.current = false;
    }
  });

  const canOpen = opts.mode === 'widget' ? !desktop : !!opts.openUrl;
  return {
    openInNewTab: canOpen ? openInNewTab : undefined,
    downloadHtml,
    exportPdf,
  };
}
