import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { Previewer } from 'pagedjs';
import { useReactToPrint } from 'react-to-print';
import { useTranslation } from 'react-i18next';
import { Minus, Plus } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { MobileBottomSheet } from '@/components/ui/mobile-bottom-sheet';
import { Select } from '@/components/ui/select';
import { useIsMobile } from '@/hooks/useIsMobile';
import Markdown from './Markdown';
import { stripLineNumbers } from './toolDisplayConfig';
import { PRINT_PAGE_STYLE } from './printPageStyle';
import { renderToPdf } from '@/lib/shellPdf';
import './ExportPreviewModal.css';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface PrintFont {
  value: string;
  label: string;
  group: string;
  google?: string;
}

interface PrintPreset {
  label: string;
  font: string;
  size: number;
  height: number;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const PRINT_FONTS: PrintFont[] = [
  // Sans-serif
  { value: 'system-ui, -apple-system, sans-serif', label: 'System Sans', group: 'Sans-serif' },
  { value: '"Inter", sans-serif', label: 'Inter', group: 'Sans-serif', google: 'Inter' },
  { value: '"Open Sans", sans-serif', label: 'Open Sans', group: 'Sans-serif', google: 'Open+Sans' },
  { value: '"Noto Sans", sans-serif', label: 'Noto Sans', group: 'Sans-serif', google: 'Noto+Sans' },
  { value: '"Roboto", sans-serif', label: 'Roboto', group: 'Sans-serif', google: 'Roboto' },
  // Serif
  { value: '"Merriweather", serif', label: 'Merriweather', group: 'Serif', google: 'Merriweather' },
  { value: '"Lora", serif', label: 'Lora', group: 'Serif', google: 'Lora' },
  { value: '"Source Serif 4", serif', label: 'Source Serif', group: 'Serif', google: 'Source+Serif+4' },
  { value: '"Noto Serif", serif', label: 'Noto Serif', group: 'Serif', google: 'Noto+Serif' },
  // Monospace
  { value: '"JetBrains Mono", monospace', label: 'JetBrains Mono', group: 'Mono', google: 'JetBrains+Mono' },
  { value: '"Fira Code", monospace', label: 'Fira Code', group: 'Mono', google: 'Fira+Code' },
  { value: '"Source Code Pro", monospace', label: 'Source Code Pro', group: 'Mono', google: 'Source+Code+Pro' },
];

export const GOOGLE_FONTS_URL =
  'https://fonts.googleapis.com/css2?' +
  PRINT_FONTS.filter((f) => f.google)
    .map((f) => `family=${f.google}:wght@400;600;700`)
    .join('&') +
  '&display=swap';

export const PRINT_PRESETS: PrintPreset[] = [
  { label: 'Equity Research', font: '"Inter", sans-serif', size: 11, height: 1.4 },
  { label: 'Academic', font: '"Source Serif 4", serif', size: 12, height: 1.6 },
  { label: 'Technical', font: '"JetBrains Mono", monospace', size: 12, height: 1.5 },
  { label: 'General', font: 'system-ui, -apple-system, sans-serif', size: 14, height: 1.6 },
];

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface ExportPreviewModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  content: string;
  fileName: string;
  workspaceId: string;
  readFileFullFn: (workspaceId: string, filePath: string) => Promise<{ content: string }>;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function ExportPreviewModal({
  open,
  onOpenChange,
  content,
  fileName,
  workspaceId,
  readFileFullFn,
}: ExportPreviewModalProps) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const printRef = useRef<HTMLDivElement>(null);
  const pagedContainerRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);

  // ---- Typography settings ----
  const [printFontFamily, setPrintFontFamily] = useState(PRINT_FONTS[1].value);
  const [printFontSize, setPrintFontSize] = useState(11);
  const [printLineHeight, setPrintLineHeight] = useState(1.4);

  // ---- Async content ----
  const [fullContent, setFullContent] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pageCount, setPageCount] = useState(0);
  const [rendering, setRendering] = useState(false);
  /** A save in flight, which is a different wait from `rendering` (the preview). */
  const [saving, setSaving] = useState(false);
  const [previewZoom, setPreviewZoom] = useState(0.5);
  const previewZoomRef = useRef(previewZoom);
  previewZoomRef.current = previewZoom;

  // ---- Derived ----
  const activePreset = useMemo(
    () =>
      PRINT_PRESETS.find(
        (p) => p.font === printFontFamily && p.size === printFontSize && p.height === printLineHeight,
      ) ?? null,
    [printFontFamily, printFontSize, printLineHeight],
  );

  const displayContent = fullContent ?? content;

  // ---- Fetch full content ----
  const fetchContent = useCallback(() => {
    // Abort any in-flight request before starting a new one
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    const signal = controller.signal;

    setLoading(true);
    setError(null);
    readFileFullFn(workspaceId, fileName)
      .then((res) => {
        if (signal.aborted) return;
        const raw = fileName.startsWith('/large_tool_results/')
          ? stripLineNumbers(res.content) ?? res.content
          : res.content;
        setFullContent(raw);
        setLoading(false);
      })
      .catch((err) => {
        if (signal.aborted) return;
        setError(err instanceof Error ? err.message : 'Failed to load content');
        setLoading(false);
      });
  }, [readFileFullFn, workspaceId, fileName]);

  // ---- Open / close lifecycle ----
  useEffect(() => {
    if (!open) {
      // Reset on close, abort any in-flight request
      abortRef.current?.abort();
      setFullContent(null);
      setLoading(false);
      setError(null);
      setPageCount(0);
      setRendering(false);
      return;
    }

    fetchContent();
    return () => abortRef.current?.abort();
  }, [open, fetchContent]);

  // ---- Lazy-load Google Fonts ----
  useEffect(() => {
    if (!open) return;
    if (document.getElementById('print-google-fonts')) return;
    const link = document.createElement('link');
    link.id = 'print-google-fonts';
    link.rel = 'stylesheet';
    link.href = GOOGLE_FONTS_URL;
    document.head.appendChild(link);
  }, [open]);

  // ---- Debounced typography values (avoid re-paginating on every stepper click) ----
  const typoTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [debouncedTypo, setDebouncedTypo] = useState({
    font: printFontFamily,
    size: printFontSize,
    height: printLineHeight,
  });
  useEffect(() => {
    if (typoTimerRef.current) clearTimeout(typoTimerRef.current);
    typoTimerRef.current = setTimeout(() => {
      setDebouncedTypo({ font: printFontFamily, size: printFontSize, height: printLineHeight });
    }, 300);
    return () => { if (typoTimerRef.current) clearTimeout(typoTimerRef.current); };
  }, [printFontFamily, printFontSize, printLineHeight]);

  // ---- Paged.js preview rendering ----
  // Generation counter: incremented on each render attempt. Stale renders
  // (where a newer render was triggered before the old one finished) are
  // detected by comparing the counter and silently ignored. This prevents
  // crashes from Paged.js trying to layout elements that were removed from
  // the DOM by a newer render clearing the container.
  const renderGenRef = useRef(0);
  useEffect(() => {
    if (!fullContent || loading || error) return;
    const sourceEl = printRef.current;
    const container = pagedContainerRef.current;
    if (!sourceEl || !container) return;

    renderGenRef.current += 1;
    const gen = renderGenRef.current;
    setRendering(true);

    const renderPreview = async () => {
      // Wait two frames for Markdown component to render into sourceEl
      await new Promise((r) => requestAnimationFrame(r));
      await new Promise((r) => requestAnimationFrame(r));
      if (gen !== renderGenRef.current) return;

      // Hide container during rendering to prevent layout flash.
      // Paged.js adds pages at full A4 size (794×1123px) one by one,
      // then we apply zoom after. Without hiding, the user sees
      // full-size pages appear and then snap to zoomed size.
      container.style.visibility = 'hidden';

      // Clean up previous Paged.js artifacts
      container.innerHTML = '';
      document.querySelectorAll('style[data-pagedjs-inserted-styles]').forEach((el) => el.remove());

      // Clone source content into a wrapper with our typography classes
      const wrapper = document.createElement('div');
      wrapper.className = 'markdown-print-content print-preview-active';
      wrapper.style.cssText = [
        `--print-font-size:${debouncedTypo.size}px`,
        `--print-line-height:${debouncedTypo.height}`,
        `--print-font-family:${debouncedTypo.font}`,
      ].join(';');
      wrapper.innerHTML = sourceEl.innerHTML;

      const fragment = document.createDocumentFragment();
      fragment.appendChild(wrapper);

      // @page stylesheet for Paged.js to determine page dimensions
      const pageCSS = `@page { size: A4; margin: 15mm; }`;

      try {
        const previewer = new Previewer();
        const flow = await previewer.preview(
          fragment,
          [{ [window.location.href]: pageCSS }],
          container,
        );
        if (gen !== renderGenRef.current) return; // superseded
        setPageCount(flow.total);
        // Apply zoom AFTER Paged.js finishes measuring.
        // We use transform:scale() instead of CSS zoom for GPU-accelerated
        // compositing. CSS zoom is layout-affecting (every scroll frame
        // recalculates layout), while transform:scale() is paint-only.
        // The tradeoff: transform doesn't change the element's layout box,
        // so we set the wrapper height explicitly to fix scroll extent.
        const pagesEl = container.querySelector('.pagedjs_pages') as HTMLElement | null;
        if (pagesEl) {
          const zoom = previewZoomRef.current;
          const naturalHeight = pagesEl.scrollHeight;
          pagesEl.style.transform = `scale(${zoom})`;
          pagesEl.style.transformOrigin = 'top center';
          container.style.height = `${naturalHeight * zoom}px`;
        }
        // Reveal now that zoom is applied — no layout flash
        container.style.visibility = 'visible';
        setRendering(false);
      } catch (err) {
        // Ignore errors from superseded renders (stale Paged.js instances
        // trying to layout elements removed by a newer render)
        if (gen !== renderGenRef.current) return;
        console.error('[ExportPreviewModal] Paged.js render failed:', err);
        container.style.visibility = 'visible';
        setRendering(false);
      }
    };

    renderPreview();
  }, [fullContent, loading, error, debouncedTypo]);

  // ---- Apply zoom changes without re-rendering Paged.js ----
  useEffect(() => {
    const container = pagedContainerRef.current;
    if (!container) return;
    const pagesEl = container.querySelector('.pagedjs_pages') as HTMLElement | null;
    if (pagesEl) {
      pagesEl.style.transform = `scale(${previewZoom})`;
      pagesEl.style.transformOrigin = 'top center';
      // scrollHeight is layout-based (unaffected by transform), so it's always the natural height
      container.style.height = `${pagesEl.scrollHeight * previewZoom}px`;
    }
  }, [previewZoom]);

  // ---- Cleanup Paged.js styles on unmount ----
  useEffect(() => {
    return () => {
      document.querySelectorAll('style[data-pagedjs-inserted-styles]').forEach((el) => el.remove());
    };
  }, []);

  // ---- Print handler ----
  // react-to-print injects pageStyle FIRST into the iframe <head>, then copies
  // all parent document <style> elements AFTER it. Paged.js has injected
  // <style data-pagedjs-inserted-styles> with @page{margin:0} into the parent,
  // so those copied styles win by cascade order. Using !important ensures our
  // margins take precedence regardless of source order.
  //
  // color-scheme belongs here rather than in the stylesheet: pageStyle is
  // inlined into the iframe, whereas this component's lazy chunk arrives as a
  // <link> that the iframe re-fetches — and react-to-print prints anyway if
  // that fetch fails, which would leave index.html's unscoped dark scheme in
  // force. Scoping it here also keeps it off the app's own Ctrl+P output.

  // What the saved file is called, on both paths. `fileName` is the workspace
  // path the panel selected, not a name: left whole, the shell would suggest
  // `results-q3.md.pdf` for `results/q3.md`, since it flattens the separator
  // rather than reading a path out of a name it was handed.
  const pdfStem = useMemo(
    () => (fileName.split('/').pop() || 'export').replace(/\.[^.]+$/, ''),
    [fileName],
  );

  const handlePrint = useReactToPrint({
    contentRef: printRef,
    pageStyle: PRINT_PAGE_STYLE,
    // The browser print dialog names the file after `document.title`, which is
    // the app's, so every report saved from a browser landed in the user's
    // downloads as `LangAlpha.pdf`. react-to-print swaps the title in for the
    // duration of the print and puts it back after.
    documentTitle: pdfStem,
  });

  // A desktop shell renders the PDF itself, which skips the print dialog and
  // produces a tagged, outlined file that browser print cannot emit at all.
  // Feature-detected per the bridge contract, because the shell updates on its
  // own cadence and this app deploys continuously: an older shell simply has no
  // savePdf and keeps the browser path below.
  const handleSave = useCallback(async () => {
    const source = printRef.current;
    if (!source) {
      handlePrint();
      return;
    }
    // Cloning the whole rendered report and then reflowing the document to
    // paper width is seconds of frozen tab on a long one. Without this the
    // button stays live throughout, and a second click is swallowed by the
    // re-entry guard in renderToPdf, which answers `canceled` and deliberately
    // shows nothing: the control reads as dead rather than as busy.
    setSaving(true);
    try {
      // printToPDF reflows the live document to the paper width before it takes
      // its snapshot. The app re-renders at that width and the dialog unmounts,
      // taking the portal below with it, so anything React owns is already gone
      // when the snapshot happens — the file comes out correctly paginated and
      // completely blank. A detached copy is not React's to remove and survives
      // the reflow, which is why the export is cloned rather than printed in
      // place. renderToPdf owns that node, the page geometry and the teardown.
      const result = await renderToPdf((root) => {
        root.appendChild(source.cloneNode(true));
      }, pdfStem);

      // No shell, or the shell failed inside the channel. A `canceled` result is
      // the user's own answer and must not reopen a dialog they just dismissed.
      if (!result || 'error' in result) handlePrint();
    } finally {
      setSaving(false);
    }
  }, [handlePrint, pdfStem]);

  // ---- CSS vars for source (used by react-to-print) ----
  const cssVars = useMemo(
    () =>
      ({
        '--print-font-size': `${printFontSize}px`,
        '--print-line-height': printLineHeight,
        '--print-font-family': printFontFamily,
      }) as React.CSSProperties,
    [printFontSize, printLineHeight, printFontFamily],
  );

  // ---- Font groups for <optgroup> ----
  const fontGroups = useMemo(() => {
    const groups: Record<string, PrintFont[]> = {};
    for (const f of PRINT_FONTS) {
      (groups[f.group] ??= []).push(f);
    }
    return groups;
  }, []);

  // ---- Shared control fragments ----
  const presetSelect = (
    <Select
      value={activePreset?.label ?? ''}
      onChange={(e) => {
        const p = PRINT_PRESETS.find((pr) => pr.label === e.target.value);
        if (p) {
          setPrintFontFamily(p.font);
          setPrintFontSize(p.size);
          setPrintLineHeight(p.height);
        }
      }}
    >
      {!activePreset && <option value="">{t('filePanel.custom') ?? 'Custom'}</option>}
      {PRINT_PRESETS.map((p) => (
        <option key={p.label} value={p.label}>
          {p.label}
        </option>
      ))}
    </Select>
  );

  const fontSelect = (
    <Select value={printFontFamily} onChange={(e) => setPrintFontFamily(e.target.value)}>
      {Object.entries(fontGroups).map(([group, fonts]) => (
        <optgroup key={group} label={group}>
          {fonts.map((f) => (
            <option key={f.value} value={f.value}>
              {f.label}
            </option>
          ))}
        </optgroup>
      ))}
    </Select>
  );

  const fontSizeStepper = (
    <div className="export-preview-stepper">
      <button
        className="export-preview-stepper-btn"
        onClick={() => setPrintFontSize((v) => Math.max(10, v - 1))}
        disabled={printFontSize <= 10}
      >
        <Minus className="h-3 w-3" />
      </button>
      <span className="export-preview-stepper-value">{printFontSize}px</span>
      <button
        className="export-preview-stepper-btn"
        onClick={() => setPrintFontSize((v) => Math.min(22, v + 1))}
        disabled={printFontSize >= 22}
      >
        <Plus className="h-3 w-3" />
      </button>
    </div>
  );

  const lineHeightStepper = (
    <div className="export-preview-stepper">
      <button
        className="export-preview-stepper-btn"
        onClick={() => setPrintLineHeight((v) => +Math.max(1.2, v - 0.2).toFixed(1))}
        disabled={printLineHeight <= 1.2}
      >
        <Minus className="h-3 w-3" />
      </button>
      <span className="export-preview-stepper-value">{printLineHeight.toFixed(1)}</span>
      <button
        className="export-preview-stepper-btn"
        onClick={() => setPrintLineHeight((v) => +Math.min(2.4, v + 0.2).toFixed(1))}
        disabled={printLineHeight >= 2.4}
      >
        <Plus className="h-3 w-3" />
      </button>
    </div>
  );

  const zoomStepper = (
    <div className="export-preview-stepper">
      <button
        className="export-preview-stepper-btn"
        onClick={() => setPreviewZoom((v) => +Math.max(0.25, v - 0.1).toFixed(2))}
        disabled={previewZoom <= 0.25}
      >
        <Minus className="h-3 w-3" />
      </button>
      <span className="export-preview-stepper-value">{Math.round(previewZoom * 100)}%</span>
      <button
        className="export-preview-stepper-btn"
        onClick={() => setPreviewZoom((v) => +Math.min(1, v + 0.1).toFixed(2))}
        disabled={previewZoom >= 1}
      >
        <Plus className="h-3 w-3" />
      </button>
    </div>
  );

  const pageCountLabel = !loading && !error && pageCount > 0 && (
    <span className="export-preview-page-count">
      {t('filePanel.pages', { count: pageCount }) ?? `~${pageCount} pages`}
    </span>
  );

  // Disable Save when preview is still catching up to typography changes
  const typoPending =
    debouncedTypo.font !== printFontFamily ||
    debouncedTypo.size !== printFontSize ||
    debouncedTypo.height !== printLineHeight;

  const saveBtn = (
    <button
      className="export-preview-save-btn"
      onClick={() => void handleSave()}
      disabled={rendering || typoPending || saving}
      style={rendering || typoPending || saving ? { opacity: 0.6, cursor: 'wait' } : undefined}
    >
      {saving ? t('filePanel.pdfGenerating') : t('filePanel.saveAsPdf')}
    </button>
  );

  // ---- Desktop sidebar (vertical stack) ----
  const desktopSidebar = (
    <div className="export-preview-sidebar">
      <label className="export-preview-sidebar-label">{t('filePanel.style')}</label>
      {presetSelect}
      <label className="export-preview-sidebar-label">{t('filePanel.font')}</label>
      {fontSelect}
      <label className="export-preview-sidebar-label">{t('filePanel.fontSize')}</label>
      {fontSizeStepper}
      <label className="export-preview-sidebar-label">{t('filePanel.lineHeight')}</label>
      {lineHeightStepper}
      <label className="export-preview-sidebar-label">{t('filePanel.zoom') ?? 'Zoom'}</label>
      {zoomStepper}
      {pageCountLabel}
      {saveBtn}
    </div>
  );

  // ---- Mobile settings (2-column grid rows) ----
  const mobileSettings = (
    <div className="export-preview-mobile-settings">
      <div className="export-preview-mobile-row">
        <label className="export-preview-sidebar-label">{t('filePanel.style')}</label>
        {presetSelect}
      </div>
      <div className="export-preview-mobile-row">
        <label className="export-preview-sidebar-label">{t('filePanel.font')}</label>
        {fontSelect}
      </div>
      <div className="export-preview-mobile-row-pair">
        <div className="export-preview-mobile-row">
          <label className="export-preview-sidebar-label">{t('filePanel.fontSize')}</label>
          {fontSizeStepper}
        </div>
        <div className="export-preview-mobile-row">
          <label className="export-preview-sidebar-label">{t('filePanel.lineHeight')}</label>
          {lineHeightStepper}
        </div>
        <div className="export-preview-mobile-row">
          <label className="export-preview-sidebar-label">{t('filePanel.zoom') ?? 'Zoom'}</label>
          {zoomStepper}
        </div>
        <div className="export-preview-mobile-row">
          {pageCountLabel}
        </div>
      </div>
      {saveBtn}
    </div>
  );

  // ---- Shared preview area ----
  const previewArea = (
    <div className="export-preview-content">
      {error ? (
        <div className="export-preview-error">
          <p>
            {t('filePanel.exportLoadError') ??
              'Could not load full content. Export may be incomplete.'}
          </p>
          <div style={{ display: 'flex', gap: 8 }}>
            <button onClick={() => fetchContent()} className="export-preview-retry-btn">
              {t('filePanel.tryAgain') ?? 'Try Again'}
            </button>
            <button onClick={() => { setError(null); setFullContent(content); }} className="export-preview-anyway-btn">
              {t('filePanel.exportAnyway') ?? 'Export Anyway'}
            </button>
          </div>
        </div>
      ) : (
        <>
          {/* Skeleton overlay — shown during fetch AND Paged.js rendering */}
          {(loading || !fullContent || rendering) && (
            <div className="export-preview-skeleton">
              {[80, 60, 90, 40, 75].map((w, i) => (
                <div
                  key={i}
                  className="export-preview-skeleton-bar"
                  style={{ width: `${w}%` }}
                />
              ))}
            </div>
          )}
          {/* Hidden source, portaled to <body> rather than left inside the
              modal, and parked off-screen. This is what Paged.js clones for the
              preview and what react-to-print copies into its iframe; the
              desktop path clones it again into a detached `#export-pdf-root`
              for the duration of the render (see handleSave). It carries no id
              of its own: the print stylesheet keeps `#export-pdf-root` and
              drops every other body child out of the flow, and an invisible app
              still lays out — the first attempt at this produced a 57-page PDF
              with the report buried in blank pages of chat. */}
          {createPortal(
            <div className="export-pdf-host">
              <div
                ref={printRef}
                className="markdown-print-content print-preview-active export-preview-source"
                style={cssVars}
              >
                <Markdown variant="panel" content={displayContent} codeTheme="light" />
              </div>
            </div>,
            document.body,
          )}
          {/* Paged.js renders paginated page sheets here */}
          <div ref={pagedContainerRef} className="export-preview-paged" />
        </>
      )}
    </div>
  );

  // ---- Mobile: bottom sheet with stacked layout ----
  // Portal to document.body so the sheet escapes FilePanel's stacking context
  // (FilePanel's mobile overlay has zIndex:30, trapping children below the
  // BottomTabBar at z-1000). Portaling lets z-1020 work as intended.
  if (isMobile) {
    return createPortal(
      <MobileBottomSheet
        open={open}
        onClose={() => onOpenChange(false)}
        sizing="fixed"
        height="92vh"
        style={{ paddingBottom: 'calc(var(--bottom-tab-height, 0px) + 16px)' }}
      >
        <div className="export-preview-mobile-header">
          <h2 className="export-preview-mobile-title">{t('filePanel.exportPreview')}</h2>
        </div>
        {mobileSettings}
        {previewArea}
      </MobileBottomSheet>,
      document.body,
    );
  }

  // ---- Desktop: centered dialog with side-by-side layout ----
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="export-preview-modal !w-[90vw] !max-w-7xl !p-0 !overflow-hidden"
        variant="centered"
        aria-describedby={undefined}
      >
        <DialogHeader className="px-6 pt-6 pb-0">
          <DialogTitle>{t('filePanel.exportPreview')}</DialogTitle>
        </DialogHeader>

        <div className="export-preview-body">
          {desktopSidebar}
          {previewArea}
        </div>
      </DialogContent>
    </Dialog>
  );
}
