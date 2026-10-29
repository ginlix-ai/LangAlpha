import React, { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { PDFDocumentProxy } from 'pdfjs-dist';
import { PanelLeft } from 'lucide-react';
import { useStableHandler } from '@/hooks/useStableHandler';
import { isRangedFile, markStale, type RangedFile } from '@/lib/rangedFile';
import { Document, Page, pdfjs } from 'react-pdf';
import PdfRail, { type RailTab } from './pdf/PdfRail';
import { type PageSize, sizeOf, withMeasured } from './pdf/pageSize';
import { rangeTransport } from './pdf/rangeTransport';
import { useReleasePages } from './pdf/useReleasePages';
import 'react-pdf/dist/Page/AnnotationLayer.css';
import 'react-pdf/dist/Page/TextLayer.css';
import './PdfViewer.css';

// Vite-native ?url import resolves correctly in both dev and build. Legacy, to
// match the main-thread build vite.config.js aliases react-pdf to.
import pdfjsWorkerUrl from 'pdfjs-dist/legacy/build/pdf.worker.min.mjs?url';
pdfjs.GlobalWorkerOptions.workerSrc = pdfjsWorkerUrl;

// Runtime data the build emits beside the worker (scripts/vite-plugins/pdfjsData.ts).
// With all of these set pdf.js lets the worker fetch for itself, which it also
// requires before color-managing ICC content. Module-level so react-pdf sees
// one options object and never reloads over it.
const PDFJS_DATA_URL = `${import.meta.env.BASE_URL}assets/pdfjs/${pdfjs.version}/`;
const DOCUMENT_OPTIONS = {
  wasmUrl: `${PDFJS_DATA_URL}wasm/`,
  cMapUrl: `${PDFJS_DATA_URL}cmaps/`,
  standardFontDataUrl: `${PDFJS_DATA_URL}standard_fonts/`,
  iccUrl: `${PDFJS_DATA_URL}iccs/`,
};
// A file opened from its first slice. pdf.js fetches only the ranges the pages
// it draws need, so pages are measured as they render. Its default chunk stays:
// opening reads every page dict of a flat page tree, and a larger chunk turns
// each small dict into that much of the file, while one object larger than a
// chunk still arrives in one request.
const RANGED_OPTIONS = { ...DOCUMENT_OPTIONS, disableAutoFetch: true, disableStream: true };

// Scales relative to the PDF's own size (1 = one point per CSS pixel). Chrome's
// presets, stopped at 3: every page in the render window holds a canvas, and a
// Letter page at 3x on a 2x display is already 17M pixels.
const ZOOM_STEPS = [0.25, 0.33, 0.5, 0.67, 0.75, 0.8, 0.9, 1, 1.1, 1.25, 1.5, 1.75, 2, 2.5, 3];
// A preset within 3% of the current scale counts as that scale, so + from a fit
// of 123% lands on 150%, not on a 125% that looks like nothing happened.
const STEP_SLACK = 1.03;

// Space around and between pages. The scroll math and the column's CSS both
// read it, so a scroll to a page's top lands on the edge the reader sees.
const GAP = 12;
// Pages mount within one scroller height above and below the visible area.
const RENDER_MARGIN = '100% 0px';
// A panel drag changes the scale every frame, and react-pdf blanks a canvas
// before redrawing it, so pages are stretched to the new scale at once and
// redrawn only once the scale has held this long.
const REDRAW_DELAY_MS = 150;
// Past this many pages the rest are measured only as they load, pdf.js's own
// viewer's limit (PagesCountLimit.FORCE_LAZY_PAGE_INIT).
const MEASURE_ALL_UP_TO = 5000;
// Safari draws a canvas over 16.7M pixels blank, the lowest limit of the
// browsers this app supports, and Chrome fails a side over 32767, so a page
// that large draws at fewer pixels per CSS pixel: softer, never blank. pdf.js's
// own viewer caps it the same way (maxCanvasPixels).
const MAX_CANVAS_PIXELS = 2 ** 24;
const MAX_CANVAS_SIDE = 32767;

type Zoom = 'fit' | number;

interface Layout {
  scale: number;
  widths: number[];
  heights: number[];
  tops: number[];
}

/**
 * A point on a page, which a change of scale can find again, and whether it
 * belongs at the viewport's middle or its top. A page navigated to keeps its
 * top edge in place; a free scroll keeps what sits under the middle, which is
 * also what the indicator reads, so a relayout never changes the page shown.
 */
interface Anchor {
  page: number;
  frac: number;
  px: number;
  middle: boolean;
}

function computeLayout(sizes: PageSize[], scale: number): Layout {
  const widths: number[] = [];
  const heights: number[] = [];
  const tops: number[] = [];
  let y = GAP;
  for (const size of sizes) {
    // Floored like react-pdf's canvas, so the slot is the size the page draws
    // at, but never to 0: the anchor divides by it, and a page far wider than
    // the rest can fit the others down to nothing.
    const height = Math.max(1, Math.floor(size.height * scale));
    widths.push(Math.max(1, Math.floor(size.width * scale)));
    heights.push(height);
    tops.push(y);
    y += height + GAP;
  }
  return { scale, widths, heights, tops };
}

/** The last page whose top is at or above `y`. */
function pageAt(layout: Layout, y: number): number {
  const { tops } = layout;
  let lo = 0;
  let hi = tops.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (tops[mid] <= y) lo = mid;
    else hi = mid - 1;
  }
  return lo;
}

/** The gap above a page belongs to it, so a point in the gap keeps its pixels. */
function anchorAt(layout: Layout, y: number): Anchor {
  const page = pageAt(layout, y + GAP);
  const into = y - layout.tops[page];
  return into < 0
    ? { page, frac: 0, px: into, middle: true }
    : { page, frac: into / layout.heights[page], px: 0, middle: true };
}

function scrollTopFor(layout: Layout, anchor: Anchor, viewHeight: number): number {
  const page = Math.min(anchor.page, layout.tops.length - 1);
  const y = layout.tops[page] + anchor.frac * layout.heights[page] + anchor.px;
  return anchor.middle ? y - viewHeight / 2 : y;
}

const pageTop = (page: number): Anchor => ({ page, frac: 0, px: -GAP, middle: false });

/** The display's pixel ratio lowered to fit the canvas caps, or undefined for react-pdf's own. */
function canvasPixelRatio(width: number, height: number): number | undefined {
  const cap = Math.min(Math.sqrt(MAX_CANVAS_PIXELS / (width * height)), MAX_CANVAS_SIDE / Math.max(width, height));
  return cap < window.devicePixelRatio ? cap : undefined;
}

function nextZoom(scale: number, direction: 1 | -1): number | null {
  const step = direction > 0
    ? ZOOM_STEPS.find((s) => s > scale * STEP_SLACK)
    : ZOOM_STEPS.findLast((s) => s < scale / STEP_SLACK);
  return step ?? null;
}

interface PdfViewerProps {
  data: ArrayBuffer | Uint8Array | RangedFile;
  /**
   * A ranged file lost a slice, to a change in the file or a failed read, so
   * it has to be read again, into a new `data`. Left out, that is a failure.
   */
  onFileChanged?: () => void;
  /** Page a reference pointed at; `focusSeq` changes on each visit so a repeat click returns to it. */
  focusPage?: number | null;
  focusSeq?: number | null;
  onPageCount?: (count: number) => void;
}

export default function PdfViewer({ data, onFileChanged, focusPage = null, focusSeq = null, onPageCount }: PdfViewerProps) {
  const { t } = useTranslation();
  const [pageSizes, setPageSizes] = useState<PageSize[] | null>(null);
  const [zoom, setZoom] = useState<Zoom>('fit');
  const [fitWidth, setFitWidth] = useState(0);
  const [drawScale, setDrawScale] = useState<number | null>(null);
  const [mounted, setMounted] = useState<ReadonlySet<number>>(() => new Set());
  const [currentPage, setCurrentPage] = useState(1);
  const [error, setError] = useState<Error | null>(null);
  // The document on show and the bytes it came from. While those are not the
  // current bytes, a newer copy of the file is loading over it.
  const [loaded, setLoaded] = useState<{ pdf: PDFDocumentProxy; from: unknown } | null>(null);
  const [hasOutline, setHasOutline] = useState(false);
  const [railOpen, setRailOpen] = useState(false);
  const [railTab, setRailTab] = useState<RailTab>('pages');
  // What the reader is typing into the page field; null while they are not.
  const [pageDraft, setPageDraft] = useState<string | null>(null);

  // Always rendered, so every effect and handler below finds it set.
  const scrollerRef = useRef<HTMLDivElement>(null);
  // Where the reader is, taken at each scroll so a relayout can put them back.
  const anchor = useRef<Anchor>(pageTop(0));
  const anchorX = useRef(0.5);
  // A page Prev/Next, a reference or a link went to. The indicator names it for
  // as long as the scroll stays where that left it, because a last page too
  // short to reach the top leaves another page under the middle.
  const pinned = useRef<{ page: number; y: number } | null>(null);
  const loadingDoc = useRef<PDFDocumentProxy | null>(null);
  // The slice whose failure asked for a re-read, and the file it failed on,
  // whose other slices failing must not ask again. It stands until a newer
  // file reads those bytes. Any other read cannot clear it: after a reload
  // pdf.js reads the trailer first, which succeeds while a slice that fails
  // every time would reload over and over. A failure before then is the error.
  const reread = useRef<{ file: RangedFile; begin: number; end: number } | null>(null);
  // pdf.js aborts a closed document's reads only once its worker confirms the
  // close, so a slice can still fail after the viewer is gone, and must not
  // reload whichever file the panel shows by then.
  const live = useRef(false);
  const handledSeq = useRef<number | null>(null);
  // A relayout made while the panel was hidden, to apply once it shows.
  const restorePending = useRef(false);
  // Whether the page field holds something typed, which leaving it goes to.
  const pageTyped = useRef(false);
  // WebKit's mouseup after the click that focuses the field clears the
  // selection made on focus, and what is typed would append to the number.
  const keepPageSelection = useRef(false);

  const numPages = pageSizes?.length ?? null;
  const widest = pageSizes ? pageSizes.reduce((w, s) => Math.max(w, s.width), 0) : 0;
  const fitScale = widest > 0 && fitWidth > 0 ? fitWidth / widest : null;
  const scale = zoom === 'fit' ? fitScale : zoom;
  const layout = useMemo(() => (pageSizes && scale ? computeLayout(pageSizes, scale) : null), [pageSizes, scale]);
  const pageScale = drawScale ?? scale ?? 1;
  const ready = layout !== null;

  const onDocumentLoadSuccess = useStableHandler((pdf: PDFDocumentProxy) => {
    const n = pdf.numPages;
    loadingDoc.current = pdf;
    // The viewer is keyed by path, so a document it showed before was this
    // file: a refetched copy keeps the reader where they were. A first open,
    // or one a new reference is waiting on, goes to the referenced page. A
    // reference to a page this copy lacks stays waiting rather than handled,
    // so a newer copy that has the page still goes there.
    const fits = !focusPage || focusPage <= n;
    if (loaded && (handledSeq.current === focusSeq || !fits)) {
      const last = n - 1;
      if (anchor.current.page > last) anchor.current = pageTop(last);
      if (pinned.current) pinned.current = { page: Math.min(pinned.current.page, last), y: Number.NaN };
      setCurrentPage((page) => Math.min(page, n));
    } else {
      const target = focusPage && fits ? focusPage - 1 : 0;
      anchor.current = pageTop(target);
      anchorX.current = 0.5;
      pinned.current = { page: target, y: Number.NaN };
      setCurrentPage(target + 1);
    }
    if (fits) handledSeq.current = focusSeq;
    setPageSizes(null);
    setDrawScale(null);
    setLoaded({ pdf, from: data });
    onPageCount?.(n);
    pdf.getOutline().then(
      (outline) => {
        if (loadingDoc.current === pdf) setHasOutline(!!outline?.length);
      },
      () => {
        if (loadingDoc.current === pdf) setHasOutline(false);
      },
    );
    // Every page starts at page 1's size, which most documents keep throughout,
    // so the column is its full height and the first page draws without
    // waiting on the rest. A page that differs is corrected when it is
    // measured, and the anchor keeps the reader where they were.
    pdf.getPage(1).then(
      (first) => {
        if (loadingDoc.current !== pdf) return;
        setPageSizes(Array.from({ length: n }, () => sizeOf(first)));
        // With every byte in hand, measuring the rest is cheap and settles the
        // column early. A document loaded by ranges skips it, since it would
        // fetch every page to learn its size; pdf.js's viewer does the same.
        if (n < 2 || n > MEASURE_ALL_UP_TO || pdf.loadingParams.disableAutoFetch) return;
        const rest = Array.from({ length: n - 1 }, (_, i) =>
          pdf.getPage(i + 2).then(
            (page): [number, PageSize] => [i + 1, sizeOf(page)],
            () => null,
          ),
        );
        Promise.all(rest).then((entries) => {
          if (loadingDoc.current !== pdf) return;
          const measured = new Map(entries.filter((entry) => entry !== null));
          setPageSizes((prev) => prev && withMeasured(prev, measured));
        });
      },
      (err: unknown) => {
        if (loadingDoc.current === pdf) setError(err instanceof Error ? err : new Error(String(err)));
      },
    );
  });

  const measurePage = useStableHandler((index: number, size: PageSize) => {
    setPageSizes((prev) => prev && withMeasured(prev, new Map([[index, size]])));
  });

  const syncIndicator = useStableHandler(() => {
    const scroller = scrollerRef.current;
    if (!scroller || !layout) return;
    if (pinned.current) {
      setCurrentPage(pinned.current.page + 1);
      return;
    }
    const y = scroller.scrollTop;
    const atEnd = y > 0 && y >= scroller.scrollHeight - scroller.clientHeight - 1;
    setCurrentPage(atEnd ? layout.tops.length : pageAt(layout, y + scroller.clientHeight / 2) + 1);
  });

  const onScroll = useStableHandler(() => {
    const scroller = scrollerRef.current;
    // A hidden panel scrolls to 0 as it collapses, and so does a column whose
    // file is reloading; neither is the reader moving.
    if (!scroller || !layout || scroller.clientHeight === 0 || loaded?.from !== data) return;
    if (scroller.scrollWidth > 0) {
      anchorX.current = (scroller.scrollLeft + scroller.clientWidth / 2) / scroller.scrollWidth;
    }
    const y = scroller.scrollTop;
    if (pinned.current && Math.abs(y - pinned.current.y) < 1) return;
    pinned.current = null;
    // At the very top the top edge is held instead, so zooming in from there
    // still shows page 1's top.
    anchor.current = y <= 0 ? pageTop(0) : anchorAt(layout, y + scroller.clientHeight / 2);
    syncIndicator();
  });

  const scrollToPage = useStableHandler((index: number) => {
    const scroller = scrollerRef.current;
    if (!scroller || !layout) return;
    const page = Math.max(0, Math.min(index, layout.tops.length - 1));
    anchor.current = pageTop(page);
    scroller.scrollTop = scrollTopFor(layout, anchor.current, scroller.clientHeight);
    pinned.current = { page, y: scroller.scrollTop };
    setCurrentPage(page + 1);
  });

  // Puts the reader back after any relayout: first layout, zoom, panel resize,
  // reload. A hidden panel takes no scroll position, so its relayout waits for
  // the panel to show again, which the ResizeObserver below sees.
  const restoreScroll = useStableHandler(() => {
    const scroller = scrollerRef.current;
    if (!scroller || !layout) return;
    if (scroller.clientHeight === 0) {
      restorePending.current = true;
      return;
    }
    restorePending.current = false;
    scroller.scrollTop = scrollTopFor(layout, anchor.current, scroller.clientHeight);
    scroller.scrollLeft = anchorX.current * scroller.scrollWidth - scroller.clientWidth / 2;
    if (pinned.current) pinned.current = { page: pinned.current.page, y: scroller.scrollTop };
    syncIndicator();
  });

  // Fit width follows the scroller. Its scrollbar lane is reserved in CSS, so a
  // scrollbar appearing cannot narrow the box and flip the width back.
  useEffect(() => {
    const scroller = scrollerRef.current;
    if (!scroller) return;
    const ro = new ResizeObserver(() => {
      const width = scroller.clientWidth;
      // A hidden panel measures 0; keeping the last width keeps the reader's place.
      if (width > 0) setFitWidth(Math.max(1, width - 2 * GAP));
      if (restorePending.current) restoreScroll();
    });
    ro.observe(scroller);
    return () => ro.disconnect();
  }, [restoreScroll]);

  // Canvases redraw at the new scale once it settles; until then the surface
  // transform below stretches the last drawing to the slot.
  useEffect(() => {
    if (scale === null || scale === drawScale) return;
    const id = setTimeout(() => setDrawScale(scale), drawScale === null ? 0 : REDRAW_DELAY_MS);
    return () => clearTimeout(id);
  }, [scale, drawScale]);

  // The render window. Slots keep their elements across a change of scale, so
  // the observer only starts over when the column itself is rebuilt, which
  // every document load does.
  const shownDoc = loaded?.pdf ?? null;
  useEffect(() => {
    const scroller = scrollerRef.current;
    if (!scroller || !ready || !shownDoc) return;
    const visible = new Set<number>();
    const io = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          const index = Number((entry.target as HTMLElement).dataset.pdfSlot);
          if (entry.isIntersecting) visible.add(index);
          else visible.delete(index);
        }
        setMounted(new Set(visible));
      },
      { root: scroller, rootMargin: RENDER_MARGIN },
    );
    for (const slot of scroller.querySelectorAll<HTMLElement>('[data-pdf-slot]')) io.observe(slot);
    return () => io.disconnect();
  }, [ready, numPages, shownDoc]);

  useReleasePages(shownDoc, mounted);

  // A layout effect, so the frame that shows the new sizes is already scrolled.
  useLayoutEffect(() => {
    if (layout) restoreScroll();
  }, [layout, restoreScroll]);

  // A repeat visit to a reference returns to its page. The first visit is
  // handled at load, so the column opens there rather than passing page 1.
  useEffect(() => {
    if (!layout || handledSeq.current === focusSeq) return;
    // A page the copy on show lacks waits for a copy that has it, as at load.
    if (focusPage && focusPage > layout.tops.length) return;
    handledSeq.current = focusSeq;
    if (focusPage) scrollToPage(focusPage - 1);
  }, [layout, focusSeq, focusPage, scrollToPage]);

  const zoomOutTo = scale === null ? null : nextZoom(scale, -1);
  const zoomInTo = scale === null ? null : nextZoom(scale, 1);

  // pdf.js never asks again for a slice that failed, so its document would
  // wait on it for good. Whatever the failure, the file reloads once, from its
  // first slice; if that request fails too, the panel shows its error, with
  // Retry. A failure on the reloaded file before it has read the bytes that
  // were lost shows the error here instead. A slice of a file since replaced,
  // being read again, or left by a closed viewer fails with nobody waiting on it.
  const onRangeFail = useStableHandler((source: RangedFile, failure: unknown, begin: number, end: number) => {
    if (!live.current || source !== data || source === reread.current?.file) return;
    if (onFileChanged && reread.current === null) {
      // So the re-read replaces it even when it finds the same version.
      markStale(source);
      reread.current = { file: source, begin, end };
      onFileChanged();
      return;
    }
    setError(failure instanceof Error ? failure : new Error(String(failure)));
  });
  const onRangeRead = useStableHandler((source: RangedFile, begin: number, end: number) => {
    const lost = reread.current;
    if (source === data && lost && source !== lost.file && begin < lost.end && end > lost.begin) {
      reread.current = null;
    }
  });

  // A new `file` object is a new document to react-pdf, so a render that rebuilt
  // it would reload the PDF and reset the page.
  const fileData = useMemo(() => {
    if (!data) return null;
    if (isRangedFile(data)) {
      return {
        range: rangeTransport(data, {
          onRead: (begin, end) => onRangeRead(data, begin, end),
          onFail: (failure, begin, end) => onRangeFail(data, failure, begin, end),
        }),
      };
    }
    const bytes = data instanceof ArrayBuffer ? new Uint8Array(data) : data;
    return { data: bytes };
  }, [data, onRangeFail, onRangeRead]);

  // A document stops being current as soon as its bytes are replaced, not once
  // the next one loads: react-pdf destroys it first, and the requests it still
  // has out reject on the way, which must not read as the new file failing.
  useEffect(() => {
    loadingDoc.current = null;
  }, [fileData]);

  useEffect(() => {
    live.current = true;
    return () => {
      live.current = false;
    };
  }, []);

  // Bubble errors to the error boundary via render-phase throw
  if (error) throw error;

  const stretch = layout ? layout.scale / pageScale : 1;
  // Names what the rail holds, so it never reads as the app's own sidebar.
  const railLabel = t(hasOutline ? 'pdfViewer.sidebar' : 'pdfViewer.pages');
  // The rail draws only from the current copy, never one being torn down.
  const railDoc = loaded && loaded.from === data && pageSizes ? loaded.pdf : null;

  // Leaving the field goes to the page typed, as Enter does: the number pad
  // iOS shows for it has no Return key.
  const commitPageDraft = () => {
    const page = Number.parseInt(pageDraft ?? '', 10);
    if (pageTyped.current && numPages && page >= 1) scrollToPage(Math.min(page, numPages) - 1);
    pageTyped.current = false;
    keepPageSelection.current = false;
    setPageDraft(null);
  };

  return (
    <div className="pdf-viewer">
      {/* Controls */}
      <div className="pdf-controls">
        <div className="pdf-nav">
          <button
            type="button"
            onClick={() => setRailOpen((open) => !open)}
            disabled={!railDoc}
            aria-pressed={railOpen}
            className="pdf-btn pdf-btn-icon"
            aria-label={railLabel}
            title={railLabel}
          >
            <PanelLeft size={14} aria-hidden />
          </button>
          <button
            type="button"
            onClick={() => scrollToPage(currentPage - 2)}
            disabled={!numPages || currentPage <= 1}
            className="pdf-btn"
          >
            {t('pdfViewer.prev')}
          </button>
          <span className="pdf-page-info">
            <input
              className="pdf-page-input"
              type="text"
              inputMode="numeric"
              aria-label={t('pdfViewer.pageNumber')}
              disabled={!numPages}
              value={pageDraft ?? String(currentPage)}
              style={{ width: `calc(${String(numPages ?? currentPage).length}ch + 14px)` }}
              onFocus={(e) => {
                setPageDraft(String(currentPage));
                keepPageSelection.current = true;
                e.currentTarget.select();
              }}
              onMouseUp={(e) => {
                if (!keepPageSelection.current) return;
                keepPageSelection.current = false;
                e.preventDefault();
              }}
              onChange={(e) => {
                pageTyped.current = true;
                setPageDraft(e.target.value.replace(/\D/g, ''));
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.currentTarget.blur();
                } else if (e.key === 'Escape') {
                  // The field's own: it puts the page back, and the file panel's
                  // Escape (leave select mode, refocus the tree) stays out of it.
                  e.stopPropagation();
                  pageTyped.current = false;
                  e.currentTarget.blur();
                }
              }}
              onBlur={commitPageDraft}
            />
            <span aria-hidden>/</span>
            <span>{numPages ?? '…'}</span>
          </span>
          <button
            type="button"
            onClick={() => scrollToPage(currentPage)}
            disabled={!numPages || currentPage >= numPages}
            className="pdf-btn"
          >
            {t('pdfViewer.next')}
          </button>
        </div>
        <div className="pdf-zoom">
          <button
            type="button"
            onClick={() => zoomOutTo && setZoom(zoomOutTo)}
            disabled={zoomOutTo === null}
            className="pdf-btn"
            aria-label={t('pdfViewer.zoomOut')}
            title={t('pdfViewer.zoomOut')}
          >
            −
          </button>
          <span className="pdf-zoom-info">{scale === null ? '…' : `${Math.round(scale * 100)}%`}</span>
          <button
            type="button"
            onClick={() => zoomInTo && setZoom(zoomInTo)}
            disabled={zoomInTo === null}
            className="pdf-btn"
            aria-label={t('pdfViewer.zoomIn')}
            title={t('pdfViewer.zoomIn')}
          >
            +
          </button>
          <button
            type="button"
            onClick={() => setZoom('fit')}
            aria-pressed={zoom === 'fit'}
            className="pdf-btn"
            title={t('pdfViewer.fitWidth')}
          >
            {t('pdfViewer.fit')}
          </button>
        </div>
      </div>

      <div className="pdf-body">
        {railOpen && railDoc && pageSizes && (
          <PdfRail
            pdf={railDoc}
            pageSizes={pageSizes}
            currentPage={currentPage}
            hasOutline={hasOutline}
            tab={railTab}
            onTabChange={setRailTab}
            onGoToPage={scrollToPage}
            onMeasure={measurePage}
          />
        )}

        {/* Document */}
        <div
          ref={scrollerRef}
          className="pdf-scroller"
          onScroll={onScroll}
          style={{ '--pdf-gap': `${GAP}px` } as React.CSSProperties}
        >
          {/* react-pdf defaults to Suspense, which would discard `fileData` with this
              never-committed component on every retry and reload forever. Effect
              mode keeps the loading props and the error throw above; Page inherits it. */}
          <Document
            suspense={false}
            className="pdf-pages"
            file={fileData}
            options={isRangedFile(data) ? RANGED_OPTIONS : DOCUMENT_OPTIONS}
            onLoadSuccess={onDocumentLoadSuccess}
            onLoadError={(err: Error) => setError(err)}
            onItemClick={({ pageNumber }) => scrollToPage(pageNumber - 1)}
            externalLinkTarget="_blank"
            externalLinkRel="noopener noreferrer"
            loading={<div className="pdf-loading">{t('pdfViewer.loading')}</div>}
          >
            {layout && pageSizes ? (
              layout.heights.map((height, i) => (
                <div
                  key={i}
                  className="pdf-page"
                  data-pdf-slot={i}
                  style={{ width: layout.widths[i], height }}
                >
                  {mounted.has(i) && (
                    <div
                      className="pdf-page-surface"
                      style={stretch === 1 ? undefined : { transform: `scale(${stretch})` }}
                    >
                      <Page
                        pageNumber={i + 1}
                        scale={pageScale}
                        devicePixelRatio={canvasPixelRatio(
                          pageSizes[i].width * pageScale,
                          pageSizes[i].height * pageScale,
                        )}
                        onLoadSuccess={(page) => measurePage(i, sizeOf(page))}
                        loading={
                          <div
                            className="pdf-page-loading"
                            style={{
                              width: Math.floor(pageSizes[i].width * pageScale),
                              height: Math.floor(pageSizes[i].height * pageScale),
                            }}
                          >
                            {t('pdfViewer.rendering')}
                          </div>
                        }
                      />
                    </div>
                  )}
                </div>
              ))
            ) : (
              <div className="pdf-loading">{t('pdfViewer.loading')}</div>
            )}
          </Document>
        </div>
      </div>
    </div>
  );
}
