import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { PDFDocumentProxy } from 'pdfjs-dist';
import { Outline, Page } from 'react-pdf';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { type PageSize, sizeOf } from './pageSize';
import { useReleasePages } from './useReleasePages';

export type RailTab = 'pages' | 'contents';

// The rail's width in PdfViewer.css is this plus its padding.
const THUMB_WIDTH = 104;
// Thumbnails mount within one rail height above and below what it shows, as the
// pages do in the document, so a long PDF never draws them all.
const THUMB_MARGIN = '100% 0px';

interface PdfRailProps {
  pdf: PDFDocumentProxy;
  pageSizes: PageSize[];
  /** 1-based, as the toolbar shows it. */
  currentPage: number;
  hasOutline: boolean;
  tab: RailTab;
  onTabChange: (tab: RailTab) => void;
  onGoToPage: (index: number) => void;
  onMeasure: (index: number, size: PageSize) => void;
}

/**
 * Page thumbnails, plus the document's own bookmarks when it has any. The
 * pages and the outline come from the document the viewer already holds, so
 * opening the rail loads nothing the reader has not asked to see.
 */
export default function PdfRail({
  pdf,
  pageSizes,
  currentPage,
  hasOutline,
  tab,
  onTabChange,
  onGoToPage,
  onMeasure,
}: PdfRailProps) {
  const { t } = useTranslation();
  const showContents = hasOutline && tab === 'contents';

  return (
    <div className="pdf-rail">
      {hasOutline && (
        <div className="pdf-rail-tabs">
          <SegmentedControl
            size="compact"
            label={t('pdfViewer.sidebar')}
            value={showContents ? 'contents' : 'pages'}
            onChange={onTabChange}
            options={[
              { value: 'pages', label: t('pdfViewer.pages') },
              { value: 'contents', label: t('pdfViewer.contents') },
            ]}
          />
        </div>
      )}
      {showContents ? (
        <div className="pdf-rail-scroller">
          <Outline
            pdf={pdf}
            suspense={false}
            className="pdf-outline"
            onItemClick={({ pageNumber }) => onGoToPage(pageNumber - 1)}
          />
        </div>
      ) : (
        <Thumbnails
          pdf={pdf}
          pageSizes={pageSizes}
          currentPage={currentPage}
          onGoToPage={onGoToPage}
          onMeasure={onMeasure}
        />
      )}
    </div>
  );
}

function Thumbnails({
  pdf,
  pageSizes,
  currentPage,
  onGoToPage,
  onMeasure,
}: Pick<PdfRailProps, 'pdf' | 'pageSizes' | 'currentPage' | 'onGoToPage' | 'onMeasure'>) {
  const { t } = useTranslation();
  const scrollerRef = useRef<HTMLDivElement>(null);
  const [mounted, setMounted] = useState<ReadonlySet<number>>(() => new Set());

  useEffect(() => {
    const scroller = scrollerRef.current;
    if (!scroller) return;
    const visible = new Set<number>();
    const io = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          const index = Number((entry.target as HTMLElement).dataset.pdfThumb);
          if (entry.isIntersecting) visible.add(index);
          else visible.delete(index);
        }
        setMounted(new Set(visible));
      },
      { root: scroller, rootMargin: THUMB_MARGIN },
    );
    for (const thumb of scroller.querySelectorAll<HTMLElement>('[data-pdf-thumb]')) io.observe(thumb);
    return () => io.disconnect();
  }, [pageSizes.length]);

  useReleasePages(pdf, mounted);

  // Follows the page being read, and opening the rail lands on it. Only a page
  // that has left the rail's view moves it, so reading never fights a reader
  // who scrolled the rail to look further on.
  useLayoutEffect(() => {
    const scroller = scrollerRef.current;
    const thumb = scroller?.querySelector<HTMLElement>(`[data-pdf-thumb="${currentPage - 1}"]`);
    if (!scroller || !thumb) return;
    const top = thumb.offsetTop;
    const bottom = top + thumb.offsetHeight;
    if (top < scroller.scrollTop || bottom > scroller.scrollTop + scroller.clientHeight) {
      scroller.scrollTop = top - (scroller.clientHeight - thumb.offsetHeight) / 2;
    }
  }, [currentPage]);

  return (
    <div ref={scrollerRef} className="pdf-rail-scroller">
      <div className="pdf-thumbs">
        {pageSizes.map((size, i) => (
          // A link, as react-pdf's own Thumbnail is: it moves within the
          // document, and aria-current="page" is the state a link carries.
          <a
            key={i}
            href="#"
            className="pdf-thumb"
            data-pdf-thumb={i}
            aria-label={t('pdfViewer.goToPage', { page: i + 1 })}
            aria-current={i === currentPage - 1 ? 'page' : undefined}
            onClick={(e) => {
              e.preventDefault();
              onGoToPage(i);
            }}
          >
            <span
              className="pdf-thumb-paper"
              style={{ width: THUMB_WIDTH, height: Math.round((THUMB_WIDTH * size.height) / size.width) }}
            >
              {mounted.has(i) && (
                <Page
                  pdf={pdf}
                  pageNumber={i + 1}
                  width={THUMB_WIDTH}
                  suspense={false}
                  renderAnnotationLayer={false}
                  renderTextLayer={false}
                  loading={null}
                  error={null}
                  onLoadSuccess={(page) => onMeasure(i, sizeOf(page))}
                />
              )}
            </span>
            <span className="pdf-thumb-number">{i + 1}</span>
          </a>
        ))}
      </div>
    </div>
  );
}
