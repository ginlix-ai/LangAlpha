import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import type { PDFDocumentProxy } from 'pdfjs-dist';
import { ViewerComments, type ViewerCommentsTarget } from '../ViewerComments';
import { pdfSource } from '../region/regionPictures';
import { pdfRegions } from './pdfSpots';

/** The comment layer over a PDF's pages: words selected on a page, or a box drawn on it, take a comment. */
export function PdfComments({ scroller, content, pdf, target }: {
  scroller: HTMLElement | null;
  /** The column of pages. */
  content: HTMLElement | null;
  /** The document shown, which a box's picture is drawn from. */
  pdf: PDFDocumentProxy | null;
  target: ViewerCommentsTarget;
}) {
  const { t } = useTranslation();
  const strategy = useMemo(() => (content ? pdfRegions(content) : null), [content]);
  const pictures = useMemo(() => (pdf ? pdfSource(pdf) : null), [pdf]);
  return (
    <ViewerComments
      scroller={scroller}
      content={content}
      strategy={strategy}
      target={target}
      modeHint={t('pageViewer.comments.modeHintPdf')}
      pictures={pictures}
      // The words or the box a comment is on stay in view.
      keepSpotClear
    />
  );
}
