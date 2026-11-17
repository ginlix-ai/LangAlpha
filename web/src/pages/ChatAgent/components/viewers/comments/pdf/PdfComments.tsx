import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { ViewerComments, type ViewerCommentsTarget } from '../ViewerComments';
import { pdfSpots } from './pdfSpots';


/** The comment layer over a PDF's pages: words selected on a page take a comment. */
export function PdfComments({ scroller, content, target }: {
  scroller: HTMLElement | null;
  /** The column of pages. */
  content: HTMLElement | null;
  target: ViewerCommentsTarget;
}) {
  const { t } = useTranslation();
  const strategy = useMemo(() => (content ? pdfSpots(content) : null), [content]);
  return (
    <ViewerComments
      scroller={scroller}
      content={content}
      strategy={strategy}
      target={target}
      modeHint={t('pageViewer.comments.modeHintPdf')}
    />
  );
}
