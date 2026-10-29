import { useEffect } from 'react';
import type { PDFDocumentProxy } from 'pdfjs-dist';

function release(pdf: PDFDocumentProxy, index: number): void {
  // A document already torn down (its file was reloaded) has nothing left to
  // release, and pdf.js throws rather than rejects when asked for its pages.
  if (pdf.loadingTask.destroyed) return;
  pdf.getPage(index + 1).then(
    (page) => page.cleanup(),
    () => {},
  );
}

/**
 * Lets pdf.js drop what it decoded to draw each page in `mounted` (its images
 * and drawing commands) once the set moves on. react-pdf cleans a page up
 * only before drawing it again, so otherwise every page ever drawn stays
 * decoded until the document closes, which a long scanned PDF does not
 * survive; pdf.js's own viewer releases the pages it stops showing the same
 * way. A page still on screen loses nothing it shows, and one still drawing
 * is cleaned once the draw finishes.
 */
export function useReleasePages(pdf: PDFDocumentProxy | null, mounted: ReadonlySet<number>): void {
  useEffect(() => {
    if (!pdf) return;
    return () => {
      for (const index of mounted) release(pdf, index);
    };
  }, [pdf, mounted]);
}
