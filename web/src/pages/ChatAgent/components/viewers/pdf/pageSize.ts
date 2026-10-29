import type { PDFPageProxy } from 'pdfjs-dist';

/** A page's size at scale 1, in PDF points, with its rotation applied. */
export interface PageSize {
  width: number;
  height: number;
}

export function sizeOf(page: PDFPageProxy): PageSize {
  const { width, height } = page.getViewport({ scale: 1 });
  return { width, height };
}

/**
 * `sizes` with each measured page written in, or `sizes` itself when nothing
 * changed, so a measurement that matches the guess costs no relayout.
 */
export function withMeasured(sizes: PageSize[], measured: ReadonlyMap<number, PageSize>): PageSize[] {
  let next: PageSize[] | null = null;
  for (const [index, size] of measured) {
    const prev = sizes[index];
    if (!prev || (prev.width === size.width && prev.height === size.height)) continue;
    next ??= sizes.slice();
    next[index] = size;
  }
  return next ?? sizes;
}
