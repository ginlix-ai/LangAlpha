import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import type { PDFDocumentProxy } from 'pdfjs-dist';
import DocumentErrorBoundary from '../../DocumentErrorBoundary';
import { useReleasePages } from '../useReleasePages';

function Pages({ pdf, mounted }: { pdf: PDFDocumentProxy; mounted: ReadonlySet<number> }) {
  useReleasePages(pdf, mounted);
  return null;
}

describe('useReleasePages', () => {
  it('leaves alone a document torn down by a reload of its file', () => {
    // pdf.js throws, rather than rejects, when a destroyed document is asked for a page.
    const replaced = {
      loadingTask: { destroyed: false },
      getPage: () => {
        throw new TypeError("Cannot read properties of null (reading 'sendWithPromise')");
      },
    };
    const next = { loadingTask: { destroyed: false }, getPage: () => new Promise(() => {}) };
    const view = (pdf: object) => (
      <DocumentErrorBoundary fallback={<p>failed</p>}>
        <Pages pdf={pdf as PDFDocumentProxy} mounted={new Set([0, 1])} />
      </DocumentErrorBoundary>
    );
    const { rerender } = render(view(replaced));

    // The new copy shows only once the old one is destroyed, and showing it
    // releases what the old one had drawn.
    replaced.loadingTask.destroyed = true;
    rerender(view(next));

    expect(screen.queryByText('failed')).toBeNull();
  });
});
