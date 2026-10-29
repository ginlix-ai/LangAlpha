import { beforeAll, describe, expect, it, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import i18n from '@/i18n';
import { isStale, type RangedFile } from '@/lib/rangedFile';
import DocumentErrorBoundary from '../DocumentErrorBoundary';
import PdfViewer from '../PdfViewer';

interface RangeTransportLike {
  requestDataRange: (begin: number, end: number) => void;
}

// The Document react-pdf would render, held open so a test decides when each
// file finishes loading, and what it was handed to read.
const doc = vi.hoisted(() => ({
  onLoadSuccess: null as ((pdf: unknown) => void) | null,
  file: null as { range?: RangeTransportLike } | null,
}));
vi.mock('react-pdf', () => ({
  pdfjs: {
    GlobalWorkerOptions: {},
    version: 'test',
    PDFDataRangeTransport: class {
      constructor(public length: number, public initialData: Uint8Array) {}
      onDataRange() {}
    },
  },
  Document: ({ file, onLoadSuccess, children }: {
    file: { range?: RangeTransportLike };
    onLoadSuccess: (pdf: unknown) => void;
    children: ReactNode;
  }) => {
    doc.file = file;
    doc.onLoadSuccess = onLoadSuccess;
    return <div>{children}</div>;
  },
  Page: () => null,
  Outline: () => null,
}));

function deferred<T>() {
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((_resolve, rej) => {
    reject = rej;
  });
  return { promise, reject };
}

describe('PdfViewer', () => {
  beforeAll(async () => {
    await i18n.changeLanguage('en-US');
  });

  it('does not fail a new file when the replaced one rejects its first page', async () => {
    const firstPage = deferred<never>();
    const replaced = {
      numPages: 3,
      loadingParams: { disableAutoFetch: false },
      loadingTask: { destroyed: false },
      getOutline: () => Promise.resolve(null),
      getPage: () => firstPage.promise,
    };
    const view = (data: Uint8Array) => (
      <DocumentErrorBoundary fallback={<p>failed</p>}>
        <PdfViewer data={data} />
      </DocumentErrorBoundary>
    );
    const { rerender } = render(view(new Uint8Array([1])));
    act(() => doc.onLoadSuccess!(replaced));

    // A refetch hands over new bytes; react-pdf tears the old document down,
    // and the page request it still had out rejects on the way.
    rerender(view(new Uint8Array([2])));
    replaced.loadingTask.destroyed = true;
    await act(async () => {
      firstPage.reject(new Error('Transport destroyed'));
      await Promise.resolve();
    });

    expect(screen.queryByText('failed')).toBeNull();
  });

  it('goes to a referenced page that only the refetched copy has', () => {
    const copy = (numPages: number) => ({
      numPages,
      loadingParams: { disableAutoFetch: false },
      loadingTask: { destroyed: false },
      getOutline: () => Promise.resolve(null),
      getPage: () => new Promise(() => {}),
    });
    const view = (data: Uint8Array) => <PdfViewer data={data} focusPage={6} focusSeq={1} />;
    const { rerender } = render(view(new Uint8Array([1])));

    // The cached copy opens first and has no page 6; the refetched one does.
    act(() => doc.onLoadSuccess!(copy(5)));
    rerender(view(new Uint8Array([2])));
    act(() => doc.onLoadSuccess!(copy(8)));

    expect(screen.getByLabelText('Page number')).toHaveValue('6');
  });

  describe('a slice that fails', () => {
    const SLICE = 65_536;
    /** Where pdf.js reads first in a file it opens that is not linearized. */
    const TRAILER = 10_000_000 - SLICE;

    /** A ranged file whose reads stay out until the test answers or fails them. */
    function rangedFile() {
      interface Read { begin: number; answer: () => void; reject: (reason: unknown) => void }
      const pending: Read[] = [];
      const file: RangedFile = {
        length: 10_000_000,
        etag: '"v1"',
        initial: new Uint8Array(8),
        read: (begin, end) => new Promise((resolve, reject) => {
          pending.push({ begin, answer: () => resolve(new Uint8Array(end - begin)), reject });
        }),
      };
      /** Settles the reads out for `begin`, or all of them. */
      const settle = async (how: (read: Read) => void, begin?: number) => {
        await act(async () => {
          for (const read of pending.filter((r) => begin === undefined || r.begin === begin)) {
            pending.splice(pending.indexOf(read), 1);
            how(read);
          }
          await new Promise((resolve) => setTimeout(resolve, 0));
        });
      };
      return {
        file,
        fail: (reason: unknown, begin?: number) => settle((read) => read.reject(reason), begin),
        answer: (begin?: number) => settle((read) => read.answer(), begin),
      };
    }
    const requestSlices = (count: number) => {
      for (let i = 0; i < count; i++) requestSlice(i * SLICE);
    };
    const requestSlice = (begin: number) => doc.file!.range!.requestDataRange(begin, begin + SLICE);
    const view = (data: RangedFile, onFileChanged: () => void) => (
      <DocumentErrorBoundary fallback={<p>failed</p>}>
        <PdfViewer data={data} onFileChanged={onFileChanged} />
      </DocumentErrorBoundary>
    );

    it('reloads the file once, whatever the failure, and marks it so the re-read replaces it', async () => {
      const onFileChanged = vi.fn();
      const { file, fail } = rangedFile();
      render(view(file, onFileChanged));
      requestSlices(3);
      await fail(new Error('Network Error'));

      expect(onFileChanged).toHaveBeenCalledTimes(1);
      expect(isStale(file)).toBe(true);
      expect(screen.queryByText('failed')).toBeNull();
    });

    it('shows the error when the reloaded file fails before it reads anything', async () => {
      const onFileChanged = vi.fn();
      const first = rangedFile();
      const { rerender } = render(view(first.file, onFileChanged));
      requestSlices(1);
      await first.fail(new Error('Network Error'));

      const second = rangedFile();
      rerender(view(second.file, onFileChanged));
      requestSlices(1);
      await second.fail(new Error('Network Error'));

      expect(onFileChanged).toHaveBeenCalledTimes(1);
      expect(screen.getByText('failed')).toBeInTheDocument();
    });

    it('shows the error when the slice that was lost fails again, whatever else the reloaded file read', async () => {
      const onFileChanged = vi.fn();
      const first = rangedFile();
      const { rerender } = render(view(first.file, onFileChanged));
      requestSlice(2 * SLICE);
      await first.fail(new Error('A file slice arrived incomplete'));
      expect(onFileChanged).toHaveBeenCalledTimes(1);

      const second = rangedFile();
      rerender(view(second.file, onFileChanged));
      requestSlice(TRAILER);
      requestSlice(2 * SLICE);
      await second.answer(TRAILER);
      await second.fail(new Error('A file slice arrived incomplete'));

      expect(onFileChanged).toHaveBeenCalledTimes(1);
      expect(screen.getByText('failed')).toBeInTheDocument();
    });

    it('reloads again for a later failure once the reloaded file has read what was lost', async () => {
      const onFileChanged = vi.fn();
      const first = rangedFile();
      const { rerender } = render(view(first.file, onFileChanged));
      requestSlice(2 * SLICE);
      await first.fail(new Error('Network Error'));

      const second = rangedFile();
      rerender(view(second.file, onFileChanged));
      requestSlice(2 * SLICE);
      await second.answer(2 * SLICE);
      requestSlice(5 * SLICE);
      await second.fail(new Error('Network Error'));

      expect(onFileChanged).toHaveBeenCalledTimes(2);
      expect(isStale(second.file)).toBe(true);
      expect(screen.queryByText('failed')).toBeNull();
    });

    it('asks nothing more of the file it is reloading, whatever its other slices do', async () => {
      const onFileChanged = vi.fn();
      const { file, fail, answer } = rangedFile();
      render(view(file, onFileChanged));
      requestSlices(3);
      await fail(new Error('Network Error'), 0);
      // Even a read that holds the bytes it lost, as a wider span would.
      doc.file!.range!.requestDataRange(0, 2 * SLICE);
      await answer(0);
      await fail(new Error('Network Error'), 2 * SLICE);

      expect(onFileChanged).toHaveBeenCalledTimes(1);
      expect(screen.queryByText('failed')).toBeNull();
    });

    it('does not reload for a slice that fails after the viewer closed', async () => {
      const onFileChanged = vi.fn();
      const { file, fail } = rangedFile();
      const { unmount } = render(view(file, onFileChanged));
      requestSlices(1);
      // pdf.js aborts the transport only after its worker confirms the close.
      unmount();
      await fail(new Error('Network Error'));

      expect(onFileChanged).not.toHaveBeenCalled();
      expect(isStale(file)).toBe(false);
    });
  });
});
