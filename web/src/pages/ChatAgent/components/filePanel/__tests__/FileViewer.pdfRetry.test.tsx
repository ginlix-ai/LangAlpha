import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { FileViewer, type FileViewerProps } from '../FileViewer';
import type { RangedFile } from '@/lib/rangedFile';

// The viewer fails while the slices it needs do, as one does when a read keeps
// failing, and draws once they read again. Each one drawn leaves the reload it
// was handed, as a slice failing later would call it.
const slices = vi.hoisted(() => ({ failing: true, reloads: [] as Array<() => void> }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'dark' }) }));
vi.mock('../../viewers/PdfViewer', () => ({
  default: function PdfViewer({ onFileChanged }: { onFileChanged?: () => void }) {
    if (slices.failing) throw new Error('A slice could not be read');
    if (onFileChanged) slices.reloads.push(onFileChanged);
    return <div data-testid="pdf" />;
  },
}));

const RANGED: RangedFile = { length: 100, etag: '"v1"', initial: new Uint8Array(10), read: async () => new Uint8Array() };

const props = (body: FileViewerProps['body']): FileViewerProps => ({
  path: 'report.pdf',
  body,
  loading: false,
  error: null,
  onRetry: () => {},
  workspaceId: 'ws',
  onAnchorLink: () => {},
});

describe('FileViewer PDF that failed', () => {
  beforeEach(() => {
    slices.failing = true;
    slices.reloads = [];
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  it('offers Try again on a large PDF, which opens it again', async () => {
    render(<FileViewer {...props({ mode: 'buffer', content: null, mime: 'pdf', buffer: null, ranged: RANGED, truncated: false })} />);
    const retry = await screen.findByRole('button', { name: 'Try again' });
    slices.failing = false;
    fireEvent.click(retry);
    expect(await screen.findByTestId('pdf')).toBeTruthy();
  });

  it('offers no Try again where the whole file failed, which would fail again', async () => {
    render(<FileViewer {...props({ mode: 'buffer', content: null, mime: 'pdf', buffer: new ArrayBuffer(8), truncated: false })} />);
    expect(await screen.findByText('Unable to preview this file')).toBeTruthy();
    expect(screen.queryByRole('button', { name: 'Try again' })).toBeNull();
  });

  it('reloads the file whose viewer lost a slice, even once another file is open', async () => {
    // pdf.js lets a closed document's reads go only once its worker confirms,
    // so a slice of the file just left can still fail after the next opens.
    slices.failing = false;
    const onFileChanged = vi.fn();
    const body: FileViewerProps['body'] = { mode: 'buffer', content: null, mime: 'pdf', buffer: null, ranged: RANGED, truncated: false };
    const { rerender } = render(<FileViewer {...props(body)} path="a.pdf" onFileChanged={onFileChanged} />);
    await screen.findByTestId('pdf');
    const left = slices.reloads[slices.reloads.length - 1];
    rerender(<FileViewer {...props(body)} path="b.pdf" onFileChanged={onFileChanged} />);
    await screen.findByTestId('pdf');
    left();
    expect(onFileChanged).toHaveBeenCalledWith('a.pdf');
  });
});
