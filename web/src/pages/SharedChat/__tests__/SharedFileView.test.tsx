/**
 * A file behind its owner's `/a/` link, not HTML, renders under the serve
 * prefix the metadata named: it is where the panel's own reader fetches the
 * body and a Markdown document's images,
 * while a link to a sibling workspace file is plain text, since a visitor
 * can open only the files listed.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, waitFor } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';
import { buildServeUrl } from '../../ChatAgent/components/viewers/html/wsfilesUrl';

vi.mock('../../../contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', setTheme: () => {} }),
}));

import SharedFileView, { FileBody } from '../SharedFileView';
import type { SharedFileMetadata } from '../api';

const CODE = 'k7f2m9q1x4z8';
const FRAME_BASE = '/api/v1/wsfiles/s/2f9c1a7e/';

function metadata(path: string): SharedFileMetadata {
  return {
    kind: 'file', name: path.split('/').pop()!, path, access: 'owner',
    artifact_id: '5b0c3d1e-7a2f-4c8b-9e6d-1a2b3c4d5e6f', workspace_id: 'ws-1', shared: false, latest_seq: 0,
    frame_base: FRAME_BASE, expires_in: 12 * 60 * 60,
  };
}

/** The served origin: each path under the prefix answers with its body, anything else 404s. */
function serve(files: Record<string, string>) {
  const fetchMock = vi.fn(async (url: string) => {
    const hit = Object.entries(files).find(([path]) => url === buildServeUrl(FRAME_BASE, path));
    if (!hit) return { ok: false, status: 404 };
    return { ok: true, status: 200, text: async () => hit[1], blob: async () => new Blob([hit[1]]) };
  });
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

describe('SharedFileView', () => {
  beforeEach(() => {
    URL.createObjectURL = vi.fn(() => 'blob:served-image');
    URL.revokeObjectURL = vi.fn();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('reads Markdown and its images under frame_base, encoded per buildServeUrl', async () => {
    const fetchMock = serve({
      'notes/README.md': '# Notes\n\n![chart](notes/img/c 1.png)\n',
      'notes/img/c 1.png': 'png-bytes',
    });
    renderWithProviders(<SharedFileView code={CODE} metadata={metadata('notes/README.md')} />);

    expect(await screen.findByRole('heading', { level: 1, name: 'Notes' })).toBeInTheDocument();
    const img = await screen.findByAltText('chart');
    await waitFor(() => expect(img).toHaveAttribute('src', 'blob:served-image'));

    const urls = fetchMock.mock.calls.map(([url]) => url);
    expect(urls).toContain(buildServeUrl(FRAME_BASE, 'notes/README.md'));
    expect(urls.some((u) => u.endsWith(`${FRAME_BASE}notes/img/c%201.png`))).toBe(true);
    expect(screen.getByRole('button', { name: 'Download' })).toBeInTheDocument();
  });

  it('tells the page when the file answers 404, an Update having ended its version, and only then', async () => {
    serve({ 'kept.md': '# Kept\n' });
    const onGone = vi.fn();
    const body = (path: string) => (
      <FileBody scope="share:c:v1" path={path} frameBase={FRAME_BASE} onDownload={() => {}} downloading={false} onGone={onGone} />
    );

    const view = renderWithProviders(body('kept.md'));
    expect(await screen.findByRole('heading', { level: 1, name: 'Kept' })).toBeInTheDocument();
    expect(onGone).not.toHaveBeenCalled();

    view.rerender(body('ended.md'));
    await waitFor(() => expect(onGone).toHaveBeenCalledTimes(1));
  });

  it('renders a Markdown link to another workspace file as plain text', async () => {
    const fetchMock = serve({
      'notes/index.md': 'See [the other one](notes/other.md) and [docs](https://example.com/docs).\n',
    });
    renderWithProviders(<SharedFileView code={CODE} metadata={metadata('notes/index.md')} />);

    const sibling = await screen.findByText('the other one');
    expect(sibling.tagName).toBe('SPAN');
    expect(sibling.closest('a')).toBeNull();
    expect(sibling).not.toHaveAttribute('href');

    // The control: a web link keeps its href.
    const web = screen.getByText('docs');
    expect(web.closest('a')).toHaveAttribute('href', 'https://example.com/docs');
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('a file that will not load says why, the way the panel does', async () => {
    serve({});
    renderWithProviders(<SharedFileView code={CODE} metadata={metadata('data/summary.json')} />);

    // A 404 is classified, so it is not offered a retry that cannot help.
    expect(await screen.findByText('File not found')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Try again' })).not.toBeInTheDocument();
  });
});
