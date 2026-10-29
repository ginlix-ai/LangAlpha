/**
 * A file behind its `/a/` link renders under the serve prefix the metadata
 * named. For HTML that is the iframe's src; for everything else it is where
 * the panel's own reader fetches the body and a Markdown document's images,
 * while a link to a sibling workspace file is plain text, since a visitor
 * can open only the files listed.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, waitFor } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';
import { buildServeUrl } from '../../ChatAgent/components/viewers/html/wsfilesUrl';
import { SERVED_HTML_SANDBOX } from '../../ChatAgent/components/viewers/html/sandbox';

vi.mock('../../../contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', setTheme: () => {} }),
}));
vi.mock('../../ChatAgent/components/viewers/html/useHtmlSandbox', () => ({
  useHtmlSandbox: () => ({ height: null, pushTheme: () => {}, scrollToAnchor: () => {} }),
}));

import SharedFileView from '../SharedFileView';
import type { SharedFileMetadata } from '../api';

const CODE = 'k7f2m9q1x4z8';
const FRAME_BASE = '/api/v1/wsfiles/s/2f9c1a7e/';

function metadata(path: string): SharedFileMetadata {
  return { kind: 'file', name: path.split('/').pop()!, path, access: 'public', frame_base: FRAME_BASE };
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

  it('renders HTML in an iframe served under frame_base with the theme injected', () => {
    const fetchMock = serve({});
    const { container } = renderWithProviders(<SharedFileView code={CODE} metadata={metadata('results/Q3 report.html')} />);

    const frame = container.querySelector('iframe')!;
    expect(frame).toHaveAttribute('title', 'Q3 report.html');
    const src = frame.getAttribute('src')!;
    expect(src).toBe(buildServeUrl(FRAME_BASE, 'results/Q3 report.html', { injectTheme: true }));
    // The shape that matters: the prefix is the credential, the path rides
    // under it with its segments encoded, and the theme query is the only one.
    expect(src.endsWith(`${FRAME_BASE}results/Q3%20report.html?inject=theme`)).toBe(true);
    expect(src).not.toContain('workspace');
    expect(frame).toHaveAttribute('sandbox', SERVED_HTML_SANDBOX);
    // No read for HTML: the frame is the reader.
    expect(fetchMock).not.toHaveBeenCalled();
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('Q3 report.html');
    expect(screen.getByRole('button', { name: 'Save as PDF' })).toBeInTheDocument();
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
