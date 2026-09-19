import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';

import WorkspaceImage from '../WorkspaceImage';
import { WorkspaceProvider } from '../../contexts/WorkspaceContext';

// WorkspaceImage's only utils/api dependency is the authenticated downloader.
const downloadWorkspaceFile = vi.fn((..._args: unknown[]) => Promise.resolve('blob:authed'));
vi.mock('../../utils/api', () => ({
  downloadWorkspaceFile: (...args: unknown[]) => downloadWorkspaceFile(...args),
}));

function renderInWorkspace(
  src: string,
  { workspaceId, downloadFile }: { workspaceId: string | null; downloadFile: ((p: string) => void) | null },
) {
  return render(
    <WorkspaceProvider workspaceId={workspaceId} downloadFile={downloadFile}>
      <WorkspaceImage src={src} alt="chart" />
    </WorkspaceProvider>,
  );
}

describe('WorkspaceImage — __wsref__ downloader selection', () => {
  beforeEach(() => {
    downloadWorkspaceFile.mockClear();
  });

  // Regression: the public shared view (no workspace context, only a
  // share-token blob fetcher) must NOT fall back to the authed
  // /workspaces/{id}/files/download endpoint for __wsref__ images — that 401s
  // when logged out and broke shared report links.
  it('uses the context downloader for __wsref__ images when there is no workspace context (shared view)', async () => {
    const sharedDownloader = vi.fn(() => Promise.resolve('blob:shared'));
    renderInWorkspace('__wsref__/ws-shared/results/charts/rev.png', {
      workspaceId: null,
      downloadFile: sharedDownloader,
    });

    await waitFor(() => expect(sharedDownloader).toHaveBeenCalledWith('results/charts/rev.png'));
    expect(downloadWorkspaceFile).not.toHaveBeenCalled();
  });

  // Unchanged authed behavior: with a real workspace context, a cross-workspace
  // __wsref__ ref resolves through the authed downloader against the referenced
  // workspace UUID, bypassing the context (active-workspace) downloader.
  it('uses the authed downloader for __wsref__ images when a workspace context exists', async () => {
    const activeDownloader = vi.fn(() => Promise.resolve('blob:active'));
    renderInWorkspace('__wsref__/ws-other/results/charts/rev2.png', {
      workspaceId: 'ws-active',
      downloadFile: activeDownloader,
    });

    await waitFor(() =>
      expect(downloadWorkspaceFile).toHaveBeenCalledWith('ws-other', 'results/charts/rev2.png'),
    );
    expect(activeDownloader).not.toHaveBeenCalled();
  });

  // Plain relative-path images always use the context downloader in both modes.
  it('uses the context downloader for plain relative-path images', async () => {
    const sharedDownloader = vi.fn(() => Promise.resolve('blob:rel'));
    renderInWorkspace('results/charts/plain.png', {
      workspaceId: null,
      downloadFile: sharedDownloader,
    });

    await waitFor(() => expect(sharedDownloader).toHaveBeenCalledWith('results/charts/plain.png'));
    expect(downloadWorkspaceFile).not.toHaveBeenCalled();
  });
});

describe('WorkspaceImage reads a destination the way a link does', () => {
  beforeEach(() => {
    downloadWorkspaceFile.mockClear();
  });

  // Regression: the raw src went to the downloader, so an absolute destination
  // asked for `/home/workspace/charts/x.png`, which no workspace path matches.
  // The image then failed with nothing on screen saying so. A link to the same
  // file has always gone through the normalizer; the image now does too.
  it.each([
    ['a sandbox-rooted path', '/home/workspace/charts/rooted.png', 'charts/rooted.png'],
    ['a file:// destination', 'file:///home/workspace/charts/proto.png', 'charts/proto.png'],
    ['the older sandbox root', '/home/daytona/charts/daytona.png', 'charts/daytona.png'],
    ['a percent-encoded name', 'charts/%E5%9B%BE%E8%A1%A8.png', 'charts/图表.png'],
  ])('fetches the workspace-relative path for %s', async (_label, src, expected) => {
    renderInWorkspace(src, { workspaceId: 'ws-1', downloadFile: null });
    await waitFor(() => expect(downloadWorkspaceFile).toHaveBeenCalledWith('ws-1', expected));
  });

  // An image that did not load is a state, not a blank: the name plus why.
  it('names the file when it could not be loaded', async () => {
    const failing = vi.fn(() => Promise.reject(new Error('gone')));
    renderInWorkspace('results/charts/broken.png', { workspaceId: null, downloadFile: failing });

    await waitFor(() =>
      expect(screen.getByText('broken.png could not be loaded')).toBeInTheDocument(),
    );
  });
});
