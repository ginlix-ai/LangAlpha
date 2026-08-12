import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, waitFor } from '@testing-library/react';
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
