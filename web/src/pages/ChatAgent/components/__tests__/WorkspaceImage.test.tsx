import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';

import { runAuthResets } from '@/lib/authResets';
import WorkspaceImage from '../WorkspaceImage';
import { WorkspaceProvider } from '../../contexts/WorkspaceContext';
import type { ComputerFolders } from '../../utils/agentPaths';

// WorkspaceImage's only utils/api dependency is the authenticated downloader.
const downloadWorkspaceFile = vi.fn((..._args: unknown[]) => Promise.resolve('blob:authed'));
vi.mock('../../utils/api', () => ({
  downloadWorkspaceFile: (...args: unknown[]) => downloadWorkspaceFile(...args),
}));

function renderInWorkspace(
  src: string,
  { workspaceId, downloadFile, folders = null }: {
    workspaceId: string | null;
    downloadFile: ((p: string) => void) | null;
    folders?: ComputerFolders | null;
  },
) {
  return render(
    <WorkspaceProvider workspaceId={workspaceId} downloadFile={downloadFile} folders={folders}>
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

  // From the working directory `../Home/x.png` is `x.png`, and the server
  // refuses the climb as written.
  it('fetches a path through the workspace\'s own folder from its root', async () => {
    renderInWorkspace('../Home/charts/own.png', {
      workspaceId: 'ws-home',
      downloadFile: null,
      folders: { dirName: 'Home', siblings: [] },
    });
    await waitFor(() => expect(downloadWorkspaceFile).toHaveBeenCalledWith('ws-home', 'charts/own.png'));
  });

  // One name per row: the blob cache would answer the second from the first.
  it.each([
    ['climbed into', '../NVDA/charts/climbed.png', 'charts/climbed.png'],
    ['rooted', '/home/workspace/NVDA/charts/rooted.png', 'charts/rooted.png'],
  ])('fetches an image in a sibling folder %s from that workspace', async (_how, src, expected) => {
    renderInWorkspace(src, {
      workspaceId: 'ws-home',
      downloadFile: null,
      folders: { dirName: 'Home', siblings: [{ workspaceId: 'ws-nvda', dirName: 'NVDA' }] },
    });
    await waitFor(() => expect(downloadWorkspaceFile).toHaveBeenCalledWith('ws-nvda', expected));
  });

  it('names an image on another computer outside its workspaces instead of fetching it here', async () => {
    const SSH = '/home/nosudo/langalpha-2';
    const folders: ComputerFolders = {
      dirName: 'Home',
      siblings: [],
      otherComputers: [{ root: SSH, workspaces: [{ workspaceId: 'ws-ssh', dirName: 'lithium' }] }],
    };
    renderInWorkspace(`${SSH}/scratch/elsewhere.png`, { workspaceId: 'ws-home', downloadFile: null, folders });
    expect(screen.getByText('chart')).toBeInTheDocument();
    expect(downloadWorkspaceFile).not.toHaveBeenCalled();

    renderInWorkspace(`${SSH}/lithium/charts/there.png`, { workspaceId: 'ws-home', downloadFile: null, folders });
    await waitFor(() => expect(downloadWorkspaceFile).toHaveBeenCalledWith('ws-ssh', 'charts/there.png'));
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

describe('WorkspaceImage forgets the account on sign-out', () => {
  let revokeUrl: typeof URL.revokeObjectURL;
  beforeEach(() => {
    downloadWorkspaceFile.mockClear();
    revokeUrl = URL.revokeObjectURL;
    URL.revokeObjectURL = vi.fn();
  });
  afterEach(() => {
    URL.revokeObjectURL = revokeUrl;
  });

  // Regression: the cache outlived the account. The next account to render the
  // same key was shown the last one's image with no request, and the bytes
  // stayed in the tab until it closed.
  it('revokes what it cached and fetches again', async () => {
    downloadWorkspaceFile.mockResolvedValueOnce('blob:first-account');
    const first = renderInWorkspace('charts/signout.png', { workspaceId: 'ws-1', downloadFile: null });
    await waitFor(() => expect(screen.getByAltText('chart')).toHaveAttribute('src', 'blob:first-account'));
    first.unmount();

    runAuthResets();
    expect(URL.revokeObjectURL).toHaveBeenCalledWith('blob:first-account');

    downloadWorkspaceFile.mockResolvedValueOnce('blob:next-account');
    renderInWorkspace('charts/signout.png', { workspaceId: 'ws-1', downloadFile: null });
    await waitFor(() => expect(screen.getByAltText('chart')).toHaveAttribute('src', 'blob:next-account'));
    expect(downloadWorkspaceFile).toHaveBeenCalledTimes(2);
  });

  // A load still running at sign-out lands with the last account's file.
  it('keeps a load that finishes after the sign-out out of the cache', async () => {
    let finish!: (url: string) => void;
    downloadWorkspaceFile.mockReturnValueOnce(new Promise<string>((resolve) => { finish = resolve; }));
    renderInWorkspace('charts/inflight.png', { workspaceId: 'ws-1', downloadFile: null });
    await waitFor(() => expect(downloadWorkspaceFile).toHaveBeenCalledTimes(1));

    runAuthResets();
    await act(async () => finish('blob:previous-account'));
    expect(URL.revokeObjectURL).toHaveBeenCalledWith('blob:previous-account');

    renderInWorkspace('charts/inflight.png', { workspaceId: 'ws-1', downloadFile: null });
    await waitFor(() => expect(downloadWorkspaceFile).toHaveBeenCalledTimes(2));
  });

  // One chart referenced twice fetches twice; the copy that lands second would
  // have replaced the first in the cache and left it past the sign-out.
  it('shows the cached copy when a second load of the file lands', async () => {
    downloadWorkspaceFile.mockResolvedValueOnce('blob:first').mockResolvedValueOnce('blob:second');
    renderInWorkspace('charts/twice.png', { workspaceId: 'ws-1', downloadFile: null });
    renderInWorkspace('charts/twice.png', { workspaceId: 'ws-1', downloadFile: null });
    await waitFor(() => expect(downloadWorkspaceFile).toHaveBeenCalledTimes(2));

    await waitFor(() =>
      expect(screen.getAllByAltText('chart').map((img) => img.getAttribute('src'))).toEqual(['blob:first', 'blob:first']),
    );
    expect(URL.revokeObjectURL).toHaveBeenCalledWith('blob:second');
    runAuthResets();
    expect(URL.revokeObjectURL).toHaveBeenCalledWith('blob:first');
  });
});
