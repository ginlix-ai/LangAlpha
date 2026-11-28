import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import type { DrivePageBridge } from '@/api/drive';

const api = vi.hoisted(() => ({
  getHomeWorkspace: vi.fn(),
  readPage: vi.fn(),
}));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHomeWorkspace: api.getHomeWorkspace,
}));
vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  readPage: api.readPage,
}));

import { useHomeWorkspaceId } from '@/hooks/useHomeWorkspace';
import { queryKeys } from '@/lib/queryKeys';
import { useDrivePage, type DrivePageRead } from '../useDriveComments';

afterEach(() => {
  api.getHomeWorkspace.mockReset();
  api.readPage.mockReset();
});

const client = () => new QueryClient({ defaultOptions: { queries: { retry: false } } });
const page = (sha: string, inject: DrivePageBridge = 'comments', at: number | null = null): DrivePageRead => ({ entryId: 'e1', sha, seq: null, at, inject });

describe('an HTML page opened before Home is known', () => {
  it('is read once, with the comment bridge, not first with the view bridge', async () => {
    let home: (id: string) => void = () => {};
    api.getHomeWorkspace.mockImplementation(() => new Promise((resolve) => { home = (id) => resolve({ workspace_id: id }); }));
    api.readPage.mockResolvedValue('<html></html>');
    // As the Drive asks: the comment bridge once Home's id is known, the view bridge until then.
    const { result } = renderHookWithProviders(() => {
      const homeId = useHomeWorkspaceId();
      return useDrivePage('personal', page('a', homeId ? 'comments' : 'view'));
    }, { queryClient: client() });
    expect(result.current.isPending).toBe(true);
    expect(api.readPage).not.toHaveBeenCalled();

    await act(async () => home('home-1'));

    await waitFor(() => expect(result.current.data?.doc).toBe('<html></html>'));
    expect(api.readPage).toHaveBeenCalledTimes(1);
    expect(api.readPage).toHaveBeenCalledWith('personal', 'e1', 'comments', null);
  });

  it('is read at once with the view bridge in a space that takes no comments', async () => {
    api.getHomeWorkspace.mockImplementation(() => new Promise(() => {}));
    api.readPage.mockResolvedValue('<html></html>');
    const { result } = renderHookWithProviders(() => useDrivePage('team-1', page('a', 'view')), { queryClient: client() });
    await waitFor(() => expect(result.current.data?.doc).toBe('<html></html>'));
    expect(api.readPage).toHaveBeenCalledWith('team-1', 'e1', 'view', null);
    expect(api.getHomeWorkspace).not.toHaveBeenCalled();
  });

  it('reads the view bridge once Home cannot be read', async () => {
    api.getHomeWorkspace.mockRejectedValue(new Error('down'));
    api.readPage.mockResolvedValue('<html></html>');
    const { result } = renderHookWithProviders(() => useDrivePage('personal', page('a', 'view')), { queryClient: client() });

    await waitFor(() => expect(result.current.data?.doc).toBe('<html></html>'));
    expect(api.readPage).toHaveBeenCalledWith('personal', 'e1', 'view', null);
  });
});

describe('an HTML page saved again while it is framed', () => {
  it('stays framed as it was until the new text is read, then moves to it', async () => {
    const qc = client();
    qc.setQueryData(queryKeys.drive.page('personal', 'e1', 'a', 'comments', null), { sha: 'a', doc: '<p>a</p>' });
    let arrive: () => void = () => {};
    api.readPage.mockImplementation(() => new Promise((resolve) => { arrive = () => resolve('<p>b</p>'); }));
    let sha = 'a';
    const { result, rerender } = renderHookWithProviders(() => useDrivePage('personal', page(sha)), { queryClient: qc });
    expect(result.current.data?.sha).toBe('a');

    sha = 'b';
    rerender();
    expect(result.current.data).toEqual({ sha: 'a', doc: '<p>a</p>' });
    expect(result.current.isPlaceholderData).toBe(true);

    await act(async () => arrive());
    await waitFor(() => expect(result.current.data).toEqual({ sha: 'b', doc: '<p>b</p>' }));
    expect(result.current.isPlaceholderData).toBe(false);
  });

  it('never stands another page, or another bridge, in for one still loading', () => {
    const qc = client();
    qc.setQueryData(queryKeys.drive.page('team-1', 'e1', 'a', 'comments', null), { sha: 'a', doc: '<p>a</p>' });
    api.readPage.mockImplementation(() => new Promise(() => {}));
    for (const next of [{ ...page('b'), entryId: 'e2' }, page('b', 'view'), { ...page('b'), seq: 3 }]) {
      let read = page('a');
      const { result, rerender, unmount } = renderHookWithProviders(() => useDrivePage('team-1', read), { queryClient: qc });
      expect(result.current.data?.sha).toBe('a');
      read = next;
      rerender();
      expect(result.current.data).toBeUndefined();
      unmount();
    }
  });

  it('asks for a first page as it is, for the frame to wait on', () => {
    api.readPage.mockImplementation(() => new Promise(() => {}));
    const { result } = renderHookWithProviders(() => useDrivePage('personal', page('a')), { queryClient: client() });
    expect(result.current.isPending).toBe(true);
  });
});

describe("the head's page", () => {
  it('is read at the version its hash names, so a save made since never lands under that hash', async () => {
    api.readPage.mockResolvedValue('<html></html>');
    const { result } = renderHookWithProviders(() => useDrivePage('team-1', page('a', 'view', 7)), { queryClient: client() });
    await waitFor(() => expect(result.current.data).toEqual({ sha: 'a', doc: '<html></html>' }));
    expect(api.readPage).toHaveBeenCalledWith('team-1', 'e1', 'view', 7);
  });
});
