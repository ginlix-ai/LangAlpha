/**
 * A share or stop answers with the link itself. Two panels can hold the same
 * link under different spellings of the file (the ChatView's crumb and the
 * FilePanel's header both ask), so the answer replaces every cached entry
 * whose code matches, not just the one keyed by the path this call used; the
 * workspace's shared list is re-read rather than patched. A 409 re-reads
 * what it says is out of date and writes nothing.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { waitFor } from '@testing-library/react';

import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { FileShareLink } from '@/types/api';

vi.mock('@/pages/ChatAgent/utils/api/shareLinks', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/pages/ChatAgent/utils/api/shareLinks')>();
  return {
    ...actual,
    createShareLink: vi.fn(),
    getShareLinkFiles: vi.fn(),
    patchShareLink: vi.fn(),
    listSharedLinks: vi.fn(async () => ({ links: [] })),
  };
});

import { patchShareLink } from '@/pages/ChatAgent/utils/api/shareLinks';
import { useShareLinkMutations } from '../useShareLink';

const mockPatch = patchShareLink as Mock;

const WS = 'ws-1';
const CODE = 'k7f2m9q1x4z8';
const OTHER_CODE = 'p3d8w1n6v2b5';

function link(overrides: Partial<FileShareLink> = {}): FileShareLink {
  return {
    code: CODE,
    workspace_id: WS,
    url: `/a/${CODE}`,
    kind: 'file',
    path: 'results/report.html',
    port: null,
    title: null,
    shared: false,
    shared_at: null,
    shared_files: null,
    created_at: '2026-09-24T00:00:00Z',
    ...overrides,
  };
}

function conflict(code: string) {
  return Object.assign(new Error('Request failed with status code 409'), {
    response: { status: 409, data: { detail: { code } } },
  });
}

// The spellings a panel might have asked under: the canonical key, a rooted
// one nothing normalized, and an unrelated link that must be left alone.
const CANONICAL = queryKeys.shareLinks.link(WS, 'file', 'results/report.html');
const ROOTED = queryKeys.shareLinks.link(WS, 'file', '/home/workspace/results/report.html');
const UNRELATED = queryKeys.shareLinks.link(WS, 'file', 'results/other.html');
const SHARED = queryKeys.shareLinks.shared(WS);
const FILES = queryKeys.shareLinks.files(WS, CODE);

// Seeded entries have no observer, so they must outlive the default gcTime of 0
// for the test to see what settle did to them.
function fresh() {
  return new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } } });
}

function seed() {
  const queryClient = fresh();
  queryClient.setQueryData(CANONICAL, link());
  queryClient.setQueryData(ROOTED, link());
  queryClient.setQueryData(UNRELATED, link({ code: OTHER_CODE, path: 'results/other.html' }));
  queryClient.setQueryData(SHARED, { links: [] });
  queryClient.setQueryData(FILES, { files: [], total_size: 0, drift: null });
  return queryClient;
}

describe('useShareLinkMutations settle', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('a share replaces every cached entry with that code and invalidates the shared list', async () => {
    const shared = link({ shared: true, shared_at: '2026-09-24T01:00:00Z', shared_files: ['results/report.html'] });
    mockPatch.mockResolvedValue(shared);
    const queryClient = seed();
    const { result } = renderHookWithProviders(() => useShareLinkMutations(WS), { queryClient });

    await result.current.patch.mutateAsync({ code: CODE, patch: { shared: true, files: ['results/report.html'] } });

    expect(mockPatch).toHaveBeenCalledWith(WS, CODE, { shared: true, files: ['results/report.html'] });
    expect(queryClient.getQueryData(CANONICAL)).toEqual(shared);
    expect(queryClient.getQueryData(ROOTED)).toEqual(shared);
    expect(queryClient.getQueryData(UNRELATED)).toMatchObject({ code: OTHER_CODE, shared: false });
    await waitFor(() => expect(queryClient.getQueryState(SHARED)?.isInvalidated).toBe(true));
    expect(queryClient.getQueryState(FILES)?.isInvalidated).toBe(true);
    expect(queryClient.getQueryState(UNRELATED)?.isInvalidated).toBe(false);
  });

  it('a stop writes the private link back the same way', async () => {
    const stopped = link({ shared: false });
    mockPatch.mockResolvedValue(stopped);
    const queryClient = seed();
    queryClient.setQueryData(CANONICAL, link({ shared: true, shared_files: ['results/report.html'] }));
    queryClient.setQueryData(ROOTED, link({ shared: true, shared_files: ['results/report.html'] }));
    const { result } = renderHookWithProviders(() => useShareLinkMutations(WS), { queryClient });

    await result.current.patch.mutateAsync({ code: CODE, patch: { shared: false } });

    expect(mockPatch).toHaveBeenCalledWith(WS, CODE, { shared: false });
    expect(queryClient.getQueryData(CANONICAL)).toEqual(stopped);
    expect(queryClient.getQueryData(ROOTED)).toEqual(stopped);
    await waitFor(() => expect(queryClient.getQueryState(SHARED)?.isInvalidated).toBe(true));
  });

  it('a 409 files_changed re-reads only the file list', async () => {
    mockPatch.mockRejectedValue(conflict('files_changed'));
    const queryClient = seed();
    const { result } = renderHookWithProviders(() => useShareLinkMutations(WS), { queryClient });

    await expect(
      result.current.patch.mutateAsync({ code: CODE, patch: { shared: true, files: ['results/report.html'] } }),
    ).rejects.toThrow();

    await waitFor(() => expect(queryClient.getQueryState(FILES)?.isInvalidated).toBe(true));
    expect(queryClient.getQueryState(CANONICAL)?.isInvalidated).toBe(false);
    expect(queryClient.getQueryState(SHARED)?.isInvalidated).toBe(false);
  });

  it('a 409 link_changed re-reads every entry holding that code, the shared list and the file list', async () => {
    mockPatch.mockRejectedValue(conflict('link_changed'));
    const queryClient = seed();
    const { result } = renderHookWithProviders(() => useShareLinkMutations(WS), { queryClient });

    await expect(result.current.patch.mutateAsync({ code: CODE, patch: { shared: false } })).rejects.toThrow();

    await waitFor(() => expect(queryClient.getQueryState(CANONICAL)?.isInvalidated).toBe(true));
    expect(queryClient.getQueryState(ROOTED)?.isInvalidated).toBe(true);
    expect(queryClient.getQueryState(UNRELATED)?.isInvalidated).toBe(false);
    expect(queryClient.getQueryState(SHARED)?.isInvalidated).toBe(true);
    expect(queryClient.getQueryState(FILES)?.isInvalidated).toBe(true);
    // Nothing is written on a conflict: the entries keep what they held until the re-read lands.
    expect(queryClient.getQueryData(CANONICAL)).toEqual(link());
  });
});
