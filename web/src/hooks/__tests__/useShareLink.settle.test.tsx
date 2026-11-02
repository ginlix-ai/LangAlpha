/**
 * A stop answers with the link itself. Two panels can hold the same link
 * under different spellings of the file (the ChatView's crumb and the
 * FilePanel's header both ask), so the answer replaces every cached entry
 * whose code matches, not just the one keyed by the path this call used; the
 * workspace's shared list is re-read rather than patched.
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
    created_at: '2026-09-24T00:00:00Z',
    ...overrides,
  };
}

// The spellings a panel might have asked under: the canonical key, a rooted
// one nothing normalized, and an unrelated link that must be left alone.
const CANONICAL = queryKeys.shareLinks.link(WS, 'file', 'results/report.html');
const ROOTED = queryKeys.shareLinks.link(WS, 'file', '/home/workspace/results/report.html');
const UNRELATED = queryKeys.shareLinks.link(WS, 'file', 'results/other.html');
const SHARED = queryKeys.shareLinks.shared(WS);

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
  return queryClient;
}

describe('useShareLinkMutations settle', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('a stop replaces every cached entry with that code and invalidates the shared list', async () => {
    const stopped = link({ shared: false });
    mockPatch.mockResolvedValue(stopped);
    const queryClient = seed();
    queryClient.setQueryData(CANONICAL, link({ shared: true }));
    queryClient.setQueryData(ROOTED, link({ shared: true }));
    const { result } = renderHookWithProviders(() => useShareLinkMutations(WS), { queryClient });

    await result.current.patch.mutateAsync({ code: CODE, patch: { shared: false } });

    expect(mockPatch).toHaveBeenCalledWith(WS, CODE, { shared: false });
    expect(queryClient.getQueryData(CANONICAL)).toEqual(stopped);
    expect(queryClient.getQueryData(ROOTED)).toEqual(stopped);
    expect(queryClient.getQueryData(UNRELATED)).toMatchObject({ code: OTHER_CODE, shared: false });
    await waitFor(() => expect(queryClient.getQueryState(SHARED)?.isInvalidated).toBe(true));
    expect(queryClient.getQueryState(UNRELATED)?.isInvalidated).toBe(false);
  });
});
