/**
 * The status of the file a view shows now, read with what that page reported
 * loading where the list is reviewed. A view stays mounted from tab to tab, so
 * the list it keeps on screen while it reads again must be its own file's.
 */
import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import React, { type ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  getArtifactStatus: vi.fn(),
}));

import { runAuthResets } from '@/lib/authResets';
import { NO_PAGE_LOADS, notePageLoads, notePageOpen } from '@/lib/pageLoads';
import { queryKeys } from '@/lib/queryKeys';
import { getArtifactStatus } from '@/pages/ChatAgent/utils/api/artifacts';
import type { Artifact, ArtifactStatus } from '@/types/api';
import { useArtifactStatus } from '../useArtifact';

const read = getArtifactStatus as Mock;
const never = new Promise<ArtifactStatus>(() => {});

const record = (id: string) => ({ artifact_id: id, workspace_id: 'ws', path: `${id}.html` }) as Artifact;
const answer = (id: string) => ({ live_sha256: id }) as ArtifactStatus;
const frame = {};
const reported = (paths: string[]) => notePageLoads('ws', 'a.html', paths, frame);

function harness(first: Artifact, report = true) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  const view = renderHook(
    ({ artifact, enabled }: { artifact: Artifact; enabled: boolean }) => useArtifactStatus(artifact, { enabled, report }),
    { wrapper, initialProps: { artifact: first, enabled: true } },
  );
  return { client, ...view };
}

describe('useArtifactStatus', () => {
  beforeEach(() => {
    read.mockReset();
    runAuthResets();
    notePageOpen('ws', 'a.html', frame);
  });

  it("never shows one file's status as another's, read or not", async () => {
    read.mockImplementation((id: string) => (id === 'a' ? Promise.resolve(answer('a')) : never));
    const { result, rerender } = harness(record('a'));
    await waitFor(() => expect(result.current.data?.live_sha256).toBe('a'));

    rerender({ artifact: record('b'), enabled: true });
    expect(result.current.data).toBeUndefined();
    // A disabled query takes a placeholder too.
    rerender({ artifact: record('c'), enabled: false });
    expect(result.current.data).toBeUndefined();
  });

  it("keeps a file's list on screen while a new report reads it again", async () => {
    read.mockResolvedValueOnce(answer('a')).mockReturnValueOnce(never);
    const { result } = harness(record('a'));
    await waitFor(() => expect(result.current.data?.live_sha256).toBe('a'));
    expect(read).toHaveBeenLastCalledWith('a', NO_PAGE_LOADS);

    act(() => reported(['data/a.csv']));
    await waitFor(() => expect(read).toHaveBeenLastCalledWith('a', { now: ['data/a.csv'], earlier: [] }));
    expect(result.current.isFetching).toBe(true);
    expect(result.current.data?.live_sha256).toBe('a');
  });

  it('reads once, with no report, where nobody reviews the list', async () => {
    read.mockResolvedValue(answer('a'));
    reported(['data/a.csv']);
    const { result } = harness(record('a'), false);
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(read).toHaveBeenLastCalledWith('a', NO_PAGE_LOADS);

    // A page that keeps loading files does not make the crumb read again.
    act(() => reported(['data/a.csv', 'data/b.csv']));
    expect(read).toHaveBeenCalledTimes(1);
  });

  it('is read again by the key that names the file alone', async () => {
    read.mockResolvedValue(answer('a'));
    reported(['data/a.csv']);
    const { result, client } = harness(record('a'));
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(read).toHaveBeenCalledTimes(1);

    await act(() => client.invalidateQueries({ queryKey: queryKeys.artifacts.status('a') }));
    expect(read).toHaveBeenCalledTimes(2);
  });
});
