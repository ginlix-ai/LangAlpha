/**
 * Marking everything read on a chart covers its ticker's other timeframes,
 * which its badge counts with it: each record is marked, and each record's
 * count clears where the chart's list of records holds it.
 */
import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import React, { type ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  markArtifactCommentsRead: vi.fn(),
}));

import { queryKeys } from '@/lib/queryKeys';
import { markArtifactCommentsRead } from '@/pages/ChatAgent/utils/api/artifacts';
import type { Artifact } from '@/types/api';
import { useMarkArtifactCommentsRead } from '../useArtifact';

const mark = markArtifactCommentsRead as Mock;
const record = (id: string, path: string, unread: number) =>
  ({ artifact_id: id, workspace_id: 'ws', kind: 'chart', path, unread }) as Artifact;

describe('useMarkArtifactCommentsRead', () => {
  beforeEach(() => mark.mockReset());

  it('marks each record and clears its count where the ticker\'s records are held', async () => {
    mark.mockResolvedValue([]);
    const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
    const charts = queryKeys.artifacts.chartsOf('ws', 'NVDA');
    client.setQueryData(charts, [record('d', 'NVDA:1day', 2), record('h', 'NVDA:1hour', 1)]);
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useMarkArtifactCommentsRead(), { wrapper });

    act(() => result.current.mutate(['d', 'h']));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mark.mock.calls.map(([id]) => id)).toEqual(['d', 'h']);
    expect(client.getQueryData<Artifact[]>(charts)?.map((a) => a.unread)).toEqual([0, 0]);
  });

  it('puts back only the count of a record whose mark was refused', async () => {
    mark.mockImplementation((id: string) => (id === 'h' ? Promise.reject(new Error('refused')) : Promise.resolve([])));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
    const charts = queryKeys.artifacts.chartsOf('ws', 'NVDA');
    client.setQueryData(charts, [record('d', 'NVDA:1day', 2), record('h', 'NVDA:1hour', 1)]);
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useMarkArtifactCommentsRead(), { wrapper });

    act(() => result.current.mutate(['d', 'h']));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(client.getQueryData<Artifact[]>(charts)?.map((a) => a.unread)).toEqual([0, 1]);
  });
});
