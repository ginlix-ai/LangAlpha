/**
 * A comment layer waits for its record's comments before it settles an open
 * asked of it (a reply's comment number, a chart comment followed from the
 * chat). A comments read that gives up ends that wait as a failed record
 * read does, so the open settles on what is there rather than never.
 */
import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import React, { type ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  findArtifact: vi.fn(),
  listArtifactComments: vi.fn(),
}));

import { findArtifact, listArtifactComments } from '@/pages/ChatAgent/utils/api/artifacts';
import type { Artifact } from '@/types/api';
import { useCommentRecord } from '../useArtifact';

const find = findArtifact as Mock;
const list = listArtifactComments as Mock;
const chart = { kind: 'chart', path: 'NVDA:1day' } as const;
const record = { artifact_id: 'a-1', workspace_id: 'ws', kind: 'chart', path: 'NVDA:1day', open_comments: 1, latest_seq: 0 } as Artifact;

function mount() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  return renderHook(() => useCommentRecord('ws', chart, { ensure: false }), { wrapper });
}

describe('useCommentRecord', () => {
  beforeEach(() => {
    find.mockReset();
    list.mockReset();
  });

  it('settles with the error when the comments read fails after the record is found', async () => {
    find.mockResolvedValue(record);
    list.mockRejectedValue(Object.assign(new Error('Artifact not found'), { status: 404 }));
    const { result } = mount();

    await waitFor(() => expect(result.current.loaded).toBe(true));
    expect(result.current.artifact).toBe(record);
    expect(result.current.error?.message).toBe('Artifact not found');
  });

  it('waits for the comments while they are still being read', async () => {
    find.mockResolvedValue(record);
    list.mockReturnValue(new Promise(() => {}));
    const { result } = mount();

    await waitFor(() => expect(list).toHaveBeenCalled());
    expect(result.current.loaded).toBe(false);
    expect(result.current.error).toBeNull();
  });
});
