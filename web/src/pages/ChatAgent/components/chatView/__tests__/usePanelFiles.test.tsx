import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import { listWorkspaceFiles } from '@/pages/ChatAgent/utils/api';
import { usePanelFiles } from '../usePanelFiles';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  listWorkspaceFiles: vi.fn(async (workspaceId: string) => ({ files: [`${workspaceId}.md`] })),
}));

// Home's panel may show a sibling's files, read-only. The agent's writes land
// in Home, so the refresh after one lists Home's: listing the sibling's with
// autoStart would start its computer, and staling everything under Home's id
// would drop the file bodies its open tabs read.

const HOME = '11111111-1111-4111-8111-111111111111';
const SIBLING = '22222222-2222-4222-8222-222222222222';
const list = vi.mocked(listWorkspaceFiles);

let queryClient: QueryClient;

function render(props: Partial<Parameters<typeof usePanelFiles>[0]> = {}) {
  return renderHook(() => usePanelFiles({
    isHome: true,
    workspaceId: HOME,
    includeSystem: false,
    ...props,
  }), {
    wrapper: ({ children }) => <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>,
  }).result;
}

beforeEach(() => {
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  list.mockClear();
});

describe('usePanelFiles', () => {
  it("refreshes Home's own files after a write while a sibling's are shown", async () => {
    const homeBody = queryKeys.workspaceFiles.body(HOME, 'notes.md', 'text');
    queryClient.setQueryData(homeBody, 'notes');
    const result = render();
    act(() => result.current.setOverride(SIBLING));
    await waitFor(() => expect(result.current.mentionFiles).toEqual([`${HOME}.md`]));
    expect(result.current.files).toEqual([`${SIBLING}.md`]);
    list.mockClear();

    await act(() => result.current.refreshOwn());

    expect(list).toHaveBeenCalledTimes(1);
    expect(list).toHaveBeenCalledWith(HOME, '.', expect.objectContaining({ autoStart: true }));
    expect(queryClient.getQueryState(homeBody)?.isInvalidated).toBe(false);
  });

  it("shows a sibling's files read-only, with the way back to Home's", async () => {
    const result = render();
    expect(result.current.panelAccess).toMatchObject({ readOnly: false, isHome: true, onReturnHome: null });
    act(() => result.current.setOverride(SIBLING));
    expect(result.current.shownWorkspaceId).toBe(SIBLING);
    expect(result.current.panelAccess).toMatchObject({ readOnly: true, singleFileMode: true, canShare: false, isHome: false });

    act(() => result.current.panelAccess.onReturnHome?.());
    expect(result.current.shownWorkspaceId).toBe(HOME);
  });
});
