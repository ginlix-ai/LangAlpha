import type { ReactNode } from 'react';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Workspace } from '@/types/api';

const getWorkspace = vi.fn();
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getWorkspace: (id: string) => getWorkspace(id),
}));

import { useRestoredWorkspace } from '../useRestoredWorkspace';
import { useWorkspace } from '@/hooks/useWorkspace';
import { savePref } from '../../utils/prefs';

const ws = (id: string, status = 'running'): Workspace => ({ workspace_id: id, name: id, is_home: false, status });
// The first page of an account with more workspaces than one page holds.
const FIRST_PAGE = Array.from({ length: 50 }, (_, i) => ws(`ws-${i}`));
const httpError = (status: number) => Object.assign(new Error(`HTTP ${status}`), { response: { status } });

function render(restored: string, total: number, workspaces: Workspace[] = FIRST_PAGE, linkedId: string | null = null) {
  savePref('selectedWorkspaceId', restored);
  const onDropped = vi.fn();
  // The app's own retry default, which a check that inherited it would follow.
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 1 } } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  const hook = renderHook(() => useRestoredWorkspace({
    linkedId,
    workspaces,
    total,
    isFetchedAfterMount: true,
    isSuccess: true,
    onDropped,
  }), { wrapper });
  return { ...hook, onDropped, wrapper };
}

describe('useRestoredWorkspace', () => {
  beforeEach(() => {
    localStorage.clear();
    getWorkspace.mockReset();
  });

  it('keeps a restored workspace that exists past the first page, held until the check answers', async () => {
    let answer!: (w: Workspace) => void;
    getWorkspace.mockReturnValue(new Promise<Workspace>((resolve) => { answer = resolve; }));
    const { result, onDropped, wrapper } = render('ws-idle', 120);

    expect(result.current).toMatchObject({ selectedWorkspaceId: null, pending: true });

    await act(async () => answer(ws('ws-idle')));
    expect(result.current).toMatchObject({ selectedWorkspaceId: 'ws-idle', pending: false });
    expect(onDropped).not.toHaveBeenCalled();

    // The panel that opens the workspace reads the check's answer.
    const panel = renderHook(() => useWorkspace('ws-idle'), { wrapper });
    expect(panel.result.current.data).toMatchObject({ workspace_id: 'ws-idle' });
    expect(getWorkspace).toHaveBeenCalledTimes(1);
  });

  it('lists a kept workspace from past the first page so the selector can show it', async () => {
    getWorkspace.mockResolvedValue(ws('ws-idle'));
    const { result } = render('ws-idle', 120);

    await waitFor(() => expect(result.current.selectedWorkspaceId).toBe('ws-idle'));
    await waitFor(() => expect(result.current.workspaces.at(-1)).toMatchObject({ workspace_id: 'ws-idle' }));
    expect(result.current.workspaces.slice(0, FIRST_PAGE.length)).toEqual(FIRST_PAGE);
    expect(getWorkspace).toHaveBeenCalledTimes(1);
  });

  it('fetches a linked workspace from past the first page, which no chat panel asks for on a phone', async () => {
    getWorkspace.mockResolvedValue(ws('ws-far'));
    const { result, onDropped } = render('ws-3', 120, FIRST_PAGE, 'ws-far');

    expect(result.current.selectedWorkspaceId).toBe('ws-far');
    await waitFor(() => expect(result.current.workspaces.at(-1)).toMatchObject({ workspace_id: 'ws-far' }));
    expect(onDropped).not.toHaveBeenCalled();
    expect(getWorkspace).toHaveBeenCalledTimes(1);
  });

  it('hands the page over as it is when it lists the selection', () => {
    const { result } = render('ws-3', 120);

    expect(result.current.selectedWorkspaceId).toBe('ws-3');
    expect(result.current.workspaces).toBe(FIRST_PAGE);
    expect(getWorkspace).not.toHaveBeenCalled();
  });

  it.each([
    ['not found', () => Promise.reject(httpError(404))],
    ['forbidden', () => Promise.reject(httpError(403))],
    ['Home', () => Promise.resolve({ ...ws('ws-idle'), is_home: true })],
  ])('drops a restored workspace past the first page that is %s', async (_, check) => {
    getWorkspace.mockImplementation(check);
    const { result, onDropped } = render('ws-idle', 120);

    await waitFor(() => expect(result.current.selectedWorkspaceId).toBe('ws-0'));
    expect(onDropped).toHaveBeenCalledTimes(1);
    expect(getWorkspace).toHaveBeenCalledTimes(1);
  });

  it('keeps the restored workspace when the check itself fails', async () => {
    getWorkspace.mockRejectedValue(new Error('Network Error'));
    const { result, onDropped } = render('ws-idle', 120);

    await waitFor(() => expect(result.current.pending).toBe(false));
    expect(result.current.selectedWorkspaceId).toBe('ws-idle');
    expect(onDropped).not.toHaveBeenCalled();
    expect(getWorkspace).toHaveBeenCalledTimes(1);
  });

  it('does not let a late answer overrule a workspace chosen meanwhile', async () => {
    let refuse!: () => void;
    getWorkspace.mockReturnValue(new Promise((_, reject) => { refuse = () => reject(httpError(404)); }));
    const { result, onDropped } = render('ws-idle', 120);

    act(() => result.current.select('ws-3'));
    await act(async () => refuse());
    expect(result.current.selectedWorkspaceId).toBe('ws-3');
    expect(onDropped).not.toHaveBeenCalled();
  });

  it('drops a restored workspace missing from a list that holds them all, without naming it', () => {
    const { result, onDropped } = render('ws-gone', 2, FIRST_PAGE.slice(0, 2));

    expect(result.current.selectedWorkspaceId).toBe('ws-0');
    expect(onDropped).toHaveBeenCalledTimes(1);
    expect(getWorkspace).not.toHaveBeenCalled();
  });
});
