/**
 * A thread URL carries no workspace, and a navigation may carry none either:
 * back/forward to an entry without route state, a link from outside the chat.
 * The workspace in view before is no stand-in for the thread's own, so the
 * route makes no view of the thread until its workspace is known. A view made
 * under the old one was a second view of the thread: it loaded the thread a
 * second time and stayed in the cache.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, act, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useNavigate, type NavigateFunction } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatAgent from '../ChatAgent';

const WS_A = '11111111-1111-4111-8111-111111111111';
const WS_B = '22222222-2222-4222-8222-222222222222';
const A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';

// Each view loads its thread's history once, when it mounts.
const historyLoads = vi.fn();
const lookups = new Map<string, (thread: { workspace_id: string }) => void>();

vi.mock('../components/ChatView', () => ({
  default: function ChatViewStub({ workspaceId, threadId, isActive }: { workspaceId: string; threadId: string; isActive: boolean }) {
    React.useEffect(() => {
      historyLoads(threadId, workspaceId);
    }, []); // eslint-disable-line react-hooks/exhaustive-deps
    return <div data-testid="view" data-thread={threadId} data-workspace={workspaceId} data-active={isActive} />;
  },
}));
vi.mock('../utils/threadQueries', () => ({
  threadDetailQuery: (threadId: string) => ({
    queryKey: ['threads', 'detail', threadId],
    queryFn: () => new Promise((resolve) => lookups.set(threadId, resolve)),
    retry: false,
  }),
}));
vi.mock('../components/ComputersDialogHost', () => ({ default: () => null }));
vi.mock('../hooks/useComputers', () => ({ useComputerStatusFanout: () => {} }));
vi.mock('../hooks/useWarmWorkspaceSandbox', () => ({ useWarmWorkspaceSandbox: () => false }));
vi.mock('@/lib/threadLifecycle/useActiveThreadPublisher', () => ({ useActiveThreadPublisher: () => {} }));
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));
vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (key: string) => key }) }));

let navigate: NavigateFunction;
function NavigateHandle() {
  const nav = useNavigate();
  React.useEffect(() => {
    navigate = nav;
  });
  return null;
}

function renderAt(threadId: string, state: unknown) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[{ pathname: `/chat/t/${threadId}`, state }]}>
        <NavigateHandle />
        <Routes>
          <Route path="/chat" element={<ChatAgent />} />
          <Route path="/chat/t/:threadId/:taskId" element={<ChatAgent />} />
          <Route path="/chat/t/:threadId" element={<ChatAgent />} />
          <Route path="/chat/:workspaceId" element={<ChatAgent />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const viewsOf = (threadId: string) =>
  screen.queryAllByTestId('view').filter((el) => el.dataset.thread === threadId).map((el) => ({
    workspace: el.dataset.workspace,
    active: el.dataset.active === 'true',
  }));
const loadsOf = (threadId: string) => historyLoads.mock.calls.filter(([tid]) => tid === threadId);

describe('opening a thread without a workspace in the navigation', () => {
  beforeEach(() => {
    historyLoads.mockClear();
    lookups.clear();
    sessionStorage.clear();
  });

  it('makes one view of the thread, under its own workspace, once the lookup has it', async () => {
    renderAt(A, { workspaceId: WS_A });
    expect(viewsOf(A)).toEqual([{ workspace: WS_A, active: true }]);

    act(() => {
      navigate(`/chat/t/${B}`);
    });
    // Not under the workspace of the thread before.
    expect(viewsOf(B)).toEqual([]);
    expect(loadsOf(B)).toEqual([]);
    expect(lookups.has(B)).toBe(true);

    act(() => {
      lookups.get(B)!({ workspace_id: WS_B });
    });
    await waitFor(() => expect(viewsOf(B)).toEqual([{ workspace: WS_B, active: true }]));
    expect(loadsOf(B)).toEqual([[B, WS_B]]);
    expect(viewsOf(A)).toEqual([{ workspace: WS_A, active: false }]);
  });

  it('shows a cached thread at once, without a second view or load', async () => {
    renderAt(A, { workspaceId: WS_A });
    act(() => {
      navigate(`/chat/t/${B}`, { state: { workspaceId: WS_B } });
    });
    expect(viewsOf(B)).toEqual([{ workspace: WS_B, active: true }]);

    act(() => {
      navigate(`/chat/t/${A}`);
    });
    expect(viewsOf(A)).toEqual([{ workspace: WS_A, active: true }]);
    act(() => {
      navigate(`/chat/t/${B}`);
    });
    expect(viewsOf(B)).toEqual([{ workspace: WS_B, active: true }]);

    expect(loadsOf(A)).toHaveLength(1);
    expect(loadsOf(B)).toHaveLength(1);
    expect(screen.queryAllByTestId('view')).toHaveLength(2);
  });
});
