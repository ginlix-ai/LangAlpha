/**
 * A thread's own effort and fast mode reach the server two ways: a pick on a
 * live thread is PATCHed on its row, so a send there carries only the turn's
 * level; a landing composer's picks arrive in the navigation state and ride
 * the first send, whose create stores them, and are what the new thread's
 * composer shows.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import '@testing-library/jest-dom';
import type React from 'react';
import { act, render, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClientProvider } from '@tanstack/react-query';
import { createTestQueryClient } from '@/test/utils';

const mocks = vi.hoisted(() => ({
  getThread: vi.fn(),
  getWorkspace: vi.fn(),
  updateThread: vi.fn(),
  setValue: vi.fn(),
}));

vi.mock('@/lib/framer', async () => (await import('./chatViewHarness')).framerMock());
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock('@/lib/supabase', () => ({ supabase: null }));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', setTheme: () => {} }),
}));
vi.mock('../Markdown', async () => (await import('./chatViewHarness')).markdownMock());
// The harness's composer, with the one handle method a draft is seeded through.
vi.mock('../../../../components/ui/chat-input', async () => {
  const { useImperativeHandle } = await import('react');
  const { default: Stub } = (await import('./chatViewHarness')).chatInputMock();
  return {
    default: function ChatInputStub(props: Record<string, unknown>) {
      useImperativeHandle(props.ref as React.Ref<unknown>, () => ({ setValue: mocks.setValue }));
      return Stub(props);
    },
  };
});
vi.mock('../../hooks/useChatMessages', async (importOriginal) =>
  (await import('./chatViewHarness')).chatMessagesMock(await importOriginal()));
vi.mock('../../hooks/useWorkspaceFiles', async () => (await import('./chatViewHarness')).workspaceFilesMock());
vi.mock('../../hooks/useNavigationData', async (importOriginal) =>
  (await import('./chatViewHarness')).navigationDataMock(await importOriginal()));
vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await import('./chatViewHarness')).apiMock(await importOriginal()),
  getThread: mocks.getThread,
  getWorkspace: mocks.getWorkspace,
  updateThread: mocks.updateThread,
}));

import { THREAD_ID, WORKSPACE_ID, chatInput, chatState, mountChatView, resetChatState } from './chatViewHarness';
import ChatView from '../ChatView';

type OnSend = (message: string, attachments?: unknown[], slashCommands?: unknown[], options?: Record<string, unknown>) => void;

/** The composer's send, as a press of Enter makes it. */
function send(options: Record<string, unknown> = {}) {
  act(() => { (chatInput.props!.onSend as OnSend)('hello', [], [], options); });
  const calls = chatState.handleSendMessage.mock.calls;
  return calls[calls.length - 1][3] as Record<string, unknown>;
}

beforeAll(() => {
  if (!Element.prototype.scrollTo) {
    Element.prototype.scrollTo = (() => {}) as typeof Element.prototype.scrollTo;
  }
  if (!Element.prototype.scrollIntoView) {
    Element.prototype.scrollIntoView = (() => {}) as typeof Element.prototype.scrollIntoView;
  }
});

beforeEach(() => {
  resetChatState();
  mocks.getThread.mockReset();
  mocks.getWorkspace.mockReset();
  mocks.updateThread.mockReset();
  mocks.setValue.mockReset();
});

describe('ChatView: the thread\'s effort and fast mode', () => {
  it('shows the row\'s own and leaves the override off a send on a live thread', async () => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' });
    mocks.getThread.mockResolvedValue({ thread_id: THREAD_ID, reasoning_effort: 'high', fast_mode: null });
    mountChatView(ChatView);

    await waitFor(() => expect(chatInput.props?.tuning).toEqual({ reasoningEffort: 'high', fastMode: null }));
    const options = send({ reasoningEffort: 'high' });
    expect(options).toMatchObject({ reasoningEffort: 'high' });
    expect(options.reasoningEffortOverride).toBeUndefined();
    expect(options.fastModeOverride).toBeUndefined();

    mocks.updateThread.mockReturnValue(new Promise(() => {}));
    act(() => { void (chatInput.props!.onPickFastMode as (next: boolean | null) => Promise<boolean>)(true); });
    await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledWith(THREAD_ID, { fast_mode: true }));
  });

  it("carries a landing composer's picks on the first send, beside the turn's level", async () => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' });
    chatState.threadId = '__default__';
    const entry = {
      pathname: '/chat/t/__default__',
      state: {
        workspaceId: WORKSPACE_ID,
        initialMessage: 'hello',
        reasoningEffort: 'low',
        fastMode: true,
        reasoningEffortOverride: 'low',
        fastModeOverride: true,
      },
    };
    const queryClient = createTestQueryClient();
    render(<ChatView workspaceId={WORKSPACE_ID} threadId="__default__" onBack={vi.fn()} workspaceName="Harness WS" />, {
      wrapper: ({ children }: { children: React.ReactNode }) => (
        <QueryClientProvider client={queryClient}>
          <MemoryRouter initialEntries={[entry]}>{children}</MemoryRouter>
        </QueryClientProvider>
      ),
    });

    await waitFor(() => expect(chatState.handleSendMessage).toHaveBeenCalledTimes(1));
    expect(chatState.handleSendMessage.mock.calls[0][3]).toMatchObject({
      reasoningEffort: 'low',
      fastMode: true,
      reasoningEffortOverride: 'low',
      fastModeOverride: true,
    });
    expect(chatInput.props?.tuning).toEqual({ reasoningEffort: 'low', fastMode: true });
  });
});
