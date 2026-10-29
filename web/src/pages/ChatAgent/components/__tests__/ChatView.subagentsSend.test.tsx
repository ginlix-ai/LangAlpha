/**
 * A send carries the subagents setting only when it creates the thread: on a
 * live thread the PATCH a flip makes is the only writer, so a send never
 * re-sends a cached value over a newer flip from another tab. Home's threads
 * hold it like any workspace's. A landing composer's toggle arrives in the
 * navigation state, rides the first send and is what the new thread's toggle
 * shows. What the toggle holds when the new thread's id arrives is saved on
 * its row, so a flip made while the send was creating it is not lost. A new
 * chat's view is cached while unsent and taken up by the next navigation to
 * one, whose pick replaces whatever the view held.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import '@testing-library/jest-dom';
import type React from 'react';
import { act, render, waitFor } from '@testing-library/react';
import { MemoryRouter, useLocation, useNavigate, type NavigateFunction } from 'react-router';
import { QueryClientProvider } from '@tanstack/react-query';
import { createTestQueryClient } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

const mocks = vi.hoisted(() => ({
  getThread: vi.fn(),
  getWorkspace: vi.fn(),
  updateThread: vi.fn(),
  setValue: vi.fn(),
}));

vi.mock('@/lib/framer', async () => (await import('./chatViewHarness')).framerMock());
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: 'en-US', resolvedLanguage: 'en-US' } }),
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

import { THREAD_ID, WORKSPACE_ID, chatInput, chatState, mountChatView, resetChatState, type ChatState } from './chatViewHarness';
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

describe('ChatView: the subagents setting on a send', () => {
  it("is left off a send on a live thread, whose row holds it", async () => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' });
    mocks.getThread.mockResolvedValue({ thread_id: THREAD_ID, subagents_allowed: false });
    mountChatView(ChatView);

    await waitFor(() => expect(chatInput.props?.subagentsAllowed).toBe(false));
    const options = send({ model: 'm1' });
    expect(options).toMatchObject({ model: 'm1' });
    expect(options.subagentsAllowed).toBeUndefined();
  });

  it('is left off a send while a flip is still saving, which goes out as the PATCH alone', async () => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' });
    mocks.getThread.mockResolvedValue({ thread_id: THREAD_ID, subagents_allowed: true });
    mountChatView(ChatView);
    await waitFor(() => expect(chatInput.props?.subagentsAllowed).toBe(true));

    mocks.updateThread.mockReturnValue(new Promise(() => {}));
    act(() => { void (chatInput.props!.onToggleSubagents as (next: boolean) => Promise<boolean>)(false); });
    await waitFor(() => expect(chatInput.props?.subagentsAllowed).toBe(false));
    expect(send().subagentsAllowed).toBeUndefined();
    expect(mocks.updateThread).toHaveBeenCalledWith(THREAD_ID, { subagents_allowed: false });
  });

  it("shows Home's toggle from its thread row and leaves it off a send", async () => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Home', status: 'running', is_home: true });
    mocks.getThread.mockResolvedValue({ thread_id: THREAD_ID, subagents_allowed: false });
    mountChatView(ChatView);

    await waitFor(() => expect(chatInput.props?.subagentsAllowed).toBe(false));
    const options = send({ model: 'm1' });
    expect(options).toMatchObject({ model: 'm1' });
    expect(options.subagentsAllowed).toBeUndefined();
  });
});

/** Mounts a new chat as a landing composer's send opens it. */
function mountLanding(subagentsAllowed: boolean, subagentsDefault: boolean) {
  const queryClient = createTestQueryClient();
  queryClient.setQueryData(queryKeys.user.preferences(), { other_preference: { subagents_default: subagentsDefault } });
  chatState.threadId = '__default__';
  const entry = {
    pathname: '/chat/t/__default__',
    state: { workspaceId: WORKSPACE_ID, initialMessage: 'hello', subagentsAllowed },
  };
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[entry]}>{children}</MemoryRouter>
    </QueryClientProvider>
  );
  const ui = () => <ChatView workspaceId={WORKSPACE_ID} threadId="__default__" onBack={vi.fn()} workspaceName="Harness WS" />;
  const view = render(ui(), { wrapper });
  const update = (patch: Partial<ChatState>) => {
    Object.assign(chatState, patch);
    act(() => { view.rerender(ui()); });
  };
  return { update, queryClient };
}

describe("ChatView: a landing composer's subagents setting", () => {
  it.each([false, true])('rides the first send as %s over the opposite default, and the toggle shows it', async (value) => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' });
    mountLanding(value, !value);

    await waitFor(() => expect(chatState.handleSendMessage).toHaveBeenCalledTimes(1));
    expect(chatState.handleSendMessage.mock.calls[0][0]).toBe('hello');
    expect(chatState.handleSendMessage.mock.calls[0][3]).toMatchObject({ subagentsAllowed: value });
    expect(chatInput.props?.subagentsAllowed).toBe(value);
  });

  it('is not written again once the row its send created holds it', async () => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' });
    const { update } = mountLanding(false, true);
    await waitFor(() => expect(chatState.handleSendMessage).toHaveBeenCalledTimes(1));

    // The send stored the pick with the row. A second write would be judged
    // against the default as it is by then, which another tab may have moved.
    mocks.getThread.mockResolvedValue({ thread_id: THREAD_ID, subagents_allowed: false });
    update({ threadId: THREAD_ID, isLoading: true });
    await waitFor(() => expect(mocks.getThread).toHaveBeenCalled());
    expect(chatInput.props?.subagentsAllowed).toBe(false);
    expect(send().subagentsAllowed).toBeUndefined();

    update({ isLoading: false });
    await waitFor(() => expect(chatInput.props?.subagentsAllowed).toBe(false));
    expect(mocks.updateThread).not.toHaveBeenCalled();
  });

  it('saves a flip made while the first send creates the thread, once its id arrives', async () => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' });
    const { update, queryClient } = mountLanding(false, true);
    const cachedRow = () => queryClient.getQueryData<{ subagents_allowed?: boolean | null }>(queryKeys.threads.detail(THREAD_ID));
    await waitFor(() => expect(chatState.handleSendMessage).toHaveBeenCalledTimes(1));

    // Back on while the thread is being created, with no row to save on yet.
    act(() => { void (chatInput.props!.onToggleSubagents as (next: boolean) => Promise<boolean>)(true); });
    await waitFor(() => expect(chatInput.props?.subagentsAllowed).toBe(true));
    expect(mocks.updateThread).not.toHaveBeenCalled();

    // The row holds what the send stored; on is the default's side, which
    // the server keeps as no value of its own.
    mocks.getThread.mockResolvedValue({ thread_id: THREAD_ID, subagents_allowed: false });
    mocks.updateThread.mockResolvedValue({ thread_id: THREAD_ID, subagents_allowed: null });
    update({ threadId: THREAD_ID, isLoading: true });
    await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledWith(THREAD_ID, { subagents_allowed: true }));
    expect(chatInput.props?.subagentsAllowed).toBe(true);

    mocks.getThread.mockResolvedValue({ thread_id: THREAD_ID, subagents_allowed: null });
    update({ isLoading: false });
    await waitFor(() => expect(cachedRow()?.subagents_allowed).toBeNull());
    expect(chatInput.props?.subagentsAllowed).toBe(true);
    expect(mocks.updateThread).toHaveBeenCalledTimes(1);
  });
});

/** Opens a new chat with no first message, as its view sits cached and
 *  unsent, and returns the navigation a landing composer's send makes to it. */
function mountUnsentNewChat() {
  const queryClient = createTestQueryClient();
  queryClient.setQueryData(queryKeys.user.preferences(), { other_preference: { subagents_default: true } });
  chatState.threadId = '__default__';
  let navigate: NavigateFunction | null = null;
  const Navigator = () => {
    navigate = useNavigate();
    return null;
  };
  const entry = { pathname: '/chat/t/__default__', state: { workspaceId: WORKSPACE_ID } };
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[entry]}>
        <Navigator />
        <ChatView workspaceId={WORKSPACE_ID} threadId="__default__" onBack={vi.fn()} workspaceName="Harness WS" />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  const land = (pick: { subagentsAllowed?: boolean }) => {
    act(() => {
      navigate!('/chat/t/__default__', { state: { workspaceId: WORKSPACE_ID, initialMessage: 'hello', ...pick } });
    });
  };
  return { land };
}

describe('ChatView: a new chat view taken up by a later landing send', () => {
  it("carries that send's pick, which the view did not have when it opened", async () => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' });
    const { land } = mountUnsentNewChat();
    await waitFor(() => expect(chatInput.props?.subagentsAllowed).toBe(true));

    land({ subagentsAllowed: false });
    await waitFor(() => expect(chatState.handleSendMessage).toHaveBeenCalledTimes(1));
    expect(chatState.handleSendMessage.mock.calls[0][3]).toMatchObject({ subagentsAllowed: false });
    expect(chatInput.props?.subagentsAllowed).toBe(false);
  });

  it('lets go of a flip left on the view, which the landing composer never showed', async () => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' });
    const { land } = mountUnsentNewChat();
    await waitFor(() => expect(chatInput.props?.subagentsAllowed).toBe(true));
    act(() => { void (chatInput.props!.onToggleSubagents as (next: boolean) => Promise<boolean>)(false); });
    await waitFor(() => expect(chatInput.props?.subagentsAllowed).toBe(false));

    land({});
    await waitFor(() => expect(chatState.handleSendMessage).toHaveBeenCalledTimes(1));
    expect((chatState.handleSendMessage.mock.calls[0][3] as Record<string, unknown>).subagentsAllowed).toBeUndefined();
    expect(chatInput.props?.subagentsAllowed).toBe(true);
  });
});

describe("ChatView: a draft from Subagents' Create with the agent", () => {
  it('lands in the composer unsent, once, and leaves the navigation state', async () => {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Home', status: 'running', is_home: true });
    chatState.threadId = '__default__';
    let state = null as Record<string, unknown> | null;
    const Probe = () => {
      state = useLocation().state as Record<string, unknown> | null;
      return null;
    };
    const entry = {
      pathname: '/chat/t/__default__',
      state: { workspaceId: WORKSPACE_ID, draftMessage: 'Set up a subagent that ' },
    };
    render(
      <QueryClientProvider client={createTestQueryClient()}>
        <MemoryRouter initialEntries={[entry]}>
          <Probe />
          <ChatView workspaceId={WORKSPACE_ID} threadId="__default__" onBack={vi.fn()} workspaceName="Harness WS" />
        </MemoryRouter>
      </QueryClientProvider>,
    );

    await waitFor(() => expect(state?.draftMessage).toBeUndefined());
    expect(state?.workspaceId).toBe(WORKSPACE_ID);
    expect(mocks.setValue).toHaveBeenCalledTimes(1);
    expect(mocks.setValue).toHaveBeenCalledWith('Set up a subagent that ');
    expect(chatState.handleSendMessage).not.toHaveBeenCalled();
  });

  function mountDraftView(isActive: boolean) {
    mocks.getWorkspace.mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Home', status: 'running', is_home: true });
    chatState.threadId = '__default__';
    let state = null as Record<string, unknown> | null;
    let navigate!: NavigateFunction;
    const Probe = () => {
      state = useLocation().state as Record<string, unknown> | null;
      navigate = useNavigate();
      return null;
    };
    render(
      <QueryClientProvider client={createTestQueryClient()}>
        <MemoryRouter initialEntries={['/chat']}>
          <Probe />
          <ChatView workspaceId={WORKSPACE_ID} threadId="__default__" onBack={vi.fn()} workspaceName="Harness WS" isActive={isActive} />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    const open = (draftMessage: string) =>
      act(() => { void navigate('/chat/t/__default__', { state: { workspaceId: WORKSPACE_ID, draftMessage } }); });
    return { open, state: () => state };
  }

  it('takes each later draft into the same kept view, as a second starter question does', async () => {
    const { open, state } = mountDraftView(true);

    open('What moved in my watchlist today, and why?');
    await waitFor(() => expect(mocks.setValue).toHaveBeenCalledWith('What moved in my watchlist today, and why?'));
    await waitFor(() => expect(state()?.draftMessage).toBeUndefined());
    open('Set up a workspace to track semiconductor breadth');

    await waitFor(() => expect(mocks.setValue).toHaveBeenLastCalledWith('Set up a workspace to track semiconductor breadth'));
    expect(mocks.setValue).toHaveBeenCalledTimes(2);
  });

  it('leaves the draft to the view on screen when this one is kept hidden', async () => {
    const { open, state } = mountDraftView(false);

    open('What moved in my watchlist today, and why?');
    await new Promise((r) => setTimeout(r, 50));

    expect(mocks.setValue).not.toHaveBeenCalled();
    expect(state()?.draftMessage).toBe('What moved in my watchlist today, and why?');
  });
});
