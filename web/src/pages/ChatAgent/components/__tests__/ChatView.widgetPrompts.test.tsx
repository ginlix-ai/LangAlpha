/**
 * What ChatView adds to a ShowWidget widget's prompt calls: a send carries
 * what the composer's send carries for the page around it (the market-watch
 * skill, the panel's charts), and a new thread opens the way a new chat's
 * composer opens one, keeping this thread's settings. The data's card and the
 * draft are `useWidgetPromptActions`'s, tested beside it.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import '@testing-library/jest-dom';
import type React from 'react';
import { act, render, waitFor } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';
import { QueryClientProvider } from '@tanstack/react-query';
import { createTestQueryClient } from '@/test/utils';

const mocks = vi.hoisted(() => ({
  getThread: vi.fn(),
  getWorkspace: vi.fn(),
  composer: {
    getModelOptions: vi.fn(),
    addWidgetSnapshot: vi.fn(),
    addContext: vi.fn(),
  },
  actions: { current: null as Record<string, unknown> | null },
  chartViews: [] as unknown[],
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
vi.mock('@/hooks/useFeatures', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useFeatureEnabled: (feature: string) => feature === 'market_watch',
}));
vi.mock('../chatView/usePanelChart', async (importOriginal) => {
  const real = await importOriginal<typeof import('../chatView/usePanelChart')>();
  return {
    usePanelChart: (...args: Parameters<typeof real.usePanelChart>) => ({
      ...real.usePanelChart(...args),
      viewsForSend: () => mocks.chartViews,
    }),
  };
});
vi.mock('../../../../components/ui/chat-input', async () => {
  const { useImperativeHandle } = await import('react');
  const { default: Stub } = (await import('./chatViewHarness')).chatInputMock();
  return {
    default: function ChatInputStub(props: Record<string, unknown>) {
      useImperativeHandle(props.ref as React.Ref<unknown>, () => mocks.composer);
      return Stub(props);
    },
  };
});
// The transcript, reduced to the actions it is handed.
vi.mock('../MessageList', async (importOriginal) => {
  const { useMessageActions } = await import('../messageList/MessageActionsContext');
  return {
    ...(await importOriginal<Record<string, unknown>>()),
    LiveMessageList: function LiveMessageListStub() {
      mocks.actions.current = useMessageActions() as Record<string, unknown>;
      return null;
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
}));

import { THREAD_ID, WORKSPACE_ID, chatInput, chatState, resetChatState, userMsg } from './chatViewHarness';
import ChatView from '../ChatView';
import type { WidgetPrompts, WidgetSent } from '../../utils/widgetPrompt';

const SENT: WidgetSent = { data: { scenario: 'bull' }, widgetId: 'call_w1', title: 'Scenarios' };
const BOX = { model: 'm-box', reasoningEffort: 'high', fastMode: false, marketWatch: false };

// Every place the router went. The harness mounts no ChatAgent, so the view
// on screen takes up a new chat's route state itself and clears it; the
// navigation is what a new view would receive.
const location = { seen: [] as Array<ReturnType<typeof useLocation>> };
function LocationProbe() {
  const here = useLocation();
  if (location.seen.at(-1)?.key !== here.key) location.seen.push(here);
  return null;
}
const newChat = () => location.seen.find((l) => l.pathname === '/chat/t/__default__');

function mount() {
  const queryClient = createTestQueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[`/chat/t/${THREAD_ID}`]}>
        {children}
        <LocationProbe />
      </MemoryRouter>
    </QueryClientProvider>
  );
  render(<ChatView workspaceId={WORKSPACE_ID} threadId={THREAD_ID} onBack={vi.fn()} workspaceName="Harness WS" />, { wrapper });
}

const chatInputProps = () => chatInput.props;

async function prompts() {
  await waitFor(() => expect((mocks.actions.current?.widgetPrompts as WidgetPrompts | undefined)?.startThread).toBeTypeOf('function'));
  return mocks.actions.current!.widgetPrompts as Required<WidgetPrompts>;
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
  chatState.messages = [userMsg('u1', 'hi')];
  mocks.actions.current = null;
  location.seen = [];
  mocks.getWorkspace.mockReset().mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' });
  mocks.getThread.mockReset().mockResolvedValue({ thread_id: THREAD_ID, subagents_allowed: false, reasoning_effort: 'low', fast_mode: null });
  mocks.composer.getModelOptions.mockReset().mockReturnValue(BOX);
  mocks.composer.addWidgetSnapshot.mockReset();
  mocks.composer.addContext.mockReset();
  mocks.chartViews = [];
});

describe('ChatView: a widget prompt call', () => {
  it("sends with the message box's model and the card for its data", async () => {
    mount();
    const a = await prompts();
    act(() => a.sendPrompt('Model the bull case', SENT));

    const [text, contexts, meta, options] = chatState.handleSendMessage.mock.calls.at(-1)!;
    expect(text).toBe('Model the bull case');
    expect(meta).toBeNull();
    expect(contexts).toEqual([expect.objectContaining({ type: 'widget', widget_id: 'call_w1' })]);
    expect(options).toMatchObject({ model: 'm-box', reasoningEffort: 'high', fastMode: false });
    // The thread's settings ride a send only when it creates the thread.
    expect(options).toHaveProperty('subagentsAllowed', undefined);
  });

  it("carries what the composer's send carries for the page: the watch skill and the panel's charts", async () => {
    mount();
    const a = await prompts();
    mocks.composer.getModelOptions.mockReturnValue({ ...BOX, marketWatch: true });
    mocks.chartViews = [{ symbol: 'NVDA' }];
    act(() => a.sendPrompt('Go'));
    const [, contexts, , options] = chatState.handleSendMessage.mock.calls.at(-1)!;
    expect(contexts).toEqual([expect.objectContaining({ type: 'skills', name: 'market-watch' })]);
    expect(options.chartViews).toEqual([{ symbol: 'NVDA' }]);

    // The composer's own send carries the same.
    act(() => (chatInput.props!.onSend as (send: unknown) => void)({
      message: 'Also this', files: [], commands: [], modelOptions: { ...BOX, marketWatch: true },
    }));
    const [, sentContexts, , sentOptions] = chatState.handleSendMessage.mock.calls.at(-1)!;
    expect(sentContexts).toEqual([expect.objectContaining({ type: 'skills', name: 'market-watch' })]);
    expect(sentOptions.chartViews).toEqual([{ symbol: 'NVDA' }]);
  });

  // Sent by a widget all the same: the agent reads it differently from typed input.
  it('sends a text with no data as the widget it came from, its card the title alone', async () => {
    mount();
    const a = await prompts();
    act(() => a.sendPrompt('Go', { widgetId: 'widget-call_w9', title: 'Picker' }));
    const [text, contexts, , options] = chatState.handleSendMessage.mock.calls.at(-1)!;
    expect(text).toBe('Go');
    expect(contexts).toEqual([expect.objectContaining({
      type: 'widget', widget_type: 'chat.show_widget', widget_id: 'widget-call_w9', label: 'Picker', data: {}, text: '',
    })]);
    expect(options.widgetSnapshots).toEqual([expect.objectContaining({ label: 'Picker' })]);
    expect(options.widgetSnapshots[0]).not.toHaveProperty('description');
  });

  it('starts a private new thread in this workspace, its first message the text with the card', async () => {
    mount();
    const a = await prompts();
    await waitFor(() => expect(mocks.getThread).toHaveBeenCalled());
    act(() => a.startThread('Explore the bear case', { ...SENT, data: { scenario: 'bear' } }));

    const state = newChat()?.state as Record<string, unknown>;
    expect(state).toMatchObject({
      workspaceId: WORKSPACE_ID,
      initialMessage: 'Explore the bear case',
      model: 'm-box',
      reasoningEffort: 'high',
      fastMode: false,
    });
    expect(state.additionalContext).toEqual([expect.objectContaining({ type: 'widget', data: { scenario: 'bear' } })]);
    expect(state.widgetSnapshots).toEqual([expect.objectContaining({ widget_id: 'call_w1' })]);
    expect(state).not.toHaveProperty('sharedWithWorkspace');
  });

  it("carries this thread's subagents and effort to the new one once its row is read", async () => {
    mount();
    const a = await prompts();
    await waitFor(() => expect(chatInputProps()?.subagentsAllowed).toBe(false));
    act(() => a.startThread('Next'));
    const state = newChat()?.state as Record<string, unknown>;
    expect(state).toMatchObject({ initialMessage: 'Next', subagentsAllowed: false, reasoningEffortOverride: 'low' });
    expect(state).not.toHaveProperty('fastModeOverride');
    expect(state).not.toHaveProperty('additionalContext');
  });
});
