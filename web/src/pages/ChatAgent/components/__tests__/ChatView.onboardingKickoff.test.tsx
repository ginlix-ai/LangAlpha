/**
 * The opening turn the welcome flow asks for. Asked where to start, it goes
 * out with no words of the user's and no bubble, under a title of its own, in
 * the language the app is in. A task written in the flow's composer goes out
 * as the user's message, titled from it like any other. The view opened for
 * it draws no empty chat while the turn goes out, and tells the hand-off into
 * it once the agent is drawn at work.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import '@testing-library/jest-dom';
import type React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClientProvider } from '@tanstack/react-query';
import { createTestQueryClient } from '@/test/utils';

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
vi.mock('../../../../components/ui/chat-input', async () => (await import('./chatViewHarness')).chatInputMock());
// The harness's hook state, drawn again when a send lands a message, as the
// real hook's state is, so the view's own reading of the list is exercised.
const live = vi.hoisted(() => {
  const listeners = new Set<() => void>();
  return {
    subscribe: (fn: () => void) => {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
    publish: () => listeners.forEach((fn) => fn()),
  };
});
vi.mock('../../hooks/useChatMessages', async (importOriginal) => {
  const { useSyncExternalStore } = await import('react');
  const harness = await import('./chatViewHarness');
  const mocked = harness.chatMessagesMock(await importOriginal());
  const liveMessages = { get: () => harness.chatState.messages, set: () => {}, subscribe: live.subscribe };
  return {
    ...mocked,
    useChatMessages: () => {
      useSyncExternalStore(live.subscribe, () => harness.chatState.messages);
      return { ...mocked.useChatMessages(), liveMessages };
    },
  };
});
// Whether the agent's row was on the page each time the hand-off was told.
const handoff = vi.hoisted(() => ({ readyWithRow: [] as boolean[] }));
vi.mock('@/pages/Onboarding/welcome/handoff', () => ({
  markHandoffReady: () => {
    handoff.readyWithRow.push(document.querySelector('[data-testid="streaming-indicator"]') !== null);
  },
}));
vi.mock('../../hooks/useWorkspaceFiles', async () => (await import('./chatViewHarness')).workspaceFilesMock());
vi.mock('../../hooks/useNavigationData', async (importOriginal) =>
  (await import('./chatViewHarness')).navigationDataMock(await importOriginal()),
);
vi.mock('../../utils/api', async (importOriginal) =>
  (await import('./chatViewHarness')).apiMock(await importOriginal()),
);

import { WORKSPACE_ID, assistant, chatInput, chatState, resetChatState, userMsg } from './chatViewHarness';
import ChatView from '../ChatView';
import type { ReadyAttachment } from '@/components/ui/chat-input.types';
import { queryKeys } from '@/lib/queryKeys';
import { BRIEF_PREAMBLE, kickoffSend, type OnboardingKickoff } from '@/pages/Onboarding/welcome/kickoff';
import { screenshotAttachment } from '@/pages/Onboarding/welcome/screenshots';

const EMPTY_CHAT = 'Start a conversation by typing a message below';
const TITLE = 'onboarding.kickoff.threadTitle';

/** A kickoff with nothing but what a test sets. */
const kickoff = (over: Partial<OnboardingKickoff> = {}): OnboardingKickoff => ({
  task: '',
  brokerages: [],
  brokeragesUsed: [],
  attachments: [],
  screenshots: 0,
  brief: null,
  ...over,
});

/** The route state the welcome flow's Start navigates with: Home's, built
 *  from the kickoff, with the composer's picks and the hand-off's mark. */
const welcomeState = (k: OnboardingKickoff, picks: Record<string, unknown> = {}) => ({
  workspaceId: WORKSPACE_ID,
  ...kickoffSend(k, 'zh-CN', TITLE),
  ...picks,
  handoff: true,
});

/** Opens the new chat with `state`, as the welcome flow's Start does, or with
 *  none as any other new chat opens. A stored Subagents default is set when
 *  given; otherwise the preferences are still unread. */
function mountKickoff(state: Record<string, unknown> | null, subagentsDefault?: boolean) {
  chatState.threadId = '__default__';
  const queryClient = createTestQueryClient();
  if (subagentsDefault !== undefined) {
    queryClient.setQueryData(queryKeys.user.preferences(), {
      other_preference: { subagents_default: subagentsDefault },
    });
  }
  const entry = { pathname: '/chat/t/__default__', state: { workspaceId: WORKSPACE_ID, ...state } };
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[entry]}>{children}</MemoryRouter>
    </QueryClientProvider>
  );
  render(<ChatView workspaceId={WORKSPACE_ID} threadId="__default__" onBack={vi.fn()} workspaceName="Harness WS" />, {
    wrapper,
  });
}

/** The one send the kickoff made: its message, context, attachments and options. */
async function sent() {
  await waitFor(() => expect(chatState.handleSendMessage).toHaveBeenCalledTimes(1));
  const [message, context, meta, options] = chatState.handleSendMessage.mock.calls[0];
  return {
    message: message as string,
    context: context as Record<string, unknown>[],
    meta: meta as unknown[] | null,
    options: options as Record<string, unknown>,
  };
}

/** Lands the send as the hook does, its bubbles with the reply still
 *  streaming, in the same tick as the call. Returns whether the page showed
 *  the empty chat's prompt as each send went out: the view drew that frame. */
function landSend(...messages: Record<string, unknown>[]) {
  const emptyAtSend: boolean[] = [];
  chatState.handleSendMessage.mockImplementation(() => {
    emptyAtSend.push(document.body.textContent?.includes(EMPTY_CHAT) ?? false);
    chatState.messages = messages;
    live.publish();
  });
  return emptyAtSend;
}

const instruction = (context: Record<string, unknown>[]) =>
  context.find((entry) => entry.type === 'skills')?.instruction as string;

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
  handoff.readyWithRow = [];
});

describe("ChatView: the welcome flow's opening turn", () => {
  it('opens hidden under its own title when the user asked where to start', async () => {
    mountKickoff(welcomeState(kickoff({ brokerages: ['Alpaca'] })));

    const { message, context, meta, options } = await sent();
    expect(message).toBe('');
    expect(meta).toBeNull();
    expect(options).toMatchObject({ hiddenOpener: true, threadTitle: 'onboarding.kickoff.threadTitle' });
    expect(instruction(context)).toContain('Action: propose.');
    // The flow's language, not the chat's.
    expect(instruction(context)).toContain('Language: zh-CN.');
  });

  it("sends a written task as the user's message, with no title of its own", async () => {
    mountKickoff(welcomeState(kickoff({ task: 'Check my NVDA position' })));

    const { message, context, options } = await sent();
    expect(message).toBe('Check my NVDA position');
    expect(options.hiddenOpener).toBeUndefined();
    expect(options.threadTitle).toBeUndefined();
    expect(instruction(context)).toContain('Action: first_task.');
  });

  it('goes out as the view mounts, with no wait for the chat to settle', () => {
    mountKickoff(welcomeState(kickoff()));
    // Home's send waits a beat; the hand-off's frame is already waiting on
    // this one, so it goes in the mount's own effects.
    expect(chatState.handleSendMessage).toHaveBeenCalledTimes(1);
  });

  it('draws no empty chat while a proposal goes out, and tells the hand-off once the agent is at work', async () => {
    const emptyAtSend = landSend(assistant('assistant-1', { isStreaming: true, hiddenOpener: true }));
    mountKickoff(welcomeState(kickoff()));

    await waitFor(() => expect(screen.getByTestId('streaming-indicator')).toBeInTheDocument());
    expect(emptyAtSend).toEqual([false]);
    expect(screen.queryByText(EMPTY_CHAT)).toBeNull();
    // Once, and with the row it lands on already drawn.
    expect(handoff.readyWithRow).toEqual([true]);
    expect(document.querySelector('[data-handoff-scope]')).not.toBeNull();
  });

  it("tells the hand-off once a written task is in the thread as the user's message", async () => {
    const emptyAtSend = landSend(
      userMsg('user-1', 'Check my NVDA position'),
      assistant('assistant-1', { isStreaming: true }),
    );
    mountKickoff(welcomeState(kickoff({ task: 'Check my NVDA position' })));

    await waitFor(() => expect(screen.getByTestId('streaming-indicator')).toBeInTheDocument());
    expect(emptyAtSend).toEqual([false]);
    expect(screen.queryByText(EMPTY_CHAT)).toBeNull();
    expect(document.querySelector('[data-handoff-scope] [data-message-role="user"]')).not.toBeNull();
    expect(handoff.readyWithRow).toEqual([true]);
  });

  it('leaves the prompt and the hand-off alone in a new chat opened any other way', async () => {
    mountKickoff(null);

    expect(await screen.findByText(EMPTY_CHAT)).toBeInTheDocument();
    expect(document.querySelector('[data-handoff-scope]')).toBeNull();
    chatState.messages = [userMsg('user-1', 'Hello'), assistant('assistant-1', { isStreaming: true })];
    live.publish();
    await waitFor(() => expect(screen.getByTestId('streaming-indicator')).toBeInTheDocument());
    expect(handoff.readyWithRow).toEqual([]);
  });

  it("leaves the hand-off alone in a chat Home's composer opened, which waits its beat to send", async () => {
    vi.useFakeTimers();
    try {
      mountKickoff({ initialMessage: 'hello' });
      expect(chatState.handleSendMessage).not.toHaveBeenCalled();
      expect(document.querySelector('[data-handoff-scope]')).toBeNull();
      await vi.advanceTimersByTimeAsync(100);
      expect(chatState.handleSendMessage).toHaveBeenCalledTimes(1);
      expect(chatState.handleSendMessage.mock.calls[0][0]).toBe('hello');
    } finally {
      vi.useRealTimers();
    }
  });
});

/**
 * What the chat's send was handed before the flow sent through Home's route
 * state, recorded from the kickoff branch it replaced, for the same inputs
 * the flow now builds the state from. Three things differ, each on purpose:
 * a screenshot's size is the bytes it holds, not its data URL's length; a
 * hidden opener sends no bubble's record of its files, which it never drew;
 * and the composer's Subagents pick rides the send, where it was dropped.
 */
describe("ChatView: the welcome flow's opening turn, as it went out before", () => {
  const SKILL_TAIL =
    "This is the user's first conversation, right after onboarding. " +
    'Their investing profile is in your system prompt; do not ask for it again.';
  const attachment = (name: string, type: string, body: string, dataUrl: string | null, preview: string | null) =>
    ({ file: new File([body], name, { type }), type, dataUrl, preview }) satisfies ReadyAttachment;
  const chart = attachment('chart.png', 'image/png', 'abcd', 'data:image/png;base64,AAAA', 'blob:chart');
  const composer = [
    chart,
    attachment('draft.pdf', 'application/pdf', 'ab', null, null),
    attachment('report.pdf', 'application/pdf', 'abc', 'data:application/pdf;base64,QUJD', ''),
  ];
  const positions = { id: 's1', name: 'positions.jpg', dataUrl: 'data:image/jpeg;base64,AAAA', bytes: 20 };
  const watchlist = { id: 's2', name: 'watchlist.png', dataUrl: 'data:image/png;base64,iVBORw0KGgo=', bytes: 26 };

  it('sends a written task with screenshots, files, the brief and the model as it did', async () => {
    // As Welcome's Start puts them together: the screens first, then the
    // composer's files.
    const shots = [positions, watchlist];
    mountKickoff(
      welcomeState(
        kickoff({
          task: 'Check my NVDA position',
          brokeragesUsed: ['Fidelity', 'Acme Brokers'],
          attachments: [...shots.map(screenshotAttachment), ...composer],
          screenshots: shots.length,
          brief: '## Stocks slip as yields climb\n',
        }),
        { model: 'model-x', reasoningEffort: 'high' },
      ),
    );

    const call = chatState.handleSendMessage.mock.calls[0];
    expect(call).toEqual([
      'Check my NVDA position',
      [
        {
          type: 'skills',
          name: 'onboarding',
          instruction:
            'Onboarding kickoff. Action: first_task. Language: zh-CN. Brokerage: none. ' +
            'Brokerages used: Fidelity and Acme Brokers. Screenshots: 2. Market brief: attached. ' +
            SKILL_TAIL,
        },
        {
          type: 'directive',
          content: `${BRIEF_PREAMBLE}\n<market-brief>\n## Stocks slip as yields climb\n</market-brief>`,
        },
        { type: 'image', data: 'data:image/jpeg;base64,AAAA', description: 'positions.jpg' },
        { type: 'image', data: 'data:image/png;base64,iVBORw0KGgo=', description: 'watchlist.png' },
        { type: 'image', data: 'data:image/png;base64,AAAA', description: 'chart.png' },
        { type: 'pdf', data: 'data:application/pdf;base64,QUJD', description: 'report.pdf' },
      ],
      [
        // Was 20 and 26, the data URLs' lengths.
        { name: 'positions.jpg', type: 'image/jpeg', size: 3, preview: null, dataUrl: 'data:image/jpeg;base64,AAAA' },
        {
          name: 'watchlist.png',
          type: 'image/png',
          size: 8,
          preview: null,
          dataUrl: 'data:image/png;base64,iVBORw0KGgo=',
        },
        { name: 'chart.png', type: 'image/png', size: 4, preview: 'blob:chart', dataUrl: 'data:image/png;base64,AAAA' },
        {
          name: 'report.pdf',
          type: 'application/pdf',
          size: 3,
          preview: null,
          dataUrl: 'data:application/pdf;base64,QUJD',
        },
      ],
      // The preferences are unread, so the send names what the toggle shows.
      { model: 'model-x', reasoningEffort: 'high', subagentsAllowed: true },
    ]);
  });

  it('sends a proposal with a screenshot, a file, the brief and the model as it did', async () => {
    mountKickoff(
      welcomeState(
        kickoff({
          brokerages: ['Alpaca', 'moomoo'],
          attachments: [screenshotAttachment(positions), chart],
          screenshots: 1,
          brief: '## Headline',
        }),
        { model: 'model-x' },
      ),
    );

    expect(chatState.handleSendMessage.mock.calls[0]).toEqual([
      '',
      [
        {
          type: 'skills',
          name: 'onboarding',
          instruction:
            'Onboarding kickoff. Action: propose. Language: zh-CN. Brokerage: Alpaca and moomoo. ' +
            'Screenshots: 1. Market brief: attached. ' +
            SKILL_TAIL,
        },
        { type: 'directive', content: `${BRIEF_PREAMBLE}\n<market-brief>\n## Headline\n</market-brief>` },
        { type: 'image', data: 'data:image/jpeg;base64,AAAA', description: 'positions.jpg' },
        { type: 'image', data: 'data:image/png;base64,AAAA', description: 'chart.png' },
      ],
      // Was the chart's record, for a bubble a hidden opener does not draw.
      null,
      {
        model: 'model-x',
        reasoningEffort: undefined,
        subagentsAllowed: true,
        hiddenOpener: true,
        threadTitle: 'onboarding.kickoff.threadTitle',
      },
    ]);
  });

  it('sends a bare proposal as it did', async () => {
    mountKickoff(welcomeState(kickoff()));

    expect(chatState.handleSendMessage.mock.calls[0]).toEqual([
      '',
      [
        {
          type: 'skills',
          name: 'onboarding',
          instruction: 'Onboarding kickoff. Action: propose. Language: zh-CN. Brokerage: none. ' + SKILL_TAIL,
        },
      ],
      null,
      { subagentsAllowed: true, hiddenOpener: true, threadTitle: 'onboarding.kickoff.threadTitle' },
    ]);
  });
});

describe("ChatView: the welcome composer's Subagents pick", () => {
  it.each([
    ['a written task', 'Check my NVDA position'],
    ['a proposal', ''],
  ])('rides %s turned off over a default of on, and the toggle shows it', async (_, task) => {
    mountKickoff(welcomeState(kickoff({ task }), { subagentsAllowed: false }), true);

    const { options } = await sent();
    expect(options.subagentsAllowed).toBe(false);
    expect(chatInput.props?.subagentsAllowed).toBe(false);
  });
});
