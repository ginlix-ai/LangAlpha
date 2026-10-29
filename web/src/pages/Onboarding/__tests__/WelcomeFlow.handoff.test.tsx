/**
 * The welcome flow as a conversation, and its hand-off into the chat: what
 * Start and the composer send, what the flow waits for first (a fresh
 * brokerage's tools, today's brief) and what it shows while it does, and a
 * start that cannot go on.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, cleanup, fireEvent, screen, waitFor, within } from '@testing-library/react';

// The flow's stand-ins, shared with its other test files (welcomeHarness.tsx).
vi.mock('@/lib/framer', async (importOriginal) => (await import('./welcomeHarness')).framerMock(importOriginal));
vi.mock('@/components/ui/chat-input', async () => (await import('./welcomeHarness')).composerMock());
vi.mock('@/hooks/useUpdatePreferences', async () => (await import('./welcomeHarness')).updatePreferencesMock());
vi.mock('../welcome/useBrokers', async () => (await import('./welcomeHarness')).brokersMock());
vi.mock('../welcome/steps/BrokerageStep', async () => (await import('./welcomeHarness')).brokerageStepMock());
vi.mock('../welcome/steps/ScreenshotDrop', async () => (await import('./welcomeHarness')).screenshotDropMock());
vi.mock('../welcome/steps/HoldingsEntry', async () => (await import('./welcomeHarness')).holdingsEntryMock());
vi.mock('@/pages/ChatAgent/utils/warmWorkspace', async () => (await import('./welcomeHarness')).warmWorkspaceMock());
vi.mock('@/hooks/useMcpServers', async (importOriginal) =>
  (await import('./welcomeHarness')).mcpServersMock(importOriginal),
);
vi.mock('@/hooks/useHomeWorkspace', async () => (await import('./welcomeHarness')).homeWorkspaceMock());
vi.mock('../welcome/marketBrief', async () => (await import('./welcomeHarness')).marketBriefMock());
vi.mock('@/contexts/ThemeContext', async () => (await import('./welcomeHarness')).themeMock());
vi.mock('@/components/ui/use-toast', async () => (await import('./welcomeHarness')).toastMock());
vi.mock('@/pages/Dashboard/utils/api', async (importOriginal) =>
  (await import('./welcomeHarness')).dashboardApiMock(importOriginal),
);
vi.mock('@/hooks/useSyncUserLocale', async () => (await import('./welcomeHarness')).syncUserLocaleMock());
vi.mock('@/hooks/useUser', async () => (await import('./welcomeHarness')).userMock());
vi.mock('../OnboardingProvider', async () => (await import('./welcomeHarness')).onboardingProviderMock());

import {
  answer,
  answered,
  briefSent,
  CHART,
  composer,
  composerSend,
  DRAFT,
  flowRenderer,
  holdWorkspace,
  instruction,
  location,
  mocks,
  mutateAsync,
  primary,
  readBrief,
  resetWelcomeMocks,
  resolveChatWorkspace,
  routeState,
  send,
  toast,
  turn,
  waitForServerTools,
} from './welcomeHarness';
import { WelcomeFlow } from '../welcome/Welcome';
import { BRIEF_PREAMBLE } from '../welcome/kickoff';
import { emptyProfile } from '../welcome/profile';

const renderFlow = flowRenderer(WelcomeFlow);

describe('WelcomeFlow: starting the chat', () => {
  beforeEach(() => {
    resetWelcomeMocks();
  });

  it("Start folds the last card into its answer under the last line and sends nothing of the user's while the chat opens", async () => {
    const release = holdWorkspace();
    renderFlow({ initialStep: 'agent', profile: answered, saved: emptyProfile() });
    fireEvent.click(primary());

    // Where the card was: the answer it took, then the agent taking over.
    // The user wrote nothing, so no message is shown as theirs.
    expect(await screen.findByTestId('welcome-pending')).toBeInTheDocument();
    expect(screen.queryByTestId('welcome-answer-start')).toBeNull();
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(turn('agent')).toContainElement(answer('agent'));
    // How much research, what an answer looks like and when to reach out, in
    // the card's own labels.
    expect(answer('agent')).toHaveTextContent(/^Full research · Summary with a file · When it matters$/);
    expect(turn('agent')).toContainElement(screen.getByTestId('welcome-pending'));
    // The question before it keeps its own answer above.
    expect(answer('style')).toHaveTextContent(/^Fundamental analysis · Growth$/);
    // Nothing above can be changed, or sent again, while it starts.
    expect(screen.queryByTestId('welcome-continue')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Change answer' })).toBeNull();
    expect(composer()).toBeDisabled();
    expect(location()).toBe('/welcome');

    await release();
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(routeState()).toMatchObject({ initialMessage: '', hiddenOpener: true, handoff: true });
    expect(instruction()).toContain('Action: propose. Language: en-US. Brokerage: none.');
  });

  it("carries today's brief to the chat when there is one, and nothing in its place when there is not", async () => {
    mocks.brief = '## Headline: chips lead a broad rally\n\nSemiconductors led the S&P 500 higher.';
    renderFlow({ initialStep: 'agent', profile: answered });
    // Read as the flow opens, so the last question has it to hand.
    await waitFor(() => expect(readBrief).toHaveBeenCalledTimes(1));
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(instruction()).toContain('Action: propose.');
    expect(instruction()).toContain('Market brief: attached.');
    expect(briefSent()).toBe(`${BRIEF_PREAMBLE}\n<market-brief>\n${mocks.brief}\n</market-brief>`);
    // The hand-off takes the one read as the flow opened.
    expect(readBrief).toHaveBeenCalledTimes(1);

    cleanup();
    localStorage.clear();
    mocks.brief = null;
    renderFlow({ initialStep: 'agent', profile: answered });
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(instruction()).toContain('Action: propose.');
    expect(instruction()).not.toContain('Market brief');
    expect(briefSent()).toBeNull();
  });

  it("waits for a fresh brokerage's tools before Start hands off", async () => {
    mocks.connected = [{ label: 'moomoo', b: { name: 'moomoo' }, row: { tool_count: 0 } }];
    let release: (ok: boolean) => void = () => {};
    waitForServerTools.mockImplementation(() => new Promise((resolve) => (release = resolve)));
    renderFlow({ initialStep: 'agent', profile: answered });

    fireEvent.click(primary());
    expect(await screen.findByText('Loading moomoo…')).toBeInTheDocument();
    // The card gives way to the hand-off, with no message of the user's.
    expect(screen.queryByTestId('welcome-answer-start')).toBeNull();
    expect(screen.queryByTestId('welcome-continue')).toBeNull();
    expect(waitForServerTools).toHaveBeenCalledWith(expect.anything(), 'moomoo', {
      timeoutMs: 20_000,
      signal: expect.any(AbortSignal),
    });
    expect(location()).toBe('/welcome');

    await act(async () => release(true));
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(instruction()).toContain('Action: propose. Language: en-US. Brokerage: moomoo.');

    // One already showing its tools has nothing to wait for.
    cleanup();
    localStorage.clear();
    waitForServerTools.mockClear();
    mocks.connected = [{ label: 'moomoo', b: { name: 'moomoo' }, row: { tool_count: 3 } }];
    renderFlow({ initialStep: 'agent', profile: answered });
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(waitForServerTools).not.toHaveBeenCalled();
    expect(instruction()).toContain('Action: propose. Language: en-US. Brokerage: moomoo.');
  });

  it('holds Start and the composer until the broker lists say what is connected', async () => {
    // A reload onto the last turn, before the lists are in.
    mocks.brokersReady = false;
    renderFlow({ initialStep: 'agent', profile: answered });
    expect(primary()).toBeDisabled();
    expect(composer()).toBeDisabled();
    fireEvent.click(primary());
    expect(location()).toBe('/welcome');
    expect(mutateAsync).not.toHaveBeenCalled();

    // A list that failed is all there is to go on: the start goes on without it.
    cleanup();
    localStorage.clear();
    mocks.brokersError = new Error('down');
    renderFlow({ initialStep: 'agent', profile: answered });
    expect(composer()).toBeEnabled();
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(instruction()).toContain('Brokerage: none.');
  });

  it('Skip while it starts goes to the dashboard, drops the draft the save made moot, and opens no chat', async () => {
    mocks.connected = [{ label: 'moomoo', b: { name: 'moomoo' }, row: { tool_count: 0 } }];
    let release: (ok: boolean) => void = () => {};
    waitForServerTools.mockImplementation(() => new Promise((resolve) => (release = resolve)));
    const { onSkip } = renderFlow({ initialStep: 'agent', profile: answered, saved: emptyProfile() });
    fireEvent.click(primary());
    expect(await screen.findByText('Loading moomoo…')).toBeInTheDocument();
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(localStorage.getItem(DRAFT)).not.toBeNull();

    fireEvent.click(screen.getByTestId('welcome-exit'));
    expect(onSkip).toHaveBeenCalledTimes(1);
    expect(location()).toBe('/dashboard');
    expect(localStorage.getItem(DRAFT)).toBeNull();
    // The wait is called off, and its end opens nothing.
    const [, , { signal }] = waitForServerTools.mock.calls[0] as [unknown, string, { signal: AbortSignal }];
    expect(signal.aborted).toBe(true);
    await act(async () => release(true));
    expect(location()).toBe('/dashboard');
    expect(resolveChatWorkspace).toHaveBeenCalledTimes(1);
  });

  it('Skip while the save is still running opens no chat once it lands', async () => {
    let land: () => void = () => {};
    mutateAsync.mockImplementation(() => new Promise((resolve) => (land = () => resolve({}))));
    renderFlow({ initialStep: 'agent', profile: answered, saved: emptyProfile() });
    fireEvent.click(primary());
    expect(mutateAsync).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByTestId('welcome-exit'));
    expect(location()).toBe('/dashboard');
    await act(async () => land());
    expect(location()).toBe('/dashboard');
    expect(resolveChatWorkspace).not.toHaveBeenCalled();
  });

  it("waits for a late brief as long as a brokerage's tools hold the hand-off anyway", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      mocks.connected = [{ label: 'moomoo', b: { name: 'moomoo' }, row: { tool_count: 0 } }];
      let releaseTools: (ok: boolean) => void = () => {};
      waitForServerTools.mockImplementation(() => new Promise((resolve) => (releaseTools = resolve)));
      let land: (brief: string) => void = () => {};
      readBrief.mockImplementationOnce(() => new Promise((resolve) => (land = resolve)));
      renderFlow({ initialStep: 'agent', profile: answered });
      fireEvent.click(primary());
      expect(await screen.findByText('Loading moomoo…')).toBeInTheDocument();

      // Past the brief's own wait, with the tools still to come.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(5_000);
      });
      await act(async () => land('## Late, but in time'));
      await act(async () => releaseTools(true));
      await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
      expect(briefSent()).toBe(`${BRIEF_PREAMBLE}\n<market-brief>\n## Late, but in time\n</market-brief>`);
    } finally {
      vi.useRealTimers();
    }
  });

  it('goes without a brief that is not in once the rest of the hand-off is', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      readBrief.mockImplementationOnce(() => new Promise(() => {}));
      renderFlow({ initialStep: 'agent', profile: answered });
      fireEvent.click(primary());
      expect(await screen.findByTestId('welcome-pending')).toBeInTheDocument();
      expect(location()).toBe('/welcome');

      await act(async () => {
        await vi.advanceTimersByTimeAsync(3_000);
      });
      await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
      expect(instruction()).toContain('Action: propose.');
      expect(briefSent()).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it('a failed save gives a task written in the composer back with its files, and starts nothing', async () => {
    mutateAsync.mockRejectedValue(new Error('boom'));
    composerSend.attachments = [CHART];
    renderFlow({ initialStep: 'agent', profile: answered, saved: emptyProfile() });
    fireEvent.change(composer(), { target: { value: 'Find a cheap semiconductor ETF' } });
    send();

    await waitFor(() => expect(toast).toHaveBeenCalledTimes(1));
    expect(toast.mock.calls[0][0]).toMatchObject({
      description: "Couldn't save your investing profile. Try again.",
    });
    expect(composer()).toHaveValue('Find a cheap semiconductor ETF');
    expect(composerSend.givenBack).toEqual([CHART]);
    expect(composer()).toBeEnabled();
    expect(screen.queryByTestId('welcome-answer-start')).toBeNull();
    expect(location()).toBe('/welcome');
    expect(resolveChatWorkspace).not.toHaveBeenCalled();
  });

  describe('as a conversation', () => {
    it("shows each passed turn's answer in the user's bubble, in the card's own labels", () => {
      renderFlow({ profile: { ...answered, instruments: ['option'] } });
      // The brokerage turn above the first question, passed over.
      expect(answer('brokerage')).toHaveTextContent(/^Skipped$/);

      fireEvent.click(primary());
      expect(answer('experience')).toHaveTextContent(/^Individual investor · 3–10 years · Options$/);

      fireEvent.click(screen.getByRole('radio', { name: /Moderate/ }));
      fireEvent.click(screen.getByTestId('welcome-horizon-months'));
      fireEvent.click(primary());
      expect(answer('risk')).toHaveTextContent(/^Moderate · Months$/);
      // The agent's line stays above the answer it asked for.
      expect(screen.getByText(/^How much risk are you comfortable with\?/)).toBeInTheDocument();

      fireEvent.click(screen.getByRole('checkbox', { name: 'Hong Kong' }));
      fireEvent.click(screen.getByRole('checkbox', { name: 'Researching companies' }));
      fireEvent.click(primary());
      expect(answer('markets')).toHaveTextContent(/^United States and Hong Kong · Researching companies$/);
    });

    it('greets the user by first name on the first turn, and keeps the greeting there once it is answered', () => {
      renderFlow({ initialStep: 'brokerage', userName: '  Ada  Lovelace ' });
      const hello = screen.getByRole('heading', { level: 1 });
      expect(hello).toHaveTextContent(/^Hi Ada\.$/);
      expect(turn('brokerage')).toContainElement(hello);
      expect(turn('brokerage')).toHaveTextContent("I'm your LangAlpha Chief of Staff.");

      fireEvent.click(screen.getByRole('button', { name: 'stand-in elsewhere' }));
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/^Hi Ada\.$/);
      expect(within(turn('elsewhere')).queryByRole('heading', { level: 1 })).toBeNull();

      // No name on the account: a plain greeting, since the line under it names the product.
      cleanup();
      renderFlow({ initialStep: 'brokerage' });
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/^Hi there\.$/);

      // Nor is the name a self-hosted install gives its user until they set one.
      cleanup();
      renderFlow({ initialStep: 'brokerage', userName: 'Local User' });
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/^Hi there\.$/);
    });

    it('keeps the composer shut until the last question is answered, where a task written there starts the chat', async () => {
      composerSend.options = { model: 'model-x', reasoningEffort: 'high', fastMode: false, subagentsAllowed: false };
      renderFlow({
        initialStep: 'markets',
        profile: { ...answered, styles: [], proactivity: null },
        saved: emptyProfile(),
      });
      // An earlier turn, answered or not, keeps it shut.
      expect(primary()).toBeEnabled();
      expect(composer()).toBeDisabled();
      expect(composer()).toHaveAttribute('placeholder', "You can message me once we're set up");

      fireEvent.click(primary());
      expect(turn('style')).toBeInTheDocument();
      expect(primary()).toBeDisabled();
      expect(composer()).toBeDisabled();
      // The question that used to be the last one, answered, still keeps it shut.
      fireEvent.click(screen.getByRole('checkbox', { name: 'Value' }));
      expect(primary()).toBeEnabled();
      expect(composer()).toBeDisabled();
      expect(composer()).toHaveAttribute('placeholder', "You can message me once we're set up");

      // The last question, still unanswered.
      fireEvent.click(primary());
      expect(turn('agent')).toBeInTheDocument();
      expect(primary()).toBeDisabled();
      expect(composer()).toBeDisabled();
      expect(composer()).toHaveAttribute('placeholder', "You can message me once we're set up");

      fireEvent.click(screen.getByTestId('welcome-proactivity-on_request'));
      expect(composer()).toBeEnabled();
      expect(composer()).toHaveAttribute('placeholder', 'Add a question or context to start with (optional)');

      // Nothing written sends nothing, and saves nothing.
      fireEvent.change(composer(), { target: { value: '   ' } });
      send();
      expect(turn('agent')).toBeInTheDocument();
      expect(screen.queryByTestId('welcome-answer-start')).toBeNull();
      expect(mutateAsync).not.toHaveBeenCalled();

      composerSend.attachments = [CHART];
      fireEvent.change(composer(), { target: { value: '  How exposed am I to rate cuts? ' } });
      send();
      await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
      // The answers saved first, as Start saves them.
      expect(mutateAsync).toHaveBeenCalledTimes(1);
      expect(mutateAsync.mock.calls[0][0]).toMatchObject({
        investment_preference: { styles: ['value'] },
        agent_preference: { proactivity: 'on_request' },
      });
      expect(routeState()).toMatchObject({
        workspaceId: 'home-1',
        // The task as the user's message, trimmed as a send trims it.
        initialMessage: 'How exposed am I to rate cuts?',
        // The composer's picks, as a send from Home carries them.
        additionalContext: [
          {
            type: 'skills',
            name: 'onboarding',
            instruction: expect.stringContaining('Action: first_task. Language: en-US. Brokerage: none.'),
          },
          { type: 'image', data: 'data:image/png;base64,AAAA', description: 'chart.png' },
        ],
        attachmentMeta: [
          { name: 'chart.png', type: 'image/png', size: 4, preview: null, dataUrl: 'data:image/png;base64,AAAA' },
        ],
        model: 'model-x',
        reasoningEffort: 'high',
        subagentsAllowed: false,
        handoff: true,
      });
      // Titled from the message, as any other.
      expect(routeState()).not.toHaveProperty('hiddenOpener');
      expect(routeState()).not.toHaveProperty('threadTitle');
      expect(localStorage.getItem(DRAFT)).toBeNull();
    });

    it('Start takes a task written in the composer and not sent, as Enter would', async () => {
      composerSend.options = { model: 'model-x', reasoningEffort: 'high', fastMode: false };
      renderFlow({ initialStep: 'agent', profile: answered });
      fireEvent.change(composer(), { target: { value: 'Is my tech exposure too concentrated?' } });
      fireEvent.click(primary());
      expect(await screen.findByTestId('welcome-answer-start')).toHaveTextContent(
        /^Is my tech exposure too concentrated\?$/,
      );
      await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
      expect(routeState()).toMatchObject({ initialMessage: 'Is my tech exposure too concentrated?', model: 'model-x' });
      expect(instruction()).toContain('Action: first_task.');
    });

    it('Start with nothing written asks for suggestions on the model picked in the composer', async () => {
      composerSend.options = { model: 'model-x', reasoningEffort: 'high', fastMode: false };
      renderFlow({ initialStep: 'agent', profile: answered });
      fireEvent.click(primary());
      await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
      expect(screen.queryByTestId('welcome-answer-start')).toBeNull();
      expect(routeState()).toMatchObject({
        initialMessage: '',
        hiddenOpener: true,
        model: 'model-x',
        reasoningEffort: 'high',
      });
      expect(instruction()).toContain('Action: propose.');
    });

    it('a file sent from the composer with nothing written goes with the suggestions rather than being dropped', async () => {
      composerSend.attachments = [CHART];
      renderFlow({ initialStep: 'agent', profile: answered });
      send();
      await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
      expect(screen.queryByTestId('welcome-answer-start')).toBeNull();
      // For the agent to read; a turn with no message draws no bubble to
      // show it under.
      expect(routeState()).toMatchObject({
        initialMessage: '',
        hiddenOpener: true,
        additionalContext: [
          { type: 'skills', name: 'onboarding', instruction: expect.stringContaining('Action: propose.') },
          { type: 'image', data: 'data:image/png;base64,AAAA', description: 'chart.png' },
        ],
        attachmentMeta: null,
      });
    });

    it("waits for a fresh brokerage's tools before a written task too, showing it as sent", async () => {
      mocks.connected = [{ label: 'moomoo', b: { name: 'moomoo' }, row: { tool_count: 0 } }];
      let release: (ok: boolean) => void = () => {};
      waitForServerTools.mockImplementation(() => new Promise((resolve) => (release = resolve)));
      renderFlow({ initialStep: 'agent', profile: answered });

      fireEvent.change(composer(), { target: { value: 'Which of my positions is most exposed to rates?' } });
      send();
      // The task, as written, in place of the request to suggest a start.
      expect(await screen.findByTestId('welcome-answer-start')).toHaveTextContent(
        /^Which of my positions is most exposed to rates\?$/,
      );
      expect(await screen.findByText('Loading moomoo…')).toBeInTheDocument();
      // Nothing above can be changed, or sent again, while it starts.
      expect(screen.queryByTestId('welcome-continue')).toBeNull();
      expect(screen.queryByRole('button', { name: 'Change answer' })).toBeNull();
      expect(composer()).toBeDisabled();
      expect(location()).toBe('/welcome');

      await act(async () => release(true));
      await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
      expect(routeState()).toMatchObject({ initialMessage: 'Which of my positions is most exposed to rates?' });
      expect(instruction()).toContain('Action: first_task. Language: en-US. Brokerage: moomoo.');
    });

    it('a start that cannot open the chat gives the written task back to the composer, and says so', async () => {
      resolveChatWorkspace.mockRejectedValue(new Error('down'));
      renderFlow({ initialStep: 'agent', profile: answered });
      fireEvent.change(composer(), { target: { value: 'Find a cheap semiconductor ETF' } });
      send();

      await waitFor(() => expect(toast).toHaveBeenCalledTimes(1));
      expect(toast.mock.calls[0][0]).toMatchObject({ description: "Couldn't open the chat. Try again." });
      // As before the send: the text to send again, the card to start from.
      expect(composer()).toHaveValue('Find a cheap semiconductor ETF');
      expect(composer()).toBeEnabled();
      expect(primary()).toBeEnabled();
      expect(turn('agent')).toBeInTheDocument();
      expect(screen.queryByTestId('welcome-answer-start')).toBeNull();
      expect(location()).toBe('/welcome');
      expect(localStorage.getItem(DRAFT)).not.toBeNull();
    });

    it('a Start that cannot open the chat puts the card back, says so, and can be pressed again', async () => {
      resolveChatWorkspace.mockRejectedValueOnce(new Error('down'));
      renderFlow({ initialStep: 'agent', profile: answered, saved: emptyProfile() });
      fireEvent.click(primary());

      await waitFor(() => expect(toast).toHaveBeenCalledTimes(1));
      expect(toast.mock.calls[0][0]).toMatchObject({ description: "Couldn't open the chat. Try again." });
      expect(screen.queryByTestId('welcome-answer-start')).toBeNull();
      expect(primary()).toBeEnabled();
      expect(location()).toBe('/welcome');

      // Saved already, so the second press only opens the chat.
      fireEvent.click(primary());
      await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
      expect(mutateAsync).toHaveBeenCalledTimes(1);
      expect(routeState()).toMatchObject({ hiddenOpener: true });
      expect(instruction()).toContain('Action: propose.');
    });
  });
});
