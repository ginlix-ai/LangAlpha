/**
 * The welcome flow's questions: what each asks for before it goes on, what
 * the first walk-through and Settings' editor save, and Enter, Skip and a
 * failed save along the way.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';

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
  answered,
  composer,
  DRAFT,
  flowRenderer,
  location,
  mutateAsync,
  pencil,
  primary,
  readBrief,
  resetWelcomeMocks,
  resolveChatWorkspace,
  routeState,
  toast,
  turn,
  waitForServerTools,
  warmWorkspace,
} from './welcomeHarness';
import { WelcomeFlow } from '../welcome/Welcome';
import { welcomeSteps } from '../welcome/paths';
import { emptyProfile } from '../welcome/profile';

const renderFlow = flowRenderer(WelcomeFlow);

describe('WelcomeFlow: the questions and their save', () => {
  beforeEach(() => {
    resetWelcomeMocks();
  });

  it('walks the questions, saves them once, then hands Home the conversation to suggest where to start', async () => {
    renderFlow();
    expect(turn('experience')).toBeInTheDocument();
    expect(primary()).toBeDisabled();
    fireEvent.click(screen.getByRole('radio', { name: /Individual investor/ }));
    fireEvent.click(screen.getByRole('radio', { name: '3–10 years' }));
    fireEvent.click(screen.getByRole('checkbox', { name: 'ETFs' }));

    fireEvent.click(primary());
    expect(turn('risk')).toBeInTheDocument();
    expect(primary()).toBeDisabled();
    fireEvent.click(screen.getByRole('radio', { name: /Aggressive/ }));
    fireEvent.click(screen.getByRole('radio', { name: 'Years' }));

    fireEvent.click(primary());
    expect(turn('markets')).toBeInTheDocument();
    expect(primary()).toBeDisabled();
    fireEvent.click(screen.getByRole('checkbox', { name: 'Hong Kong' }));
    fireEvent.click(screen.getByRole('checkbox', { name: 'United States' }));
    fireEvent.click(screen.getByRole('checkbox', { name: 'Planning trades' }));

    fireEvent.click(primary());
    expect(turn('style')).toBeInTheDocument();
    expect(primary()).toBeDisabled();
    fireEvent.click(screen.getByRole('checkbox', { name: 'Technical analysis' }));
    fireEvent.click(screen.getByRole('checkbox', { name: 'Value' }));
    expect(primary()).toHaveTextContent('Continue');

    fireEvent.click(primary());
    expect(turn('agent')).toBeInTheDocument();
    expect(primary()).toBeDisabled();
    fireEvent.click(screen.getByTestId('welcome-depth-light'));
    // One answer each: a second pick takes the first one's place.
    fireEvent.click(screen.getByTestId('welcome-deliverable-visual'));
    fireEvent.click(screen.getByTestId('welcome-deliverable-markdown'));
    expect(screen.getByRole('radio', { name: /^Charts and reports/ })).not.toBeChecked();
    fireEvent.click(screen.getByTestId('welcome-proactivity-proactive'));
    expect(mutateAsync).not.toHaveBeenCalled();
    // The last question's way on is the start of the chat.
    expect(primary()).toHaveTextContent('Start');
    fireEvent.click(primary());

    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    // Only what was answered: the lists left empty were empty on the server.
    // The answer leads the output format's ranking.
    expect(mutateAsync).toHaveBeenCalledWith({
      risk_preference: { risk_level: 'aggressive' },
      investment_preference: {
        investor_type: 'individual',
        experience: '3_10y',
        help_with: ['trading'],
        horizon: 'years',
        markets: ['hk', 'us'],
        asset_classes: ['etf'],
        approaches: ['technical'],
        styles: ['value'],
      },
      agent_preference: {
        research_depth: 'light',
        proactivity: 'proactive',
        output_format: ['markdown', 'text', 'visual'],
      },
    });

    // Nothing written in the composer: the agent is asked where to start, as
    // a turn with no message of the user's under a title of its own, and with
    // nothing connected, no brokerages used and no brief, nothing else. The
    // state is Home's, for the chat to send as it sends Home's.
    expect(routeState()).toEqual({
      workspaceId: 'home-1',
      initialMessage: '',
      additionalContext: [
        {
          type: 'skills',
          name: 'onboarding',
          instruction:
            'Onboarding kickoff. Action: propose. Language: en-US. Brokerage: none. ' +
            "This is the user's first conversation, right after onboarding. " +
            'Their investing profile is in your system prompt; do not ask for it again.',
        },
      ],
      attachmentMeta: null,
      fastMode: false,
      hiddenOpener: true,
      threadTitle: 'Getting started',
      handoff: true,
    });
    expect(warmWorkspace).toHaveBeenCalledWith('home-1', expect.anything());
    expect(localStorage.getItem(DRAFT)).toBeNull();
  });

  // What they do and for how long set how much the agent explains, so both
  // are asked for; an investor who buys funds now and then has no list of
  // instruments to give.
  it('asks who the user is and for how long before going on, and leaves what they want to trade optional', () => {
    renderFlow();
    fireEvent.click(screen.getByTestId('welcome-investor-student'));
    expect(primary()).toBeDisabled();
    fireEvent.click(screen.getByTestId('welcome-experience-under_1y'));
    expect(primary()).toBeEnabled();
    // Picked and taken back again: the list is optional, not merely unread.
    fireEvent.click(screen.getByRole('checkbox', { name: 'Options' }));
    fireEvent.click(screen.getByRole('checkbox', { name: 'Options' }));
    expect(primary()).toBeEnabled();
    fireEvent.click(primary());
    expect(turn('risk')).toBeInTheDocument();
  });

  it('asks how long the user holds beside the risk level, and needs both', () => {
    renderFlow({ initialStep: 'risk', profile: { ...answered, horizon: null } });
    expect(screen.getByRole('radio', { name: /Growth/ })).toBeChecked();
    expect(screen.getByRole('heading', { name: 'How long do you usually hold a position?' })).toBeInTheDocument();
    expect(primary()).toBeDisabled();
    fireEvent.click(screen.getByTestId('welcome-horizon-intraday'));
    expect(primary()).toBeEnabled();
    fireEvent.click(primary());
    expect(turn('markets')).toBeInTheDocument();
  });

  it('asks for an approach as well as a style before it goes on, and saves them from the last question', async () => {
    renderFlow({ initialStep: 'style', profile: { ...answered, approaches: [] }, saved: emptyProfile() });
    expect(screen.getByRole('checkbox', { name: 'Growth' })).toBeChecked();
    expect(primary()).toBeDisabled();
    // Enter does not get past it either.
    await act(async () => fireEvent.keyDown(document.body, { key: 'Enter' }));
    expect(turn('style')).toBeInTheDocument();
    expect(mutateAsync).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('checkbox', { name: 'Macro trends' }));
    expect(primary()).toBeEnabled();
    // And a style as well as an approach.
    fireEvent.click(screen.getByRole('checkbox', { name: 'Growth' }));
    expect(primary()).toBeDisabled();
    fireEvent.click(screen.getByRole('checkbox', { name: 'Passive and index' }));
    // No longer the last question: its way on goes on, and saves nothing.
    expect(primary()).toHaveTextContent('Continue');
    fireEvent.click(primary());
    expect(turn('agent')).toBeInTheDocument();
    expect(mutateAsync).not.toHaveBeenCalled();

    fireEvent.click(primary());
    await waitFor(() => expect(mutateAsync).toHaveBeenCalledTimes(1));
    expect(mutateAsync.mock.calls[0][0]).toMatchObject({
      investment_preference: { approaches: ['macro'], styles: ['index'] },
    });
  });

  // How much research an answer gets, what it usually looks like and when the
  // agent reaches out are all read on every turn, so none is left to a default.
  it('asks how much research, what an answer looks like and when to reach out before Start lights up', async () => {
    renderFlow({
      initialStep: 'agent',
      profile: { ...answered, depth: null, deliverable: null, proactivity: null },
      saved: emptyProfile(),
    });
    expect(screen.getByRole('heading', { name: 'What should an answer usually look like?' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'When should I reach out to you?' })).toBeInTheDocument();
    // Three depths, labelled by the agent's line that asks it.
    expect(within(turn('agent')).getAllByRole('radiogroup')[0]).toHaveAccessibleName(
      /^Last, how should I answer your questions by default\?/,
    );
    expect(primary()).toHaveTextContent('Start');
    expect(primary()).toBeDisabled();
    expect(composer()).toBeDisabled();

    fireEvent.click(screen.getByTestId('welcome-depth-adaptive'));
    expect(primary()).toBeDisabled();
    fireEvent.click(screen.getByTestId('welcome-deliverable-text'));
    expect(primary()).toBeDisabled();
    // Enter does not get past it with one still to answer.
    await act(async () => fireEvent.keyDown(document.body, { key: 'Enter' }));
    expect(mutateAsync).not.toHaveBeenCalled();
    expect(location()).toBe('/welcome');

    fireEvent.click(screen.getByTestId('welcome-proactivity-on_request'));
    expect(primary()).toBeEnabled();
    expect(composer()).toBeEnabled();
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect((mutateAsync.mock.calls[0][0] as { agent_preference: object }).agent_preference).toEqual({
      research_depth: 'adaptive',
      proactivity: 'on_request',
      output_format: ['text', 'markdown', 'visual'],
    });
  });

  it('edits from Settings: the questions alone, prefilled, only the change saved back', async () => {
    expect(welcomeSteps('edit')).toEqual(['experience', 'risk', 'markets', 'style', 'agent']);
    const { onSkip } = renderFlow({ mode: 'edit', profile: answered });
    // Opened at the first question with the profile as it stands: no greeting,
    // and no brokerage turn above it.
    expect(turn('experience')).toHaveTextContent("Here's your investing profile. Change any answer, then save.");
    expect(screen.queryByRole('heading', { level: 1 })).toBeNull();
    expect(screen.queryByTestId('welcome-answer-brokerage')).toBeNull();
    expect(screen.getByTestId('welcome-exit')).toHaveTextContent('Cancel');
    expect(screen.getByRole('radio', { name: /Individual investor/ })).toBeChecked();
    fireEvent.click(primary());
    expect(screen.getByRole('radio', { name: /Growth/ })).toBeChecked();
    fireEvent.click(primary());
    fireEvent.click(screen.getByRole('checkbox', { name: 'Japan' }));
    fireEvent.click(primary());
    // The style question goes on to the last one, which saves.
    expect(primary()).toHaveTextContent('Continue');
    fireEvent.click(primary());
    expect(turn('agent')).toBeInTheDocument();
    expect(primary()).toHaveTextContent('Save');
    fireEvent.click(primary());

    await waitFor(() => expect(location()).toBe('/settings?tab=preferences'));
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(mutateAsync).toHaveBeenCalledWith({ investment_preference: { markets: ['us', 'jp'] } });
    // Nothing is handed to the agent, so nothing is readied for it.
    expect(warmWorkspace).not.toHaveBeenCalled();
    expect(readBrief).not.toHaveBeenCalled();
    expect(onSkip).not.toHaveBeenCalled();
    expect(localStorage.getItem(DRAFT)).toBeNull();
  });

  it('edits how the agent works from Settings, prefilled, and saves back only the key that changed', async () => {
    renderFlow({ mode: 'edit', profile: answered });
    for (const id of ['experience', 'risk', 'markets', 'style']) {
      expect(turn(id)).toBeInTheDocument();
      fireEvent.click(primary());
    }
    expect(turn('agent')).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /^Full research/ })).toBeChecked();
    // Read off the output format Settings keeps, as the save writes it.
    expect(screen.getByRole('radio', { name: /^Summary with a file/ })).toBeChecked();
    expect(screen.getByRole('radio', { name: /^When it matters/ })).toBeChecked();

    fireEvent.click(screen.getByTestId('welcome-depth-adaptive'));
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/settings?tab=preferences'));
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(mutateAsync).toHaveBeenCalledWith({ agent_preference: { research_depth: 'adaptive' } });
  });

  it("a new answer from Settings' editor writes the ranking it leads", async () => {
    renderFlow({ mode: 'edit', initialStep: 'agent', profile: answered });
    fireEvent.click(screen.getByTestId('welcome-deliverable-visual'));
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/settings?tab=preferences'));
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(mutateAsync).toHaveBeenCalledWith({ agent_preference: { output_format: ['visual', 'markdown', 'text'] } });
  });

  it('edits with no composer, since Settings hands nothing to the agent', () => {
    renderFlow({ mode: 'edit', initialStep: 'agent', profile: answered });
    expect(turn('agent')).toBeInTheDocument();
    expect(screen.queryByTestId('composer')).toBeNull();
  });

  it('a save that changes nothing writes nothing', async () => {
    renderFlow({ mode: 'edit', initialStep: 'agent', profile: answered });
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/settings?tab=preferences'));
    expect(mutateAsync).not.toHaveBeenCalled();
  });

  it('keeps a sector the list does not name: shown picked, and left alone by a save', async () => {
    renderFlow({ mode: 'edit', initialStep: 'style', profile: { ...answered, sectors: ['biotech', 'clean_energy'] } });
    expect(screen.getByRole('checkbox', { name: 'biotech' })).toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'clean energy' })).toBeChecked();
    fireEvent.click(screen.getByRole('checkbox', { name: 'Value' }));
    fireEvent.click(primary());
    fireEvent.click(primary());

    await waitFor(() => expect(mutateAsync).toHaveBeenCalledTimes(1));
    expect(mutateAsync).toHaveBeenCalledWith({ investment_preference: { styles: ['growth', 'value'] } });
  });

  it('a sector the list does not name can be removed, and its chip stays to put it back', async () => {
    renderFlow({ mode: 'edit', initialStep: 'style', profile: { ...answered, sectors: ['biotech', 'energy'] } });
    fireEvent.click(screen.getByRole('checkbox', { name: 'biotech' }));
    expect(screen.getByRole('checkbox', { name: 'biotech' })).not.toBeChecked();
    fireEvent.click(primary());
    fireEvent.click(primary());
    await waitFor(() => expect(mutateAsync).toHaveBeenCalledTimes(1));
    expect(mutateAsync).toHaveBeenCalledWith({ investment_preference: { sectors: ['energy'] } });
  });

  it('Skip passes on the flow and keeps the draft for the banner to resume', () => {
    const { onSkip } = renderFlow({ initialStep: 'markets', profile: answered });
    fireEvent.click(screen.getByTestId('welcome-exit'));
    expect(onSkip).toHaveBeenCalledTimes(1);
    expect(location()).toBe('/dashboard');
    expect(JSON.parse(localStorage.getItem(DRAFT) ?? 'null')).toMatchObject({ step: 'markets' });
  });

  it('Enter continues, except from a control that takes Enter for itself', () => {
    renderFlow({ initialStep: 'markets', profile: answered });
    fireEvent.keyDown(pencil('risk'), { key: 'Enter' });
    expect(turn('markets')).toBeInTheDocument();
    fireEvent.keyDown(composer(), { key: 'Enter' });
    expect(turn('markets')).toBeInTheDocument();
    fireEvent.keyDown(document.body, { key: 'Enter', shiftKey: true });
    expect(turn('markets')).toBeInTheDocument();
    fireEvent.keyDown(document.body, { key: 'Enter' });
    expect(turn('style')).toBeInTheDocument();
  });

  it('a failed save stays on the turn, says so, and starts nothing', async () => {
    mutateAsync.mockRejectedValue(new Error('boom'));
    renderFlow({ initialStep: 'agent', profile: answered, saved: emptyProfile() });
    fireEvent.click(primary());
    await waitFor(() => expect(toast).toHaveBeenCalledTimes(1));
    expect(toast.mock.calls[0][0]).toMatchObject({
      description: "Couldn't save your investing profile. Try again.",
    });
    // The card as it was, Start there to try again.
    expect(turn('agent')).toBeInTheDocument();
    expect(primary()).toHaveTextContent('Start');
    expect(primary()).toBeEnabled();
    expect(screen.queryByTestId('welcome-answer-start')).toBeNull();

    // Enter tries the save again rather than moving on to the kickoff.
    await act(async () => fireEvent.keyDown(document.body, { key: 'Enter' }));
    await waitFor(() => expect(mutateAsync).toHaveBeenCalledTimes(2));
    expect(turn('agent')).toBeInTheDocument();
    expect(location()).toBe('/welcome');
    expect(waitForServerTools).not.toHaveBeenCalled();
    expect(resolveChatWorkspace).not.toHaveBeenCalled();
    expect(localStorage.getItem(DRAFT)).not.toBeNull();
  });

  // The save carries the answers as Start found them, so a pick made while it
  // is out would show on the card and never be saved. jsdom keeps no input
  // from an inert subtree, so the attribute is what is read here.
  it('shuts the card to input while its answers save, and opens it again when the save fails', async () => {
    let refuse: () => void = () => {};
    mutateAsync.mockImplementation(() => new Promise((_resolve, reject) => (refuse = () => reject(new Error('boom')))));
    renderFlow({ initialStep: 'agent', profile: answered, saved: emptyProfile() });
    const shut = () => screen.getByTestId('welcome-depth-adaptive').closest('[inert]');
    expect(shut()).toBeNull();

    fireEvent.click(primary());
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(shut()).not.toBeNull();
    expect(shut()).toContainElement(primary());

    await act(async () => refuse());
    await waitFor(() => expect(toast).toHaveBeenCalledTimes(1));
    expect(shut()).toBeNull();
    expect(primary()).toBeEnabled();
  });

  it("shuts Settings' editor's last card to input while it saves", async () => {
    let land: () => void = () => {};
    mutateAsync.mockImplementation(() => new Promise((resolve) => (land = () => resolve({}))));
    renderFlow({ mode: 'edit', initialStep: 'agent', profile: answered });
    fireEvent.click(screen.getByTestId('welcome-depth-adaptive'));
    fireEvent.click(primary());
    expect(mutateAsync).toHaveBeenCalledWith({ agent_preference: { research_depth: 'adaptive' } });
    expect(screen.getByTestId('welcome-depth-light').closest('[inert]')).not.toBeNull();

    await act(async () => land());
    await waitFor(() => expect(location()).toBe('/settings?tab=preferences'));
  });
});
