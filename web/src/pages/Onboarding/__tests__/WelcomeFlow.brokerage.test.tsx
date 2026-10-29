/**
 * The welcome flow's brokerage screen with nothing connected: the brokerages
 * the user names instead, the way they bring their holdings in, and what of
 * it is saved and sent with the kickoff.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { cleanup, fireEvent, screen, waitFor, within } from '@testing-library/react';

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
  flowRenderer,
  holdWorkspace,
  instruction,
  location,
  mocks,
  mutateAsync,
  primary,
  resetWelcomeMocks,
  revisit,
  routeState,
  turn,
  waitForServerTools,
} from './welcomeHarness';
import { WelcomeFlow } from '../welcome/Welcome';
import { emptyProfile, type InvestingProfile } from '../welcome/profile';

const renderFlow = flowRenderer(WelcomeFlow);

describe('WelcomeFlow: the brokerage screen with nothing connected', () => {
  beforeEach(() => {
    resetWelcomeMocks();
  });

  const elsewhere = () => fireEvent.click(screen.getByRole('button', { name: 'stand-in elsewhere' }));

  /** Through the second half, then the questions, to the last one. */
  function walkToLast(pick: () => void = () => {}) {
    renderFlow({ initialStep: 'brokerage', profile: answered, saved: emptyProfile() });
    elsewhere();
    pick();
    fireEvent.click(primary());
    for (let i = 0; i < 4; i++) fireEvent.click(primary());
    expect(turn('agent')).toBeInTheDocument();
  }

  /** Through to the last question, then Start, until the chat opens. */
  async function startFromLast(pick?: () => void) {
    walkToLast(pick);
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
  }

  it('has no Continue on the brokerage card until one is connected: the way on is the card itself', () => {
    renderFlow({ initialStep: 'brokerage' });
    expect(screen.getByTestId('brokerage-step')).toBeInTheDocument();
    expect(screen.queryByTestId('welcome-continue')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Continue' })).toBeNull();
    // The first turn: nothing answered above it to go back to.
    expect(screen.queryByRole('button', { name: 'Change answer' })).toBeNull();

    cleanup();
    mocks.connected = [{ label: 'moomoo', b: { name: 'moomoo' }, row: { tool_count: 3 } }];
    renderFlow({ initialStep: 'brokerage' });
    expect(primary()).toHaveTextContent('Continue');
    fireEvent.click(primary());
    expect(turn('experience')).toBeInTheDocument();
    expect(answer('brokerage')).toHaveTextContent('Connected moomoo');
  });

  it('goes on from the elsewhere turn with nothing picked, and the pencil on an answer returns to its turn, answer kept', () => {
    renderFlow({ initialStep: 'brokerage' });
    elsewhere();
    expect(turn('elsewhere')).toBeInTheDocument();
    expect(answer('brokerage')).toHaveTextContent('Continue without connecting');
    expect(primary()).toBeEnabled();

    fireEvent.click(primary());
    expect(turn('experience')).toBeInTheDocument();

    // The pencil on the elsewhere answer goes back to it while nothing is
    // connected, and what came after is asked again once it is answered...
    revisit('elsewhere');
    expect(turn('elsewhere')).toBeInTheDocument();
    expect(screen.queryByTestId('welcome-turn-experience')).toBeNull();
    fireEvent.click(screen.getByRole('checkbox', { name: 'Fidelity' }));
    fireEvent.click(primary());
    expect(turn('experience')).toBeInTheDocument();

    // ...with the answer still picked on a second visit...
    revisit('elsewhere');
    expect(screen.getByRole('checkbox', { name: 'Fidelity' })).toBeChecked();
    // ...and the one on the brokerage answer goes back to the cards.
    revisit('brokerage');
    expect(screen.getByTestId('brokerage-step')).toBeInTheDocument();
    expect(screen.queryByTestId('welcome-answer-brokerage')).toBeNull();
    expect(screen.queryByTestId('welcome-continue')).toBeNull();
  });

  it('offers screenshots first and search beside it, with neither picked to begin with', () => {
    renderFlow({ initialStep: 'brokerage' });
    elsewhere();
    const panel = (way: string) => screen.queryByTestId(`welcome-method-${way}-panel`);
    const picked = () => screen.queryAllByRole('radio').filter((r) => (r as HTMLInputElement).checked);
    expect(picked()).toHaveLength(0);
    expect(panel('screenshot')).toBeNull();
    expect(panel('search')).toBeNull();

    fireEvent.click(screen.getByRole('radio', { name: /Upload screenshots/ }));
    expect(screen.getByTestId('welcome-method-screenshot')).toHaveAttribute('data-state', 'checked');
    expect(panel('screenshot')).toHaveAttribute('data-state', 'open');
    expect(panel('search')).toHaveAttribute('data-state', 'closed');
    expect(screen.getByRole('button', { name: 'stand-in add screenshot' })).toBeInTheDocument();

    // The way not picked stays on the page, unseen and out of reach, so
    // switching never moves what is around it.
    fireEvent.click(screen.getByRole('radio', { name: /Search and add/ }));
    expect(panel('search')).toHaveAttribute('data-state', 'open');
    expect(panel('screenshot')).toHaveAttribute('data-state', 'closed');
    expect(panel('screenshot')).toHaveAttribute('aria-hidden', 'true');
    expect(screen.queryByRole('button', { name: 'stand-in add screenshot' })).toBeNull();

    // Picking the picked way again puts it down, and the panels go with it.
    fireEvent.click(screen.getByRole('radio', { name: /Search and add/ }));
    expect(picked()).toHaveLength(0);
    expect(panel('screenshot')).toBeNull();
    expect(panel('search')).toBeNull();
  });

  it('continues with no way picked, and answers with just the brokerages', () => {
    renderFlow({ initialStep: 'brokerage', profile: answered });
    elsewhere();
    fireEvent.click(screen.getByRole('checkbox', { name: 'Fidelity' }));
    fireEvent.click(screen.getByRole('checkbox', { name: 'Webull' }));
    fireEvent.click(primary());
    expect(answer('elsewhere')).toHaveTextContent(/^Fidelity and Webull$/);
  });

  it('answers the turn as skipped while no brokerage is picked, and with the ones picked and how', () => {
    renderFlow({ initialStep: 'brokerage', profile: answered });
    elsewhere();
    fireEvent.click(primary());
    expect(answer('elsewhere')).toHaveTextContent(/^Skipped$/);

    revisit('elsewhere');
    fireEvent.click(screen.getByRole('checkbox', { name: 'Fidelity' }));
    fireEvent.click(primary());
    // No way picked: just the brokerage.
    expect(answer('elsewhere')).toHaveTextContent(/^Fidelity$/);

    revisit('elsewhere');
    fireEvent.click(screen.getByRole('radio', { name: /Upload screenshots/ }));
    fireEvent.click(screen.getByRole('button', { name: 'stand-in add screenshot' }));
    fireEvent.click(primary());
    expect(answer('elsewhere')).toHaveTextContent(/^Fidelity · 1 screenshot$/);

    revisit('elsewhere');
    fireEvent.click(screen.getByRole('radio', { name: /Search and add/ }));
    fireEvent.click(primary());
    expect(answer('elsewhere')).toHaveTextContent(/^Fidelity · I'll add them by search$/);
  });

  it('keeps "No account yet" apart from the other brokerages, each clearing the other', () => {
    renderFlow({ initialStep: 'brokerage' });
    elsewhere();
    const box = (name: string) => screen.getByRole('checkbox', { name });
    fireEvent.click(box('Fidelity'));
    fireEvent.click(box('Webull'));
    expect(box('Fidelity')).toBeChecked();
    expect(box('Webull')).toBeChecked();

    fireEvent.click(box('No account yet'));
    expect(box('No account yet')).toBeChecked();
    expect(box('Fidelity')).not.toBeChecked();
    expect(box('Webull')).not.toBeChecked();

    fireEvent.click(box('Webull'));
    expect(box('Webull')).toBeChecked();
    expect(box('No account yet')).not.toBeChecked();
  });

  it('points a picked brokerage that can be connected back at its card', () => {
    mocks.rows = [
      { b: { name: 'ibkr' }, label: 'Interactive Brokers', connected: false },
      { b: { name: 'moomoo' }, label: 'moomoo', connected: true },
    ];
    renderFlow({ initialStep: 'brokerage' });
    elsewhere();
    expect(screen.queryByTestId('welcome-elsewhere-connectable')).toBeNull();
    // A brokerage already connected has no card to go back to.
    fireEvent.click(screen.getByRole('checkbox', { name: 'moomoo' }));
    expect(screen.queryByTestId('welcome-elsewhere-connectable')).toBeNull();
    fireEvent.click(screen.getByRole('checkbox', { name: 'Interactive Brokers' }));
    const hint = screen.getByTestId('welcome-elsewhere-connectable');
    expect(hint).toHaveTextContent('You can connect Interactive Brokers so the agent can read your holdings directly');

    fireEvent.click(within(hint).getByRole('button', { name: 'Connect instead' }));
    expect(screen.getByTestId('brokerage-step')).toBeInTheDocument();
  });

  it('asks for a name when "Other" is picked', () => {
    renderFlow({ initialStep: 'brokerage' });
    elsewhere();
    expect(screen.queryByTestId('welcome-elsewhere-other')).toBeNull();
    fireEvent.click(screen.getByRole('checkbox', { name: 'Other' }));
    expect(screen.getByTestId('welcome-elsewhere-other')).toBeInTheDocument();
  });

  it('sends the brokerages used, by label and by the name typed for Other, with the kickoff', async () => {
    await startFromLast(() => {
      fireEvent.click(screen.getByRole('checkbox', { name: 'Charles Schwab' }));
      fireEvent.click(screen.getByRole('checkbox', { name: 'Other' }));
      fireEvent.change(screen.getByTestId('welcome-elsewhere-other'), { target: { value: '  Acme Brokers ' } });
    });
    // No screenshots named, and none attached.
    expect(instruction()).toContain(
      'Action: propose. Language: en-US. Brokerage: none. Brokerages used: Charles Schwab and Acme Brokers. This is',
    );
    expect(routeState().additionalContext).toHaveLength(1);
  });

  it('drops an Other left unnamed and a "No account yet" from the brokerages sent', async () => {
    await startFromLast(() => {
      fireEvent.click(screen.getByRole('checkbox', { name: 'Fidelity' }));
      fireEvent.click(screen.getByRole('checkbox', { name: 'Other' }));
    });
    expect(instruction()).toContain('Brokerages used: Fidelity. This is');

    cleanup();
    localStorage.clear();
    await startFromLast(() => fireEvent.click(screen.getByRole('checkbox', { name: 'No account yet' })));
    expect(instruction()).not.toContain('Brokerages used');
  });

  it('saves the brokerages picked with the profile when the last question is left', async () => {
    walkToLast(() => {
      fireEvent.click(screen.getByRole('checkbox', { name: 'Fidelity' }));
      fireEvent.click(screen.getByRole('checkbox', { name: 'Interactive Brokers' }));
    });
    expect(mutateAsync).not.toHaveBeenCalled();
    fireEvent.click(primary());
    await waitFor(() => expect(mutateAsync).toHaveBeenCalledTimes(1));
    expect(mutateAsync.mock.calls[0][0]).toMatchObject({
      investment_preference: { brokerages: ['fidelity', 'ibkr'] },
    });
  });

  // The agent reads its profile from the saved row, which must not say there
  // is no account beside the one it reads.
  it('saves no "No account yet" when a brokerage is connected after it was picked', async () => {
    renderFlow({ initialStep: 'brokerage', profile: answered, saved: emptyProfile() });
    elsewhere();
    fireEvent.click(screen.getByRole('checkbox', { name: 'No account yet' }));
    // Back to the brokers, where one is connected after all.
    mocks.connected = [{ label: 'moomoo', b: { name: 'moomoo' }, row: { tool_count: 3 } }];
    revisit('brokerage');
    for (let i = 0; i < 5; i++) fireEvent.click(primary());
    expect(turn('agent')).toBeInTheDocument();
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(instruction()).toContain('Brokerage: moomoo.');
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(
      (mutateAsync.mock.calls[0][0] as { investment_preference: object }).investment_preference,
    ).not.toHaveProperty('brokerages');
  });

  it('clears a saved "No account yet" once a brokerage is connected', async () => {
    mocks.connected = [{ label: 'moomoo', b: { name: 'moomoo' }, row: { tool_count: 3 } }];
    const noAccount: InvestingProfile = { ...answered, brokerages: ['none'] };
    renderFlow({ initialStep: 'agent', profile: noAccount, saved: noAccount });
    fireEvent.click(primary());
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(mutateAsync).toHaveBeenCalledWith({ investment_preference: { brokerages: [] } });
  });

  it('does not save brokerages when none was picked', async () => {
    await startFromLast();
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(
      (mutateAsync.mock.calls[0][0] as { investment_preference: object }).investment_preference,
    ).not.toHaveProperty('brokerages');
  });

  it('sends a screenshot added with the kickoff', async () => {
    const release = holdWorkspace();
    walkToLast(() => {
      fireEvent.click(screen.getByRole('radio', { name: /Upload screenshots/ }));
      fireEvent.click(screen.getByRole('button', { name: 'stand-in add screenshot' }));
    });
    fireEvent.click(primary());
    expect(await screen.findByTestId('welcome-pending')).toBeInTheDocument();
    expect(screen.queryByTestId('welcome-answer-start')).toBeNull();

    await release();
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    // Named in the instruction and attached as the composer attaches an
    // image, for the agent to read; a turn with no message draws no bubble.
    expect(instruction()).toContain('Action: propose. Language: en-US. Brokerage: none. Screenshots: 1. This is');
    expect(routeState()).toMatchObject({
      additionalContext: [
        { type: 'skills', name: 'onboarding' },
        { type: 'image', data: 'data:image/jpeg;base64,AAAA', description: 'positions.png' },
      ],
      attachmentMeta: null,
    });
    // Nothing connected, so no tools to wait for.
    expect(waitForServerTools).not.toHaveBeenCalled();
  });

  it('drops the screenshots, and the holdings they would bring in, when the way is put down', async () => {
    const release = holdWorkspace();
    walkToLast(() => {
      fireEvent.click(screen.getByRole('radio', { name: /Upload screenshots/ }));
      fireEvent.click(screen.getByRole('button', { name: 'stand-in add screenshot' }));
      fireEvent.click(screen.getByRole('radio', { name: /Upload screenshots/ }));
    });
    fireEvent.click(primary());
    expect(await screen.findByTestId('welcome-pending')).toBeInTheDocument();

    await release();
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(instruction()).toContain('Action: propose.');
    expect(instruction()).not.toContain('Screenshots');
    expect(routeState().additionalContext).toHaveLength(1);
  });
});
