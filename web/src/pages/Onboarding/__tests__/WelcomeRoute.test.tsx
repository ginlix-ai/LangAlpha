/**
 * The welcome route: what it reads before it opens the flow (the saved
 * profile, a draft left behind, a typed `?step=`) and where it opens it.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';

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
  DRAFT,
  getPreferences,
  instruction,
  location,
  mutateAsync,
  primary,
  resetWelcomeMocks,
  routeRenderer,
} from './welcomeHarness';
import Welcome from '../welcome/Welcome';
import { emptyProfile, profileRow, type InvestingProfile } from '../welcome/profile';

const renderRoute = routeRenderer(Welcome);

describe('Welcome route', () => {
  beforeEach(() => {
    resetWelcomeMocks();
  });

  const writeDraft = (step: string, profile: InvestingProfile, base: InvestingProfile) =>
    localStorage.setItem(
      DRAFT,
      JSON.stringify({ step, savedAt: Date.now(), profile: profileRow(profile), base: profileRow(base) }),
    );

  const LAST_QUESTION = 'welcome-turn-agent';
  const FIRST_QUESTION = 'welcome-turn-experience';

  it('a profile it cannot read is not opened blank, and Retry reads it again', async () => {
    getPreferences.mockRejectedValueOnce(new Error('down'));
    getPreferences.mockResolvedValueOnce(profileRow(answered));
    renderRoute('/welcome?edit=1');
    expect(await screen.findByTestId('welcome-load-failed')).toHaveTextContent("Couldn't load your investing profile.");
    expect(screen.queryByTestId('welcome-flow')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(await screen.findByTestId('welcome-flow')).toBeInTheDocument();
    expect(getPreferences).toHaveBeenCalledTimes(2);
  });

  it('edits the row as it is now, not as the cache last saw it', async () => {
    let resolve: (row: unknown) => void = () => {};
    getPreferences.mockImplementation(() => new Promise((r) => (resolve = r)));
    renderRoute('/welcome?edit=1', { cached: profileRow(answered) });
    // The cached row is not enough to open on.
    expect(screen.queryByTestId('welcome-flow')).toBeNull();

    // Since the cache was filled, the agent moved the experience, the level
    // and how often it reaches out, and added a sector.
    await act(async () =>
      resolve(
        profileRow({
          ...answered,
          experience: 'over_10y',
          riskLevel: 'moderate',
          sectors: ['biotech'],
          proactivity: 'proactive',
        }),
      ),
    );
    expect(await screen.findByRole('radio', { name: 'Over 10 years' })).toBeChecked();
    fireEvent.click(primary());
    expect(screen.getByRole('radio', { name: /Moderate/ })).toBeChecked();
    fireEvent.click(primary());
    fireEvent.click(primary());
    expect(screen.getByRole('checkbox', { name: 'biotech' })).toBeChecked();
    fireEvent.click(primary());
    expect(screen.getByRole('radio', { name: /^Regularly/ })).toBeChecked();
    fireEvent.click(primary());

    await waitFor(() => expect(location()).toBe('/settings?tab=preferences'));
    expect(mutateAsync).not.toHaveBeenCalled();
  });

  it('opens a draft left on the last question there, its answers ready to start from, whatever the server holds', async () => {
    writeDraft('agent', answered, emptyProfile());
    getPreferences.mockResolvedValue(null);
    renderRoute('/welcome');
    expect(await screen.findByTestId(LAST_QUESTION)).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /^Full research/ })).toBeChecked();
    // Kept in the draft as the output format, the way the server keeps it.
    expect(screen.getByRole('radio', { name: /^Summary with a file/ })).toBeChecked();
    expect(answer('style')).toHaveTextContent(/^Fundamental analysis · Growth$/);
    expect(primary()).toHaveTextContent('Start');
    expect(primary()).toBeEnabled();
  });

  it('opens a draft at its first unanswered question, answers kept, once the profile was reset', async () => {
    writeDraft('style', { ...answered, riskLevel: null }, emptyProfile());
    getPreferences.mockResolvedValue(null);
    renderRoute('/welcome');
    expect(await screen.findByTestId('welcome-turn-risk')).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'Years' })).toBeChecked();
    expect(answer('experience')).toHaveTextContent(/^Individual investor · 3–10 years$/);
  });

  it('a typed ?step=style opens there only once the questions before it are answered', async () => {
    getPreferences.mockResolvedValue({ risk_preference: {}, investment_preference: {} });
    renderRoute('/welcome?step=style');
    expect(await screen.findByTestId(FIRST_QUESTION)).toBeInTheDocument();
  });

  it('a typed ?step=style opens there with a saved profile, its way on going to the last question', async () => {
    getPreferences.mockResolvedValue(profileRow(answered));
    renderRoute('/welcome?step=style');
    expect(await screen.findByTestId('welcome-turn-style')).toBeInTheDocument();
    expect(primary()).toHaveTextContent('Continue');
    fireEvent.click(primary());
    expect(screen.getByTestId(LAST_QUESTION)).toBeInTheDocument();
    expect(mutateAsync).not.toHaveBeenCalled();
  });

  it('a draft left past a question it never asked opens at that question', async () => {
    // Saved by a build that had no experience screen: risk, markets and style
    // are answered, who the user is is not, and Start on the last question
    // would save the profile without it.
    const { investorType: _type, experience: _years, ...older } = answered;
    writeDraft('agent', { ...emptyProfile(), ...older }, emptyProfile());
    getPreferences.mockResolvedValue({ risk_preference: {}, investment_preference: {} });
    renderRoute('/welcome');
    expect(await screen.findByTestId(FIRST_QUESTION)).toBeInTheDocument();
  });

  it('a resumed draft keeps the answers given and takes newer values for the rest', async () => {
    writeDraft(
      'style',
      { ...emptyProfile(), experience: 'under_1y', riskLevel: 'aggressive', markets: ['hk'] },
      emptyProfile(),
    );
    getPreferences.mockResolvedValue(profileRow({ ...answered, sectors: ['biotech'] }));
    renderRoute('/welcome');
    expect(await screen.findByRole('checkbox', { name: 'biotech' })).toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'Growth' })).toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'Fundamental analysis' })).toBeChecked();
    fireEvent.click(primary());
    // The agent's answers are the server's too, shown and not written back.
    expect(screen.getByRole('radio', { name: /^Full research/ })).toBeChecked();
    expect(screen.getByRole('radio', { name: /^Summary with a file/ })).toBeChecked();
    fireEvent.click(primary());

    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(mutateAsync).toHaveBeenCalledWith({
      risk_preference: { risk_level: 'aggressive' },
      investment_preference: { experience: 'under_1y', markets: ['hk'] },
    });
  });

  it("a resumed draft brings back the brokerage step's own answers, so the kickoff names the broker typed there", async () => {
    // As a desktop sign-in that runs out leaves it: the window reloads on a
    // later question, past the screen where the broker was named.
    localStorage.setItem(
      DRAFT,
      JSON.stringify({
        step: 'agent',
        savedAt: Date.now(),
        profile: profileRow({ ...answered, brokerages: ['other'] }),
        base: profileRow(emptyProfile()),
        elsewhere: true,
        otherName: 'Tiger Brokers',
        method: 'search',
      }),
    );
    getPreferences.mockResolvedValue(null);
    renderRoute('/welcome');
    expect(await screen.findByTestId(LAST_QUESTION)).toBeInTheDocument();
    fireEvent.click(primary());

    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(instruction()).toContain(
      'Action: propose. Language: en-US. Brokerage: none. Brokerages used: Tiger Brokers.',
    );
  });
});
