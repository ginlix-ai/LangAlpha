/**
 * The welcome flow with its own holdings search: a row still being written
 * when the user goes on, through questions answered already and on to Start,
 * is in the lists before the agent is handed the conversation, and one that
 * fails once its turn has gone is still said. Each test holds the write and
 * lets it finish.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';

// The flow's stand-ins, shared with its other test files (welcomeHarness.tsx),
// all but the holdings search's.
vi.mock('@/lib/framer', async (importOriginal) => (await import('./welcomeHarness')).framerMock(importOriginal));
vi.mock('@/components/ui/chat-input', async () => (await import('./welcomeHarness')).composerMock());
vi.mock('@/hooks/useUpdatePreferences', async () => (await import('./welcomeHarness')).updatePreferencesMock());
vi.mock('../welcome/useBrokers', async () => (await import('./welcomeHarness')).brokersMock());
vi.mock('../welcome/steps/BrokerageStep', async () => (await import('./welcomeHarness')).brokerageStepMock());
vi.mock('../welcome/steps/ScreenshotDrop', async () => (await import('./welcomeHarness')).screenshotDropMock());
vi.mock('@/pages/ChatAgent/utils/warmWorkspace', async () => (await import('./welcomeHarness')).warmWorkspaceMock());
vi.mock('@/hooks/useMcpServers', async (importOriginal) =>
  (await import('./welcomeHarness')).mcpServersMock(importOriginal),
);
vi.mock('@/hooks/useHomeWorkspace', async () => (await import('./welcomeHarness')).homeWorkspaceMock());
vi.mock('../welcome/marketBrief', async () => (await import('./welcomeHarness')).marketBriefMock());
vi.mock('@/contexts/ThemeContext', async () => (await import('./welcomeHarness')).themeMock());
vi.mock('@/components/ui/use-toast', async () => (await import('./welcomeHarness')).toastMock());
// The lists the search writes to start empty here.
vi.mock('@/pages/Dashboard/utils/api', async (importOriginal) => ({
  ...(await (await import('./welcomeHarness')).dashboardApiMock(importOriginal)),
  getPortfolio: async () => ({ holdings: [] }),
  listWatchlists: async () => ({ watchlists: [] }),
  listWatchlistItems: async () => ({ items: [] }),
}));
vi.mock('@/hooks/useSyncUserLocale', async () => (await import('./welcomeHarness')).syncUserLocaleMock());
vi.mock('@/hooks/useUser', async () => (await import('./welcomeHarness')).userMock());
vi.mock('../OnboardingProvider', async () => (await import('./welcomeHarness')).onboardingProviderMock());
vi.mock('@/hooks/useSymbolSearch', () => ({
  useSymbolSearch: (q: string) => ({
    hits: q.trim() ? [{ symbol: 'AAPL', name: 'Apple Inc.', exchangeShortName: 'NASDAQ', currency: 'USD' }] : [],
    loading: false,
  }),
}));
vi.mock('@/pages/Dashboard/utils/portfolio', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/Dashboard/utils/portfolio')>()),
  addPortfolioHolding: vi.fn(),
}));

import { answered, flowRenderer, location, primary, resetWelcomeMocks, toast, turn } from './welcomeHarness';
import { WelcomeFlow } from '../welcome/Welcome';
import { emptyProfile } from '../welcome/profile';
import * as portfolio from '@/pages/Dashboard/utils/portfolio';

const renderFlow = flowRenderer(WelcomeFlow);
const addHolding = vi.mocked(portfolio.addPortfolioHolding);

describe('WelcomeFlow: a holding still being written at Start', () => {
  let land: () => void;
  let refuse: (err: Error) => void;

  beforeEach(() => {
    resetWelcomeMocks();
    addHolding.mockReset();
    addHolding.mockImplementation(
      () =>
        new Promise((resolve, reject) => {
          land = () => resolve({});
          refuse = reject;
        }),
    );
  });

  /** A holding added by search on the elsewhere turn, which is then left
   *  before the write is done. */
  function addAndGoOn() {
    renderFlow({ initialStep: 'brokerage', profile: answered, saved: emptyProfile() });
    fireEvent.click(screen.getByRole('button', { name: 'stand-in elsewhere' }));
    fireEvent.click(screen.getByRole('radio', { name: /Search and add/ }));
    const search = screen.getByTestId('welcome-holdings-search');
    fireEvent.focus(search);
    fireEvent.change(search, { target: { value: 'aap' } });
    fireEvent.mouseDown(screen.getByRole('option', { name: /AAPL/ }));
    fireEvent.change(screen.getByTestId('welcome-holdings-quantity'), { target: { value: '25' } });
    fireEvent.click(screen.getByTestId('welcome-holdings-to-holdings'));
    expect(addHolding).toHaveBeenCalledTimes(1);
    fireEvent.click(primary());
    expect(turn('experience')).toBeInTheDocument();
    expect(screen.queryByTestId('welcome-holdings-search')).toBeNull();
  }

  it('waits for it before the agent, whose first move is to read the lists, is handed the turn', async () => {
    addAndGoOn();
    for (let i = 0; i < 4; i++) fireEvent.click(primary());
    expect(turn('agent')).toBeInTheDocument();
    fireEvent.click(primary());
    await act(async () => new Promise((resolve) => setTimeout(resolve, 0)));
    expect(location()).toBe('/welcome');
    expect(screen.getByTestId('welcome-pending')).toBeInTheDocument();

    await act(async () => land());
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(toast).not.toHaveBeenCalled();
  });

  it('says one that fails after its turn is left in a toast, with no line under it to say it', async () => {
    addAndGoOn();
    await act(async () => refuse(new Error('nope')));
    await waitFor(() => expect(toast).toHaveBeenCalledTimes(1));
    expect(toast.mock.calls[0][0]).toMatchObject({ variant: 'destructive', description: "Couldn't add AAPL." });
  });
});
