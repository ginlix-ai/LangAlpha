/**
 * The welcome flow with the brokers' own trading permission: a level lowered
 * to ask for approval, whose save is still out when the user goes on through
 * questions answered already and on to Start, is on the server before the
 * agent is handed the conversation. Until then the server holds the level it
 * replaces, which would let an order the first turn places through unasked.
 * One that fails holds the hand-off only until it is answered, and says so in
 * a toast. Each test holds the save and lets it finish.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { TRADING_AGREEMENT_VERSION } from '@/lib/tradingPermission';
import type { TradingPermission, TradingPermissionUpdate } from '@/types/api';

// The flow's stand-ins, shared with its other test files (welcomeHarness.tsx),
// all but the brokerage step's.
vi.mock('@/lib/framer', async (importOriginal) => (await import('./welcomeHarness')).framerMock(importOriginal));
vi.mock('@/components/ui/chat-input', async () => (await import('./welcomeHarness')).composerMock());
vi.mock('@/hooks/useUpdatePreferences', async () => (await import('./welcomeHarness')).updatePreferencesMock());
// The brokers' screen reads more of the lists than the flow does: none of
// it but the one broker connected is offered here.
vi.mock('../welcome/useBrokers', async () => {
  const { useBrokers } = (await import('./welcomeHarness')).brokersMock();
  return {
    useBrokers: () => ({
      ...useBrokers(),
      desktopOnly: [],
      shellOutdated: false,
      loading: false,
      resolved: new Map(),
      refetchOffers: () => {},
      refetchCatalog: () => {},
    }),
  };
});
vi.mock('../welcome/steps/ScreenshotDrop', async () => (await import('./welcomeHarness')).screenshotDropMock());
vi.mock('../welcome/steps/HoldingsEntry', async () => (await import('./welcomeHarness')).holdingsEntryMock());
vi.mock('@/pages/ChatAgent/utils/warmWorkspace', async () => (await import('./welcomeHarness')).warmWorkspaceMock());
vi.mock('@/hooks/useMcpServers', async (importOriginal) =>
  (await import('./welcomeHarness')).mcpServersMock(importOriginal),
);
vi.mock('@/hooks/useHomeWorkspace', async () => (await import('./welcomeHarness')).homeWorkspaceMock());
vi.mock('../welcome/marketBrief', async () => (await import('./welcomeHarness')).marketBriefMock());
vi.mock('@/contexts/ThemeContext', async () => (await import('./welcomeHarness')).themeMock());
// The trading choice says a failed save through the hook, the flow through
// the module's own toast: both are the one the harness reads.
vi.mock('@/components/ui/use-toast', async () => {
  const harness = await import('./welcomeHarness');
  return { ...harness.toastMock(), useToast: () => ({ toast: harness.toast }) };
});
vi.mock('@/pages/Dashboard/utils/api', async (importOriginal) =>
  (await import('./welcomeHarness')).dashboardApiMock(importOriginal),
);
vi.mock('@/hooks/useSyncUserLocale', async () => (await import('./welcomeHarness')).syncUserLocaleMock());
vi.mock('@/hooks/useUser', async () => (await import('./welcomeHarness')).userMock());
vi.mock('../OnboardingProvider', async () => (await import('./welcomeHarness')).onboardingProviderMock());
// The brokers' cards and a connect's round trip have their own tests
// (BrokerageStep.test.tsx); the trading permission below them is what is
// read here.
vi.mock('@/pages/Plugins/hooks/useBrokerageConnect', () => ({
  useBrokerageConnect: () => ({
    oauth: { connectingName: null },
    requestConnect: () => {},
    toggle: () => {},
    togglingName: null,
  }),
}));
vi.mock('@/pages/Plugins/hooks/useConnectLanding', () => ({
  useWelcomeLanding: () => null,
  spendConnectLanding: () => false,
}));
vi.mock('@/pages/Plugins/components/BrokerageCard', () => ({
  BrokerageCard: ({ broker }: { broker: { label: string } }) => <article>{broker.label}</article>,
}));
vi.mock('@/pages/ChatAgent/components/mcp/BrandMark', () => ({ BrandMark: () => <span /> }));
vi.mock('@/api/tradingPermission', () => ({
  getTradingPermission: vi.fn(),
  updateTradingPermission: vi.fn(),
}));

import { answered, flowRenderer, location, mocks, primary, resetWelcomeMocks, toast, turn } from './welcomeHarness';
import { WelcomeFlow } from '../welcome/Welcome';
import { emptyProfile } from '../welcome/profile';
import { getTradingPermission, updateTradingPermission } from '@/api/tradingPermission';

const renderFlow = flowRenderer(WelcomeFlow);
const mockGet = vi.mocked(getTradingPermission);
const mockUpdate = vi.mocked(updateTradingPermission);

/** A broker connected and showing its tools, so the hand-off waits on
 *  nothing of its own. */
const MOOMOO = { label: 'moomoo', b: { name: 'moomoo' }, row: { tool_count: 4 } };

const permission = (over: Partial<TradingPermission> = {}): TradingPermission => ({
  level: 'autonomous',
  agreement_version: TRADING_AGREEMENT_VERSION,
  agreed_at: '2026-10-06T00:00:00Z',
  updated_at: null,
  ...over,
});

describe('WelcomeFlow: a trading level still being saved at Start', () => {
  let land: () => void;
  let refuse: (err: Error) => void;

  beforeEach(() => {
    resetWelcomeMocks();
    mocks.connected = [MOOMOO];
    mocks.rows = [{ b: MOOMOO.b, label: MOOMOO.label, connected: true }];
    mockGet.mockReset();
    mockGet.mockResolvedValue(permission());
    mockUpdate.mockReset();
    mockUpdate.mockImplementation(
      (body: TradingPermissionUpdate) =>
        new Promise((resolve, reject) => {
          land = () => resolve(permission({ level: body.level }));
          refuse = reject;
        }),
    );
  });

  /** Approve each picked over full autonomy on the brokers' turn, then a
   *  Continue on each question, every one answered already as a resumed draft
   *  brings them, and Start, all before the save is answered. */
  async function lowerAndStart() {
    renderFlow({ initialStep: 'brokerage', profile: answered, saved: emptyProfile() });
    fireEvent.click(await screen.findByRole('radio', { name: /^Approve each order/ }));
    await waitFor(() => expect(mockUpdate).toHaveBeenCalledWith({ level: 'approve_each' }));
    for (let i = 0; i < 5; i++) fireEvent.click(primary());
    expect(turn('agent')).toBeInTheDocument();
    fireEvent.click(primary());
    await act(async () => new Promise((resolve) => setTimeout(resolve, 0)));
    expect(location()).toBe('/welcome');
    expect(screen.getByTestId('welcome-pending')).toBeInTheDocument();
  }

  it('waits for it before the agent, whose orders it governs, is handed the turn', async () => {
    await lowerAndStart();

    await act(async () => land());
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(toast).not.toHaveBeenCalled();
  });

  it('goes on with the level the server kept once one that fails is answered, and says so', async () => {
    await lowerAndStart();

    await act(async () => refuse(new Error('Request failed with status code 500')));
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(toast).toHaveBeenCalledTimes(1);
    expect(toast.mock.calls[0][0]).toMatchObject({
      variant: 'destructive',
      description: "Couldn't change your trading permission",
    });
  });
});
