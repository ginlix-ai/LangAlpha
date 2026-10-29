/**
 * The welcome flow with its own drop zone: a screenshot still being read when
 * the user goes on, through questions answered already and on to Start, goes
 * with the opening message all the same. Preparing one needs a canvas, which
 * jsdom lacks, so each test holds `prepareScreenshot` and lets it finish.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';

// The flow's stand-ins, shared with its other test files (welcomeHarness.tsx),
// all but the drop zone's.
vi.mock('@/lib/framer', async (importOriginal) => (await import('./welcomeHarness')).framerMock(importOriginal));
vi.mock('@/components/ui/chat-input', async () => (await import('./welcomeHarness')).composerMock());
vi.mock('@/hooks/useUpdatePreferences', async () => (await import('./welcomeHarness')).updatePreferencesMock());
vi.mock('../welcome/useBrokers', async () => (await import('./welcomeHarness')).brokersMock());
vi.mock('../welcome/steps/BrokerageStep', async () => (await import('./welcomeHarness')).brokerageStepMock());
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
vi.mock('../welcome/screenshots', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../welcome/screenshots')>()),
  prepareScreenshot: vi.fn(),
}));

import {
  answer,
  answered,
  flowRenderer,
  holdWorkspace,
  instruction,
  location,
  primary,
  resetWelcomeMocks,
  routeState,
  turn,
} from './welcomeHarness';
import { WelcomeFlow } from '../welcome/Welcome';
import { emptyProfile } from '../welcome/profile';
import * as screenshots from '../welcome/screenshots';
import type { Screenshot } from '../welcome/screenshots';

const renderFlow = flowRenderer(WelcomeFlow);
const prepare = vi.mocked(screenshots.prepareScreenshot);

const POSITIONS: Screenshot = { id: 's1', name: 'positions.png', dataUrl: 'data:image/jpeg;base64,AAAA', bytes: 4 };

describe('WelcomeFlow: a screenshot still being read at Start', () => {
  let finish: (shot: Screenshot) => void;

  beforeEach(() => {
    resetWelcomeMocks();
    prepare.mockReset();
    prepare.mockImplementation(() => new Promise((resolve) => (finish = resolve)));
  });

  /** A screenshot dropped on the elsewhere turn, then a Continue on each
   *  question, every one answered already as a resumed draft brings them, and
   *  Start, all before it is ready. */
  function startWhileReading() {
    renderFlow({ initialStep: 'brokerage', profile: answered, saved: emptyProfile() });
    fireEvent.click(screen.getByRole('button', { name: 'stand-in elsewhere' }));
    fireEvent.click(screen.getByRole('radio', { name: /Upload screenshots/ }));
    const zone = screen.getByTestId('welcome-method-screenshot-panel');
    const input = zone.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [new File(['x'], 'positions.png', { type: 'image/png' })] } });
    expect(prepare).toHaveBeenCalledTimes(1);
    for (let i = 0; i < 5; i++) fireEvent.click(primary());
    expect(turn('agent')).toBeInTheDocument();
    fireEvent.click(primary());
  }

  const sentWithIt = () => {
    expect(instruction()).toContain('Brokerage: none. Screenshots: 1. This is');
    expect(routeState().additionalContext).toContainEqual(
      expect.objectContaining({ type: 'image', data: POSITIONS.dataUrl, description: 'positions.png' }),
    );
  };

  it('waits for it when everything else the hand-off waits for is in', async () => {
    startWhileReading();
    expect(await screen.findByTestId('welcome-pending')).toBeInTheDocument();
    await act(async () => {});
    expect(location()).toBe('/welcome');

    await act(async () => finish(POSITIONS));
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    sentWithIt();
  });

  it('takes one that is ready while the hand-off is still waiting on the rest', async () => {
    const release = holdWorkspace();
    startWhileReading();
    expect(await screen.findByTestId('welcome-pending')).toBeInTheDocument();
    await act(async () => finish(POSITIONS));
    // Shown as taken in the answer above.
    expect(answer('elsewhere')).toHaveTextContent('1 screenshot');

    await release();
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    sentWithIt();
  });
});
