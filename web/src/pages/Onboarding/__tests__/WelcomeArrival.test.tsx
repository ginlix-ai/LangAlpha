/**
 * Arriving from setup: the flow fades in and its first turn writes itself,
 * the brokerage card coming up only once the line has settled. A visit that
 * setup did not send opens standing.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen } from '@testing-library/react';

type Opts = { onUpdate: (v: number) => void; onComplete: () => void };
// Motion allowed, with the typewriter's chains run to their end on the next
// timer, since jsdom gives framer no frames.
vi.mock('@/lib/framer', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/framer')>()),
  useReducedMotion: () => false,
  animate: (_from: number, to: number, opts: Opts) => {
    const timer = setTimeout(() => {
      opts.onUpdate(to);
      opts.onComplete();
    }, 0);
    return { stop: () => clearTimeout(timer) };
  },
}));
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

import { resetWelcomeMocks, routeRenderer } from './welcomeHarness';
import Welcome from '../welcome/Welcome';

const renderRoute = routeRenderer(Welcome);

describe('Welcome, arriving from setup', () => {
  beforeEach(() => {
    resetWelcomeMocks();
  });

  it('fades in and writes its first line before the brokerage card comes up', async () => {
    renderRoute('/welcome', { state: { fromSetup: true } });
    expect(await screen.findByTestId('welcome-flow')).toHaveClass('welcome-chat--arrive');
    expect(screen.queryByTestId('brokerage-step')).toBeNull();
    expect(await screen.findByTestId('brokerage-step', {}, { timeout: 4000 })).toBeInTheDocument();
  });

  it('opens standing when setup did not send it', async () => {
    renderRoute('/welcome');
    expect(await screen.findByTestId('welcome-flow')).not.toHaveClass('welcome-chat--arrive');
    expect(screen.getByTestId('brokerage-step')).toBeInTheDocument();
  });
});
