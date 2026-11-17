/**
 * A fact setup resumes on that could not be read holds the answers' pages,
 * and /setup itself, on the way to read it again; the questions and the
 * provider pages need no fact and open as ever.
 */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes } from 'react-router';

const h = vi.hoisted(() => ({ failed: true, retry: vi.fn() }));
vi.mock('@/config/hostMode', () => ({ isPlatformMode: false, HOST_MODE: 'oss', APP_ENTRY_PATH: '/' }));
vi.mock('@/hooks/useSetupGate', () => ({ skipSetup: vi.fn() }));
vi.mock('@/hooks/useOnboardingRoute', () => ({ useOnboardingRoute: () => ({ kind: 'done' }) }));
vi.mock('@/hooks/useConfiguredProviders', () => ({ useConfiguredProviders: () => ({ providers: [], hasAny: false }) }));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: { access_tier: -1 } }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));
vi.mock('../answers/useSetupFacts', () => ({
  useSetupFacts: () => ({ rail: ['model', 'default'], factsFailed: h.failed, retryFacts: h.retry }),
}));
vi.mock('../steps/MethodStep', () => ({ default: () => <p>step</p> }));
vi.mock('../steps/UseStep', () => ({ default: () => <p>step</p> }));
vi.mock('../steps/ModelStep', () => ({ default: () => <p>step</p> }));

import SetupWizard from '../SetupWizard';

function renderAt(pathname: string) {
  render(
    <QueryClientProvider client={new QueryClient()}>
      <MemoryRouter initialEntries={[pathname]}>
        <Routes>
          <Route path="/setup/*" element={<SetupWizard />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('SetupWizard with a fact it could not read', () => {
  beforeEach(() => {
    h.failed = true;
    h.retry.mockClear();
  });

  it.each(['/setup/model', '/setup'])('holds %s on Try again, which reads it again', async (pathname) => {
    renderAt(pathname);
    expect(screen.getByTestId('setup-load-failed')).toBeInTheDocument();
    expect(screen.queryByText('step')).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: /try again/i }));
    expect(h.retry).toHaveBeenCalled();
  });

  it.each(['/setup/use', '/setup/method'])('opens %s, which reads no fact', async (pathname) => {
    renderAt(pathname);
    await screen.findByText('step');
    expect(screen.queryByTestId('setup-load-failed')).toBeNull();
  });

  it('opens the step once the facts are read', async () => {
    h.failed = false;
    renderAt('/setup/model');
    await screen.findByText('step');
  });
});
