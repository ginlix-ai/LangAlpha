/**
 * The account's look is applied on every signed-in page, the setup wizard and
 * the welcome flow included, which render outside Main.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useEffect } from 'react';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const h = vi.hoisted(() => ({ mounts: 0, loggedIn: true }));

vi.mock('@/hooks/useAppearance', () => ({
  useSyncAppearance: () => {
    useEffect(() => {
      h.mounts += 1;
    }, []);
  },
}));
vi.mock('@/contexts/AuthContext', () => ({
  useAuth: () => ({ isLoggedIn: h.loggedIn, isInitialized: true }),
}));
vi.mock('@/hooks/useSetupGate', () => ({ useSetupGate: () => ({ isLoading: false, needsSetup: false }) }));
vi.mock('@/pages/Setup/SetupWizard', () => ({ default: () => <div>setup wizard</div> }));
vi.mock('@/pages/Onboarding/welcome/Welcome', () => ({ default: () => <div>welcome flow</div> }));
vi.mock('@/components/Main/Main', () => ({ default: () => <div>main</div>, preloadRouteChunk: () => {} }));
vi.mock('@/pages/Onboarding', () => ({
  OnboardingProvider: ({ children }: { children: React.ReactNode }) => children,
  OnboardingHostGate: () => null,
}));
vi.mock('@/lib/threadLifecycle/ThreadLifecycleFeed', () => ({ ThreadLifecycleFeed: () => null }));
vi.mock('@/components/WarmHome', () => ({ WarmHome: () => null }));
vi.mock('@/pages/Plugins/components/ConnectConsentHost', () => ({ ConnectConsentHost: () => null }));
vi.mock('@/pages/Plugins/components/ConnectLandingHost', () => ({ ConnectLandingHost: () => null }));
vi.mock('@/components/Sidebar/AppSidebar', () => ({ default: () => null }));

import App from '../App';

function renderAt(path: string) {
  return render(
    <QueryClientProvider client={new QueryClient()}>
      <MemoryRouter initialEntries={[path]}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  h.mounts = 0;
  h.loggedIn = true;
});

describe('the appearance sync', () => {
  it.each([
    ['/setup/method', 'setup wizard'],
    ['/welcome', 'welcome flow'],
    ['/dashboard', 'main'],
  ])('runs once on %s', async (path, page) => {
    renderAt(path);
    expect(await screen.findByText(page)).toBeInTheDocument();
    expect(h.mounts).toBe(1);
  });

  it('does not run signed out', () => {
    h.loggedIn = false;
    renderAt('/dashboard');
    expect(h.mounts).toBe(0);
  });
});
