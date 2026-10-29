/**
 * Another page can send a user into setup with `?return=<URL>` and expect them
 * back. The wizard keeps the target, takes the param out of the address bar,
 * and its exit goes back there instead of to the dashboard. Without one, the
 * exit behaves as it always has.
 */
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { SETUP_RETURN_KEY } from '@/lib/setupReturn';

const h = vi.hoisted(() => ({
  assign: vi.fn(),
  skipSetup: vi.fn(),
  hasAny: false,
  accessTier: -1,
}));

vi.mock('@/config/hostMode', () => ({ isPlatformMode: true, HOST_MODE: 'platform', APP_ENTRY_PATH: '/app' }));
vi.mock('@/config/supportLinks', () => ({ PLATFORM_URL: '/account' }));
vi.mock('@/hooks/useSetupGate', () => ({ skipSetup: h.skipSetup }));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], hasAny: h.hasAny }),
}));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: { access_tier: h.accessTier } }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));

function StepProbe() {
  const location = useLocation();
  return (
    <div>
      <p data-testid="search">{location.search}</p>
      <p data-testid="state">{JSON.stringify(location.state)}</p>
    </div>
  );
}
vi.mock('../steps/MethodStep', () => ({ default: StepProbe }));
vi.mock('../steps/ProviderStep', () => ({ default: StepProbe }));

import SetupWizard from '../SetupWizard';

/**
 * The way back is a full page load, which jsdom will not perform. `assign` is an
 * own non-configurable property of Location, so the whole object is swapped
 * rather than the one method spied.
 */
const realLocation = window.location;
Object.defineProperty(window, 'location', {
  configurable: true,
  value: { href: realLocation.href, origin: realLocation.origin, assign: h.assign },
});
afterAll(() => {
  Object.defineProperty(window, 'location', { configurable: true, value: realLocation });
});

const returnUrl = () => `${window.location.origin}/account/onboarding/models`;
const withReturn = (url: string, rest = '') => `?return=${encodeURIComponent(url)}${rest}`;

function renderWizard(pathname: string, search = '', state: unknown = null) {
  const client = new QueryClient();
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[{ pathname, search, state }]}>
        <Routes>
          <Route path="/setup/*" element={<SetupWizard />} />
          <Route path="/dashboard" element={<p>dashboard</p>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const exitButton = () => screen.queryByRole('button', { name: 'Exit setup' });

beforeEach(() => {
  h.hasAny = false;
  h.accessTier = -1;
});

afterEach(() => {
  vi.clearAllMocks();
  sessionStorage.clear();
});

describe('SetupWizard return', () => {
  it('keeps the return and takes it out of the address bar, other params and state kept', async () => {
    renderWizard('/setup/provider', withReturn(returnUrl(), '&foo=1'), { method: 'oauth' });

    await waitFor(() => expect(screen.getByTestId('search')).toHaveTextContent(/^\?foo=1$/));
    expect(screen.getByTestId('state')).toHaveTextContent('{"method":"oauth"}');
    expect(JSON.parse(sessionStorage.getItem(SETUP_RETURN_KEY) ?? '{}').url).toBe(returnUrl());
  });

  it('shows the exit without model access, and it goes back without naming a provider', async () => {
    renderWizard('/setup/method', withReturn(returnUrl()));

    const exit = await screen.findByRole('button', { name: 'Exit setup' });
    await userEvent.setup().click(exit);

    expect(h.assign).toHaveBeenCalledWith(returnUrl());
    expect(h.skipSetup).not.toHaveBeenCalled();
    expect(sessionStorage.getItem(SETUP_RETURN_KEY)).toBeNull();
  });

  it('ignores a return to anywhere else, and still drops the param', async () => {
    renderWizard('/setup/method', withReturn('https://evil.example/account/onboarding/models'));

    await waitFor(() => expect(screen.getByTestId('search')).toBeEmptyDOMElement());
    expect(exitButton()).toBeNull();
    expect(sessionStorage.getItem(SETUP_RETURN_KEY)).toBeNull();
  });

  it('without a return, hides the exit from a user with no model access', async () => {
    renderWizard('/setup/method');
    await screen.findByTestId('search');
    expect(exitButton()).toBeNull();
  });

  it('without a return, the exit skips setup and opens the dashboard', async () => {
    h.hasAny = true;
    renderWizard('/setup/method');

    await act(async () => {
      await userEvent.setup().click(await screen.findByRole('button', { name: 'Exit setup' }));
    });

    expect(await screen.findByText('dashboard')).toBeInTheDocument();
    expect(h.skipSetup).toHaveBeenCalledTimes(1);
    expect(h.assign).not.toHaveBeenCalled();
  });
});
