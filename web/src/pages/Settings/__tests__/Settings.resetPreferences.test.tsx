/**
 * Resetting preferences on the Investing profile section also marks onboarding
 * incomplete on the server, so the user row the welcome offer reads must be
 * fetched again: the cached one still says the profile is saved.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// The server's user row, which a reset flips the way the server does.
const h = vi.hoisted(() => ({
  onboardingCompleted: true,
  getCurrentUser: vi.fn(),
  clearPreferences: vi.fn(),
}));

vi.mock('@/pages/Dashboard/utils/api', () => ({
  getCurrentUser: h.getCurrentUser,
  clearPreferences: h.clearPreferences,
}));

const prefsStub = { preferences: { risk_preference: { risk_tolerance: 'moderate' } }, isLoading: false };
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => prefsStub,
}));

const mutationStub = { mutateAsync: vi.fn(async () => ({})) };
vi.mock('@/hooks/useUpdatePreferences', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useUpdatePreferences')>()),
  useUpdatePreferences: () => mutationStub,
}));

const toastStub = { toast: vi.fn() };
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => toastStub,
}));

// Import after mocks are registered.
import { InvestingProfilePanel } from '../panels/InvestingProfilePanel';

function renderInvestingProfile() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/settings?tab=preferences']}>
        <InvestingProfilePanel />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  h.onboardingCompleted = true;
  h.getCurrentUser.mockReset();
  h.getCurrentUser.mockImplementation(async () => ({
    user_id: 'u-1',
    email: 'tester@example.com',
    onboarding_completed: h.onboardingCompleted,
  }));
  h.clearPreferences.mockReset();
  h.clearPreferences.mockImplementation(async () => {
    h.onboardingCompleted = false;
    return { success: true };
  });
});

describe('Settings: resetting preferences', () => {
  it('offers the welcome flow again once the reset lands', async () => {
    renderInvestingProfile();
    // An account still loading counts as not done, so wait for the saved row.
    await waitFor(() => expect(screen.queryByText('Set up your investing profile')).not.toBeInTheDocument());
    expect(h.getCurrentUser).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole('button', { name: 'Reset preferences' }));
    const dialog = await screen.findByRole('dialog');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Reset preferences' }));

    await waitFor(() => expect(h.clearPreferences).toHaveBeenCalledTimes(1));
    expect(await screen.findByText('Set up your investing profile')).toBeInTheDocument();
    expect(h.getCurrentUser).toHaveBeenCalledTimes(2);
  });
});
