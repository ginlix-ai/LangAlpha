/**
 * The setup wizard's last screen offers the investing profile next, unless a
 * page that sent the user into setup is waiting for them back: the profile
 * ends in the app, so that page would never be returned to.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { SETUP_RETURN_KEY } from '@/lib/setupReturn';

vi.mock('@/config/supportLinks', () => ({ PLATFORM_URL: '/account' }));
vi.mock('@/hooks/usePreferences', () => ({ usePreferences: () => ({ preferences: {} }) }));
vi.mock('@/hooks/useApiKeys', () => ({ useApiKeys: () => ({ apiKeys: null }) }));
vi.mock('@/components/model/useModelSlotCopy', () => ({
  useModelSlotCopy: () => ({ default: { summary: 'Default model' }, background: { summary: 'Background model' } }),
}));
vi.mock('@/pages/Onboarding/welcome/useWelcomeOffer', () => ({ useWelcomeOffer: () => ({ open: vi.fn() }) }));

import DoneStep from '../DoneStep';

const renderDone = () =>
  renderWithProviders(<DoneStep />);

afterEach(() => sessionStorage.clear());

describe('DoneStep', () => {
  it('offers the investing profile with no return kept', () => {
    renderDone();
    expect(screen.getByRole('button', { name: 'Set up investing profile' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Go to Dashboard' })).toBeInTheDocument();
  });

  it('leaves the profile out while a return is kept, and continues back to it', () => {
    sessionStorage.setItem(
      SETUP_RETURN_KEY,
      JSON.stringify({ url: `${window.location.origin}/account/onboarding/models`, at: Date.now() }),
    );
    renderDone();
    expect(screen.queryByRole('button', { name: 'Set up investing profile' })).toBeNull();
    expect(screen.getByRole('button', { name: 'Continue' })).toBeInTheDocument();
  });
});
