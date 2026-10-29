/**
 * Invitation redemption in MethodStep. The code goes to the platform service
 * (`/api/auth/`, not `/api/v1/`), each refusal has its own copy, and a 409
 * means this account already redeemed the code, so it proceeds like a success.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Routes, Route } from 'react-router';
import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

const { post, getCurrentUser, configured } = vi.hoisted(() => ({
  post: vi.fn(),
  getCurrentUser: vi.fn(),
  configured: { providers: [] as Array<Record<string, unknown>> },
}));

vi.mock('@/api/client', () => ({ api: { post, defaults: { baseURL: '' } } }));
vi.mock('@/pages/Dashboard/utils/api', () => ({
  getCurrentUser,
  deleteUserApiKey: vi.fn(),
  disconnectCodexOAuth: vi.fn(),
  disconnectClaudeOAuth: vi.fn(),
}));
// A signed-in user with no platform access and no provider yet: the one state
// that offers the invitation code.
vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: { access_tier: -1 }, isLoading: false }),
}));
vi.mock('@/hooks/useConfiguredProviders', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useConfiguredProviders')>()),
  useConfiguredProviders: () => ({ providers: configured.providers, hasAny: configured.providers.length > 0 }),
}));
vi.mock('@/hooks/usePreferences', () => ({ usePreferences: () => ({ preferences: null }) }));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: vi.fn() }),
}));
vi.mock('@/hooks/useAllModels', () => ({ useAllModels: () => ({ metadata: {} }) }));

import MethodStep from '../MethodStep';

function renderStep() {
  const utils = renderWithProviders(
    <Routes>
      <Route path="/setup/method" element={<MethodStep />} />
      <Route path="/setup/defaults" element={<p>defaults step</p>} />
    </Routes>,
    { route: '/setup/method' },
  );
  const invalidate = vi.spyOn(utils.queryClient, 'invalidateQueries');
  return { invalidate };
}

async function redeem(code: string) {
  const user = userEvent.setup();
  const rendered = renderStep();
  await user.click(screen.getByRole('button', { name: 'Have an invitation code?' }));
  await user.type(screen.getByPlaceholderText('Enter your invitation code'), code);
  await user.click(screen.getByRole('button', { name: 'Redeem' }));
  return rendered;
}

function rejectWith(status: number, detail?: unknown) {
  post.mockRejectedValueOnce({ response: { status, data: detail === undefined ? {} : { detail } } });
}

describe('MethodStep invitation redemption', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    getCurrentUser.mockResolvedValue({});
  });

  it('redeems the trimmed code on the platform service, refreshes access, and moves on', async () => {
    post.mockResolvedValueOnce({ data: { ok: true } });
    const { invalidate } = await redeem('  INVITE-123 ');

    expect(await screen.findByText('defaults step')).toBeInTheDocument();
    expect(post).toHaveBeenCalledWith('/api/auth/invitations/redeem', { code: 'INVITE-123' });
    expect(getCurrentUser).toHaveBeenCalledWith({ refresh_tier: true });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.user.me() });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.platform.models() });
  });

  it('moves on when the code was already redeemed by this account (409)', async () => {
    rejectWith(409, { message: 'Already redeemed', type: 'conflict' });
    const { invalidate } = await redeem('USED-CODE');

    expect(await screen.findByText('defaults step')).toBeInTheDocument();
    expect(getCurrentUser).toHaveBeenCalledWith({ refresh_tier: true });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.user.me() });
  });

  it.each([
    [404, { message: 'Invitation not found', type: 'not_found' }, 'Invalid invitation code.'],
    [410, { message: 'Code exhausted', type: 'gone' }, 'This code has expired or been fully used.'],
    [422, { message: 'Code format invalid', type: 'validation_error' }, 'Code format invalid'],
    [500, 'Internal server error', 'Internal server error'],
    [500, undefined, 'Something went wrong. Please try again.'],
  ])('stays on the step and explains a %i refusal', async (status, detail, copy) => {
    rejectWith(status, detail);
    await redeem('SOME-CODE');

    expect(await screen.findByText(copy)).toBeInTheDocument();
    expect(screen.queryByText('defaults step')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Redeem' })).toBeEnabled();
  });

  it('offers Redeem only once a code is typed', async () => {
    const user = userEvent.setup();
    renderStep();
    await user.click(screen.getByRole('button', { name: 'Have an invitation code?' }));
    const button = screen.getByRole('button', { name: 'Redeem' });

    expect(button).toBeDisabled();
    await user.type(screen.getByPlaceholderText('Enter your invitation code'), '   ');
    expect(button).toBeDisabled();
    await user.type(screen.getByPlaceholderText('Enter your invitation code'), 'X');
    expect(button).toBeEnabled();
    expect(post).not.toHaveBeenCalled();
  });
});

describe('MethodStep connected summary', () => {
  it("offers no removal for the deployment's own key", async () => {
    configured.providers = [
      { provider: 'openai', display_name: 'OpenAI', access_type: 'api_key', masked_key: '(server)' },
      { provider: 'anthropic', display_name: 'Anthropic', access_type: 'api_key', masked_key: 'sk-...abcd' },
    ];
    const user = userEvent.setup();
    renderStep();
    await user.click(screen.getByRole('button', { name: /2 providers connected/i }));

    expect(screen.getAllByRole('button', { name: 'Remove' })).toHaveLength(1);
    configured.providers = [];
  });
});
