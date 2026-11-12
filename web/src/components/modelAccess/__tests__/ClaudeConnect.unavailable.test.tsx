/**
 * A user the server does not let sign in with Claude is told so, with no
 * "start again" to offer: a new attempt gets the same refusal. Every other
 * refusal keeps its way to try again.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  initiateClaudeOAuth: vi.fn(),
  submitClaudeCallback: vi.fn(),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({ rawModels: {}, metadata: {}, isLoading: false }),
}));
vi.mock('@/api/oauth', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/oauth')>()),
  initiateClaudeOAuth: h.initiateClaudeOAuth,
  submitClaudeCallback: h.submitClaudeCallback,
}));
vi.mock('@/config/hostMode', async (original) => ({
  ...(await original<typeof import('@/config/hostMode')>()),
  HOST_MODE: 'oss',
  isPlatformMode: false,
}));

import { ClaudeConnect } from '../ClaudeConnect';

const TITLE = "Signing in with Claude isn't available for this account.";
const refusal = (code: string) => ({
  response: {
    status: 403,
    data: {
      detail: 'A plain sentence that is not shown.',
      error: { code, message: 'unused', retryable: false },
    },
  },
});
const flow = { authorize_url: 'https://claude.ai/oauth/authorize?x=1', expires_at: null };

async function start(onCancel?: () => void) {
  const user = userEvent.setup();
  renderWithProviders(<ClaudeConnect onConnected={() => {}} onCancel={onCancel} />);
  await user.click(screen.getByRole('checkbox'));
  await user.click(screen.getByRole('button', { name: 'Continue' }));
  return user;
}

afterEach(() => {
  vi.clearAllMocks();
});

describe('ClaudeConnect: a sign-in the server does not offer this account', () => {
  it('says so when starting is refused, with nothing to try again', async () => {
    h.initiateClaudeOAuth.mockRejectedValue(refusal('unavailable'));
    await start(() => {});

    expect(await screen.findByText(TITLE)).toBeInTheDocument();
    expect(screen.queryByText('Start again to get a new code.')).toBeNull();
    expect(screen.queryByText('This is usually brief. Try again in a moment.')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Start again' })).toBeNull();
    expect(screen.queryByRole('button', { name: /try again/i })).toBeNull();
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument();
    expect(h.initiateClaudeOAuth).toHaveBeenCalledTimes(1);
  });

  it('says so when the code is refused, with nothing to try again', async () => {
    h.initiateClaudeOAuth.mockResolvedValue(flow);
    h.submitClaudeCallback.mockRejectedValue(refusal('unavailable'));
    const user = await start(() => {});
    await user.type(await screen.findByLabelText('Code from claude.ai'), 'abc123#def456');
    await user.click(screen.getByRole('button', { name: 'Connect' }));

    expect(await screen.findByText(TITLE)).toBeInTheDocument();
    expect(h.submitClaudeCallback).toHaveBeenCalledWith('abc123#def456');
    expect(screen.queryByText('Start again to get a new code.')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Start again' })).toBeNull();
    expect(screen.queryByRole('button', { name: /try again/i })).toBeNull();
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument();
  });

  it('leaves Cancel out when the caller gave no way to cancel', async () => {
    h.initiateClaudeOAuth.mockRejectedValue(refusal('unavailable'));
    await start();

    expect(await screen.findByText(TITLE)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Cancel' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Start again' })).toBeNull();
  });

  it('hands Cancel to the caller', async () => {
    h.initiateClaudeOAuth.mockRejectedValue(refusal('unavailable'));
    const onCancel = vi.fn();
    const user = await start(onCancel);
    await user.click(await screen.findByRole('button', { name: 'Cancel' }));

    expect(onCancel).toHaveBeenCalledTimes(1);
  });

  it('still offers Start again for another refusal of the code', async () => {
    h.initiateClaudeOAuth.mockResolvedValue(flow);
    h.submitClaudeCallback.mockRejectedValue(refusal('exchange_failed'));
    const user = await start(() => {});
    await user.type(await screen.findByLabelText('Code from claude.ai'), 'abc123#def456');
    await user.click(screen.getByRole('button', { name: 'Connect' }));

    expect(await screen.findByText("Claude didn't accept the code.")).toBeInTheDocument();
    expect(screen.queryByText(TITLE)).toBeNull();
    expect(screen.getByText('Start again to get a new code.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Start again' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument();
  });

  it('still offers Start again for another refusal of the start', async () => {
    h.initiateClaudeOAuth.mockRejectedValue(refusal('save_failed'));
    await start(() => {});

    expect(await screen.findByText("Your account couldn't be saved.")).toBeInTheDocument();
    expect(screen.queryByText(TITLE)).toBeNull();
    expect(screen.getByText('Start again to get a new code.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Start again' })).toBeInTheDocument();
  });

  it('still offers Try again for a transient refusal', async () => {
    h.initiateClaudeOAuth.mockRejectedValue(refusal('redis_unavailable'));
    await start(() => {});

    expect(await screen.findByText(/Connecting accounts isn't available right now/)).toBeInTheDocument();
    expect(screen.queryByText(TITLE)).toBeNull();
    expect(screen.getByText('This is usually brief. Try again in a moment.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Try again' })).toBeInTheDocument();
  });
});
