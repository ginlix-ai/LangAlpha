/**
 * The setup-token sheet for one of the user's own computers: it signs in and
 * saves through that computer's routes, and what they answer is that
 * computer's row in the list. A token change rereads every other Claude
 * Code read, since a computer can run on the saved token, but never the one
 * it answered with.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { ClaudeCodeComputer, ClaudeCodeStatus } from '@/api/model';

vi.mock('@/api/model', () => ({
  saveClaudeCodeToken: vi.fn(),
  removeClaudeCodeToken: vi.fn(),
  startClaudeCodeSignIn: vi.fn(),
  submitClaudeCodeSignInCode: vi.fn(),
  saveClaudeCodeComputerToken: vi.fn(),
  removeClaudeCodeComputerToken: vi.fn(),
  startClaudeCodeComputerSignIn: vi.fn(),
  submitClaudeCodeComputerSignInCode: vi.fn(),
}));

import {
  saveClaudeCodeComputerToken, saveClaudeCodeToken, startClaudeCodeComputerSignIn, startClaudeCodeSignIn,
  submitClaudeCodeComputerSignInCode,
} from '@/api/model';
import { ClaudeCodeTokenSheet } from '../ClaudeCodeToken';

const CODE = 'placeholder-code#placeholder-state';
const TOKEN = 'sk-ant-oat01-placeholder';
const MAC: ClaudeCodeComputer = {
  computer_id: 'cmp-mac', name: 'Studio Mac', kind: 'helper', state: 'no_token', source: null, email: null, expires_at: null,
};
const BOX: ClaudeCodeComputer = { ...MAC, computer_id: 'cmp-box', name: 'Lab box', kind: 'ssh', state: 'signed_in', source: 'machine' };
const STATUS: ClaudeCodeStatus = { enabled: true, state: 'no_token', email: null, expires_at: null, saved: false };
const STATUS_KEY = queryKeys.features.claudeCodeStatus();
const LIST_KEY = queryKeys.features.claudeCodeComputers();

function client() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  queryClient.setQueryData(LIST_KEY, { computers: [MAC, BOX] });
  queryClient.setQueryData(STATUS_KEY, STATUS);
  return queryClient;
}
const invalidated = (queryClient: QueryClient, key: readonly unknown[]) => queryClient.getQueryState(key)?.isInvalidated;
const refused = (code: string) =>
  Object.assign(new Error('Request failed'), { response: { status: 400, data: { error: { code, message: '', retryable: false } } } });
const BOX_TARGET = { kind: 'computer', computerId: 'cmp-box', name: 'Lab box' } as const;

describe('ClaudeCodeTokenSheet for a computer', () => {
  beforeEach(() => vi.clearAllMocks());

  it("signs in through the computer's routes and keeps its answer as that computer's row", async () => {
    (startClaudeCodeComputerSignIn as Mock).mockResolvedValue({ authorize_url: 'https://claude.com/cai/oauth/authorize?x', expires_at: '2099-01-01T00:00:00Z' });
    const saved = { ...MAC, state: 'signed_in', source: 'machine', email: 'user@example.com' };
    (submitClaudeCodeComputerSignInCode as Mock).mockResolvedValue(saved);
    const onSaved = vi.fn();
    const { queryClient } = renderWithProviders(
      <ClaudeCodeTokenSheet target={{ kind: 'computer', computerId: 'cmp-mac', name: 'Studio Mac' }} onSaved={onSaved} onCancel={() => {}} />,
      { queryClient: client() },
    );
    expect(screen.getByText(/It stays on Studio Mac, and LangAlpha doesn't keep a copy/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    fireEvent.change(await screen.findByLabelText('Code from Claude'), { target: { value: CODE } });
    fireEvent.click(screen.getByRole('button', { name: 'Connect' }));
    await waitFor(() => expect(onSaved).toHaveBeenCalledTimes(1));
    expect(startClaudeCodeComputerSignIn).toHaveBeenCalledWith('cmp-mac');
    expect(submitClaudeCodeComputerSignInCode).toHaveBeenCalledWith('cmp-mac', CODE);
    expect(startClaudeCodeSignIn).not.toHaveBeenCalled();
    expect(queryClient.getQueryData(LIST_KEY)).toEqual({ computers: [saved, BOX] });
    // The list is the answer; the saved token's status is read again.
    expect(invalidated(queryClient, LIST_KEY)).toBe(false);
    expect(invalidated(queryClient, STATUS_KEY)).toBe(true);
    // The saved token's status is another token's.
    expect(queryClient.getQueryData(STATUS_KEY)).toEqual(STATUS);
  });

  it('a change to the saved token rereads the computers, which can run on it, but not the status it answered with', async () => {
    (saveClaudeCodeToken as Mock).mockResolvedValue({ ...STATUS, state: 'signed_in', saved: true });
    const onSaved = vi.fn();
    const { queryClient } = renderWithProviders(<ClaudeCodeTokenSheet onSaved={onSaved} onCancel={() => {}} />, { queryClient: client() });
    fireEvent.change(screen.getByLabelText('Setup token'), { target: { value: TOKEN } });
    fireEvent.click(screen.getByRole('button', { name: 'Save token' }));
    await waitFor(() => expect(onSaved).toHaveBeenCalledTimes(1));
    expect(invalidated(queryClient, LIST_KEY)).toBe(true);
    expect(invalidated(queryClient, STATUS_KEY)).toBe(false);
    expect(JSON.stringify(queryClient.getQueryCache().getAll().map((q) => q.state.data))).not.toContain(TOKEN);
  });

  // The row still says how the token stood before the policy refused it.
  it.each([
    ['policy_blocked', true],
    ['needs_perl', true],
    ['computer_unsupported', true],
    ['token_rejected', false],
  ])('a pasted token refused with %s rereads the computers: %s', async (code, reread) => {
    (saveClaudeCodeComputerToken as Mock).mockRejectedValue(refused(code));
    const { queryClient } = renderWithProviders(
      <ClaudeCodeTokenSheet target={BOX_TARGET} onSaved={() => {}} onCancel={() => {}} />,
      { queryClient: client() },
    );
    fireEvent.change(screen.getByLabelText('Setup token'), { target: { value: TOKEN } });
    fireEvent.click(screen.getByRole('button', { name: 'Save token' }));
    await screen.findByRole('alert');
    expect(invalidated(queryClient, LIST_KEY)).toBe(reread);
  });

  it.each(['needs_perl', 'computer_unsupported'])('a sign-in refused at its start with %s rereads the computers', async (code) => {
    (startClaudeCodeComputerSignIn as Mock).mockRejectedValueOnce(refused(code));
    const { queryClient } = renderWithProviders(
      <ClaudeCodeTokenSheet target={BOX_TARGET} onSaved={() => {}} onCancel={() => {}} />,
      { queryClient: client() },
    );
    fireEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    await screen.findByRole('alert');
    expect(invalidated(queryClient, LIST_KEY)).toBe(true);
  });

  it('a sign-in refused by the policy, at its start or its code, rereads the computers', async () => {
    (startClaudeCodeComputerSignIn as Mock).mockRejectedValueOnce(refused('policy_blocked'));
    const first = renderWithProviders(
      <ClaudeCodeTokenSheet target={BOX_TARGET} onSaved={() => {}} onCancel={() => {}} />,
      { queryClient: client() },
    );
    fireEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    await screen.findByRole('alert');
    expect(invalidated(first.queryClient, LIST_KEY)).toBe(true);
    first.unmount();

    (startClaudeCodeComputerSignIn as Mock).mockResolvedValue({ authorize_url: 'https://claude.com/cai/oauth/authorize?x', expires_at: '2099-01-01T00:00:00Z' });
    (submitClaudeCodeComputerSignInCode as Mock).mockRejectedValue(refused('policy_blocked'));
    const { queryClient } = renderWithProviders(
      <ClaudeCodeTokenSheet target={BOX_TARGET} onSaved={() => {}} onCancel={() => {}} />,
      { queryClient: client() },
    );
    fireEvent.click(screen.getByRole('button', { name: 'Sign in with Claude' }));
    fireEvent.change(await screen.findByLabelText('Code from Claude'), { target: { value: CODE } });
    fireEvent.click(screen.getByRole('button', { name: 'Connect' }));
    await screen.findByRole('alert');
    expect(invalidated(queryClient, LIST_KEY)).toBe(true);
  });
});
