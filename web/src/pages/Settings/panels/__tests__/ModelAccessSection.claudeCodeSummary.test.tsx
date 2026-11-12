/**
 * Claude Code's row in the Subscriptions card shows the plan of one token
 * when the user has several, and a limit it is held at stands whole there,
 * with the status the view gives that token.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor, within } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { ClaudeCodeComputer, ClaudeCodePlan, ClaudeCodeSetup, ClaudeCodeStatus } from '@/api/model';

vi.mock('@/hooks/useProviderCatalog', () => ({
  useProviderCatalog: () => ({ keyGroups: [], planGroups: [], localEntries: [], placeOf: () => undefined, optionFor: () => null }),
}));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], withdrawn: [], configuredSet: new Set(), statusLoading: false, statusError: false }),
  useOAuthStatus: () => ({ data: undefined }),
}));
vi.mock('@/api/model', () => ({
  getClaudeCodeSetup: vi.fn(),
  getClaudeCodeStatus: vi.fn(),
  getClaudeCodeComputers: vi.fn(),
}));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getWorkspace: vi.fn(),
}));
vi.mock('@/config/hostMode', async (original) => ({
  ...(await original<typeof import('@/config/hostMode')>()),
  HOST_MODE: 'oss',
  isPlatformMode: false,
}));

import { getClaudeCodeComputers, getClaudeCodeSetup, getClaudeCodeStatus } from '@/api/model';
import { ModelAccessSection } from '../ModelAccessSection';

const FUTURE = '2099-01-01T00:00:00Z';
const used = (five: number, over: Partial<ClaudeCodePlan> = {}): ClaudeCodePlan => ({
  five_hour: { used: five, resets_at: FUTURE }, seven_day: { used: five / 2, resets_at: FUTURE }, limits: [], credits: null,
  observed_at: '2026-11-07T15:00:00Z', ...over,
});
const held = (window: 'five_hour' | 'seven_day', five: number) => used(five, { limits: [{ window, family: null, until: FUTURE, reset_known: true }] });

const computer = (id: string, email: string, plan: ClaudeCodePlan | null, state: ClaudeCodeComputer['state'] = 'signed_in'): ClaudeCodeComputer => ({
  computer_id: id, name: `Box ${id}`, kind: 'ssh', state, source: 'machine', email, expires_at: null, plan,
});
const status = (s: Partial<ClaudeCodeStatus> = {}): ClaudeCodeStatus => ({
  enabled: true, state: 'no_token', email: null, expires_at: null, saved: false, ...s,
});

function render(computers: ClaudeCodeComputer[], account = status()) {
  const setup: ClaudeCodeSetup = { state: 'connected', on: true, computers, saved_token_runs: true, cloud: false, too_small: [] };
  (getClaudeCodeSetup as Mock).mockResolvedValue(setup);
  (getClaudeCodeStatus as Mock).mockResolvedValue(account);
  (getClaudeCodeComputers as Mock).mockResolvedValue({ computers });
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, retryDelay: 0 } } });
  queryClient.setQueryData(queryKeys.features.list(), [{ key: 'claude_code_runtime', enabled: true, gate: 'opt_in' }]);
  return renderWithProviders(<ModelAccessSection />, { queryClient });
}

const claudeRow = () => screen.getByText('Claude Code').closest('li') as HTMLElement;
const detail = () => claudeRow().querySelector('[data-row-detail]') as HTMLElement;

describe('Settings: Claude Code\'s row in the Subscriptions card', () => {
  beforeEach(() => vi.clearAllMocks());

  it('shows the token nearest a limit of several: the highest use', async () => {
    render(
      [computer('a', 'a@example.com', used(0.3)), computer('b', 'b@example.com', used(0.7))],
      status({ state: 'signed_in', saved: true, email: 's@example.com', plan: used(0.5) }),
    );
    await waitFor(() => expect(detail()).toHaveTextContent('b@example.com'));
    expect(claudeRow()).toHaveTextContent('70%');
    expect(claudeRow()).not.toHaveTextContent('a@example.com');
    expect(claudeRow()).not.toHaveTextContent('s@example.com');
  });

  it('puts the saved token first when it is the fullest', async () => {
    render(
      [computer('a', 'a@example.com', used(0.3))],
      status({ state: 'signed_in', saved: true, email: 's@example.com', plan: used(0.8) }),
    );
    await waitFor(() => expect(detail()).toHaveTextContent('s@example.com'));
  });

  it('puts a token at a limit before one that is fuller but not at one', async () => {
    render([computer('a', 'a@example.com', used(0.95)), computer('b', 'b@example.com', held('five_hour', 0.5))]);
    await waitFor(() => expect(detail()).toHaveTextContent('5h limit used up, resets'));
    expect(claudeRow()).not.toHaveTextContent('a@example.com');
  });

  it('puts the fuller of two tokens at a limit first', async () => {
    render([computer('a', 'a@example.com', held('five_hour', 0.4)), computer('b', 'b@example.com', held('seven_day', 0.6))]);
    await waitFor(() => expect(detail()).toHaveTextContent('7d limit used up, resets'));
  });

  it('passes over a token that does not work, however full its plan reads', async () => {
    render([computer('a', 'a@example.com', used(0.3)), computer('b', 'b@example.com', used(0.99), 'rejected')]);
    await waitFor(() => expect(detail()).toHaveTextContent('a@example.com'));
    expect(claudeRow()).not.toHaveTextContent('b@example.com');
  });

  it('shows a row at a limit as its limit line, whole, with no bars after it', async () => {
    render([computer('a', 'a@example.com', held('five_hour', 0.5))]);
    await waitFor(() => expect(detail()).toHaveTextContent('5h limit used up, resets'));
    expect(detail().querySelector('[data-usage-whole]')).toHaveTextContent('5h limit used up, resets');
    expect(detail().querySelector('[data-usage-bar]')).toBeNull();
    expect(detail().querySelector('[data-plan-window]')).toBeNull();
  });

  it('says Limit reached for a row at a limit, as the view\'s row for that token does', async () => {
    render([computer('a', 'a@example.com', held('five_hour', 0.5))]);
    await waitFor(() => expect(within(claudeRow()).getByText('Limit reached')).toBeInTheDocument());
    expect(within(claudeRow()).queryByText('Connected')).toBeNull();
  });

  it('shows a row short of a limit as its account, with each window as a bar after it', async () => {
    render([computer('a', 'a@example.com', used(0.4))]);
    await waitFor(() => expect(detail()).toHaveTextContent('a@example.com'));
    expect(detail().querySelector('[data-usage-whole]')).toBeNull();
    expect(detail().querySelectorAll('[data-usage-bar]')).toHaveLength(2);
    expect(detail().querySelector('[data-plan-window="five_hour"]')).toHaveTextContent('40%');
  });
});
