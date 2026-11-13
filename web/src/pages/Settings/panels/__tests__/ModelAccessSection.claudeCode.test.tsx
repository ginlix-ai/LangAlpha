/**
 * The Claude Code row says what the user's setup token is doing and offers
 * the one next step for it, and a link from elsewhere opens its sheet.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, screen, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { CLAUDE_CODE_TOKEN_URL } from '@/lib/claudeCode';
import type { ClaudeCodeSetup, ClaudeCodeState } from '@/api/model';

type Status = { state: ClaudeCodeState; email: string | null; expiresAt: string | null; isLoading: boolean };
const status = vi.hoisted(() => ({ current: {} as Status }));

// A self-hosted server, which runs turns on the saved token while Claude Code is on.
const setupOf = (s: Status): ClaudeCodeSetup => ({
  state: ({ signed_in: 'connected', unverified: 'connected', checking: 'checking', rejected: 'refused' } as const)[s.state as string] ?? 'not_connected',
  on: s.state !== 'off',
  computers: [],
  saved_token_runs: true,
  cloud: false,
  too_small: [],
});
vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeSetup: () => ({ setup: setupOf(status.current), isLoading: false }),
  useClaudeCodeStatus: () => ({ ...status.current, enabled: status.current.state !== 'off', runsHere: status.current.state !== 'off', saved: false }),
  useClaudeCodeComputers: () => ({ computers: [], isLoading: false }),
}));
vi.mock('@/hooks/useAllModels', () => ({ useAllModels: () => ({ models: {}, metadata: {} }) }));
vi.mock('@/hooks/useSlotDefaultModels', async (original) => ({
  ...(await original<typeof import('@/hooks/useSlotDefaultModels')>()),
  useDefaultModel: () => null,
}));
vi.mock('@/hooks/useProviderCatalog', () => ({
  useProviderCatalog: () => ({ keyGroups: [], planGroups: [], localEntries: [], placeOf: () => undefined, optionFor: () => null }),
}));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], withdrawn: [], configuredSet: new Set(), statusLoading: false, statusError: false }),
  useOAuthStatus: () => ({ data: undefined }),
}));
vi.mock('@/api/model', () => ({ saveClaudeCodeToken: vi.fn(), removeClaudeCodeToken: vi.fn() }));
// Pinned, since the no-token detail differs by mode and VITE_HOST_MODE can come from web/.env or another file's stub.
vi.mock('@/config/hostMode', async (original) => ({
  ...(await original<typeof import('@/config/hostMode')>()),
  HOST_MODE: 'oss',
  isPlatformMode: false,
}));

import { ModelAccessSection } from '../ModelAccessSection';

const base: Status = { state: 'no_token', email: null, expiresAt: null, isLoading: false };
const SIGNED_IN: Status = { ...base, state: 'signed_in', email: 'user@example.com', expiresAt: '2027-10-29T12:00:00Z' };
/** Claude Code's view, opened from its one row in the list. */
const openView = () => {
  fireEvent.click(within(screen.getByText('Claude Code').closest('li') as HTMLElement).getByRole('button', { name: 'Manage' }));
};
/** The saved token's row, in the view's Setup token group. */
const row = () => {
  const group = within(screen.getByRole('dialog', { name: 'Claude Code' })).getByText('Setup token').parentElement as HTMLElement;
  return within(group).getByRole('listitem');
};


/** Remove is a line's button where it is the only action, and the item of
 *  its More actions menu beside any other. */
function clickRemove(row: HTMLElement) {
  const more = within(row).queryByRole('button', { name: 'More actions' });
  if (!more) return fireEvent.click(within(row).getByRole('button', { name: 'Remove' }));
  fireEvent.keyDown(more, { key: 'Enter' });
  fireEvent.click(screen.getByRole('menuitem', { name: 'Remove' }));
}

describe('ModelAccessSection: Claude Code', () => {
  // The expiry lines read against today, so today is fixed.
  beforeEach(() => {
    status.current = base;
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-11-03T12:00:00Z'));
  });
  afterEach(() => { vi.useRealTimers(); });

  it('offers a setup token while there is none', () => {
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Not connected')).toBeInTheDocument();
    fireEvent.click(within(row()).getByRole('button', { name: 'Add setup token' }));
    expect(screen.getByRole('dialog', { name: 'Add a setup token' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Sign in with Claude' })).toBeInTheDocument();
  });

  // A check that never answers must not hold the token out of reach.
  it('says it is checking, and still offers to replace or remove the token', () => {
    status.current = { ...base, state: 'checking' };
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Checking')).toBeInTheDocument();
    expect(within(row()).getAllByRole('button').map((b) => b.textContent || b.getAttribute('aria-label'))).toEqual(['Replace', 'More actions']);
  });

  // One the check could not reach still runs turns, so it reads as connected.
  it.each(['signed_in', 'unverified'] as const)('while %s, reads as connected and replaces or removes the token', (state) => {
    status.current = { ...SIGNED_IN, state };
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Connected')).toBeInTheDocument();
    expect(within(row()).getByText('Oct 29, 2027').parentElement).toHaveTextContent(/^user@example\.com · Expires by Oct 29, 2027$/);
    expect(within(row()).getAllByRole('button').map((b) => b.textContent || b.getAttribute('aria-label'))).toEqual(['Replace', 'More actions']);
  });

  it('once connected, replaces or removes the token, removal confirmed first', async () => {
    status.current = { ...base, state: 'signed_in' };
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Connected')).toBeInTheDocument();
    fireEvent.click(within(row()).getByRole('button', { name: 'Replace' }));
    fireEvent.click(within(screen.getByRole('dialog', { name: 'Replace setup token' })).getByRole('button', { name: 'Close' }));
    // Uncovered, the view is exposed again once its inert attribute is gone.
    await screen.findByRole('dialog', { name: 'Claude Code' });
    clickRemove(row());
    expect(screen.getByRole('dialog', { name: 'Remove the Claude Code setup token?' })).toBeInTheDocument();
  });

  // As a subscription's row reads: the account and the rest under the name,
  // and a detail that wraps rather than loses its end beside the actions.
  it('names the account a sign-in made the token for, and when it expires', () => {
    status.current = SIGNED_IN;
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Connected')).toBeInTheDocument();
    const date = within(row()).getByText('Oct 29, 2027');
    const detail = date.parentElement!;
    expect(detail).toHaveTextContent(/^user@example\.com · Expires by Oct 29, 2027$/);
    expect(detail).not.toHaveClass('truncate');
    // A wrap may fall before the date, never inside it.
    expect(date).toHaveClass('whitespace-nowrap');
  });

  it('gives a pasted token its latest expiry alone', () => {
    status.current = { ...SIGNED_IN, email: null };
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Connected')).toBeInTheDocument();
    expect(within(row()).getByText('Oct 29, 2027').parentElement).toHaveTextContent(/^Expires by Oct 29, 2027$/);
  });

  it('wraps the no-token detail rather than cutting it off', () => {
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Runs Claude Code on this server')).not.toHaveClass('truncate');
  });

  it('says why a refused token stopped working, and offers a new one or its removal', () => {
    status.current = { ...base, state: 'rejected' };
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Needs sign-in')).toBeInTheDocument();
    expect(within(row()).getByText('Claude refused this token. It may have expired or been revoked.')).toBeInTheDocument();
    expect(within(row()).getByRole('button', { name: 'More actions' })).toBeInTheDocument();
    fireEvent.click(within(row()).getByRole('button', { name: 'Add new token' }));
    expect(screen.getByRole('dialog', { name: 'Add a setup token' })).toBeInTheDocument();
  });

  it('says when a token expired, and offers a new one or its removal', () => {
    status.current = { ...base, state: 'rejected', expiresAt: '2026-10-30T12:00:00Z' };
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Needs sign-in')).toBeInTheDocument();
    expect(within(row()).getByText('Oct 30, 2026').parentElement).toHaveTextContent(
      /^This token expired by Oct 30, 2026\. Add a new one to keep using Claude Code\.$/,
    );
    expect(within(row()).getAllByRole('button').map((b) => b.textContent || b.getAttribute('aria-label'))).toEqual(['Add new token', 'More actions']);
  });

  it('asks for a new token in the two weeks before one expires, and not before', () => {
    status.current = { ...SIGNED_IN, expiresAt: '2026-11-13T12:00:00Z' };
    const { unmount } = renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Connected')).toBeInTheDocument();
    expect(within(row()).getByText('Nov 13, 2026').parentElement).toHaveTextContent(
      /^This token expires by Nov 13, 2026\. Add a new one before then so Claude Code keeps running\.$/,
    );
    expect(within(row()).getAllByRole('button').map((b) => b.textContent || b.getAttribute('aria-label'))).toEqual(['Add new token', 'More actions']);
    unmount();
    status.current = { ...SIGNED_IN, expiresAt: '2026-11-18T12:00:00Z' };
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Nov 18, 2026').parentElement).toHaveTextContent(/^user@example\.com · Expires by Nov 18, 2026$/);
  });

  it('moves the expiry lines on while Settings stays open', () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] });
    vi.setSystemTime(new Date('2026-11-03T12:00:00Z'));
    status.current = { ...SIGNED_IN, expiresAt: '2026-11-17T12:00:30Z' };
    renderWithProviders(<ModelAccessSection />);
    openView();
    expect(within(row()).getByText('Nov 17, 2026').parentElement).toHaveTextContent(/^user@example\.com · Expires by Nov 17, 2026$/);
    act(() => { vi.advanceTimersByTime(60_000); });
    expect(within(row()).getByText('Nov 17, 2026').parentElement).toHaveTextContent(/^This token expires by Nov 17, 2026\./);
    vi.setSystemTime(new Date('2026-11-17T12:01:00Z'));
    act(() => { vi.advanceTimersByTime(60_000); });
    expect(within(row()).getByText('Needs sign-in')).toBeInTheDocument();
    expect(within(row()).getByText('Nov 17, 2026').parentElement).toHaveTextContent(/^This token expired by Nov 17, 2026\./);
  });

  it('opens the sheet from a link, and closing it drops the link', () => {
    renderWithProviders(<ModelAccessSection />, { route: CLAUDE_CODE_TOKEN_URL });
    fireEvent.click(within(screen.getByRole('dialog', { name: 'Add a setup token' })).getByRole('button', { name: 'Close' }));
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('opens nothing from the link while Claude Code is not listed', () => {
    status.current = { ...base, state: 'off' };
    renderWithProviders(<ModelAccessSection />, { route: CLAUDE_CODE_TOKEN_URL });
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(screen.queryByText('Claude Code')).toBeNull();
  });
});
