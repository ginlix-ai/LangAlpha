/**
 * The Claude Code row says what the user's setup token is doing and offers
 * the one next step for it, and a link from elsewhere opens its sheet.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { CLAUDE_CODE_TOKEN_URL } from '@/lib/claudeCode';
import type { ClaudeCodeState } from '@/api/model';

type Status = { state: ClaudeCodeState; email: string | null; expiresAt: string | null; isLoading: boolean };
const status = vi.hoisted(() => ({ current: {} as Status }));

vi.mock('@/hooks/useClaudeCode', () => ({ useClaudeCodeStatus: () => status.current }));
vi.mock('@/hooks/useProviderCatalog', () => ({
  useProviderCatalog: () => ({ keyGroups: [], planGroups: [], localEntries: [], placeOf: () => undefined, optionFor: () => null }),
}));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], withdrawn: [], configuredSet: new Set(), statusLoading: false, statusError: false }),
}));
vi.mock('@/api/model', () => ({ saveClaudeCodeToken: vi.fn(), removeClaudeCodeToken: vi.fn() }));

import { ModelAccessSection } from '../ModelAccessSection';

const base: Status = { state: 'no_token', email: null, expiresAt: null, isLoading: false };
const SIGNED_IN: Status = { ...base, state: 'signed_in', email: 'user@example.com', expiresAt: '2027-10-29T12:00:00Z' };
const row = () => screen.getByText('Claude Code').closest('li') as HTMLElement;

describe('ModelAccessSection: Claude Code', () => {
  beforeEach(() => { status.current = base; });

  it('offers a setup token while there is none', () => {
    renderWithProviders(<ModelAccessSection />);
    expect(within(row()).getByText('Not connected')).toBeInTheDocument();
    fireEvent.click(within(row()).getByRole('button', { name: 'Add setup token' }));
    expect(screen.getByRole('dialog', { name: 'Add a setup token' })).toBeInTheDocument();
    expect(screen.getByLabelText('Setup token')).toBeInTheDocument();
  });

  // A check that never answers must not hold the token out of reach.
  it('says it is checking, and still offers to replace or remove the token', () => {
    status.current = { ...base, state: 'checking' };
    renderWithProviders(<ModelAccessSection />);
    expect(within(row()).getByText('Checking')).toBeInTheDocument();
    expect(within(row()).getAllByRole('button').map((b) => b.textContent)).toEqual(['Replace', 'Remove']);
  });

  // One the check could not reach still runs turns, so it reads as connected.
  it.each(['signed_in', 'unverified'] as const)('while %s, reads as connected and replaces or removes the token', (state) => {
    status.current = { ...SIGNED_IN, state };
    renderWithProviders(<ModelAccessSection />);
    expect(within(row()).getByText('Connected')).toBeInTheDocument();
    expect(within(row()).getByText('Oct 29, 2027').parentElement).toHaveTextContent(/^user@example\.com · Expires by Oct 29, 2027$/);
    expect(within(row()).getAllByRole('button').map((b) => b.textContent)).toEqual(['Replace', 'Remove']);
  });

  it('once connected, replaces or removes the token, removal confirmed first', () => {
    status.current = { ...base, state: 'signed_in' };
    renderWithProviders(<ModelAccessSection />);
    expect(within(row()).getByText('Connected')).toBeInTheDocument();
    fireEvent.click(within(row()).getByRole('button', { name: 'Replace' }));
    expect(screen.getByRole('dialog', { name: 'Replace setup token' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    fireEvent.click(within(row()).getByRole('button', { name: 'Remove' }));
    expect(screen.getByRole('dialog', { name: 'Remove the Claude Code setup token?' })).toBeInTheDocument();
  });

  // As a subscription's row reads: the account and the rest under the name,
  // and a detail that wraps rather than loses its end beside the actions.
  it('names the account a sign-in made the token for, and when it expires', () => {
    status.current = SIGNED_IN;
    renderWithProviders(<ModelAccessSection />);
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
    expect(within(row()).getByText('Connected')).toBeInTheDocument();
    expect(within(row()).getByText('Oct 29, 2027').parentElement).toHaveTextContent(/^Expires by Oct 29, 2027$/);
  });

  it('wraps the no-token detail rather than cutting it off', () => {
    renderWithProviders(<ModelAccessSection />);
    expect(within(row()).getByText('Its own setup token, not your Claude Code sign-in')).not.toHaveClass('truncate');
  });

  it('says why a refused token stopped working, and offers a new one or its removal', () => {
    status.current = { ...base, state: 'rejected' };
    renderWithProviders(<ModelAccessSection />);
    expect(within(row()).getByText('Not connected')).toBeInTheDocument();
    expect(within(row()).getByText('Claude refused this token. It may have expired or been revoked.')).toBeInTheDocument();
    expect(within(row()).getByRole('button', { name: 'Remove' })).toBeInTheDocument();
    fireEvent.click(within(row()).getByRole('button', { name: 'Add new token' }));
    expect(screen.getByRole('dialog', { name: 'Add a setup token' })).toBeInTheDocument();
  });

  it('opens the sheet from a link, and closing it drops the link', () => {
    renderWithProviders(<ModelAccessSection />, { route: CLAUDE_CODE_TOKEN_URL });
    expect(screen.getByRole('dialog', { name: 'Add a setup token' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('opens nothing from the link while Claude Code is not listed', () => {
    status.current = { ...base, state: 'off' };
    renderWithProviders(<ModelAccessSection />, { route: CLAUDE_CODE_TOKEN_URL });
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(screen.queryByText('Claude Code')).toBeNull();
  });
});
