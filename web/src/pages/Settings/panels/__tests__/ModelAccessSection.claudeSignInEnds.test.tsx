/**
 * The Claude sign-in's row in Model access says the day it stops working,
 * and offers a way to Claude Code's sheet where the server offers Claude Code.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { ClaudeCodeSetup } from '@/api/model';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';

const state = vi.hoisted(() => ({
  oauth: undefined as unknown,
  setup: null as unknown,
  unread: false,
}));

vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeSetup: () => ({ setup: state.setup, isLoading: false, unread: state.unread }),
  useClaudeCodeStatus: () => ({ state: 'no_token', email: null, expiresAt: null, isLoading: false, enabled: false, runsHere: false, saved: false }),
  useClaudeCodeComputers: () => ({ computers: [], isLoading: false }),
}));
vi.mock('@/hooks/useProviderCatalog', () => ({
  useProviderCatalog: () => ({ keyGroups: [], planGroups: [], localEntries: [], placeOf: () => undefined, optionFor: () => null }),
}));
const claudeRow: ConfiguredProvider = {
  provider: 'claude-oauth', display_name: 'Claude', access_type: 'oauth', state: 'connected', email: 'user@example.com',
};
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({
    providers: [claudeRow], withdrawn: [], configuredSet: new Set(['claude-oauth']), statusLoading: false, statusError: false,
  }),
  useOAuthStatus: () => ({ data: state.oauth }),
}));
vi.mock('@/api/model', () => ({ saveClaudeCodeToken: vi.fn(), removeClaudeCodeToken: vi.fn() }));
vi.mock('@/config/hostMode', async (original) => ({
  ...(await original<typeof import('@/config/hostMode')>()),
  HOST_MODE: 'oss',
  isPlatformMode: false,
}));

import { ModelAccessSection } from '../ModelAccessSection';

const setupOf = (over: Partial<ClaudeCodeSetup> = {}): ClaudeCodeSetup => ({
  state: 'not_connected', on: false, computers: [], saved_token_runs: false, cloud: true, too_small: [], ...over,
});
const claudeListItem = () => screen.getByText('Claude').closest('li') as HTMLElement;
const SWITCH = { name: 'Switch to Claude Code' };

describe('ModelAccessSection: Claude sign-in end day', () => {
  beforeEach(() => {
    state.oauth = { connected: true, ends_on: '2026-11-30' };
    state.setup = setupOf();
    state.unread = false;
  });

  it('says the day the sign-in ends on its row', () => {
    renderWithProviders(<ModelAccessSection />);
    expect(within(claudeListItem()).getByText(/This sign-in ends on Nov 30, 2026\./)).toBeInTheDocument();
  });

  it('opens Claude Code\'s sheet from its Switch to Claude Code button', () => {
    renderWithProviders(<ModelAccessSection />);
    expect(screen.queryByRole('dialog')).toBeNull();
    fireEvent.click(within(claudeListItem()).getByRole('button', SWITCH));
    expect(screen.getByRole('dialog', { name: 'Claude Code' })).toBeInTheDocument();
  });

  it('offers the button while the setup read failed, since Claude Code may still be offered', () => {
    state.setup = null;
    state.unread = true;
    renderWithProviders(<ModelAccessSection />);
    expect(within(claudeListItem()).getByRole('button', SWITCH)).toBeInTheDocument();
  });

  it('gives the day alone where the server does not offer Claude Code', () => {
    state.setup = null;
    renderWithProviders(<ModelAccessSection />);
    expect(within(claudeListItem()).getByText(/This sign-in ends on Nov 30, 2026\./)).toBeInTheDocument();
    expect(screen.queryByRole('button', SWITCH)).toBeNull();
  });

  it('says nothing where the deployment set no end day', () => {
    state.oauth = { connected: true, ends_on: null };
    renderWithProviders(<ModelAccessSection />);
    expect(screen.queryByText(/This sign-in ends on/)).toBeNull();
    expect(screen.queryByRole('button', SWITCH)).toBeNull();
  });

  it('says nothing while the sign-in status is unread', () => {
    state.oauth = undefined;
    renderWithProviders(<ModelAccessSection />);
    expect(screen.queryByText(/This sign-in ends on/)).toBeNull();
  });
});
