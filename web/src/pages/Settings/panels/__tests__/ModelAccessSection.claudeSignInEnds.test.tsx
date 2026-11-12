/**
 * The Claude sign-in's row in Model access is marked Legacy once the
 * deployment set it an end day, says that day until it passes and to use
 * Claude Code after, and offers a way to Claude Code's sheet where the server
 * offers Claude Code.
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
const GA = /Native Claude Code mode is now generally available\. Use it instead of this sign-in\./;

describe('ModelAccessSection: Claude sign-in end day', () => {
  beforeEach(() => {
    state.oauth = { connected: true, ends_on: '2026-11-30', lifecycle: 'ending' };
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
    state.oauth = { connected: true, ends_on: null, lifecycle: 'active' };
    renderWithProviders(<ModelAccessSection />);
    expect(screen.queryByText(/This sign-in ends on/)).toBeNull();
    expect(screen.queryByText('Legacy')).toBeNull();
    expect(screen.queryByRole('button', SWITCH)).toBeNull();
  });

  it('marks the row Legacy until the day', () => {
    renderWithProviders(<ModelAccessSection />);
    expect(within(claudeListItem()).getByText('Legacy')).toBeInTheDocument();
    expect(screen.queryByText(GA)).toBeNull();
  });

  describe('once the day has passed', () => {
    beforeEach(() => {
      state.oauth = { connected: true, ends_on: null, lifecycle: 'legacy' };
    });

    it('keeps the mark and says to use Claude Code instead, with the way there', () => {
      renderWithProviders(<ModelAccessSection />);
      const row = within(claudeListItem());
      expect(row.getByText('Legacy')).toBeInTheDocument();
      expect(row.getByText(GA)).toBeInTheDocument();
      expect(screen.queryByText(/This sign-in ends on/)).toBeNull();
      fireEvent.click(row.getByRole('button', SWITCH));
      expect(screen.getByRole('dialog', { name: 'Claude Code' })).toBeInTheDocument();
    });

    it('keeps the mark alone where the server does not offer Claude Code', () => {
      state.setup = null;
      renderWithProviders(<ModelAccessSection />);
      expect(within(claudeListItem()).getByText('Legacy')).toBeInTheDocument();
      expect(screen.queryByText(GA)).toBeNull();
      expect(screen.queryByRole('button', SWITCH)).toBeNull();
    });
  });

  it('says nothing while the sign-in status is unread', () => {
    state.oauth = undefined;
    renderWithProviders(<ModelAccessSection />);
    expect(screen.queryByText(/This sign-in ends on/)).toBeNull();
  });
});
