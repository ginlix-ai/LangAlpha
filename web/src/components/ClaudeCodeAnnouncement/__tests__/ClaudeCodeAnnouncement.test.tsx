/**
 * The app-wide Claude Code announcement shows to users still signed in with
 * the Claude sign-in once a day is set for it to end, until they have added
 * a token for Claude Code (or it is off) or dismiss it for that day.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { CLAUDE_CODE_CHOICE_URL, CLAUDE_CODE_DOCS } from '@/lib/claudeCode';
import type { ClaudeCodeSetup } from '@/api/model';
import type { OAuthStatus } from '@/api/oauth';

const state = vi.hoisted(() => ({
  oauth: undefined as unknown,
  prefs: null as unknown,
  loaded: true,
  setup: null as unknown,
  mutate: vi.fn(),
  setupArgs: vi.fn(),
}));

vi.mock('@/hooks/useConfiguredProviders', () => ({
  useOAuthStatus: () => ({ data: state.oauth }),
}));
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: state.prefs, isLoading: !state.loaded, isLoaded: state.loaded }),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutate: state.mutate }),
}));
vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeSetup: (args: unknown) => {
    state.setupArgs(args);
    return { setup: state.setup, isLoading: false, unread: false };
  },
}));
vi.mock('@/config/hostMode', async (original) => ({
  ...(await original<typeof import('@/config/hostMode')>()),
  HOST_MODE: 'oss',
  isPlatformMode: false,
}));

import ClaudeCodeAnnouncement from '../ClaudeCodeAnnouncement';

const signedIn = (endsOn: string | null): OAuthStatus => ({
  connected: true, ends_on: endsOn,
} as OAuthStatus);
const setupOf = (over: Partial<ClaudeCodeSetup> = {}): ClaudeCodeSetup => ({
  state: 'not_connected', on: true, computers: [], saved_token_runs: false, cloud: true, too_small: [], ...over,
});
const prefsWith = (dismissed: unknown) => ({ other_preference: { claude_sign_in_notice_dismissed: dismissed } });

const banner = () => screen.queryByRole('region', { name: 'Native Claude Code mode is here' });

describe('ClaudeCodeAnnouncement', () => {
  beforeEach(() => {
    state.oauth = signedIn('2026-11-30');
    state.prefs = {};
    state.loaded = true;
    state.setup = setupOf();
    state.mutate.mockClear();
    state.setupArgs.mockClear();
  });

  it('shows with the end date while the Claude sign-in is connected and ends on a day', () => {
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(banner()).toBeInTheDocument();
    expect(screen.getByText(/The old sign-in ends on Nov 30, 2026\./)).toBeInTheDocument();
  });

  it('shows where the server has Claude Code off, since the sheet turns it on', () => {
    state.setup = setupOf({ on: false });
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(banner()).toBeInTheDocument();
  });

  it.each(['not_connected', 'no_computer', 'unsupported'] as const)(
    'shows while Claude Code is on and its state is %s, since no token is added yet',
    (setupState) => {
      state.setup = setupOf({ on: true, state: setupState });
      renderWithProviders(<ClaudeCodeAnnouncement />);
      expect(banner()).toBeInTheDocument();
    },
  );

  it.each(['connected', 'checking', 'refused', 'offline'] as const)(
    'hides once Claude Code is on and its state is %s, since a token is added',
    (setupState) => {
      state.setup = setupOf({ on: true, state: setupState });
      renderWithProviders(<ClaudeCodeAnnouncement />);
      expect(banner()).toBeNull();
    },
  );

  it.each(['not_connected', 'no_computer', 'unsupported', 'connected', 'checking', 'refused', 'offline'] as const)(
    'shows where Claude Code is off whatever its state, here %s',
    (setupState) => {
      state.setup = setupOf({ on: false, state: setupState });
      renderWithProviders(<ClaudeCodeAnnouncement />);
      expect(banner()).toBeInTheDocument();
    },
  );

  it('hides when the user is not signed in with Claude', () => {
    state.oauth = { connected: false, ends_on: '2026-11-30' };
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(banner()).toBeNull();
  });

  it('hides when the deployment set no end day', () => {
    state.oauth = signedIn(null);
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(banner()).toBeNull();
  });

  it('hides while the sign-in status is unread', () => {
    state.oauth = undefined;
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(banner()).toBeNull();
  });

  it('hides until the preferences are loaded, so a dismissal is not missed', () => {
    state.prefs = null;
    state.loaded = false;
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(banner()).toBeNull();
  });

  it('hides when Claude Code is not offered', () => {
    state.setup = null;
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(banner()).toBeNull();
  });

  it('hides when the user dismissed it for this end day', () => {
    state.prefs = prefsWith('2026-11-30');
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(banner()).toBeNull();
  });

  it('shows again when the dismissal was for an earlier end day', () => {
    state.prefs = prefsWith('2026-11-15');
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(banner()).toBeInTheDocument();
    expect(screen.getByText(/ends on Nov 30, 2026/)).toBeInTheDocument();
  });

  it('ignores a dismissal that is not a day string', () => {
    state.prefs = prefsWith(true);
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(banner()).toBeInTheDocument();
  });

  it('reads the setup only while it is due', () => {
    state.prefs = prefsWith('2026-11-30');
    const { unmount } = renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(state.setupArgs).toHaveBeenLastCalledWith({ enabled: false });
    unmount();
    state.prefs = {};
    renderWithProviders(<ClaudeCodeAnnouncement />);
    expect(state.setupArgs).toHaveBeenLastCalledWith({ enabled: true });
  });

  it('writes the end day as the dismissal when dismissed', () => {
    renderWithProviders(<ClaudeCodeAnnouncement />);
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(state.mutate).toHaveBeenCalledTimes(1);
    expect(state.mutate).toHaveBeenCalledWith({ other_preference: { claude_sign_in_notice_dismissed: '2026-11-30' } });
  });

  it('writes the later day when the end day moved after an earlier dismissal', () => {
    state.prefs = prefsWith('2026-11-15');
    state.oauth = signedIn('2026-12-15');
    renderWithProviders(<ClaudeCodeAnnouncement />);
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(state.mutate).toHaveBeenCalledWith({ other_preference: { claude_sign_in_notice_dismissed: '2026-12-15' } });
  });

  it('links Switch to Claude Code to its sheet in Model access, and Learn more to the docs', () => {
    renderWithProviders(<ClaudeCodeAnnouncement />);
    const switchLink = screen.getByRole('link', { name: 'Switch to Claude Code' });
    expect(switchLink).toHaveAttribute('href', '/settings?tab=model&connect=claude-code-cloud');
    expect(switchLink).toHaveAttribute('href', CLAUDE_CODE_CHOICE_URL);
    const learn = screen.getByRole('link', { name: /Learn more/ });
    expect(learn).toHaveAttribute('href', CLAUDE_CODE_DOCS.fromClaudeSignIn);
    expect(learn).toHaveAttribute('target', '_blank');
  });
});
