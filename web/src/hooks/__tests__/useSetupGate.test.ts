import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { renderHookWithProviders } from '../../test/utils';
import { useSetupGate, skipSetup } from '../useSetupGate';

vi.mock('../useUser', () => ({
  useUser: vi.fn(),
}));
vi.mock('../useClaudeCode', () => ({
  useClaudeCodeSetup: vi.fn(),
}));

import { useUser } from '../useUser';
import { useClaudeCodeSetup } from '../useClaudeCode';

const mockUseUser = useUser as Mock;
const mockUseClaudeCodeSetup = useClaudeCodeSetup as Mock;

describe('useSetupGate', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    sessionStorage.clear();
    mockUseClaudeCodeSetup.mockReturnValue({ setup: null, isLoading: false, unread: false });
  });

  it('returns needsSetup=false when user has_api_key=true', () => {
    mockUseUser.mockReturnValue({
      user: { user_id: 'u-1', email: 'a@b.com', has_api_key: true, has_oauth_token: false, access_tier: -1 },
      isLoading: false,
    });

    const { result } = renderHookWithProviders(() => useSetupGate());

    expect(result.current.isLoading).toBe(false);
    expect(result.current.needsSetup).toBe(false);
  });

  it('returns needsSetup=false when user has_oauth_token=true', () => {
    mockUseUser.mockReturnValue({
      user: { user_id: 'u-1b', email: 'oauth@b.com', has_api_key: false, has_oauth_token: true, access_tier: -1 },
      isLoading: false,
    });

    const { result } = renderHookWithProviders(() => useSetupGate());

    expect(result.current.isLoading).toBe(false);
    expect(result.current.needsSetup).toBe(false);
  });

  it('returns needsSetup=false when user has platform access (access_tier >= 0)', () => {
    mockUseUser.mockReturnValue({
      user: { user_id: 'u-2', email: 'b@c.com', has_api_key: false, has_oauth_token: false, access_tier: 0 },
      isLoading: false,
    });

    const { result } = renderHookWithProviders(() => useSetupGate());

    expect(result.current.isLoading).toBe(false);
    expect(result.current.needsSetup).toBe(false);
  });

  it('lets a self-hosted user through when the deployment key runs the default model', () => {
    mockUseUser.mockReturnValue({
      user: { user_id: 'u-oss', email: 'o@s.s', has_api_key: false, has_oauth_token: false, access_tier: -1, deployment_serves: true },
      isLoading: false,
    });

    const { result } = renderHookWithProviders(() => useSetupGate());

    expect(result.current.needsSetup).toBe(false);
  });

  // Claude Code runs on the user's own Claude plan, from a token the profile
  // cannot name; on is not enough, since a server can turn it on for everyone.
  const NONE = { user_id: 'u-cc', email: 'c@c.c', has_api_key: false, has_oauth_token: false, access_tier: -1 };
  it.each([
    ['connected', true, false],
    ['refused', true, false],
    ['offline', true, false],
    ['not_connected', true, true],
    ['no_computer', true, true],
    ['connected', false, true],
  ] as const)('with a Claude Code setup %s (on: %s), needsSetup is %s', (state, on, needsSetup) => {
    mockUseUser.mockReturnValue({ user: NONE, isLoading: false });
    mockUseClaudeCodeSetup.mockReturnValue({ setup: { state, on }, isLoading: false, unread: false });

    const { result } = renderHookWithProviders(() => useSetupGate());

    expect(result.current).toEqual({ isLoading: false, needsSetup });
    expect(mockUseClaudeCodeSetup).toHaveBeenLastCalledWith({ enabled: true });
  });

  it('sends no one to setup on a Claude Code setup read that failed', () => {
    mockUseUser.mockReturnValue({ user: NONE, isLoading: false });
    mockUseClaudeCodeSetup.mockReturnValue({ setup: null, isLoading: false, unread: true });

    const { result } = renderHookWithProviders(() => useSetupGate());

    expect(result.current).toEqual({ isLoading: false, needsSetup: false });
  });

  it('waits on the Claude Code setup read only for a user nothing else lets through', () => {
    mockUseUser.mockReturnValue({ user: NONE, isLoading: false });
    mockUseClaudeCodeSetup.mockReturnValue({ setup: null, isLoading: true, unread: false });
    expect(renderHookWithProviders(() => useSetupGate()).result.current.isLoading).toBe(true);

    mockUseUser.mockReturnValue({ user: { ...NONE, has_api_key: true }, isLoading: false });
    const { result } = renderHookWithProviders(() => useSetupGate());
    expect(result.current).toEqual({ isLoading: false, needsSetup: false });
    expect(mockUseClaudeCodeSetup).toHaveBeenLastCalledWith({ enabled: false });
  });

  it('returns needsSetup=true when user has none of the three', () => {
    mockUseUser.mockReturnValue({
      user: { user_id: 'u-3', email: 'c@d.com', has_api_key: false, has_oauth_token: false, access_tier: -1 },
      isLoading: false,
    });

    const { result } = renderHookWithProviders(() => useSetupGate());

    expect(result.current.isLoading).toBe(false);
    expect(result.current.needsSetup).toBe(true);
  });

  it('returns isLoading=true while user data is loading', () => {
    mockUseUser.mockReturnValue({
      user: null,
      isLoading: true,
    });

    const { result } = renderHookWithProviders(() => useSetupGate());

    expect(result.current.isLoading).toBe(true);
    expect(result.current.needsSetup).toBe(false);
  });

  it('returns needsSetup=false when setup was skipped via sessionStorage', () => {
    mockUseUser.mockReturnValue({
      user: { user_id: 'u-skip', email: 'skip@b.com', has_api_key: false, has_oauth_token: false, access_tier: -1 },
      isLoading: false,
    });

    // Simulate clicking "Exit setup"
    skipSetup();

    const { result } = renderHookWithProviders(() => useSetupGate());

    expect(result.current.isLoading).toBe(false);
    expect(result.current.needsSetup).toBe(false);
  });
});
