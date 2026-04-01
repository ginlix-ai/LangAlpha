import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { renderHookWithProviders } from '../../test/utils';
import { useSetupGate, skipSetup } from '../useSetupGate';

vi.mock('../useUser', () => ({
  useUser: vi.fn(),
}));

import { useUser } from '../useUser';

const mockUseUser = useUser as Mock;

describe('useSetupGate', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    sessionStorage.clear();
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
