// @vitest-environment node
import { describe, it, expect, vi, beforeEach } from 'vitest';

const mockPost = vi.fn();

async function load(platform: boolean) {
  vi.resetModules();
  vi.doMock('@/api/client', () => ({ api: { post: mockPost } }));
  vi.doMock('@/config/hostMode', () => ({
    HOST_MODE: platform ? 'platform' : 'oss',
    isPlatformMode: platform,
  }));
  return (await import('../connectedApps')).revokeAllConnectedApps;
}

describe('revokeAllConnectedApps', () => {
  beforeEach(() => {
    mockPost.mockReset();
  });

  it('asks the platform to sign every connected app out', async () => {
    mockPost.mockResolvedValue({ data: { revoked: 2 } });
    const revoke = await load(true);
    revoke();
    expect(mockPost).toHaveBeenCalledWith('/api/auth/connected-apps/revoke-all');
  });

  it('swallows a failure rather than handing it to the caller', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    mockPost.mockRejectedValue(new Error('Network Error'));
    const revoke = await load(true);

    expect(() => revoke()).not.toThrow();
    await vi.waitFor(() => expect(warn).toHaveBeenCalled());
    warn.mockRestore();
  });

  it('does nothing in a self-hosted build, which has no platform service', async () => {
    const revoke = await load(false);
    revoke();
    expect(mockPost).not.toHaveBeenCalled();
  });
});
