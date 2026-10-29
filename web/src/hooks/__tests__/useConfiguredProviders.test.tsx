/**
 * A key stored for a provider the catalog no longer has is listed so the user
 * can remove it, and is never a connection: no model, default or "connected"
 * count may read it as one. A subscription waiting to be signed in again is
 * listed for its reconnect, and does not count as access to models.
 */
import { describe, expect, it, vi } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

const KEYS = [
  { provider: 'openai', display_name: 'OpenAI', has_key: true, masked_key: 'sk-...7e48', base_url: null },
  { provider: 'volcengine', display_name: 'volcengine', has_key: true, masked_key: 'ark...91c2', base_url: null, withdrawn: true },
];

const h = vi.hoisted(() => ({
  keys: [] as unknown[],
  status: (_provider: string): Record<string, unknown> => ({ connected: false }),
}));

vi.mock('@/hooks/useApiKeys', () => ({
  useApiKeys: () => ({ isLoading: false, apiKeys: { byok_enabled: true, providers: h.keys } }),
}));
vi.mock('@/api/oauth', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/oauth')>()),
  getOAuthStatus: vi.fn(async (provider: string) => h.status(provider)),
}));

import { useConfiguredProviders } from '../useConfiguredProviders';

describe('useConfiguredProviders', () => {
  it('lists a withdrawn key apart from the connections', () => {
    h.keys = KEYS;
    const { result } = renderHookWithProviders(() => useConfiguredProviders());

    expect(result.current.providers.map((p) => p.provider)).toEqual(['openai']);
    expect(result.current.configuredSet.has('volcengine')).toBe(false);
    expect(result.current.withdrawn.map((p) => [p.provider, p.masked_key])).toEqual([['volcengine', 'ark...91c2']]);
  });

  it('does not count a subscription waiting to be signed in again as access', async () => {
    h.keys = [];
    h.status = (provider) => (provider.startsWith('codex') ? { connected: true, state: 'needs_reauth' } : { connected: false });
    const { result } = renderHookWithProviders(() => useConfiguredProviders());

    await waitFor(() => expect(result.current.providers.map((p) => p.provider)).toEqual(['codex-oauth']));
    expect(result.current.hasAny).toBe(false);
  });
});
