/**
 * A model is named as needing sign-in exactly when the subscription it runs
 * on has to be renewed: a working sign-in, a key and an unknown model are not.
 */
import { describe, expect, it, vi } from 'vitest';
import { renderHook } from '@testing-library/react';

vi.mock('../useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({
    providers: [
      { provider: 'codex-oauth', access_type: 'oauth', state: 'needs_reauth' },
      { provider: 'claude-oauth', access_type: 'oauth', state: 'connected' },
      { provider: 'openai', access_type: 'api_key', state: 'connected' },
    ],
  }),
}));
vi.mock('../useAllModels', () => ({
  useAllModels: () => ({
    metadata: {
      'gpt-x': { provider: 'codex-oauth' },
      'claude-x': { provider: 'claude-oauth' },
      'gpt-keyed': { provider: 'openai' },
    },
  }),
}));

import { useLapsedSignIn } from '../useLapsedSignIn';

describe('useLapsedSignIn', () => {
  it('names the subscription only for a model on a lapsed sign-in', () => {
    const { result } = renderHook(() => useLapsedSignIn());
    expect(result.current('gpt-x')).toBe('codex-oauth');
    expect(result.current('claude-x')).toBeNull();
    expect(result.current('gpt-keyed')).toBeNull();
    expect(result.current('unknown')).toBeNull();
    expect(result.current(null)).toBeNull();
  });
});
