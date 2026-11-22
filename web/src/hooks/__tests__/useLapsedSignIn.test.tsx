/**
 * A subscription model is named when its sign-in cannot run a turn: one that
 * has to be renewed, or one that is not there at all once every status has
 * been read, which a status paused offline has not. A working sign-in, a
 * key and an unknown model never are.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { renderHook } from '@testing-library/react';

const status = vi.hoisted(() => ({ statusLoading: false, statusPending: false, statusError: false }));

vi.mock('../useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({
    providers: [
      { provider: 'codex-oauth', access_type: 'oauth', state: 'needs_reauth' },
      { provider: 'claude-oauth', access_type: 'oauth', state: 'connected' },
      { provider: 'openai', access_type: 'api_key', state: 'connected' },
    ],
    ...status,
  }),
}));
vi.mock('../useAllModels', () => ({
  useAllModels: () => ({
    metadata: {
      'gpt-x': { provider: 'codex-oauth' },
      'claude-x': { provider: 'claude-oauth' },
      'grok-x': { provider: 'grok-oauth' },
      'gpt-keyed': { provider: 'openai' },
    },
  }),
}));

import { useLapsedSignIn } from '../useLapsedSignIn';

describe('useLapsedSignIn', () => {
  beforeEach(() => {
    status.statusLoading = false;
    status.statusPending = false;
    status.statusError = false;
  });

  it('names an expired sign-in and a missing one, and nothing else', () => {
    const { result } = renderHook(() => useLapsedSignIn());
    expect(result.current('gpt-x')).toEqual({ provider: 'codex-oauth', reason: 'expired' });
    expect(result.current('grok-x')).toEqual({ provider: 'grok-oauth', reason: 'missing' });
    expect(result.current('claude-x')).toBeNull();
    expect(result.current('gpt-keyed')).toBeNull();
    expect(result.current('unknown')).toBeNull();
    expect(result.current(null)).toBeNull();
  });

  it.each([
    ['still being read', { statusLoading: true, statusPending: true }],
    // A query paused offline is pending without loading.
    ['paused offline', { statusLoading: false, statusPending: true }],
    ['unreadable', { statusError: true }],
  ])('never calls a sign-in missing while the statuses are %s', (_, flags) => {
    Object.assign(status, flags);
    const { result } = renderHook(() => useLapsedSignIn());
    expect(result.current('grok-x')).toBeNull();
    expect(result.current('gpt-x')).toEqual({ provider: 'codex-oauth', reason: 'expired' });
  });
});
