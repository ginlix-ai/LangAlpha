/**
 * The Claude sign-in's payer tag, which names its models' group in the
 * pickers, is marked legacy once the deployment set the sign-in an end day.
 */
import { describe, expect, it, vi } from 'vitest';
import { renderHook } from '@testing-library/react';
import { subscriptionPayer } from '@/lib/modelPayer';

const status = vi.hoisted(() => ({ data: undefined as unknown }));
vi.mock('@/hooks/useConfiguredProviders', () => ({ useOAuthStatus: () => ({ data: status.data }) }));
vi.mock('@/hooks/useProviderCatalog', () => ({ useProviderCatalog: () => ({ entryFor: () => undefined }) }));

import { usePayerLabel } from '../usePayerLabel';

const label = (lifecycle: string | undefined) => {
  status.data = lifecycle ? { lifecycle } : undefined;
  return renderHook(() => usePayerLabel()).result.current;
};

describe('usePayerLabel: the Claude sign-in', () => {
  it.each(['ending', 'legacy'])('is marked legacy once the deployment set it an end day (%s)', (lifecycle) => {
    const of = label(lifecycle);
    expect(of(subscriptionPayer('claude-oauth'))).toBe('Claude plan (legacy)');
    expect(of(subscriptionPayer('codex-oauth'))).toBe('ChatGPT plan');
    expect(of(subscriptionPayer('claude-code'))).toBe('Claude Code');
  });

  it('keeps its bare name where the caller asks', () => {
    expect(label('legacy')(subscriptionPayer('claude-oauth'), { markLegacy: false })).toBe('Claude plan');
  });

  it.each(['active', undefined])('is named as before otherwise (%s)', (lifecycle) => {
    expect(label(lifecycle)(subscriptionPayer('claude-oauth'))).toBe('Claude plan');
  });
});
