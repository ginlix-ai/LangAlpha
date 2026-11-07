/**
 * What Claude Code turns do at a Claude plan's limit, read from and written
 * to `other_preference.claude_code_plan_out`. Stop is the default, so any
 * other value reads as Stop and choosing Stop deletes the key.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { queryKeys } from '@/lib/queryKeys';

const h = vi.hoisted(() => ({ other: undefined as Record<string, unknown> | undefined, mutateAsync: vi.fn() }));

vi.mock('../usePreferences', () => ({ usePreferences: () => ({ preferences: { other_preference: h.other } }) }));
vi.mock('../useUpdatePreferences', () => ({ useUpdatePreferences: () => ({ mutateAsync: h.mutateAsync }) }));

import { usePlanOut } from '../useClaudeCodePlan';

let client: QueryClient;
const render = () => renderHook(() => usePlanOut(), {
  wrapper: ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>,
});

describe('usePlanOut', () => {
  beforeEach(() => {
    h.other = undefined;
    h.mutateAsync.mockReset();
    client = new QueryClient();
  });

  it('reads Stop unless usage credits were chosen', () => {
    expect(render().result.current.planOut).toBe('stop');
    h.other = { claude_code_plan_out: 'always' };
    expect(render().result.current.planOut).toBe('stop');
    h.other = { claude_code_plan_out: 'credits' };
    expect(render().result.current.planOut).toBe('credits');
  });

  it('saves usage credits, and Stop by deleting the key', () => {
    const { result } = render();
    void result.current.set('credits');
    void result.current.set('stop');
    expect(h.mutateAsync.mock.calls).toEqual([
      [{ other_preference: { claude_code_plan_out: 'credits' } }],
      [{ other_preference: { claude_code_plan_out: null } }],
    ]);
  });

  it('reads the Claude Code plan again once either choice is saved, so a hold credits now carry lifts', async () => {
    const invalidate = vi.spyOn(client, 'invalidateQueries');
    const { result } = render();
    h.mutateAsync.mockResolvedValue({});
    await result.current.set('credits');
    await result.current.set('stop');
    expect(invalidate.mock.calls).toEqual([
      [{ queryKey: queryKeys.features.claudeCode() }],
      [{ queryKey: queryKeys.features.claudeCode() }],
    ]);
    invalidate.mockClear();
    h.mutateAsync.mockRejectedValue(new Error('offline'));
    await expect(result.current.set('credits')).rejects.toThrow('offline');
    expect(invalidate).not.toHaveBeenCalled();
  });
});
