import { describe, it, expect, vi } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { waitFor } from '@testing-library/react';
import { queryKeys } from '@/lib/queryKeys';
import { renderHookWithProviders } from '@/test/utils';
import { invalidateMcpFanout } from '../useMcpServers';
import { invalidateSkillFanout } from '../useSkills';
import { useSubagents } from '../useSubagents';

const get = vi.fn(async () => ({ subagents: [], main: {} }));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getSubagents: () => get(),
}));

/**
 * The subagents list says which preferred connectors are off or gone, whether
 * a brokerage is connected and which preferred skills are gone, and the
 * Plugins page keeps it mounted, so a connector or skill write re-reads it.
 */

describe.each([
  ['connector', invalidateMcpFanout],
  ['skill', invalidateSkillFanout],
])('a %s write', (_kind, fanout) => {
  it('re-reads the subagents list', () => {
    const qc = new QueryClient();
    const spy = vi.spyOn(qc, 'invalidateQueries');
    fanout(qc);
    expect(spy).toHaveBeenCalledWith({ queryKey: queryKeys.subagents.all });
  });
});

/**
 * What it says about the main agent and each row's problems follows writes
 * made away from the page, in Settings or by the agent, which nothing here
 * invalidates, so coming back to the page re-reads it.
 */
describe('a surface showing the list', () => {
  it('re-reads it when it appears again, even while fresh', async () => {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const first = renderHookWithProviders(() => useSubagents(), { queryClient: qc });
    await waitFor(() => expect(first.result.current.isSuccess).toBe(true));
    first.unmount();

    renderHookWithProviders(() => useSubagents(), { queryClient: qc });
    await waitFor(() => expect(get).toHaveBeenCalledTimes(2));
  });
});
