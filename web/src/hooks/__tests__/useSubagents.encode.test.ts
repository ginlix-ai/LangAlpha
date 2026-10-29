import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { SubagentsResponse, SubagentView } from '@/pages/ChatAgent/utils/api';

/**
 * An edit to a base goes as null where it equals what the base ships with.
 * The preferred lists are read in order (connectors lead the roster, skills
 * preload, in it), so the same names in another order are an edit; the tool
 * and audience lists are sets.
 */

const put = vi.fn();
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  putSubagent: (...args: unknown[]) => put(...args),
}));

const { sendSubagentEdit } = await import('../useSubagents');

const VIEW = {
  name: 'research',
  kind: 'base',
  version: 'v1',
  base: {
    tools: ['web', 'code'],
    preferred_connectors: ['alpha', 'beta'],
    preferred_skills: ['dcf', 'comps'],
  },
} as unknown as SubagentView;

function client(): QueryClient {
  const qc = new QueryClient();
  qc.setQueryData<SubagentsResponse>(queryKeys.subagents.list(), {
    subagents: [VIEW],
  } as unknown as SubagentsResponse);
  return qc;
}

function sent(edit: Record<string, unknown>): unknown {
  void sendSubagentEdit(client(), 'research', edit);
  return put.mock.lastCall?.[1].fields;
}

describe('sendSubagentEdit on a base', () => {
  beforeEach(() => put.mockReset().mockResolvedValue(VIEW));

  it.each([
    ['preferred_connectors', ['beta', 'alpha']],
    ['preferred_skills', ['comps', 'dcf']],
  ])('keeps a reordered %s', (key, value) => {
    expect(sent({ [key]: value })).toEqual({ [key]: value });
  });

  it('sends the shipped order as null', () => {
    expect(sent({ preferred_connectors: ['alpha', 'beta'] })).toEqual({ preferred_connectors: null });
  });

  it('reads the tools as a set', () => {
    expect(sent({ tools: ['code', 'web'] })).toEqual({ tools: null });
  });
});
