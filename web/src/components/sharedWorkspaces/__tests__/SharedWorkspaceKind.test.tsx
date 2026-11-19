import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { SharedWorkspaceEntry, TeamView } from '@/types/sharedWorkspaces';
import { SharedWorkspaceKind } from '../SharedWorkspaceHeader';

const asked: string[] = [];
let answer: () => Promise<TeamView> = () => new Promise(() => {});
vi.mock('@/api/sharedWorkspaces', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/sharedWorkspaces')>()),
  getTeam: (id: string) => {
    asked.push(id);
    return answer();
  },
}));
vi.mock('@/hooks/useAccount', () => ({ useAccount: () => ({ organizationName: 'Northfield' }) }));

function entry(name: string | null): SharedWorkspaceEntry {
  return {
    workspace: { workspace_id: 'ws-1', name: 'Desk' },
    shared: { role: 'manager', team: { id: 'team-1', name } },
  } as unknown as SharedWorkspaceEntry;
}

function show(e: SharedWorkspaceEntry) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <span data-testid="kind"><SharedWorkspaceKind entry={e} /></span>
    </QueryClientProvider>,
  );
  return client;
}

describe('SharedWorkspaceKind', () => {
  beforeEach(() => { asked.length = 0; });

  it('names the team from the list without reading the roster', () => {
    show(entry('Semis desk'));
    expect(screen.getByTestId('kind')).toHaveTextContent('Shared · Northfield · Semis desk · Manager');
    expect(asked).toEqual([]);
  });

  it('reads the name from the roster for someone outside the team', async () => {
    answer = async () => ({ id: 'team-1', name: 'Semis desk', people: [] });
    show(entry(null));
    await waitFor(() => expect(screen.getByTestId('kind')).toHaveTextContent('Shared · Northfield · Semis desk · Manager'));
    expect(asked).toEqual(['ws-1']);
  });

  it('says nothing of the team when the roster cannot be read', async () => {
    answer = async () => { throw new Error('Request failed with status code 503'); };
    const client = show(entry(null));
    await waitFor(() => expect(client.getQueryState(queryKeys.sharedWorkspaces.team('ws-1'))?.status).toBe('error'));
    expect(screen.getByTestId('kind')).toHaveTextContent(/^Shared · Northfield · Manager$/);
    expect(screen.getByTestId('kind')).not.toHaveTextContent('team-1');
  });
});
