/**
 * Who can open a shared workspace, read once for every surface: the team it
 * names, by the name the list or the roster gives, the people listed, and
 * how many can open it, each counted once and only while active.
 */
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { SharedWorkspaceEntry } from '@/types/sharedWorkspaces';

const api = vi.hoisted(() => ({ getMembers: vi.fn(), getTeam: vi.fn() }));
vi.mock('@/api/sharedWorkspaces', () => api);

import { useWorkspaceAudience } from '../useSharedWorkspaces';

const person = (user_id: string, active = true) => ({ user_id, name: user_id, email: null, avatar_url: null, role: 'member', active, listed: false });
const entry = (team: { id: string; name: string | null } | null) => ({
  workspace: { workspace_id: 'ws-1', name: 'Desk' },
  shared: { role: 'member', can_manage: false, everyone: false, team },
}) as unknown as SharedWorkspaceEntry;

function wrapper({ children }: { children: React.ReactNode }) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

beforeEach(() => {
  api.getMembers.mockReset();
  api.getTeam.mockReset();
  api.getMembers.mockResolvedValue({
    workspace_id: 'ws-1', everyone: false, sharing: 'none', team: { id: 't1', name: null },
    members: [{ user_id: 'maya', active: true }, { user_id: 'lee', active: true }, { user_id: 'olive', active: false }],
    you: { role: 'member', can_manage: false },
  });
  api.getTeam.mockResolvedValue({ id: 't1', name: 'Semis desk', people: [person('lee'), person('nate'), person('ada', false)] });
});

describe('useWorkspaceAudience', () => {
  it('counts each active person once across the list and the team, and names the team from its roster', async () => {
    const { result } = renderHook(() => useWorkspaceAudience(entry({ id: 't1', name: null }), { people: true, roster: true }), { wrapper });
    await waitFor(() => expect(result.current.reach).toBe(3));
    expect(result.current.team).toMatchObject({ id: 't1', name: 'Semis desk', failed: false });
    expect(result.current.team?.people).toHaveLength(3);
    expect(result.current.listed).toHaveLength(3);
    expect(result.current.inTeam('nate')).toBe(true);
    expect(result.current.inTeam('maya')).toBe(false);
  });

  it('reads no roster for a team the list already names, unless asked', async () => {
    const { result } = renderHook(() => useWorkspaceAudience(entry({ id: 't1', name: 'Semis desk' }), { people: false, roster: 'to name' }), { wrapper });
    expect(result.current.team?.name).toBe('Semis desk');
    expect(result.current.reach).toBeNull();
    expect(api.getTeam).not.toHaveBeenCalled();
    expect(api.getMembers).not.toHaveBeenCalled();
  });

  it('names no team while the roster cannot be read', async () => {
    api.getTeam.mockRejectedValue(new Error('Request failed with status code 503'));
    const { result } = renderHook(() => useWorkspaceAudience(entry({ id: 't1', name: null }), { people: false, roster: 'to name' }), { wrapper });
    await waitFor(() => expect(result.current.team?.failed).toBe(true));
    expect(result.current.team?.name).toBeNull();
  });

  it('is empty outside a shared workspace', () => {
    const { result } = renderHook(() => useWorkspaceAudience(null, { people: true, roster: true }), { wrapper });
    expect(result.current).toMatchObject({ team: null, listed: undefined, reach: null });
  });
});
