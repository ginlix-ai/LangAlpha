/**
 * Shared workspaces show only for an active member of an organization with
 * the feature on, and a workspace reads as shared only once the caller's
 * list says so: a personal one, and every one with the feature off, never
 * changes how it looks.
 */
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const state = vi.hoisted(() => ({
  flag: true,
  account: { organization: { id: 'org-1', name: 'Northfield' } as { id: string; name: string } | null, suspended: false },
  list: vi.fn(),
}));

vi.mock('../useFeatures', () => ({ useFeatureEnabled: () => state.flag }));
vi.mock('../useAccount', () => ({ useAccount: () => state.account }));
vi.mock('@/api/sharedWorkspaces', () => ({
  listSharedWorkspaces: state.list,
  createSharedWorkspace: vi.fn(),
  getMembers: vi.fn(),
  addMember: vi.fn(),
  setEveryone: vi.fn(),
  setMemberRole: vi.fn(),
  removeMember: vi.fn(),
}));

import { useSharedEntry, useSharedWorkspacesEnabled } from '../useSharedWorkspaces';

const entry = {
  workspace: { workspace_id: 'ws-shared', name: 'Q4 earnings review' },
  shared: { role: 'member', can_manage: false, everyone: false, created_by: 'u-maya' },
};

function wrapper({ children }: { children: React.ReactNode }) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

beforeEach(() => {
  state.flag = true;
  state.account = { organization: { id: 'org-1', name: 'Northfield' }, suspended: false };
  state.list.mockReset();
  state.list.mockResolvedValue({ workspaces: [entry], can_create: true });
});

describe('useSharedWorkspacesEnabled', () => {
  it('is on for an active member of an organization with the feature', () => {
    expect(renderHook(() => useSharedWorkspacesEnabled()).result.current).toBe(true);
  });

  it('is off with the flag off, outside an organization, and for a suspended account', () => {
    state.flag = false;
    expect(renderHook(() => useSharedWorkspacesEnabled()).result.current).toBe(false);
    state.flag = true;
    state.account = { organization: null, suspended: false };
    expect(renderHook(() => useSharedWorkspacesEnabled()).result.current).toBe(false);
    state.account = { organization: { id: 'org-1', name: 'Northfield' }, suspended: true };
    expect(renderHook(() => useSharedWorkspacesEnabled()).result.current).toBe(false);
  });
});

describe('useSharedEntry', () => {
  it('is unknown while the list is read, then the entry or null', async () => {
    const shared = renderHook(() => useSharedEntry('ws-shared'), { wrapper });
    expect(shared.result.current).toBeUndefined();
    await waitFor(() => expect(shared.result.current).toEqual(entry));
    const personal = renderHook(() => useSharedEntry('ws-personal'), { wrapper });
    await waitFor(() => expect(personal.result.current).toBeNull());
  });

  it('reads nothing and is null with the feature off', () => {
    state.flag = false;
    const { result } = renderHook(() => useSharedEntry('ws-shared'), { wrapper });
    expect(result.current).toBeNull();
    expect(state.list).not.toHaveBeenCalled();
  });
});
