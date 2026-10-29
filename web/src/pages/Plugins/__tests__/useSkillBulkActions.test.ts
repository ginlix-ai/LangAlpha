import { beforeEach, describe, it, expect, vi } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import type { BulkTarget } from '../components/useBulkSelection';
import type { PluginListSurface } from '../hooks/usePluginListSurface';
import { skillRowKey } from '../utils/rowKeys';

const api = vi.hoisted(() => ({
  setWorkspaceSkillEnabled: vi.fn(async () => ({})),
}));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  setWorkspaceSkillEnabled: api.setWorkspaceSkillEnabled,
}));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHomeWorkspace: vi.fn(async () => ({ workspace_id: 'home-1' })),
}));

import { queryKeys } from '@/lib/queryKeys';
import { useSkillBulkActions } from '../hooks/useSkillBulkActions';

beforeEach(() => {
  api.setWorkspaceSkillEnabled.mockClear();
});

function skill(overrides: Partial<SkillInfo>): SkillInfo {
  return {
    name: 'skill',
    description: '',
    tool_count: 0,
    tools: [],
    command: null,
    origin: 'user',
    enabled: true,
    editable: true,
    deletable: true,
    confirmed: true,
    plugin_id: null,
    size_bytes: 0,
    updated_at: null,
    disabled_scope: null,
    shadows_inherited: false,
    workspace_id: null,
    disabled_workspace_ids: [],
    ...overrides,
  };
}

function surfaceFor(rows: SkillInfo[]) {
  const run = vi.fn<(targets: BulkTarget[]) => void>();
  const surface = {
    selection: { selected: new Set(rows.map(skillRowKey)) },
    run,
  } as unknown as PluginListSurface;
  return { run, surface };
}

/**
 * Home turns read Home's own skill disables, and every account and platform
 * skill's checklist lists Home, so both bulk actions count it as one of the
 * row's workspaces. A workspace's own skill has no checklist, so "Only in"
 * leaves it where it is.
 */
describe('useSkillBulkActions: Home', () => {
  const workspaces = [
    { id: 'ws-a', name: 'Alpha' },
    { id: 'ws-b', name: 'Beta' },
  ];

  it('clears a Home deny on All workspaces', async () => {
    const rows = [
      skill({ name: 'mine', disabled_workspace_ids: ['home-1'] }),
      skill({ name: 'builtin', origin: 'platform', disabled_workspace_ids: ['home-1'] }),
    ];
    const { run, surface } = surfaceFor(rows);

    const { result } = renderHookWithProviders(() =>
      useSkillBulkActions(rows, surface, workspaces),
    );
    await waitFor(() => expect(result.current.scope.everywhereCount).toBe(2));

    result.current.scope.onEverywhere();
    for (const target of run.mock.calls[0][0]) await target.run();
    expect(api.setWorkspaceSkillEnabled.mock.calls).toEqual([
      ['home-1', 'mine', true],
      ['home-1', 'builtin', true],
    ]);
  });

  it('switches Home off on Only in, which never offers it as a choice', async () => {
    const rows = [
      skill({ name: 'mine' }),
      skill({ name: 'local', origin: 'workspace', workspace_id: 'ws-a' }),
    ];
    const { run, surface } = surfaceFor(rows);

    const { result, queryClient } = renderHookWithProviders(() =>
      useSkillBulkActions(rows, surface, workspaces),
    );
    // Home resolves through React Query; the row can only name it after that.
    await waitFor(() =>
      expect(queryClient.getQueryData(queryKeys.workspaces.home())).toBeDefined(),
    );

    result.current.scope.onOnlyIn(['ws-a']);
    for (const target of run.mock.calls[0][0]) await target.run();
    expect(api.setWorkspaceSkillEnabled.mock.calls).toEqual([
      ['ws-b', 'mine', false],
      ['home-1', 'mine', false],
    ]);
  });
});
