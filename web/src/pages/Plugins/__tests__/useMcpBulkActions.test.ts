import { beforeEach, describe, it, expect, vi } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { catalogProbe, catalogServer } from '@/test/factories';
import type { BuiltinMcpServer } from '@/pages/ChatAgent/utils/api';
import type { BulkTarget } from '../components/useBulkSelection';
import type { PluginListSurface } from '../hooks/usePluginListSurface';

const api = vi.hoisted(() => ({
  setWorkspaceMcpServerEnabled: vi.fn(async () => ({})),
  setMcpCatalogServerNewWorkspaces: vi.fn(async () => ({})),
}));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  setWorkspaceMcpServerEnabled: api.setWorkspaceMcpServerEnabled,
  setMcpCatalogServerNewWorkspaces: api.setMcpCatalogServerNewWorkspaces,
}));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHomeWorkspace: vi.fn(async () => ({ workspace_id: 'home-1' })),
}));

import { queryKeys } from '@/lib/queryKeys';
import { useMcpBulkActions } from '../hooks/useMcpBulkActions';

beforeEach(() => {
  api.setWorkspaceMcpServerEnabled.mockClear();
  api.setMcpCatalogServerNewWorkspaces.mockClear();
});

/**
 * Bulk "All workspaces" has to reach the workspaces not created yet too, or a
 * server added from a workspace keeps a badge reading "2 workspaces" after the
 * button promised all of them. Only user rows have the setting, and only the
 * ones whose flag is off need the call.
 */
describe('useMcpBulkActions: All workspaces', () => {
  it('turns new workspaces on beside the deny clears, on user rows that start off', async () => {
    const workspaces = [
      { id: 'ws-a', name: 'Alpha' },
      { id: 'ws-b', name: 'Beta' },
    ];
    const builtin: BuiltinMcpServer = {
      name: 'builtin_srv',
      description: '',
      transport: 'stdio',
      enabled: true,
      disabled_workspace_ids: ['ws-a'],
    };
    const catalog = [
      // Switched off in one workspace and in new ones: both steps, one target.
      catalogServer({ name: 'off_both', enabled_in_new_workspaces: false, disabled_workspace_ids: ['ws-b'] }),
      // Nothing to clear, but new workspaces still start without it.
      catalogServer({ name: 'off_new', enabled_in_new_workspaces: false }),
      // Already everywhere, including new workspaces: left out of the run.
      catalogServer({ name: 'on_all', enabled_in_new_workspaces: true }),
    ];
    const run = vi.fn<(targets: BulkTarget[]) => void>();
    const surface = {
      selection: {
        selected: new Set([
          'builtin:builtin_srv',
          'catalog:off_both',
          'catalog:off_new',
          'catalog:on_all',
        ]),
      },
      run,
    } as unknown as PluginListSurface;

    const { result } = renderHookWithProviders(() =>
      useMcpBulkActions({ builtins: [builtin], catalog, surface, workspaces }),
    );
    expect(result.current.scope.everywhereCount).toBe(3);

    result.current.scope.onEverywhere();
    const targets = run.mock.calls[0][0];
    expect(targets.map((target) => target.key).sort()).toEqual([
      'builtin:builtin_srv',
      'catalog:off_both',
      'catalog:off_new',
    ]);
    for (const target of targets) await target.run();

    expect(api.setMcpCatalogServerNewWorkspaces.mock.calls).toEqual([
      ['off_both', true],
      ['off_new', true],
    ]);
    expect(api.setWorkspaceMcpServerEnabled.mock.calls).toEqual([
      ['ws-a', 'builtin_srv', true],
      ['ws-b', 'off_both', true],
    ]);

    // A refused flag fails the row's own target, which is what the runner
    // counts toward "N failed".
    api.setMcpCatalogServerNewWorkspaces.mockRejectedValueOnce(new Error('gone'));
    const offNew = targets.find((target) => target.key === 'catalog:off_new');
    await expect(offNew?.run()).rejects.toThrow('gone');
  });
});

/**
 * "Only in A" is the user asking for A alone, so the next workspace has to
 * start without the server too, or the badge goes on reading "All workspaces
 * except 1" and a new workspace gets it anyway. A row with no flag reads as
 * on, the way the badge reads it.
 */
describe('useMcpBulkActions: Only in', () => {
  it('turns new workspaces off beside the deny flips, on user rows that start on', async () => {
    const workspaces = [
      { id: 'ws-a', name: 'Alpha' },
      { id: 'ws-b', name: 'Beta' },
    ];
    const builtin: BuiltinMcpServer = {
      name: 'builtin_srv',
      description: '',
      transport: 'stdio',
      enabled: true,
      disabled_workspace_ids: [],
    };
    const catalog = [
      // Already off in Beta; only what the next workspace starts with changes.
      catalogServer({ name: 'on_new', enabled_in_new_workspaces: true, disabled_workspace_ids: ['ws-b'] }),
      // No flag: counts as on, so both steps run.
      catalogServer({ name: 'absent' }),
      // Already only in Alpha, new workspaces included: left out of the run.
      catalogServer({ name: 'already_only', enabled_in_new_workspaces: false, disabled_workspace_ids: ['ws-b'] }),
    ];
    const run = vi.fn<(targets: BulkTarget[]) => void>();
    const surface = {
      selection: {
        selected: new Set([
          'builtin:builtin_srv',
          'catalog:on_new',
          'catalog:absent',
          'catalog:already_only',
        ]),
      },
      run,
    } as unknown as PluginListSurface;

    const { result } = renderHookWithProviders(() =>
      useMcpBulkActions({ builtins: [builtin], catalog, surface, workspaces }),
    );
    result.current.scope.onOnlyIn(['ws-a']);
    const targets = run.mock.calls[0][0];
    expect(targets.map((target) => target.key).sort()).toEqual([
      'builtin:builtin_srv',
      'catalog:absent',
      'catalog:on_new',
    ]);
    for (const target of targets) await target.run();

    expect(api.setMcpCatalogServerNewWorkspaces.mock.calls).toEqual([
      ['on_new', false],
      ['absent', false],
    ]);
    expect(api.setWorkspaceMcpServerEnabled.mock.calls).toEqual([
      ['ws-b', 'builtin_srv', false],
      ['ws-b', 'absent', false],
    ]);
  });
});

/**
 * A user row lists Home in its checklist and the badge counts it there, so
 * "All workspaces" has to clear a Home deny too, or the row goes on reading
 * "All workspaces except 1" after the button promised all of them. A server
 * added from a workspace starts with exactly that deny.
 */
describe('useMcpBulkActions: All workspaces and Home', () => {
  it('clears a Home deny on a user row, and not on a builtin', async () => {
    const workspaces = [{ id: 'ws-a', name: 'Alpha' }];
    // A builtin's checklist has no Home, and its badge ignores the deny.
    const builtin: BuiltinMcpServer = {
      name: 'builtin_srv',
      description: '',
      transport: 'stdio',
      enabled: true,
      disabled_workspace_ids: ['home-1'],
    };
    const catalog = [catalogServer({ name: 'user_srv', disabled_workspace_ids: ['home-1'] })];
    const run = vi.fn<(targets: BulkTarget[]) => void>();
    const surface = {
      selection: { selected: new Set(['builtin:builtin_srv', 'catalog:user_srv']) },
      run,
    } as unknown as PluginListSurface;

    const { result } = renderHookWithProviders(() =>
      useMcpBulkActions({ builtins: [builtin], catalog, surface, workspaces }),
    );
    await waitFor(() => expect(result.current.scope.everywhereCount).toBe(1));

    result.current.scope.onEverywhere();
    const targets = run.mock.calls[0][0];
    expect(targets.map((target) => target.key)).toEqual(['catalog:user_srv']);
    for (const target of targets) await target.run();
    expect(api.setWorkspaceMcpServerEnabled.mock.calls).toEqual([['home-1', 'user_srv', true]]);
  });
});

/**
 * "Only in" never offers Home as a choice, so on a row whose checklist lists
 * Home, Home is one of the workspaces the pick leaves out. Left on, the
 * badge read "2 workspaces" after the button promised "Only in Alpha".
 */
describe('useMcpBulkActions: Only in and Home', () => {
  it('switches Home off on a user row, and not on a builtin', async () => {
    const workspaces = [
      { id: 'ws-a', name: 'Alpha' },
      { id: 'ws-b', name: 'Beta' },
    ];
    // A builtin's checklist has no Home, so Home is not its to change.
    const builtin: BuiltinMcpServer = {
      name: 'builtin_srv',
      description: '',
      transport: 'stdio',
      enabled: true,
    };
    const catalog = [catalogServer({ name: 'user_srv', enabled_in_new_workspaces: false })];
    const run = vi.fn<(targets: BulkTarget[]) => void>();
    const surface = {
      selection: { selected: new Set(['builtin:builtin_srv', 'catalog:user_srv']) },
      run,
    } as unknown as PluginListSurface;

    const { result, queryClient } = renderHookWithProviders(() =>
      useMcpBulkActions({ builtins: [builtin], catalog, surface, workspaces }),
    );
    // Home resolves through React Query; the row can only name it after that.
    await waitFor(() =>
      expect(queryClient.getQueryData(queryKeys.workspaces.home())).toBeDefined(),
    );

    result.current.scope.onOnlyIn(['ws-a']);
    for (const target of run.mock.calls[0][0]) await target.run();

    expect(api.setWorkspaceMcpServerEnabled.mock.calls).toEqual([
      ['ws-b', 'builtin_srv', false],
      ['ws-b', 'user_srv', false],
      ['home-1', 'user_srv', false],
    ]);
  });
});

/**
 * The backend takes a switch-on for a row a check refused, so a bulk one put a
 * failing server in every workspace while its own switch said it could not be
 * switched on. Bulk holds it as that switch does.
 */
describe('useMcpBulkActions: a row a check refused', () => {
  it('leaves it out of Switch on, and still switches it off', () => {
    const refused = { verdict: 'credential_rejected' as const, missing_secrets: [] };
    const catalog = [
      catalogServer({ name: 'fine', enabled: false }),
      catalogServer({ name: 'refused_off', enabled: false, probe: catalogProbe(refused) }),
      catalogServer({ name: 'refused_on', enabled: true, probe: catalogProbe(refused) }),
    ];
    const run = vi.fn<(targets: BulkTarget[]) => void>();
    const surface = {
      selection: { selected: new Set(catalog.map((s) => `catalog:${s.name}`)) },
      run,
    } as unknown as PluginListSurface;

    const { result } = renderHookWithProviders(() =>
      useMcpBulkActions({ builtins: [], catalog, surface, workspaces: [] }),
    );
    const [enable, disable] = result.current.actions;
    enable.run();
    disable.run();
    expect(run.mock.calls.map(([targets]) => targets.map((target) => target.name))).toEqual([
      ['fine'],
      ['refused_on'],
    ]);
  });
});

/**
 * A row whose plugin is off keeps `enabled: true`, but its own checklist is
 * locked: a deny it cleared could not be put back, and the plugin coming back
 * on would land in workspaces the user never picked from that row.
 */
describe('useMcpBulkActions: a plugin that is off', () => {
  it('leaves its rows out of both scope actions, as their checklists do', () => {
    const workspaces = [
      { id: 'ws-a', name: 'Alpha' },
      { id: 'ws-b', name: 'Beta' },
    ];
    const catalog = [
      catalogServer({ name: 'open', disabled_workspace_ids: ['ws-b'] }),
      catalogServer({ name: 'plugin_off', plugin_enabled: false, disabled_workspace_ids: ['ws-b'] }),
    ];
    const run = vi.fn<(targets: BulkTarget[]) => void>();
    const surface = {
      selection: { selected: new Set(['catalog:open', 'catalog:plugin_off']) },
      run,
    } as unknown as PluginListSurface;

    const { result } = renderHookWithProviders(() =>
      useMcpBulkActions({ builtins: [], catalog, surface, workspaces }),
    );
    expect(result.current.scope.everywhereCount).toBe(1);

    result.current.scope.onEverywhere();
    result.current.scope.onOnlyIn(['ws-a']);
    for (const [targets] of run.mock.calls) {
      expect(targets.map((target) => target.key)).toEqual(['catalog:open']);
    }
  });

  // Under "Off" such a row still has `enabled: true`: "Switch off" counted it
  // and changed nothing on screen, and "Switch on" skipped it unexplained.
  it('switches on and off by the state the pills show, and says why a row is left out', () => {
    const catalog = [
      catalogServer({ name: 'on', enabled: true }),
      catalogServer({ name: 'off', enabled: false }),
      catalogServer({ name: 'held_on', plugin_name: 'wind', plugin_enabled: false, enabled: true }),
      catalogServer({ name: 'held_off', plugin_name: 'wind', plugin_enabled: false, enabled: false }),
    ];
    const run = vi.fn<(targets: BulkTarget[]) => void>();
    const surface = {
      selection: { selected: new Set(catalog.map((s) => `catalog:${s.name}`)) },
      run,
    } as unknown as PluginListSurface;

    const { result } = renderHookWithProviders(() =>
      useMcpBulkActions({ builtins: [], catalog, surface, workspaces: [] }),
    );
    const [enable, disable] = result.current.actions;
    expect(enable.label).toBe('Switch on 1');
    expect(disable.label).toBe('Switch off 1');
    expect(result.current.note).toContain('2 selected are off because their plugin is off.');

    enable.run();
    disable.run();
    expect(run.mock.calls.map(([targets]) => targets.map((target) => target.name))).toEqual([
      ['off'],
      ['on'],
    ]);
  });
});
