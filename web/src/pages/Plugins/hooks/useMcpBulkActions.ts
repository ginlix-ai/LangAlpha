import { useTranslation } from 'react-i18next';
import {
  adoptMcpServerToWorkspace,
  deleteMcpCatalogServer,
  promoteWorkspaceMcpServerToTemplate,
  setBuiltinMcpServerEnabled,
  setMcpCatalogServerEnabled,
  setWorkspaceMcpServerEnabled,
  type BuiltinMcpServer,
  type CatalogServer,
  type WorkspaceScopedMcpServer,
} from '@/pages/ChatAgent/utils/api';
import type { BulkAction } from '../components/BulkActionBar';
import type { BulkScopeSpec } from '../components/BulkScopeMenu';
import type { ScopeWorkspace } from '../components/ScopeControl';
import type { BulkTarget } from '../components/useBulkSelection';
import { isPluginOwned } from '../utils/provenance';
import type { PluginListSurface } from './usePluginListSurface';
import { useScopeBulk } from './useScopeBulk';

/**
 * The select-mode actions for the MCP tab. One selection spans three row
 * tiers with three different endpoints, so the keys are tier-namespaced and
 * the rows travel as a tagged union rather than as three parallel arrays —
 * the scope algorithm downstream wants one list.
 */

type McpScopeRow =
  | { tier: 'builtin'; server: BuiltinMcpServer }
  | { tier: 'catalog'; server: CatalogServer }
  | { tier: 'workspace'; server: WorkspaceScopedMcpServer };

const rowKey = (row: McpScopeRow) =>
  row.tier === 'workspace'
    ? `ws:${row.server.workspace_id}:${row.server.name}`
    : `${row.tier}:${row.server.name}`;

export function useMcpBulkActions({
  builtins,
  catalog,
  workspaceServers,
  surface,
  workspaces,
}: {
  builtins: readonly BuiltinMcpServer[];
  catalog: readonly CatalogServer[];
  workspaceServers: readonly WorkspaceScopedMcpServer[];
  surface: PluginListSurface;
  workspaces: ScopeWorkspace[];
}): { actions: BulkAction[]; scope: BulkScopeSpec } {
  const { t } = useTranslation();
  const { selected } = surface.selection;

  const rows: McpScopeRow[] = [
    ...builtins.map((server) => ({ tier: 'builtin' as const, server })),
    ...catalog.map((server) => ({ tier: 'catalog' as const, server })),
    ...workspaceServers.map((server) => ({ tier: 'workspace' as const, server })),
  ].filter((row) => selected.has(rowKey(row)));

  function toggleTargets(enabled: boolean): BulkTarget[] {
    return rows.flatMap((row) => {
      if (!!row.server.enabled === enabled) return [];
      const key = rowKey(row);
      const { name } = row.server;
      if (row.tier === 'builtin') {
        return [{ key, run: () => setBuiltinMcpServerEnabled(name, enabled) }];
      }
      if (row.tier === 'catalog') {
        return [{ key, run: () => setMcpCatalogServerEnabled(name, enabled) }];
      }
      const workspaceId = row.server.workspace_id;
      return [
        { key, run: () => setWorkspaceMcpServerEnabled(workspaceId, name, enabled) },
      ];
    });
  }

  // Same eligibility as each row's ScopeControl: the deny-list checklist
  // exists on enabled user-tier rows (builtin + catalog); moving into a
  // workspace exists for catalog rows that are neither plugin-owned nor
  // OAuth-connected (connections live only at the user tier); a workspace
  // row's only destination is up, blocked while it shadows an inherited name.
  const movableCatalog = (row: McpScopeRow) =>
    row.tier === 'catalog' &&
    !isPluginOwned(row.server) &&
    !(row.server.oauth_status && row.server.oauth_status !== 'revoked');

  const scope = useScopeBulk<McpScopeRow>(rows, {
    workspaces,
    run: surface.run,
    key: rowKey,
    denyMarkers: (row) =>
      row.tier !== 'workspace' && row.server.enabled
        ? (row.server.disabled_workspace_ids ?? [])
        : null,
    setWorkspaceEnabled: (row, workspaceId, enabled) =>
      setWorkspaceMcpServerEnabled(workspaceId, row.server.name, enabled),
    promote: (row) => {
      if (row.tier !== 'workspace' || row.server.shadows_inherited) return null;
      const { workspace_id: workspaceId, name } = row.server;
      return () => promoteWorkspaceMcpServerToTemplate(workspaceId, name, false, true);
    },
    movable: movableCatalog,
    moveTo: (row, workspaceId) => {
      if (!movableCatalog(row)) return null;
      const { name } = row.server;
      return () => adoptMcpServerToWorkspace(workspaceId, name);
    },
  });

  // Builtins have no delete, and plugin-owned rows uninstall through their
  // plugin — bulk delete covers only the user's own catalog rows.
  const deleteTargets = rows.filter(
    (row) => row.tier === 'catalog' && !isPluginOwned(row.server),
  );
  const enableCount = toggleTargets(true).length;
  const disableCount = toggleTargets(false).length;

  const actions: BulkAction[] = [
    {
      id: 'enable',
      label: t('plugins.bulk.enable', { count: enableCount }),
      disabled: enableCount === 0,
      run: () => surface.run(toggleTargets(true)),
    },
    {
      id: 'disable',
      label: t('plugins.bulk.disable', { count: disableCount }),
      disabled: disableCount === 0,
      run: () => surface.run(toggleTargets(false)),
    },
    {
      id: 'delete',
      label: t('plugins.bulk.delete', { count: deleteTargets.length }),
      destructive: true,
      disabled: deleteTargets.length === 0,
      confirmMessage: t('plugins.bulk.confirmDelete', { count: deleteTargets.length }),
      run: () =>
        surface.run(
          deleteTargets.map((row) => ({
            key: rowKey(row),
            run: () => deleteMcpCatalogServer(row.server.name),
          })),
        ),
    },
  ];

  return { actions, scope };
}
