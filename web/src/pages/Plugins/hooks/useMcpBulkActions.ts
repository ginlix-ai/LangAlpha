import { useTranslation } from 'react-i18next';
import {
  deleteMcpCatalogServer,
  setBuiltinMcpServerEnabled,
  setMcpCatalogServerEnabled,
  setMcpCatalogServerNewWorkspaces,
  setWorkspaceMcpServerEnabled,
  type BuiltinMcpServer,
  type CatalogServer,
} from '@/pages/ChatAgent/utils/api';
import { useHomeWorkspaceId } from '@/hooks/useHomeWorkspace';
import type { BulkScopeSpec } from '../components/BulkScopeMenu';
import { scopeLocked, type ScopeWorkspace } from '../components/ScopeControl';
import type { BulkRunOptions } from '../components/useBulkSelection';
import { buildBulkActions, type BulkActions } from '../utils/bulkActions';
import { connectorState } from '../utils/connectorState';
import { isEffectivelyEnabled, isPluginOwned, isPluginSuppressed } from '../utils/provenance';
import { connectorRowKey, type ConnectorRow } from '../utils/rowKeys';
import type { PluginListSurface } from './usePluginListSurface';
import { useScopeBulk } from './useScopeBulk';

/** The select-mode actions for the MCP tab, over both row tiers at once. */
export function useMcpBulkActions({
  builtins,
  catalog,
  surface,
  workspaces,
  signIn,
}: {
  builtins: readonly BuiltinMcpServer[];
  catalog: readonly CatalogServer[];
  surface: PluginListSurface;
  workspaces: ScopeWorkspace[];
  /** Starts the sign-in a switch-on was refused for, until the connector is
   *  signed in to. */
  signIn?: BulkRunOptions['signIn'];
}): BulkActions & { scope: BulkScopeSpec } {
  const { t } = useTranslation();
  const { selected } = surface.selection;
  const homeId = useHomeWorkspaceId() ?? null;

  const rows: ConnectorRow[] = [
    ...builtins.map((server) => ({ tier: 'builtin' as const, server })),
    ...catalog.map((server) => ({ tier: 'catalog' as const, server })),
  ].filter((row) => selected.has(connectorRowKey(row)));

  // Same eligibility as each row's ScopeControl: a locked checklist, off or
  // under a plugin that is off, is out. No move: every server lives on the account.
  const scope = useScopeBulk<ConnectorRow>(rows, {
    workspaces,
    // Switching a connector on in a workspace is refused for a missing
    // sign-in as its account switch is.
    run: (targets) => surface.run(targets, { verb: 'scope', noun: 'connector', signIn }),
    key: connectorRowKey,
    name: (row) => row.server.name,
    denyMarkers: (row) =>
      scopeLocked(row.server) ? null : (row.server.disabled_workspace_ids ?? []),
    // Home, for user rows only, as each row's checklist offers it.
    homeWorkspaceId: (row) => (row.tier === 'catalog' ? homeId : null),
    setWorkspaceEnabled: (row, workspaceId, enabled) =>
      setWorkspaceMcpServerEnabled(workspaceId, row.server.name, enabled),
    // Only a user row (brokerages included) has the setting. Absent reads as
    // on, like the badge: every row inherited before the setting existed.
    newWorkspaces: (row, on) => {
      if (row.tier !== 'catalog') return null;
      const current = row.server.enabled_in_new_workspaces !== false;
      return current === on
        ? null
        : () => setMcpCatalogServerNewWorkspaces(row.server.name, on);
    },
  });

  return {
    ...buildBulkActions(t, surface, rows, {
      noun: 'connector',
      key: connectorRowKey,
      name: (row) => row.server.name,
      // Decided on the state the pills show, not on the row's own flag.
      isOn: (row) => isEffectivelyEnabled(row.server),
      held: (row) => isPluginSuppressed(row.server),
      // Held as the row's own switch holds it: a row a check refused would be
      // switched on as asked and sent failing to every workspace. A missing
      // sign-in is left in, since that switch-on is refused before anything is
      // written and the refusal is what offers the sign-in.
      enableLocked: (row) => row.tier === 'catalog' && connectorState(row.server).kind === 'failing',
      setEnabled: (row, on) =>
        row.tier === 'builtin'
          ? setBuiltinMcpServerEnabled(row.server.name, on)
          : setMcpCatalogServerEnabled(row.server.name, on),
      // Builtins have no delete, and plugin-owned rows uninstall through
      // their plugin: bulk delete covers only the user's own catalog rows.
      remove: {
        verb: 'remove',
        labelKey: 'plugins.bulk.remove',
        confirmKey: 'plugins.bulk.confirmRemoveConnectors',
        eligible: (row) => row.tier === 'catalog' && !isPluginOwned(row.server),
        run: (row) => deleteMcpCatalogServer(row.server.name),
      },
      keptKey: 'plugins.bulk.keptBundled',
      signIn,
    }),
    scope,
  };
}
