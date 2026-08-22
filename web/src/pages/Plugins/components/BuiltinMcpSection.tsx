import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Server } from 'lucide-react';
import { IdentityTile } from '@/pages/ChatAgent/components/mcp/IdentityTile';
import {
  EnabledToggle,
  MetaText,
  ServerNameLine,
  ServerRowShell,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import type { BuiltinMcpServer } from '@/pages/ChatAgent/utils/api';
import { GroupDeck } from './GroupDeck';
import { ScopeControl, type ScopeWorkspace } from './ScopeControl';
import { rowSelection, type BulkSelection } from './useBulkSelection';

/**
 * The `Platform servers` deck: process-global builtins presented read-only,
 * with two affordances — the account-wide disable, and a per-workspace
 * "active in" checklist (deny-list markers). A workspace cannot re-enable a
 * server disabled account-wide, so the checklist locks while the row is off.
 *
 * Purely presentational. It used to re-instantiate the query, the two
 * mutations and its own busy key, all of which the parent tab already held for
 * its bulk actions and its detail overlay: React Query dedupes the fetch but
 * not `isPending`, so one builtin row ended up with two independent busy
 * states and toggling from the overlay left the row behind it looking idle.
 */

export function BuiltinMcpSection({
  servers,
  workspaces,
  busyName,
  forceExpanded = false,
  selection,
  onOpen,
  onToggle,
  onSetWorkspaceDisabled,
}: {
  /** Already filtered by the parent's search + state pills. */
  servers: BuiltinMcpServer[];
  workspaces: ScopeWorkspace[];
  /** The one row with a write in flight, whichever surface started it. */
  busyName?: string | null;
  forceExpanded?: boolean;
  selection?: BulkSelection;
  /** Open a builtin's detail view (name button + row-body click). */
  onOpen?: (server: BuiltinMcpServer) => void;
  onToggle: (name: string, enabled: boolean) => void;
  onSetWorkspaceDisabled: (name: string, workspaceId: string, disabled: boolean) => void;
}) {
  const { t } = useTranslation();

  // Empty renders nothing: builtins are ambient platform furniture, not the
  // user's own list — a skeleton here would imply their data is late.
  if (servers.length === 0) return null;

  return (
    <GroupDeck
      id="mcp:platform"
      title={t('plugins.mcp.platform')}
      icon={Server}
      count={servers.length}
      enabledCount={servers.filter((s) => s.enabled).length}
      forceExpanded={forceExpanded}
      selection={selection}
      selectionKeys={servers.map((s) => `builtin:${s.name}`)}
    >
      <AnimatePresence initial={false}>
        {servers.map((server) => (
          <ServerRowShell
            key={server.name}
            testid={`builtin-row-${server.name}`}
            {...(selection ? rowSelection(selection, `builtin:${server.name}`) : {})}
            tile={<IdentityTile name={server.name} />}
            onOpen={onOpen ? () => onOpen(server) : undefined}
            main={
              <>
                <ServerNameLine
                  name={server.name}
                  onOpen={onOpen ? () => onOpen(server) : undefined}
                >
                  <MetaText>{server.transport}</MetaText>
                </ServerNameLine>
                {server.description && (
                  <p
                    className="text-[0.6875rem] line-clamp-2"
                    style={{ color: 'var(--color-text-tertiary)' }}
                  >
                    {server.description}
                  </p>
                )}
              </>
            }
            actions={
              <>
                <ScopeControl
                  workspaces={workspaces}
                  scopeWorkspaceId={null}
                  disabledWorkspaceIds={server.disabled_workspace_ids ?? []}
                  checklistLocked={!server.enabled}
                  busy={busyName === server.name}
                  onSetWorkspaceDisabled={(wsId, disabled) =>
                    onSetWorkspaceDisabled(server.name, wsId, disabled)
                  }
                />
                <EnabledToggle
                  enabled={server.enabled}
                  name={server.name}
                  disabled={busyName === server.name}
                  onToggle={() => onToggle(server.name, !server.enabled)}
                />
              </>
            }
          />
        ))}
      </AnimatePresence>
    </GroupDeck>
  );
}
