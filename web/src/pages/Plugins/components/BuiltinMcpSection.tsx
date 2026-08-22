import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Server } from 'lucide-react';
import { toast } from '@/components/ui/use-toast';
import {
  useBuiltinMcpServers,
  useToggleBuiltinMcpServer,
  useSetMcpServerEnabledInWorkspace,
} from '@/hooks/useMcpServers';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import {
  EnabledToggle,
  ServerNameLine,
  ServerRowShell,
  TagBadge,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { formatApiErrorDetail } from '@/pages/ChatAgent/utils/api';
import { matchesFilter } from '../utils/groupOrigins';
import { GroupDeck } from './GroupDeck';
import { ScopeControl } from './ScopeControl';
import type { ScopeWorkspace } from './ScopeControl';
import { rowSelection, type BulkSelection } from './useBulkSelection';

/**
 * The `Platform servers` deck: process-global builtins presented read-only,
 * with two affordances — the account-wide disable, and a per-workspace
 * "active in" checklist (deny-list markers). A workspace cannot re-enable a
 * server disabled account-wide, so the checklist locks while the row is off.
 * Filter and selection state live in the parent tab (McpServers), which
 * reads the same builtin query for its bulk actions.
 */

export function BuiltinMcpSection({
  filter = '',
  selection,
}: {
  filter?: string;
  selection?: BulkSelection;
}) {
  const { t } = useTranslation();
  const { data, isLoading } = useBuiltinMcpServers();
  const toggleMutation = useToggleBuiltinMcpServer();
  const wsEnableMutation = useSetMcpServerEnabledInWorkspace();
  const { data: wsData } = useWorkspaces({ limit: 100 });
  // Keyed by row so one row's in-flight toggle doesn't lock its siblings.
  const [busyName, setBusyName] = useState<string | null>(null);

  const servers = (data?.servers ?? []).filter((s) =>
    matchesFilter(filter, s.name, s.description),
  );
  const workspaces = (
    (wsData as { workspaces?: { workspace_id: string; name?: string }[] })
      ?.workspaces ?? []
  );
  const wsOptions: ScopeWorkspace[] = workspaces.map((w) => ({
    id: w.workspace_id,
    name: w.name || t('plugins.scope.unknownWorkspace'),
  }));

  // Loading and empty render nothing: builtins are ambient platform furniture,
  // not the user's own list — a skeleton here would imply their data is late.
  if (isLoading || servers.length === 0) return null;

  async function handleToggle(name: string, enabled: boolean) {
    setBusyName(name);
    try {
      await toggleMutation.mutateAsync({ name, enabled });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.servers.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setBusyName(null);
    }
  }

  async function handleSetWorkspaceDisabled(
    name: string,
    workspaceId: string,
    disabled: boolean,
  ) {
    setBusyName(name);
    try {
      await wsEnableMutation.mutateAsync({ workspaceId, name, enabled: !disabled });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.servers.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setBusyName(null);
    }
  }

  const selecting = !!selection?.selecting;

  return (
    <GroupDeck
      id="mcp:platform"
      title={t('plugins.mcp.platform')}
      icon={Server}
      count={servers.length}
      enabledCount={servers.filter((s) => s.enabled).length}
      forceExpanded={selecting || !!filter.trim()}
      selection={selection}
      selectionKeys={servers.map((s) => `builtin:${s.name}`)}
    >
      <AnimatePresence initial={false}>
        {servers.map((server) => (
          <ServerRowShell
            key={server.name}
            testid={`builtin-row-${server.name}`}
            {...(selection ? rowSelection(selection, `builtin:${server.name}`) : {})}
            main={
              <>
                <ServerNameLine icon={Server} name={server.name}>
                  <TagBadge>{server.transport}</TagBadge>
                  <TagBadge soft>{t('plugins.mcp.platformBadge')}</TagBadge>
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
                  workspaces={wsOptions}
                  scopeWorkspaceId={null}
                  disabledWorkspaceIds={server.disabled_workspace_ids ?? []}
                  checklistLocked={!server.enabled}
                  busy={busyName === server.name}
                  onSetWorkspaceDisabled={(wsId, disabled) =>
                    handleSetWorkspaceDisabled(server.name, wsId, disabled)
                  }
                />
                <EnabledToggle
                  enabled={server.enabled}
                  name={server.name}
                  disabled={busyName === server.name}
                  onToggle={() => handleToggle(server.name, !server.enabled)}
                />
              </>
            }
          />
        ))}
      </AnimatePresence>
    </GroupDeck>
  );
}
