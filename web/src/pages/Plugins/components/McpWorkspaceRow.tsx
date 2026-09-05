import { useTranslation } from 'react-i18next';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import {
  EnabledToggle,
  MetaText,
  ServerNameLine,
  ServerRowShell,
  TagBadge,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import type { WorkspaceScopedMcpServer } from '@/pages/ChatAgent/utils/api';
import { ScopeControl, type ScopeWorkspace } from './ScopeControl';
import { rowSelection, type BulkSelection } from './useBulkSelection';

/**
 * One workspace-local MCP row on the Plugins page. It carries no OAuth
 * lifecycle (connections live only at the user tier) and its only scope
 * destination is up, which is why it is a different row from the catalog one
 * rather than the same row with half its affordances switched off.
 */

export function McpWorkspaceRow({
  server,
  workspaceId,
  workspaces,
  selection,
  moving,
  toggling,
  onOpen,
  onMoveUp,
  onSetEnabled,
}: {
  server: WorkspaceScopedMcpServer;
  workspaceId: string;
  workspaces: ScopeWorkspace[];
  selection: BulkSelection;
  moving: boolean;
  toggling: boolean;
  onOpen: () => void;
  onMoveUp: () => void;
  onSetEnabled: (enabled: boolean) => void;
}) {
  const { t } = useTranslation();

  return (
    <ServerRowShell
      testid={`ws-server-row-${server.name}`}
      {...rowSelection(selection, `ws:${workspaceId}:${server.name}`)}
      tile={<BrandMark name={server.name} kind="server" />}
      onOpen={onOpen}
      main={
        <>
          <ServerNameLine name={server.name} onOpen={onOpen}>
            <MetaText>{server.transport}</MetaText>
            {server.shadows_inherited && (
              <TagBadge soft title={t('mcp.row.overridesInheritedHint')}>
                {t('mcp.row.overridesInherited')}
              </TagBadge>
            )}
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
            scopeWorkspaceId={workspaceId}
            // No cross-workspace move endpoint for MCP servers: the only
            // destination is the user tier.
            allowWorkspaceTargets={false}
            busy={moving}
            moveToAllBlockedReason={
              // The promote endpoint 409s when the name already exists at the
              // user tier, so don't advertise a move that is known to fail for
              // a shadowing row.
              server.shadows_inherited ? t('plugins.scope.moveShadowBlocked') : null
            }
            onMove={(toWorkspaceId) => {
              if (toWorkspaceId === null) onMoveUp();
            }}
          />
          <EnabledToggle
            enabled={server.enabled}
            name={server.name}
            disabled={toggling}
            onToggle={() => onSetEnabled(!server.enabled)}
          />
        </>
      }
    />
  );
}
