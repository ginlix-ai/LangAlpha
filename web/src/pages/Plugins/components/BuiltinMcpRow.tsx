import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { mcpServerArt } from '@/lib/brandArt';
import {
  EnabledToggle,
  MetaText,
  ServerNameLine,
  ServerRowShell,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import type { BuiltinMcpServer } from '@/pages/ChatAgent/utils/api';
import { ScopeControl, scopeLocked, type ScopeWorkspace } from './ScopeControl';
import { rowSelection, type BulkSelection } from './useBulkSelection';

/**
 * One process-global builtin, presented read-only with two affordances: the
 * account-wide disable, and a per-workspace "active in" checklist (deny-list
 * markers). A workspace cannot re-enable a server disabled account-wide, so
 * the checklist locks while the row is off -- and "off" means the effective
 * state, not this server's own flag. A server whose bundle is switched off
 * keeps `enabled: true` and travels with `plugin_enabled: false`, and the
 * account-level subtraction covers both, so reading only the flag leaves a
 * control that can add a deny it cannot take back.
 *
 * Purely presentational. It holds no query and no mutation: the tab already
 * owns both for its bulk actions and its detail overlay, and React Query
 * dedupes the fetch but not `isPending` — a second copy here gave one row two
 * independent busy states, so toggling from the overlay left the row behind
 * it looking idle.
 */

export function BuiltinMcpRow({
  server,
  workspaces,
  busy,
  selection,
  onOpen,
  onToggle,
  onSetWorkspaceDisabled,
}: {
  server: BuiltinMcpServer;
  workspaces: ScopeWorkspace[];
  /** A write is in flight for this row, whichever surface started it. */
  busy: boolean;
  selection?: BulkSelection;
  /** Open this builtin's detail view (name button + row-body click). */
  onOpen?: () => void;
  onToggle: (enabled: boolean) => void;
  onSetWorkspaceDisabled: (workspaceId: string, disabled: boolean) => void;
}) {
  return (
    <ServerRowShell
      testid={`builtin-row-${server.name}`}
      {...(selection ? rowSelection(selection, `builtin:${server.name}`) : {})}
      tile={<BrandMark name={server.name} kind="server" art={mcpServerArt(server)} />}
      onOpen={onOpen}
      main={
        <>
          <ServerNameLine name={server.name} onOpen={onOpen}>
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
            checklistLocked={scopeLocked(server)}
            busy={busy}
            onSetWorkspaceDisabled={onSetWorkspaceDisabled}
          />
          <EnabledToggle
            enabled={server.enabled}
            name={server.name}
            disabled={busy}
            onToggle={() => onToggle(!server.enabled)}
          />
        </>
      }
    />
  );
}
