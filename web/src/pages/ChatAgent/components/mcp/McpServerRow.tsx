import React from 'react';
import { useTranslation } from 'react-i18next';
import { Pencil, Zap, Trash2, KeyRound, BookmarkPlus, Blocks } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { BrandMark } from './BrandMark';
import { McpLifecycle } from './McpLifecycle';
import {
  EnabledToggle,
  KebabTrigger,
  ServerNameLine,
  ServerRowShell,
  TagBadge,
} from './McpPrimitives';
import { PluginOriginBadge } from '@/pages/Plugins/components/PluginBadges';
import { isHostDiscovered, isOauthBroken, showsWorkspaceDetail } from './mcpState';
import type { EffectiveServer } from '../../utils/api';

/**
 * One row in the effective per-workspace MCP list.
 *
 * - Origin badge (builtin / inherited / workspace).
 * - Enabled toggle (the only interactive control for builtins).
 * - Tool count + status pill.
 * - Kebab menu: Edit / Test connection / Save to your servers / Delete — all
 *   disabled for builtins. "Test connection" is also disabled when the server
 *   is off (discovery only runs against enabled servers). "Save to your servers"
 *   copies the server's definition up into the user's reusable catalog (vault
 *   refs travel, values don't). A disabled workspace server still renders with
 *   its toggle so it can be re-enabled.
 * - needs_secret rows surface a "Set up NAME" affordance that deep-links to the
 *   Vault tab with the secret name prefilled.
 * - The status area is a single `McpLifecycle` signal (Saved → Verifying →
 *   Ready) that fuses the verify axis (discovery: `checking`/status) and the
 *   apply axis (`synced`: the running agent has loaded the saved config). A
 *   still-progressing server shows an animated track; a verified+applied one
 *   collapses to the clean green pill.
 *
 * The row is a `motion.div` (via `ServerRowShell`): the enabled toggle springs,
 * and rows animate in/out + reflow via `layout` when the parent adds/removes
 * them. The parent freezes display order within a session, so toggling never
 * reorders a row — it just restyles in place.
 */

interface McpServerRowProps {
  server: EffectiveServer;
  /** An enable/disable PATCH is in flight — locks the switch against a double
   *  fire. Optimistic, so it shows NO spinner (the switch already moved). */
  toggling?: boolean;
  /** A delete is in flight — the row is actually leaving, so the kebab shows
   *  the spinner. (Toggle does not.) */
  deleting?: boolean;
  /** A discovery probe is in flight for this row. */
  checking?: boolean;
  /** The running session has applied the saved config (apply axis complete). */
  synced?: boolean;
  /** Whether the workspace sandbox is running. */
  sandboxRunning?: boolean;
  /** The sandbox is warming up toward running (a background apply kicked it). */
  sandboxWarming?: boolean;
  // Handlers receive the row's own `server` (and any extra arg) so the parent
  // can pass ONE stable `useCallback` per action instead of a fresh inline
  // closure per row — that referential stability is what makes `React.memo`
  // below actually skip re-renders during the settling poll / sibling toggles.
  onToggle: (server: EffectiveServer, enabled: boolean) => void;
  onEdit: (server: EffectiveServer) => void;
  onDiscover: (server: EffectiveServer) => void;
  onDelete: (server: EffectiveServer) => void;
  /** Save this workspace server's definition up into the user template catalog.
   *  Builtins pass nothing here, which disables the menu item. */
  onPromoteToTemplate?: (server: EffectiveServer) => void;
  /** Deep-link to the Vault tab, optionally prefilling a secret name. */
  onSetupSecret: (secretName: string) => void;
  /** Navigate to /plugins — offered on inherited (user-origin) rows, whose
   *  definition and OAuth lifecycle are managed there, not per-workspace. */
  onManageInPlugins?: () => void;
}

function McpServerRowImpl({
  server,
  toggling = false,
  deleting = false,
  checking = false,
  synced = false,
  sandboxRunning = false,
  sandboxWarming = false,
  onToggle,
  onEdit,
  onDiscover,
  onDelete,
  onPromoteToTemplate,
  onSetupSecret,
  onManageInPlugins,
}: McpServerRowProps) {
  const { t } = useTranslation();
  const isBuiltin = server.origin === 'builtin';
  const isInherited = server.origin === 'user';
  // Account-level disable: a workspace cannot undo it, and the backend 409s
  // an attempt, so the toggle is inert here and the badge says where to go.
  const lockedByUserTier = server.disabled_scope === 'user';
  // The user-level OAuth connection is broken (revoked / needs reauth) — the
  // only fix is reconnecting on the Plugins page, so the row leads with that.
  const oauthBroken = isOauthBroken(server.oauth_status);
  // The one gate for every piece of workspace-local detail below. Sharing it is
  // the point: when it was spelled out per-gate, the needs_secret one silently
  // dropped the OAuth conjunct and a revoked row offered both "Set up NAME" and
  // "Reconnect in Plugins".
  const showsDetail = showsWorkspaceDetail(server);

  return (
    <ServerRowShell
      testid={`mcp-row-${server.name}`}
      tile={<BrandMark name={server.name} kind="server" />}
      main={
        <>
          <ServerNameLine name={server.name}>
            <TagBadge title={isInherited ? t('mcp.row.inheritedHint') : undefined}>
              {isBuiltin ? t('mcp.row.builtin') : isInherited ? t('mcp.row.inherited') : t('mcp.row.workspace')}
            </TagBadge>
            {server.shadows_inherited && (
              <TagBadge soft title={t('mcp.row.overridesInheritedHint')}>
                {t('mcp.row.overridesInherited')}
              </TagBadge>
            )}
            <PluginOriginBadge plugin={server.plugin_name} />
            {lockedByUserTier && <TagBadge soft>{t('mcp.row.userDisabled')}</TagBadge>}
          </ServerNameLine>

          {/* Lifecycle (verify + apply) + tool count */}
          <div className="flex items-center gap-2 flex-wrap">
            <McpLifecycle
              status={server.status}
              enabled={server.enabled}
              origin={server.origin}
              checking={checking}
              synced={synced}
              sandboxRunning={sandboxRunning}
              sandboxWarming={sandboxWarming}
              oauthStatus={server.oauth_status}
            />
            {showsDetail && server.status === 'connected' && server.tool_count > 0 && (
              <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('mcp.row.toolCount', { count: server.tool_count })}
              </span>
            )}
          </div>

          {/* Error text — silenced when the OAuth pill already names the real
              problem (any cached error predates the disconnect). */}
          {showsDetail && server.status === 'error' && server.error && (
            <p className="text-[0.6875rem] break-words" style={{ color: 'var(--color-loss)' }}>
              {server.error}
            </p>
          )}

          {/* Broken OAuth → "Reconnect in Plugins" affordance */}
          {server.enabled && oauthBroken && onManageInPlugins && (
            <div className="flex flex-wrap gap-1.5">
              <button
                type="button"
                onClick={onManageInPlugins}
                className="inline-flex items-center gap-1 text-[0.6875rem] px-2 py-0.5 rounded"
                style={{
                  color: 'var(--color-warning)',
                  backgroundColor: 'var(--color-bg-tag)',
                  border: '1px dashed var(--color-border-default)',
                }}
              >
                <Blocks className="h-3 w-3" />
                {t('mcp.row.reconnectInPlugins')}
              </button>
            </div>
          )}

          {/* needs_secret → "Set up NAME" affordance(s) */}
          {showsDetail && server.status === 'needs_secret' && server.missing_secrets.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {server.missing_secrets.map((name) => (
                <button
                  key={name}
                  type="button"
                  onClick={() => onSetupSecret(name)}
                  className="inline-flex items-center gap-1 text-[0.6875rem] px-2 py-0.5 rounded"
                  style={{
                    color: 'var(--color-warning)',
                    backgroundColor: 'var(--color-bg-tag)',
                    border: '1px dashed var(--color-border-default)',
                  }}
                >
                  <KeyRound className="h-3 w-3" />
                  {t('mcp.row.setupSecret', { name })}
                </button>
              ))}
            </div>
          )}
        </>
      }
      actions={
        <>
          <EnabledToggle
            enabled={server.enabled}
            name={server.name}
            disabled={toggling || deleting || lockedByUserTier}
            onToggle={() => onToggle(server, !server.enabled)}
          />

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <KebabTrigger busy={deleting} aria-label={t('mcp.row.actionsAria', { name: server.name })} />
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {isInherited && onManageInPlugins && (
                <DropdownMenuItem onSelect={onManageInPlugins}>
                  <Blocks className="h-3.5 w-3.5 mr-2" />
                  {t('mcp.row.manageInPlugins')}
                </DropdownMenuItem>
              )}
              <DropdownMenuItem disabled={!server.editable} onSelect={() => onEdit(server)}>
                <Pencil className="h-3.5 w-3.5 mr-2" />
                {t('mcp.row.edit')}
              </DropdownMenuItem>
              {/* OAuth rows are discovered host-side — the backend 409s an
                  in-sandbox probe, so don't offer one. */}
              <DropdownMenuItem
                disabled={isBuiltin || !server.enabled || isHostDiscovered(server)}
                onSelect={() => onDiscover(server)}
              >
                <Zap className="h-3.5 w-3.5 mr-2" />
                {t('mcp.row.testConnection')}
              </DropdownMenuItem>
              <DropdownMenuItem
                disabled={isBuiltin || !onPromoteToTemplate}
                onSelect={() => onPromoteToTemplate?.(server)}
              >
                <BookmarkPlus className="h-3.5 w-3.5 mr-2" />
                {t('mcp.row.promoteToUser')}
              </DropdownMenuItem>
              <DropdownMenuItem
                disabled={!server.deletable}
                onSelect={() => onDelete(server)}
                variant="destructive"
              >
                <Trash2 className="h-3.5 w-3.5 mr-2" />
                {t('mcp.row.delete')}
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </>
      }
    />
  );
}

/**
 * Memoized so a settling poll / a sibling row's toggle doesn't re-run this row's
 * framer-motion layout work. Effective only because the parent passes a stable
 * `server` object (frozen-order map lookup) and stable `useCallback` handlers.
 */
export const McpServerRow = React.memo(McpServerRowImpl);
