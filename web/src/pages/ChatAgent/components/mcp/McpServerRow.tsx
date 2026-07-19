import React from 'react';
import { motion } from 'framer-motion';
import { MoreVertical, Pencil, Zap, Trash2, Server, KeyRound, Loader2, BookmarkPlus } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { McpLifecycle } from './McpLifecycle';
import type { EffectiveServer } from '../../utils/api';

// Matches the spring used across the chat UI (ActivityBlock) so motion feels
// consistent. SNAPPY for the toggle knob; the row layout/enter/exit reuse it.
const SPRING_SNAPPY = { type: 'spring' as const, stiffness: 200, damping: 22 };

/**
 * One row in the effective per-workspace MCP list.
 *
 * - Origin badge (builtin / workspace).
 * - Enabled toggle (the only interactive control for builtins).
 * - Tool count + status pill.
 * - Kebab menu: Edit / Test connection / Save as template / Delete — all
 *   disabled for builtins. "Test connection" is also disabled when the server
 *   is off (discovery only runs against enabled servers). "Save as template"
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
 * The row is a `motion.div`: the enabled toggle springs (no instant teleport),
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
}: McpServerRowProps) {
  const isBuiltin = server.origin === 'builtin';

  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: -4 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, height: 0, marginTop: 0, paddingTop: 0, paddingBottom: 0 }}
      transition={SPRING_SNAPPY}
      className="flex items-start justify-between gap-3 py-2.5 px-3 rounded-lg overflow-hidden"
      style={{ backgroundColor: 'var(--color-bg-card)' }}
      data-testid={`mcp-row-${server.name}`}
    >
      <div className="min-w-0 flex flex-col gap-1">
        {/* Name + origin badge */}
        <div className="flex items-center gap-2 flex-wrap">
          <Server className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
          <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
            {server.name}
          </span>
          <span
            className="text-[10px] px-1.5 py-0.5 rounded uppercase tracking-wide"
            style={{
              color: 'var(--color-text-tertiary)',
              backgroundColor: 'var(--color-bg-default)',
              border: '1px solid var(--color-border-muted)',
            }}
          >
            {isBuiltin ? 'built-in' : 'workspace'}
          </span>
          {server.tool_deny && server.tool_deny.length > 0 && (
            <span
              className="text-[10px] px-1.5 py-0.5 rounded uppercase tracking-wide"
              style={{
                color: 'var(--color-text-tertiary)',
                backgroundColor: 'var(--color-bg-default)',
                border: '1px solid var(--color-border-muted)',
              }}
              title={`Gated tools (never callable): ${server.tool_deny.join(', ')}`}
            >
              {server.tool_deny.length} gated
            </span>
          )}
        </div>

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
          />
          {server.enabled && server.status === 'connected' && server.tool_count > 0 && (
            <span className="text-[11px]" style={{ color: 'var(--color-text-tertiary)' }}>
              {server.tool_count} tool{server.tool_count === 1 ? '' : 's'}
            </span>
          )}
        </div>

        {/* Error text */}
        {server.enabled && server.status === 'error' && server.error && (
          <p className="text-[11px] break-words" style={{ color: 'var(--color-loss)' }}>
            {server.error}
          </p>
        )}

        {/* needs_secret → "Set up NAME" affordance(s) */}
        {server.enabled && server.status === 'needs_secret' && server.missing_secrets.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {server.missing_secrets.map((name) => (
              <button
                key={name}
                type="button"
                onClick={() => onSetupSecret(name)}
                className="inline-flex items-center gap-1 text-[11px] px-2 py-0.5 rounded"
                style={{
                  color: 'var(--color-warning, #d97706)',
                  backgroundColor: 'var(--color-bg-default)',
                  border: '1px dashed var(--color-border-default)',
                }}
              >
                <KeyRound className="h-3 w-3" />
                Set up {name}
              </button>
            ))}
          </div>
        )}
      </div>

      <div className="flex items-center gap-2 flex-shrink-0">
        {/* Enabled toggle */}
        <button
          type="button"
          role="switch"
          aria-checked={server.enabled}
          aria-label={`${server.enabled ? 'Disable' : 'Enable'} ${server.name}`}
          disabled={toggling || deleting}
          onClick={() => onToggle(server, !server.enabled)}
          className="relative inline-flex h-5 w-9 items-center rounded-full transition-colors"
          style={{
            backgroundColor: server.enabled ? 'var(--color-accent-primary)' : 'var(--color-border-muted)',
          }}
        >
          <motion.span
            className="inline-block h-4 w-4 rounded-full bg-white"
            animate={{ x: server.enabled ? 18 : 2 }}
            transition={SPRING_SNAPPY}
          />
        </button>

        {/* Kebab menu */}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button
              type="button"
              className="p-1.5 rounded transition-colors hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
              aria-label={`Actions for ${server.name}`}
            >
              {deleting ? <Loader2 className="h-4 w-4 animate-spin" /> : <MoreVertical className="h-4 w-4" />}
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem disabled={!server.editable} onSelect={() => onEdit(server)}>
              <Pencil className="h-3.5 w-3.5 mr-2" />
              Edit
            </DropdownMenuItem>
            <DropdownMenuItem disabled={isBuiltin || !server.enabled} onSelect={() => onDiscover(server)}>
              <Zap className="h-3.5 w-3.5 mr-2" />
              Test connection
            </DropdownMenuItem>
            <DropdownMenuItem
              disabled={isBuiltin || !onPromoteToTemplate}
              onSelect={() => onPromoteToTemplate?.(server)}
            >
              <BookmarkPlus className="h-3.5 w-3.5 mr-2" />
              Save as template
            </DropdownMenuItem>
            <DropdownMenuItem
              disabled={!server.deletable}
              onSelect={() => onDelete(server)}
              variant="destructive"
            >
              <Trash2 className="h-3.5 w-3.5 mr-2" />
              Delete
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </motion.div>
  );
}

/**
 * Memoized so a settling poll / a sibling row's toggle doesn't re-run this row's
 * framer-motion layout work. Effective only because the parent passes a stable
 * `server` object (frozen-order map lookup) and stable `useCallback` handlers.
 */
export const McpServerRow = React.memo(McpServerRowImpl);
