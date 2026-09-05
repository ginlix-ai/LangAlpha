/**
 * Package provenance for the Plugins page lists — which package a row came
 * from, and whether that package is holding it down.
 *
 * Typed against the concrete rows that carry BOTH provenance fields rather
 * than a structural shape. `EffectiveServer` has `plugin_name` but no
 * `plugin_enabled`, so a structural parameter accepted a workspace row and
 * answered "not suppressed" for it forever, silently; naming the real types
 * turns that gap into a compile error at the call site.
 */
import type {
  BuiltinMcpServer,
  CatalogServer,
  SkillInfo,
} from '@/pages/ChatAgent/utils/api';

/**
 * The row tiers a package can own: the user MCP catalog, the skill list, and
 * the built-ins a shipped bundle declares. The last one is owned by a package
 * nobody installed, which is exactly why it belongs here — the row's identity
 * is the same question whether the owner arrived in a zip or in the image.
 */
export type PluginProvenancedRow = CatalogServer | SkillInfo | BuiltinMcpServer;

/**
 * The row came from a package rather than from the user. Its config is the
 * package's to define, so the surfaces offer state rather than shape: enable
 * or disable it, bind its secrets, and, on an installed plugin's row, edit it
 * and thereby detach it.
 */
export function isPluginOwned(row: PluginProvenancedRow): boolean {
  return !!row.plugin_name;
}

/**
 * Suppressed by its package being switched off, so the row's own `enabled` is
 * not the truth the agent sees. Undefined package state means "not
 * suppressed": a row we can't prove is held down is shown as it is.
 */
export function isPluginSuppressed(row: PluginProvenancedRow): boolean {
  return isPluginOwned(row) && row.plugin_enabled === false;
}

/**
 * The state the agent actually sees. A row the user switched on is still off
 * everywhere if its package is off, so filters and counts have to ask this
 * rather than `enabled` — a deck that reads the row's own flag files a
 * suppressed server under Enabled and contradicts its own badge.
 */
export function isEffectivelyEnabled(row: PluginProvenancedRow): boolean {
  return !!row.enabled && !isPluginSuppressed(row);
}
