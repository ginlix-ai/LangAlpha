/**
 * Plugin provenance for the Plugins page lists — whether a row came from an
 * installed plugin, and whether that plugin is holding it down.
 *
 * Typed against the concrete rows that carry BOTH provenance fields rather
 * than a structural shape. `EffectiveServer` has `plugin_name` but no
 * `plugin_enabled`, so a structural parameter accepted a workspace row and
 * answered "not suppressed" for it forever, silently; naming the real types
 * turns that gap into a compile error at the call site.
 */
import type { CatalogServer, SkillInfo } from '@/pages/ChatAgent/utils/api';

/** The row tiers a plugin can own: the user MCP catalog and the skill list. */
export type PluginProvenancedRow = CatalogServer | SkillInfo;

/**
 * The row came from an installed plugin. Its config is the plugin's to
 * define; the in-place actions are enable/disable and secret bindings, and an
 * edit detaches it.
 */
export function isPluginOwned(row: PluginProvenancedRow): boolean {
  return !!row.plugin_name;
}

/**
 * Suppressed by its plugin being switched off, so the row's own `enabled` is
 * not the truth the agent sees. Undefined plugin state means "not suppressed":
 * a row we can't prove is held down is shown as it is.
 */
export function isPluginSuppressed(row: PluginProvenancedRow): boolean {
  return isPluginOwned(row) && row.plugin_enabled === false;
}
