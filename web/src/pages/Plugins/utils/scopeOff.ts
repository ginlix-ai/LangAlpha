/** What has a row off before any workspace has a say: its own switch, or its plugin's. */
export type ScopeOff = 'self' | 'plugin' | null;

/**
 * Why a row's scope checklist locks, as the control says it. The plugin wins
 * over the row's own switch: with the plugin off, switching the row on would
 * leave it just as off, so that is not the step to point at.
 */
export function scopeOff(row: {
  enabled?: boolean | null;
  plugin_enabled?: boolean | null;
}): ScopeOff {
  if (row.plugin_enabled === false) return 'plugin';
  return row.enabled ? null : 'self';
}
