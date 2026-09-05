import { useTranslation } from 'react-i18next';
import { MetaText, TagBadge } from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { isPluginSuppressed, type PluginProvenancedRow } from '../utils/provenance';

/**
 * The two provenance badges every plugin-aware surface renders: "this row came
 * from a plugin", and "its plugin is off, so what you see is not what the agent
 * sees".
 *
 * Both existed once per surface and had already drifted — the same badge was a
 * `TagBadge` on the rows, a bare span in the detail overlays, and a hand-copied
 * set of TagBadge's styles in the vault cards. Holding them here makes the two
 * shapes a choice (`variant`) rather than a coincidence.
 *
 * They take different things on purpose. Origin is answered by one field, so
 * that badge takes the plugin name and renders whenever there is one.
 * Suppression is a predicate over two fields (`plugin_name` AND
 * `plugin_enabled === false`), so that badge takes the ROW and evaluates it
 * here: a badge named for a condition it does not check is a trap, and a call
 * site that forgot the guard would mark every plugin-owned row suppressed,
 * enabled or not.
 */

/**
 * Where the badge sits. `chip` is a badge on a row's name line; `prose` is
 * quiet metadata on a status line or in a detail header, where a badge would
 * out-shout the state that actually needs attention.
 */
export type PluginBadgeVariant = 'chip' | 'prose';

/** The row was installed by `plugin`. */
export function PluginOriginBadge({
  /** The owning plugin. Falsy renders nothing, so call sites that only have a
   *  nullable field don't each need their own guard. */
  plugin,
  variant = 'chip',
}: {
  plugin: string | null | undefined;
  variant?: PluginBadgeVariant;
}) {
  const { t } = useTranslation();
  if (!plugin) return null;
  const label = t('plugins.component.fromPlugin', { plugin });
  if (variant === 'prose') return <MetaText>{label}</MetaText>;
  return (
    <TagBadge soft title={label}>
      {plugin}
    </TagBadge>
  );
}

/**
 * The row's plugin is switched off, so the row is suppressed wherever it
 * appears. Renders nothing for a row that is not suppressed — including one
 * that never came from a plugin, and a missing row — so no call site carries
 * its own condition.
 */
export function PluginSuppressedBadge({
  row,
  variant = 'chip',
}: {
  row: PluginProvenancedRow | null | undefined;
  variant?: PluginBadgeVariant;
}) {
  const { t } = useTranslation();
  if (!row || !isPluginSuppressed(row)) return null;
  const label = t('plugins.component.suppressed', { plugin: row.plugin_name });
  if (variant === 'prose') return <MetaText>{label}</MetaText>;
  return (
    <TagBadge soft title={label}>
      {t('plugins.component.suppressedBadge')}
    </TagBadge>
  );
}
