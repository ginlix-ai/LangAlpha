/**
 * What the Plugins page needs to know about a package, asked as questions
 * rather than as `source_type === 'bundled'` spread across the surfaces.
 *
 * Every check the card and the overlay used to make was one of two questions,
 * and neither is "is it bundled": does this package have a lifecycle, and how
 * should its origin read and draw. `bundled` is only the current answer to
 * both. Naming the questions is what makes a second lifecycle-less source (a
 * pinned marketplace entry, a read-only org install) one edit here instead of
 * a dozen across four files.
 */
import type { PluginInfo } from '@/pages/ChatAgent/utils/api/plugins';
import type { BuiltinMcpServer } from '@/pages/ChatAgent/utils/api/mcpBuiltins';
import { bundleArt, mcpServerArt, type BrandArt } from '@/lib/brandArt';
import type { MarkKind } from '@/pages/ChatAgent/components/mcp/KindTile';

type SourceShape = Pick<PluginInfo, 'source_type'>;

/** The one place the literal lives. */
export function isBundled(plugin: SourceShape): boolean {
  return plugin.source_type === 'bundled';
}

/**
 * Whether this package can be installed, updated, exported or removed.
 *
 * A bundle is read from disk rather than installed, so none of those have
 * anything to act on: Update re-fetches a source it never had, Export
 * packages files it does not own, Uninstall has nothing to delete, and bulk
 * selection has nothing to select. All of it answers 404, so the surfaces
 * hide it rather than offering a verb that fails.
 *
 * The enable switch is deliberately NOT one of these. A bundle has no row to
 * carry a flag, but that is a storage detail the page has no business
 * knowing: the answer is stored as the absence of a disable instead, and the
 * switch works the same from here. See `plugins.card.disabledState`.
 */
export function hasLifecycle(plugin: SourceShape): boolean {
  return !isBundled(plugin);
}

/** The i18n key for how this package got here. */
export function sourceLabelKey(plugin: SourceShape): string {
  if (isBundled(plugin)) return 'plugins.card.sourceBundled';
  return plugin.source_type === 'zip'
    ? 'plugins.card.sourceZip'
    : 'plugins.card.sourceRemote';
}

/**
 * The mark a package draws, and the glyph it falls back to.
 *
 * Ours wear our own mark, but only when the manifest hands us no vendor art:
 * a wrapper bundle that fails to fetch the brand it wraps is not one of ours,
 * so it drops to the category glyph rather than to our logo.
 */
export function pluginMark(plugin: PluginInfo): { art?: BrandArt; kind: MarkKind } {
  const art = bundleArt(plugin);
  return { art, kind: isBundled(plugin) && !art ? 'langalpha' : 'plugin' };
}

/**
 * The same rule for a server a bundle ships, so a row matches its bundle's
 * card. One no bundle claims is an operator's own entry in
 * `agent_config.yaml`, which is not ours either and keeps the server glyph.
 */
export function builtinServerMark(
  server: Pick<BuiltinMcpServer, 'icon_url' | 'plugin_name'>,
): { art?: BrandArt; kind: MarkKind } {
  const art = mcpServerArt(server);
  return { art, kind: server.plugin_name && !art ? 'langalpha' : 'server' };
}
