import { bundleArt, type BrandArt } from '@/lib/brandArt';
import type { MarkKind } from '@/pages/ChatAgent/components/mcp/KindTile';
import type { MarketplaceEntry, PluginInfo } from '@/pages/ChatAgent/utils/api';
import { pluginMark } from '../../utils/pluginSurface';
import type { DiscoverCategory } from '../../utils/marketplace';

// Explicit maps so the i18n parity test sees each key as a literal.
export const CATEGORY_KEYS: Record<DiscoverCategory, string> = {
  data: 'plugins.discoverPage.categories.data',
  research: 'plugins.discoverPage.categories.research',
  modeling: 'plugins.discoverPage.categories.modeling',
  reporting: 'plugins.discoverPage.categories.reporting',
  quant: 'plugins.discoverPage.categories.quant',
  trading: 'plugins.discoverPage.categories.trading',
  other: 'plugins.discoverPage.categories.other',
};

export const ACCESS_KEYS: Record<NonNullable<MarketplaceEntry['auth']>, string> = {
  oauth: 'plugins.discoverPage.access.oauth',
  'api-key': 'plugins.discoverPage.access.apiKey',
  none: 'plugins.discoverPage.access.none',
};

/** How an entry signs in, as a locale key; null for a skill collection. */
export function accessKey(auth: MarketplaceEntry['auth']): string | null {
  return auth ? ACCESS_KEYS[auth] : null;
}

/**
 * The mark an entry draws. One logo per vendor: once installed, the plugin's
 * own mark, which is what every other surface shows for it; before that, the
 * catalog's.
 */
export function entryArt(
  entry: MarketplaceEntry,
  plugin: PluginInfo | undefined,
): { art?: BrandArt; kind?: MarkKind } {
  return plugin ? pluginMark(plugin) : { art: bundleArt(entry) };
}
