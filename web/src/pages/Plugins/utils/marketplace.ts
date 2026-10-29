/**
 * The Discover tab's reading of the catalog: which entries a search and the
 * chips leave, which chips are worth offering, how an entry's coverage reads
 * on one line, and the order the categories stack in.
 *
 * Coverage is an asset-class x region map (`{ equity: ['cn', 'hk'] }`). The
 * Connectors chips are the regions entries name (`namesRegion`), so an entry
 * listing `global` sits under the Global chip, not under every region.
 *
 * Each of the Plugins, Connectors and Skills sections shows the catalog
 * through its own scope (`inScope`): an entry declaring how its connector
 * signs in is a data vendor, the rest are skill collections.
 */
import type {
  MarketplaceCoverage,
  MarketplaceEntry,
  MarketplaceTerm,
} from '@/pages/ChatAgent/utils/api/marketplace';
import { groupBy, matchesFilter } from './groupOrigins';
import { capitalizeFirst } from './sentence';

/** The order sections stack in. Anything else lands in `OTHER_CATEGORY`. */
export const CATEGORY_ORDER = [
  'data',
  'research',
  'modeling',
  'reporting',
  'quant',
  'trading',
] as const;

export const OTHER_CATEGORY = 'other';

export type DiscoverCategory = (typeof CATEGORY_ORDER)[number] | typeof OTHER_CATEGORY;

/** The search box: name, description, publisher and keywords. */
export function matchesQuery(entry: MarketplaceEntry, query: string): boolean {
  return matchesFilter(
    query,
    entry.name,
    entry.description,
    entry.publisher?.name,
    ...(entry.keywords ?? []),
  );
}

/** A term's label in the reader's language; the catalog may omit zh. */
export function termLabel(term: MarketplaceTerm, locale: string): string {
  return locale.startsWith('zh') && term.label_zh ? term.label_zh : term.label;
}

/** A key the vocabulary does not carry still has to read as something. */
function unknownTerm(key: string): MarketplaceTerm {
  return { key, label: key.toUpperCase(), label_zh: null };
}

/**
 * Order keys by the vocabulary, which is the display order, with any key it
 * lacks after them in the order first seen. Returns the terms.
 */
function orderByVocabulary(
  keys: Iterable<string>,
  vocabulary: readonly MarketplaceTerm[],
): MarketplaceTerm[] {
  const wanted = new Set(keys);
  const known = vocabulary.filter((t) => wanted.has(t.key));
  const knownKeys = new Set(known.map((t) => t.key));
  const extra = [...wanted].filter((k) => !knownKeys.has(k)).map(unknownTerm);
  return [...known, ...extra];
}

/**
 * The market chips worth offering: regions some entry actually names. A chip
 * nothing matches can only produce an empty list.
 */
export function usedRegionTerms(
  entries: readonly MarketplaceEntry[],
  vocabulary: readonly MarketplaceTerm[],
): MarketplaceTerm[] {
  const keys = entries.flatMap((e) => Object.values(e.coverage?.markets ?? {}).flat());
  return orderByVocabulary(keys, vocabulary);
}

/** Every market at once: FX, crypto, and vendors that cover stocks worldwide. */
const GLOBAL_REGION = 'global';

/**
 * One line of coverage, e.g. `Equities US · CN · HK  ·  Funds CN  ·  FX`.
 * Asset classes read as labels; regions as their codes, since the full names
 * the chips use ("Mainland China") would push a three-market line past one
 * row. A class whose only region is global shows the class alone, and global
 * beside named markets keeps its label: "GLOBAL" is not a code. Both follow
 * the vocabulary order. The wider gap between classes is meant to render with
 * `white-space: pre`.
 */
export function coverageLine(
  coverage: MarketplaceCoverage | null | undefined,
  vocabulary: { assets: readonly MarketplaceTerm[]; regions: readonly MarketplaceTerm[] },
  locale: string,
): string {
  const markets = coverage?.markets ?? {};
  return orderByVocabulary(Object.keys(markets), vocabulary.assets)
    .map((asset) => {
      const regions = markets[asset.key] ?? [];
      const label = termLabel(asset, locale);
      if (regions.length === 0 || (regions.length === 1 && regions[0] === GLOBAL_REGION)) {
        return label;
      }
      const regionText = orderByVocabulary(regions, vocabulary.regions)
        .map((r) => (r.key === GLOBAL_REGION ? termLabel(r, locale) : r.key.toUpperCase()))
        .join(' · ');
      return `${label} ${regionText}`;
    })
    .join('  ·  ');
}

export function entryCategory(entry: MarketplaceEntry): DiscoverCategory {
  const c = entry.category;
  return c && (CATEGORY_ORDER as readonly string[]).includes(c)
    ? (c as DiscoverCategory)
    : OTHER_CATEGORY;
}

const SECTION_ORDER: readonly DiscoverCategory[] = [...CATEGORY_ORDER, OTHER_CATEGORY];

/** Sections in `CATEGORY_ORDER`, then Other; input order inside each. */
export function groupByCategory(
  entries: readonly MarketplaceEntry[],
): [DiscoverCategory, MarketplaceEntry[]][] {
  const groups = groupBy(entries, entryCategory);
  return SECTION_ORDER.flatMap((c) => {
    const rows = groups.get(c);
    return rows ? [[c, rows] as [DiscoverCategory, MarketplaceEntry[]]] : [];
  });
}

/** The section a Discover view sits in. */
export type DiscoverScope = 'plugins' | 'connectors' | 'skills';

/** The `?tab=` each scope lives on, for links that come back to it. */
export const SCOPE_TAB: Record<DiscoverScope, string> = {
  plugins: 'plugins',
  connectors: 'mcp',
  skills: 'skills',
};

/**
 * Whether installing this entry adds a connector. An entry whose files ship
 * in the catalog says so itself (`contents`). For one that lives upstream the
 * index still says how a vendor's server signs in (`auth`, including `none`
 * for a keyless one), and only an entry carrying a server has anything to
 * sign in to. A skill collection declares no auth.
 */
export function addsConnector(entry: MarketplaceEntry): boolean {
  const parts = entry.contents;
  if (parts && parts.connectors.length + parts.skills.length > 0) {
    return parts.connectors.length > 0;
  }
  return entry.auth !== null || entry.category === 'data';
}

export function inScope(entry: MarketplaceEntry, scope: DiscoverScope): boolean {
  if (scope === 'connectors') return addsConnector(entry);
  if (scope === 'skills') return !addsConnector(entry);
  return true;
}

/** Categories in display order with how many entries each holds. */
export function categoryCounts(
  entries: readonly MarketplaceEntry[],
): [DiscoverCategory, number][] {
  return groupByCategory(entries).map(([c, rows]) => [c, rows.length]);
}

/**
 * The entries to feature. The catalog marks none, so it is the vendors that
 * publish their own server, in catalog order, skipping what the user already
 * has: an installed entry is not news.
 */
export function featuredEntries(
  entries: readonly MarketplaceEntry[],
  count: number,
): MarketplaceEntry[] {
  return entries.filter((e) => e.publisher?.official && !e.installed).slice(0, count);
}

/** Whether an entry covers a region in any asset class: a Connectors market chip. */
export function namesRegion(entry: MarketplaceEntry, region: string): boolean {
  return Object.values(entry.coverage?.markets ?? {}).some((r) => r.includes(region));
}

/**
 * The catalog's `requires` line split into what it asks for. It is written
 * for people ("FactSet subscription", "nothing (shared public rate limit)"),
 * so the only reading done is noticing that nothing is required, and keeping
 * the aside that follows as the note.
 */
export function readRequires(
  requires: string | null | undefined,
): { nothing: true; note: string | null } | { nothing: false; text: string } | null {
  const text = requires?.trim();
  if (!text) return null;
  const nothing = /^nothing\b[\s;:,(]*(.*?)\)?$/i.exec(text);
  if (nothing) return { nothing: true, note: nothing[1] ? capitalizeFirst(nothing[1]) : null };
  return { nothing: false, text: capitalizeFirst(text) };
}

/** The data kinds an entry covers, labelled in the reader's language. */
export function dataLabels(
  entry: MarketplaceEntry,
  vocabulary: readonly MarketplaceTerm[],
  locale: string,
): string[] {
  const known = new Set(vocabulary.map((t) => t.key));
  return orderByVocabulary(entry.coverage?.data ?? [], vocabulary).map((t) =>
    known.has(t.key) ? termLabel(t, locale) : t.key.replace(/-/g, ' '),
  );
}
