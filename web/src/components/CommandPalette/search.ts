import { matchRank, matchSpans } from '@/lib/textMatch';
import type { PaletteGroup, PaletteItem } from './types';

const CJK = /[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}\p{Script=Hangul}]/u;

/** An item a search found, and how well: lower is better, below zero the exact ticker typed. */
export interface Hit {
  item: PaletteItem;
  rank: number;
}

/**
 * The items whose title (or keywords) hold every word, best first and then in
 * the order given, each with the spans of its title the words cover.
 */
export function findItems(items: readonly PaletteItem[], words: readonly string[], cap: number): Hit[] {
  // One or two letters match too much mid-word ("nv" in "conventions"), so a
  // query that short finds only titles with a word starting with it. Not so
  // for Chinese, Japanese or Korean, whose words are one or two characters
  // and whose titles have no spaces to start a word after.
  const phrase = words.join(' ');
  const short = phrase.length < 3 && !CJK.test(phrase);
  return items
    .flatMap((item, index) => {
      const rank = matchRank(item.title, words, item.keywords);
      if (rank === null || (short && rank > 1)) return [];
      return [{ item: { ...item, spans: matchSpans(item.title, words) }, rank, index }];
    })
    .sort((a, b) => a.rank - b.rank || a.index - b.index)
    .slice(0, cap)
    .map(({ item, rank }) => ({ item, rank }));
}

export interface Found {
  threads: Hit[];
  /** In the symbol search's order, the exact symbol typed (if found) ranked below zero. */
  tickers: Hit[];
  workspaces: Hit[];
  skills: Hit[];
  places: Hit[];
  actions: Hit[];
}

/**
 * What a search shows, in groups. The top hit is a workspace, skill, page or
 * action whose name starts with the query, then the exact ticker typed, then a
 * thread whose title starts with it. A short word reads as a ticker too ("dcf"
 * is a fund), so a ticker leads only when nothing of the app's own is called
 * that; a thread named after a ticker ("NVDA Q3 preview") still yields to the
 * chart. Tickers found by company name ("dark" finds Darktrace) come after the
 * app's own matches: they arrive a beat later, and landing above would take
 * the first row from what the reader was about to pick. Ask, when offered, is last.
 */
export function arrangeSearch(found: Found, ask: PaletteItem | null, t: (key: string) => string): PaletteGroup[] {
  const named = [found.workspaces, found.skills, found.places, found.actions]
    .map((hits) => hits[0])
    .find((hit) => hit?.rank === 0);
  const exactTicker = found.tickers.find((hit) => hit.rank < 0);
  const top = named ?? exactTicker ?? (found.threads[0]?.rank === 0 ? found.threads[0] : undefined);

  const group = (id: string, label: string, hits: Hit[]): PaletteGroup => ({
    id,
    label,
    items: hits.filter((hit) => hit !== top).map((hit) => hit.item),
  });
  const tickers = group('tickers', t('palette.group.tickers'), found.tickers);
  return [
    { id: 'top', label: t('palette.group.topHit'), items: top ? [top.item] : [] },
    group('threads', t('palette.group.threads'), found.threads),
    ...(exactTicker ? [tickers] : []),
    group('workspaces', t('palette.group.workspaces'), found.workspaces),
    group('skills', t('palette.group.skills'), found.skills),
    group('places', t('palette.group.places'), found.places),
    group('actions', t('palette.group.actions'), found.actions),
    ...(exactTicker ? [] : [tickers]),
    { id: 'ask', label: t('palette.group.ask'), items: ask ? [ask] : [] },
  ].filter((g) => g.items.length > 0);
}
