import { ChevronsRight } from 'lucide-react';
import type { SearchKind, SearchResponse } from '@/api/search';
import { matchRank, matchSpans, matchesAllWords } from '@/lib/textMatch';
import type { PaletteGroup, PaletteItem } from './types';

type T = (key: string) => string;

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

/**
 * The Threads group: the recent threads matched in the browser first, in the
 * order they showed at once, then the server's finds not among them, in its
 * order (newest first). A server row never lands above a row already shown,
 * so the one the reader is on stays where it is; a thread both found keeps
 * its place and takes the server's meta, which can name its automation.
 * `more` says the group holds more than it shows.
 */
export function mergeThreads(
  local: readonly Hit[],
  server: readonly PaletteItem[],
  words: readonly string[],
  cap: number,
): { hits: Hit[]; more: boolean } {
  const served = new Map(server.map((item) => [item.id, item]));
  const merged: Hit[] = local.map((hit) => {
    const found = served.get(hit.item.id);
    return found ? { item: { ...hit.item, meta: found.meta }, rank: hit.rank } : hit;
  });
  const shown = new Set(local.map((hit) => hit.item.id));
  for (const item of server) {
    if (shown.has(item.id)) continue;
    shown.add(item.id);
    // Ranked as a title the words lead would be, so a found thread named by
    // the query can still be the top hit; one the server matched some other
    // way (a word split differently) ranks last.
    merged.push({ item, rank: matchRank(item.title, words) ?? 3 });
  }
  return { hits: merged.slice(0, cap), more: merged.length > cap };
}

/** The row at a capped group's end that opens the whole group. */
export function showAllRow(kind: SearchKind, t: T): PaletteItem {
  const title = t(`palette.showAll.${kind}`);
  return {
    id: `more:${kind}`,
    icon: ChevronsRight,
    title,
    actions: [{ id: 'open', label: title, icon: ChevronsRight, effect: 'open', view: { kind, via: 'drill' } }],
  };
}

/** A server kind's group in a view of everything: its first rows, then Show all when it holds more. */
export function serverGroup(kind: SearchKind, items: PaletteItem[], more: boolean, t: T): PaletteGroup {
  return { id: kind, label: t(`palette.group.${kind}`), open: { kind, via: 'drill' }, items: more ? [...items, showAllRow(kind, t)] : items };
}

export interface Found {
  threads: Hit[];
  /** Conversation matches, in the server's order. */
  turns: Hit[];
  /** Files in the user's workspaces, in the server's order (names matched first). */
  files: Hit[];
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
 * the first row from what the reader was about to pick. Conversation matches
 * follow the threads, and files the conversations; a file is never the top
 * hit, since it arrives with the server's answer. A server group holding more
 * than it shows (`more`) ends with its Show all row.
 */
export function arrangeSearch(found: Found, more: Readonly<Record<SearchKind, boolean>>, t: T): PaletteGroup[] {
  const named = [found.workspaces, found.skills, found.places, found.actions]
    .map((hits) => hits[0])
    .find((hit) => hit?.rank === 0);
  const exactTicker = found.tickers.find((hit) => hit.rank < 0);
  const top = named ?? exactTicker ?? (found.threads[0]?.rank === 0 ? found.threads[0] : undefined);

  const rest = (hits: Hit[]) => hits.filter((hit) => hit !== top).map((hit) => hit.item);
  const group = (id: string, label: string, hits: Hit[]): PaletteGroup => ({ id, label, items: rest(hits) });
  const tickers = group('tickers', t('palette.group.tickers'), found.tickers);
  return [
    { id: 'top', label: t('palette.group.topHit'), items: top ? [top.item] : [] },
    ...(['threads', 'turns', 'files'] as const).map((kind) => serverGroup(kind, rest(found[kind]), found[kind].length > 0 && more[kind], t)),
    ...(exactTicker ? [tickers] : []),
    group('workspaces', t('palette.group.workspaces'), found.workspaces),
    group('skills', t('palette.group.skills'), found.skills),
    group('places', t('palette.group.places'), found.places),
    group('actions', t('palette.group.actions'), found.actions),
    ...(exactTicker ? [] : [tickers]),
  ].filter((g) => g.items.length > 0);
}

/**
 * A previous answer, kept on screen while the server answers a query that
 * extends it, narrowed to the rows the new words still match, with their
 * spans redrawn for them. Rows the new query no longer matches would answer
 * another question, so none is offered.
 */
export function narrowAnswer(answer: SearchResponse, words: readonly string[]): SearchResponse {
  return {
    threads: answer.threads
      .filter((hit) => matchesAllWords(hit.title, words))
      .map((hit) => ({ ...hit, match: matchSpans(hit.title, words) })),
    turns: answer.turns
      .filter((hit) => matchesAllWords(hit.snippet, words))
      .map((hit) => ({ ...hit, matches: matchSpans(hit.snippet, words) })),
    files: answer.files
      .filter((hit) => matchesAllWords(hit.path, words))
      .map((hit) => ({ ...hit, match: matchSpans(hit.path, words) })),
    next_cursor: answer.next_cursor,
    failed: answer.failed,
  };
}

/** Whether `next` only adds to `previous`, so every row matching it matches `previous` too. */
export function extendsQuery(next: string, previous: string): boolean {
  return previous !== '' && next.toLowerCase().startsWith(previous.toLowerCase());
}
