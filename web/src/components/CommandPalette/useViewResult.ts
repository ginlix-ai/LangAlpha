import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import type { SearchKind } from '@/api/search';
import { useSymbolSearch } from '@/hooks/useSymbolSearch';
import { resolveDualName } from '@/lib/displayName';
import { readMarkedTicker, readTypedTicker } from '@/lib/marketUtils';
import type { RecentTicker } from '@/lib/recentTickers';
import { queryWords } from '@/lib/textMatch';
import { recentThreadsQuery, workspaceThreadsQuery } from '@/pages/ChatAgent/utils/threadQueries';
import { askRow, fileRow, threadOfHit, threadOfList, threadRow, tickerRow, turnRow, type Verbs } from './rows';
import { searchWithin } from './scope';
import { boostOwnTickers } from './ownTickers';
import { arrangeSearch, findItems, mergeThreads, serverGroup, type Hit } from './search';
import type { LocalSources } from './useLocalSources';
import { useOwnTickers } from './useOwnTickers';
import { useConversationSearch, useSearchPages, type SearchFailure } from './useServerSearch';
import type { PaletteGroup, PaletteItem, PreviewTarget, ViewSpec } from './types';

/** How many rows each group of a view of everything shows. */
const CAP = { threads: 5, turns: 4, files: 4, tickers: 5, workspaces: 4, skills: 4, places: 5, actions: 4 } as const;
/** One more than a server group shows, so a full group knows it has more. */
const SERVER_LIMIT = Math.max(CAP.threads, CAP.turns, CAP.files) + 1;
/** How many recent threads the palette reads: the newest shown before a word is typed, and the titles matched at once while the server answers. */
const RECENT_THREADS = 50;
/** How many of them a view of threads shows before a word is typed. */
const RECENT_SHOWN = 10;
/** How many tickers the main view shows, and the tickers view, which is tickers alone. */
const TICKER_HITS = { main: 6, alone: 12 } as const;
const RECENT_TICKERS = { main: 3, alone: 8 } as const;
const TICKER_FIRST: readonly SearchKind[] = ['threads'];

/** What a view shows for its query, however it was reached. */
export interface ViewResult {
  groups: PaletteGroup[];
  /** Rows may still arrive above the first, so Enter on it waits. */
  rowsPending: boolean;
  /**
   * Only Ask matched here while the server answers, so rows may still land
   * above it; Enter waits for them, but only for a moment, since a stalled
   * request would otherwise hold it for good.
   */
  serverMayLead: boolean;
  /** Rows are on their way: the field shows it's busy. */
  pending: boolean;
  /** Said above the rows: what went wrong, why there are none, or what to type. */
  note: string | null;
  /** What ⌘↵ hands to the agent. */
  ask: PaletteItem | null;
  /** The words searched for, bolded in the preview. */
  words: string[];
  /** Asks for the next page as the list nears its end; null for a view that is not paged. */
  loadMore: (() => void) | null;
  /** A preview pane beside the rows, and what it shows with no row's preview; null for none. */
  preview: { fallback?: PreviewTarget } | null;
}

/** What one view's rows came to, before the parts every view shares. */
interface Shown {
  groups: PaletteGroup[];
  rowsPending?: boolean;
  serverMayLead?: boolean;
  pending?: boolean;
  failure?: SearchFailure | null;
  /** Kinds the server's answer came back without (it ran out of time on them). */
  failedKinds?: readonly SearchKind[];
  hint?: string | null;
}

const group = (id: string, label: string, items: PaletteItem[]): PaletteGroup[] => (items.length > 0 ? [{ id, label, items }] : []);

/** A user's newest threads, everywhere or in one workspace (the list the sidebar reads). */
function useRecentThreads(workspaceId: string | null) {
  const { data, isLoading } = useQuery(
    workspaceId
      ? workspaceThreadsQuery(workspaceId, RECENT_THREADS)
      : recentThreadsQuery(RECENT_THREADS),
  );
  return { threads: data?.threads ?? [], loading: isLoading };
}

/**
 * The tickers the symbol search finds for `query`: the exact symbol typed
 * first, then the user's own that it names (held, watched, opened lately),
 * then the search's order.
 */
function useTickerHits(query: string, cap: number, enabled: boolean, v: Verbs, recent: readonly RecentTicker[]) {
  const { hits, loading } = useSymbolSearch(query, cap, { enabled });
  const own = useOwnTickers(enabled, recent);
  const typed = readTypedTicker(query);
  // The last query's hits stay while the next one loads; they answer
  // another question, so no row offers them.
  const found: Hit[] = (loading || !enabled ? [] : boostOwnTickers(hits, query, own).slice(0, cap))
    .map((hit, index) => {
      const name = resolveDualName(hit, hit.symbol);
      return {
        // The exact symbol typed is the hit everyone means.
        rank: typed && hit.symbol.toUpperCase() === typed.toUpperCase() ? -1 : index,
        item: tickerRow(hit.symbol, name.named ? name.primary : '', hit.exchangeShortName || hit.stockExchange || undefined, v),
      };
    })
    .sort((a, b) => a.rank - b.rank);
  // A symbol in a form no word takes is a chart even when the search did not
  // confirm it (it missed the symbol, or failed): Enter opens it rather than
  // asking. A plain word waits on a hit to say it is a ticker.
  const marked = readMarkedTicker(query);
  if (enabled && marked && !loading && !found.some((hit) => hit.rank < 0)) {
    found.unshift({ rank: -1, item: tickerRow(marked, '', undefined, v) });
  }
  return { hits: found, loading, typed };
}

/**
 * The rows of one view of the palette. Every source is mounted once and
 * asked only for what the view shows: the newest threads (everywhere or in
 * the view's workspace), the symbol search, the server's capped search for a
 * view of everything, its pages of one kind for a view of that kind, and
 * what the browser holds (`local`). A kind shows the same whether its prefix
 * asked for it or its group opened it. Local rows answer on every keystroke;
 * Enter waits (`rowsPending`) only for rows that may land above the first
 * the reader saw: the chart a typed ticker most likely means, the recent
 * threads on a cold open, and the server where it is the only source, or
 * where nothing but Ask matched while it answers.
 */
export function useViewResult({ place, lens }: ViewSpec, query: string, v: Verbs, local: LocalSources): ViewResult {
  const { t } = v;
  // A ticker's view searches for the symbol, and for what is typed with it.
  const text = lens?.kind === 'ticker' ? `${lens.symbol} ${query}`.trim() : query;
  // One array per query, not per render: the preview pane parses its turn
  // again, and scrolls back to the first match, when its words change.
  const words = useMemo(() => queryWords(text), [text]);
  const searching = words.length > 0;
  const within = searchWithin(place);
  const view = lens ? lens.kind : place?.kind === 'thread' ? 'thread' : 'all';

  const recent = useRecentThreads(place?.kind === 'workspace' ? place.workspaceId : null);
  const alone = view === 'tickers';
  const tickers = useTickerHits(query, TICKER_HITS[alone ? 'alone' : 'main'], searching && (alone || (view === 'all' && !place)), v, local.recentTickers);
  const capped = useConversationSearch(query, SERVER_LIMIT, view === 'all' && searching, within);
  const threadPages = useSearchPages(text, 'threads', view === 'threads', within);
  // The ticker's view asks for its threads with its first page of
  // conversations, in one request: its threads never page.
  const turnPages = useSearchPages(text, 'turns', view === 'turns' || view === 'ticker' || view === 'thread', within, view === 'ticker' ? TICKER_FIRST : undefined);
  const filePages = useSearchPages(text, 'files', view === 'files', within);

  const recentRows = recent.threads.map((thread) => threadRow(threadOfList(thread, v), v));
  const recentTickers = (count: number) =>
    local.recentTickers.slice(0, count).map((row) => ({ row, item: tickerRow(row.symbol, '', t('palette.kind.chart'), v) }));
  const noMatches = (rows: readonly unknown[], pending: boolean, failure: SearchFailure | null = null) =>
    searching && rows.length === 0 && !pending && !failure ? t('palette.noMatches') : null;
  // Before a word is typed, a view of threads (or a workspace) lists its newest.
  const newest = (): Shown => ({
    groups: group('recent', t('palette.group.recent'), recentRows.slice(0, RECENT_SHOWN)),
    pending: recent.loading,
    hint: !recent.loading && recentRows.length === 0 ? t('palette.scope.noThreads') : null,
  });
  // Every thread whose title matches: the recent ones at once, the server's pages after them.
  const allThreads = (served: typeof threadPages.threads) =>
    mergeThreads(
      findItems(recentRows, words, RECENT_THREADS),
      served.map((hit) => threadRow(threadOfHit(hit, v), v)),
      words,
      Infinity,
    ).hits.map((hit) => hit.item);
  const turnRows = (inThread = false) => turnPages.turns.map((hit) => turnRow(hit, v, { inThread }));

  const shown = ((): Shown => {
    switch (view) {
      case 'all': {
        if (!searching && place) return newest();
        if (!searching) {
          // Recent places, newest first: threads and the charts opened lately.
          const mixed = [
            ...recentRows.slice(0, 5).map((item, i) => ({ item, at: Date.parse(recent.threads[i].updated_at ?? '') || 0 })),
            ...recentTickers(RECENT_TICKERS.main).map(({ row, item }) => ({ item, at: row.at })),
          ].sort((a, b) => b.at - a.at);
          return {
            groups: [
              ...group('recent', t('palette.group.recent'), mixed.slice(0, 6).map((row) => row.item)),
              ...group('actions', t('palette.group.actions'), local.actions.slice(0, 3)),
            ],
          };
        }
        const { answer } = capped;
        const localThreads = findItems(recentRows, words, CAP.threads);
        const threads = mergeThreads(
          localThreads,
          answer.threads.map((hit) => threadRow(threadOfHit(hit, v), v)),
          words,
          CAP.threads,
        );
        const turns = answer.turns.map((hit, rank) => ({ item: turnRow(hit, v), rank }));
        const files = answer.files.map((hit, rank) => ({ item: fileRow(hit, v), rank }));
        const more = {
          threads: threads.more || !!answer.next_cursor.threads,
          turns: turns.length > CAP.turns || !!answer.next_cursor.turns,
          files: files.length > CAP.files || !!answer.next_cursor.files,
        };
        // With nothing but Ask found here, Enter waits for the server's
        // answer rather than asking: what was typed most likely names
        // something it holds. A row found here stays the one Enter takes.
        const onlyAsk = (found: number) => found === 0 && capped.settling;
        const serverShown = { failure: capped.failure, failedKinds: capped.failedKinds };
        if (place) {
          // A workspace's own threads, conversations and files, each group opening whole.
          const items = { threads: threads.hits, turns: turns.slice(0, CAP.turns), files: files.slice(0, CAP.files) };
          const groups = (['threads', 'turns', 'files'] as const).map((kind) =>
            serverGroup(kind, items[kind].map((hit) => hit.item), more[kind], t),
          );
          return {
            groups: groups.filter((g) => g.items.length > 0),
            rowsPending: recent.loading,
            serverMayLead: onlyAsk(localThreads.length),
            ...serverShown,
          };
        }
        const found = {
          threads: threads.hits,
          turns: turns.slice(0, CAP.turns),
          files: files.slice(0, CAP.files),
          tickers: tickers.hits.slice(0, CAP.tickers),
          workspaces: findItems(local.workspaces, words, CAP.workspaces),
          skills: findItems(local.skills, words, CAP.skills),
          places: findItems([...local.pages, ...local.settings], words, CAP.places),
          actions: findItems(local.actions, words, CAP.actions),
        };
        const here = [localThreads, found.tickers, found.workspaces, found.skills, found.places, found.actions];
        return {
          groups: arrangeSearch(found, more, t),
          rowsPending: (!!tickers.typed && tickers.loading) || recent.loading,
          serverMayLead: onlyAsk(here.reduce((n, hits) => n + hits.length, 0)),
          ...serverShown,
        };
      }
      case 'thread': {
        // What was said in the thread, a page at a time with nothing capped.
        if (!searching) return { groups: [], hint: t('palette.scope.threadHint') };
        const rows = turnRows(true);
        const { settling, pending, failure } = turnPages;
        return { groups: group('turns', t('palette.group.turns'), rows), rowsPending: settling, pending, failure, hint: noMatches(rows, pending, failure) };
      }
      case 'ticker': {
        // The threads that name it come first and stay at their first page,
        // so a page of conversations added at the end never lands above them.
        const symbol = lens?.kind === 'ticker' ? lens.symbol : '';
        return {
          groups: [
            ...group('ticker', t('palette.group.ticker'), [tickerRow(symbol, '', undefined, v, { inView: true })]),
            ...group('threads', t('palette.group.threadsOn', { symbol }), allThreads(turnPages.threads)),
            ...group('turns', t('palette.group.turns'), turnRows()),
          ],
          pending: turnPages.pending,
          failure: turnPages.failure,
          failedKinds: turnPages.failedKinds,
        };
      }
      case 'threads': {
        if (!searching) return newest();
        // The recent threads matched at once lead, so Enter waits for nothing.
        const rows = allThreads(threadPages.threads);
        const { pending, failure } = threadPages;
        return { groups: group('threads', t('palette.group.threads'), rows), pending, failure, hint: noMatches(rows, pending, failure) };
      }
      case 'turns':
      case 'files': {
        if (!searching) return { groups: [], hint: t(view === 'files' ? 'palette.scope.filesHint' : 'palette.scope.turnsHint') };
        // The server is their only source, so Enter waits for it.
        const pages = view === 'files' ? filePages : turnPages;
        const rows = view === 'files' ? filePages.files.map((hit) => fileRow(hit, v)) : turnRows();
        const { settling, pending, failure } = pages;
        return { groups: group(view, t(`palette.group.${view}`), rows), rowsPending: settling, pending, failure, hint: noMatches(rows, pending, failure) };
      }
      case 'tickers': {
        // The charts opened lately until a word is typed.
        const rows = searching ? tickers.hits.map((hit) => hit.item) : recentTickers(RECENT_TICKERS.alone).map(({ item }) => item);
        return {
          groups: group('tickers', t(searching ? 'palette.group.tickers' : 'palette.group.recent'), rows),
          rowsPending: !!tickers.typed && tickers.loading,
          hint: searching ? noMatches(rows, tickers.loading) : rows.length === 0 ? t('palette.scope.tickersHint') : null,
        };
      }
      case 'workspaces': {
        const rows = searching ? findItems(local.workspaces, words, Infinity).map((hit) => hit.item) : local.workspaces;
        return { groups: group('workspaces', t('palette.group.workspaces'), rows), hint: noMatches(rows, false) };
      }
      case 'actions': {
        // The app's actions and pages; settings are found by name.
        const matched = (items: PaletteItem[]) => (searching ? findItems(items, words, Infinity).map((hit) => hit.item) : items);
        const groups = [
          ...group('actions', t('palette.group.actions'), matched(local.actions)),
          ...group('places', t('palette.group.places'), searching ? matched([...local.pages, ...local.settings]) : local.pages),
        ];
        return { groups, hint: groups.length === 0 ? t('palette.noMatches') : null };
      }
    }
  })();

  // ⌘↵ asks the place's agent, or where new work goes; a prefix says what is
  // being looked for, so it asks nothing. The Ask row is a view of
  // everything's (a place's included) last row.
  const ask = lens?.via === 'prefix' ? null : askRow(place, text, v);
  const pages = view === 'threads' ? threadPages : view === 'files' ? filePages : view === 'turns' || view === 'ticker' || view === 'thread' ? turnPages : null;
  const failure = shown.failure ?? null;
  // A kind the server ran out of time on is said once, above the rows; the others still show.
  const kindsNote = (shown.failedKinds ?? []).map((kind) => t(`palette.kindFailed.${kind}`)).join(' ') || null;
  return {
    groups: lens || !ask ? shown.groups : [...shown.groups, { id: 'ask', label: t('palette.group.ask'), items: [ask] }],
    rowsPending: searching && !!shown.rowsPending,
    serverMayLead: searching && !!shown.serverMayLead,
    pending: !!shown.pending,
    note: failure === 'busy' ? t('palette.busy') : failure ? t('palette.searchFailed') : (kindsNote ?? shown.hint ?? null),
    ask,
    words,
    loadMore: pages?.loadMore ?? null,
    // A file has no preview; a thread's own search shows its latest exchange until a row is picked.
    preview: !pages || view === 'files' ? null : { fallback: place?.kind === 'thread' ? { kind: 'thread', threadId: place.threadId } : undefined },
  };
}
