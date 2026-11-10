import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { getSearchPreview, searchConversations, type SearchKind, type SearchResponse } from '@/api/search';
import { apiErrorStatus, retryUnlessClientError } from '@/api/errors';
import { queryKeys } from '@/lib/queryKeys';
import { queryWords } from '@/lib/textMatch';
import { useDebouncedValue } from '@/lib/useDebouncedValue';
import type { SearchWithin } from './scope';
import { extendsQuery, narrowAnswer } from './search';
import type { PreviewTarget } from './types';

/**
 * How long the field rests before the server is asked: longer than the gap
 * between keystrokes of a word typed steadily, so a word asks once, and long
 * enough that typing alone stays under the server's 60 searches per 10 s.
 */
export const SEARCH_DEBOUNCE_MS = 250;
/** How long the selection rests on a row before its preview is read: holding ↓ reads none on the way. */
const PREVIEW_DEBOUNCE_MS = 120;
/** One drilled-in page. */
const PAGE = 20;

/** An answer, with the query and the scope it answered, so one is never kept on screen for another scope. */
type Answered = SearchResponse & { q: string; within: string };

const withinKey = ({ workspaceId, threadId }: SearchWithin) => `${workspaceId ?? ''}/${threadId ?? ''}`;
const keeps = (previous: Answered | undefined, q: string, within: string) =>
  !!previous && previous.within === within && extendsQuery(q, previous.q);

/** Why a search has nothing to show: the server is turning requests away for a moment, or failed. */
export type SearchFailure = 'busy' | 'failed';

const failureOf = (error: unknown): SearchFailure => (apiErrorStatus(error) === 429 ? 'busy' : 'failed');

/** A 503 is the server's search timing out on every kind asked: the same words would time out again. */
const retrySearch = (count: number, error: unknown) => apiErrorStatus(error) !== 503 && retryUnlessClientError(count, error);

const EMPTY: SearchResponse = { threads: [], turns: [], files: [], next_cursor: { threads: null, turns: null, files: null }, failed: [] };

/**
 * Threads, turns and files matching `query`, asked once the field rests and
 * cancelled when it changes (React Query aborts the superseded request).
 * While a query that extends the last one is out, the last answer stays,
 * narrowed to the rows the new words still match, so rows already read do
 * not blink out and back. A query that is not an extension shows nothing
 * from the server until it answers. A 429 or 503 is not asked again: the
 * next keystroke asks anyway. A failed answer is `failure`, and nothing is
 * pending after it; `failedKinds` names the kinds an answer came back
 * without.
 */
export function useConversationSearch(query: string, limit: number, enabled: boolean, within: SearchWithin = {}) {
  const live = query.trim();
  const rested = useDebouncedValue(live, SEARCH_DEBOUNCE_MS);
  const scope = withinKey(within);
  // Off, it asks nothing new but keeps following the field, so whatever was
  // last read for it shows the moment it is back on.
  const { data, isPlaceholderData, isFetching, isError, error } = useQuery({
    queryKey: queryKeys.threads.search({ q: rested, limit, ...within }),
    queryFn: async ({ signal }): Promise<Answered> => ({
      ...(await searchConversations({ q: rested, limit, ...within }, { signal })),
      q: rested,
      within: scope,
    }),
    enabled: enabled && rested !== '',
    staleTime: 30_000,
    retry: retrySearch,
    placeholderData: (previous) => (keeps(previous, rested, scope) ? previous : undefined),
  });
  // The field as it stands was asked and failed: nothing more is on its way.
  const failure = live !== '' && rested === live && isError && !isFetching ? failureOf(error) : null;
  const words = queryWords(live);
  const current = !!data && live !== '' && data.q === live && data.within === scope && !isPlaceholderData;
  // An answer to an earlier query is shown only while this one extends it.
  const kept = live !== '' && !current && keeps(data, live, scope);
  const answer = !data || live === '' ? EMPTY : current ? data : kept ? narrowAnswer(data, words) : EMPTY;
  return {
    answer,
    pending: live !== '' && !failure && (!current || isFetching),
    // Still on its way, as `useSearchPages` has it: not paused offline.
    settling: live !== '' && !current && (live !== rested || isFetching),
    failure,
    failedKinds: current ? data.failed : [],
  };
}

const NONE: readonly SearchKind[] = [];

/**
 * One kind of result for a drilled-in view, a page at a time from the
 * server's cursor. While a changed query is out, the pages already read stay
 * on the same terms as `useConversationSearch`: only for an extension, and
 * only the rows the new words still match. `firstAlso` names kinds the first
 * page asks for too, in the same request, and which never page (a cursor
 * pages its own kind alone).
 */
export function useSearchPages(
  query: string,
  kind: SearchKind,
  enabled: boolean,
  within: SearchWithin = {},
  firstAlso: readonly SearchKind[] = NONE,
) {
  const live = enabled ? query.trim() : '';
  const rested = useDebouncedValue(live, SEARCH_DEBOUNCE_MS);
  const scope = withinKey(within);
  const firstKinds = firstAlso.length > 0 ? [...firstAlso, kind] : kind;
  const result = useInfiniteQuery({
    queryKey: queryKeys.threads.search({ q: rested, kinds: firstKinds, limit: PAGE, paged: true, ...within }),
    queryFn: async ({ signal, pageParam }): Promise<Answered> => ({
      ...(await searchConversations(
        { q: rested, kinds: pageParam ? kind : firstKinds, limit: PAGE, cursor: pageParam, ...within },
        { signal },
      )),
      q: rested,
      within: scope,
    }),
    initialPageParam: null as string | null,
    getNextPageParam: (last) => last.next_cursor[kind],
    enabled: rested !== '',
    staleTime: 30_000,
    retry: retrySearch,
    placeholderData: (previous) => (keeps(previous?.pages[0], rested, scope) ? previous : undefined),
  });
  // The field as it stands was asked and failed: nothing more is on its way.
  const failure =
    live !== '' && rested === live && result.isError && !result.isFetching ? failureOf(result.error) : null;
  const pages = result.data?.pages ?? [];
  const first = pages[0];
  const current = live !== '' && first?.q === live && first.within === scope && !result.isPlaceholderData;
  const kept = live !== '' && !current && keeps(first, live, scope);
  const words = queryWords(live);
  const shown = current ? pages : kept ? pages.map((page) => narrowAnswer(page, words)) : [];
  return {
    threads: shown.flatMap((page) => page.threads),
    turns: shown.flatMap((page) => page.turns),
    files: shown.flatMap((page) => page.files),
    loadMore: () => {
      if (current && result.hasNextPage && !result.isFetchingNextPage) void result.fetchNextPage();
    },
    pending: live !== '' && !failure && (!current || result.isFetchingNextPage),
    // Still on its way: the field has not rested, or the request is out. Not
    // a request paused offline, which would hold whatever waits on it for good.
    settling: live !== '' && !current && (live !== rested || result.isFetching),
    failure,
    failedKinds: current ? [...new Set(pages.flatMap((page) => page.failed))] : NONE,
  };
}

const previewKey = (target: PreviewTarget | undefined) =>
  !target || target.kind === 'ticker' ? '' : target.kind === 'turn' ? `${target.threadId}#${target.turnIndex}` : target.threadId;

/** The turn (or a thread's latest exchange) behind the selected row, read once the selection rests. */
export function useSearchPreview(target: PreviewTarget | undefined) {
  const key = previewKey(target);
  const rested = useDebouncedValue(key, PREVIEW_DEBOUNCE_MS);
  const [threadId, turn] = rested.split('#');
  const turnIndex = turn === undefined ? null : Number(turn);
  const result = useQuery({
    queryKey: queryKeys.threads.searchPreview(threadId, turnIndex),
    queryFn: ({ signal }) => getSearchPreview(threadId, turnIndex, { signal }),
    enabled: rested !== '' && rested === key,
    // A turn's words do not change once it ended; a thread's newest turn can.
    staleTime: turnIndex === null ? 30_000 : 5 * 60_000,
    retry: retrySearch,
  });
  const failure: SearchFailure | 'missing' | null =
    rested !== key || !result.isError ? null : apiErrorStatus(result.error) === 404 ? 'missing' : failureOf(result.error);
  return {
    preview: rested === key ? result.data : undefined,
    loading: key !== '' && (rested !== key || result.isPending),
    failure,
  };
}
