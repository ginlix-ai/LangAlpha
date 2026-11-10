import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { otherPreferencePatch, readOtherPreference } from '@/lib/otherPreference';
import { preferenceWriteLock } from '@/lib/preferenceLock';
import { announcePreferencesChanged } from '@/lib/preferencesChannel';
import { queryKeys } from '@/lib/queryKeys';
import { getPreferences } from '@/pages/Dashboard/utils/api';
import type { UserPreferences } from '@/types/api';
import { usePreferences } from './usePreferences';
import { PREFERENCE_MUTATION_KEY, useUpdatePreferences } from './useUpdatePreferences';

/**
 * What an update makes of the latest value: the value to write, or why not.
 * The `next` arm may carry more, which the saved result hands back (what a
 * removal took, say), so a caller needs nothing out of the update's closure.
 */
export type PreferenceUpdate<T, R = unknown> = { next: T } | { refuse: R };

type Refusal<U> = U extends { refuse: infer R } ? R : never;

export type PreferenceKeyWrite<T, R = never, A extends { next: T } = { next: T }> =
  | ({ status: 'saved'; value: T } & Omit<A, 'next'>)
  /** The saved value was never read, so a whole-key write would erase it. */
  | { status: 'notLoaded' }
  /** The update looked at the latest value and declined. */
  | { status: 'refused'; reason: R }
  | { status: 'failed'; error: unknown };

const writing = (qc: QueryClient) => qc.isMutating({ mutationKey: PREFERENCE_MUTATION_KEY }) > 0;

/**
 * The latest row to build a write on. While this tab has a write in flight
 * the cache already holds it and the server may not, so the cache wins;
 * otherwise the server is read first, since another tab may have written
 * since this one last read.
 */
export async function latestPreferences(qc: QueryClient): Promise<UserPreferences | null | undefined> {
  const key = queryKeys.user.preferences();
  const cached = () => qc.getQueryData<UserPreferences | null>(key);
  if (writing(qc)) return cached();
  try {
    return await qc.fetchQuery({ queryKey: key, queryFn: getPreferences as () => Promise<UserPreferences>, staleTime: 0 });
  } catch (error) {
    // Another setting's write began meanwhile and cancelled the read, as every
    // write does: the cache is ahead of the server again.
    if (writing(qc)) return cached();
    throw error;
  }
}

/** Tries of one write while another device keeps moving the key under it. */
const TRIES = 3;

/** The server's refusal of a write whose key moved since it was read. */
function keyMoved(error: unknown): boolean {
  const response = (error as { response?: { status?: number; data?: { detail?: { code?: unknown } } } } | null)?.response;
  return response?.status === 409 && response.data?.detail?.code === 'preference_changed';
}

/**
 * The row again after the server refused a write as stale. The cache cannot
 * answer it: it was built on the stale value, and while another write is in
 * flight a refetch would land over that write's optimistic value, so the
 * server is read without touching the cache.
 */
async function rowAfterConflict(qc: QueryClient): Promise<UserPreferences | null | undefined> {
  if (!writing(qc)) return latestPreferences(qc);
  return (await getPreferences()) as UserPreferences | null;
}

// Whole-key writes take turns, each from its read to its response: two begun
// together would both build on the row as it stood, and the second to land
// would leave out the first.
const turns = new WeakMap<QueryClient, Promise<unknown>>();

function inTurn<R>(qc: QueryClient, run: () => Promise<R>): Promise<R> {
  const turn = (turns.get(qc) ?? Promise.resolve()).then(run, run);
  turns.set(qc, turn.catch(() => {}));
  return turn;
}

/**
 * A key of `other_preference` that is written whole (a list, a map). Each
 * write is built at write time from the latest row, never from a render's
 * copy, and waits for the one before it, in this tab or another, so two
 * writes in a row, a slow caller, or another tab's write are not undone by it.
 * Nothing orders writes from another device (or a tab with no locks), so each
 * write also names the value it was built from and the server refuses it if
 * the key has moved; the update then runs again on the row read anew.
 * A write is refused until the row has been read, and announced to other
 * tabs once it lands (`usePreferencesSync` hears it there).
 */
export function usePreferenceKey<T>(key: string, read: (raw: unknown) => T): {
  value: T;
  isLoaded: boolean;
  write: <U extends PreferenceUpdate<T>>(
    update: (latest: T) => U,
  ) => Promise<PreferenceKeyWrite<T, Refusal<U>, Extract<U, { next: T }>>>;
} {
  const qc = useQueryClient();
  const { preferences, isLoaded } = usePreferences();
  const { mutateAsync } = useUpdatePreferences();

  const writeNow = async <U extends PreferenceUpdate<T>>(
    update: (latest: T) => U,
  ): Promise<PreferenceKeyWrite<T, Refusal<U>, Extract<U, { next: T }>>> => {
    const prefsKey = queryKeys.user.preferences();
    if (qc.getQueryData(prefsKey) === undefined) return { status: 'notLoaded' };
    for (let attempt = 1; ; attempt++) {
      let row: UserPreferences | null | undefined;
      try {
        row = await (attempt === 1 ? latestPreferences(qc) : rowAfterConflict(qc));
      } catch (error) {
        return { status: 'failed', error };
      }
      if (row === undefined) return { status: 'notLoaded' };
      const raw = readOtherPreference(row, key);
      const outcome: PreferenceUpdate<T> = update(read(raw));
      if ('refuse' in outcome) return { status: 'refused', reason: outcome.refuse as Refusal<U> };
      const { next, ...extra } = outcome as Extract<U, { next: T }>;
      // A user with no row reads as null, and the optimistic patch lands only on
      // a row: start them on the empty one this write creates.
      if (qc.getQueryData(prefsKey) === null) qc.setQueryData<Partial<UserPreferences>>(prefsKey, {});
      try {
        await mutateAsync({
          ...otherPreferencePatch(key, next),
          // The stored value, not `read`'s cleaned copy: the server compares it.
          expect_other_preference: { [key]: raw ?? null },
        });
      } catch (error) {
        if (keyMoved(error) && attempt < TRIES) continue;
        return { status: 'failed', error };
      }
      announcePreferencesChanged();
      return { ...extra, status: 'saved', value: next };
    }
  };

  const write = <U extends PreferenceUpdate<T>>(update: (latest: T) => U) => inTurn(qc, () => preferenceWriteLock(() => writeNow(update)));

  return { value: read(readOtherPreference(preferences, key)), isLoaded, write };
}
