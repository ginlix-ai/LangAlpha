import { useCallback, useEffect, useRef } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import { threadFieldMutationKey, type ThreadField } from '@/lib/threadFieldMutations';
import type { Thread } from '@/types/api';
import { updateThread, type ThreadUpdates } from '../utils/api';

type FieldValue<F extends ThreadField> = Exclude<Thread[F], undefined>;

export interface ThreadFieldSave<V> {
  threadId: string;
  value: V;
  /** What the composer showed before this save, put back if it fails. */
  before: V | null;
}

interface UseThreadFieldSaveOptions<F extends ThreadField> {
  /** The live thread; null until a send creates one, when there is no row to save on. */
  threadId: string | null;
  field: F;
  /** What the toast says when the latest save on a thread fails. */
  failedMessage: string;
  /** Puts the composer back after that failure, called only while it still shows the thread. */
  onFailed: (save: ThreadFieldSave<FieldValue<F>>) => void;
  /** Hears the server's answer to each save that lands. */
  onSaved?: (saved: Thread) => void;
}

/** What the server is known to hold, or null when no row was cached to restore. */
type Known<F extends ThreadField> = { value: Thread[F] } | null;

/**
 * Saves one field of a thread row the way a composer needs it. The cached
 * detail moves first, so every view of the thread agrees before the server
 * answers. Saves on one thread and field reach the server in the order they
 * were made, since two in flight could otherwise land in either order and
 * leave the row on the older one. Only the last save in flight speaks for the
 * thread: an earlier one failing behind it would restore a value the user has
 * already moved off.
 *
 * Resolves false only when this save itself failed.
 */
export function useThreadFieldSave<F extends ThreadField>({
  threadId,
  field,
  failedMessage,
  onFailed,
  onSaved,
}: UseThreadFieldSaveOptions<F>) {
  const queryClient = useQueryClient();

  // Which thread the composer shows now, for a save that fails after a move.
  const shownThreadRef = useRef(threadId);
  useEffect(() => {
    shownThreadRef.current = threadId;
  }, [threadId]);

  // What the server is known to hold per thread while saves on it are out:
  // the row before the first of them, then each one it accepts. A failure
  // restores this, not its own snapshot, which may be the optimistic write of
  // an earlier save that failed too.
  const confirmedRef = useRef(new Map<string, Known<F>>());
  // Threads whose row is to be read again once the save settles: a read it
  // cancelled, or a stale mark its optimistic write cleared.
  const interruptedRef = useRef(new Set<string>());

  const { mutateAsync } = useMutation({
    mutationKey: threadFieldMutationKey(threadId ?? '', field),
    scope: { id: `thread-field:${field}:${threadId ?? ''}` },
    mutationFn: ({ threadId: id, value }: ThreadFieldSave<FieldValue<F>>) => (
      updateThread(id, { [field]: value } as ThreadUpdates)
    ),
    onMutate: async ({ threadId: id, value }: ThreadFieldSave<FieldValue<F>>) => {
      const key = queryKeys.threads.detail(id);
      // Read before the await, so a second save cannot slip in between the
      // count and the snapshot. This save already counts, so 1 means no other
      // is out and the cached row is the server's.
      const cached = queryClient.getQueryData<Thread>(key);
      const previous: Known<F> = cached ? { value: cached[field] } : null;
      if (queryClient.isMutating({ mutationKey: threadFieldMutationKey(id, field) }) === 1) {
        confirmedRef.current.set(id, previous);
      }
      // A read in flight was asked before this save and would land over it.
      // It may carry other fields too, so it is asked again once the save
      // settles rather than dropped.
      if (queryClient.isFetching({ queryKey: key }) > 0) interruptedRef.current.add(id);
      await queryClient.cancelQueries({ queryKey: key });
      // A stale mark, left while an earlier save was out for the settle to
      // act on, does not survive the write below, so it is carried over.
      if (queryClient.getQueryState(key)?.isInvalidated) interruptedRef.current.add(id);
      writeField(id, value);
      return { previous };
    },
    onSuccess: (saved, { threadId: id, value }) => {
      confirmedRef.current.set(id, { value: answered(saved, value) });
      if (saved) onSaved?.(saved);
    },
    onError: (_error, save, context) => {
      if (queryClient.isMutating({ mutationKey: threadFieldMutationKey(save.threadId, field) }) !== 1) return;
      // Only this field rolls back; anything else a read brought in since the
      // optimistic write is newer than the snapshot. The save's own snapshot
      // stands in only when the chain began in another mount.
      const confirmed = confirmedRef.current;
      const restored = confirmed.has(save.threadId) ? confirmed.get(save.threadId) : context?.previous;
      if (restored) writeField(save.threadId, restored.value);
      if (shownThreadRef.current === save.threadId) onFailed(save);
      toast({ description: failedMessage, variant: 'destructive' });
    },
    onSettled: (saved, error, { threadId: id, value }) => {
      if (queryClient.isMutating({ mutationKey: threadFieldMutationKey(id, field) }) !== 1) return;
      confirmedRef.current.delete(id);
      const interrupted = interruptedRef.current.delete(id);
      const key = queryKeys.threads.detail(id);
      // Marked stale while the save was out, by the lifecycle feed reporting a
      // change made elsewhere or by a turn ending. Read before the write
      // below, which clears it.
      const marked = queryClient.getQueryState(key)?.isInvalidated ?? false;
      if (!error) writeField(id, answered(saved, value));
      // A failure is read back, since the save may have landed with its
      // response lost; so is a row never cached, a read this chain cancelled,
      // one started meanwhile, which the server may have answered first, and
      // one marked stale.
      if (error || interrupted || marked || !queryClient.getQueryData<Thread>(key) || queryClient.isFetching({ queryKey: key }) > 0) {
        // While another field's save is out, a read now could land over its
        // optimistic value, so the row is only marked, for that save's settle
        // to read.
        const othersOut = queryClient.isMutating({ mutationKey: threadFieldMutationKey(id) }) > 1;
        void queryClient.invalidateQueries({ queryKey: key, refetchType: othersOut ? 'none' : 'active' });
      }
    },
  });

  // The server's answer wherever it names the field, null included, since it
  // may store other than what was sent: a switch saved on the side the
  // owner's default is on is kept as no value of its own.
  function answered(saved: Thread | undefined, value: Thread[F]): Thread[F] {
    return saved && saved[field] !== undefined ? saved[field] : value;
  }

  function writeField(id: string, value: Thread[F]) {
    queryClient.setQueryData<Thread>(queryKeys.threads.detail(id), (current) => (
      current ? { ...current, [field]: value } : current
    ));
  }

  return useCallback(
    (save: ThreadFieldSave<FieldValue<F>>): Promise<boolean> => mutateAsync(save).then(() => true, () => false),
    [mutateAsync],
  );
}
