/**
 * The thread owns its model. The hook holds what the composer shows: the
 * row's model while the catalog carries it, else the mode default, following the row
 * when it moves. A pick is saved on the row, never on the account: the cached
 * detail and the model move first, and only the latest pick's failure puts
 * them back, with a toast. PATCHes on one thread go out one at a time, in
 * pick order. A thread not created yet has no row, so its pick is held for
 * the first send to carry. The default offer follows a pick and stays
 * dismissed for that thread.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders, createTestQueryClient } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { userSessionStorage } from '@/lib/userStorage';
import type { Thread } from '@/types/api';
import { useThreadFieldSave } from '../useThreadFieldSave';
import { useThreadModel } from '../useThreadModel';

const mocks = vi.hoisted(() => ({
  getThread: vi.fn(),
  updateThread: vi.fn(),
  toast: vi.fn(),
  defaultModel: 'model-default' as string | null,
  catalogModelNames: new Set<string>(),
  /** The server's row, which a read returns unless a test says otherwise. */
  server: null as unknown,
}));

// A save the server accepts moves its row, so a read after it finds the save.
vi.mock('../../utils/api', () => ({
  getThread: mocks.getThread,
  updateThread: (...args: unknown[]) => Promise.resolve(mocks.updateThread(...args)).then((saved: unknown) => {
    mocks.server = saved;
    return saved;
  }),
}));

vi.mock('@/hooks/useSlotDefaultModels', () => ({
  useDefaultModel: () => mocks.defaultModel,
}));

// No sign-in has lapsed here; that hint has its own test.
vi.mock('@/hooks/useLapsedSignIn', () => ({ useLapsedSignIn: () => () => null }));
vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({ catalogModelNames: mocks.catalogModelNames }),
}));

vi.mock('@/components/ui/use-toast', () => ({
  toast: mocks.toast,
}));

const THREAD = 'thread-1';
const PICK_FAILED = { description: "Couldn't change this thread's model", variant: 'destructive' };

function row(llm_model: string | null): Thread {
  return { thread_id: THREAD, llm_model } as unknown as Thread;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

type Props = Parameters<typeof useThreadModel>[0];

function setup(
  props: Partial<Props> = {},
  cached: Thread | null = row('model-thread'),
  queryClient = createTestQueryClient(),
) {
  if (cached) queryClient.setQueryData(queryKeys.threads.detail(THREAD), cached);
  mocks.server = cached ?? row(null);
  mocks.getThread.mockImplementation(async () => mocks.server);
  const initial: Props = { threadId: THREAD, isLoading: false, ...props };
  const view = renderHookWithProviders(
    (p: Props = initial) => useThreadModel(p),
    { queryClient },
  );
  return { ...view, queryClient };
}

function cachedModel(queryClient: ReturnType<typeof createTestQueryClient>) {
  return queryClient.getQueryData<Thread>(queryKeys.threads.detail(THREAD))?.llm_model;
}

const failure = () => Object.assign(new Error('400'), { response: { status: 400 } });

describe('useThreadModel', () => {
  beforeEach(() => {
    mocks.getThread.mockReset();
    mocks.updateThread.mockReset();
    mocks.toast.mockReset();
    mocks.defaultModel = 'model-default';
    mocks.catalogModelNames = new Set();
    sessionStorage.clear();
  });

  describe('the model it holds', () => {
    it("opens on the thread's model from its row", () => {
      const { result } = setup();
      expect(result.current.model).toBe('model-thread');
    });

    it("falls back to the default when the row's model has left the catalog, and names it", () => {
      mocks.catalogModelNames = new Set(['model-default', 'model-beta']);
      const { result } = setup({}, row('model-retired'));
      expect(result.current.model).toBe('model-default');
      expect(result.current.retired).toEqual({ model: 'model-retired', fallback: 'model-default' });
    });

    it('names no retired model before there is a default to name beside it', () => {
      mocks.catalogModelNames = new Set(['model-beta']);
      mocks.defaultModel = null;
      const { result } = setup({}, row('model-retired'));
      expect(result.current.retired).toBeNull();
    });

    it('follows the row when its model changes', async () => {
      const { result, queryClient } = setup();
      // Let the mount's own read land first, or it would answer over the move.
      await waitFor(() => expect(queryClient.isFetching()).toBe(0));
      act(() => { queryClient.setQueryData(queryKeys.threads.detail(THREAD), row('model-moved')); });
      await waitFor(() => expect(result.current.model).toBe('model-moved'));
    });

    it('follows a default change only on a thread with no model of its own', () => {
      const own = setup();
      mocks.defaultModel = 'model-gamma';
      own.rerender({ threadId: THREAD, isLoading: false });
      expect(own.result.current.model).toBe('model-thread');

      mocks.defaultModel = 'model-default';
      const following = setup({}, row(null));
      expect(following.result.current.model).toBe('model-default');
      mocks.defaultModel = 'model-gamma';
      following.rerender({ threadId: THREAD, isLoading: false });
      expect(following.result.current.model).toBe('model-gamma');
    });

    it('names no model until the row is read, so a send cannot store the default over it', async () => {
      const pending = deferred<Thread>();
      mocks.getThread.mockReturnValueOnce(pending.promise);
      const { result } = setup({}, null);
      expect(result.current.model).toBeNull();

      await act(async () => { pending.resolve(row('model-thread')); });
      await waitFor(() => expect(result.current.model).toBe('model-thread'));
    });

    it('names no model when the row could not be read', async () => {
      mocks.getThread.mockRejectedValueOnce(failure());
      const { result, queryClient } = setup({}, null);
      await waitFor(() => expect(queryClient.getQueryState(queryKeys.threads.detail(THREAD))?.status).toBe('error'));
      expect(result.current.model).toBeNull();
    });

    it('opens a thread started from another page on the model its first send named', () => {
      const { result } = setup({ threadId: '__default__', initialModel: 'model-beta' }, null);
      expect(result.current.model).toBe('model-beta');
    });
  });

  describe('a pick', () => {
    it('is saved on the thread, moving the model and the cached row first', async () => {
      const answer = deferred<Thread>();
      mocks.updateThread.mockReturnValue(answer.promise);
      const { result, queryClient } = setup();

      let saved!: Promise<boolean>;
      act(() => { saved = result.current.pickModel('model-beta'); });
      expect(result.current.model).toBe('model-beta');
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledWith(THREAD, { llm_model: 'model-beta' }));
      expect(cachedModel(queryClient)).toBe('model-beta');

      await act(async () => { answer.resolve(row('model-beta')); expect(await saved).toBe(true); });
      expect(cachedModel(queryClient)).toBe('model-beta');
      expect(result.current.model).toBe('model-beta');
      expect(mocks.toast).not.toHaveBeenCalled();
    });

    it('puts the model and the row back, and says so, when the server refuses it', async () => {
      mocks.updateThread.mockRejectedValue(failure());
      const { result, queryClient } = setup();

      let saved: boolean | undefined;
      await act(async () => { saved = await result.current.pickModel('model-beta'); });

      expect(saved).toBe(false);
      expect(result.current.model).toBe('model-thread');
      expect(cachedModel(queryClient)).toBe('model-thread');
      expect(mocks.toast).toHaveBeenCalledWith(PICK_FAILED);
      expect(result.current.offer).toBeNull();
    });

    it('puts a held pick back when there was no row cached to move', async () => {
      // The row never arrives, so the optimistic write has nothing to move.
      mocks.updateThread.mockRejectedValue(failure());
      const { result } = setup({}, null);
      mocks.getThread.mockReturnValue(new Promise(() => {}));

      await act(async () => { await result.current.pickModel('model-beta'); });
      expect(result.current.model).toBeNull();
    });

    it('leaves another thread the composer moved to alone when a save fails late', async () => {
      const save = deferred<Thread>();
      mocks.updateThread.mockReturnValue(save.promise);
      const queryClient = createTestQueryClient();
      // The thread the composer leaves stays cached, as it does in the app;
      // the test client's gcTime of 0 would drop it once nothing observes it.
      queryClient.setQueryDefaults(queryKeys.threads.all, { gcTime: Infinity });
      const { result, rerender } = setup({}, row('model-thread'), queryClient);
      // The other thread's own row, which its mount reads again.
      const other = { thread_id: 'thread-2', llm_model: 'model-beta' } as unknown as Thread;
      mocks.getThread.mockImplementation(async (id: string) => (id === 'thread-2' ? other : row('model-thread')));
      let pick!: Promise<boolean>;
      act(() => { pick = result.current.pickModel('model-beta'); });

      queryClient.setQueryData(queryKeys.threads.detail('thread-2'), other);
      rerender({ threadId: 'thread-2', isLoading: false });
      await act(async () => { save.reject(failure()); await pick; });
      // Let that read land, so the result never depends on when it does.
      await waitFor(() => expect(mocks.getThread).toHaveBeenCalledWith('thread-2'));
      await act(async () => { await new Promise((r) => setTimeout(r, 30)); });

      expect(result.current.model).toBe('model-beta');
      expect(cachedModel(queryClient)).toBe('model-thread');
    });

    it('stays with its thread when the composer moves to another whose row is unread too', async () => {
      // Both rows unread seed the same null, so only the move can drop the pick.
      mocks.updateThread.mockResolvedValue(row('model-beta'));
      const { result, rerender } = setup({}, null);
      mocks.getThread.mockReturnValue(new Promise(() => {}));
      await act(async () => { await result.current.pickModel('model-beta'); });
      expect(result.current.model).toBe('model-beta');

      rerender({ threadId: 'thread-2', isLoading: false });
      expect(result.current.model).toBeNull();
    });

    it('before the thread exists is held for the first send', async () => {
      const { result } = setup({ threadId: '__default__' }, null);
      let saved: boolean | undefined;
      await act(async () => { saved = await result.current.pickModel('model-beta'); });

      expect(saved).toBe(true);
      expect(result.current.model).toBe('model-beta');
      expect(mocks.updateThread).not.toHaveBeenCalled();
      expect(mocks.getThread).not.toHaveBeenCalled();
      expect(result.current.offer).toEqual({ model: 'model-beta', defaultModel: 'model-default' });
    });
  });

  describe('picks that overlap', () => {
    // Fallback switch to F, then a menu pick of Y, then F's save fails: the
    // stale failure must not put the composer back on the old model while
    // the row says Y, or the next send would store the old model.
    it('leave the later pick standing when an earlier one fails behind it', async () => {
      const f = deferred<Thread>();
      const y = deferred<Thread>();
      mocks.updateThread.mockReturnValueOnce(f.promise).mockReturnValueOnce(y.promise);
      const { result, queryClient } = setup();

      let fSaved!: Promise<boolean>;
      act(() => { fSaved = result.current.pickModel('model-fallback'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-fallback'));
      act(() => { void result.current.pickModel('model-y'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-y'));

      await act(async () => { f.reject(failure()); expect(await fSaved).toBe(false); });
      expect(result.current.model).toBe('model-y');
      expect(cachedModel(queryClient)).toBe('model-y');
      expect(mocks.toast).not.toHaveBeenCalled();

      await waitFor(() => expect(mocks.updateThread).toHaveBeenLastCalledWith(THREAD, { llm_model: 'model-y' }));
      await act(async () => { y.resolve(row('model-y')); });
      expect(result.current.model).toBe('model-y');
      expect(cachedModel(queryClient)).toBe('model-y');
    });

    // X (slow, fails), Y, X again: a value compare alone would see the first
    // X's failure as the latest pick's and revert to the model before it.
    it('keep a re-pick of the same model when its first save fails', async () => {
      const x1 = deferred<Thread>();
      const y = deferred<Thread>();
      const x2 = deferred<Thread>();
      mocks.updateThread
        .mockReturnValueOnce(x1.promise)
        .mockReturnValueOnce(y.promise)
        .mockReturnValueOnce(x2.promise);
      const { result, queryClient } = setup();

      act(() => { void result.current.pickModel('model-x'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-x'));
      act(() => { void result.current.pickModel('model-y'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-y'));
      act(() => { void result.current.pickModel('model-x'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-x'));

      // One PATCH at a time, in pick order.
      expect(mocks.updateThread).toHaveBeenCalledTimes(1);
      await act(async () => { x1.reject(failure()); });
      expect(result.current.model).toBe('model-x');
      expect(cachedModel(queryClient)).toBe('model-x');

      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledTimes(2));
      expect(mocks.updateThread).toHaveBeenLastCalledWith(THREAD, { llm_model: 'model-y' });
      await act(async () => { y.resolve(row('model-y')); });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledTimes(3));
      expect(mocks.updateThread).toHaveBeenLastCalledWith(THREAD, { llm_model: 'model-x' });
      await act(async () => { x2.resolve(row('model-x')); });

      expect(result.current.model).toBe('model-x');
      expect(cachedModel(queryClient)).toBe('model-x');
      expect(mocks.toast).not.toHaveBeenCalled();
    });

    it('put back what the server holds when the latest fails behind one it accepted', async () => {
      const x = deferred<Thread>();
      const y = deferred<Thread>();
      mocks.updateThread.mockReturnValueOnce(x.promise).mockReturnValueOnce(y.promise);
      const { result, queryClient } = setup();
      await waitFor(() => expect(queryClient.isFetching()).toBe(0));

      act(() => { void result.current.pickModel('model-x'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-x'));
      act(() => { void result.current.pickModel('model-y'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-y'));

      // What the read-back after the failure finds: the pick it accepted.
      mocks.getThread.mockResolvedValue(row('model-x'));
      await act(async () => { x.resolve(row('model-x')); });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledTimes(2));
      await act(async () => { y.reject(failure()); });

      expect(result.current.model).toBe('model-x');
      expect(cachedModel(queryClient)).toBe('model-x');
      expect(mocks.toast).toHaveBeenCalledTimes(1);
    });

    it('reads the row back after a failure, since the save may have landed', async () => {
      const lost = deferred<Thread>();
      mocks.updateThread.mockReturnValueOnce(lost.promise);
      const { result, queryClient } = setup();
      await waitFor(() => expect(queryClient.isFetching()).toBe(0));

      act(() => { void result.current.pickModel('model-beta'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-beta'));
      // The PATCH committed, but its response never arrived.
      const readBack = deferred<Thread>();
      mocks.getThread.mockReturnValue(readBack.promise);
      await act(async () => { lost.reject(new Error('Network Error')); });
      expect(result.current.model).toBe('model-thread');

      await act(async () => { readBack.resolve(row('model-beta')); });
      await waitFor(() => expect(result.current.model).toBe('model-beta'));
      expect(cachedModel(queryClient)).toBe('model-beta');
    });

    it('put back the row from before them all when every one fails', async () => {
      const x = deferred<Thread>();
      const y = deferred<Thread>();
      mocks.updateThread.mockReturnValueOnce(x.promise).mockReturnValueOnce(y.promise);
      const { result, queryClient } = setup();

      act(() => { void result.current.pickModel('model-x'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-x'));
      act(() => { void result.current.pickModel('model-y'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-y'));

      await act(async () => { x.reject(failure()); });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledTimes(2));
      await act(async () => { y.reject(failure()); });

      expect(result.current.model).toBe('model-thread');
      expect(cachedModel(queryClient)).toBe('model-thread');
      expect(mocks.toast).toHaveBeenCalledTimes(1);
    });
  });

  describe('the default offer', () => {
    it('follows a pick while the composer shows it, until dismissed for the thread', async () => {
      mocks.updateThread.mockResolvedValue(row('model-beta'));
      const { result, queryClient } = setup();
      expect(result.current.offer).toBeNull();

      await act(async () => { await result.current.pickModel('model-beta'); });
      expect(result.current.offer).toEqual({ model: 'model-beta', defaultModel: 'model-default' });

      // The thread moving off the pick takes the offer with it.
      act(() => { queryClient.setQueryData(queryKeys.threads.detail(THREAD), row('model-gamma')); });
      await waitFor(() => expect(result.current.model).toBe('model-gamma'));
      expect(result.current.offer).toBeNull();

      await act(async () => { await result.current.pickModel('model-beta'); });
      act(() => { result.current.dismissOffer(); });
      expect(result.current.offer).toBeNull();
      expect(userSessionStorage.getItem(`thread-model-offer-dismissed:${THREAD}`)).not.toBeNull();

      await act(async () => { await result.current.pickModel('model-gamma'); });
      expect(result.current.offer).toBeNull();
    });

    it('offers the model a thread started from another page was sent with', () => {
      const picked = setup({ threadId: '__default__', initialModel: 'model-beta' }, null);
      expect(picked.result.current.offer).toEqual({ model: 'model-beta', defaultModel: 'model-default' });

      const unpicked = setup({ threadId: '__default__', initialModel: 'model-default' }, null);
      expect(unpicked.result.current.offer).toBeNull();
    });

    it('stays with its thread when the composer moves to another', async () => {
      // The market panel keeps one composer across threads. The next thread
      // holding the same model must not inherit the offer made on this one.
      mocks.updateThread.mockResolvedValue(row('model-beta'));
      const { result, queryClient, rerender } = setup();
      await act(async () => { await result.current.pickModel('model-beta'); });
      expect(result.current.offer).not.toBeNull();

      queryClient.setQueryData(queryKeys.threads.detail('thread-2'), { thread_id: 'thread-2', llm_model: 'model-beta' });
      rerender({ threadId: 'thread-2', isLoading: false });
      expect(result.current.model).toBe('model-beta');
      expect(result.current.offer).toBeNull();
    });

    it('is not made for a pick of the default itself', async () => {
      mocks.updateThread.mockResolvedValue(row('model-default'));
      const { result } = setup();
      await act(async () => { await result.current.pickModel('model-default'); });
      expect(result.current.offer).toBeNull();
    });
  });

  describe('when a turn ends', () => {
    it('rereads the row, since the send stored its model there', () => {
      const { rerender, queryClient } = setup({ isLoading: true });
      const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
      rerender({ threadId: THREAD, isLoading: false });
      expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.threads.detail(THREAD) });
    });

    it('leaves the reread to a pick still saving, which reads the row once it settles', async () => {
      const answer = deferred<Thread>();
      mocks.updateThread.mockReturnValue(answer.promise);
      const { result, rerender, queryClient } = setup({ isLoading: true });
      await waitFor(() => expect(queryClient.isFetching()).toBe(0));
      const reads = mocks.getThread.mock.calls.length;
      act(() => { void result.current.pickModel('model-beta'); });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalled());

      rerender({ threadId: THREAD, isLoading: false });
      // A read now would land over the pick.
      expect(mocks.getThread).toHaveBeenCalledTimes(reads);

      await act(async () => { answer.resolve(row('model-beta')); });
      await waitFor(() => expect(mocks.getThread).toHaveBeenCalledTimes(reads + 1));
      expect(result.current.model).toBe('model-beta');
    });

    it("finds a model the turn cleared while the subagents switch was saving", async () => {
      // The flip's save writes only its own field, so the model the turn
      // refused as removed stays cached unless the turn-end reread survives.
      const flip = deferred<Thread>();
      mocks.updateThread.mockReturnValue(flip.promise);
      const queryClient = createTestQueryClient();
      queryClient.setQueryData(queryKeys.threads.detail(THREAD), row('model-thread'));
      mocks.server = row('model-thread');
      mocks.getThread.mockImplementation(async () => mocks.server);
      const { result, rerender } = renderHookWithProviders(
        (p: Props = { threadId: THREAD, isLoading: true }) => ({
          thread: useThreadModel(p),
          flip: useThreadFieldSave({
            threadId: THREAD,
            field: 'subagents_allowed',
            failedMessage: 'flip failed',
            onFailed: () => {},
          }),
        }),
        { queryClient },
      );
      await waitFor(() => expect(queryClient.isFetching()).toBe(0));

      act(() => { void result.current.flip({ threadId: THREAD, value: false, before: null }); });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalled());
      rerender({ threadId: THREAD, isLoading: false });

      await act(async () => { flip.resolve({ ...row(null), subagents_allowed: false } as Thread); });
      await waitFor(() => expect(cachedModel(queryClient)).toBeNull());
      expect(result.current.thread.model).toBe('model-default');
    });

    it('holds the reread until the save of every field has settled', async () => {
      const pick = deferred<Thread>();
      const flip = deferred<Thread>();
      mocks.updateThread.mockReturnValueOnce(pick.promise).mockReturnValueOnce(flip.promise);
      const queryClient = createTestQueryClient();
      queryClient.setQueryData(queryKeys.threads.detail(THREAD), row('model-thread'));
      mocks.server = row('model-thread');
      mocks.getThread.mockImplementation(async () => mocks.server);
      const { result, rerender } = renderHookWithProviders(
        (p: Props = { threadId: THREAD, isLoading: true }) => ({
          thread: useThreadModel(p),
          flip: useThreadFieldSave({
            threadId: THREAD,
            field: 'subagents_allowed',
            failedMessage: 'flip failed',
            onFailed: () => {},
          }),
        }),
        { queryClient },
      );
      await waitFor(() => expect(queryClient.isFetching()).toBe(0));
      const reads = mocks.getThread.mock.calls.length;
      const cachedSwitch = () => queryClient.getQueryData<Thread>(queryKeys.threads.detail(THREAD))?.subagents_allowed;

      act(() => { void result.current.thread.pickModel('model-beta'); });
      act(() => { void result.current.flip({ threadId: THREAD, value: false, before: null }); });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledTimes(2));
      rerender({ threadId: THREAD, isLoading: false });

      // The server has not taken the flip yet, so a read now would undo it.
      await act(async () => { pick.resolve(row('model-beta')); });
      expect(mocks.getThread).toHaveBeenCalledTimes(reads);
      expect(cachedSwitch()).toBe(false);

      await act(async () => { flip.resolve({ ...row('model-beta'), subagents_allowed: false } as Thread); });
      await waitFor(() => expect(mocks.getThread).toHaveBeenCalledTimes(reads + 1));
      expect(cachedSwitch()).toBe(false);
      expect(result.current.thread.model).toBe('model-beta');
    });

    it('keeps the reread when a later pick starts before the one it waited on settles', async () => {
      const first = deferred<Thread>();
      const second = deferred<Thread>();
      mocks.updateThread.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
      const { result, rerender, queryClient } = setup({ isLoading: true });
      await waitFor(() => expect(queryClient.isFetching()).toBe(0));

      act(() => { void result.current.pickModel('model-x'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-x'));
      rerender({ threadId: THREAD, isLoading: false });
      // The second pick's optimistic write would clear the stale mark.
      act(() => { void result.current.pickModel('model-y'); });
      await waitFor(() => expect(cachedModel(queryClient)).toBe('model-y'));

      await act(async () => { first.resolve(row('model-x')); });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledTimes(2));
      // The turn stored subagents on the row; the pick's answer carries it,
      // but the save writes only the model.
      await act(async () => { second.resolve({ ...row('model-y'), subagents_allowed: false } as Thread); });
      await waitFor(() => expect(
        queryClient.getQueryData<Thread>(queryKeys.threads.detail(THREAD))?.subagents_allowed,
      ).toBe(false));
      expect(result.current.model).toBe('model-y');
    });
  });
});
