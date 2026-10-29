/**
 * The thread owns its subagents setting, and a row with no value of its own
 * follows the user's default. A flip moves the cached row first and PATCHes
 * the side flipped to; the server decides whether that is the thread's own
 * value or the default's, and the cache keeps its answer. Only the latest
 * flip's failure rolls back, with a toast. The PATCH is the only writer once
 * the thread exists, so its sends carry nothing. A thread not created yet
 * shows the default too, and a flip is held for the send that creates it,
 * which names the pick. A flip made while that send is creating the thread is
 * PATCHed once, when the thread's id arrives.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { useQuery } from '@tanstack/react-query';
import { renderHookWithProviders, createTestQueryClient } from '@/test/utils';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { queryKeys } from '@/lib/queryKeys';
import { readSubagentsDefault, subagentsDefaultPatch } from '@/lib/subagentsDefault';
import type { Thread, UserPreferences } from '@/types/api';
import { threadDetailQuery } from '../../utils/threadQueries';
import { useThreadSubagents } from '../useThreadSubagents';

const mocks = vi.hoisted(() => ({
  getThread: vi.fn(),
  updateThread: vi.fn(),
  getPreferences: vi.fn(),
  updatePreferences: vi.fn(),
  toast: vi.fn(),
}));

vi.mock('../../utils/api', () => ({
  getThread: mocks.getThread,
  updateThread: mocks.updateThread,
}));

vi.mock('@/pages/Dashboard/utils/api', () => ({
  getPreferences: mocks.getPreferences,
  updatePreferences: mocks.updatePreferences,
}));

vi.mock('@/components/ui/use-toast', () => ({
  toast: mocks.toast,
}));

const THREAD = 'thread-1';
const FLIP_FAILED = { description: "Couldn't change this thread's subagents setting", variant: 'destructive' };

function row(subagents_allowed?: boolean | null, id = THREAD): Thread {
  return { thread_id: id, ...(subagents_allowed === undefined ? {} : { subagents_allowed }) } as unknown as Thread;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

const DEFAULT_OFF: UserPreferences = { other_preference: { subagents_default: false } };
const DEFAULT_ON: UserPreferences = { other_preference: { subagents_default: true } };
/** Preferences left to `mocks.getPreferences`, whose read is still out by default. */
const UNREAD = 'unread';

type Props = Parameters<typeof useThreadSubagents>[0];

function setup(
  props: Partial<Props> = {},
  cached: Thread | null = row(true),
  queryClient = createTestQueryClient(),
  // Read and holding no row unless a test says otherwise: the default is on.
  prefs: UserPreferences | null | typeof UNREAD = null,
) {
  if (cached) queryClient.setQueryData(queryKeys.threads.detail(THREAD), cached);
  mocks.getThread.mockResolvedValue(cached ?? row(true));
  if (prefs !== UNREAD) queryClient.setQueryData(queryKeys.user.preferences(), prefs);
  const initial: Props = { threadId: THREAD, ...props };
  const view = renderHookWithProviders(
    (p: Props = initial) => useThreadSubagents(p),
    { queryClient },
  );
  return { ...view, queryClient };
}

/** The switch beside the default it reads and the write that accepting its
 *  offer makes. The default is read in the same render, so a test can wait
 *  until the switch has seen a change. */
function setupWithDefault(cached: Thread, prefs: UserPreferences) {
  const queryClient = createTestQueryClient();
  queryClient.setQueryData(queryKeys.threads.detail(THREAD), cached);
  mocks.getThread.mockResolvedValue(cached);
  queryClient.setQueryData(queryKeys.user.preferences(), prefs);
  const view = renderHookWithProviders(
    () => ({
      thread: useThreadSubagents({ threadId: THREAD }),
      subagentsDefault: readSubagentsDefault(usePreferences().preferences),
      accept: useUpdatePreferences(),
    }),
    { queryClient },
  );
  return { ...view, queryClient };
}

function cachedAllowed(queryClient: ReturnType<typeof createTestQueryClient>) {
  return queryClient.getQueryData<Thread>(queryKeys.threads.detail(THREAD))?.subagents_allowed;
}

const failure = () => Object.assign(new Error('400'), { response: { status: 400 } });

describe('useThreadSubagents', () => {
  beforeEach(() => {
    mocks.getThread.mockReset();
    mocks.updateThread.mockReset();
    mocks.getPreferences.mockReset();
    mocks.getPreferences.mockReturnValue(new Promise(() => {}));
    mocks.updatePreferences.mockReset();
    mocks.toast.mockReset();
  });

  describe('the value it reads', () => {
    it("is the row's", () => {
      const { result } = setup({}, row(false));
      expect(result.current.allowed).toBe(false);
    });

    it.each([
      ['null', row(null)],
      ['absent, from a server that predates the field', row()],
    ])("is the user's default for a row with no value of its own: %s", (_label, cached) => {
      expect(setup({}, cached).result.current.allowed).toBe(true);
      expect(setup({}, cached, undefined, DEFAULT_OFF).result.current.allowed).toBe(false);
    });

    it('is unknown on a live thread until its row is read, since the server runs the row', async () => {
      const pending = deferred<Thread>();
      mocks.getThread.mockReturnValueOnce(pending.promise);
      const { result } = setup({}, null, undefined, DEFAULT_ON);
      expect(result.current.allowed).toBeNull();

      await act(async () => { pending.resolve(row(false)); });
      await waitFor(() => expect(result.current.allowed).toBe(false));
    });

    it('stays unknown when the read fails, but shows a flip made meanwhile', async () => {
      mocks.getThread.mockRejectedValueOnce(failure());
      mocks.updateThread.mockReturnValue(new Promise(() => {}));
      const { result } = setup({}, null);
      await waitFor(() => expect(mocks.getThread).toHaveBeenCalled());
      expect(result.current.allowed).toBeNull();

      act(() => { void result.current.setAllowed(false); });
      expect(result.current.allowed).toBe(false);
    });

    it('is on for a thread not created yet', () => {
      const { result } = setup({ threadId: '__default__' }, null);
      expect(result.current.allowed).toBe(true);
      expect(mocks.getThread).not.toHaveBeenCalled();
    });
  });

  describe('a send', () => {
    it('carries nothing on a live thread, read or not, since the PATCH is its only writer', async () => {
      const pending = deferred<Thread>();
      mocks.getThread.mockReturnValueOnce(pending.promise);
      const { result } = setup({}, null);
      expect(result.current.toSend).toBeUndefined();

      await act(async () => { pending.resolve(row(false)); });
      await waitFor(() => expect(result.current.allowed).toBe(false));
      expect(result.current.toSend).toBeUndefined();

      mocks.updateThread.mockReturnValue(new Promise(() => {}));
      act(() => { void result.current.setAllowed(true); });
      expect(result.current.allowed).toBe(true);
      expect(result.current.toSend).toBeUndefined();
    });

    it('carries nothing on a new thread left alone, so the thread follows the default', () => {
      const { result } = setup({ threadId: '__default__' }, null, undefined, DEFAULT_OFF);
      expect(result.current.allowed).toBe(false);
      expect(result.current.toSend).toBeUndefined();
    });

    it('names what a new thread shows while the default is unread, which the server would not apply', async () => {
      // A stored default of off is not known yet, so the switch shows on; a
      // send naming nothing would start the thread off.
      const prefs = deferred<UserPreferences>();
      mocks.getPreferences.mockReturnValue(prefs.promise);
      const { result } = setup({ threadId: '__default__' }, null, undefined, UNREAD);
      expect(result.current.allowed).toBe(true);
      expect(result.current.toSend).toBe(true);

      await act(async () => { prefs.resolve(DEFAULT_OFF); });
      await waitFor(() => expect(result.current.allowed).toBe(false));
      expect(result.current.toSend).toBeUndefined();
    });

    it("names a new thread's pick on either side, the default's included, which the server sorts out", async () => {
      const { result } = setup({ threadId: '__default__' }, null);
      await act(async () => { await result.current.setAllowed(false); });
      expect(result.current.toSend).toBe(false);

      await act(async () => { await result.current.setAllowed(true); });
      expect(result.current.toSend).toBe(true);
    });
  });

  describe("the user's default", () => {
    it('is what a new thread shows once preferences are read', async () => {
      const prefs = deferred<UserPreferences>();
      mocks.getPreferences.mockReturnValue(prefs.promise);
      const { result } = setup({ threadId: '__default__' }, null, undefined, UNREAD);
      expect(result.current.allowed).toBe(true);

      await act(async () => { prefs.resolve(DEFAULT_OFF); });
      await waitFor(() => expect(result.current.allowed).toBe(false));
    });

    it('gives way to a flip, which a send names even before preferences are read', async () => {
      const { result } = setup({ threadId: '__default__' }, null, undefined, DEFAULT_OFF);
      expect(result.current.allowed).toBe(false);
      await act(async () => { await result.current.setAllowed(true); });
      expect(result.current.allowed).toBe(true);
      expect(result.current.toSend).toBe(true);

      const unread = setup({ threadId: '__default__' }, null, undefined, UNREAD);
      await act(async () => { await unread.result.current.setAllowed(false); });
      expect(unread.result.current.toSend).toBe(false);
    });

    it("leaves a thread's own value standing", () => {
      const { result } = setup({}, row(true), undefined, DEFAULT_OFF);
      expect(result.current.allowed).toBe(true);
    });

    it('moves a thread with no value of its own when it changes', async () => {
      const { result, queryClient } = setup({}, row(null));
      expect(result.current.allowed).toBe(true);

      act(() => { queryClient.setQueryData(queryKeys.user.preferences(), DEFAULT_OFF); });
      await waitFor(() => expect(result.current.allowed).toBe(false));
    });
  });

  describe('a flip', () => {
    it('is saved on the thread, moving the cached row first', async () => {
      const answer = deferred<Thread>();
      mocks.updateThread.mockReturnValue(answer.promise);
      const { result, queryClient } = setup();

      let saved!: Promise<boolean>;
      act(() => { saved = result.current.setAllowed(false); });
      expect(result.current.allowed).toBe(false);
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledWith(THREAD, { subagents_allowed: false }));
      expect(cachedAllowed(queryClient)).toBe(false);

      // The mount's read was cut short by the save, so the row is read again.
      mocks.getThread.mockResolvedValue(row(false));
      await act(async () => { answer.resolve(row(false)); expect(await saved).toBe(true); });
      expect(result.current.allowed).toBe(false);
      expect(mocks.toast).not.toHaveBeenCalled();
    });

    it.each([
      ['read', null],
      ['unread', UNREAD],
    ] as const)('saves exactly the side flipped to, with the default %s', async (_label, prefs) => {
      mocks.updateThread.mockResolvedValue(row(true));
      const { result } = setup({}, row(null), undefined, prefs);

      await act(async () => { await result.current.setAllowed(true); });
      expect(mocks.updateThread).toHaveBeenCalledWith(THREAD, { subagents_allowed: true });
    });

    it("onto the default's side keeps the server's answer, so the thread follows the default again", async () => {
      // The server stores the default's side as no value of its own, and its
      // answer says so; nothing reads the row again to find out.
      mocks.updateThread.mockResolvedValue(row(null));
      const { result, queryClient } = setup({}, row(false));
      await waitFor(() => expect(queryClient.isFetching()).toBe(0));
      const reads = mocks.getThread.mock.calls.length;

      await act(async () => { await result.current.setAllowed(true); });
      expect(mocks.updateThread).toHaveBeenCalledWith(THREAD, { subagents_allowed: true });
      expect(cachedAllowed(queryClient)).toBeNull();
      expect(mocks.getThread).toHaveBeenCalledTimes(reads);
      expect(result.current.allowed).toBe(true);

      act(() => { queryClient.setQueryData(queryKeys.user.preferences(), DEFAULT_OFF); });
      await waitFor(() => expect(result.current.allowed).toBe(false));
    });

    it('reads the row back when the lifecycle feed marked it stale while the save was out', async () => {
      const answer = deferred<Thread>();
      mocks.updateThread.mockReturnValue(answer.promise);
      const { result, queryClient } = setup();
      await waitFor(() => expect(queryClient.isFetching()).toBe(0));
      const reads = mocks.getThread.mock.calls.length;

      act(() => { void result.current.setAllowed(false); });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalled());
      // Another tab's flip, reported while this one is saving.
      act(() => { void queryClient.invalidateQueries({ queryKey: queryKeys.threads.detail(THREAD), refetchType: 'none' }); });
      expect(mocks.getThread).toHaveBeenCalledTimes(reads);

      mocks.getThread.mockResolvedValue(row(true));
      await act(async () => { answer.resolve(row(false)); });
      await waitFor(() => expect(mocks.getThread).toHaveBeenCalledTimes(reads + 1));
      await waitFor(() => expect(result.current.allowed).toBe(true));
    });

    it('puts the value and the row back, and says so, when the server refuses it', async () => {
      mocks.updateThread.mockRejectedValue(failure());
      const { result, queryClient } = setup();

      let saved: boolean | undefined;
      await act(async () => { saved = await result.current.setAllowed(false); });

      expect(saved).toBe(false);
      expect(result.current.allowed).toBe(true);
      expect(cachedAllowed(queryClient)).toBe(true);
      expect(mocks.toast).toHaveBeenCalledWith(FLIP_FAILED);
    });

    it('leaves the later flip standing when an earlier one fails behind it, one PATCH at a time', async () => {
      const off = deferred<Thread>();
      const on = deferred<Thread>();
      mocks.updateThread.mockReturnValueOnce(off.promise).mockReturnValueOnce(on.promise);
      const { result, queryClient } = setup();

      act(() => { void result.current.setAllowed(false); });
      await waitFor(() => expect(cachedAllowed(queryClient)).toBe(false));
      act(() => { void result.current.setAllowed(true); });
      await waitFor(() => expect(cachedAllowed(queryClient)).toBe(true));
      expect(mocks.updateThread).toHaveBeenCalledTimes(1);

      await act(async () => { off.reject(failure()); });
      expect(result.current.allowed).toBe(true);
      expect(mocks.toast).not.toHaveBeenCalled();

      await waitFor(() => expect(mocks.updateThread).toHaveBeenLastCalledWith(THREAD, { subagents_allowed: true }));
      mocks.getThread.mockResolvedValue(row(null));
      await act(async () => { on.resolve(row(null)); });
      expect(result.current.allowed).toBe(true);
      expect(cachedAllowed(queryClient)).toBeNull();
    });

    it('puts the hold back when it fails with no row to restore', async () => {
      mocks.getThread.mockReset().mockReturnValue(new Promise(() => {}));
      mocks.updateThread.mockResolvedValueOnce(row(false)).mockRejectedValueOnce(failure());
      const { result } = setup({}, null);
      mocks.getThread.mockReturnValue(new Promise(() => {}));

      await act(async () => { await result.current.setAllowed(false); });
      await act(async () => { await result.current.setAllowed(true); });
      expect(mocks.updateThread).toHaveBeenLastCalledWith(THREAD, { subagents_allowed: true });
      expect(result.current.allowed).toBe(false);
    });

    it('leaves the hold alone when it fails after the toggle has moved on', async () => {
      // No row is cached, so the flips live in the hold. A read that lands
      // while the second is out agrees with it and releases the hold, and its
      // failure then has nothing of its own to put back.
      const queryClient = createTestQueryClient();
      queryClient.setQueryData(queryKeys.user.preferences(), null);
      mocks.getThread.mockReturnValue(new Promise(() => {}));
      const answer = deferred<Thread>();
      mocks.updateThread.mockResolvedValueOnce(row(false)).mockReturnValueOnce(answer.promise);
      // The row read beside the switch, to know the switch has seen it too.
      const { result } = renderHookWithProviders(
        () => ({
          switch: useThreadSubagents({ threadId: THREAD }),
          row: useQuery(threadDetailQuery(THREAD)).data,
        }),
        { queryClient },
      );

      await act(async () => { await result.current.switch.setAllowed(false); });
      act(() => { void result.current.switch.setAllowed(true); });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledTimes(2));

      mocks.getThread.mockResolvedValueOnce(row(true));
      act(() => { void queryClient.invalidateQueries({ queryKey: queryKeys.threads.detail(THREAD) }); });
      await waitFor(() => expect(result.current.row?.subagents_allowed).toBe(true));

      await act(async () => { answer.reject(failure()); });
      expect(mocks.toast).toHaveBeenCalledWith(FLIP_FAILED);
      expect(result.current.switch.allowed).toBe(true);
    });

    it('before the thread exists is held for the first send', async () => {
      const { result } = setup({ threadId: '__default__' }, null);
      let saved: boolean | undefined;
      await act(async () => { saved = await result.current.setAllowed(false); });

      expect(saved).toBe(true);
      expect(result.current.allowed).toBe(false);
      expect(result.current.toSend).toBe(false);
      expect(mocks.updateThread).not.toHaveBeenCalled();

      await act(async () => { await result.current.setAllowed(true); });
      expect(result.current.allowed).toBe(true);
      expect(result.current.toSend).toBe(true);
    });
  });

  describe('the offer to make a flip the default', () => {
    beforeEach(() => {
      sessionStorage.clear();
      // The server's answer, as it stores a value with the default on.
      mocks.updateThread.mockImplementation(async (id: string, { subagents_allowed }: { subagents_allowed: boolean }) => (
        row(subagents_allowed ? null : false, id)
      ));
    });

    it('follows a saved flip to the side the default is not on, and only while the thread stays there', async () => {
      const { result } = setup();
      expect(result.current.offer).toBeNull();

      await act(async () => { await result.current.setAllowed(false); });
      expect(result.current.offer).toBe(false);

      await act(async () => { await result.current.setAllowed(true); });
      expect(result.current.offer).toBeNull();
    });

    it('waits for the flip to be saved, so a default accepted from it cannot land first', async () => {
      // Landing first, the default would decide whether the thread keeps the
      // value as its own or follows the default.
      const patch = deferred<Thread>();
      mocks.updateThread.mockReturnValue(patch.promise);
      const { result } = setup();

      let saving!: Promise<boolean>;
      act(() => { saving = result.current.setAllowed(false); });
      expect(result.current.allowed).toBe(false);
      expect(result.current.offer).toBeNull();

      await act(async () => { patch.resolve(row(false)); await saving; });
      expect(result.current.offer).toBe(false);
    });

    it('waits for the default to be read', async () => {
      const unread = setup({}, row(true), undefined, UNREAD);
      await act(async () => { await unread.result.current.setAllowed(false); });
      expect(unread.result.current.offer).toBeNull();
    });

    it('comes back with a later flip once run out, but closed stays closed for the thread', async () => {
      const first = setup();
      await act(async () => { await first.result.current.setAllowed(false); });
      act(() => first.result.current.expireOffer());
      expect(first.result.current.offer).toBeNull();

      await act(async () => { await first.result.current.setAllowed(true); });
      await act(async () => { await first.result.current.setAllowed(false); });
      expect(first.result.current.offer).toBe(false);

      act(() => first.result.current.closeOffer());
      expect(first.result.current.offer).toBeNull();
      first.unmount();

      const reopened = setup();
      await act(async () => { await reopened.result.current.setAllowed(false); });
      expect(reopened.result.current.offer).toBeNull();
    });

    it('is dropped when the composer moves to another thread', async () => {
      const { result, rerender, queryClient } = setup();
      await act(async () => { await result.current.setAllowed(false); });
      expect(result.current.offer).toBe(false);
      queryClient.setQueryData(queryKeys.threads.detail('thread-2'), row(false, 'thread-2'));
      rerender({ threadId: 'thread-2' });
      expect(result.current.offer).toBeNull();
    });

    it('is not raised by a save that lands after the composer moved to another thread', async () => {
      const patch = deferred<Thread>();
      mocks.updateThread.mockReturnValue(patch.promise);
      const { result, rerender, queryClient } = setup();
      let saving!: Promise<boolean>;
      act(() => { saving = result.current.setAllowed(false); });
      queryClient.setQueryData(queryKeys.threads.detail('thread-2'), row(false, 'thread-2'));
      rerender({ threadId: 'thread-2' });

      await act(async () => { patch.resolve(row(false)); await saving; });
      expect(result.current.offer).toBeNull();
    });

    it('is dropped when the flip fails to save', async () => {
      mocks.updateThread.mockRejectedValue(failure());
      const { result } = setup();
      await act(async () => { await result.current.setAllowed(false); });
      expect(result.current.offer).toBeNull();
    });

    it("follows a flip that a navigation's first send carried", () => {
      const { result } = setup({ landing: { key: 'nav-1', allowed: false } }, row(false));
      expect(result.current.offer).toBe(false);
    });

    it('is answered once the default reaches its side, and stays answered when the default moves away again', async () => {
      // Another tab made the flip's side the default, then moved it back,
      // while this view stayed open on the thread.
      const { result, queryClient } = setupWithDefault(row(true), DEFAULT_ON);
      await act(async () => { await result.current.thread.setAllowed(false); });
      expect(result.current.thread.offer).toBe(false);

      act(() => { queryClient.setQueryData(queryKeys.user.preferences(), DEFAULT_OFF); });
      await waitFor(() => expect(result.current.subagentsDefault).toBe(false));
      expect(result.current.thread.offer).toBeNull();

      act(() => { queryClient.setQueryData(queryKeys.user.preferences(), DEFAULT_ON); });
      await waitFor(() => expect(result.current.subagentsDefault).toBe(true));
      expect(result.current.thread.allowed).toBe(false);
      expect(result.current.thread.offer).toBeNull();
    });

    it('is answered by an accept that lands, and stays answered when the default moves away again', async () => {
      const write = deferred<UserPreferences>();
      mocks.updatePreferences.mockReturnValue(write.promise);
      const { result, queryClient } = setupWithDefault(row(true), DEFAULT_ON);
      await act(async () => { await result.current.thread.setAllowed(false); });
      expect(result.current.thread.offer).toBe(false);

      act(() => { result.current.accept.mutate(subagentsDefaultPatch(false)); });
      await waitFor(() => expect(result.current.subagentsDefault).toBe(false));
      expect(result.current.thread.offer).toBeNull();
      await act(async () => { write.resolve(DEFAULT_OFF); });
      await waitFor(() => expect(result.current.accept.isSuccess).toBe(true));

      act(() => { queryClient.setQueryData(queryKeys.user.preferences(), DEFAULT_ON); });
      await waitFor(() => expect(result.current.subagentsDefault).toBe(true));
      expect(result.current.thread.allowed).toBe(false);
      expect(result.current.thread.offer).toBeNull();
    });

    it('comes back when the accept is refused, since the default never moved', async () => {
      const write = deferred<UserPreferences>();
      mocks.updatePreferences.mockReturnValue(write.promise);
      const { result } = setupWithDefault(row(true), DEFAULT_ON);
      await act(async () => { await result.current.thread.setAllowed(false); });
      // A refusal rereads threads.
      mocks.getThread.mockResolvedValue(row(false));

      act(() => { result.current.accept.mutate(subagentsDefaultPatch(false)); });
      await waitFor(() => expect(result.current.subagentsDefault).toBe(false));
      expect(result.current.thread.offer).toBeNull();

      await act(async () => { write.reject(failure()); });
      await waitFor(() => expect(result.current.subagentsDefault).toBe(true));
      expect(result.current.thread.offer).toBe(false);
    });
  });

  describe('a flip made while the first send creates the thread', () => {
    it("is saved on the row once, when the new row is read", async () => {
      // The send went out with nothing held; the flip came after it.
      const { result, rerender } = setup({ threadId: '__default__' }, null);
      await act(async () => { await result.current.setAllowed(false); });
      expect(mocks.updateThread).not.toHaveBeenCalled();

      mocks.getThread.mockResolvedValueOnce(row(null)).mockResolvedValue(row(false));
      mocks.updateThread.mockResolvedValue(row(false));
      rerender({ threadId: THREAD });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledWith(THREAD, { subagents_allowed: false }));
      await waitFor(() => expect(result.current.allowed).toBe(false));
      expect(result.current.toSend).toBeUndefined();

      rerender({ threadId: THREAD });
      await act(async () => {});
      expect(mocks.updateThread).toHaveBeenCalledTimes(1);
    });

    it('names the side it was flipped to, the default included', async () => {
      // The send named off; the flip back on came while it was creating.
      const { result, rerender } = setup({ threadId: '__default__' }, null);
      await act(async () => { await result.current.setAllowed(false); });
      await act(async () => { await result.current.setAllowed(true); });

      mocks.getThread.mockResolvedValueOnce(row(false)).mockResolvedValue(row(null));
      mocks.updateThread.mockResolvedValue(row(null));
      rerender({ threadId: THREAD });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledWith(THREAD, { subagents_allowed: true }));
      await waitFor(() => expect(result.current.allowed).toBe(true));
      expect(mocks.updateThread).toHaveBeenCalledTimes(1);
    });

    it('saves nothing when nothing was held', async () => {
      const untouched = setup({ threadId: '__default__' }, null);
      untouched.rerender({ threadId: THREAD });
      await waitFor(() => expect(mocks.getThread).toHaveBeenCalled());
      await act(async () => {});
      expect(mocks.updateThread).not.toHaveBeenCalled();
    });

    it('stays with its thread when the composer moves to another', async () => {
      mocks.updateThread.mockResolvedValue(row(false));
      const { result, rerender, queryClient } = setup({ threadId: '__default__' }, null);
      await act(async () => { await result.current.setAllowed(false); });
      queryClient.setQueryData(queryKeys.threads.detail(THREAD), row(true));
      rerender({ threadId: THREAD });
      expect(result.current.allowed).toBe(false);
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledWith(THREAD, { subagents_allowed: false }));

      queryClient.setQueryData(queryKeys.threads.detail('thread-2'), row(true, 'thread-2'));
      rerender({ threadId: 'thread-2' });
      expect(result.current.allowed).toBe(true);
      await act(async () => {});
      expect(mocks.updateThread).toHaveBeenCalledTimes(1);
    });
  });
});
