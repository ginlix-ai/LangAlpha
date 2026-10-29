/**
 * A thread may hold its own reasoning effort and fast mode over what Settings
 * gives its model, and a row with neither follows Settings. A pick on a live
 * thread moves the cached row and PATCHes exactly what was picked, null
 * included, which returns the thread to Settings. A thread not created yet
 * holds its picks for the create. A pick the new row does not hold, made while
 * the create was out or lost when the create failed and the legacy send minted
 * the thread, is PATCHed once the row is read.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders, createTestQueryClient } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { Thread } from '@/types/api';
import { useThreadTuning } from '../useThreadTuning';

const mocks = vi.hoisted(() => ({
  getThread: vi.fn(),
  updateThread: vi.fn(),
  toast: vi.fn(),
}));

vi.mock('../../utils/api', () => ({
  getThread: mocks.getThread,
  updateThread: mocks.updateThread,
}));

vi.mock('@/components/ui/use-toast', () => ({
  toast: mocks.toast,
}));

const THREAD = 'thread-1';

function row(tuning: { reasoning_effort?: string | null; fast_mode?: boolean | null } = {}, id = THREAD): Thread {
  return { thread_id: id, ...tuning } as unknown as Thread;
}

type Props = Parameters<typeof useThreadTuning>[0];

function setup(props: Partial<Props> = {}, cached: Thread | null = row()) {
  const queryClient = createTestQueryClient();
  if (cached) queryClient.setQueryData(queryKeys.threads.detail(THREAD), cached);
  mocks.getThread.mockResolvedValue(cached ?? row());
  const initial: Props = { threadId: THREAD, ...props };
  const view = renderHookWithProviders((p: Props = initial) => useThreadTuning(p), { queryClient });
  return { ...view, queryClient };
}

describe('useThreadTuning', () => {
  beforeEach(() => {
    mocks.getThread.mockReset();
    mocks.updateThread.mockReset();
    mocks.toast.mockReset();
  });

  describe('the value it reads', () => {
    it("is the row's own, and null where the row follows Settings", () => {
      const { result } = setup({}, row({ reasoning_effort: 'low', fast_mode: null }));
      expect(result.current.tuning).toEqual({ reasoningEffort: 'low', fastMode: null });
    });

    it('follows Settings for a row from a server that predates the fields', () => {
      expect(setup({}, row()).result.current.tuning).toEqual({ reasoningEffort: null, fastMode: null });
    });

    it('claims nothing on a live thread until its row is read', () => {
      mocks.getThread.mockReturnValue(new Promise(() => {}));
      const queryClient = createTestQueryClient();
      const { result } = renderHookWithProviders(() => useThreadTuning({ threadId: THREAD }), { queryClient });
      expect(result.current.tuning).toEqual({ reasoningEffort: undefined, fastMode: undefined });
    });

    it('follows Settings on a thread not created yet', () => {
      const { result } = setup({ threadId: '__default__' }, null);
      expect(result.current.tuning).toEqual({ reasoningEffort: null, fastMode: null });
      expect(result.current.toSend).toEqual({});
    });
  });

  describe('a pick on a live thread', () => {
    it('is saved on the row as picked, null included, and a send carries nothing', async () => {
      mocks.updateThread.mockResolvedValueOnce(row({ reasoning_effort: 'high' }));
      const { result, queryClient } = setup({}, row({ fast_mode: true }));

      await act(async () => { expect(await result.current.setReasoningEffort('high')).toBe(true); });
      expect(mocks.updateThread).toHaveBeenCalledWith(THREAD, { reasoning_effort: 'high' });
      expect(result.current.tuning.reasoningEffort).toBe('high');

      mocks.updateThread.mockResolvedValueOnce(row({ fast_mode: null }));
      await act(async () => { await result.current.setFastMode(null); });
      expect(mocks.updateThread).toHaveBeenLastCalledWith(THREAD, { fast_mode: null });
      expect(queryClient.getQueryData<Thread>(queryKeys.threads.detail(THREAD))?.fast_mode).toBeNull();
      expect(result.current.tuning.fastMode).toBeNull();
      expect(result.current.toSend).toEqual({});
    });

    it('puts the row back, and says so, when the server refuses it', async () => {
      mocks.updateThread.mockRejectedValue(Object.assign(new Error('400'), { response: { status: 400 } }));
      const { result } = setup({}, row({ reasoning_effort: 'low' }));

      let saved: boolean | undefined;
      await act(async () => { saved = await result.current.setReasoningEffort('high'); });
      expect(saved).toBe(false);
      expect(result.current.tuning.reasoningEffort).toBe('low');
      expect(mocks.toast).toHaveBeenCalledWith({
        description: "Couldn't change this thread's reasoning effort",
        variant: 'destructive',
      });
    });
  });

  describe('a thread not created yet', () => {
    it('holds a pick for the create, and only a pick', async () => {
      const { result } = setup({ threadId: '__default__' }, null);
      await act(async () => { await result.current.setReasoningEffort('high'); });
      await act(async () => { await result.current.setFastMode(false); });
      expect(result.current.tuning).toEqual({ reasoningEffort: 'high', fastMode: false });
      expect(result.current.toSend).toEqual({ reasoningEffortOverride: 'high', fastModeOverride: false });

      await act(async () => { await result.current.setReasoningEffort(null); });
      expect(result.current.toSend).toEqual({ fastModeOverride: false });
      expect(mocks.updateThread).not.toHaveBeenCalled();
    });

    it("takes a navigation's picks, and a new navigation replaces them", () => {
      const { result, rerender } = setup({
        threadId: '__default__',
        landing: { key: 'a', reasoningEffort: 'low', fastMode: true },
      }, null);
      expect(result.current.toSend).toEqual({ reasoningEffortOverride: 'low', fastModeOverride: true });

      rerender({ threadId: '__default__', landing: { key: 'b', reasoningEffort: null, fastMode: null } });
      expect(result.current.toSend).toEqual({});
    });
  });

  describe('once the first send has created the thread', () => {
    it('saves nothing when the create stored the pick', async () => {
      const { result, rerender } = setup({ threadId: '__default__' }, null);
      await act(async () => { await result.current.setReasoningEffort('high'); });

      mocks.getThread.mockResolvedValue(row({ reasoning_effort: 'high' }));
      rerender({ threadId: THREAD });
      await waitFor(() => expect(mocks.getThread).toHaveBeenCalled());
      await act(async () => {});
      expect(result.current.tuning.reasoningEffort).toBe('high');
      expect(mocks.updateThread).not.toHaveBeenCalled();
    });

    it('saves a pick the new row does not hold, once', async () => {
      // The create failed and the legacy send minted the thread, which keeps
      // no level of a send's.
      const { result, rerender } = setup({ threadId: '__default__' }, null);
      await act(async () => { await result.current.setFastMode(true); });

      mocks.getThread.mockResolvedValueOnce(row()).mockResolvedValue(row({ fast_mode: true }));
      mocks.updateThread.mockResolvedValue(row({ fast_mode: true }));
      rerender({ threadId: THREAD });
      await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledWith(THREAD, { fast_mode: true }));
      await waitFor(() => expect(result.current.tuning.fastMode).toBe(true));

      rerender({ threadId: THREAD });
      await act(async () => {});
      expect(mocks.updateThread).toHaveBeenCalledTimes(1);
    });

    it('leaves a pick behind when the composer moves to another thread', async () => {
      const { result, rerender, queryClient } = setup({}, row({ reasoning_effort: 'low' }));
      mocks.updateThread.mockReturnValue(new Promise(() => {}));
      act(() => { void result.current.setReasoningEffort('high'); });

      queryClient.setQueryData(queryKeys.threads.detail('thread-2'), row({}, 'thread-2'));
      rerender({ threadId: 'thread-2' });
      expect(result.current.tuning.reasoningEffort).toBeNull();
    });
  });
});
