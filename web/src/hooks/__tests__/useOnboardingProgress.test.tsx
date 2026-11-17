/**
 * A write to the onboarding record answered after the account changed was
 * the last account's: it stores nothing over the new account's record.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient, focusManager, onlineManager } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { runAuthResets } from '@/lib/authResets';
import type { OnboardingProgress } from '@/api/onboarding';

vi.mock('@/api/onboarding', () => ({ getOnboardingProgress: vi.fn(), patchOnboardingProgress: vi.fn() }));
import { getOnboardingProgress, patchOnboardingProgress } from '@/api/onboarding';
import { useOnboardingProgress } from '../useOnboardingProgress';

const EMPTY: OnboardingProgress = {
  models: null, runs_on: null, plan_step_done_at: null, this_mac: null, setup_on: null, setup_step: null, setup_completed_at: null,
  created_at: null, updated_at: null,
};

describe('useOnboardingProgress', () => {
  beforeEach(() => { vi.resetAllMocks(); });

  it('reads a record it could not read again only when asked, not on a focus or another reader\'s mount', async () => {
    (getOnboardingProgress as Mock).mockRejectedValue(new Error('down'));
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { result } = renderHookWithProviders(() => useOnboardingProgress(), { queryClient });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(getOnboardingProgress).toHaveBeenCalledTimes(1);
    act(() => {
      focusManager.setFocused(false);
      focusManager.setFocused(true);
      onlineManager.setOnline(false);
      onlineManager.setOnline(true);
    });
    renderHookWithProviders(() => useOnboardingProgress(), { queryClient });
    await act(async () => {});
    expect(getOnboardingProgress).toHaveBeenCalledTimes(1);
    expect(result.current.isLoading).toBe(false);
    (getOnboardingProgress as Mock).mockResolvedValue(EMPTY);
    await act(async () => { await result.current.refetch(); });
    expect(getOnboardingProgress).toHaveBeenCalledTimes(2);
    await waitFor(() => expect(result.current.progress).toEqual(EMPTY));
    focusManager.setFocused(undefined);
  });

  it('keeps a write answered after the account changed out of the new account\'s record, and rejects it', async () => {
    (getOnboardingProgress as Mock).mockResolvedValue(EMPTY);
    let answer!: (v: OnboardingProgress) => void;
    (patchOnboardingProgress as Mock).mockImplementation(() => new Promise((resolve) => { answer = resolve; }));
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { result } = renderHookWithProviders(() => useOnboardingProgress(), { queryClient });
    await waitFor(() => expect(result.current.progress).toEqual(EMPTY));
    let outcome: Promise<unknown> = Promise.resolve();
    act(() => { outcome = result.current.update({ setup_completed: true }).then(() => 'kept', () => 'dropped'); });
    await waitFor(() => expect(patchOnboardingProgress).toHaveBeenCalled());
    runAuthResets();
    await act(async () => { answer({ ...EMPTY, models: 'claude_code', runs_on: 'cloud', setup_completed_at: '2026-11-18T00:00:00Z' }); });
    expect(await outcome).toBe('dropped');
    expect(result.current.progress).toEqual(EMPTY);
  });

  it('keeps an answer that lands after a later write\'s out of the record, and reads it again', async () => {
    const FINISHED = { ...EMPTY, setup_on: 'browser' as const, setup_completed_at: '2026-11-18T00:00:00Z' };
    (getOnboardingProgress as Mock).mockResolvedValue(EMPTY);
    const answers: Array<(v: OnboardingProgress) => void> = [];
    (patchOnboardingProgress as Mock).mockImplementation(() => new Promise((resolve) => { answers.push(resolve); }));
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { result } = renderHookWithProviders(
      () => ({ claim: useOnboardingProgress(), finish: useOnboardingProgress() }),
      { queryClient },
    );
    await waitFor(() => expect(result.current.claim.progress).toEqual(EMPTY));
    act(() => { void result.current.claim.update({ setup_on: 'browser' }); });
    await waitFor(() => expect(answers).toHaveLength(1));
    act(() => { void result.current.finish.update({ setup_completed: true }); });
    await waitFor(() => expect(answers).toHaveLength(2));
    (getOnboardingProgress as Mock).mockResolvedValue(FINISHED);
    await act(async () => { answers[1](FINISHED); });
    await waitFor(() => expect(result.current.finish.progress).toEqual(FINISHED));
    const reads = (getOnboardingProgress as Mock).mock.calls.length;
    (getOnboardingProgress as Mock).mockImplementation(() => new Promise(() => {}));
    await act(async () => { answers[0]({ ...EMPTY, setup_on: 'browser' }); });
    await waitFor(() => expect((getOnboardingProgress as Mock).mock.calls.length).toBeGreaterThan(reads));
    expect(result.current.finish.progress).toEqual(FINISHED);
  });
});
