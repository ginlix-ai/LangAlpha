/**
 * The onboarded signals the door reads: the hosted onboarding's account setup
 * stamp from the record, read fresh, and an account that already has a
 * computer or a thread on either mode, so an existing account is never sent
 * back to the hosted onboarding or the questions.
 */
import { describe, expect, it, vi, beforeEach } from 'vitest';
import type { ReactNode } from 'react';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const h = vi.hoisted(() => ({
  platform: true,
  progress: null as Record<string, unknown> | null,
  computers: [] as unknown[],
  threads: [] as unknown[],
  answers: null as Record<string, unknown> | null,
  factsFailed: false,
  progressFailed: false,
  prefsLoaded: true,
  needsProvider: false,
  userFailed: false,
  prefsLoading: false,
}));
vi.mock('@/config/hostMode', () => ({
  get isPlatformMode() { return h.platform; },
  HOST_MODE: 'platform',
  APP_ENTRY_PATH: '/app',
}));
vi.mock('../useUser', () => ({ useUser: () => ({ user: h.userFailed ? null : { user_id: 'u1' }, isLoading: false, isError: h.userFailed }) }));
vi.mock('../usePreferences', () => ({ usePreferences: () => ({ preferences: null, isLoading: h.prefsLoading, isLoaded: h.prefsLoaded }) }));
vi.mock('../useSetupGate', () => ({
  useSetupGate: ({ enabled = true }: { enabled?: boolean } = {}) => ({ isLoading: false, needsSetup: enabled && h.needsProvider }),
}));
vi.mock('@/pages/Setup/answers/useSetupFacts', () => ({
  useSetupFacts: () => ({ progress: h.progress, progressFailed: h.progressFailed, answers: h.answers, isLoading: false, next: null, factsFailed: h.factsFailed }),
}));
vi.mock('@/pages/ChatAgent/hooks/useComputers', () => ({
  useComputers: () => ({ data: { computers: h.computers }, isLoading: false, isError: false }),
}));
vi.mock('@/pages/ChatAgent/utils/threadQueries', () => ({
  recentThreadsQuery: () => ({ queryKey: ['threads', 'recent'], queryFn: async () => ({ threads: h.threads }) }),
}));

import { useOnboardingRoute } from '../useOnboardingRoute';

const RECORD = {
  models: null, runs_on: null, this_mac: 'unavailable', setup_on: null, setup_step: null,
  setup_completed_at: null, plan_step_done_at: null, account_setup_completed_at: null,
};

function route() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  return renderHook(() => useOnboardingRoute(), { wrapper });
}

async function settled() {
  const { result } = route();
  await waitFor(() => expect(result.current.kind).not.toBe('loading'));
  return result.current;
}

describe('useOnboardingRoute', () => {
  beforeEach(() => {
    h.platform = true;
    h.progress = { ...RECORD };
    h.computers = [];
    h.threads = [];
    h.answers = null;
    h.factsFailed = false;
    h.progressFailed = false;
    h.prefsLoaded = true;
    h.needsProvider = false;
    h.userFailed = false;
    h.prefsLoading = false;
  });

  it('stays decided while a read that failed is taken again, which would unmount the app', async () => {
    h.platform = false;
    h.prefsLoaded = false;
    const { result, rerender } = route();
    await waitFor(() => expect(result.current).toEqual({ kind: 'done' }));
    h.prefsLoading = true;
    rerender();
    expect(result.current).toEqual({ kind: 'done' });
  });

  it('counts the computers of an account whose Mac paired in setup is gone', async () => {
    h.platform = false;
    h.progress = { ...RECORD, this_mac: 'paired' };
    h.computers = [{ computer_id: 'lab', ssh: { helper: false } }];
    const { result } = route();
    await waitFor(() => expect(result.current).toEqual({ kind: 'done' }));
  });

  it('sends a hosted account with answers nowhere while a fact setup resumes on could not be read', async () => {
    h.progress = { ...RECORD, models: 'subscription_or_key', runs_on: 'cloud' };
    h.answers = { models: 'subscription_or_key', runsOn: 'cloud' };
    h.factsFailed = true;
    const { result } = route();
    await waitFor(() => expect(result.current).toEqual({ kind: 'done' }));
  });

  it('asks the questions of a self-hosted account whose only computer is the Mac it paired in setup', async () => {
    h.platform = false;
    h.progress = { ...RECORD, this_mac: 'paired' };
    h.computers = [{ computer_id: 'mac', ssh: { helper: true } }];
    const { result } = route();
    await waitFor(() => expect(result.current).toEqual({ kind: 'questions', unpaired: false }));
  });

  it('sends no one to the older provider setup on a record or preferences read that failed', async () => {
    h.platform = false;
    h.needsProvider = true;
    h.progress = null;
    h.progressFailed = true;
    expect((await settled()).kind).toBe('done');
    h.progress = { ...RECORD, account_setup_completed_at: '2026-11-15T00:00:00Z' };
    h.progressFailed = false;
    h.prefsLoaded = false;
    expect((await settled()).kind).toBe('done');
    h.prefsLoaded = true;
    expect((await settled()).kind).toBe('provider');
  });

  it('sends no one anywhere on a profile read that failed', async () => {
    h.userFailed = true;
    expect((await settled()).kind).toBe('done');
    h.platform = false;
    h.needsProvider = true;
    expect((await settled()).kind).toBe('done');
  });

  it('sends no one anywhere while a fact setup resumes on could not be read', async () => {
    h.platform = false;
    h.progress = { ...RECORD, models: 'subscription_or_key', runs_on: 'cloud' };
    h.answers = { models: 'subscription_or_key', runsOn: 'cloud' };
    h.factsFailed = true;
    const { result } = route();
    await waitFor(() => expect(result.current).toEqual({ kind: 'done' }));
  });

  it('sends a hosted account the platform has not set up to its onboarding', async () => {
    const { result } = route();
    await waitFor(() => expect(result.current).toEqual({ kind: 'platform' }));
  });

  it('reads the platform\'s account setup stamp from the record', async () => {
    h.progress = { ...RECORD, account_setup_completed_at: '2026-11-15T00:00:00Z' };
    const { result } = route();
    await waitFor(() => expect(result.current).toEqual({ kind: 'done' }));
  });

  it('asks a self-hosted account never set up the two questions', async () => {
    h.platform = false;
    const { result } = route();
    await waitFor(() => expect(result.current).toEqual({ kind: 'questions', unpaired: false }));
  });

  it('leaves a self-hosted account with a thread alone', async () => {
    h.platform = false;
    h.threads = [{ thread_id: 't1' }];
    const { result } = route();
    await waitFor(() => expect(result.current).toEqual({ kind: 'done' }));
  });
});
