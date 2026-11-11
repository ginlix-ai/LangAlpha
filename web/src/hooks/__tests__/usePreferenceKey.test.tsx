/**
 * A whole-key write names the value it was built from, and the server refuses
 * it once another device has moved the key. The write then runs again on the
 * row read anew, a bounded number of times, so the other device's change is
 * kept rather than written over.
 */
import type { ReactNode } from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ApiError } from '@/api/client';
import { queryKeys } from '@/lib/queryKeys';
import { usePreferenceKey } from '../usePreferenceKey';

const KEY = 'grants';

const mocks = vi.hoisted(() => ({
  server: { other_preference: {} as Record<string, unknown> },
  /** Another device's writes, each landing just before one of ours arrives. */
  elsewhere: [] as string[][],
  sent: [] as Record<string, unknown>[],
}));

vi.mock('@/pages/Dashboard/utils/api', () => ({
  getPreferences: () => Promise.resolve(structuredClone(mocks.server)),
  updatePreferences: (patch: { other_preference: Record<string, unknown>; expect_other_preference: Record<string, unknown> }) => {
    mocks.sent.push(patch);
    const moved = mocks.elsewhere.shift();
    if (moved) mocks.server = { other_preference: { ...mocks.server.other_preference, [KEY]: moved } };
    const held = mocks.server.other_preference[KEY] ?? null;
    if (JSON.stringify(held) !== JSON.stringify(patch.expect_other_preference[KEY])) {
      const detail = { code: 'preference_changed', message: 'Preferences changed since they were read', keys: [KEY] };
      return Promise.reject(new ApiError('conflict', 'ERR_BAD_REQUEST', { data: { detail }, status: 409, statusText: '', headers: {} }));
    }
    mocks.server = { other_preference: { ...mocks.server.other_preference, ...patch.other_preference } };
    return Promise.resolve(structuredClone(mocks.server));
  },
  clearPreferences: vi.fn(),
}));

const readList = (raw: unknown): string[] => (Array.isArray(raw) ? (raw as string[]) : []);

function setup(stored: string[]) {
  mocks.server = { other_preference: { [KEY]: stored } };
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  qc.setQueryData(queryKeys.user.preferences(), structuredClone(mocks.server));
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
  return { qc, ...renderHook(() => usePreferenceKey(KEY, readList), { wrapper }) };
}

const cachedList = (qc: QueryClient) =>
  (qc.getQueryData(queryKeys.user.preferences()) as { other_preference: Record<string, unknown> }).other_preference[KEY];

beforeEach(() => {
  mocks.elsewhere = [];
  mocks.sent = [];
});

describe('usePreferenceKey across devices', () => {
  it('rebuilds a write the server refused as stale, keeping the other device\'s change', async () => {
    const { qc, result } = setup(['a', 'b']);
    // The other device revokes `a` while this one adds `c` from the same read.
    mocks.elsewhere = [['b']];
    let outcome: Awaited<ReturnType<typeof result.current.write>> | undefined;
    await act(async () => {
      outcome = await result.current.write((latest) => ({ next: [...latest, 'c'] }));
    });

    expect(mocks.sent.map((p) => p.expect_other_preference)).toEqual([{ [KEY]: ['a', 'b'] }, { [KEY]: ['b'] }]);
    expect(outcome).toEqual({ status: 'saved', value: ['b', 'c'] });
    expect(mocks.server.other_preference[KEY]).toEqual(['b', 'c']);
    expect(cachedList(qc)).toEqual(['b', 'c']);
  });

  it('gives up after three refusals and leaves the server\'s value in the cache', async () => {
    const { qc, result } = setup(['a']);
    mocks.elsewhere = [['x'], ['y'], ['z']];
    let outcome: Awaited<ReturnType<typeof result.current.write>> | undefined;
    await act(async () => {
      outcome = await result.current.write((latest) => ({ next: [...latest, 'c'] }));
    });

    expect(mocks.sent).toHaveLength(3);
    expect(outcome?.status).toBe('failed');
    expect(mocks.server.other_preference[KEY]).toEqual(['z']);
    // The optimistic ['y', 'c'] is rolled back, and the settle reads the row again.
    await vi.waitFor(() => expect(cachedList(qc)).toEqual(['z']));
  });
});
