/**
 * An app's grants are kept under its code and the digest of its start
 * command, so a changed command asks again; the map is written whole from
 * the latest row, keeping what other tabs (and newer builds) granted.
 */
import type { ReactNode } from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { appGrantKey, useAppScopes } from '../useAppScopes';

const mocks = vi.hoisted(() => ({
  server: { other_preference: {} as Record<string, unknown> },
  updates: [] as Record<string, unknown>[],
  failNextUpdate: false,
  toasts: [] as { title?: unknown }[],
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: (t: { title?: unknown }) => mocks.toasts.push(t) }));
vi.mock('@/pages/Dashboard/utils/api', () => ({
  getPreferences: () => Promise.resolve(structuredClone(mocks.server)),
  updatePreferences: (patch: { other_preference: Record<string, unknown> }) => {
    if (mocks.failNextUpdate) {
      mocks.failNextUpdate = false;
      return Promise.reject(new Error('down'));
    }
    mocks.updates.push(patch.other_preference);
    mocks.server = { other_preference: { ...mocks.server.other_preference, ...patch.other_preference } };
    return Promise.resolve(structuredClone(mocks.server));
  },
  clearPreferences: vi.fn(),
}));

function setup(appKey: string | null, server: Record<string, unknown>, cached: Record<string, unknown> = server) {
  mocks.server = { other_preference: { app_scopes: server } };
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  qc.setQueryData(queryKeys.user.preferences(), { other_preference: { app_scopes: cached } });
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
  return renderHook(({ k }) => useAppScopes(k), { wrapper, initialProps: { k: appKey } });
}

beforeEach(() => {
  mocks.updates = [];
  mocks.failNextUpdate = false;
  mocks.toasts = [];
});

describe('appGrantKey', () => {
  it('ties the grant to the start command when the link names one', () => {
    expect(appGrantKey({ code: 'abc', command_key: '0123456789abcdef' })).toBe('abc#0123456789abcdef');
    expect(appGrantKey({ code: 'abc', command_key: null })).toBe('abc');
    expect(appGrantKey({ code: 'abc' })).toBe('abc');
  });
});

describe('useAppScopes', () => {
  it('does not carry a grant over to a changed command', () => {
    const { result, rerender } = setup('abc#1111111111111111', { 'abc#1111111111111111': ['portfolio'] });
    expect([...result.current.granted]).toEqual(['portfolio']);
    rerender({ k: 'abc#2222222222222222' });
    expect(result.current.granted.size).toBe(0);
  });

  it('writes a grant over the latest map, keeping other apps and scopes it does not know', async () => {
    const { result } = setup(
      'abc#1',
      // Another tab granted `other` since this one read, under a scope from a newer build.
      { 'abc#1': ['quotes'], other: ['threads', 'future-scope'] },
      { 'abc#1': ['quotes'] },
    );
    await act(async () => {
      result.current.grant(['portfolio']);
    });
    await waitFor(() => expect(mocks.updates).toHaveLength(1));
    expect(mocks.updates[0].app_scopes).toEqual({
      'abc#1': ['quotes', 'portfolio'],
      other: ['threads', 'future-scope'],
    });
  });

  it('takes one grant back and leaves the rest, the other frames\' included', async () => {
    const { result } = setup('abc#1', { 'abc#1': ['portfolio', 'threads'], other: ['portfolio'] });
    await act(async () => {
      await result.current.revoke(['portfolio']);
    });
    expect(mocks.updates[0].app_scopes).toEqual({ 'abc#1': ['threads'], other: ['portfolio'] });
  });

  it('lands two changes begun together, the second built on the first', async () => {
    const { result } = setup('abc#1', { 'abc#1': ['portfolio', 'threads'] });
    await act(async () => {
      await Promise.all([result.current.revoke(['portfolio']), result.current.revoke(['threads'])]);
    });
    expect(mocks.updates.at(-1)?.app_scopes).toEqual({ 'abc#1': [] });
  });

  it('answers that a change was not saved, and says so', async () => {
    const { result } = setup('abc#1', { 'abc#1': ['portfolio'] });
    mocks.failNextUpdate = true;
    let saved: boolean | undefined;
    await act(async () => {
      saved = await result.current.revoke(['portfolio']);
    });
    expect(saved).toBe(false);
    expect(mocks.toasts).toEqual([expect.objectContaining({ title: "Couldn't save that choice. Try again." })]);
    await waitFor(() => expect([...result.current.granted]).toEqual(['portfolio']));
  });
});
