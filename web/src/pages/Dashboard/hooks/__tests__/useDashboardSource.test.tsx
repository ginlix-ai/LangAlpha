/**
 * Where a kept page loads from: the newest version under the owner's grant,
 * and for each way that has nothing to load, which one it is, since each
 * has a different way out.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import type { AppDashboard, PageDashboard } from '@/lib/dashboards';
import { useDashboardSource } from '../useDashboardSource';

const mocks = vi.hoisted(() => ({
  record: null as unknown,
  kept: [] as string[],
  toasts: [] as { title?: unknown }[],
  listed: true,
  app: null as unknown,
}));

const page: PageDashboard = {
  kind: 'page', code: 'pg1', title: 'Desk', path: 'dashboard/index.html', artifactId: 'art1', workspaceId: 'ws', threadId: null,
};
const record = (latest_seq: number, more: { kept?: boolean } = {}) => ({
  artifact_id: 'art1', workspace_id: 'ws', code: 'pg1', path: 'dashboard/index.html', latest_seq, ...more,
});

vi.mock('@/pages/ChatAgent/utils/api/artifacts', () => ({
  getArtifact: () => (mocks.record instanceof Error ? Promise.reject(mocks.record) : Promise.resolve(mocks.record)),
  createArtifactGrant: () => Promise.resolve({ base: '/api/v1/artifacts/g/tok', expires_in: 3600, isolated: false }),
  setArtifactKept: (id: string) => {
    mocks.kept.push(id);
    mocks.record = record(Math.max(1, (mocks.record as { latest_seq?: number }).latest_seq ?? 0), { kept: true });
    return Promise.resolve(mocks.record);
  },
}));
vi.mock('@/pages/Dashboard/utils/api', async (actual) => ({
  ...(await actual<typeof import('@/pages/Dashboard/utils/api')>()),
  getPreferences: () =>
    Promise.resolve({ other_preference: { custom_dashboards: { current: null, list: mocks.listed ? [page] : [] } } }),
}));
vi.mock('@/pages/SharedChat/api', () => ({
  getSharedMetadata: () => Promise.resolve(mocks.app),
}));
vi.mock('@/contexts/AuthContext', () => ({ useAuth: () => ({ isInitialized: true, userId: 'u1' }) }));
vi.mock('@/components/ui/use-toast', () => ({ toast: (t: { title?: unknown }) => mocks.toasts.push(t) }));

// A 4xx is not retried, so the read fails at once.
const refused = (status: number) =>
  Object.assign(new Error(`Request failed with status code ${status}`), { response: { status, data: {} } });
const notFound = () => refused(404);

beforeEach(() => {
  mocks.kept = [];
  mocks.toasts = [];
  mocks.listed = true;
});

describe('useDashboardSource: a kept page', () => {
  it('loads the newest version under the grant', async () => {
    mocks.record = record(3);
    const { result } = renderHookWithProviders(() => useDashboardSource(page));
    await waitFor(() => expect(result.current.url).toMatch(/\/api\/v1\/artifacts\/g\/tok\/v\/3\/dashboard\/index\.html$/));
    expect(result.current.away).toBeNull();
    expect(result.current.grantKey).toBe('pg1');
  });

  it('says a removed page is gone', async () => {
    mocks.record = notFound();
    const { result } = renderHookWithProviders(() => useDashboardSource(page));
    await waitFor(() => expect(result.current.away).toEqual({ kind: 'gone' }));
    expect(result.current.url).toBeNull();
  });

  it('keeps a copy of a page that has none when asked again', async () => {
    mocks.record = record(0);
    const { result } = renderHookWithProviders(() => useDashboardSource(page));
    await waitFor(() => expect(result.current.away).toEqual({ kind: 'noVersion' }));
    act(() => result.current.refresh());
    await waitFor(() => expect(result.current.url).toMatch(/\/v\/1\/dashboard\/index\.html$/));
    expect(mocks.kept).toEqual(['art1']);
    expect(result.current.away).toBeNull();
  });

  it('keeps a listed page again that was let go, once', async () => {
    mocks.record = record(2, { kept: false });
    const { result } = renderHookWithProviders(() => useDashboardSource(page));
    await waitFor(() => expect(mocks.kept).toEqual(['art1']));
    await waitFor(() => expect(result.current.url).toMatch(/\/v\/2\/dashboard\/index\.html$/));
    expect(mocks.kept).toHaveLength(1);
  });

  it('leaves a page let go that another device took off the list', async () => {
    mocks.record = record(2, { kept: false });
    mocks.listed = false;
    const { result } = renderHookWithProviders(() => useDashboardSource(page));
    await waitFor(() => expect(result.current.url).toMatch(/\/v\/2\/dashboard\/index\.html$/));
    await act(() => new Promise((r) => setTimeout(r, 20)));
    expect(mocks.kept).toEqual([]);
  });

  it('says a reload failed when the page on screen is all there is to show', async () => {
    mocks.record = record(3);
    const { result } = renderHookWithProviders(() => useDashboardSource(page));
    await waitFor(() => expect(result.current.url).not.toBeNull());
    mocks.record = refused(403);
    act(() => result.current.refresh());
    await waitFor(() => expect(mocks.toasts).toEqual([expect.objectContaining({ title: "Couldn't refresh Desk" })]));
    expect(result.current.url).toMatch(/\/v\/3\//);
    expect(result.current.away).toBeNull();
  });
});

describe('useDashboardSource: a running app', () => {
  const app: AppDashboard = { kind: 'app', code: 'ap1', title: 'Desk', path: null, workspaceId: 'ws', threadId: null };

  it('keys its grants by the command the answer with its address names', async () => {
    mocks.app = { kind: 'app', title: 'Desk', url: 'https://preview.example.com/a?t=1', expires_in: 3600, command_key: 'c0ffee' };
    const { result } = renderHookWithProviders(() => useDashboardSource(app));
    await waitFor(() => expect(result.current.url).toBe('https://preview.example.com/a?t=1'));
    expect(result.current.grantKey).toBe('ap1#c0ffee');

    // Another program on the same link: the address and the key move together.
    mocks.app = { kind: 'app', title: 'Desk', url: 'https://preview.example.com/a?t=2', expires_in: 3600, command_key: 'deadbeef' };
    act(() => result.current.refresh());
    await waitFor(() => expect(result.current.url).toBe('https://preview.example.com/a?t=2'));
    expect(result.current.grantKey).toBe('ap1#deadbeef');
  });
});
