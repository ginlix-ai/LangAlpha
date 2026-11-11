/**
 * The dashboards list is one preference key written whole, so every write is
 * built from the latest row (never a render's copy), refused before the row
 * is read, capped without dropping anything, and a removed page's copy is
 * let go only once its Undo is gone.
 */
import type { ReactElement, ReactNode } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import { queryKeys } from '@/lib/queryKeys';
import { UNDO_MS } from '../keptCopy';
import { MAX_DASHBOARDS, readSavedDashboards, type SavedDashboard } from '../saved';
import { useSavedDashboards } from '../useSavedDashboards';

const mocks = vi.hoisted(() => ({
  // The row on the server, which another tab may move behind this one's back.
  server: { other_preference: {} as Record<string, unknown> },
  readPending: false,
  failNextUpdate: false,
  updates: [] as Record<string, unknown>[],
  kept: [] as [string, boolean][],
  toasts: [] as { description?: unknown; title?: unknown; action?: ReactElement }[],
}));

vi.mock('@/pages/Dashboard/utils/api', () => ({
  getPreferences: () =>
    mocks.readPending ? new Promise(() => {}) : Promise.resolve(structuredClone(mocks.server)),
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

vi.mock('@/pages/ChatAgent/utils/api/artifacts', () => ({
  setArtifactKept: (id: string, kept: boolean) => {
    mocks.kept.push([id, kept]);
    return Promise.resolve({ artifact_id: id, workspace_id: 'ws', path: 'p.html', kept, latest_seq: 1 });
  },
  listArtifactVersions: () => Promise.resolve([]),
  getArtifact: vi.fn(),
}));

vi.mock('@/components/ui/use-toast', () => ({
  toast: (t: (typeof mocks.toasts)[number]) => {
    mocks.toasts.push(t);
    return { id: String(mocks.toasts.length), dismiss: vi.fn(), update: vi.fn() };
  },
}));

const entry = (code: string) => ({ code, title: code.toUpperCase(), path: null, workspaceId: 'ws', threadId: null });
const app = (code: string, extra: { title?: string } = {}): SavedDashboard => ({ ...entry(code), kind: 'app', ...extra });
const page = (code: string): SavedDashboard => ({ ...entry(code), kind: 'page', artifactId: `art-${code}` });

const lastSaved = () => (mocks.updates.at(-1)?.custom_dashboards as { list: SavedDashboard[]; current: string | null });

function setup(stored: unknown, { cached = stored }: { cached?: unknown } = {}) {
  mocks.server = { other_preference: { custom_dashboards: stored } };
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  if (!mocks.readPending) qc.setQueryData(queryKeys.user.preferences(), { other_preference: { custom_dashboards: cached } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={qc}>
      <MemoryRouter>{children}</MemoryRouter>
    </QueryClientProvider>
  );
  return { qc, ...renderHook(() => useSavedDashboards(), { wrapper }) };
}

beforeEach(() => {
  mocks.readPending = false;
  mocks.failNextUpdate = false;
  mocks.updates = [];
  mocks.kept = [];
  mocks.toasts = [];
});

afterEach(() => {
  vi.useRealTimers();
});

describe('readSavedDashboards', () => {
  it('drops a page with no record, recovers bad fields alone, and keeps fields it does not know', () => {
    const read = readSavedDashboards({
      current: 'gone',
      future: { kept: true },
      list: [
        { kind: 'page', code: 'p1', title: 'P' },
        { kind: 'page', code: 'p2', artifactId: 'a2', title: 42, pinnedAt: 7 },
        { kind: 'app', code: 'a1', title: 'A', path: '', workspaceId: 'ws' },
        { kind: 'app', code: 'a1', title: 'dup' },
        { kind: 'widget', code: 'w1' },
        'junk',
      ],
    });
    expect(read.list.map((d) => [d.kind, d.code])).toEqual([['page', 'p2'], ['app', 'a1']]);
    expect(read.list[0]).toMatchObject({ title: '', artifactId: 'a2', pinnedAt: 7 });
    expect(read.list[1]).toMatchObject({ path: null, workspaceId: 'ws' });
    // A current that names nothing on the list is the built-in dashboard.
    expect(read.current).toBeNull();
    expect((read as unknown as Record<string, unknown>).future).toEqual({ kept: true });
  });

  it('reads anything else as no dashboards', () => {
    expect(readSavedDashboards(undefined)).toEqual({ current: null, list: [] });
    expect(readSavedDashboards('x')).toEqual({ current: null, list: [] });
  });
});

describe('useSavedDashboards', () => {
  it('refuses to write before the saved list is read', async () => {
    mocks.readPending = true;
    const { result } = setup({ current: null, list: [app('a')] });
    let status: string | undefined;
    await act(async () => {
      status = await result.current.add(app('b'));
    });
    expect(status).toBe('notLoaded');
    expect(mocks.updates).toEqual([]);
    expect(mocks.toasts).toHaveLength(1);
  });

  it('builds a pin on the latest row, not the one this tab last read', async () => {
    // Another tab added `b` since this one read.
    const { result } = setup({ current: null, list: [app('a'), app('b')] }, { cached: { current: null, list: [app('a')] } });
    await act(async () => {
      await result.current.add(app('c'));
    });
    expect(lastSaved().list.map((d) => d.code)).toEqual(['c', 'a', 'b']);
    expect(lastSaved().current).toBe('c');
  });

  it('keeps both of two pins made before either re-renders', async () => {
    const { result } = setup({ current: null, list: [] });
    const { add } = result.current;
    await act(async () => {
      await add(app('a'));
      await add(app('b'));
    });
    expect(lastSaved().list.map((d) => d.code)).toEqual(['b', 'a']);
  });

  it('keeps both of two changes begun together', async () => {
    const { result } = setup({ current: null, list: [app('a'), app('b'), app('c')] });
    await act(async () => {
      result.current.remove('a');
      result.current.remove('b');
    });
    await waitFor(() => expect(mocks.updates).toHaveLength(2));
    expect(lastSaved().list.map((d) => d.code)).toEqual(['c']);
  });

  it('keeps both of two changes begun together in two tabs', async () => {
    // Each tab has a queue of its own; the browser's lock is what they share.
    let held: Promise<unknown> = Promise.resolve();
    const locks = {
      request: (_name: string, run: () => Promise<unknown>) => {
        const turn = held.then(run, run);
        held = turn.catch(() => {});
        return turn;
      },
    };
    Object.defineProperty(navigator, 'locks', { value: locks, configurable: true });
    try {
      const stored = { current: null, list: [app('a'), app('b'), app('c')] };
      const one = setup(stored);
      const other = setup(stored);
      await act(async () => {
        one.result.current.remove('a');
        other.result.current.remove('b');
      });
      await waitFor(() => expect(mocks.updates).toHaveLength(2));
      expect(lastSaved().list.map((d) => d.code)).toEqual(['c']);
    } finally {
      delete (navigator as { locks?: unknown }).locks;
    }
  });

  it('switches on the latest list, keeping one added elsewhere since this tab read', async () => {
    const { result } = setup({ current: null, list: [app('a'), app('b')] }, { cached: { current: null, list: [app('a')] } });
    await act(async () => {
      result.current.select('a');
    });
    await waitFor(() => expect(mocks.updates).toHaveLength(1));
    expect(lastSaved()).toEqual({ current: 'a', list: [app('a'), app('b')] });
  });

  it('tells other tabs to read again once a write lands', async () => {
    const heard = vi.fn();
    const chan = new BroadcastChannel('dashboard-prefs');
    chan.onmessage = (e) => heard(e.data);
    const { result } = setup({ current: null, list: [] });
    await act(async () => {
      await result.current.add(app('a'));
    });
    await waitFor(() => expect(heard).toHaveBeenCalledWith({ type: 'updated' }));
    chan.close();
  });

  it('refuses a new pin at the cap and drops nothing', async () => {
    const full = Array.from({ length: MAX_DASHBOARDS }, (_, i) => app(`a${i}`));
    const { result } = setup({ current: null, list: full });
    let status: string | undefined;
    await act(async () => {
      status = await result.current.add(app('new'));
    });
    expect(status).toBe('full');
    expect(mocks.updates).toEqual([]);
    expect(String(mocks.toasts.at(-1)?.description)).toContain(String(MAX_DASHBOARDS));
    expect(mocks.toasts.at(-1)?.action).toBeTruthy();
    // One already listed still refreshes at the cap.
    await act(async () => {
      status = await result.current.add(app('a3', { title: 'renamed' }));
    });
    expect(status).toBe('saved');
    expect(lastSaved().list).toHaveLength(MAX_DASHBOARDS);
  });

  it('lets a removed page go only once its Undo is gone', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { result } = setup({ current: 'p', list: [app('a'), page('p'), app('b')] });
    await act(async () => {
      result.current.remove('p');
    });
    await waitFor(() => expect(mocks.toasts.at(-1)?.action).toBeTruthy());
    expect(lastSaved()).toMatchObject({ current: null, list: [{ code: 'a' }, { code: 'b' }] });
    expect(mocks.kept).toEqual([]);
    await act(async () => {
      vi.advanceTimersByTime(UNDO_MS + 10);
    });
    expect(mocks.kept).toEqual([['art-p', false]]);
  });

  it('puts an undone removal back where it was and never lets it go', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { result } = setup({ current: 'p', list: [app('a'), page('p'), app('b')] });
    await act(async () => {
      result.current.remove('p');
    });
    await waitFor(() => expect(mocks.toasts.at(-1)?.action).toBeTruthy());
    const undo = mocks.toasts.at(-1)!.action!.props as { onClick: () => void };
    await act(async () => {
      undo.onClick();
    });
    await waitFor(() => expect(lastSaved().list.map((d) => d.code)).toEqual(['a', 'p', 'b']));
    expect(lastSaved().current).toBe('p');
    await act(async () => {
      vi.advanceTimersByTime(UNDO_MS + 10);
    });
    expect(mocks.kept).toEqual([]);
  });

  it('lets nothing go when the removal fails to save', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const { result } = setup({ current: null, list: [page('p')] });
    mocks.failNextUpdate = true;
    await act(async () => {
      result.current.remove('p');
    });
    await waitFor(() => expect(mocks.toasts.at(-1)?.title).toBeTruthy());
    await act(async () => {
      vi.advanceTimersByTime(UNDO_MS + 10);
    });
    expect(mocks.kept).toEqual([]);
  });
});
