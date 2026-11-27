import { useLayoutEffect } from 'react';
import { act, render } from '@testing-library/react';
import { MemoryRouter, useLocation, useNavigate, type NavigateFunction } from 'react-router';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { setStorageUser } from '@/lib/userStorage';
import { PageTabsProvider } from '../PageTabsProvider';
import { usePageTabActions, usePageTabs, type PageTabActions, type PageTabsState } from '../pageTabsContext';
import { usePageTabLeaveGuard } from '../usePageTabLeaveGuard';
import { MAX_TABS } from '../pageTabsStorage';

vi.mock('@/hooks/useFeatures', () => ({ useFeatureEnabled: () => true }));
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));
// A platform build, where the account can change in place.
vi.mock('@/config/hostMode', async (original) => ({ ...(await original<object>()), isPlatformMode: true }));
vi.mock('@/config/localUser', async (original) => ({ ...(await original<object>()), hasAuthEnv: true }));
const toast = vi.hoisted(() => vi.fn());
vi.mock('@/components/ui/use-toast', () => ({ toast }));

const seen: { path: string; tabs: PageTabsState | null; actions: PageTabActions | null; navigate: NavigateFunction | null } = { path: '', tabs: null, actions: null, navigate: null };

function Probe({ guard }: { guard: ((go: () => void) => void) | null }) {
  const path = useLocation().pathname;
  const tabs = usePageTabs();
  const actions = usePageTabActions();
  const navigate = useNavigate();
  useLayoutEffect(() => {
    Object.assign(seen, { path, tabs, actions, navigate });
  });
  usePageTabLeaveGuard(guard);
  return null;
}

/** A guard that holds each leave until the test answers it. */
function heldGuard() {
  const asked: Array<() => void> = [];
  return { asked, guard: (go: () => void) => void asked.push(go) };
}

function mount(guard: ((go: () => void) => void) | null) {
  return render(
    <MemoryRouter initialEntries={['/dashboard']}>
      <PageTabsProvider>
        <Probe guard={guard} />
      </PageTabsProvider>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  setStorageUser('u1');
});
afterEach(() => setStorageUser(null));

describe('PageTabsProvider on an account switch in place', () => {
  it("goes to the incoming account's front tab and keeps it", () => {
    localStorage.setItem('user:u2:app-page-tabs', JSON.stringify({ tabs: [{ id: 'o', path: '/orders' }], activeId: 'o' }));
    mount(null);
    act(() => seen.actions!.open('/market'));
    act(() => setStorageUser('u2'));
    expect(seen.path).toBe('/orders');
    const saved = JSON.parse(localStorage.getItem('user:u2:app-page-tabs') ?? '{}');
    expect(saved.tabs.find((t: { id: string }) => t.id === saved.activeId).path).toBe('/orders');
  });
});

describe('PageTabsProvider when the strip is full', () => {
  it('says so instead of opening nothing', () => {
    mount(null);
    for (let i = 1; i < MAX_TABS; i++) act(() => seen.actions!.open(`/p${i}`));
    expect(seen.tabs!.tabs).toHaveLength(MAX_TABS);
    act(() => seen.actions!.open('/one-more'));
    expect(seen.tabs!.tabs).toHaveLength(MAX_TABS);
    expect(seen.path).toBe(`/p${MAX_TABS - 1}`);
    expect(toast).toHaveBeenCalledTimes(1);
  });

  it("reopens a closed tab on Back rather than putting its page in the tab in front", () => {
    mount(null);
    const first = seen.tabs!.activeId;
    act(() => seen.actions!.open('/closed'));
    const gone = seen.tabs!.activeId;
    act(() => seen.actions!.select(first));
    act(() => seen.actions!.close(gone));
    for (let i = 1; i < MAX_TABS; i++) act(() => seen.actions!.open(`/p${i}`));
    expect(seen.tabs!.tabs).toHaveLength(MAX_TABS);
    // Back past the opens and the switch, onto the entry the closed tab was on.
    act(() => void seen.navigate!(-MAX_TABS));
    expect(seen.path).toBe('/closed');
    expect(seen.tabs!.activeId).toBe(gone);
    expect(seen.tabs!.tabs).toHaveLength(MAX_TABS + 1);
    expect(seen.tabs!.tabs.find((t) => t.path === `/p${MAX_TABS - 1}`)).toBeDefined();
  });
});

describe('PageTabsProvider leave guard', () => {
  it('opens a tab only once the page on screen lets it go', () => {
    const { asked, guard } = heldGuard();
    mount(guard);
    act(() => seen.actions!.open('/market'));
    expect(asked).toHaveLength(1);
    expect(seen.path).toBe('/dashboard');
    expect(seen.tabs!.tabs).toHaveLength(1);

    act(() => asked[0]());
    expect(seen.path).toBe('/market');
    expect(seen.tabs!.tabs).toHaveLength(2);
  });

  it('keeps the tab in front when closing it is declined', () => {
    const { asked, guard } = heldGuard();
    const { rerender } = mount(null);
    act(() => seen.actions!.open('/market'));
    rerender(
      <MemoryRouter initialEntries={['/dashboard']}>
        <PageTabsProvider>
          <Probe guard={guard} />
        </PageTabsProvider>
      </MemoryRouter>,
    );
    const front = seen.tabs!.activeId;
    act(() => seen.actions!.close(front));
    expect(asked).toHaveLength(1);
    // Declined: nothing runs, so the tab stays open and in front.
    expect(seen.tabs!.tabs.map((t) => t.id)).toContain(front);
    expect(seen.tabs!.activeId).toBe(front);
    expect(seen.path).toBe('/market');
  });

  it('drops a leave answered after another account signed in', () => {
    const { asked, guard } = heldGuard();
    mount(guard);
    act(() => seen.actions!.open('/market'));
    act(() => setStorageUser('u2'));
    act(() => asked[0]());
    expect(seen.path).toBe('/dashboard');
    expect(localStorage.getItem('user:u2:app-page-tabs') ?? '').not.toContain('/market');
  });

  it('asks nothing once the page withdraws its guard', () => {
    const { asked, guard } = heldGuard();
    const { rerender } = mount(guard);
    rerender(
      <MemoryRouter initialEntries={['/dashboard']}>
        <PageTabsProvider>
          <Probe guard={null} />
        </PageTabsProvider>
      </MemoryRouter>,
    );
    act(() => seen.actions!.open('/market'));
    expect(asked).toHaveLength(0);
    expect(seen.path).toBe('/market');
  });
});
