import { describe, it, expect, beforeEach, vi } from 'vitest';
import { useEffect, useLayoutEffect, useState } from 'react';
import { act, renderHook } from '@testing-library/react';
import { runAuthResets } from '@/lib/authResets';
import { userLocalStorage } from '@/lib/userStorage';
import { freshStrips, handChatStrips, releaseChatStrips } from '../freshStrips';
import { useFileTabs, lastChartSymbol, lastChartStorageKey, tabsStorageKey, threadTabsStorageKey, type FileTab } from '../useFileTabs';

/** File paths in strip order; the empty tab reads as null. */
const paths = (tabs: FileTab[]) => tabs.map((t) => (t.kind === 'file' ? t.path : null));
const activePath = ({ tabs, activeId }: { tabs: FileTab[]; activeId: string }) => {
  const tab = tabs.find((t) => t.id === activeId);
  return tab?.kind === 'file' ? tab.path : null;
};
const ports = (tabs: FileTab[]) => tabs.map((t) => (t.kind === 'preview' ? t.port : undefined));
const stored = (key: string) => JSON.parse(userLocalStorage.getItem(key)!);

beforeEach(() => localStorage.clear());

describe('useFileTabs browser tab', () => {
  it('is one tab, kept across a reload', () => {
    const first = renderHook(() => useFileTabs('ws', 't1'));
    act(() => first.result.current.openBrowser());
    act(() => first.result.current.openFile('a.md', { pin: true }));
    act(() => first.result.current.openBrowser());
    expect(first.result.current.tabs.filter((t) => t.kind === 'browser')).toHaveLength(1);
    expect(first.result.current.activeTab.kind).toBe('browser');
    first.unmount();

    const second = renderHook(() => useFileTabs('ws', 't1'));
    expect(second.result.current.tabs.some((t) => t.kind === 'browser')).toBe(true);
  });

  it('can arrive without taking the reader off what they are reading', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openBrowser({ focus: false }));
    expect(result.current.tabs.some((t) => t.kind === 'browser')).toBe(true);
    expect(activePath(result.current)).toBe('a.md');
  });
});

describe('useFileTabs preview semantics', () => {
  it('opens on a single empty tab', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    expect(paths(result.current.tabs)).toEqual([null]);
    expect(result.current.activeTab.kind).toBe('empty');
  });

  it('reuses the one preview tab so browsing never piles tabs up', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.openFile('b.md'));

    expect(paths(result.current.tabs)).toEqual(['b.md']);
    expect(result.current.activeTab).toMatchObject({ kind: 'file', preview: true });
  });

  it('keeps a pinned tab when the next file is only previewed', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openFile('b.md'));

    expect(paths(result.current.tabs)).toEqual(['a.md', 'b.md']);
    expect(result.current.activeTab).toMatchObject({ path: 'b.md' });
  });

  it('fills the empty tab the user is looking at, not the preview elsewhere', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.newTab());
    act(() => result.current.openFile('b.md'));

    expect(paths(result.current.tabs)).toEqual(['a.md', 'b.md']);
    expect(result.current.activeTab).toMatchObject({ path: 'b.md' });
    expect(result.current.tabs[0]).toMatchObject({ preview: true });
  });

  it('opens beside the active tab rather than at the end of the strip', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openFile('c.md', { pin: true }));
    act(() => result.current.activate(result.current.tabs[0].id));
    act(() => result.current.openFile('b.md', { pin: true }));

    expect(paths(result.current.tabs)).toEqual(['a.md', 'b.md', 'c.md']);
  });

  it('pins the preview tab in place rather than opening a second one', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.pinTab(result.current.activeId));
    act(() => result.current.openFile('b.md'));

    expect(paths(result.current.tabs)).toEqual(['a.md', 'b.md']);
  });

  it('activates the tab a file already has instead of opening another', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openFile('b.md', { pin: true }));
    act(() => result.current.openFile('a.md'));

    expect(paths(result.current.tabs)).toEqual(['a.md', 'b.md']);
    // Re-opening as a preview must not un-pin what was already kept.
    expect(result.current.activeTab).toMatchObject({ path: 'a.md', preview: false });
  });

  it('replays a location asked for twice, so a repeat click highlights again', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { location: { line: 4 } }));
    act(() => result.current.openFile('a.md', { location: { line: 4 } }));

    expect(result.current.activeTab).toMatchObject({ location: { line: 4 }, locationSeq: 2 });
  });

  it('keeps each tab its own location so switching back returns there', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { pin: true, location: { line: 9 } }));
    const aId = result.current.activeId;
    act(() => result.current.openFile('b.md', { pin: true, location: { page: 3 } }));
    act(() => result.current.activate(aId));

    expect(result.current.activeTab).toMatchObject({ location: { line: 9 } });
  });

  it('does not carry one file’s location onto the next file browsed into its tab', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { location: { line: 4 } }));
    act(() => result.current.openFile('b.md'));

    expect(result.current.activeTab).toMatchObject({ path: 'b.md', location: null, locationSeq: 0 });
  });
});

describe('useFileTabs running apps', () => {
  it('names a preview tab by its port when the agent gave it no title', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openPreview({ port: 8050 }));

    expect(result.current.activeTab).toMatchObject({ kind: 'preview', port: 8050 });
    // An app is not on loan the way a browsed file is: it stays until closed.
    expect(result.current.activeTab).not.toHaveProperty('preview');
  });

  it('gives a port one tab however often it is opened', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openPreview({ port: 8050, title: 'Dashboard' }));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openPreview({ port: 8050 }));

    expect(result.current.tabs).toHaveLength(2);
    // Re-opening by port alone must not blank the title the agent gave it.
    expect(result.current.activeTab).toMatchObject({ port: 8050, title: 'Dashboard' });
  });

  it('gives two ports two tabs, opened beside the active one', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openPreview({ port: 8050 }));
    act(() => result.current.openPreview({ port: 8051 }));

    expect(ports(result.current.tabs)).toEqual([undefined, 8050, 8051]);
    expect(result.current.activeTab).toMatchObject({ port: 8051 });
  });

  it('fills the empty tab the reader is looking at', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openPreview({ port: 8050 }));

    expect(result.current.tabs).toHaveLength(1);
  });

  it('stores the port and its labels, never the signed URL', () => {
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    act(() => result.current.openPreview({
      port: 8050, title: 'Dashboard', path: '/timeline.html', command: 'python app.py',
    }));

    expect(stored(threadTabsStorageKey('ws', 't1'))).toEqual({
      tabs: [{ kind: 'preview', port: 8050, title: 'Dashboard', previewPath: '/timeline.html', command: 'python app.py' }],
      active: 0,
    });
  });

  it('brings the start command back with a restored preview tab', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [{ kind: 'preview', port: 8050, command: 'python app.py' }],
      active: 0,
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(result.current.activeTab).toMatchObject({ port: 8050, command: 'python app.py' });
  });

  it('reopens a running app as a port to mint against', () => {
    const first = renderHook(() => useFileTabs('ws', 't1'));
    act(() => first.result.current.openPreview({ port: 8050, title: 'Dashboard' }));
    first.unmount();

    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(result.current.activeTab).toMatchObject({ kind: 'preview', port: 8050, title: 'Dashboard' });
  });

  it('drops a stored preview tab that names no port', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [{ kind: 'preview' }],
      active: 0,
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));

    expect(paths(result.current.tabs)).toEqual([null]);
    expect(result.current.activeTab.kind).toBe('empty');
  });

  it('drops a stored preview whose port the preview endpoint would refuse', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [{ kind: 'preview', port: 80 }, { kind: 'preview', port: 10000 }, { kind: 'preview', port: 5173 }],
      active: 0,
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));

    expect(ports(result.current.tabs)).toEqual([5173]);
  });
});

describe('useFileTabs pages', () => {
  it('drops a page tab a build with published pages stored, keeping the file beside it', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [{ kind: 'file', path: 'a.md' }, { kind: 'page', pageId: '6f1c2a7e-3b4d-4e5f-8a9b-0c1d2e3f4a5b', version: 2 }],
      active: 1,
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));

    expect(paths(result.current.tabs)).toEqual(['a.md']);
    expect(result.current.activeTab).toMatchObject({ kind: 'file', path: 'a.md' });
  });
});

describe('useFileTabs closing', () => {
  it('leaves an empty tab rather than a panel with no way back', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.closeTab(result.current.activeId));

    expect(paths(result.current.tabs)).toEqual([null]);
    expect(result.current.activeTab.kind).toBe('empty');
  });

  it('hands the active slot to the tab that takes the closed one’s place', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openFile('b.md', { pin: true }));
    const bId = result.current.activeId;
    act(() => result.current.openFile('c.md', { pin: true }));
    act(() => result.current.closeTab(bId));

    expect(paths(result.current.tabs)).toEqual(['a.md', 'c.md']);
    expect(result.current.activeTab).toMatchObject({ path: 'c.md' });
  });

  it('leaves the active tab alone when another one closes', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { pin: true }));
    const aId = result.current.activeId;
    act(() => result.current.openFile('b.md', { pin: true }));
    act(() => result.current.closeTab(aId));

    expect(result.current.activeTab).toMatchObject({ path: 'b.md' });
  });
});

describe('useFileTabs persistence', () => {
  it('stores names and the active index, never the bytes', () => {
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openFile('b.md'));

    expect(stored(threadTabsStorageKey('ws', 't1'))).toEqual({
      tabs: [{ kind: 'file', path: 'a.md', preview: false }, { kind: 'file', path: 'b.md', preview: true }],
      active: 1,
    });
  });

  it('reopens the strip, and the tab that was in front, on a fresh mount', () => {
    const first = renderHook(() => useFileTabs('ws', 't1'));
    act(() => first.result.current.openFile('a.md', { pin: true }));
    act(() => first.result.current.openFile('b.md', { pin: true }));
    act(() => first.result.current.activate(first.result.current.tabs[0].id));
    first.unmount();

    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(paths(result.current.tabs)).toEqual(['a.md', 'b.md']);
    expect(result.current.activeTab).toMatchObject({ path: 'a.md' });
  });

  it('keeps the empty tab out of storage', () => {
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.newTab());

    expect(stored(threadTabsStorageKey('ws', 't1')).tabs).toEqual([{ kind: 'file', path: 'a.md', preview: false }]);
  });

  it('still reads a strip written before tabs carried a kind', () => {
    // File tabs were stored as `{ path, preview }` and every other kind carried
    // `path: null, preview: false` beside its own fields; a chart could also
    // have no interval yet.
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [
        { path: 'a.md', preview: true },
        { path: null, kind: 'settings', preview: false },
        { path: null, kind: 'preview', preview: false, port: 8050, title: 'Dashboard' },
        { path: null, kind: 'chart', preview: false, symbol: 'NVDA' },
      ],
      active: 3,
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));

    expect(result.current.tabs.map((t) => t.kind)).toEqual(['file', 'settings', 'preview', 'chart']);
    expect(result.current.tabs[0]).toMatchObject({ path: 'a.md', preview: true, location: null, locationSeq: 0 });
    expect(result.current.tabs[2]).toMatchObject({ port: 8050, title: 'Dashboard' });
    expect(result.current.activeTab).toMatchObject({ kind: 'chart', symbol: 'NVDA', timeframe: '1day' });
  });

  it('drops the entries it cannot read and keeps the rest', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [{ kind: 'file', path: 'a.md', preview: false }, { kind: 'file', path: 7 }, 'junk', { kind: 'unknown' }],
      active: 'nope',
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));

    expect(paths(result.current.tabs)).toEqual(['a.md']);
  });

  it('keeps the tab that was in front when an entry before it is dropped', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [
        { kind: 'file', path: 'a.md', preview: false },
        { kind: 'unknown' },
        { kind: 'file', path: 'b.md', preview: false },
        { kind: 'file', path: 'c.md', preview: false },
      ],
      active: 2,
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));

    expect(paths(result.current.tabs)).toEqual(['a.md', 'b.md', 'c.md']);
    expect(activePath(result.current)).toBe('b.md');
  });

  it('falls back to the surviving tab before one that was itself dropped', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [{ kind: 'file', path: 'a.md', preview: false }, { kind: 'unknown' }, { kind: 'file', path: 'c.md', preview: false }],
      active: 1,
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));

    expect(activePath(result.current)).toBe('a.md');
  });

  it('gives each workspace its own strip', () => {
    const ws = renderHook(() => useFileTabs('ws', 't1'));
    act(() => ws.result.current.openFile('a.md', { pin: true }));
    ws.unmount();

    const { result } = renderHook(() => useFileTabs('other', 't1'));
    expect(paths(result.current.tabs)).toEqual([null]);
  });

  it('keeps memory and memo across a reload, and lets the watch tab go', () => {
    const first = renderHook(() => useFileTabs('ws', 't1'));
    act(() => first.result.current.openMemory());
    act(() => first.result.current.openMemo());
    act(() => first.result.current.openStatus());

    // A watch may be over by the next mount, so its tab is not written; the
    // stores are always there to come back to.
    expect(stored(threadTabsStorageKey('ws', 't1'))).toEqual({ tabs: [{ kind: 'memory' }, { kind: 'memo' }], active: 1 });
    first.unmount();

    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(result.current.tabs.map((t) => t.kind)).toEqual(['memory', 'memo']);
  });

  it('opens each store once, and comes back to the tab it has', () => {
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    act(() => result.current.openMemory());
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openMemory());

    expect(result.current.tabs.map((t) => t.kind)).toEqual(['memory', 'file']);
    expect(result.current.activeTab.kind).toBe('memory');
  });

  it('persists nothing for a share, which has no workspace of its own', () => {
    const { result } = renderHook(() => useFileTabs(''));
    act(() => result.current.openFile('a.md', { pin: true }));

    expect(localStorage.length).toBe(0);
    expect(paths(result.current.tabs)).toEqual(['a.md']);
  });

  it('starts clean on a strip that cannot be read back', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), '{not json');
    const { result } = renderHook(() => useFileTabs('ws', 't1'));

    expect(paths(result.current.tabs)).toEqual([null]);
  });
});

describe('useFileTabs thread scope', () => {
  it('keeps one strip per thread, starts a new thread empty, and restores a thread it returns to', () => {
    const { result, rerender } = renderHook(
      ({ thread }: { thread: string | null }) => useFileTabs('ws', thread),
      { initialProps: { thread: 't1' } },
    );
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openFile('b.md', { pin: true }));
    expect(stored(threadTabsStorageKey('ws', 't1')).tabs.map((t: { path: string }) => t.path))
      .toEqual(['a.md', 'b.md']);
    // The workspace key is only a fresh chat's in-memory identity: no strip lands on it.
    expect(userLocalStorage.getItem(tabsStorageKey('ws'))).toBeNull();

    // A new thread does not inherit the strip shown last.
    rerender({ thread: 't2' });
    expect(paths(result.current.tabs)).toEqual([null]);
    act(() => result.current.openFile('c.md', { pin: true }));
    expect(paths(result.current.tabs)).toEqual(['c.md']);

    // Coming back to the first thread restores exactly what it had.
    rerender({ thread: 't1' });
    expect(paths(result.current.tabs)).toEqual(['a.md', 'b.md']);
    rerender({ thread: 't2' });
    expect(paths(result.current.tabs)).toEqual(['c.md']);
    expect(userLocalStorage.getItem(tabsStorageKey('ws'))).toBeNull();
  });

  it('keeps a fresh chat’s strip in memory and stores it under the thread its first message mints', () => {
    const { result, rerender } = renderHook(
      ({ thread }: { thread: string | null }) => useFileTabs('ws', thread),
      { initialProps: { thread: null as string | null } },
    );
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openFile('b.md', { pin: true }));
    expect(paths(result.current.tabs)).toEqual(['a.md', 'b.md']);
    expect(localStorage.length).toBe(0);

    rerender({ thread: 't1' });
    expect(paths(result.current.tabs)).toEqual(['a.md', 'b.md']);
    expect(stored(threadTabsStorageKey('ws', 't1')).tabs.map((t: { path: string }) => t.path))
      .toEqual(['a.md', 'b.md']);
    expect(userLocalStorage.getItem(tabsStorageKey('ws'))).toBeNull();
  });

  it('holds a fresh chat’s strip under the chat, across the panel’s remounts, and no other chat’s', () => {
    // The panel unmounts when it closes or when the window crosses into the
    // phone layout; the chat it belongs to does not.
    const first = renderHook(() => useFileTabs('ws', null, { chat: 'c1' }));
    act(() => first.result.current.openFile('a.md', { pin: true }));
    first.unmount();

    const again = renderHook(() => useFileTabs('ws', null, { chat: 'c1' }));
    expect(paths(again.result.current.tabs)).toEqual(['a.md']);
    expect(renderHook(() => useFileTabs('ws', null, { chat: 'c2' })).result.current.tabs.map((t) => t.kind)).toEqual(['empty']);
    expect(localStorage.length).toBe(0);
    again.unmount();

    // Once its first message mints the thread the strip is the thread's, and
    // the chat holds nothing more.
    const minting = renderHook(
      ({ thread }: { thread: string | null }) => useFileTabs('ws', thread, { chat: 'c1' }),
      { initialProps: { thread: null as string | null } },
    );
    minting.rerender({ thread: 't1' });
    expect(paths(minting.result.current.tabs)).toEqual(['a.md']);
    minting.unmount();
    expect(renderHook(() => useFileTabs('ws', null, { chat: 'c1' })).result.current.tabs.map((t) => t.kind)).toEqual(['empty']);
  });

  it('hands a fresh chat’s strip to the thread minted while the panel was closed, and drops it on sign-out', () => {
    const closed = renderHook(() => useFileTabs('ws', null, { chat: 'c3' }));
    act(() => closed.result.current.openFile('a.md', { pin: true }));
    closed.unmount();

    // A thread that already has a strip keeps its own.
    userLocalStorage.setItem(threadTabsStorageKey('ws', 'old'), JSON.stringify({ tabs: [{ kind: 'file', path: 'kept.md' }], active: 0 }));
    expect(paths(renderHook(() => useFileTabs('ws', 'old', { chat: 'c3' })).result.current.tabs)).toEqual(['kept.md']);

    const reopened = renderHook(() => useFileTabs('ws', 't3', { chat: 'c3' }));
    expect(paths(reopened.result.current.tabs)).toEqual(['a.md']);
    expect(stored(threadTabsStorageKey('ws', 't3')).tabs.map((t: { path: string }) => t.path)).toEqual(['a.md']);
    expect(renderHook(() => useFileTabs('ws', null, { chat: 'c3' })).result.current.tabs.map((t) => t.kind)).toEqual(['empty']);

    const signedIn = renderHook(() => useFileTabs('ws', null, { chat: 'c4' }));
    act(() => signedIn.result.current.openFile('b.md', { pin: true }));
    signedIn.unmount();
    runAuthResets();
    expect(renderHook(() => useFileTabs('ws', null, { chat: 'c4' })).result.current.tabs.map((t) => t.kind)).toEqual(['empty']);
  });

  it('finds a chat’s held strip again when the panel comes back to its workspace after the thread is minted', () => {
    const { result, rerender } = renderHook(
      ({ ws, thread }: { ws: string; thread: string | null }) => useFileTabs(ws, thread, { chat: 'c5' }),
      { initialProps: { ws: 'A', thread: null as string | null } },
    );
    act(() => result.current.openFile('a.md', { pin: true }));
    rerender({ ws: 'B', thread: null });
    rerender({ ws: 'B', thread: 't5' });
    rerender({ ws: 'A', thread: 't5' });
    expect(paths(result.current.tabs)).toEqual(['a.md']);
  });

  it('keeps a chat’s held strip while its thread’s copy cannot be saved', () => {
    const first = renderHook(() => useFileTabs('ws', null, { chat: 'c6' }));
    act(() => first.result.current.openFile('a.md', { pin: true }));
    first.unmount();

    // It stays current while the writes keep failing, through to a last-tab close.
    const refused = vi.spyOn(userLocalStorage, 'setItem').mockImplementation(() => false);
    const handed = renderHook(() => useFileTabs('ws', 't6', { chat: 'c6' }));
    act(() => handed.result.current.openFile('b.md', { pin: true }));
    handed.unmount();
    expect(paths(renderHook(() => useFileTabs('ws', 't6', { chat: 'c6' })).result.current.tabs)).toEqual(['a.md', 'b.md']);
    const closing = renderHook(() => useFileTabs('ws', 't6', { chat: 'c6' }));
    act(() => closing.result.current.closeTab(closing.result.current.tabs[1].id));
    closing.unmount();
    expect(paths(renderHook(() => useFileTabs('ws', 't6', { chat: 'c6' })).result.current.tabs)).toEqual(['a.md']);
    const lastTab = renderHook(() => useFileTabs('ws', 't6', { chat: 'c6' }));
    act(() => lastTab.result.current.closeTab(lastTab.result.current.tabs[0].id));
    lastTab.unmount();
    expect(renderHook(() => useFileTabs('ws', 't6', { chat: 'c6' })).result.current.tabs.map((t) => t.kind)).toEqual(['empty']);
    const out = renderHook(() => useFileTabs('ws', 't6', { chat: 'c6' }));
    act(() => out.result.current.openFile('c.md', { pin: true }));
    act(() => out.result.current.closeOut());
    out.unmount();
    refused.mockRestore();

    expect(renderHook(() => useFileTabs('ws', 't6', { chat: 'c6' })).result.current.tabs.map((t) => t.kind)).toEqual(['empty']);
  });

  it('saves the strip a hidden chat hands to the thread its first message mints', () => {
    const { result, rerender } = renderHook(
      ({ thread, active }: { thread: string | null; active: boolean }) => useFileTabs('ws', thread, { chat: 'c8', active }),
      { initialProps: { thread: null as string | null, active: true } },
    );
    act(() => result.current.openFile('a.md', { pin: true }));
    rerender({ thread: null, active: false });
    rerender({ thread: 't8', active: false });
    expect(stored(threadTabsStorageKey('ws', 't8')).tabs.map((t: { path: string }) => t.path)).toEqual(['a.md']);

    // Saved once; later changes wait for the panel to be shown again.
    act(() => result.current.openFile('b.md', { pin: true }));
    expect(stored(threadTabsStorageKey('ws', 't8')).tabs.map((t: { path: string }) => t.path)).toEqual(['a.md']);
  });

  it('leaves a saved hand-off as any thread’s strip, and holds nothing for a panel with no chat', () => {
    const saved = renderHook(
      ({ thread }: { thread: string | null }) => useFileTabs('ws', thread, { chat: 'c9' }),
      { initialProps: { thread: null as string | null } },
    );
    act(() => saved.result.current.openFile('a.md', { pin: true }));
    saved.rerender({ thread: 't9' });
    const refused = vi.spyOn(userLocalStorage, 'setItem').mockImplementation(() => false);
    act(() => saved.result.current.openFile('b.md', { pin: true }));
    saved.unmount();
    expect(paths(renderHook(() => useFileTabs('ws', null, { chat: 'c9' })).result.current.tabs)).toEqual([null]);

    const keyless = renderHook(
      ({ thread }: { thread: string | null }) => useFileTabs('ws', thread),
      { initialProps: { thread: null as string | null } },
    );
    act(() => keyless.result.current.openFile('c.md', { pin: true }));
    keyless.rerender({ thread: 't10' });
    expect(paths(keyless.result.current.tabs)).toEqual(['c.md']);
    refused.mockRestore();
    expect(freshStrips.has(tabsStorageKey('ws'))).toBe(false);
  });

  it('hands what a chat held to its thread with the panel closed, before the chat goes', () => {
    const open = renderHook(() => useFileTabs('ws', null, { chat: 'c12' }));
    act(() => open.result.current.openFile('a.md', { pin: true }));
    open.unmount();
    userLocalStorage.setItem(threadTabsStorageKey('other', 't12'), JSON.stringify({ tabs: [{ kind: 'file', path: 'kept.md' }], active: 0 }));
    const elsewhere = renderHook(() => useFileTabs('other', null, { chat: 'c12' }));
    act(() => elsewhere.result.current.openFile('b.md', { pin: true }));
    elsewhere.unmount();

    handChatStrips('c12', 't12');
    releaseChatStrips('c12');
    expect(paths(renderHook(() => useFileTabs('ws', 't12', { chat: 'c12' })).result.current.tabs)).toEqual(['a.md']);
    // A thread that already had a strip keeps its own.
    expect(paths(renderHook(() => useFileTabs('other', 't12', { chat: 'c12' })).result.current.tabs)).toEqual(['kept.md']);
  });

  it('lets a chat’s held strips go with the chat', () => {
    const open = renderHook(() => useFileTabs('ws', null, { chat: 'c7' }));
    act(() => open.result.current.openFile('a.md', { pin: true }));
    open.unmount();
    releaseChatStrips('c7');
    expect(renderHook(() => useFileTabs('ws', null, { chat: 'c7' })).result.current.tabs.map((t) => t.kind)).toEqual(['empty']);
  });

  it('lets a hidden panel keep its strip without writing it over the one on screen', () => {
    // The chat keeps recently visited threads mounted off screen, and a hidden
    // panel is not showing anything, so it must not write while hidden.
    const hidden = renderHook(
      ({ active }: { active: boolean }) => useFileTabs('ws', 't1', { active }),
      { initialProps: { active: true } },
    );
    act(() => hidden.result.current.openFile('one.md', { pin: true }));
    hidden.rerender({ active: false });

    const shown = renderHook(() => useFileTabs('ws', 't2'));
    act(() => shown.result.current.openFile('two.md', { pin: true }));
    expect(stored(threadTabsStorageKey('ws', 't2')).tabs.map((t: { path: string }) => t.path)).toEqual(['two.md']);

    // A change in the hidden strip stays in memory until it is back on screen.
    act(() => hidden.result.current.openFile('three.md', { pin: true }));
    expect(paths(hidden.result.current.tabs)).toEqual(['one.md', 'three.md']);
    expect(stored(threadTabsStorageKey('ws', 't1')).tabs.map((t: { path: string }) => t.path)).toEqual(['one.md']);
    expect(stored(threadTabsStorageKey('ws', 't2')).tabs.map((t: { path: string }) => t.path)).toEqual(['two.md']);

    hidden.rerender({ active: true });
    expect(stored(threadTabsStorageKey('ws', 't1')).tabs.map((t: { path: string }) => t.path)).toEqual(['one.md', 'three.md']);
    expect(userLocalStorage.getItem(tabsStorageKey('ws'))).toBeNull();
  });

  it('does not write the outgoing thread’s strip over the incoming thread’s saved one', () => {
    const strip = (path: string) => JSON.stringify({ tabs: [{ kind: 'file', path, preview: false }], active: 0 });
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), strip('one.md'));
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't2'), strip('two.md'));
    // A sibling effect updating the same component first, the way a real panel
    // has several: with an update already pending, React defers the switch's
    // updater to the next render instead of computing it on the spot, and the
    // persist effect runs before it, holding t2's strip under t1's key.
    const { result, rerender } = renderHook(
      ({ thread }: { thread: string }) => {
        const [, bump] = useState(0);
        useEffect(() => { bump((n) => n + 1); }, [thread]);
        return useFileTabs('ws', thread);
      },
      { initialProps: { thread: 't2' } },
    );
    expect(paths(result.current.tabs)).toEqual(['two.md']);

    rerender({ thread: 't1' });
    expect(paths(result.current.tabs)).toEqual(['one.md']);
    expect(stored(threadTabsStorageKey('ws', 't1')).tabs.map((t: { path: string }) => t.path)).toEqual(['one.md']);
    expect(stored(threadTabsStorageKey('ws', 't2')).tabs.map((t: { path: string }) => t.path)).toEqual(['two.md']);
  });

  it('carries the strip over when a fresh chat gets its thread id', () => {
    const { result, rerender } = renderHook(
      ({ thread }: { thread: string | null }) => useFileTabs('ws', thread),
      { initialProps: { thread: null as string | null } },
    );
    act(() => result.current.openFile('a.md', { pin: true }));
    const id = result.current.activeTab.id;
    rerender({ thread: 't9' });
    // Same tab objects, not a re-read: a location or scroll it held survives.
    expect(result.current.activeTab.id).toBe(id);
    expect(paths(result.current.tabs)).toEqual(['a.md']);
    expect(userLocalStorage.getItem(threadTabsStorageKey('ws', 't9'))).not.toBeNull();
  });

  it('keeps a strip the reader emptied empty, rather than treating it as absent', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({ tabs: [], active: 0 }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(paths(result.current.tabs)).toEqual([null]);
  });

  it('comes back parked on the empty tab when that is where it was left', () => {
    const first = renderHook(() => useFileTabs('ws', 't1'));
    act(() => first.result.current.openFile('a.md', { pin: true }));
    act(() => first.result.current.newTab());
    expect(stored(threadTabsStorageKey('ws', 't1')).active).toBe(-1);

    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(paths(result.current.tabs)).toEqual(['a.md', null]);
    expect(result.current.activeTab.kind).toBe('empty');
  });

  it('drops a stored strip’s excess and out-of-range entries rather than the whole strip', () => {
    const tabs = Array.from({ length: 70 }, (_, i) => ({ kind: 'file', path: `f${i}.md`, preview: false }));
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [{ kind: 'preview', port: 0 }, { kind: 'chart', symbol: 'X'.repeat(40) }, ...tabs],
      active: 0,
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(result.current.tabs).toHaveLength(62);
    expect(result.current.tabs.every((t) => t.kind === 'file')).toBe(true);
  });

  it('drops a stored chart whose symbol is not a ticker and uppercases one that is', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [
        { kind: 'chart', symbol: '   ' },
        { kind: 'chart', symbol: 'amd' },
        { kind: 'file', path: 'a.md', preview: false },
      ],
      active: 1,
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(result.current.tabs.map((t) => t.kind)).toEqual(['chart', 'file']);
    expect(result.current.activeTab).toMatchObject({ kind: 'chart', symbol: 'AMD' });
  });

  it('starts a new workspace empty, ignoring a stale workspace strip an older build stored', () => {
    userLocalStorage.setItem(tabsStorageKey('ws2'), JSON.stringify({ tabs: [{ kind: 'file', path: 'z.md', preview: false }], active: 0 }));
    const { result, rerender } = renderHook(
      ({ ws }: { ws: string }) => useFileTabs(ws, 't1'),
      { initialProps: { ws: 'ws' } },
    );
    act(() => result.current.openFile('a.md', { pin: true }));
    rerender({ ws: 'ws2' });
    expect(paths(result.current.tabs)).toEqual([null]);
  });

  it('starts over when a strip that is not stored is pointed at another workspace', () => {
    // A peek beside a gallery keeps its strip in memory, but it is still this
    // workspace's: a second reference to another workspace swaps the id under
    // a live strip, and the tabs left on screen would be read through it.
    userLocalStorage.setItem(tabsStorageKey('ws2'), JSON.stringify({ tabs: [{ kind: 'file', path: 'z.md', preview: false }], active: 0 }));
    const { result, rerender } = renderHook(
      ({ ws }: { ws: string }) => useFileTabs(ws, null, { persist: false }),
      { initialProps: { ws: 'ws' } },
    );
    act(() => result.current.openFile('a.md', { pin: true }));
    expect(paths(result.current.tabs)).toEqual(['a.md']);

    rerender({ ws: 'ws2' });
    // Blank rather than ws2's stale strip: a strip that writes nothing reads nothing.
    expect(paths(result.current.tabs)).toEqual([null]);
    expect(stored(tabsStorageKey('ws2')).tabs).toEqual([{ kind: 'file', path: 'z.md', preview: false }]);
    expect(userLocalStorage.getItem(tabsStorageKey('ws'))).toBeNull();
  });
});

describe('useFileTabs opens a file in the workspace it moves to', () => {
  // Regression: a file row naming another workspace (a sent message's
  // attachment) moves the panel there and opens the file in one update. The
  // open ran in a layout effect and the strip switch after it, which replaced
  // the new tab with a blank strip.
  it('keeps a file opened in the same update that changes the workspace', () => {
    const { result, rerender } = renderHook(
      ({ ws, open }: { ws: string; open: string | null }) => {
        const tabs = useFileTabs(ws, null, { persist: false });
        useLayoutEffect(() => { if (open) tabs.openFile(open); }, [open]); // eslint-disable-line react-hooks/exhaustive-deps
        return tabs;
      },
      { initialProps: { ws: 'home', open: null as string | null } },
    );
    rerender({ ws: 'nvidia', open: 'charts/drop_cn.png' });
    expect(activePath(result.current)).toBe('charts/drop_cn.png');
  });
});

describe('useFileTabs chart tabs', () => {
  it('opens one tab per symbol and comes back to it', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'GOOGL', timeframe: '1day' }));
    act(() => result.current.openFile('notes.md', { pin: true }));
    act(() => result.current.openChart({ symbol: 'googl' }));

    const charts = result.current.tabs.filter((t) => t.kind === 'chart');
    expect(charts).toHaveLength(1);
    expect(result.current.activeTab).toMatchObject({ kind: 'chart', symbol: 'GOOGL', timeframe: '1day' });
  });

  it('opens on the daily interval when none is named', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'GOOGL' }));
    expect(result.current.activeTab).toMatchObject({ kind: 'chart', timeframe: '1day' });
  });

  it('moves the chart to a named interval and keeps the last one otherwise', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'GOOGL', timeframe: '1day' }));
    act(() => result.current.openChart({ symbol: 'GOOGL', timeframe: '1hour' }));
    expect(result.current.activeTab).toMatchObject({ timeframe: '1hour' });

    act(() => result.current.openChart({ symbol: 'GOOGL' }));
    expect(result.current.activeTab).toMatchObject({ timeframe: '1hour' });
  });

  it('persists the symbol and interval, and restores the tab from them', () => {
    const first = renderHook(() => useFileTabs('ws', 't1'));
    act(() => first.result.current.openChart({ symbol: 'NVDA', timeframe: '4hour' }));
    expect(stored(threadTabsStorageKey('ws', 't1')).tabs).toEqual([{ kind: 'chart', symbol: 'NVDA', timeframe: '4hour' }]);
    first.unmount();

    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(result.current.activeTab).toMatchObject({ kind: 'chart', symbol: 'NVDA', timeframe: '4hour' });
  });

  it('restores a chart tab whose stored interval this build no longer has on the daily view', () => {
    userLocalStorage.setItem(
      threadTabsStorageKey('ws', 't1'),
      JSON.stringify({ tabs: [{ kind: 'chart', symbol: 'NVDA', timeframe: '7min' }], active: 0 }),
    );
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(result.current.activeTab).toMatchObject({ kind: 'chart', symbol: 'NVDA', timeframe: '1day' });
  });

  it('drops a stored chart tab with no symbol', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({ tabs: [{ kind: 'chart' }], active: 0 }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(paths(result.current.tabs)).toEqual([null]);
  });

  it('lets a chart tab be patched in place, which is how the toolbar interval is remembered', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'GOOGL' }));
    const id = result.current.activeId;
    act(() => result.current.patchTab(id, (t) => (t.kind === 'chart' ? { ...t, timeframe: '1hour' } : t)));

    expect(result.current.activeTab).toMatchObject({ id, symbol: 'GOOGL', timeframe: '1hour' });
    expect(result.current.tabs).toHaveLength(1);
  });
});

describe('useFileTabs chart retarget', () => {
  it('points a chart tab at another symbol in place, keeping its interval', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'GOOGL', timeframe: '1hour' }));
    const id = result.current.activeId;
    act(() => result.current.retargetChart(id, 'msft'));

    expect(result.current.tabs).toHaveLength(1);
    expect(result.current.activeTab).toMatchObject({ id, symbol: 'MSFT', timeframe: '1hour' });
  });

  it('folds into the tab that already shows the symbol, on the interval being looked at', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'GOOGL', timeframe: '1day' }));
    const googl = result.current.activeId;
    act(() => result.current.newTab());
    act(() => result.current.openChart({ symbol: 'NVDA', timeframe: '1hour' }));
    act(() => result.current.retargetChart(result.current.activeId, 'GOOGL'));

    expect(result.current.tabs.filter((t) => t.kind === 'chart')).toHaveLength(1);
    expect(result.current.activeId).toBe(googl);
    expect(result.current.activeTab).toMatchObject({ symbol: 'GOOGL', timeframe: '1hour' });
  });

  it('keeps the drawings of the tab being switched when it folds into another', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    const wsA = '11111111-1111-4111-8111-111111111111';
    const wsB = '22222222-2222-4222-8222-222222222222';
    act(() => result.current.openChart({ symbol: 'NVDA', workspaceId: wsB }));
    act(() => result.current.newTab());
    act(() => result.current.openChart({ symbol: 'AAPL', workspaceId: wsA }));
    act(() => result.current.retargetChart(result.current.activeId, 'NVDA'));
    expect(result.current.activeTab).toMatchObject({ symbol: 'NVDA', workspaceId: wsA });

    // A tab on the panel's own workspace folds in without the other's.
    act(() => result.current.newTab());
    act(() => result.current.openChart({ symbol: 'MSFT' }));
    act(() => result.current.retargetChart(result.current.activeId, 'NVDA'));
    expect(result.current.activeTab).toMatchObject({ symbol: 'NVDA' });
    expect(result.current.activeTab).not.toHaveProperty('workspaceId');
  });

  it('takes the instrument forms the providers serve besides equities', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'eurusd=x' }));
    expect(result.current.activeTab).toMatchObject({ kind: 'chart', symbol: 'EURUSD=X' });
    act(() => result.current.retargetChart(result.current.activeId, 'X:BTCUSD'));
    expect(result.current.activeTab).toMatchObject({ symbol: 'X:BTCUSD' });
  });

  it('ignores a blank ticker', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'GOOGL' }));
    act(() => result.current.retargetChart(result.current.activeId, '   '));
    expect(result.current.activeTab).toMatchObject({ symbol: 'GOOGL' });
  });

  it('remembers the last symbol for the next chart opened blind', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    expect(lastChartSymbol('ws')).toBe('SPY');
    act(() => result.current.openChart({ symbol: 'AMD' }));
    expect(lastChartSymbol('ws')).toBe('AMD');
    expect(lastChartSymbol('other')).toBe('SPY');
  });

  it('falls back to the default when the stored symbol is not a ticker', () => {
    userLocalStorage.setItem(lastChartStorageKey('ws'), '   ');
    expect(lastChartSymbol('ws')).toBe('SPY');
    userLocalStorage.setItem(lastChartStorageKey('ws'), 'not a ticker at all');
    expect(lastChartSymbol('ws')).toBe('SPY');
    userLocalStorage.setItem(lastChartStorageKey('ws'), 'amd');
    expect(lastChartSymbol('ws')).toBe('AMD');
  });
});

describe('useFileTabs transcript tabs', () => {
  const kinds = (tabs: FileTab[]) => tabs.map((t) => t.kind);

  it('retargets the one loaned tool tab as rows are clicked through', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openTool({ toolCallId: 'a' }));
    act(() => result.current.openTool({ toolCallId: 'b' }));

    expect(kinds(result.current.tabs)).toEqual(['tool']);
    expect(result.current.activeTab).toMatchObject({ kind: 'tool', toolCallId: 'b', preview: true });
  });

  it('keeps a pinned tool tab and opens the next row beside it', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openTool({ toolCallId: 'a' }));
    act(() => result.current.pinTab(result.current.activeId));
    act(() => result.current.openTool({ toolCallId: 'b' }));

    expect(result.current.tabs.map((t) => (t.kind === 'tool' ? t.toolCallId : null))).toEqual(['a', 'b']);
    expect(result.current.tabs[0]).toMatchObject({ preview: false });
    expect(result.current.activeTab).toMatchObject({ toolCallId: 'b', preview: true });
  });

  it('comes back to the tab a tool call already has', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openTool({ toolCallId: 'a' }));
    act(() => result.current.pinTab(result.current.activeId));
    act(() => result.current.openTool({ toolCallId: 'b' }));
    act(() => result.current.openTool({ toolCallId: 'a' }));

    expect(result.current.tabs).toHaveLength(2);
    expect(result.current.activeTab).toMatchObject({ toolCallId: 'a', preview: false });
  });

  it('holds a tool call by id only, so the tab reads the live record', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openTool({ toolCallId: 'a' }));

    expect(result.current.activeTab).toEqual({ id: expect.any(String), kind: 'tool', toolCallId: 'a', preview: true });
  });

  it('never takes the file being browsed for a tool result', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.openTool({ toolCallId: 'a' }));

    expect(kinds(result.current.tabs)).toEqual(['file', 'tool']);
    expect(result.current.tabs[0]).toMatchObject({ path: 'a.md', preview: true });
  });

  it('opens one sources tab per turn in a slot of its own', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openTool({ toolCallId: 'a' }));
    act(() => result.current.openSources('m1'));
    act(() => result.current.openSources('m2'));

    expect(kinds(result.current.tabs)).toEqual(['tool', 'sources']);
    expect(result.current.activeTab).toMatchObject({ kind: 'sources', messageId: 'm2', preview: true });
  });

  it('stores none of them, and comes back on the stored tab before the one that was in front', () => {
    const { result, unmount } = renderHook(() => useFileTabs('ws', 't1'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openTool({ toolCallId: 'a' }));
    act(() => result.current.openSources('m1'));
    expect(stored(threadTabsStorageKey('ws', 't1'))).toEqual({ tabs: [{ kind: 'file', path: 'a.md', preview: false }], active: 0 });
    unmount();

    const { result: again } = renderHook(() => useFileTabs('ws', 't1'));
    expect(kinds(again.current.tabs)).toEqual(['file']);
    expect(again.current.activeTab).toMatchObject({ path: 'a.md' });
  });

  it('drops a stored tool or sources entry, should one turn up', () => {
    userLocalStorage.setItem(threadTabsStorageKey('ws', 't1'), JSON.stringify({
      tabs: [{ kind: 'tool', toolCallId: 'a' }, { kind: 'file', path: 'a.md' }, { kind: 'sources', messageId: 'm' }],
      active: 2,
    }));
    const { result } = renderHook(() => useFileTabs('ws', 't1'));
    expect(paths(result.current.tabs)).toEqual(['a.md']);
    expect(activePath(result.current)).toBe('a.md');
  });
});

describe('useFileTabs history', () => {
  const step = (result: { current: ReturnType<typeof useFileTabs> }, dir: 'back' | 'forward') =>
    act(() => result.current.stepHistory(result.current.activeId, dir));
  const can = (result: { current: ReturnType<typeof useFileTabs> }, dir: 'back' | 'forward') =>
    result.current.historyTarget(result.current.activeId, dir) !== null;
  /** Steps back until there is nowhere left, naming what each press showed. */
  const backWalk = (result: { current: ReturnType<typeof useFileTabs> }) => {
    const seen: string[] = [];
    while (can(result, 'back')) {
      step(result, 'back');
      const tab = result.current.activeTab;
      seen.push(tab.kind === 'chart' ? tab.symbol : tab.kind);
    }
    return seen;
  };

  it('steps a tab back through what the tree put in it, and forward again', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.openFile('b.md'));

    step(result, 'back');
    expect(paths(result.current.tabs)).toEqual(['a.md']);
    step(result, 'back');
    expect(result.current.activeTab.kind).toBe('empty');
    expect(can(result, 'back')).toBe(false);

    step(result, 'forward');
    step(result, 'forward');
    expect(paths(result.current.tabs)).toEqual(['b.md']);
    expect(can(result, 'forward')).toBe(false);
  });

  it('keeps each tab to its own history, with switching tabs no step at all', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.openChart({ symbol: 'SPY' }));
    act(() => result.current.activate(result.current.tabs[0].id));

    // The pinned file filled the empty tab; the chart opened beside it.
    expect(can(result, 'back')).toBe(true);
    act(() => result.current.activate(result.current.tabs[1].id));
    expect(can(result, 'back')).toBe(false);
  });

  it('drops what lay ahead once the tab goes somewhere new', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.openFile('b.md'));
    step(result, 'back');
    act(() => result.current.openFile('c.md'));

    expect(can(result, 'forward')).toBe(false);
    step(result, 'back');
    expect(paths(result.current.tabs)).toEqual(['a.md']);
  });

  // The panel keeps an editor's draft per tab id, so a step must not hand the
  // old file's draft to the new one.
  it('brings the tab back under a new id', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.openFile('b.md'));
    const before = result.current.activeId;
    step(result, 'back');

    expect(result.current.activeId).not.toBe(before);
    expect(can(result, 'forward')).toBe(true);
  });

  it('passes over a step another tab now shows, rather than opening the file twice', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.openFile('b.md'));
    act(() => result.current.openFile('a.md', { pin: true }));
    act(() => result.current.activate(result.current.tabs.find((t) => t.kind === 'file' && t.path === 'b.md')!.id));

    step(result, 'back');
    expect(result.current.activeTab.kind).toBe('empty');
    expect(paths(result.current.tabs).filter((p) => p === 'a.md')).toHaveLength(1);
  });

  it('counts a chart moved to another ticker as a step', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'SPY' }));
    act(() => result.current.retargetChart(result.current.activeId, 'AAPL'));

    step(result, 'back');
    expect(result.current.activeTab).toMatchObject({ kind: 'chart', symbol: 'SPY' });
  });

  it('remembers the ticker a step lands on for the next chart opened blind', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'AMD' }));
    act(() => result.current.retargetChart(result.current.activeId, 'NVDA'));
    step(result, 'back');
    expect(lastChartSymbol('ws')).toBe('AMD');
  });

  // The reader moved the tab, so where it had been is still theirs to go back to.
  it('carries a chart tab\'s history into the tab it folds into', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'MSFT' }));
    act(() => result.current.newTab());
    act(() => result.current.openChart({ symbol: 'SPY' }));
    act(() => result.current.retargetChart(result.current.activeId, 'AAPL'));
    act(() => result.current.retargetChart(result.current.activeId, 'MSFT'));

    expect(result.current.tabs.filter((t) => t.kind === 'chart')).toHaveLength(1);
    // Both tabs began on an empty tab, so the two journeys meet on that step,
    // and it is taken once: every Back press changes what the tab shows.
    expect(backWalk(result)).toEqual(['AAPL', 'SPY', 'empty']);
  });

  it('keeps the history of the tab a chart folds into', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openChart({ symbol: 'TSLA' }));
    act(() => result.current.retargetChart(result.current.activeId, 'MSFT'));
    act(() => result.current.newTab());
    act(() => result.current.openChart({ symbol: 'SPY' }));
    act(() => result.current.retargetChart(result.current.activeId, 'MSFT'));

    expect(backWalk(result)).toEqual(['SPY', 'empty', 'TSLA', 'empty']);
  });

  it('has no Back onto the file it shows once the step between two visits is deleted', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.openFile('b.md'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.forgetPaths(['b.md']));

    expect(paths(result.current.tabs)).toEqual(['a.md']);
    expect(backWalk(result)).toEqual(['empty']);
  });

  it('forgets the steps that name a deleted file', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.openFile('b.md'));
    act(() => result.current.openFile('c.md'));
    act(() => result.current.forgetPaths(['b.md']));

    step(result, 'back');
    expect(paths(result.current.tabs)).toEqual(['a.md']);
  });

  it('forgets a tab\'s history with the tab', () => {
    const { result } = renderHook(() => useFileTabs('ws'));
    act(() => result.current.openFile('a.md'));
    act(() => result.current.openFile('b.md'));
    const id = result.current.activeId;
    act(() => result.current.newTab());
    act(() => result.current.closeTab(id));

    expect(result.current.historyTarget(id, 'back')).toBeNull();
  });
});
