import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

const shared = vi.hoisted(() => ({ asked: [] as (string | null)[] }));
vi.mock('@/hooks/useWorkspaceFileGrant', () => ({
  useWorkspaceFileGrant: (workspaceId: string | null) => {
    shared.asked.push(workspaceId);
    return { data: null, error: null, refetch: vi.fn() };
  },
}));
vi.mock('@/hooks/useOwnFileGrant', () => ({
  useOwnFileGrant: (workspaceId: string | null) => ({
    data: workspaceId ? { prefix: '/api/v1/wsfiles/g/own-1/', expires_in: 12 * 60 * 60 } : undefined,
    error: null,
    refetch: vi.fn(),
  }),
}));
vi.mock('../../../../contexts/WorkspaceContext', () => ({ useWorkspaceFolders: () => null }));

import { runAuthResets } from '@/lib/authResets';
import { usePageLoads } from '@/lib/pageLoads';
import { useFrameLinks, useLiveFrame } from '../liveFrame';
import type { PageBridge, PageBridgeListener } from '../usePageBridge';

describe('useFrameLinks', () => {
  function frameLinks(isActive: boolean) {
    Object.defineProperty(navigator, 'userActivation', { configurable: true, value: { isActive } });
    let listener: PageBridgeListener | null = null;
    const bridge = {
      subscribe: (next: PageBridgeListener) => {
        listener = next;
        return () => { listener = null; };
      },
    } as unknown as PageBridge;
    const onOpen = vi.fn();
    const view = renderHook(() => useFrameLinks(bridge, onOpen));
    return { onOpen, view, say: (message: Parameters<PageBridgeListener>[0]) => listener?.(message, bridge), listening: () => listener !== null };
  }

  /** A click inside a frame leaves the frame as the app's focused element. */
  function focusFrame() {
    const frame = document.body.appendChild(document.createElement('iframe'));
    frame.focus();
  }

  afterEach(() => {
    Reflect.deleteProperty(navigator, 'userActivation');
    document.body.replaceChildren();
  });

  it('opens a linked file on the heels of a click', () => {
    focusFrame();
    const { onOpen, say } = frameLinks(true);
    say({ type: 'page:open-file', path: 'other/report.html', fragment: 'q3' });
    expect(onOpen).toHaveBeenCalledExactlyOnceWith('other/report.html', 'q3');
  });

  it('opens nothing a page asks for with no click behind it', () => {
    focusFrame();
    const { onOpen, say } = frameLinks(false);
    say({ type: 'page:open-file', path: 'other/report.html', fragment: '' });
    expect(onOpen).not.toHaveBeenCalled();
  });

  it('takes a click on the app for no click on the page', () => {
    const button = document.body.appendChild(document.createElement('button'));
    button.focus();
    const { onOpen, say } = frameLinks(true);
    say({ type: 'page:open-file', path: 'other/report.html', fragment: '' });
    expect(onOpen).not.toHaveBeenCalled();
  });

  it('hears nothing else, and stops with the viewer', () => {
    focusFrame();
    const { onOpen, say, view, listening } = frameLinks(true);
    say({ type: 'page:settled' });
    expect(onOpen).not.toHaveBeenCalled();
    view.unmount();
    expect(listening()).toBe(false);
  });
});

describe('useLiveFrame', () => {
  // The publish review's hidden load: under the shared grant, a file the agent
  // changed could come from the browser's cache of another frame's load.
  it('loads under a grant of its own when asked, and never asks for the shared one', () => {
    shared.asked = [];
    const { result } = renderHook(() => useLiveFrame({ workspaceId: 'ws', filePath: 'a.html', inject: 'view', ownGrant: true }));
    expect(result.current.source?.src).toContain('/api/v1/wsfiles/g/own-1/a.html');
    expect(shared.asked.every((asked) => asked === null)).toBe(true);
  });

  // A tab may hold the sandbox's spelling of a path, and the review reads the
  // record's. A report filed under the tab's never met the dialog.
  it('files a report where the review reads it, whatever the tab calls the file', () => {
    const listeners = new Set<PageBridgeListener>();
    const bridge = {
      ready: true,
      path: 'reports/index.html',
      subscribe: (next: PageBridgeListener) => {
        listeners.add(next);
        return () => { listeners.delete(next); };
      },
    } as unknown as PageBridge;
    const { result } = renderHook(() => useLiveFrame({
      workspaceId: 'ws', filePath: '/home/workspace/reports/index.html', inject: 'comments',
    }));
    act(() => result.current.onBridge(bridge));

    const review = renderHook(() => usePageLoads('ws', 'reports/index.html'));
    expect(review.result.current).toEqual({ now: [], earlier: [] });
    act(() => listeners.forEach((heard) => heard({ type: 'page:loads', path: 'reports/index.html', paths: ['data/a.csv'] }, bridge)));
    expect(review.result.current).toEqual({ now: ['data/a.csv'], earlier: [] });

    // An account's reports do not outlast it.
    act(() => runAuthResets());
    expect(review.result.current).toEqual({ now: [], earlier: [] });
  });

  it("takes the review's own load for the page while it is open", () => {
    const bridged = () => {
      const listeners = new Set<PageBridgeListener>();
      const bridge = {
        ready: true,
        path: 'reports/index.html',
        subscribe: (next: PageBridgeListener) => {
          listeners.add(next);
          return () => { listeners.delete(next); };
        },
      } as unknown as PageBridge;
      const say = (paths: string[]) =>
        listeners.forEach((heard) => heard({ type: 'page:loads', path: 'reports/index.html', paths }, bridge));
      return { bridge, say };
    };
    const [panel, scout] = [bridged(), bridged()];
    const panelFrame = renderHook(() => useLiveFrame({ workspaceId: 'ws', filePath: 'reports/index.html', inject: 'view' }));
    const scoutFrame = renderHook(() => useLiveFrame({
      workspaceId: 'ws', filePath: 'reports/index.html', inject: 'view', ownGrant: true, scout: true,
    }));
    act(() => panelFrame.result.current.onBridge(panel.bridge));
    act(() => scoutFrame.result.current.onBridge(scout.bridge));
    const review = renderHook(() => usePageLoads('ws', 'reports/index.html'));
    act(() => panel.say(['data/old.csv']));
    act(() => scout.say(['data/new.csv']));
    expect(review.result.current).toEqual({ now: ['data/new.csv'], earlier: ['data/old.csv'] });
    act(() => runAuthResets());
  });

  it('holds a first fragment with its file, as the viewer stays mounted from tab to tab', () => {
    const { result, rerender } = renderHook(
      ({ filePath, anchor }: { filePath: string; anchor: string | null }) =>
        useLiveFrame({ workspaceId: 'w', filePath, prefix: '/serve/', inject: 'comments', revision: 'r', anchor }),
      { initialProps: { filePath: 'a.html', anchor: 'revenue' as string | null } },
    );
    expect(result.current.source?.src).toMatch(/a\.html.*#revenue$/);

    rerender({ filePath: 'b.html', anchor: null });
    expect(result.current.source?.src).toMatch(/b\.html/);
    expect(result.current.source?.src).not.toContain('#');

    // A later ask in the same file goes to the document, not the address.
    rerender({ filePath: 'b.html', anchor: 'margin' });
    expect(result.current.source?.src).not.toContain('#');

    rerender({ filePath: 'c.html', anchor: 'margin' });
    expect(result.current.source?.src).toMatch(/c\.html.*#margin$/);
  });

  it('takes the same path in another workspace for another file', () => {
    const { result, rerender } = renderHook(
      ({ workspaceId, anchor }: { workspaceId: string; anchor: string | null }) =>
        useLiveFrame({ workspaceId, filePath: 'report.html', prefix: `/serve/${workspaceId}/`, inject: 'view', anchor }),
      { initialProps: { workspaceId: 'a', anchor: 'revenue' as string | null } },
    );
    const before = result.current.source;
    expect(before?.src).toContain('#revenue');

    rerender({ workspaceId: 'b', anchor: null });
    // Under one key the frame would read the new address as a renewed grant and keep A's document.
    expect(result.current.source?.key).not.toBe(before?.key);
    expect(result.current.source?.src).not.toContain('#');
  });
});
