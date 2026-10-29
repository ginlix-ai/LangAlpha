import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';
import type { ShareLink } from '@/types/api';

const { getShareLink, getPreviewUrl, openExternal } = vi.hoisted(() => ({ getShareLink: vi.fn(), getPreviewUrl: vi.fn(), openExternal: vi.fn() }));
vi.mock('../../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getPreviewUrl,
}));
vi.mock('../../../utils/api/shareLinks', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getShareLink,
}));
vi.mock('@/lib/shareLinkOpen', () => ({ opensShareLinksInPanel: () => true }));
vi.mock('@/lib/desktop', () => ({ desktop: { openExternal }, isDesktopShell: () => true }));

const setFilePanelWorkspaceId = vi.fn();

function open(filePanelWorkspaceId: string | null = null, mobile = false) {
  const { result, rerender, unmount } = renderHook(({ isActive, workspaceId, isMobile }) => useRightPanel({
    isMobile,
    workspaceId,
    isActive,
    containerRef: { current: null },
    setFilePanelWorkspaceId,
    filePanelWorkspaceId,
    messages: [],
  }), {
    initialProps: { isActive: true, workspaceId: 'ws', isMobile: mobile },
    wrapper: ({ children }) => <MemoryRouter>{children}</MemoryRouter>,
  });
  // Mounting clears the override once; only what a click does is asserted.
  setFilePanelWorkspaceId.mockClear();
  return {
    get current() { return result.current; },
    leave: () => rerender({ isActive: false, workspaceId: 'ws', isMobile: mobile }),
    switchWorkspace: () => rerender({ isActive: true, workspaceId: 'ws2', isMobile: mobile }),
    widen: () => rerender({ isActive: true, workspaceId: 'ws', isMobile: false }),
    unmount,
  };
}

const SIBLING = '7c9e6679-7425-40de-944b-e07fc1f90ae7';

const app = (port: number, path: string | null = null, workspace_id = 'ws') =>
  ({ code: `code${port}`, workspace_id, kind: 'app', port, path, title: 'Watchlist' }) as unknown as ShareLink;

const href = (code: string, path?: string) =>
  `${window.location.origin}/a/${code}${path === undefined ? '' : `?path=${path}`}`;

beforeEach(() => vi.clearAllMocks());

describe('an /a/ link opened in the panel', () => {
  // No path keeps the page a tab already on the port shows, so the root is named.
  it('opens an explicit root at the root, not the entry', async () => {
    getShareLink.mockResolvedValue(app(8080, 'docs/index.html'));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });

    expect(result.current.panelTarget).toMatchObject({ kind: 'preview', port: 8080, path: '/' });
  });

  it('opens the entry when the link names no page', async () => {
    getShareLink.mockResolvedValue(app(8080, 'docs/index.html'));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('code8080')); });

    expect(result.current.panelTarget).toMatchObject({ port: 8080, path: '/docs/index.html' });
  });

  it('lets the latest click win over a slower earlier lookup', async () => {
    let answerFirst!: (link: ShareLink) => void;
    getShareLink
      .mockReturnValueOnce(new Promise((resolve) => { answerFirst = resolve; }))
      .mockResolvedValueOnce(app(9090));
    const result = open();
    await act(async () => {
      result.current.handleOpenShareLink(href('code8080', '/'));
      result.current.handleOpenShareLink(href('code9090', '/'));
    });
    await act(async () => { answerFirst(app(8080)); });

    expect(result.current.panelTarget).toMatchObject({ port: 9090 });
  });

  it('gives way to a panel open made while the lookup runs', async () => {
    let answer!: (link: ShareLink) => void;
    getShareLink.mockReturnValueOnce(new Promise((resolve) => { answer = resolve; }));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });
    act(() => result.current.handleOpenSourcesFromChat('m1'));
    await act(async () => { answer(app(8080)); });

    expect(result.current.panelTarget).toMatchObject({ kind: 'sources', messageId: 'm1' });
  });

  it('drops a lookup that settles after its thread was left', async () => {
    let fail!: (err: Error) => void;
    getShareLink.mockReturnValueOnce(new Promise((_, reject) => { fail = reject; }));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });
    act(() => result.leave());
    await act(async () => { fail(new Error('404')); });

    expect(openExternal).not.toHaveBeenCalled();
  });

  it('drops a lookup that settles after its view unmounted', async () => {
    let fail!: (err: Error) => void;
    getShareLink.mockReturnValueOnce(new Promise((_, reject) => { fail = reject; }));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });
    result.unmount();
    await act(async () => { fail(new Error('404')); });

    expect(openExternal).not.toHaveBeenCalled();
  });

  it('drops a lookup that settles after its view moved to another workspace', async () => {
    let fail!: (err: Error) => void;
    getShareLink.mockReturnValueOnce(new Promise((_, reject) => { fail = reject; }));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });
    act(() => result.switchWorkspace());
    await act(async () => { fail(new Error('404')); });

    expect(openExternal).not.toHaveBeenCalled();
  });

  // A tab picked, a sheet opened or the panel closed reach it without an ask.
  it('drops a lookup that settles after a click elsewhere', async () => {
    let answer!: (link: ShareLink) => void;
    getShareLink.mockReturnValueOnce(new Promise((resolve) => { answer = resolve; }));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });
    act(() => { document.body.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true })); });
    await act(async () => { answer(app(8080)); });

    expect(result.current.panelTarget).toBeNull();
  });

  it('drops a lookup that fails after a key press', async () => {
    let fail!: (err: Error) => void;
    getShareLink.mockReturnValueOnce(new Promise((_, reject) => { fail = reject; }));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });
    act(() => { document.body.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })); });
    await act(async () => { fail(new Error('404')); });

    expect(openExternal).not.toHaveBeenCalled();
  });

  it('keeps a lookup through a modifier key alone', async () => {
    let answer!: (link: ShareLink) => void;
    getShareLink.mockReturnValueOnce(new Promise((resolve) => { answer = resolve; }));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });
    act(() => { document.body.dispatchEvent(new KeyboardEvent('keydown', { key: 'Meta', bubbles: true })); });
    await act(async () => { answer(app(8080)); });

    expect(result.current.panelTarget).toMatchObject({ kind: 'preview', port: 8080 });
  });

  it('gives way to an app the stream opens on the mobile sheet', async () => {
    let answer!: (link: ShareLink) => void;
    getShareLink.mockReturnValueOnce(new Promise((resolve) => { answer = resolve; }));
    const result = open(null, true);
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });
    act(() => result.current.handleOpenPreview({ url: 'https://app.test/', port: 3000, loading: false }));
    await act(async () => { answer(app(8080)); });

    expect(result.current.previewData).toMatchObject({ port: 3000 });
  });

  // A report-back carries an analyst's links into Home.
  it("opens a sibling's file in the panel showing that sibling", async () => {
    getShareLink.mockResolvedValue({ code: 'f1', workspace_id: SIBLING, kind: 'file', path: 'results/report.md', port: null } as unknown as ShareLink);
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('f1')); });

    expect(setFilePanelWorkspaceId).toHaveBeenCalledWith(SIBLING);
    expect(result.current.panelTarget).toMatchObject({ kind: 'file', path: 'results/report.md' });
  });

  it("opens a sibling's app in the panel showing that sibling", async () => {
    getShareLink.mockResolvedValue(app(8080, null, SIBLING));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });

    expect(setFilePanelWorkspaceId).toHaveBeenCalledWith(SIBLING);
    expect(result.current.panelTarget).toMatchObject({ kind: 'preview', port: 8080 });
  });

  it("asks the sibling for its app's URL on the mobile sheet", async () => {
    getShareLink.mockResolvedValue(app(8080, null, SIBLING));
    getPreviewUrl.mockResolvedValue({ url: 'https://app.test/' });
    const result = open(null, true);
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });

    expect(getPreviewUrl).toHaveBeenCalledWith(SIBLING, 8080, undefined);
    expect(result.current.previewData).toMatchObject({ port: 8080, workspaceId: SIBLING, url: 'https://app.test/' });
  });

  it("keeps a sibling's app in its workspace when the sheet widens into the panel", async () => {
    getShareLink.mockResolvedValue(app(8080, null, SIBLING));
    getPreviewUrl.mockResolvedValue({ url: 'https://app.test/' });
    const result = open(null, true);
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });
    setFilePanelWorkspaceId.mockClear();
    act(() => result.widen());

    expect(setFilePanelWorkspaceId).toHaveBeenCalledWith(SIBLING);
    expect(result.current.panelTarget).toMatchObject({ kind: 'preview', port: 8080 });
  });

  // The panel mints an app's URL for the workspace it shows.
  it('returns a panel showing a sibling to this workspace before opening the app', async () => {
    getShareLink.mockResolvedValue(app(8080));
    const result = open('sibling');
    await act(async () => { result.current.handleOpenShareLink(href('code8080', '/')); });

    expect(setFilePanelWorkspaceId).toHaveBeenCalledWith(null);
    expect(result.current.panelTarget).toMatchObject({ kind: 'preview', port: 8080 });
  });

  it('hands the shell an absolute URL when a relative link does not resolve', async () => {
    getShareLink.mockRejectedValue(new Error('404'));
    const result = open();
    await act(async () => { result.current.handleOpenShareLink('/a/code8080?path=/'); });

    expect(openExternal).toHaveBeenCalledWith(`${window.location.origin}/a/code8080?path=/`);
  });
});
