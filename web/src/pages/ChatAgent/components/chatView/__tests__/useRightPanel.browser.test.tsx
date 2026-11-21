/**
 * The agent's browser activity and a link opened in the computer's browser reach
 * the chat's own view directly. Neither goes through the panel's one ask slot
 * past the thread's first use, so neither drops what the reader asked the panel
 * for, and neither reaches a sibling's browser the panel happens to show.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import type { Computer, ShareLink } from '@/types/api';
import { MEMORY_WORKSPACE_DIR } from '../../../utils/agentPaths';
import type { BrowserUse } from '../../../session/stream/browserUse';
import { useRightPanel } from '../useRightPanel';

const { getShareLink } = vi.hoisted(() => ({ getShareLink: vi.fn() }));
vi.mock('../../../utils/api/shareLinks', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getShareLink,
}));
vi.mock('@/lib/shareLinkOpen', () => ({ opensShareLinksInPanel: () => true }));
vi.mock('@/lib/desktop', () => ({ desktop: { openExternal: vi.fn() }, isDesktopShell: () => true }));

const SIBLING = '7c9e6679-7425-40de-944b-e07fc1f90ae7';
const docker = { computer_id: 'c1', kind: 'docker' } as Computer;
const ssh = { computer_id: 'c2', kind: 'ssh' } as Computer;
const setFilePanelWorkspaceId = vi.fn();

function open({
  filePanelWorkspaceId = null as string | null,
  computer = docker as Computer | null,
  shared = false,
  isMobile = false,
  browserOn = true,
} = {}) {
  const { result } = renderHook(() => useRightPanel({
    isMobile,
    workspaceId: 'ws',
    threadId: 't1',
    isActive: true,
    containerRef: { current: null },
    setFilePanelWorkspaceId,
    filePanelWorkspaceId,
    messages: [],
    computer,
    shared,
    browserOn,
  }), { wrapper: ({ children }) => <MemoryRouter>{children}</MemoryRouter> });
  setFilePanelWorkspaceId.mockClear();
  return result;
}

let callSeq = 0;
const use = (command = 'agent-browser snapshot', done = false): BrowserUse =>
  ({ id: `b${++callSeq}`, command, done, running: done ? null : command });

beforeEach(() => vi.clearAllMocks());

describe('the agent using the browser', () => {
  it('captions the chat\'s own view, and asks the panel only on the thread\'s first use', () => {
    const result = open();
    act(() => result.current.handleToggleFilePanel());
    act(() => result.current.handleBrowserUse(use('agent-browser open example.com')));
    expect(result.current.panelTarget).toMatchObject({ kind: 'browser', front: false });
    const asked = result.current.panelTarget;
    act(() => result.current.handleTargetHandled(asked?.seq));

    act(() => result.current.handleBrowserUse(use()));
    expect(result.current.panelTarget).toBeNull();
    expect(result.current.browserViewFor('ws').getSnapshot().agent).toEqual({ key: 'reading', detail: null });
  });

  it('brings no tab up for a reader who has not turned the browser on', () => {
    const result = open({ browserOn: false });
    act(() => result.current.handleBrowserUse(use()));
    expect(result.current.rightPanelType).not.toBe('file');
    expect(result.current.panelTarget).toBeNull();
  });

  it('opens a closed panel on the tab, in front, for the first use', () => {
    const result = open();
    act(() => result.current.handleBrowserUse(use()));
    expect(result.current.rightPanelType).toBe('file');
    expect(result.current.panelTarget).toMatchObject({ kind: 'browser', front: true });
  });

  it('leaves a memory entry the reader clicked waiting for its list', () => {
    const result = open();
    act(() => result.current.handleBrowserUse(use()));
    act(() => result.current.handleTargetHandled(result.current.panelTarget?.seq));
    act(() => result.current.handleOpenFileFromChat(`/home/workspace/${MEMORY_WORKSPACE_DIR}/risk.md`));
    expect(result.current.panelTarget).toMatchObject({ kind: 'memory', key: 'risk.md' });

    act(() => {
      const call = use();
      result.current.handleBrowserUse(call);
      result.current.handleBrowserUse({ ...call, done: true, running: null });
    });
    expect(result.current.panelTarget).toMatchObject({ kind: 'memory', key: 'risk.md' });
  });

  it('does not supersede a share link still being looked up', async () => {
    let answer!: (link: ShareLink) => void;
    getShareLink.mockReturnValueOnce(new Promise((resolve) => { answer = resolve; }));
    const result = open();
    act(() => result.current.handleBrowserUse(use()));
    await act(async () => { result.current.handleOpenShareLink(`${window.location.origin}/a/code1`); });
    act(() => result.current.handleBrowserUse(use()));
    await act(async () => { answer({ code: 'code1', workspace_id: 'ws', kind: 'file', path: 'report.md' } as unknown as ShareLink); });

    expect(result.current.panelTarget).toMatchObject({ kind: 'file', path: 'report.md' });
  });

  it('brings no tab up over a sibling\'s files, though the chat\'s view still hears it', () => {
    const result = open({ filePanelWorkspaceId: SIBLING });
    act(() => result.current.handleBrowserUse(use()));
    expect(result.current.panelTarget).toBeNull();
    expect(result.current.browserViewFor('ws').getSnapshot().agent).not.toBeNull();
    expect(result.current.browserViewFor(SIBLING).getSnapshot().agent).toBeNull();
  });

  it('has a sibling\'s view look again, since it may be on the same computer\'s Chrome', () => {
    const result = open({ filePanelWorkspaceId: SIBLING });
    const poke = vi.spyOn(result.current.browserViewFor(SIBLING), 'poke');
    act(() => result.current.handleBrowserUse(use()));
    expect(poke).toHaveBeenCalled();
  });
});

describe('a link opened in the agent browser', () => {
  it('goes to the chat\'s own view, never to the sibling the panel shows', () => {
    const result = open({ filePanelWorkspaceId: SIBLING });
    const own = vi.spyOn(result.current.browserViewFor('ws'), 'visit').mockResolvedValue(null);
    const sibling = vi.spyOn(result.current.browserViewFor(SIBLING), 'visit');
    act(() => result.current.linkActions.openInBrowser?.('https://example.com/'));

    expect(own).toHaveBeenCalledWith('https://example.com/');
    expect(sibling).not.toHaveBeenCalled();
    expect(setFilePanelWorkspaceId).toHaveBeenCalledWith(null);
    expect(result.current.panelTarget).toMatchObject({ kind: 'browser', front: true });
  });

  it('is offered only on a desktop chat whose computer has a browser', () => {
    expect(open().current.linkActions.openInBrowser).toBeTypeOf('function');
    expect(open({ computer: ssh }).current.linkActions.openInBrowser).toBeNull();
    expect(open({ computer: null }).current.linkActions.openInBrowser).toBeNull();
    expect(open({ isMobile: true }).current.linkActions.openInBrowser).toBeNull();
    expect(open({ browserOn: false }).current.linkActions.openInBrowser).toBeNull();
  });

  it('is offered to a member of a shared workspace, whose computer always has one', () => {
    expect(open({ computer: null, shared: true }).current.linkActions.openInBrowser).toBeTypeOf('function');
  });

  it('keeps one view per workspace for the life of the chat', () => {
    const result = open();
    expect(result.current.browserViewFor('ws')).toBe(result.current.browserViewFor('ws'));
    expect(result.current.browserViewFor('ws')).not.toBe(result.current.browserViewFor(SIBLING));
  });
});
