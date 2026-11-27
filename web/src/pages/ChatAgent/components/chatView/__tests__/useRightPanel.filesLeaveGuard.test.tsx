import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';
import type { ToolCallProcessRecord } from '../../ToolCallDetailView';
import type { RouteLeaveGuard } from '../../../contexts/RouteLeaveGuardContext';

// The Files panel's drafts live in its mount, and closing the panel unmounts
// it. Those exits leave through the guard the panel hands up, and a declined
// ask leaves the panel where it is. A tool result is a tab of the same
// panel, so opening one never asks.

const setFilePanelWorkspaceId = vi.fn();

function open(panel: { filePanelWorkspaceId?: string | null; isMobile?: boolean } = {}) {
  const { result } = renderHook(() => useRightPanel({
    isMobile: false,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: null },
    setFilePanelWorkspaceId,
    ...panel,
    messages: [{ id: 'm1', toolCallProcesses: { tc1: TOOL_CALL } }],
  }), { wrapper: ({ children }) => <MemoryRouter>{children}</MemoryRouter> });
  act(() => result.current.handleToggleFilePanel());
  expect(result.current.rightPanelType).toBe('file');
  act(() => result.current.handleFilesLeaveGuardChange(guard));
  return result;
}

const TOOL_CALL = { toolCall: { id: 'tc1' }, toolCallResult: { artifact: null } } as unknown as ToolCallProcessRecord;

// The panel's guard, answered the way the reader answers its dialog.
const guard = vi.fn<RouteLeaveGuard>();
const answer = (leave: boolean) => guard.mockImplementation((go) => { if (leave) go(); });
beforeEach(() => {
  guard.mockReset();
  setFilePanelWorkspaceId.mockReset();
});

describe('leaving the Files panel with a draft open', () => {
  it('opens a tool-call detail as a Files tab without asking', () => {
    answer(false);
    const result = open();
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(guard).not.toHaveBeenCalled();
    expect(result.current.rightPanelType).toBe('file');
    expect(result.current.panelTarget).toMatchObject({ kind: 'tool', toolCallId: 'tc1' });
  });

  it('holds the panel on a declined Workspace toggle', () => {
    answer(false);
    const result = open();
    act(() => result.current.handleToggleFilePanel());
    expect(result.current.rightPanelType).toBe('file');
  });

  it('leaves once the ask is accepted', () => {
    answer(true);
    const result = open();
    act(() => result.current.handleToggleFilePanel());
    expect(guard).toHaveBeenCalledTimes(1);
    expect(result.current.rightPanelType).toBeNull();
  });

  it('leaves without asking once the panel is gone', () => {
    answer(false);
    const result = open();
    act(() => result.current.handleFilesLeaveGuardChange(null));
    act(() => result.current.handleToggleFilePanel());
    expect(guard).not.toHaveBeenCalled();
    expect(result.current.rightPanelType).toBeNull();
  });
});

// A chat link asks only when it would change the workspace the panel shows.
describe('opening a chat link with a draft open', () => {
  const OTHER_WS = '0f1e2d3c-4b5a-4968-8776-655443322110';
  const MEMO = '.agents/user/memo/my-report.pdf';

  it('opens a memo without asking', () => {
    answer(false);
    const result = open();
    act(() => result.current.handleOpenFileFromChat(MEMO));
    expect(guard).not.toHaveBeenCalled();
    expect(result.current.panelTarget).toMatchObject({ kind: 'memo', key: 'my-report.pdf' });
  });

  it('opens a file in the sibling the panel already shows without asking', () => {
    answer(false);
    const result = open({ filePanelWorkspaceId: OTHER_WS });
    act(() => result.current.handleOpenFileFromChat('results/a.md', OTHER_WS));
    expect(guard).not.toHaveBeenCalled();
    expect(result.current.panelTarget).toMatchObject({ kind: 'file', path: 'results/a.md' });
  });

  it("asks before switching to a sibling's files, and holds on a no", () => {
    answer(false);
    const result = open();
    act(() => result.current.handleOpenFileFromChat('results/a.md', OTHER_WS));
    expect(guard).toHaveBeenCalledTimes(1);
    expect(setFilePanelWorkspaceId).not.toHaveBeenCalledWith(OTHER_WS);
  });
});

// On mobile the back gesture closes the panel by popping the sentinel the
// panel pushed, which is gone by the time the handler asks.
describe('the mobile back gesture with a draft open', () => {
  let pushState: ReturnType<typeof vi.spyOn>;
  let back: ReturnType<typeof vi.spyOn>;
  beforeEach(() => {
    pushState = vi.spyOn(window.history, 'pushState').mockImplementation(() => {});
    back = vi.spyOn(window.history, 'back').mockImplementation(() => {});
  });
  afterEach(() => {
    pushState.mockRestore();
    back.mockRestore();
  });
  const goBack = () => act(() => { window.dispatchEvent(new PopStateEvent('popstate')); });

  it('holds the panel on a no and puts the sentinel back', () => {
    answer(false);
    const result = open({ isMobile: true });
    expect(pushState).toHaveBeenCalledTimes(1);
    goBack();
    expect(guard).toHaveBeenCalledTimes(1);
    expect(result.current.rightPanelType).toBe('file');
    expect(pushState).toHaveBeenCalledTimes(2);
    expect(pushState.mock.lastCall?.[0]).toMatchObject({ _panelSentinel: true });
    // The sentinel is armed again, so the next back asks again.
    goBack();
    expect(guard).toHaveBeenCalledTimes(2);
    expect(result.current.rightPanelType).toBe('file');
  });

  it('closes once the ask is accepted', () => {
    answer(true);
    const result = open({ isMobile: true });
    goBack();
    expect(guard).toHaveBeenCalledTimes(1);
    expect(result.current.rightPanelType).toBeNull();
    expect(pushState).toHaveBeenCalledTimes(1);
    expect(back).not.toHaveBeenCalled();
  });

  it('pops the restored sentinel when a held ask is accepted later', () => {
    let leave: (() => void) | null = null;
    guard.mockImplementation((go) => { leave = go; });
    const result = open({ isMobile: true });
    goBack();
    expect(result.current.rightPanelType).toBe('file');
    expect(pushState).toHaveBeenCalledTimes(2);
    act(() => leave?.());
    expect(result.current.rightPanelType).toBeNull();
    expect(back).toHaveBeenCalledTimes(1);
  });
});
