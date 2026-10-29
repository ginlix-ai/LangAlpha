import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { nextPanelOverride, useRightPanel } from '../useRightPanel';
import type { RouteLeaveGuard } from '../../../contexts/RouteLeaveGuardContext';

// Home's panel shows its own files. A `__wsref__` link names a sibling, which
// the panel shows in their place; anything that names no other workspace
// brings it back to Home's.

const HOME = '11111111-1111-4111-8111-111111111111';
const SIBLING = '22222222-2222-4222-8222-222222222222';

describe('nextPanelOverride', () => {
  const plain = { clearWorkspaceId: false, setWorkspaceId: null };
  const sibling = { clearWorkspaceId: false, setWorkspaceId: SIBLING };

  it("returns Home's panel to its own files on a plain path", () => {
    expect(nextPanelOverride(plain, HOME)).toBeNull();
  });

  it("shows a sibling's files from Home", () => {
    expect(nextPanelOverride(sibling, HOME)).toBe(SIBLING);
  });

  it('reads a link naming Home itself as its own files', () => {
    expect(nextPanelOverride({ clearWorkspaceId: false, setWorkspaceId: HOME }, HOME)).toBeNull();
  });

  it('clears for a user-scoped target', () => {
    expect(nextPanelOverride({ clearWorkspaceId: true, setWorkspaceId: null }, HOME)).toBeNull();
  });
});

const setFilePanelWorkspaceId = vi.fn();
const guard = vi.fn<RouteLeaveGuard>((go) => go());

function home(filePanelWorkspaceId: string | null) {
  const { result } = renderHook(() => useRightPanel({
    isMobile: false,
    workspaceId: HOME,
    isActive: true,
    containerRef: { current: null },
    setFilePanelWorkspaceId,
    filePanelWorkspaceId,
    messages: [],
  }), { wrapper: ({ children }) => <MemoryRouter>{children}</MemoryRouter> });
  // The mount clears the override as any change of workspace does.
  setFilePanelWorkspaceId.mockReset();
  return result;
}

beforeEach(() => {
  setFilePanelWorkspaceId.mockReset();
  guard.mockClear();
});

describe("Home's file panel", () => {
  it("asks before a plain path leaves a sibling's files for Home's", () => {
    const result = home(SIBLING);
    act(() => result.current.handleToggleFilePanel());
    setFilePanelWorkspaceId.mockReset();
    act(() => result.current.handleFilesLeaveGuardChange(guard));
    act(() => result.current.handleOpenFileFromChat('notes.md'));
    expect(guard).toHaveBeenCalledTimes(1);
    expect(setFilePanelWorkspaceId).toHaveBeenCalledWith(null);
    expect(result.current.panelTarget).toMatchObject({ kind: 'file', path: 'notes.md' });
  });

  it('opens a plain path among its own files without asking', () => {
    const result = home(null);
    act(() => result.current.handleToggleFilePanel());
    act(() => result.current.handleFilesLeaveGuardChange(guard));
    act(() => result.current.handleOpenFileFromChat('notes.md'));
    expect(guard).not.toHaveBeenCalled();
    expect(setFilePanelWorkspaceId).not.toHaveBeenCalled();
  });

  it("opens on Home's own files from the header toggle", () => {
    const result = home(SIBLING);
    act(() => result.current.handleToggleFilePanel());
    expect(setFilePanelWorkspaceId).toHaveBeenCalledWith(null);
    expect(result.current.rightPanelType).toBe('file');
  });
});
