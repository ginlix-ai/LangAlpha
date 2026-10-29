import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';

vi.mock('../../../utils/api', async (importOriginal) => ({ ...(await importOriginal<Record<string, unknown>>()), getPreviewUrl: vi.fn() }));

function open() {
  const { result } = renderHook(() => useRightPanel({
    isMobile: false,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: null },
    setFilePanelWorkspaceId: vi.fn(),
    messages: [],
  }), { wrapper: ({ children }) => <MemoryRouter>{children}</MemoryRouter> });
  return result;
}

describe('a folder opened from chat', () => {
  it('keeps the workspace root distinct from no folder at all', () => {
    // `''` is the root, which the panel opens by clearing its filter. Folded to
    // null it said no folder was asked for, and with a file on screen neither
    // panel effect ran, so the link moved nothing.
    const result = open();
    act(() => result.current.handleOpenFileFromChat('/home/workspace/'));

    expect(result.current.panelTarget).toMatchObject({ kind: 'file', dir: '' });
  });

  it('counts the same folder asked for twice as two requests', () => {
    // The folder stays on screen as the tree's filter, so the second click
    // leaves the string it carries unchanged. Only the count moves.
    const result = open();
    act(() => result.current.handleOpenFileFromChat('results/q3/'));
    const first = result.current.panelTarget as { dir?: string; seq?: number };

    act(() => result.current.handleOpenFileFromChat('results/q3/'));
    const second = result.current.panelTarget as { dir?: string; seq?: number };

    expect(second.dir).toBe(first.dir);
    expect(second.seq).toBe((first.seq ?? 0) + 1);
  });
});
