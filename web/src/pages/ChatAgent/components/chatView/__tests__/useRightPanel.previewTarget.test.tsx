import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';

const { getPreviewUrl } = vi.hoisted(() => ({ getPreviewUrl: vi.fn() }));
vi.mock('../../../utils/api', async (importOriginal) => ({ ...(await importOriginal<Record<string, unknown>>()), getPreviewUrl }));

function open(isMobile: boolean) {
  // Hoisted out of the render callback: a fresh identity per render would
  // re-run the workspace-reset effect and wipe the state under test.
  const setFilePanelWorkspaceId = vi.fn();
  const { result } = renderHook(() => useRightPanel({
    isMobile,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: null },
    setFilePanelWorkspaceId,
    messages: [],
  }), { wrapper: ({ children }) => <MemoryRouter>{children}</MemoryRouter> });
  return result;
}

const APP = { url: '', port: 8050, title: 'Dashboard', command: 'python app.py', loading: true };

beforeEach(() => {
  vi.clearAllMocks();
  getPreviewUrl.mockResolvedValue({ url: 'https://8050-sandbox.example.com/' });
});

describe('a running app opened from chat', () => {
  it('lands in the Files panel as a preview target, wide', () => {
    const result = open(false);
    act(() => result.current.handleOpenPreview(APP));

    expect(result.current.rightPanelType).toBe('file');
    expect(result.current.panelTarget).toMatchObject({
      kind: 'preview', port: 8050, title: 'Dashboard', command: 'python app.py',
    });
    expect(result.current.rightPanelWidth).toBe(850);
  });

  it('leaves the URL to the panel that owns the tab', () => {
    const result = open(false);
    act(() => result.current.handleOpenPreview(APP));

    expect(getPreviewUrl).not.toHaveBeenCalled();
  });

  it('counts the same port asked for twice as two requests', () => {
    const result = open(false);
    act(() => result.current.handleOpenPreview(APP));
    const first = result.current.panelTarget as { seq?: number };
    act(() => result.current.handleOpenPreview(APP));
    const second = result.current.panelTarget as { seq?: number };

    expect(second.seq).toBe((first.seq ?? 0) + 1);
  });

  it('clears once the panel has opened it, so a later file click is not overruled', () => {
    const result = open(false);
    act(() => result.current.handleOpenPreview(APP));
    act(() => result.current.handleTargetHandled((result.current.panelTarget as { seq: number }).seq));

    expect(result.current.panelTarget).toBeNull();
    expect(result.current.rightPanelType).toBe('file');
  });

  it('still opens the mobile sheet, which has no tab strip to land in', async () => {
    const result = open(true);
    await act(async () => { result.current.handleOpenPreview(APP); });

    expect(result.current.rightPanelType).toBe('preview');
    expect(result.current.panelTarget).toBeNull();
    expect(result.current.previewData).toMatchObject({ port: 8050, title: 'Dashboard' });
    expect(getPreviewUrl).toHaveBeenCalledWith('ws', 8050, 'python app.py');
  });
});
