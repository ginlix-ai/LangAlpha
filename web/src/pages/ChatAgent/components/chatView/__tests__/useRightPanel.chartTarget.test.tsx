import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';

const { navigate } = vi.hoisted(() => ({ navigate: vi.fn() }));
vi.mock('react-router', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return { ...orig, useNavigate: () => navigate };
});
vi.mock('../../../utils/api', async (importOriginal) => ({ ...(await importOriginal<Record<string, unknown>>()), getPreviewUrl: vi.fn() }));

function open(isMobile: boolean) {
  const setFilePanelWorkspaceId = vi.fn();
  const { result } = renderHook(() => useRightPanel({
    isMobile,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: null },
    setFilePanelWorkspaceId,
    messages: [],
  }), { wrapper: ({ children }) => <MemoryRouter initialEntries={['/chat/t1']}>{children}</MemoryRouter> });
  return result;
}

beforeEach(() => vi.clearAllMocks());

describe('a chart opened from chat', () => {
  it('lands in the Files panel as a chart target, wide', () => {
    const result = open(false);
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL', timeframe: '1day' }));

    expect(result.current.rightPanelType).toBe('file');
    expect(result.current.panelTarget).toMatchObject({ kind: 'chart', symbol: 'GOOGL', timeframe: '1day' });
    expect(result.current.rightPanelWidth).toBe(850);
    expect(navigate).not.toHaveBeenCalled();
  });

  it('counts the same symbol asked for twice as two requests', () => {
    const result = open(false);
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    const first = result.current.panelTarget as { seq?: number };
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    const second = result.current.panelTarget as { seq?: number };

    expect(second.seq).toBe((first.seq ?? 0) + 1);
  });

  it('clears once the panel has opened it', () => {
    const result = open(false);
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    const { seq } = result.current.panelTarget as { seq: number };
    act(() => result.current.handleTargetHandled(seq));

    expect(result.current.panelTarget).toBeNull();
    expect(result.current.rightPanelType).toBe('file');
  });

  it('keeps an ask that landed after the one the panel is clearing', () => {
    // The panel consumes a target in an effect, so a second click can land
    // between the first one's render and its clear; the clear names the ask
    // it consumed and leaves the newer one for the next effect.
    const result = open(false);
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    const { seq } = result.current.panelTarget as { seq: number };
    act(() => result.current.handleOpenChart({ symbol: 'NVDA' }));
    act(() => result.current.handleTargetHandled(seq));

    expect(result.current.panelTarget).toMatchObject({ kind: 'chart', symbol: 'NVDA' });
  });

  it('goes to the MarketView page on mobile, which has no tab strip to land in', () => {
    const result = open(true);
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL', timeframe: '1day' }));

    expect(result.current.panelTarget).toBeNull();
    expect(navigate).toHaveBeenCalledTimes(1);
    const url = new URL(navigate.mock.calls[0][0] as string, 'http://localhost');
    expect(url.pathname).toBe('/market');
    expect(url.searchParams.get('symbol')).toBe('GOOGL');
    expect(url.searchParams.get('tf')).toBe('1day');
    expect(url.searchParams.get('ws')).toBe('ws');
    expect(url.searchParams.get('returnTo')).toBe('/chat/t1');
  });
});
