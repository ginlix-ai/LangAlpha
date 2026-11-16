import { describe, it, expect, vi, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { CHART_SURFACE_MIN_WIDTH } from '@/pages/MarketView/components/chartSurfaceLayout';
import { useRightPanel } from '../useRightPanel';

vi.mock('../../../utils/api', async (importOriginal) => ({ ...(await importOriginal<Record<string, unknown>>()), getPreviewUrl: vi.fn() }));

let notify: (() => void) | null = null;

afterEach(() => {
  notify = null;
  vi.unstubAllGlobals();
});

function open(container: HTMLDivElement) {
  vi.stubGlobal('ResizeObserver', class {
    constructor(callback: () => void) { notify = callback; }
    observe() {}
    disconnect() {}
  });
  const { result } = renderHook(() => useRightPanel({
    isMobile: false,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: container },
    setFilePanelWorkspaceId: vi.fn(),
    messages: [],
  }), { wrapper: ({ children }) => <MemoryRouter>{children}</MemoryRouter> });
  return result;
}

function resize(container: HTMLElement, width: number) {
  Object.defineProperty(container, 'offsetWidth', { configurable: true, value: width });
  act(() => notify?.());
}

describe('panel width as the window changes', () => {
  it('keeps to its cap in a narrower window and gets its width back in a wide one', () => {
    const container = document.createElement('div');
    Object.defineProperty(container, 'offsetWidth', { configurable: true, value: 1024 });
    const result = open(container);
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    expect(result.current.rightPanelWidth).toBe(850);

    resize(container, 600);
    expect(result.current.rightPanelWidth).toBeLessThan(600);

    resize(container, 1024);
    expect(result.current.rightPanelWidth).toBe(850);
  });

  it('keeps the width chosen when the divider is clicked in a narrower window', () => {
    const container = document.createElement('div');
    Object.defineProperty(container, 'offsetWidth', { configurable: true, value: 1024 });
    const result = open(container);
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    act(() => result.current.handleActiveTabKindChange('chart'));

    resize(container, 600);
    act(() => result.current.handleDividerMouseDown({ preventDefault: () => {}, clientX: 300 } as unknown as React.MouseEvent));
    act(() => document.dispatchEvent(new MouseEvent('mouseup')));

    resize(container, 1024);
    expect(result.current.rightPanelWidth).toBe(850);
  });

  it('keeps a chart at its floor after a drag in a window too narrow for it', () => {
    const container = document.createElement('div');
    Object.defineProperty(container, 'offsetWidth', { configurable: true, value: 1024 });
    const result = open(container);
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    act(() => result.current.handleActiveTabKindChange('chart'));

    resize(container, 600);
    act(() => result.current.handleDividerMouseDown({ preventDefault: () => {}, clientX: 300 } as unknown as React.MouseEvent));
    act(() => document.dispatchEvent(new MouseEvent('mousemove', { clientX: 320 })));
    act(() => document.dispatchEvent(new MouseEvent('mouseup')));
    expect(result.current.rightPanelWidth).toBeLessThan(CHART_SURFACE_MIN_WIDTH);

    resize(container, 1024);
    expect(result.current.rightPanelWidth).toBeGreaterThanOrEqual(CHART_SURFACE_MIN_WIDTH);
  });
});
