import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';
import { CHART_SURFACE_MIN_WIDTH } from '@/pages/MarketView/components/chartSurfaceLayout';

vi.mock('../../../utils/api', async (importOriginal) => ({ ...(await importOriginal<Record<string, unknown>>()), getPreviewUrl: vi.fn() }));

// A data card opens the panel at 480, under the chart surface's floor.
const OVERVIEW = { toolName: 'get_company_overview', toolCall: { id: 'tc1', args: {} }, isComplete: true, toolCallResult: { artifact: { type: 'company_overview' } } };
const MESSAGES = [{ id: 'm1', toolCallProcesses: { tc1: OVERVIEW } }];

function container(width: number): HTMLDivElement {
  const el = document.createElement('div');
  Object.defineProperty(el, 'offsetWidth', { value: width });
  return el;
}

function open(containerWidth = 1600, isMobile = false) {
  const { result } = renderHook(() => useRightPanel({
    isMobile,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: container(containerWidth) },
    setFilePanelWorkspaceId: vi.fn(),
    messages: MESSAGES,
  }), { wrapper: ({ children }) => <MemoryRouter>{children}</MemoryRouter> });
  return result;
}

function drag(result: ReturnType<typeof open>, by: number) {
  act(() => result.current.handleDividerMouseDown({ preventDefault: () => {}, clientX: 600 } as unknown as React.MouseEvent));
  act(() => document.dispatchEvent(new MouseEvent('mousemove', { clientX: 600 - by })));
  act(() => document.dispatchEvent(new MouseEvent('mouseup')));
}

describe('the chart tab floor', () => {
  it('widens a narrow panel when a chart tab comes to the front', () => {
    const result = open();
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelWidth).toBe(480);

    act(() => result.current.handleActiveTabKindChange('chart'));
    expect(result.current.rightPanelWidth).toBe(CHART_SURFACE_MIN_WIDTH);
  });

  it('holds the divider at the floor while the chart is in front, and frees it once it is not', () => {
    const result = open();
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    act(() => result.current.handleActiveTabKindChange('chart'));
    expect(result.current.rightPanelWidth).toBe(850);

    drag(result, -600);
    expect(result.current.rightPanelWidth).toBe(CHART_SURFACE_MIN_WIDTH);

    act(() => result.current.handleActiveTabKindChange('file'));
    drag(result, -600);
    expect(result.current.rightPanelWidth).toBe(280);
  });

  it('raises the cap for a chart that came to the front without landing', () => {
    // 1000 wide: the default 55% cap (550) sits under the floor, the raised one does not.
    const result = open(1000);
    act(() => result.current.handleToolCallDetailClick('tc1'));
    act(() => result.current.handleActiveTabKindChange('chart'));
    expect(result.current.rightPanelWidth).toBe(CHART_SURFACE_MIN_WIDTH);
  });

  it('lets the container cap win on a screen too narrow for the floor', () => {
    // 600 wide: even the raised 92% cap (552) is under the floor.
    const result = open(600);
    act(() => result.current.handleToolCallDetailClick('tc1'));
    act(() => result.current.handleActiveTabKindChange('chart'));
    expect(result.current.rightPanelWidth).toBeCloseTo(552);

    drag(result, 300);
    expect(result.current.rightPanelWidth).toBeCloseTo(552);
  });

  it('leaves a mobile panel alone, which has no divider', () => {
    const result = open(1600, true);
    act(() => result.current.handleActiveTabKindChange('chart'));
    expect(result.current.rightPanelWidth).toBe(750);
  });
});
