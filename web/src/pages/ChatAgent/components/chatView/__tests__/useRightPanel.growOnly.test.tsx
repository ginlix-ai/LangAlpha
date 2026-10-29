import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';

vi.mock('../../../utils/api', async (importOriginal) => ({ ...(await importOriginal<Record<string, unknown>>()), getPreviewUrl: vi.fn() }));

// A data card wants 480, a file read 850; jsdom's 1024px window
// caps the default at 563.2 and a chart's wider ask at 942.
const OVERVIEW = { toolName: 'get_company_overview', toolCall: { id: 'tc1', args: {} }, isComplete: true, toolCallResult: { artifact: { type: 'company_overview' } } };
const READ = { toolName: 'Read', toolCall: { id: 'tc2', args: {} }, isComplete: true, toolCallResult: { content: 'x' } };
const MESSAGES = [{ id: 'm1', toolCallProcesses: { tc1: OVERVIEW, tc2: READ } }];

function open() {
  const { result } = renderHook(() => useRightPanel({
    isMobile: false,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: null },
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

describe('panel width across landings', () => {
  it('opens a closed panel at what the landing asks for', () => {
    const result = open();
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelWidth).toBe(480);
  });

  it('never narrows an open panel for a landing that asks for less', () => {
    const result = open();
    act(() => result.current.handleToolCallDetailClick('tc2'));
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelWidth).toBeCloseTo(563.2);
  });

  it('widens an open panel for a landing that asks for more', () => {
    const result = open();
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelWidth).toBe(480);
    act(() => result.current.handleToolCallDetailClick('tc2'));
    expect(result.current.rightPanelWidth).toBeCloseTo(563.2);
  });

  it('keeps the width the reader dragged to', () => {
    const result = open();
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    drag(result, 50);
    expect(result.current.rightPanelWidth).toBe(900);
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelWidth).toBe(900);
  });

  it('keeps the wider cap a chart asked for while the panel stays open', () => {
    // Folding the cap back to the default would narrow the panel under the
    // chart tab still in the strip.
    const result = open();
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    expect(result.current.rightPanelWidth).toBe(850);
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelWidth).toBe(850);
  });

  it('starts over once the panel has been closed', () => {
    const result = open();
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    act(() => result.current.handleToggleFilePanel());
    expect(result.current.rightPanelType).toBeNull();
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelWidth).toBe(480);
  });
});
