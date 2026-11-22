import React from 'react';
import { beforeEach, describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';

vi.mock('../../../utils/api', async (importOriginal) => ({ ...(await importOriginal<Record<string, unknown>>()), getPreviewUrl: vi.fn() }));

// A data card wants 480, a file read 850; a 1600px container caps the
// default at 880 and a chart's wider ask at 1472.
const OVERVIEW = { toolName: 'get_company_overview', toolCall: { id: 'tc1', args: {} }, isComplete: true, toolCallResult: { artifact: { type: 'company_overview' } } };
const READ = { toolName: 'Read', toolCall: { id: 'tc2', args: {} }, isComplete: true, toolCallResult: { content: 'x' } };
const MESSAGES = [{ id: 'm1', toolCallProcesses: { tc1: OVERVIEW, tc2: READ } }];

function container(width: number): HTMLDivElement {
  const el = document.createElement('div');
  Object.defineProperty(el, 'offsetWidth', { value: width });
  return el;
}

function open() {
  const { result } = renderHook(() => useRightPanel({
    isMobile: false,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: container(1600) },
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

// The panel reports the tab in front as it changes; these hooks run without it.
function front(result: ReturnType<typeof open>, kind: Parameters<ReturnType<typeof open>['current']['handleActiveTabKindChange']>[0]) {
  act(() => result.current.handleActiveTabKindChange(kind));
}

beforeEach(() => localStorage.clear());

describe('panel width per kind of tab', () => {
  it('opens a tool result at the width its content asks for', () => {
    const result = open();
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelWidth).toBe(480);
  });

  it('moves to the width of each landing, narrower as well as wider', () => {
    const result = open();
    act(() => result.current.handleToolCallDetailClick('tc2'));
    expect(result.current.rightPanelWidth).toBe(850);
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelWidth).toBe(480);
  });

  it('opens Status narrow, beside the chat', () => {
    const result = open();
    act(() => result.current.handleOpenStatusFromChat());
    expect(result.current.rightPanelWidth).toBe(420);
  });

  it('follows the tab in front, both ways', () => {
    const result = open();
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    front(result, 'chart');
    expect(result.current.rightPanelWidth).toBe(850);
    front(result, 'status');
    expect(result.current.rightPanelWidth).toBe(420);
    front(result, 'chart');
    expect(result.current.rightPanelWidth).toBe(850);
  });

  it('holds a landing\'s width until the panel reports the tab, rather than the old front\'s', () => {
    const result = open();
    act(() => result.current.handleOpenStatusFromChat());
    front(result, 'status');
    act(() => result.current.handleOpenChart({ symbol: 'GOOGL' }));
    expect(result.current.rightPanelWidth).toBe(850);
  });

  it('keeps a dragged width for its kind, past other tabs and a reload', () => {
    const result = open();
    act(() => result.current.handleOpenStatusFromChat());
    front(result, 'status');
    drag(result, 80);
    expect(result.current.rightPanelWidth).toBe(500);
    front(result, 'file');
    expect(result.current.rightPanelWidth).toBe(850);
    front(result, 'status');
    expect(result.current.rightPanelWidth).toBe(500);

    const reloaded = open();
    act(() => reloaded.current.handleOpenStatusFromChat());
    expect(reloaded.current.rightPanelWidth).toBe(500);
  });

  it("keeps a tool result's drag only while the panel shows it", () => {
    const result = open();
    act(() => result.current.handleToolCallDetailClick('tc1'));
    front(result, 'tool');
    drag(result, 100);
    expect(result.current.rightPanelWidth).toBe(580);
    const reloaded = open();
    act(() => reloaded.current.handleToolCallDetailClick('tc1'));
    expect(reloaded.current.rightPanelWidth).toBe(480);
  });

  it('reopens from the header at the width of the tab it had in front', () => {
    const result = open();
    act(() => result.current.handleOpenStatusFromChat());
    front(result, 'status');
    act(() => result.current.handleToggleFilePanel());
    expect(result.current.rightPanelType).toBeNull();
    act(() => result.current.handleToggleFilePanel());
    expect(result.current.rightPanelWidth).toBe(420);
  });
});
