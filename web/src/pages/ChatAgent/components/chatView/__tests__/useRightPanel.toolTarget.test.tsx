import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';
import type { TranscriptMessage } from '../toolCallLookup';

vi.mock('../../../utils/api', async (importOriginal) => ({ ...(await importOriginal<Record<string, unknown>>()), getPreviewUrl: vi.fn() }));

const TOOL_CALL = { toolName: 'WebFetch', toolCall: { id: 'tc1', args: {} }, isComplete: true, toolCallResult: { content: 'done' } };
const MESSAGES: readonly TranscriptMessage[] = [{ id: 'm1', toolCallProcesses: { tc1: TOOL_CALL } }];

function open(isMobile: boolean) {
  const setFilePanelWorkspaceId = vi.fn();
  return renderHook((props: { messages: readonly TranscriptMessage[] }) => useRightPanel({
    isMobile,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: null },
    setFilePanelWorkspaceId,
    messages: props.messages,
  }), { initialProps: { messages: MESSAGES }, wrapper: ({ children }) => <MemoryRouter>{children}</MemoryRouter> });
}

describe('a tool row whose record the transcript no longer holds', () => {
  it('still opens a tool tab on desktop, which says the call is gone', () => {
    // A regenerated turn replaces its records; the row that was clicked may
    // outlive them. Doing nothing reads as a dead click.
    const { result } = open(false);
    act(() => result.current.handleToolCallDetailClick('missing'));

    expect(result.current.rightPanelType).toBe('file');
    expect(result.current.panelTarget).toMatchObject({ kind: 'tool', toolCallId: 'missing' });
  });

  it('opens nothing on mobile, whose sheet has no body for it', () => {
    const { result } = open(true);
    act(() => result.current.handleToolCallDetailClick('missing'));

    expect(result.current.rightPanelType).toBeNull();
  });

  it('closes the mobile sheet when the record leaves under it', () => {
    const { result, rerender } = open(true);
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelType).toBe('detail');

    act(() => rerender({ messages: [{ id: 'm1', toolCallProcesses: {} }] }));

    // Left as 'detail' the sheet is invisible and the back gesture, armed
    // for it, would pop a sentinel over a panel that is not there.
    expect(result.current.rightPanelType).toBeNull();
    expect(result.current.detailToolCall).toBeNull();
  });
});
