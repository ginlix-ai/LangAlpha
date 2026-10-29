import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';
import type { TranscriptMessage } from '../toolCallLookup';

const { getPreviewUrl } = vi.hoisted(() => ({ getPreviewUrl: vi.fn() }));
vi.mock('../../../utils/api', async (importOriginal) => ({ ...(await importOriginal<Record<string, unknown>>()), getPreviewUrl }));

const TOOL_CALL = { toolName: 'WebFetch', toolCall: { id: 'tc1', args: {} }, isInProgress: true };
const MESSAGES = [{ id: 'm1', toolCallProcesses: { tc1: TOOL_CALL } }];

function open(isMobile: boolean) {
  // Hoisted out of the render callback: a fresh identity per render would
  // re-run the workspace-reset effect and wipe the state under test.
  const setFilePanelWorkspaceId = vi.fn();
  return renderHook((props: { isMobile: boolean; messages?: readonly TranscriptMessage[] }) => useRightPanel({
    isMobile: props.isMobile,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: null },
    setFilePanelWorkspaceId,
    messages: props.messages ?? MESSAGES,
  }), { initialProps: { isMobile } as { isMobile: boolean; messages?: readonly TranscriptMessage[] }, wrapper: ({ children }) => <MemoryRouter>{children}</MemoryRouter> });
}

const APP = { url: '', port: 8050, title: 'Dashboard', command: 'python app.py', path: '/timeline.html', loading: true };

describe('the detail sheet', () => {
  it('reads the tool call live rather than holding the record it was opened on', () => {
    const { result, rerender } = open(true);
    act(() => result.current.handleToolCallDetailClick('tc1'));
    expect(result.current.rightPanelType).toBe('detail');
    expect(result.current.detailToolCall).toMatchObject({ isInProgress: true });

    const settled = { ...TOOL_CALL, isInProgress: false, isComplete: true, toolCallResult: { content: 'done' } };
    act(() => rerender({ isMobile: true, messages: [{ id: 'm1', toolCallProcesses: { tc1: settled } }] }));

    expect(result.current.detailToolCall).toMatchObject({ isComplete: true, toolCallResult: { content: 'done' } });
  });

  it('lands the call it was showing as a tool tab when the viewport widens', () => {
    const { result, rerender } = open(true);
    act(() => result.current.handleToolCallDetailClick('tc1'));

    act(() => rerender({ isMobile: false }));

    expect(result.current.rightPanelType).toBe('file');
    expect(result.current.panelTarget).toMatchObject({ kind: 'tool', toolCallId: 'tc1' });
    expect(result.current.detailToolCall).toBeNull();
  });
});

beforeEach(() => {
  vi.clearAllMocks();
  getPreviewUrl.mockResolvedValue({ url: 'https://8050-sandbox.example.com/' });
});

describe('the viewport widening with the preview sheet open', () => {
  it('lands the app the sheet was showing as a Files-panel preview target', async () => {
    const { result, rerender } = open(true);
    await act(async () => { result.current.handleOpenPreview(APP); });
    expect(result.current.rightPanelType).toBe('preview');

    act(() => rerender({ isMobile: false }));

    // The desktop column renders nothing for `preview`; the app goes where a
    // desktop opens it, carrying what the tab needs to mint its own URL.
    expect(result.current.rightPanelType).toBe('file');
    expect(result.current.panelTarget).toMatchObject({
      kind: 'preview', port: 8050, title: 'Dashboard', command: 'python app.py', path: '/timeline.html',
    });
    expect(result.current.rightPanelWidth).toBe(850);
  });

  it('closes the column when the sheet had nothing to show', () => {
    const { result, rerender } = open(true);
    act(() => result.current.setRightPanelType('preview'));

    act(() => rerender({ isMobile: false }));

    expect(result.current.rightPanelType).toBeNull();
    expect(result.current.panelTarget).toBeNull();
  });

  it('leaves a file panel alone when the viewport widens', () => {
    const { result, rerender } = open(true);
    act(() => result.current.handleToggleFilePanel());

    act(() => rerender({ isMobile: false }));

    expect(result.current.rightPanelType).toBe('file');
    expect(result.current.panelTarget).toBeNull();
  });

  it('does not react to the viewport narrowing', async () => {
    const { result, rerender } = open(false);
    act(() => result.current.handleOpenPreview(APP));
    const before = result.current.panelTarget;

    act(() => rerender({ isMobile: true }));

    expect(result.current.rightPanelType).toBe('file');
    expect(result.current.panelTarget).toBe(before);
  });
});
