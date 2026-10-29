import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router';

import { useRightPanel } from '../useRightPanel';

vi.mock('../../../utils/api', async (importOriginal) => ({ ...(await importOriginal<Record<string, unknown>>()), getPreviewUrl: vi.fn() }));

const call = (id: string, action: 'watch' | 'unwatch') => ({
  toolName: 'watch_market',
  toolCall: { id, name: 'watch_market', args: { symbols: ['NVDA'], action } },
  toolCallResult: { content: 'ok' },
  isComplete: true,
});
const messages = [{ toolCallProcesses: { w1: call('w1', 'watch'), u1: call('u1', 'unwatch') } }];

function open(watching: boolean) {
  const { result } = renderHook(() => useRightPanel({
    isMobile: false,
    workspaceId: 'ws',
    isActive: true,
    containerRef: { current: null },
    setFilePanelWorkspaceId: vi.fn(),
    messages,
    watching,
  }), { wrapper: ({ children }) => <MemoryRouter initialEntries={['/chat/t1']}>{children}</MemoryRouter> });
  return result;
}

describe('a watch_market row clicked in chat', () => {
  it('opens the live Status tab while the watch runs', () => {
    const result = open(true);
    act(() => result.current.handleToolCallDetailClick('w1'));
    expect(result.current.panelTarget).toMatchObject({ kind: 'status' });
  });

  it('shows the call itself once the watch has ended', () => {
    const result = open(false);
    act(() => result.current.handleToolCallDetailClick('w1'));
    expect(result.current.panelTarget).toMatchObject({ kind: 'tool', toolCallId: 'w1' });
  });

  it('shows a stop call as itself even while another watch runs', () => {
    const result = open(true);
    act(() => result.current.handleToolCallDetailClick('u1'));
    expect(result.current.panelTarget).toMatchObject({ kind: 'tool', toolCallId: 'u1' });
  });
});
