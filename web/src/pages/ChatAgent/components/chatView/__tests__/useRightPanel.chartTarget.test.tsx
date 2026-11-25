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

function open(isMobile: boolean, threadId: string | null = null) {
  const setFilePanelWorkspaceId = vi.fn();
  const { result } = renderHook(() => useRightPanel({
    isMobile,
    workspaceId: 'ws',
    threadId,
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

  it('opens a chart comment on its own workspace\'s chart, whatever workspace the panel shows', () => {
    // A chart tab with no workspace draws the panel's, which a sibling's
    // file may have switched; the comment's record is in its own.
    const result = open(false);
    act(() => result.current.handleOpenArtifactComment({
      artifactId: 'a-1', workspaceId: 'ws', kind: 'chart', path: 'NVDA:1hour', commentId: 'c-9',
    }));

    expect(result.current.panelTarget).toMatchObject({ kind: 'chart', symbol: 'NVDA', timeframe: '1hour', workspaceId: 'ws', comment: 'c-9' });
  });

  it('opens a chart comment on the MarketView page on mobile, with the comment to open there', () => {
    const result = open(true);
    act(() => result.current.handleOpenArtifactComment({
      artifactId: 'a-1', workspaceId: 'ws', kind: 'chart', path: 'NVDA:1hour', commentId: 'c-9',
    }));

    const url = new URL(navigate.mock.calls[0][0] as string, 'http://localhost');
    expect(url.pathname).toBe('/market');
    expect(url.searchParams.get('symbol')).toBe('NVDA');
    expect(url.searchParams.get('tf')).toBe('1hour');
    expect(url.searchParams.get('comment')).toBe('c-9');
  });

  it('carries the chat on to its own workspace\'s chart, and none to a sibling\'s', () => {
    // MarketView mounts the route's thread first, and a thread lives in one
    // workspace: under a sibling it would hold the chart's comments it cannot take.
    const result = open(true, 't1');
    act(() => result.current.handleOpenChart({ symbol: 'NVDA' }));
    act(() => result.current.handleOpenArtifactComment({
      artifactId: 'a-1', workspaceId: 'ws-2', kind: 'chart', path: 'NVDA:1day', commentId: 'c-9',
    }));

    const [own, sibling] = navigate.mock.calls.map(([to]) => new URL(to as string, 'http://localhost').searchParams);
    expect(own.get('thread')).toBe('t1');
    expect(sibling.get('ws')).toBe('ws-2');
    expect(sibling.get('thread')).toBeNull();
  });
});
