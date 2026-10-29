/**
 * A subagent's streamed chunks reach its card on the next frame.
 *
 * Every chunk was a render of the whole chat view, and parallel subagents
 * stream at once, so chunk writes wait for a frame like the main transcript's.
 * The order is what can break. Each chunk carries the task's whole message
 * list as it stood, so a chunk applied after a later write would take that
 * write back: a finished tool call would spin again, and a card cleared for a
 * new turn would come back. Every other write applies what waits first.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { mockFrames, runFrame, settleFrames } from '@/test/frames';
import { useCardState } from '../useCardState';

const AGENT_ID = 'task:Q81mZx';
const CARD_ID = `subagent-${AGENT_ID}`;

beforeEach(mockFrames);

afterEach(() => {
  settleFrames();
  vi.restoreAllMocks();
});

const streaming = (content: string) => [{ id: 'a1', role: 'assistant', content, isStreaming: true }];

describe('useCardState — chunks on the next frame', () => {
  it('applies every chunk of a frame as one write, on that frame', () => {
    const { result } = renderHook(() => useCardState());
    act(() => result.current.updateSubagentCard(AGENT_ID, { status: 'active', messages: [] }));

    act(() => {
      result.current.updateSubagentCard(AGENT_ID, { messages: streaming('Rev') }, { nextFrame: true });
      result.current.updateSubagentCard(AGENT_ID, { messages: streaming('Revenue grew') }, { nextFrame: true });
    });
    expect(result.current.cards[CARD_ID].subagentData?.messages).toEqual([]);

    act(() => runFrame());
    expect(result.current.cards[CARD_ID].subagentData?.messages?.[0].content).toBe('Revenue grew');
  });

  it('never lets a waiting chunk take back a later write', () => {
    const { result } = renderHook(() => useCardState());
    act(() => result.current.updateSubagentCard(AGENT_ID, { status: 'active', messages: [] }));
    const withTool = (proc: Record<string, unknown>) => [
      { id: 'a1', role: 'assistant', isStreaming: true, toolCallProcesses: { t1: proc } },
    ];

    act(() => {
      result.current.updateSubagentCard(AGENT_ID, { messages: withTool({ isInProgress: true }) }, { nextFrame: true });
      result.current.updateSubagentCard(AGENT_ID, { messages: withTool({ isInProgress: false, isComplete: true }) });
    });
    act(() => runFrame());

    expect(result.current.cards[CARD_ID].subagentData?.messages?.[0].toolCallProcesses?.t1).toEqual({
      isInProgress: false,
      isComplete: true,
    });
  });

  it('never brings back a card cleared for a new turn', () => {
    const { result } = renderHook(() => useCardState());
    act(() => result.current.updateSubagentCard(AGENT_ID, { status: 'active', messages: [] }));

    act(() => {
      result.current.updateSubagentCard(AGENT_ID, { messages: streaming('Revenue grew') }, { nextFrame: true });
      result.current.clearSubagentCards();
    });
    act(() => runFrame());

    expect(result.current.cards).toEqual({});
  });
});
