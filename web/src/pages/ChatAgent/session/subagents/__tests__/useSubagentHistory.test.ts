/**
 * A history publish first runs what the view hands it, in the same task: the
 * chat hands its transcript's flush, so a settled subagent never renders ahead
 * of the text that streamed before it.
 */
import { describe, expect, it } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { ZERO_USAGE } from '../../../utils/tokenUsage';
import { useSubagentHistory } from '../useSubagentHistory';

const t = (k: string) => k;
const subagentStateRefsRef = { current: {} };

describe('useSubagentHistory', () => {
  it('runs beforePublish ahead of every publish', () => {
    let flushes = 0;
    const { result } = renderHook(() => useSubagentHistory(
      () => { flushes += 1; },
      { t, threadId: 'thread-1', subagentStateRefsRef },
    ));
    act(() => result.current.store.putEntries({
      'task:ch1': {
        taskId: 'task:ch1', description: 'AAPL', prompt: '', type: 'research', messages: [],
        status: 'completed', toolCalls: 0, tokenUsage: ZERO_USAGE, currentTool: '',
      },
    }));
    act(() => result.current.store.patchEntry('task:ch1', { status: 'cancelled' }));
    expect(flushes).toBe(2);
    expect(result.current.getSubagentHistory('task:ch1')?.status).toBe('cancelled');
  });
});
