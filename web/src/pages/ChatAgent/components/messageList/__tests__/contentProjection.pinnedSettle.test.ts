/**
 * A turn stays live while an always-live call runs, so it ends when that call
 * ends, not when its stream closed.
 *
 * `TaskOutput` and `WebFetch` outlive the stream that launched them, which is
 * the whole point of pinning them. The stream-close stamp is therefore taken
 * before the turn's work is done, and a fold measuring to it counts backwards:
 * the row reads `Working for 2m` and then settles to `Worked for 5s`.
 */
import { describe, it, expect } from 'vitest';
import { projectContent } from '../contentProjection';
import type { ContentInput } from '../contentProjection';

const STREAM_CLOSED = 1_700_000_000_000;
const TASK_SETTLED = STREAM_CLOSED + 143_000;

function withTask(overrides: Record<string, unknown>): ContentInput {
  return {
    segments: [
      { type: 'text', content: 'Kicking off the research.', order: 0 },
      { type: 'tool_call', toolCallId: 'tc1', order: 1 },
    ],
    reasoningProcesses: {},
    toolCallProcesses: {
      tc1: {
        toolName: 'TaskOutput', toolCall: { args: {} },
        order: 1, _createdAt: STREAM_CLOSED - 60_000,
        ...overrides,
      },
    },
    // The stream has ended; the pin is what keeps the turn alive.
    isStreaming: false,
  };
}

describe('projectContent pinned settle instant', () => {
  it('reports no settle instant while the pinned call is still running', () => {
    const projection = projectContent(withTask({ isInProgress: true, isComplete: false }));

    expect(projection.hasPinnedLive).toBe(true);
    expect(projection.pinnedSettledAt).toBeNull();
  });

  it('reports when the pinned call stopped, once it has', () => {
    const projection = projectContent(withTask({
      isInProgress: false, isComplete: true, _settledAt: TASK_SETTLED,
    }));

    expect(projection.hasPinnedLive).toBe(false);
    expect(projection.pinnedSettledAt).toBe(TASK_SETTLED);
  });

  it('takes the last of several pinned calls', () => {
    const input = withTask({ isInProgress: false, isComplete: true, _settledAt: TASK_SETTLED });
    input.segments.push({ type: 'tool_call', toolCallId: 'tc2', order: 2 });
    input.toolCallProcesses.tc2 = {
      toolName: 'WebFetch', toolCall: { args: {} }, order: 2,
      _createdAt: STREAM_CLOSED - 30_000,
      isInProgress: false, isComplete: true, _settledAt: TASK_SETTLED - 20_000,
    };

    expect(projectContent(input).pinnedSettledAt).toBe(TASK_SETTLED);
  });

  it('leaves an ordinary tool call out of it', () => {
    // Only a pinned call can settle after the stream, so only a pinned one
    // can move the turn's end. A Read that finished mid-stream did not.
    const projection = projectContent(withTask({
      toolName: 'Read', isInProgress: false, isComplete: true, _settledAt: TASK_SETTLED,
    }));

    expect(projection.pinnedSettledAt).toBeNull();
  });

  it('carries no instant when the result arrived over a reconnect backlog', () => {
    // The stamp is read on arrival and withheld on a replayed frame, so a
    // reconnect cannot date a settled turn to the moment it reconnected.
    const projection = projectContent(withTask({
      isInProgress: false, isComplete: true, _settledAt: undefined,
    }));

    expect(projection.pinnedSettledAt).toBeNull();
  });
});
