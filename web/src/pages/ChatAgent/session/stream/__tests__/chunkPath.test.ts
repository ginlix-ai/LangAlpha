/**
 * Which path a streamed chunk takes. Live, it waits for the next frame, for the
 * main transcript and for a subagent's card alike. A reconnect's backlog
 * replays in one task and applies at once, so the reconnected turn appears in a
 * single render.
 */
import { describe, expect, it, vi } from 'vitest';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

function setup(isReconnect: boolean) {
  const rt = {
    setMessages: vi.fn(),
    queueMessages: vi.fn(),
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    lastEventIdRef: { current: null },
    threadIdRef: { current: null },
  };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    isReconnect,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    { clearModelStatus: () => {} } as unknown as Processor[1],
    'a1',
    refs as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  const chunk = (agent: string) => process({ event: 'message_chunk', content_type: 'text', content: 'Rev', agent } as SSEEvent);
  return { rt, chunk };
}

describe('the chunk path', () => {
  it('holds a live chunk for the next frame', () => {
    const { rt, chunk } = setup(false);
    chunk('main');
    chunk('task:k7Xm2p');
    expect(rt.queueMessages).toHaveBeenCalledOnce();
    expect(rt.setMessages).not.toHaveBeenCalled();
    expect(rt.updateSubagentCard).toHaveBeenCalledWith('task:k7Xm2p', expect.anything(), { nextFrame: true });
  });

  it('applies a reconnect backlog at once', () => {
    const { rt, chunk } = setup(true);
    chunk('main');
    chunk('task:k7Xm2p');
    expect(rt.setMessages).toHaveBeenCalledOnce();
    expect(rt.queueMessages).not.toHaveBeenCalled();
    expect(rt.updateSubagentCard).toHaveBeenCalledWith('task:k7Xm2p', expect.anything(), { nextFrame: false });
  });
});
