// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { finalizeAssistantMessage } from '../finalizeMessage';

describe('assistant completion observations', () => {
  it('records the first terminal observation and keeps it on duplicate cleanup', () => {
    const message = { isStreaming: true, content: 'Done' };
    const ended = finalizeAssistantMessage(message, 'completed', 100);
    expect(finalizeAssistantMessage(ended, 'completed', 200)).toEqual({ ...message, isStreaming: false, completionObservedAt: 100 });
    expect(message.isStreaming).toBe(true);
  });

  it.each(['paused', 'disconnected'] as const)('does not invent a completion instant for %s', (end) => {
    expect(finalizeAssistantMessage({ isStreaming: true }, end, 100)).toEqual({ isStreaming: false, completionObservedAt: undefined });
  });

  it.each([['stopped', 'stopped'], ['failed', 'error']] as const)('applies %s and its observation together', (end, flag) => {
    expect(finalizeAssistantMessage({ isStreaming: true }, end, 100)).toMatchObject({ isStreaming: false, completionObservedAt: 100, [flag]: true });
  });

  it('leaves the server settlement instant authoritative', () => {
    expect(finalizeAssistantMessage({ completedAt: 80, completionObservedAt: 100 }, 'completed', 200)).toMatchObject({ completedAt: 80, completionObservedAt: 100 });
  });
});
