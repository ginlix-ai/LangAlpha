import { describe, it, expect } from 'vitest';
import { countToolCalls } from '../subagentMetrics';

describe('countToolCalls', () => {
  it('returns 0 for empty / nullish input', () => {
    expect(countToolCalls(undefined)).toBe(0);
    expect(countToolCalls(null)).toBe(0);
    expect(countToolCalls([])).toBe(0);
  });

  it('counts toolCallProcesses keys across multiple messages', () => {
    const messages = [
      { toolCallProcesses: { a: {}, b: {} } },
      { toolCallProcesses: { c: {} } },
      { toolCallProcesses: { d: {}, e: {}, f: {} } },
    ];
    expect(countToolCalls(messages)).toBe(6);
  });

  it('skips messages without toolCallProcesses', () => {
    const messages = [
      { role: 'assistant' },
      { toolCallProcesses: { a: {} } },
      { reasoningProcesses: {} },
      { toolCallProcesses: { b: {}, c: {} } },
    ];
    expect(countToolCalls(messages)).toBe(3);
  });

  it('treats duplicate keys across messages as separate (since maps live on each message)', () => {
    // Documents the actual semantic: a re-emitted toolCallId on a later
    // message would be a backend bug, not something we paper over.
    const messages = [
      { toolCallProcesses: { a: {} } },
      { toolCallProcesses: { a: {} } },
    ];
    expect(countToolCalls(messages)).toBe(2);
  });
});
