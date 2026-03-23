import { describe, it, expect } from 'vitest';
import { mapToolCallIdToAgentId } from '../useChatMessages';

describe('mapToolCallIdToAgentId', () => {
  /**
   * Core scenario: 3 parallel Task tool calls where artifact events
   * arrive in a different order than the tool_calls array.
   *
   * tool_calls: [tc-nvidia, tc-google, tc-amd]
   * artifacts arrive: Google first, then AMD, then NVIDIA
   */
  it('maps correctly when artifact events arrive out of order', () => {
    const map = new Map<string, string>();
    let queue = ['tc-nvidia', 'tc-google', 'tc-amd'];

    // Google's artifact arrives first
    queue = mapToolCallIdToAgentId('tc-google', 'task:B', 'init', queue, map);
    expect(map.get('tc-google')).toBe('task:B');
    expect(queue).toEqual(['tc-nvidia', 'tc-amd']);

    // AMD's artifact arrives second
    queue = mapToolCallIdToAgentId('tc-amd', 'task:C', 'init', queue, map);
    expect(map.get('tc-amd')).toBe('task:C');
    expect(queue).toEqual(['tc-nvidia']);

    // NVIDIA's artifact arrives last
    queue = mapToolCallIdToAgentId('tc-nvidia', 'task:A', 'init', queue, map);
    expect(map.get('tc-nvidia')).toBe('task:A');
    expect(queue).toEqual([]);

    // All 3 mappings are correct
    expect(map.size).toBe(3);
    expect(map.get('tc-nvidia')).toBe('task:A');
    expect(map.get('tc-google')).toBe('task:B');
    expect(map.get('tc-amd')).toBe('task:C');
  });

  it('maps correctly when artifact events arrive in order', () => {
    const map = new Map<string, string>();
    let queue = ['tc-1', 'tc-2'];

    queue = mapToolCallIdToAgentId('tc-1', 'task:A', 'init', queue, map);
    queue = mapToolCallIdToAgentId('tc-2', 'task:B', 'init', queue, map);

    expect(map.get('tc-1')).toBe('task:A');
    expect(map.get('tc-2')).toBe('task:B');
    expect(queue).toEqual([]);
  });

  it('falls back to FIFO when tool_call_id is absent (legacy events)', () => {
    const map = new Map<string, string>();
    let queue = ['tc-1', 'tc-2', 'tc-3'];

    queue = mapToolCallIdToAgentId(undefined, 'task:A', 'init', queue, map);
    expect(map.get('tc-1')).toBe('task:A'); // FIFO: first in queue
    expect(queue).toEqual(['tc-2', 'tc-3']);

    queue = mapToolCallIdToAgentId(undefined, 'task:B', 'init', queue, map);
    expect(map.get('tc-2')).toBe('task:B');
    expect(queue).toEqual(['tc-3']);
  });

  it('sets mapping even when pending queue is empty', () => {
    const map = new Map<string, string>();
    const queue = mapToolCallIdToAgentId('tc-late', 'task:X', 'init', [], map);

    expect(map.get('tc-late')).toBe('task:X');
    expect(queue).toEqual([]);
  });

  it('skips queue drain for non-init actions but still sets mapping', () => {
    const map = new Map<string, string>();
    const queue = ['tc-1', 'tc-2'];

    const result = mapToolCallIdToAgentId('tc-resume', 'task:A', 'resume', queue, map);
    expect(map.get('tc-resume')).toBe('task:A');
    // Queue unchanged for non-init actions
    expect(result).toEqual(['tc-1', 'tc-2']);
  });

  it('does not overwrite existing mapping in FIFO fallback', () => {
    const map = new Map<string, string>();
    map.set('tc-1', 'task:ORIGINAL');
    let queue = ['tc-1'];

    queue = mapToolCallIdToAgentId(undefined, 'task:WRONG', 'init', queue, map);
    // FIFO skips because tc-1 already has a mapping
    expect(map.get('tc-1')).toBe('task:ORIGINAL');
    expect(queue).toEqual([]);
  });

  it('handles tool_call_id not in pending queue gracefully', () => {
    const map = new Map<string, string>();
    const queue = ['tc-1', 'tc-2'];

    const result = mapToolCallIdToAgentId('tc-unknown', 'task:X', 'init', queue, map);
    expect(map.get('tc-unknown')).toBe('task:X');
    // Queue unchanged since tc-unknown wasn't in it
    expect(result).toEqual(['tc-1', 'tc-2']);
  });
});
