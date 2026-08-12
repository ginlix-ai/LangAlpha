import { describe, it, expect } from 'vitest';
import { isTaskAgentId, taskIdFromAgentId } from '../agentId';

describe('taskIdFromAgentId', () => {
  it('reads the task id out of a subagent lane id', () => {
    expect(taskIdFromAgentId('task:k7Xm2p')).toBe('k7Xm2p');
  });

  it('returns null for a lane that names no task', () => {
    expect(taskIdFromAgentId('main')).toBeNull();
    expect(taskIdFromAgentId('')).toBeNull();
    expect(taskIdFromAgentId(undefined)).toBeNull();
  });

  it('only strips an anchored prefix', () => {
    expect(taskIdFromAgentId('not-a-task:k7Xm2p')).toBeNull();
    expect(taskIdFromAgentId('task:task:k7Xm2p')).toBe('task:k7Xm2p');
  });

  it('distinguishes a prefixed-but-empty id from a non-task id', () => {
    // Callers separate these: an empty task id short-circuits a fetch, while
    // null falls back to the id itself. Collapsing both to null would send
    // the literal `task:` to the ledger endpoint.
    expect(taskIdFromAgentId('task:')).toBe('');
  });
});

describe('isTaskAgentId', () => {
  it('accepts a subagent lane id and rejects the root lane', () => {
    expect(isTaskAgentId('task:k7Xm2p')).toBe(true);
    expect(isTaskAgentId('main')).toBe(false);
  });

  it('rejects non-string attributions rather than throwing', () => {
    // Callers pass raw wire fields (`event.agent`), which are untyped.
    expect(isTaskAgentId(undefined)).toBe(false);
    expect(isTaskAgentId(null)).toBe(false);
    expect(isTaskAgentId('')).toBe(false);
    expect(isTaskAgentId(42)).toBe(false);
    expect(isTaskAgentId({ agent: 'task:x' })).toBe(false);
  });
});
