import { describe, it, expect } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useChatViewCache } from '../useChatViewCache';
import type { TouchParams } from '../useChatViewCache';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeParams(overrides: Partial<TouchParams> = {}): TouchParams {
  return {
    workspaceId: 'ws-1',
    threadId: 'thread-1',
    workspaceName: 'Test Workspace',
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// touch()
// ---------------------------------------------------------------------------

describe('useChatViewCache — touch()', () => {
  it('creates a new entry with correct key, instanceId, and metadata', () => {
    const { result } = renderHook(() => useChatViewCache());

    act(() => {
      result.current.touch(makeParams());
    });

    expect(result.current.entries).toHaveLength(1);
    const entry = result.current.entries[0];
    expect(entry.key).toBe('ws-1-thread-1');
    expect(entry.workspaceId).toBe('ws-1');
    expect(entry.threadId).toBe('thread-1');
    expect(entry.workspaceName).toBe('Test Workspace');
    expect(typeof entry.instanceId).toBe('number');
  });

  it('is a no-op when entry is already MRU with same metadata', () => {
    const { result } = renderHook(() => useChatViewCache());
    const params = makeParams();

    act(() => {
      result.current.touch(params);
    });

    const entriesAfterFirst = result.current.entries;

    act(() => {
      result.current.touch(params);
    });

    // Referential equality — no state update occurred
    expect(result.current.entries).toBe(entriesAfterFirst);
  });

  it('promotes a non-MRU entry to front, preserving instanceId', () => {
    const { result } = renderHook(() => useChatViewCache());

    act(() => {
      result.current.touch(makeParams({ workspaceId: 'ws-1', threadId: 'thread-1' }));
      result.current.touch(makeParams({ workspaceId: 'ws-2', threadId: 'thread-2' }));
    });

    // ws-2 is MRU, ws-1 is second
    expect(result.current.entries[0].key).toBe('ws-2-thread-2');
    expect(result.current.entries[1].key).toBe('ws-1-thread-1');
    const ws1InstanceId = result.current.entries[1].instanceId;

    act(() => {
      result.current.touch(makeParams({ workspaceId: 'ws-1', threadId: 'thread-1' }));
    });

    // ws-1 promoted to front
    expect(result.current.entries[0].key).toBe('ws-1-thread-1');
    expect(result.current.entries[0].instanceId).toBe(ws1InstanceId);
    expect(result.current.entries[1].key).toBe('ws-2-thread-2');
  });

  it('evicts the oldest entry when exceeding MAX_ENTRIES (5)', () => {
    const { result } = renderHook(() => useChatViewCache());

    // Add 5 entries
    act(() => {
      for (let i = 1; i <= 5; i++) {
        result.current.touch(makeParams({ workspaceId: `ws-${i}`, threadId: `thread-${i}` }));
      }
    });

    expect(result.current.entries).toHaveLength(5);
    // MRU order: ws-5, ws-4, ws-3, ws-2, ws-1
    expect(result.current.entries[4].key).toBe('ws-1-thread-1');

    // Add 6th entry — should evict ws-1
    act(() => {
      result.current.touch(makeParams({ workspaceId: 'ws-6', threadId: 'thread-6' }));
    });

    expect(result.current.entries).toHaveLength(5);
    expect(result.current.entries[0].key).toBe('ws-6-thread-6');
    // ws-1 (the oldest) should be gone
    expect(result.current.entries.some(e => e.key === 'ws-1-thread-1')).toBe(false);
  });

  it('updates metadata when MRU entry has changed fields', () => {
    const { result } = renderHook(() => useChatViewCache());

    act(() => {
      result.current.touch(makeParams({ workspaceName: 'Old Name' }));
    });

    const instanceId = result.current.entries[0].instanceId;

    act(() => {
      result.current.touch(makeParams({ workspaceName: 'New Name' }));
    });

    expect(result.current.entries[0].workspaceName).toBe('New Name');
    expect(result.current.entries[0].instanceId).toBe(instanceId);
  });
});

// ---------------------------------------------------------------------------
// updateKey()
// ---------------------------------------------------------------------------

describe('useChatViewCache — updateKey()', () => {
  it('renames an entry key in-place, preserving instanceId', () => {
    const { result } = renderHook(() => useChatViewCache());

    act(() => {
      result.current.touch(makeParams({ workspaceId: 'ws-1', threadId: '__default__' }));
    });

    const instanceId = result.current.entries[0].instanceId;

    act(() => {
      result.current.updateKey(
        'ws-1-__default__',
        'ws-1-real-uuid',
        { threadId: 'real-uuid' },
      );
    });

    expect(result.current.entries).toHaveLength(1);
    const entry = result.current.entries[0];
    expect(entry.key).toBe('ws-1-real-uuid');
    expect(entry.threadId).toBe('real-uuid');
    expect(entry.workspaceId).toBe('ws-1');
    expect(entry.instanceId).toBe(instanceId);
  });

  it('is a no-op when old key does not exist', () => {
    const { result } = renderHook(() => useChatViewCache());

    act(() => {
      result.current.touch(makeParams());
    });

    const entriesBefore = result.current.entries;

    act(() => {
      result.current.updateKey('nonexistent-key', 'new-key', { threadId: 'new' });
    });

    // Referential equality — no state update occurred
    expect(result.current.entries).toBe(entriesBefore);
  });
});
