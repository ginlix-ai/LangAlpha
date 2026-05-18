import { describe, it, expect, beforeEach } from 'vitest';
import {
  getMarketThreadId,
  setMarketThreadId,
  clearMarketThreadId,
  clearAllMarketThreadsForWorkspace,
} from '../threadPersistence';

const WS = 'ws-1';
const WS_OTHER = 'ws-2';

describe('threadPersistence', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  describe('setMarketThreadId / getMarketThreadId', () => {
    it('round-trips a thread id for a (workspace, symbol)', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-abc-123');
      expect(getMarketThreadId(WS, 'NVDA')).toBe('thread-abc-123');
    });

    it('uppercases the symbol for case-insensitive lookup', () => {
      setMarketThreadId(WS, 'nvda', 'thread-abc-123');
      expect(getMarketThreadId(WS, 'NVDA')).toBe('thread-abc-123');
      expect(getMarketThreadId(WS, 'Nvda')).toBe('thread-abc-123');
    });

    it('returns null when no entry exists for that (workspace, symbol)', () => {
      expect(getMarketThreadId(WS, 'AAPL')).toBeNull();
    });

    it('isolates threads by symbol within a workspace', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-nvda');
      setMarketThreadId(WS, 'AAPL', 'thread-aapl');
      expect(getMarketThreadId(WS, 'NVDA')).toBe('thread-nvda');
      expect(getMarketThreadId(WS, 'AAPL')).toBe('thread-aapl');
    });

    it('isolates threads across workspaces for the same symbol', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-ws1');
      setMarketThreadId(WS_OTHER, 'NVDA', 'thread-ws2');
      expect(getMarketThreadId(WS, 'NVDA')).toBe('thread-ws1');
      expect(getMarketThreadId(WS_OTHER, 'NVDA')).toBe('thread-ws2');
    });

    it('overwrites an existing entry', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-old');
      setMarketThreadId(WS, 'NVDA', 'thread-new');
      expect(getMarketThreadId(WS, 'NVDA')).toBe('thread-new');
    });
  });

  describe('setMarketThreadId with edge values', () => {
    it('removes the entry when threadId is __default__', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-real');
      setMarketThreadId(WS, 'NVDA', '__default__');
      expect(getMarketThreadId(WS, 'NVDA')).toBeNull();
    });

    it('removes the entry when threadId is null', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-real');
      setMarketThreadId(WS, 'NVDA', null);
      expect(getMarketThreadId(WS, 'NVDA')).toBeNull();
    });

    it('removes the entry when threadId is empty string', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-real');
      setMarketThreadId(WS, 'NVDA', '');
      expect(getMarketThreadId(WS, 'NVDA')).toBeNull();
    });

    it('ignores empty workspace', () => {
      setMarketThreadId('', 'NVDA', 'thread-real');
      expect(localStorage.length).toBe(0);
    });

    it('ignores null workspace', () => {
      setMarketThreadId(null, 'NVDA', 'thread-real');
      expect(localStorage.length).toBe(0);
    });

    it('ignores empty symbol', () => {
      setMarketThreadId(WS, '', 'thread-real');
      expect(localStorage.length).toBe(0);
    });
  });

  describe('getMarketThreadId edge cases', () => {
    it('returns null and clears the entry when the stored value is __default__', () => {
      const key = `marketview_thread_id_${WS}_NVDA`;
      localStorage.setItem(key, '__default__');
      expect(getMarketThreadId(WS, 'NVDA')).toBeNull();
      expect(localStorage.getItem(key)).toBeNull();
    });

    it('returns null for empty workspace', () => {
      expect(getMarketThreadId('', 'NVDA')).toBeNull();
    });

    it('returns null for null workspace', () => {
      expect(getMarketThreadId(null, 'NVDA')).toBeNull();
    });

    it('returns null for empty symbol', () => {
      expect(getMarketThreadId(WS, '')).toBeNull();
    });
  });

  describe('clearMarketThreadId', () => {
    it('removes the entry for the given (workspace, symbol)', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-real');
      clearMarketThreadId(WS, 'NVDA');
      expect(getMarketThreadId(WS, 'NVDA')).toBeNull();
    });

    it('leaves other symbols within the workspace intact', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-nvda');
      setMarketThreadId(WS, 'AAPL', 'thread-aapl');
      clearMarketThreadId(WS, 'NVDA');
      expect(getMarketThreadId(WS, 'NVDA')).toBeNull();
      expect(getMarketThreadId(WS, 'AAPL')).toBe('thread-aapl');
    });

    it('leaves the same symbol in other workspaces intact', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-ws1');
      setMarketThreadId(WS_OTHER, 'NVDA', 'thread-ws2');
      clearMarketThreadId(WS, 'NVDA');
      expect(getMarketThreadId(WS, 'NVDA')).toBeNull();
      expect(getMarketThreadId(WS_OTHER, 'NVDA')).toBe('thread-ws2');
    });
  });

  describe('clearAllMarketThreadsForWorkspace', () => {
    it('removes every symbol entry for the workspace', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-nvda');
      setMarketThreadId(WS, 'AAPL', 'thread-aapl');
      setMarketThreadId(WS_OTHER, 'NVDA', 'thread-ws2');
      clearAllMarketThreadsForWorkspace(WS);
      expect(getMarketThreadId(WS, 'NVDA')).toBeNull();
      expect(getMarketThreadId(WS, 'AAPL')).toBeNull();
      expect(getMarketThreadId(WS_OTHER, 'NVDA')).toBe('thread-ws2');
    });

    it('is a no-op for empty workspace', () => {
      setMarketThreadId(WS, 'NVDA', 'thread-nvda');
      clearAllMarketThreadsForWorkspace('');
      expect(getMarketThreadId(WS, 'NVDA')).toBe('thread-nvda');
    });
  });
});
