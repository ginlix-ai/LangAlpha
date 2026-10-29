import { describe, it, expect } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useChangedFiles } from '../useChangedFiles';
import type { WriteEvent } from '../../../utils/fileRefResolver';
import { createTranscriptStore } from '../transcriptStore';

/** A transcript whose write log the test sets, published the way a streamed chunk is. */
function writeLog(initial: WriteEvent[]) {
  let log = initial;
  const collectWrites = () => log;
  const store = createTranscriptStore({ messages: [], collectWrites });
  return {
    reader: store.reader,
    set(next: WriteEvent[]) {
      log = next;
      act(() => store.publish({ messages: [], collectWrites }));
    },
  };
}

describe('the write log', () => {
  it('is read again only when the messages it comes from change', () => {
    let reads = 0;
    const messages: never[] = [];
    const collectWrites = () => { reads += 1; return []; };
    const store = createTranscriptStore({ messages, collectWrites });
    store.reader.writeLog();
    // A streaming subagent publishes a new source every frame around the same messages.
    store.publish({ messages, subagentTranscripts: [[]], collectWrites });
    store.reader.writeLog();
    expect(reads).toBe(1);
  });
});

describe('useChangedFiles', () => {
  it('marks a file the agent writes again after the tab read it', () => {
    const log = writeLog([]);
    const { result } = renderHook(() => useChangedFiles(log.reader));

    act(() => result.current.markRead('a.md'));
    expect(result.current.hasChanged('a.md')).toBe(false);

    log.set([{ id: 'w1', path: 'a.md' }]);
    expect(result.current.hasChanged('a.md')).toBe(true);

    // The tab re-reads: the write it saw is the newest, so the dot goes.
    act(() => result.current.markRead('a.md'));
    expect(result.current.hasChanged('a.md')).toBe(false);

    // The same file written twice: the deduped path list would not move, the log does.
    log.set([{ id: 'w2', path: 'a.md' }, { id: 'w1', path: 'a.md' }]);
    expect(result.current.hasChanged('a.md')).toBe(true);
  });

  it('leaves a file alone that was never read, or whose write was another file', () => {
    const log = writeLog([{ id: 'w1', path: 'b.md' }]);
    const { result } = renderHook(() => useChangedFiles(log.reader));
    act(() => result.current.markRead('a.md'));
    log.set([{ id: 'w2', path: 'b.md' }, { id: 'w1', path: 'b.md' }]);
    expect(result.current.hasChanged('a.md')).toBe(false);
    expect(result.current.hasChanged('b.md')).toBe(false);
  });
});
