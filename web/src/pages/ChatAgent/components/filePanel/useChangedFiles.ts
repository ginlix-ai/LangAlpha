import { useCallback, useState } from 'react';
import type { WriteEvent } from '../../utils/fileRefResolver';
import { useTranscriptRead, type TranscriptReader } from './useTranscript';

const NO_WRITES: readonly WriteEvent[] = [];
const NO_MARKS: ReadonlyMap<string, string | null> = new Map();

/** The newest write of one path in a newest-first log, or null for none. */
const newestWriteOf = (log: readonly WriteEvent[], path: string) => log.find((w) => w.path === path)?.id ?? null;

const readWriteLog = (reader: TranscriptReader) => reader.writeLog();

/**
 * Which open files the agent has rewritten since the tab showing them last
 * read its bytes: the amber dot on a tab.
 *
 * The signal is the thread's own Write/Edit log, newest first, with every
 * write in it. A tab records the id of its file's newest write when it reads
 * the bytes; a different id at the front later is a write that happened
 * since. The id, not a count: the log a lookup uses names each file once, so
 * the second write of a file that was already newest changes nothing in it.
 * A panel with no transcript (a share, the gallery) simply never marks
 * anything, which is correct: nothing is writing.
 *
 * The log is subscribed to, so a write marks its tab as it lands even while
 * nothing else about the panel changes.
 */
export function useChangedFiles(transcript?: TranscriptReader | null) {
  const writes = useTranscriptRead(transcript, readWriteLog) ?? NO_WRITES;
  // Per path, the newest write it had when its tab last read it (null: none
  // yet). Replaced rather than edited, so `hasChanged` is a new function
  // whenever a mark moves and a strip that caches on it shows the dot go.
  const [marks, setMarks] = useState(NO_MARKS);

  /** This path's bytes are now in hand; writes after this moment are changes. */
  const markRead = useCallback((path: string) => {
    const mark = newestWriteOf(transcript?.writeLog() ?? NO_WRITES, path);
    setMarks((prev) => (prev.has(path) && prev.get(path) === mark ? prev : new Map(prev).set(path, mark)));
  }, [transcript]);

  const forget = useCallback((path: string) => {
    setMarks((prev) => {
      if (!prev.has(path)) return prev;
      const next = new Map(prev);
      next.delete(path);
      return next;
    });
  }, []);

  const hasChanged = useCallback((path: string) => {
    const mark = marks.get(path);
    if (mark === undefined) return false;
    const newest = newestWriteOf(writes, path);
    // A write that has scrolled off the capped log is unknowable, not a change.
    return newest !== null && newest !== mark;
  }, [marks, writes]);

  return { markRead, forget, hasChanged };
}
