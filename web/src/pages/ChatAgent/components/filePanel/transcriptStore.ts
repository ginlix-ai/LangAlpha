import { useLayoutEffect, useState } from 'react';
import type { ProvenanceRecord } from '@/types/chat';
import type { WriteEvent } from '../../utils/fileRefResolver';
import { findInTranscripts, NO_TRANSCRIPTS, type TranscriptMessage } from '../chatView/toolCallLookup';
import type { TranscriptReader } from './useTranscript';

/** What a transcript store reads from; published again whenever any of it changes. */
export interface TranscriptSource {
  messages: readonly TranscriptMessage[];
  subagentTranscripts?: readonly (readonly TranscriptMessage[])[];
  /** The thread's Write/Edit log off `messages`, newest first. */
  collectWrites?: ((messages: readonly TranscriptMessage[]) => WriteEvent[]) | null;
}

const NO_WRITES: readonly WriteEvent[] = [];
const NO_RECORDS: Record<string, ProvenanceRecord> = {};

/**
 * A transcript behind a reader that keeps its identity. Every read returns
 * the value it returned last until what it reads changes: records are
 * replaced rather than edited when a call or a turn's sources change, so a
 * record is the same object until then, and the merged and derived reads are
 * kept per the records they came from.
 */
export function createTranscriptStore(initial: TranscriptSource) {
  let source = initial;
  const listeners = new Set<() => void>();
  let merged: { from: readonly Record<string, ProvenanceRecord>[]; value: Record<string, ProvenanceRecord> } = { from: [], value: NO_RECORDS };
  // Kept per what collectWrites reads, not per source: a streaming subagent
  // publishes a new source every frame around the same messages.
  let writes: { messages: TranscriptSource['messages'] | null; collect: TranscriptSource['collectWrites']; value: readonly WriteEvent[] } = { messages: null, collect: null, value: NO_WRITES };

  const reader: TranscriptReader = {
    subscribe: (onChange) => {
      listeners.add(onChange);
      return () => { listeners.delete(onChange); };
    },
    toolCall: (toolCallId) => findInTranscripts(source.messages, source.subagentTranscripts ?? NO_TRANSCRIPTS, toolCallId),
    sources: (messageId) => source.messages.find((m) => m.id === messageId)?.provenanceRecords,
    allSources: () => {
      const from: Record<string, ProvenanceRecord>[] = [];
      for (const m of source.messages) {
        const recs = m.provenanceRecords;
        if (recs) from.push(recs);
      }
      if (from.length === merged.from.length && from.every((recs, i) => recs === merged.from[i])) return merged.value;
      const value: Record<string, ProvenanceRecord> = {};
      for (const recs of from) {
        // First occurrence wins: keep the earliest turn's metadata for a colliding key.
        for (const key in recs) {
          if (!(key in value)) value[key] = recs[key];
        }
      }
      merged = { from, value };
      return value;
    },
    writeLog: () => {
      if (writes.messages === source.messages && writes.collect === source.collectWrites) return writes.value;
      const next = source.collectWrites?.(source.messages) ?? NO_WRITES;
      const prev = writes.value;
      // Rebuilt from every message on each chunk, so equal content keeps the
      // old array: the dot's readers then render only for a real write.
      const same = prev.length === next.length && prev.every((w, i) => w.id === next[i].id && w.path === next[i].path);
      const value = same ? prev : next;
      writes = { messages: source.messages, collect: source.collectWrites, value };
      return value;
    },
  };

  return {
    reader,
    publish(next: TranscriptSource) {
      source = next;
      listeners.forEach((onChange) => onChange());
    },
  };
}

/**
 * The transcript as a store the file panel subscribes to. The reader is one
 * object for the life of the chat, so handing it down re-renders nothing;
 * each change is published after the commit that made it, and a component
 * reading through it renders again only when its read changed.
 */
export function useTranscriptReader(
  messages: readonly TranscriptMessage[],
  subagentTranscripts: readonly (readonly TranscriptMessage[])[] = NO_TRANSCRIPTS,
  collectWrites: TranscriptSource['collectWrites'] = null,
): TranscriptReader {
  const [store] = useState(() => createTranscriptStore({ messages, subagentTranscripts, collectWrites }));
  useLayoutEffect(() => {
    store.publish({ messages, subagentTranscripts, collectWrites });
  }, [store, messages, subagentTranscripts, collectWrites]);
  return store.reader;
}
