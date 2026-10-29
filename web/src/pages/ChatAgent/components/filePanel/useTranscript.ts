import { useCallback, useRef, useSyncExternalStore } from 'react';
import type { ProvenanceRecord } from '@/types/chat';
import type { WriteEvent } from '../../utils/fileRefResolver';
import type { ToolCallProcessRecord } from '../ToolCallDetailView';

/**
 * The chat's transcript as the panel reads it. The transcript changes on
 * every streamed chunk, so the panel subscribes to it instead of taking it as
 * a prop: each read returns the same value until what it reads changes, and
 * only a component whose read changed renders again.
 */
export interface TranscriptReader {
  subscribe: (onChange: () => void) => () => void;
  /** A tool call's live record: the main transcript first, then each subagent's. */
  toolCall: (toolCallId: string) => ToolCallProcessRecord | undefined;
  /** One turn's provenance. */
  sources: (messageId: string) => Record<string, ProvenanceRecord> | undefined;
  /** Every turn's provenance merged in order, the earliest turn winning a shared key. */
  allSources: () => Record<string, ProvenanceRecord>;
  /** Every Write/Edit in the thread, newest first. */
  writeLog: () => readonly WriteEvent[];
}

const noSubscribe = () => () => {};

/**
 * One read off the transcript, as render input. `read` has to return the same
 * value while the transcript holds the same thing, which the reader's own
 * reads do.
 */
export function useTranscriptRead<T>(
  transcript: TranscriptReader | null | undefined,
  read: (reader: TranscriptReader) => T,
): T | undefined {
  return useSyncExternalStore(transcript?.subscribe ?? noSubscribe, () => (transcript ? read(transcript) : undefined));
}

/**
 * One read per key, as an array that keeps its identity until one of the
 * reads changes: a chunk that leaves every key's value alone renders nothing.
 */
export function useTranscriptReads<K, T>(
  transcript: TranscriptReader | null | undefined,
  keys: readonly K[],
  read: (reader: TranscriptReader, key: K) => T,
): readonly (T | undefined)[] {
  const last = useRef<readonly (T | undefined)[]>([]);
  const snapshot = useCallback(() => {
    const next = keys.map((key) => (transcript ? read(transcript, key) : undefined));
    const prev = last.current;
    if (prev.length === next.length && prev.every((value, i) => value === next[i])) return prev;
    last.current = next;
    return next;
  }, [transcript, keys, read]);
  return useSyncExternalStore(transcript?.subscribe ?? noSubscribe, snapshot);
}
