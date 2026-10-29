import { useCallback } from 'react';
import type { ProvenanceRecord, SubagentTaskRecord } from '@/types/chat';
import type { ToolCallLike } from '../../utils/fileRefResolver';
import type { ToolCallProcessRecord } from '../ToolCallDetailView';

/**
 * A message as the transcript's readers read it, and no more: this lookup,
 * the file panel's transcript store, and the write log the store collects. A
 * tool call is typed as the write log reads it; the detail view reads the
 * rest, so the lookup hands the record on as that view's type.
 */
export interface TranscriptMessage {
  id?: string;
  toolCallProcesses?: Record<string, ToolCallLike>;
  subagentTasks?: Record<string, Pick<SubagentTaskRecord, 'status'>>;
  provenanceRecords?: Record<string, ProvenanceRecord>;
}

// A spawn's record with its task's outcome on it, one copy per record and
// status: a reader comparing reads then sees a change only when there is one.
const withTaskStatus = new WeakMap<ToolCallProcessRecord, { status: string | null; proc: ToolCallProcessRecord }>();

/**
 * A tool call's record as a transcript holds it now, so a surface that shows
 * one reads the live record rather than a copy taken at click time. Newest
 * first, so a regenerated turn's copy of a call wins over the one it replaced.
 *
 * A spawn's record settles as soon as the task is dispatched, long before the
 * task it started does; the outcome the detail view reports lives on the
 * message's task record and rides along as `_subagentStatus`.
 */
function findToolCallProcess(messages: readonly TranscriptMessage[], toolCallId: string): ToolCallProcessRecord | undefined {
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    const proc = msg.toolCallProcesses?.[toolCallId] as ToolCallProcessRecord | undefined;
    if (!proc) continue;
    const task = msg.subagentTasks?.[toolCallId];
    if (!task) return proc;
    const status = task.status || null;
    const kept = withTaskStatus.get(proc);
    if (kept && kept.status === status) return kept.proc;
    const next = { ...proc, _subagentStatus: status };
    withTaskStatus.set(proc, { status, proc: next });
    return next;
  }
  return undefined;
}

export function findInTranscripts(
  messages: readonly TranscriptMessage[],
  subagentTranscripts: readonly (readonly TranscriptMessage[])[],
  toolCallId: string,
): ToolCallProcessRecord | undefined {
  const own = findToolCallProcess(messages, toolCallId);
  if (own) return own;
  for (const transcript of subagentTranscripts) {
    const proc = findToolCallProcess(transcript, toolCallId);
    if (proc) return proc;
  }
  return undefined;
}

export const NO_TRANSCRIPTS: readonly (readonly TranscriptMessage[])[] = [];

/**
 * The accessor a detail surface reads a tool call through. It is remade per
 * transcript change and read at render, so the surface shows a call's result
 * as it lands without holding a copy; a copy taken at click time would show a
 * running call forever, because the stream handlers replace a record rather
 * than mutate it. The main transcript is searched first, then each subagent's
 * own messages, so a row clicked inside a subagent transcript resolves too.
 */
export function useToolCallLookup(
  messages: readonly TranscriptMessage[],
  subagentTranscripts: readonly (readonly TranscriptMessage[])[] = NO_TRANSCRIPTS,
): (toolCallId: string) => ToolCallProcessRecord | undefined {
  return useCallback(
    (toolCallId: string) => findInTranscripts(messages, subagentTranscripts, toolCallId),
    [messages, subagentTranscripts],
  );
}
