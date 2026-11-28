import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { useArtifactDestinations, useArtifactRecord, useDestinationThread } from '@/hooks/useArtifact';
import { queryKeys } from '@/lib/queryKeys';
import type { ArtifactTarget, DestinationThread } from '@/types/api';
import type { CommentTarget } from '../../utils/api/artifacts';
import { useArtifactCommentSender } from './sendContext';
import {
  commentDestinations, targetKey, threadLabel, threadOf,
  type CommentDestinations, type CommentIntent, type CommentRoute, type DestinationChoice,
} from './destinations';

/**
 * Where the comments on `target` (as `targetFor` keys it) go, beside the chat
 * this view sits next to, if any. Every control that sends a comment reads it
 * here (`commentDestinations`).
 */
export function useCommentDestinations(
  workspaceId: string | null | undefined,
  target: ArtifactTarget | null,
): CommentDestinations & {
  /** The record the destinations were read for; null until it is read. */
  artifactId: string | null;
  /** The read failed with nothing to show: the "To:" line says so, and this tries again. Null otherwise. */
  retry: (() => void) | null;
} {
  const { t } = useTranslation();
  const { beside } = useArtifactCommentSender();
  const record = useArtifactRecord(workspaceId, target);
  const artifactId = record.data?.artifact_id ?? null;
  const read = useArtifactDestinations(artifactId, threadOf(beside));
  const { data } = read;
  const shown = useMemo(() => commentDestinations(t, data, beside), [t, data, beside]);
  // The record is read first: a failure there leaves the destinations unasked.
  const failed = !data && (read.isError || (record.isError && !record.data));
  const again = record.isError && !record.data ? record.refetch : read.refetch;
  return useMemo(
    () => ({ ...shown, artifactId, retry: failed ? () => void again() : null }),
    [shown, artifactId, failed, again],
  );
}

/** How the "To:" line of a comment being written stands. */
export type DestinationStatus =
  /** The chats are still being read: nothing to name yet. */
  | { kind: 'loading' }
  /** They, or the chat picked outside the menu, could not be read: the line says so, and `retry` tries again. */
  | { kind: 'failed'; retry: () => void }
  /** A chat picked outside the menu is being checked: the line waits rather than naming a default it may not be. */
  | { kind: 'checking' }
  /** The key of the choice the comment goes to. */
  | { kind: 'ready'; selected: string };

export interface DraftDestination {
  /** The menu: the server's choices, then the chat picked outside them while it still takes the comments. */
  choices: DestinationChoice[];
  status: DestinationStatus;
  /** Where the words go for `intent`; null while that is not known, except a note, which needs no chat. */
  draftRoute: (intent: CommentIntent) => CommentRoute | null;
  onPick: (choice: DestinationChoice) => void;
  /** Binding the file to the thread beside would change where its comments go. */
  canBind: boolean;
  /** "Another chat…": a search over every chat the record's comments could go to; null where not offered. */
  search: { artifactId: string; onPick: (thread: DestinationThread) => void } | null;
}

/**
 * Where a comment being written on `target` goes: the default, or the chat
 * the owner picked (`to`, held with the draft; `onTo` null for the default).
 *
 * A chat picked outside the server's choices (found through "Another
 * chat…", `searchable`, or kept with the draft from an earlier visit) counts
 * while the server says it still takes the comments, and until it has said,
 * the draft goes nowhere rather than to the default; nor does it while the
 * server could not be asked, since only its answer that the chat takes none
 * moves the draft to the default. A chat found by the
 * search is one the server has just said takes them, so its answer is put
 * where that check reads it, and found and kept are one path.
 */
export function useDraftDestination(
  workspaceId: string,
  target: ArtifactTarget,
  { to, onTo, searchable }: { to: CommentTarget | null; onTo: (to: CommentTarget | null) => void; searchable: boolean },
): DraftDestination {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const beside = threadOf(useArtifactCommentSender().beside);
  const read = useCommentDestinations(workspaceId, target);
  const { artifactId } = read;
  const offers = (choices: readonly DestinationChoice[], pick: CommentTarget) => choices.some((c) => c.key === targetKey(pick));
  const outside = searchable && to && 'threadId' in to && read.choices.length > 0 && !offers(read.choices, to) ? to.threadId : null;
  const check = useDestinationThread(artifactId, outside);
  const checking = outside !== null && check.data === undefined && !check.isError;
  const checkFailed = outside !== null && check.data === undefined && check.isError;
  const choices = outside && check.data
    ? [...read.choices, { key: targetKey({ threadId: outside }), target: { threadId: outside }, label: threadLabel(t, check.data, beside), isDefault: false }]
    : read.choices;
  // A pick counts while the menu offers it. One that no longer takes
  // comments (the chat beside it since moved on, or its thread archived)
  // goes nowhere the "To:" line does not name: the default instead.
  const offered = to && offers(choices, to) ? to : null;
  const picked = offered ?? read.fallback;

  const status: DestinationStatus = choices.length === 0
    ? (read.retry ? { kind: 'failed', retry: read.retry } : { kind: 'loading' })
    : checkFailed
      ? { kind: 'failed', retry: () => void check.refetch() }
      : checking
        ? { kind: 'checking' }
        : { kind: 'ready', selected: targetKey(picked ?? choices[0].target) };

  const pickFound = (thread: DestinationThread) => {
    const found = { threadId: thread.thread_id };
    const known = read.choices.find((c) => c.key === targetKey(found));
    if (!known && artifactId) queryClient.setQueryData(queryKeys.artifacts.destinationThread(artifactId, thread.thread_id), thread);
    onTo(known?.isDefault ? null : found);
  };

  return {
    choices,
    status,
    draftRoute: (intent) => ((checking || checkFailed) && intent !== 'note' ? null : read.targetFor(null, intent, offered)),
    onPick: (choice) => onTo(choice.isDefault ? null : choice.target),
    canBind: read.canBind,
    search: searchable && artifactId ? { artifactId, onPick: pickFound } : null,
  };
}
