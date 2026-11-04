import { useEffect, useEffectEvent, useRef } from 'react';
import { useMarkArtifactCommentRead } from '@/hooks/useArtifact';
import type { ArtifactComment } from '@/types/api';
import { latestSeq } from '@/lib/artifactCommentReads';

/**
 * An open card shows everything in it, a reply that lands while it is open
 * too, so it is read through its newest message: over its pin or in the
 * list, on the tab in front (`active`). Each read is asked once while the
 * card stays open, so one the server refuses, which the cache puts back, is
 * not asked again and again.
 */
export function useReadWhileOpen(
  artifactId: string | null,
  /** The card open over its pin or in the list, which may name a comment not read yet. */
  shownId: string | null,
  shown: ArtifactComment | null,
  active: boolean,
): void {
  const markRead = useMarkArtifactCommentRead(artifactId);
  const readId = active && shown && shown.unread > 0 ? shown.comment_id : null;
  const readSeq = readId && shown ? latestSeq(shown) : null;
  const readAsked = useRef<string | null>(null);
  useEffect(() => {
    readAsked.current = null;
  }, [shownId]);
  const askRead = useEffectEvent((commentId: string, through: number) => markRead.mutate({ commentId, through }));
  useEffect(() => {
    if (readId === null || readSeq === null || readAsked.current === `${readId}:${readSeq}`) return;
    readAsked.current = `${readId}:${readSeq}`;
    askRead(readId, readSeq);
  }, [readId, readSeq]);
}
