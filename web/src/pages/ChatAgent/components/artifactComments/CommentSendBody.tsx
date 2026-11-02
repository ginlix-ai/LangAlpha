/**
 * The body of a user bubble that sent file comments: the note, drawn as the
 * bubble draws any message's words, then the comments it carried.
 */
import React from 'react';
import type { ArtifactCommentsShown } from '@/types/api';
import { ArtifactCommentList } from './ArtifactCommentList';

export function CommentSendBody({ shown, pending, refused, children }: {
  shown: ArtifactCommentsShown;
  /** The send has not reached the agent yet. */
  pending: boolean;
  /** Refused, a steer by its error and a fresh send as unadmitted: either way
   *  the server put the comments back in the queue. */
  refused: boolean;
  /** The note as the bubble draws a message's words, shimmering while pending. */
  children: React.ReactNode;
}): React.ReactElement {
  const hasNote = !!shown.note.trim();
  return (
    <>
      {hasNote && children}
      <ArtifactCommentList
        shown={shown}
        pending={pending}
        leads={!hasNote}
        refused={refused}
        className={hasNote ? 'mt-2' : undefined}
      />
    </>
  );
}
