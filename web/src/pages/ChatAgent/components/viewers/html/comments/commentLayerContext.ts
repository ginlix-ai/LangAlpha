import { createContext, useContext } from 'react';
import type { ArtifactComment } from '@/types/api';
import { describeComment, type CommentLook } from '../../../artifactComments/commentModel';
import type { CommentDestinations } from '../../../artifactComments/destinations';
import type { CommentActions } from './useCommentActions';

/** What every pin and card on one comment layer shares (`useLayerSending`). */
export interface CommentLayerValue {
  actions: CommentActions;
  /** The chat beside the file, where a comment's thread goes without saying; null in the standalone view. */
  besideThreadId: string | null;
  /** Where the file's comments go, which every card's moves are decided by. */
  destinations: CommentDestinations;
  /** A turn is running on the thread the comment went to. */
  runningFor: (comment: ArtifactComment) => boolean;
}

export const CommentLayerContext = createContext<CommentLayerValue | null>(null);

export function useCommentLayer(): CommentLayerValue {
  const layer = useContext(CommentLayerContext);
  if (!layer) throw new Error('A comment pin or card is drawn outside its layer.');
  return layer;
}

/** How a comment reads on this layer, its send from here and the turn on its thread included. */
export function useCommentLook(comment: ArtifactComment): CommentLook {
  const { actions, runningFor } = useCommentLayer();
  return describeComment(comment, actions.sendState(comment), runningFor(comment));
}
