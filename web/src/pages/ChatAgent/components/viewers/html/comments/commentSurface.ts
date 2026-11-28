import { createContext, use } from 'react';
import type { CommentEnter } from '@/lib/artifactPreferences';
import type { ArtifactComment } from '@/types/api';

/**
 * Where a comment layer is, where it differs from the file panel's: provided
 * once around everything on that surface that shows the file's comments (the
 * header's tools, the layer, its boxes and cards), so each reads it rather
 * than branch on where it is. Left out, every comment shows, every move is
 * offered, and the owner's preferences hold.
 */
export interface CommentSurface {
  /**
   * What Enter and the filled button do, in place of the owner's
   * preference: the Drive page sends at once, since no chat beside it holds
   * a queue to look at.
   */
  enter?: CommentEnter;
  /** The "To:" menu offers "Another chat…", a search over every chat a comment could go to. */
  pickAnyChat?: boolean;
  /**
   * The comments are shown, not written: an earlier version of a file shows
   * those made on it. No mode, no new comment, no draft taken up again, and
   * no move on a card (edit, reply, resolve, delete, send, queue, Send all);
   * opening one still reads it.
   */
  readOnly?: boolean;
  /** Only these of the record's comments are drawn, listed and counted. Kept stable, as it filters on every render. */
  shows?: (comment: ArtifactComment) => boolean;
  /** Where a comment the file no longer shows can be seen as it was made: an earlier version, named. */
  outdatedLink?: (comment: ArtifactComment) => { label: string; open: () => void } | null;
  /**
   * A box's picture for the agent is written with its comment, where the
   * viewer draws one. False where there is no workspace folder to keep it
   * in (the Drive), so the box goes without one.
   */
  keepsPictures?: boolean;
}

const PANEL: CommentSurface = {};

export const CommentSurfaceContext = createContext<CommentSurface>(PANEL);

export function useCommentSurface(): CommentSurface {
  return use(CommentSurfaceContext);
}
