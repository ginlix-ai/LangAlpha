import { useSyncExternalStore } from 'react';
import { subscribeDrafts, waitingDraft } from './store';

export {
  composerDraftsClearer,
  draftKeys,
  flushDrafts,
  forgetThreadDraft,
  holdDraftTiles,
  moveDraft,
  openDraft,
  readDraft,
  releaseDraftFiles,
  scheduleDraftSave,
} from './store';
export type { DraftContent, DraftFileMeta } from './types';

/** The start of the draft under `key` while no composer on screen shows it, for
 *  a list to point at; null when there is nothing to point at. */
export function useWaitingDraft(key: string | null): string | null {
  return useSyncExternalStore(subscribeDrafts, () => (key ? waitingDraft(key) : null));
}
