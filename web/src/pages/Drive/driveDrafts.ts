import { registerAuthReset } from '@/lib/authResets';
import type { Content } from './driveEdit';

/** Unsaved text and the content it was written on, so a save still matches on that content's hash. */
export interface DriveDraft {
  base: Content;
  draft: string;
  /** The "What changed?" typed beside it, which goes with the draft. */
  note: string;
  /**
   * The editor sitting that wrote it. A save that lands after its editor
   * closed moves the draft on to what it saved only if that sitting still
   * owns it, never a later sitting's draft on the same version.
   */
  sitting: string;
}

/**
 * Unsaved edits kept by entry id while the Drive page is away, so leaving the
 * route (a sidebar link, the browser's back) and coming back finds the editor
 * as it was. Memory only: a reload loses them, which `beforeunload` asks about.
 */
const drafts = new Map<string, DriveDraft>();

/** Asks before the window goes; Chrome before 119 asks only when `returnValue` is set. */
export function askBeforeUnload(e: BeforeUnloadEvent): void {
  e.preventDefault();
  e.returnValue = '';
}

let asking = false;
/**
 * While any draft is kept, closing or reloading the window asks first: a
 * draft outlives the editor that wrote it (a page left, a panel tab put
 * behind another), and nothing else would ask for it then.
 */
function guardUnload(): void {
  const want = drafts.size > 0;
  if (want === asking || typeof window === 'undefined') return;
  asking = want;
  if (want) window.addEventListener('beforeunload', askBeforeUnload);
  else window.removeEventListener('beforeunload', askBeforeUnload);
}

/** Past this, base plus text in characters, a draft is not kept: the file is left as it would be without the store. */
export const DRAFT_MAX_CHARS = 4 * 1024 * 1024;

export function draftOf(entryId: string): DriveDraft | undefined {
  return drafts.get(entryId);
}

export function keepDraft(entryId: string, draft: DriveDraft): void {
  drafts.delete(entryId);
  // No bound on how many: each is text the person wrote and has not saved,
  // and the chat's panel leaves an editor without asking, so a draft dropped
  // to make room would be lost unseen.
  if (draft.base.content.length + draft.draft.length <= DRAFT_MAX_CHARS) drafts.set(entryId, draft);
  guardUnload();
}

export function dropDraft(entryId: string): void {
  drafts.delete(entryId);
  guardUnload();
}

// Another account's unsaved text is theirs.
registerAuthReset(() => {
  drafts.clear();
  guardUnload();
});
