import { registerAuthReset } from '@/lib/authResets';
import type { Content } from './driveEdit';

/** Unsaved text and the content it was written on, so a save still matches on that content's hash. */
export interface DriveDraft {
  base: Content;
  draft: string;
}

/**
 * Unsaved edits kept by entry id while the Drive page is away, so leaving the
 * route (a sidebar link, the browser's back) and coming back finds the editor
 * as it was. Memory only: a reload loses them, which `beforeunload` asks about.
 */
const drafts = new Map<string, DriveDraft>();

/** Past this, base plus text in characters, a draft is not kept: the file is left as it would be without the store. */
export const DRAFT_MAX_CHARS = 4 * 1024 * 1024;
/** Drafts kept at most; the one written longest ago goes first. */
export const DRAFTS_MAX = 20;

export function draftOf(entryId: string): DriveDraft | undefined {
  return drafts.get(entryId);
}

export function keepDraft(entryId: string, draft: DriveDraft): void {
  drafts.delete(entryId);
  if (draft.base.content.length + draft.draft.length > DRAFT_MAX_CHARS) return;
  drafts.set(entryId, draft);
  for (const oldest of drafts.keys()) {
    if (drafts.size <= DRAFTS_MAX) break;
    drafts.delete(oldest);
  }
}

export function dropDraft(entryId: string): void {
  drafts.delete(entryId);
}

// Another account's unsaved text is theirs.
registerAuthReset(() => drafts.clear());
