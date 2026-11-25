/**
 * The comment being written, kept in the browser per user so the words
 * outlive what the layer is showing: another workspace picked, the page
 * reloaded, the tab closed. One per file or chart in each workspace, keyed by
 * the one the draft is on, which is where its spot was picked; it comes back
 * when a layer next shows that workspace and that file or chart.
 */
import * as z from 'zod/mini';
import { registerAuthReset } from '@/lib/authResets';
import { getUserLocalItem, removeUserLocalItem, storageUser, userLocalStorage } from '@/lib/userStorage';
import type { ArtifactCommentAnchor, ArtifactKind } from '@/types/api';
import type { CommentTarget } from '../../../../utils/api/artifacts';
import { parseDraftSpot } from '../pageProtocol';
import type { Draft } from './boxReducer';

const PREFIX = 'commentDraft:';

/** A comment being written: its spot, its words, and where they go. */
export interface KeptDraft {
  draft: Draft;
  text: string;
  forAgent: boolean;
  /** Where it goes when not the default. */
  to: CommentTarget | null;
}

// Where and what are kept or the draft is not; how it is sent falls back to
// the composer's defaults, so the words still come back.
const stored = z.object({
  spot: z.unknown(),
  path: z.string().check(z.minLength(1), z.maxLength(4096)),
  title: z.catch(z.nullable(z.string().check(z.maxLength(1000))), null),
  // The composer's own limit: longer was never written there.
  text: z.string().check(z.minLength(1), z.maxLength(8000)),
  forAgent: z.catch(z.boolean(), true),
  to: z.catch(z.nullable(z.union([
    z.object({ threadId: z.string().check(z.minLength(1)) }),
    z.object({ newThreadIn: z.string().check(z.minLength(1)) }),
  ])), null),
});

function keyOf(workspaceId: string, kind: ArtifactKind, path: string): string {
  // The path comes last, so a colon in it cannot make two keys one.
  return `${PREFIX}${workspaceId}:${kind}:${path}`;
}

/**
 * What browser storage refused to keep for this tab (blocked site data, a
 * full quota), so a draft still outlives a workspace switch, which no longer
 * asks first, though not a reload. Only a refused write is held here:
 * otherwise storage is the copy every tab reads and writes.
 */
const refused = new Map<string, string>();
registerAuthReset(() => refused.clear());

const tabKey = (user: string, key: string) => `${user}\u0000${key}`;

/** `user`'s stored entry under `key`, whoever is signed in now. */
function storedEntry(user: string, key: string): string | null {
  return user === storageUser() ? userLocalStorage.getItem(key) : getUserLocalItem(user, key);
}

function unstore(user: string, key: string): void {
  if (user === storageUser()) userLocalStorage.removeItem(key);
  else removeUserLocalItem(user, key);
}

/** The draft kept for `path` in the workspace, or null where none is, or what is there is not one. */
export function readDraft(workspaceId: string, kind: ArtifactKind, path: string): KeptDraft | null {
  try {
    const user = storageUser();
    if (!user) return null;
    const key = keyOf(workspaceId, kind, path);
    return parseKept(refused.get(tabKey(user, key)) ?? storedEntry(user, key), path);
  } catch {
    return null;
  }
}

function parseKept(raw: string | null, path: string): KeptDraft | null {
  if (!raw) return null;
  const read = stored.safeParse(JSON.parse(raw));
  if (!read.success || read.data.path !== path) return null;
  const spot = parseDraftSpot(read.data.spot);
  if (!spot) return null;
  const { title, text, forAgent, to } = read.data;
  return { draft: { anchor: spot.anchor, rect: spot.rect, path, title }, text, forAgent, to };
}

/**
 * Keeps the draft under the file or chart it is on, and returns the entry
 * written, for `dropDraft`. Words picked in comment mode are not marked as
 * such: the selection they were in is gone by the time the draft comes back.
 */
export function keepDraft(workspaceId: string, kind: ArtifactKind, { draft, text, forAgent, to }: KeptDraft): string | null {
  const user = storageUser();
  if (!user) return null;
  const key = keyOf(workspaceId, kind, draft.path);
  const value = { spot: { anchor: draft.anchor, rect: draft.rect }, path: draft.path, title: draft.title, text, forAgent, to };
  const raw = JSON.stringify(value);
  userLocalStorage.setItem(key, raw);
  if (userLocalStorage.getItem(key) === raw) refused.delete(tabKey(user, key));
  else refused.set(tabKey(user, key), raw);
  return raw;
}

/**
 * Lets go of the draft this tab kept for `path`, while what is kept there is
 * still `kept`, the entry it last wrote: words another tab wrote there since,
 * a send of this one still out, are theirs and stay.
 */
export function dropDraft(workspaceId: string, kind: ArtifactKind, path: string, kept: string): void {
  const user = storageUser();
  if (!user) return;
  const key = keyOf(workspaceId, kind, path);
  if (refused.get(tabKey(user, key)) === kept) refused.delete(tabKey(user, key));
  if (storedEntry(user, key) === kept) unstore(user, key);
}

/** A draft as it was sent: whose it was, where it was kept, its spot and its words. */
export interface SentDraft {
  user: string | null;
  workspaceId: string;
  kind: ArtifactKind;
  path: string;
  anchor: ArtifactCommentAnchor;
  text: string;
}

/** Whether a sent spot is the kept one, both read through the spot schema so
 *  that key order alone never tells them apart. */
function sameSpot(kept: KeptDraft['draft'], anchor: ArtifactCommentAnchor): boolean {
  const sent = parseDraftSpot({ anchor, rect: kept.rect });
  return sent !== null && JSON.stringify(sent.anchor) === JSON.stringify(kept.anchor);
}

/**
 * Lets go of a sent draft where it was kept, once the send comes back, and
 * only while what is kept there is still it: the same spot and words. One
 * written there since (the draft taken up again meanwhile, here or in another
 * tab, and edited or moved) is a new draft and stays. The sender's copies are
 * the ones checked, each on its own, whoever is signed in by then.
 */
export function dropSentDraft({ user, workspaceId, kind, path, anchor, text }: SentDraft): void {
  if (!user) return;
  const key = keyOf(workspaceId, kind, path);
  const isSent = (raw: string | null | undefined) => {
    try {
      const kept = parseKept(raw ?? null, path);
      return !!kept && kept.text.trim() === text.trim() && sameSpot(kept.draft, anchor);
    } catch {
      return false;
    }
  };
  if (isSent(refused.get(tabKey(user, key)))) refused.delete(tabKey(user, key));
  if (isSent(storedEntry(user, key))) unstore(user, key);
}
