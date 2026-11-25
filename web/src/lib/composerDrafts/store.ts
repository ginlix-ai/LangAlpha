import * as z from 'zod/mini';
import type { MentionedFile, SlashCommand } from '@/components/ui/chat-input.types';
import { registerAuthReset } from '../authResets';
import { getAuthHeaders } from '../authToken';
import { removeUserLocalItem, storageUser, subscribeStorageUser, userLocalStorage } from '../userStorage';
import type { DraftContent, DraftFileMeta } from './types';

/**
 * What composers held and did not send, kept per user on this device as one
 * localStorage value: each draft by the place its message would go
 * (`draftKeys`). It is read once and kept parsed until a write here or in
 * another tab. A draft goes when it is sent or emptied, when its thread is
 * deleted, when untouched for `DRAFT_TTL_MS`, past `MAX_DRAFTS` (the one
 * touched longest ago first), and at sign-out.
 */

const DAY = 24 * 60 * 60 * 1000;
const DRAFT_TTL_MS = 14 * DAY;
const MAX_DRAFTS = 50;
const SAVE_DELAY_MS = 300;
const SWEEP_DELAY_MS = 2000;
const STORAGE_KEY = 'composerDrafts';

export const draftKeys = {
  thread: (threadId: string) => `thread:${threadId}`,
  /** A message that will start a thread in a workspace, Home included. */
  newThread: (workspaceId: string) => `new:${workspaceId}`,
  /** The Dashboard's composer, floating or in its widget, whichever is shown. */
  dashboard: 'new:dashboard',
  /** Market View's new chat. */
  market: 'new:market',
};

interface StoredDraft extends DraftContent {
  savedAt: number;
}
type Drafts = Readonly<Record<string, StoredDraft>>;

// Anything on the origin can write this slot, so a read takes only what this
// module writes: an unreadable draft, pill or file is dropped, not the rest.
const str = z.string();
const list = z.catch(z.array(z.unknown()), []);
const quoteOrigin = z.union([
  z.object({
    kind: z.literal('file'),
    path: str,
    label: str,
    lineStart: z.optional(z.nullable(z.number())),
    lineEnd: z.optional(z.nullable(z.number())),
    lineCount: z.number(),
  }),
  z.object({ kind: z.literal('message'), speaker: str, message: z.optional(str) }),
]);
const mentionSchema = z.union([
  z.object({ kind: z.literal('file'), path: str }),
  z.object({ kind: z.literal('quote'), snippet: str, reply: z.optional(str), from: quoteOrigin, token: str }),
  z.object({ kind: z.literal('chip'), label: str, snippet: str, source: z.enum(['paste', 'chart']) }),
  z.object({ kind: z.literal('chip'), label: str, snippet: str, source: z.literal('range'), path: str, locator: str }),
]);
const commandSchema = z.object({
  type: str,
  name: str,
  skillName: z.optional(str),
  description: z.optional(str),
  aliases: z.optional(z.array(str)),
});
const fileSchema = z.object({
  id: str,
  name: str,
  type: str,
  mime: str,
  size: z.number(),
  lastModified: z.number(),
  attachmentId: z.optional(str),
  readyAt: z.optional(z.number()),
});
const draftSchema = z.object({ text: str, mentions: list, commands: list, files: list, savedAt: z.number() });
const storeSchema = z.object({ v: z.literal(2), drafts: z.record(str, z.unknown()) });

function itemsOf<T>(schema: z.ZodMiniType<T>, items: unknown[]): T[] {
  return items.flatMap((raw) => {
    const parsed = schema.safeParse(raw);
    return parsed.success ? [parsed.data] : [];
  });
}

function parseDraft(raw: unknown): StoredDraft | null {
  const parsed = draftSchema.safeParse(raw);
  if (!parsed.success) return null;
  const { text, savedAt, mentions, commands, files } = parsed.data;
  return {
    text,
    savedAt,
    mentions: itemsOf<MentionedFile>(mentionSchema, mentions),
    commands: itemsOf<SlashCommand>(commandSchema, commands),
    files: itemsOf<DraftFileMeta>(fileSchema, files),
  };
}

function parseJson(raw: string | null): unknown {
  if (!raw) return undefined;
  try { return JSON.parse(raw); } catch { return undefined; }
}

// --- The stored drafts ---

let cached: { user: string; drafts: Drafts } | null = null;

/** The signed-in user's drafts, past their time left out. */
function drafts(): Drafts {
  const user = storageUser();
  if (!user) return {};
  if (cached?.user === user) return cached.drafts;
  const parsed = storeSchema.safeParse(parseJson(userLocalStorage.getItem(STORAGE_KEY)));
  const now = Date.now();
  const found: Record<string, StoredDraft> = {};
  for (const [key, raw] of Object.entries(parsed.success ? parsed.data.drafts : {})) {
    const draft = parseDraft(raw);
    if (draft && now - draft.savedAt <= DRAFT_TTL_MS) found[key] = draft;
  }
  cached = { user, drafts: found };
  // Bytes a closed tab left behind, whose draft was sent, dropped or expired.
  scheduleSweep();
  return found;
}

/** The drafts as stored now, for a write to build on: another tab may have
 *  written since this one read, and its storage event comes later. */
function latestDrafts(): Drafts {
  cached = null;
  return drafts();
}

/** Whether the store now holds `next`. A full one refuses it. */
function store(next: Drafts): boolean {
  if (Object.keys(next).length === 0) {
    userLocalStorage.removeItem(STORAGE_KEY);
    return true;
  }
  const json = JSON.stringify({ v: 2, drafts: next });
  userLocalStorage.setItem(STORAGE_KEY, json);
  return userLocalStorage.getItem(STORAGE_KEY) === json;
}

/** Stores `next`, in which the drafts under `touched` changed, and says
 *  whether the store now holds no draft `next` left out. A refused write
 *  leaves the last one, whose older copies of those drafts would come back as
 *  current, so it is written again without them. */
function write(next: Drafts, touched: readonly string[]): boolean {
  const user = storageUser();
  if (!user) return false;
  let kept = next;
  let stored = store(kept);
  if (!stored) {
    kept = Object.fromEntries(Object.entries(next).filter(([key]) => !touched.includes(key)));
    stored = store(kept);
  }
  cached = { user, drafts: kept };
  changed();
  scheduleSweep();
  return stored;
}

const without = (all: Drafts, key: string): Drafts => Object.fromEntries(Object.entries(all).filter(([k]) => k !== key));

// --- Files ---

/** The tiles each mounted composer holds, kept whether or not a saved draft
 *  names them yet. */
const liveTiles = new Map<symbol, Set<string>>();

function liveTileIds(): Set<string> {
  const ids = new Set<string>();
  for (const tiles of liveTiles.values()) tiles.forEach((id) => ids.add(id));
  return ids;
}

/** The tiles a mounted composer holds now. */
export function holdDraftTiles(owner: symbol, ids: readonly string[]): void {
  liveTiles.set(owner, new Set(ids));
}

/** A composer that closes its draft or unmounts lets go of its files: those
 *  its draft names stay, the rest go at the next sweep. */
export function releaseDraftFiles(owner: symbol): void {
  liveTiles.delete(owner);
  scheduleSweep();
}

/** Deletes the server's copies of `files`, but for tiles still in a mounted
 *  composer, which are that composer's to keep or let go, unless `all`. */
function discardUploads(files: readonly DraftFileMeta[], all = false): void {
  const live = all ? new Set<string>() : liveTileIds();
  const ids = new Set(files.flatMap((f) => (f.attachmentId && !live.has(f.id) ? [f.attachmentId] : [])));
  if (ids.size === 0) return;
  // Loaded when needed: sign-out reaches this module from the first load.
  void import('@/pages/ChatAgent/utils/api/attachments').then(({ deleteAttachment }) => {
    for (const id of ids) void deleteAttachment(id).catch(() => {});
  }).catch(() => {});
}

let sweepTimer: ReturnType<typeof setTimeout> | null = null;

function wantedTileIds(): Set<string> {
  const ids = liveTileIds();
  for (const { draft } of pending.values()) draft?.files.forEach((f) => ids.add(f.id));
  for (const draft of Object.values(drafts())) draft.files.forEach((f) => ids.add(f.id));
  return ids;
}

/** Drops, shortly, the bytes no draft or composer still names. */
function scheduleSweep(): void {
  if (sweepTimer) return;
  sweepTimer = setTimeout(() => {
    sweepTimer = null;
    void import('./files').then((files) => files.sweepDraftFiles(wantedTileIds)).catch(() => {});
  }, SWEEP_DELAY_MS);
}

// --- Saves ---

const pending = new Map<string, { user: string; draft: DraftContent | null }>();
let saveTimer: ReturnType<typeof setTimeout> | null = null;

const isEmpty = (d: DraftContent) => !d.text.trim() && !d.mentions.length && !d.commands.length && !d.files.length;
const contentOf = (d: DraftContent) => JSON.stringify([d.text, d.mentions, d.commands, d.files]);

/** The draft under `key`, a save still waiting first. */
export function readDraft(key: string): DraftContent | null {
  const waiting = pending.get(key);
  if (waiting && waiting.user === storageUser()) return waiting.draft;
  return drafts()[key] ?? null;
}

/** Saves what a composer holds, shortly: keystrokes come faster than a write
 *  is worth. An empty draft removes the saved one. */
export function scheduleDraftSave(key: string, draft: DraftContent): void {
  const user = storageUser();
  if (!user) return;
  pending.set(key, { user, draft: isEmpty(draft) ? null : draft });
  if (!saveTimer) saveTimer = setTimeout(flushDrafts, SAVE_DELAY_MS);
}

/** Writes every save still waiting. */
export function flushDrafts(): void {
  if (saveTimer) {
    clearTimeout(saveTimer);
    saveTimer = null;
  }
  const user = storageUser();
  const batch = [...pending];
  pending.clear();
  if (!user || batch.length === 0) return;
  const next: Record<string, StoredDraft> = { ...latestDrafts() };
  const touched: string[] = [];
  for (const [key, { user: owner, draft }] of batch) {
    // Written for an account that has since left this page.
    if (owner !== user) continue;
    const prev = next[key];
    if (!draft) {
      if (prev) delete next[key];
    } else if (!prev || contentOf(prev) !== contentOf(draft)) {
      next[key] = { ...draft, savedAt: Date.now() };
    } else {
      continue;
    }
    touched.push(key);
  }
  if (touched.length === 0) return;
  // Past the cap, the drafts touched longest ago go, never one on screen; their
  // server copies only once the store no longer names them.
  const over = Object.keys(next).length - MAX_DRAFTS;
  const evicted = Object.keys(next)
    .filter((key) => !open.has(key))
    .sort((a, b) => next[a].savedAt - next[b].savedAt)
    .slice(0, Math.max(0, over))
    .map((key) => {
      const draft = next[key];
      delete next[key];
      return draft;
    });
  if (write(next, touched)) evicted.forEach((draft) => discardUploads(draft.files));
}

/** Files the draft under `to` that was under `from`: the same message, whose
 *  place has a new name (a new thread once the server names it). */
export function moveDraft(from: string, to: string): void {
  flushDrafts();
  const all = latestDrafts();
  const draft = all[from];
  if (!draft || from === to) return;
  write({ ...without(all, from), [to]: { ...draft, savedAt: Date.now() } }, [from, to]);
}

/** A deleted thread's draft has nowhere to go, nor have its files' server
 *  copies, those in its open composer too: its view goes with it. */
export function forgetThreadDraft(threadId: string): void {
  const key = draftKeys.thread(threadId);
  const waiting = pending.get(key)?.draft?.files ?? [];
  pending.delete(key);
  const all = latestDrafts();
  const draft = all[key];
  discardUploads([...waiting, ...(draft?.files ?? [])], true);
  if (draft) write(without(all, key), [key]);
}

/** What clears every draft of the signed-in user at a sign-out they asked
 *  for: what they wrote should not wait on this device for whoever uses it
 *  next. Taken while they are still signed in, with the headers that can
 *  delete the server's copies (those in open composers too, which close as the
 *  account goes), and run once the sign-out has gone through: a failed one
 *  leaves them signed in, drafts and all. */
export async function composerDraftsClearer(): Promise<() => void> {
  const user = storageUser();
  if (!user) return () => {};
  const files = [
    ...[...pending.values()].flatMap(({ draft }) => draft?.files ?? []),
    ...Object.values(latestDrafts()).flatMap((draft) => draft.files),
  ];
  const ids = [...new Set(files.flatMap((f) => (f.attachmentId ? [f.attachmentId] : [])))];
  const headers = await getAuthHeaders();
  return () => {
    pending.clear();
    cached = null;
    removeUserLocalItem(user, STORAGE_KEY);
    if (ids.length > 0) {
      // With the headers taken before: the session that could send them has gone.
      void import('@/pages/ChatAgent/utils/api/attachments').then(({ deleteAttachmentOnUnload }) => {
        for (const id of ids) deleteAttachmentOnUnload(id, headers);
      }).catch(() => {});
    }
    void import('./files').then((f) => f.clearDraftFiles(user)).catch(() => {});
    changed();
  };
}

// --- Which drafts wait, for the lists that point at them ---

/** How many mounted composers show each draft: one on screen shows it, so a
 *  list need not point at it. */
const open = new Map<string, number>();
const listeners = new Set<() => void>();

function changed(): void {
  listeners.forEach((listener) => listener());
}

/** A composer on screen shows the draft under `key` until the returned close. */
export function openDraft(key: string): () => void {
  open.set(key, (open.get(key) ?? 0) + 1);
  changed();
  return () => {
    const left = (open.get(key) ?? 1) - 1;
    if (left > 0) open.set(key, left);
    else open.delete(key);
    changed();
  };
}

export function subscribeDrafts(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** The start of the draft under `key` while no composer on screen shows it:
 *  its first line, else its files' names, else empty. Null when there is none
 *  to point at. */
export function waitingDraft(key: string): string | null {
  const draft = drafts()[key];
  if (!draft || open.has(key)) return null;
  const line = draft.text.split('\n').map((l) => l.trim()).find(Boolean);
  return (line ?? draft.files.map((f) => f.name).join(', ')).slice(0, 160);
}

// --- Page lifecycle ---

if (typeof window !== 'undefined') {
  // Another tab saved or dropped one.
  window.addEventListener('storage', (e) => {
    if (e.key === null || e.key.endsWith(STORAGE_KEY)) {
      cached = null;
      changed();
    }
  });
  // A save still waiting when the tab goes would be lost with it.
  window.addEventListener('pagehide', flushDrafts);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') flushDrafts();
  });
}

// Another user's drafts are not this one's.
subscribeStorageUser(changed);

registerAuthReset(() => {
  pending.clear();
  cached = null;
  changed();
});
