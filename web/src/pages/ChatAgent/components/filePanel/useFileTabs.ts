import { useCallback, useEffect, useMemo, useState } from 'react';
import * as z from 'zod/mini';
import { INTERVALS } from '@/lib/bars/chartConstants';
import { readTypedTicker } from '@/lib/marketUtils';
import { userLocalStorage } from '@/lib/userStorage';
import type { FileLocation } from '../../utils/fileLocation';
import { isValidUuid } from '../../utils/uuid';
import { freshStrips, tabsStorageKey, threadTabsStorageKey } from './freshStrips';
import { drivePathKey } from '@/pages/Drive/drivePaths';
import type { DriveLocation } from '@/lib/driveLinks';
import { PREVIEW_PORT_MAX, PREVIEW_PORT_MIN, type ChartTabSpec, type DriveTabSpec, type PreviewSpec, type ToolTabSpec } from './types';

/**
 * What one tab shows. The empty tab is the panel's landing surface and its
 * upload drop target.
 *
 * A file tab's `preview` is the VS Code rule: a single click in the tree lands
 * in the one italic tab, so browsing never piles up tabs, and anything that
 * says the file is being worked with (a double click, entering edit mode,
 * adding it to the context) pins it. It is unrelated to `kind: 'preview'`,
 * which is a running app served from the sandbox.
 *
 * A tool or sources tab is on loan the same way, each kind in a slot of its
 * own: clicking through a turn's tool rows retargets one tool tab rather than
 * opening one per row, and never takes the file being browsed. Both point into
 * the transcript and are never stored: each carries an id and reads the live
 * record at render.
 *
 * A Drive tab is a place in the person's Drive (a folder, the memo list, the
 * trash, a file), opened from a link in the chat or the new tab's list, and
 * browsed in place like a browser tab, each move a step back can undo. It is
 * on loan like a file tab, in a slot of its own, and kept once the reader
 * moves around in it or edits. A memo's record or changes, or a past version,
 * is where the reader is in the file, not what it is, so a reload opens the
 * file's document again.
 *
 * Settings, memory, memo, status and browser are each the one tab of their
 * kind, and carry nothing: the body is the store itself, the live watch, or the
 * computer's browser. All but status are kept across reloads like a file; status
 * depends on a watch that may be over by then, so it is not. A browser tab that
 * comes back only looks: it starts nothing.
 */
type TabBody =
  | { kind: 'empty' }
  | {
    kind: 'file';
    path: string;
    preview: boolean;
    /** Where a reference pointed inside this file, kept so switching back returns there. */
    location: FileLocation | null;
    /** Bumped whenever a location arrives, so asking for the same line twice replays the highlight. */
    locationSeq: number;
  }
  | { kind: 'settings' }
  | { kind: 'memory' }
  | { kind: 'memo' }
  | { kind: 'status' }
  | { kind: 'browser' }
  | {
    kind: 'preview';
    /** The sandbox port the dev server listens on. Identifies the tab. */
    port: number;
    /** What the agent called the app, when it said. */
    title?: string;
    /** A path suffix on the served app, e.g. `/timeline.html`. */
    previewPath?: string;
    /** How the agent started the server, so a restored tab can restart an idle port. */
    command?: string;
  }
  | { kind: 'chart'; symbol: string; timeframe: string; workspaceId?: string }
  | ({ kind: 'drive'; preview: boolean } & DriveTabSpec)
  | ({ kind: 'tool'; preview: boolean } & ToolTabSpec)
  | { kind: 'sources'; preview: boolean; messageId: string };

export type FileTab = { id: string } & TabBody;

/**
 * What a tab showed before something took its place, and what it showed
 * after the reader stepped back from it: a browser tab's history, kept per
 * tab. Only an open that lands in an existing tab adds a step (the on-loan
 * tab as the tree is browsed, the empty tab a file fills, a chart moved to
 * another ticker); switching tabs is not one.
 */
interface TabHistory {
  back: TabBody[];
  forward: TabBody[];
}

export type HistoryDirection = 'back' | 'forward';

/** Past this a tab forgets its oldest steps; nobody clicks back fifty times. */
const MAX_HISTORY = 50;

/** The kinds that live only as long as the transcript, or the watch, they point into. */
const EPHEMERAL_KINDS = ['tool', 'sources', 'status'] as const satisfies readonly FileTab['kind'][];
type EphemeralKind = (typeof EPHEMERAL_KINDS)[number];
type PersistableTab = Exclude<FileTab, { kind: 'empty' | EphemeralKind }>;

/** The kinds with one tab each, reopened by focusing it. */
type SingletonKind = 'settings' | 'memory' | 'memo' | 'status' | 'browser';

/** A tab the file tree can sit beside: the listing belongs to files and the empty tab, not to a chart, a tool result or an app. */
export function isListingTab(tab: FileTab): boolean {
  return tab.kind === 'file' || tab.kind === 'empty';
}

/** A tab that the next open of its kind may take over. */
export function isOnLoan(tab: FileTab): tab is Extract<FileTab, { preview: boolean }> {
  return (tab.kind === 'file' || tab.kind === 'drive' || tab.kind === 'tool' || tab.kind === 'sources') && tab.preview;
}

/** `tab` kept: a tab on loan taken off it, any other as it is. */
function kept(tab: FileTab): FileTab {
  return isOnLoan(tab) ? { ...tab, preview: false } : tab;
}

export interface OpenFileOptions {
  /** Open as a kept tab rather than the reused preview one. */
  pin?: boolean;
  location?: FileLocation | null;
}

interface TabsState {
  tabs: FileTab[];
  activeId: string;
  /**
   * The workspace and thread this strip belongs to. A thread switch queues the
   * new strip and re-runs the persist effect in the same flush, still holding
   * the old strip but already seeing the new key; without this the outgoing
   * thread's tabs would be written over the incoming thread's saved strip.
   */
  key: string | null;
  /**
   * The fresh chat's strip (`freshStrips`) this thread's strip was taken
   * from, dropped from memory once this one is saved under the thread.
   */
  handedFrom?: string;
  /** Kept for the life of the mount, never stored: a reload is a fresh start. */
  history?: Readonly<Record<string, TabHistory>>;
}

/** A strip before it is tied to a key. */
type StripBody = Omit<TabsState, 'key'>;

const DEFAULT_TIMEFRAME = '1day';

/** The intervals the chart toolbar can actually show and request bars for. */
const INTERVAL_KEYS = new Set(INTERVALS.map(({ key }) => key));

// The stored strip is names only: a file tab is its path, a preview tab its
// port (the signed URL it runs on is short-lived and never stored), a chart
// its symbol. Unknown keys are dropped, so entries written before a field
// existed still load. A kind with no schema here (tool, sources, status) is
// never written and, should one turn up, dropped on read: its body is a
// transcript record that a reload replays under a new identity, or a watch
// that has ended.
// One schema per kind, looked up by the entry's own `kind`, rather than a
// discriminated union: the union is the one zod feature nothing on the first
// load uses, and pulling it in charges the entry chunk for a lazy panel.
const persistedTabSchemas = {
  file: z.object({
    kind: z.literal('file'),
    path: z.string().check(z.minLength(1)),
    preview: z.catch(z.boolean(), false),
  }),
  settings: z.object({ kind: z.literal('settings') }),
  memory: z.object({ kind: z.literal('memory') }),
  memo: z.object({ kind: z.literal('memo') }),
  browser: z.object({ kind: z.literal('browser') }),
  drive: z.object({
    kind: z.literal('drive'),
    loc: z.catch(z.object({
      space: z.catch(z.optional(z.nullable(z.string())), undefined),
      file: z.catch(z.optional(z.nullable(z.string())), undefined),
      dir: z.catch(z.optional(z.nullable(z.string())), undefined),
      memos: z.catch(z.optional(z.boolean()), undefined),
      trash: z.catch(z.optional(z.boolean()), undefined),
    }), {}),
    entryId: z.catch(z.optional(z.string()), undefined),
    preview: z.catch(z.boolean(), false),
  }),
  preview: z.object({
    kind: z.literal('preview'),
    // Outside the served range the tab would restore as a permanent error
    // card rather than an app, so it is dropped with the entry.
    port: z.int().check(z.gte(PREVIEW_PORT_MIN), z.lte(PREVIEW_PORT_MAX)),
    title: z.optional(z.string()),
    previewPath: z.optional(z.string()),
    command: z.optional(z.string()),
  }),
  chart: z.object({
    kind: z.literal('chart'),
    // A tab whose symbol is not a ticker is dropped like any other entry this
    // build cannot read; a blank one would open every request on a blank name.
    symbol: z.pipe(
      z.string(),
      z.transform((v, ctx) => {
        const ticker = readTypedTicker(v);
        if (!ticker) { ctx.issues.push({ code: 'custom', message: 'not a ticker', input: v }); return z.NEVER; }
        return ticker;
      }),
    ),
    // An interval this build does not carry has no toolbar label and asks the
    // bars endpoint for a bucket it does not serve, so a restored tab would
    // come back blank rather than on the daily view.
    timeframe: z.catch(z.string().check(z.refine((v) => INTERVAL_KEYS.has(v))), DEFAULT_TIMEFRAME),
    // A workspace gone or never an id would be asked for on every open.
    workspaceId: z.catch(z.optional(z.string().check(z.refine(isValidUuid))), undefined),
  }),
};
type PersistedTabKind = keyof typeof persistedTabSchemas;
type PersistedTab = { [K in PersistedTabKind]: z.infer<(typeof persistedTabSchemas)[K]> }[PersistedTabKind];

function parsePersistedTab(entry: unknown): PersistedTab | null {
  const kind = typeof entry === 'object' && entry !== null ? (entry as { kind?: unknown }).kind : undefined;
  if (typeof kind !== 'string' || !Object.hasOwn(persistedTabSchemas, kind)) return null;
  const parsed = persistedTabSchemas[kind as PersistedTabKind].safeParse(entry);
  return parsed.success ? parsed.data : null;
}

/** More tabs than fit any strip is a corrupt or hostile store; the excess is dropped, not fatal. */
const MAX_PERSISTED_TABS = 64;

const persistedStateSchema = z.object({
  tabs: z.pipe(z.array(z.unknown()), z.transform((tabs) => tabs.slice(0, MAX_PERSISTED_TABS))),
  // -1 is the empty tab, which is not stored as an entry.
  active: z.catch(z.int(), 0),
});

export { tabsStorageKey, threadTabsStorageKey };

let nextId = 0;
const newId = () => `t${++nextId}`;

/** A broad index, for a chart opened before any symbol has been asked for. */
export const DEFAULT_CHART_SYMBOL = 'SPY';

/** Per workspace, like the strip: the last chart looked at is about that workspace's companies. */
export function lastChartStorageKey(workspaceId: string): string {
  return `filePanel.lastChart.${workspaceId}`;
}

/**
 * What a chart opened with no symbol shows: the last one looked at in this
 * workspace, else the default. Charts are switched in place, so the first
 * frame only has to be something worth looking at while the ticker is typed.
 * A strip that is not stored reads nothing, the same as it writes nothing.
 */
export function lastChartSymbol(workspaceId: string, { persist = true }: { persist?: boolean } = {}): string {
  if (!persist || !workspaceId) return DEFAULT_CHART_SYMBOL;
  try {
    return readTypedTicker(userLocalStorage.getItem(lastChartStorageKey(workspaceId)) ?? '') ?? DEFAULT_CHART_SYMBOL;
  } catch {
    return DEFAULT_CHART_SYMBOL;
  }
}

function rememberChartSymbol(workspaceId: string, symbol: string): void {
  if (!workspaceId) return;
  try { userLocalStorage.setItem(lastChartStorageKey(workspaceId), symbol); } catch { /* not worth failing over */ }
}

/**
 * The ticker as a tab knows it, or null for anything the persisted schema
 * would drop on reload, so a tab and its restored copy never disagree.
 */
function chartTicker(symbol: string): string | null {
  return readTypedTicker(symbol);
}

function emptyTab(): FileTab {
  return { id: newId(), kind: 'empty' };
}

function blankStrip(): StripBody {
  const tab = emptyTab();
  return { tabs: [tab], activeId: tab.id };
}

/** Strips written before tabs carried a `kind` held files only. */
function withKind(raw: unknown): unknown {
  return raw && typeof raw === 'object' && !('kind' in raw) ? { ...raw, kind: 'file' } : raw;
}

function revive(t: PersistedTab): FileTab {
  return t.kind === 'file' ? { id: newId(), ...t, location: null, locationSeq: 0 } : { id: newId(), ...t };
}

function toPersisted({ id: _id, ...body }: PersistableTab): PersistedTab {
  if (body.kind === 'drive') {
    const { tab: _tab, version: _version, against: _against, ask: _ask, ...place } = body.loc;
    return { ...body, loc: place };
  }
  if (body.kind !== 'file') return body;
  const { location: _location, locationSeq: _seq, ...file } = body;
  return file;
}

function serialize({ tabs, activeId }: StripBody): string {
  const named = tabs.filter((t): t is PersistableTab => t.kind !== 'empty' && !(EPHEMERAL_KINDS as readonly string[]).includes(t.kind));
  // -1 when the empty tab is active, so the strip comes back parked on it.
  // A tab that is not stored comes back on the stored one before it, else
  // the first, else parked: what it showed is gone with the transcript.
  let active = named.findIndex((t) => t.id === activeId);
  if (active < 0 && named.length && !tabs.some((t) => t.id === activeId && t.kind === 'empty')) {
    const at = tabs.findIndex((t) => t.id === activeId);
    active = Math.max(0, named.filter((t) => tabs.indexOf(t) < at).length - 1);
  }
  return JSON.stringify({ tabs: named.map(toPersisted), active });
}

/**
 * Save a strip. One handed on from a chat's held strip leaves that copy in
 * place, kept current, until its own reads back from storage, which refuses
 * quietly: a reopen then still finds the latest strip.
 */
function saveStrip(store: string, strip: TabsState, fresh: boolean): void {
  const json = serialize(strip);
  const saved = writeStrip(store, json, fresh);
  if (!strip.handedFrom) return;
  // Once saved, the thread's copy is the strip, and a later refusal leaves it
  // as any thread's is left.
  if (saved) freshStrips.delete(strip.handedFrom);
  else if (freshStrips.has(strip.handedFrom)) freshStrips.set(strip.handedFrom, json);
}

/** Whether the strip is now there to read back. */
function writeStrip(store: string, json: string, fresh: boolean): boolean {
  if (fresh) {
    freshStrips.set(store, json);
    return true;
  }
  try {
    userLocalStorage.setItem(store, json);
    return userLocalStorage.getItem(store) === json;
  } catch { /* a full or blocked store is not worth failing a render over */ }
  return false;
}

/**
 * The strip a key holds, or null when it holds nothing readable. A key that
 * parses to no tabs is a strip the reader emptied on purpose, and comes back
 * as the empty tab rather than as "absent".
 */
function readPersisted(key: string | null, fresh: boolean): StripBody | null {
  if (!key) return null;
  try {
    const raw = fresh ? freshStrips.get(key) : userLocalStorage.getItem(key);
    if (!raw) return null;
    const saved = persistedStateSchema.safeParse(JSON.parse(raw));
    if (!saved.success) return null;
    // `active` indexes the saved array, entries this build drops included, so
    // it is matched while walking that array: the saved tab if it survived,
    // else the nearest surviving one before it.
    const tabs: FileTab[] = [];
    let activeId: string | null = null;
    saved.data.tabs.forEach((entry, i) => {
      const tab = parsePersistedTab(withKind(entry));
      if (!tab) return;
      const revived = revive(tab);
      tabs.push(revived);
      if (i <= saved.data.active) activeId = revived.id;
    });
    if (!tabs.length) return blankStrip();
    if (saved.data.active < 0) {
      const empty = emptyTab();
      return { tabs: [...tabs, empty], activeId: empty.id };
    }
    return { tabs, activeId: activeId ?? tabs[0].id };
  } catch {
    return null;
  }
}

function replaceIn(state: TabsState, id: string, body: TabBody): TabsState {
  return { ...state, tabs: state.tabs.map((t) => (t.id !== id ? t : { ...body, id })), activeId: id };
}

function bodyOf({ id: _id, ...body }: FileTab): TabBody {
  return body;
}

/** Whether two bodies are the same thing to show, the rule that keeps one tab per file, port, symbol or call. */
function sameView(a: TabBody, b: TabBody): boolean {
  switch (a.kind) {
    case 'empty': return false;
    case 'file': return b.kind === 'file' && b.path === a.path;
    case 'preview': return b.kind === 'preview' && b.port === a.port;
    case 'chart': return b.kind === 'chart' && b.symbol === a.symbol;
    case 'drive': return b.kind === 'drive' && drivePlaceKey(a.loc) === drivePlaceKey(b.loc);
    case 'tool': return b.kind === 'tool' && b.toolCallId === a.toolCallId;
    case 'sources': return b.kind === 'sources' && b.messageId === a.messageId;
    default: return b.kind === a.kind;
  }
}

/**
 * One place in a Drive, its path matched as the server matches names, read
 * as the page reads its address: the trash over all, else a file wherever it
 * was opened from, else the memo list or a folder.
 */
function drivePlaceKey(loc: DriveLocation): string {
  const where = loc.trash ? 'trash'
    : loc.file ? `file:${drivePathKey(loc.file)}`
    : loc.memos ? 'memos'
    : `dir:${drivePathKey(loc.dir ?? '')}`;
  return `${loc.space ?? ''}|${where}`;
}

/**
 * Whether `loc` names a memo's view or a version, which an open moves the
 * tab to. A comment asked for is on the document, so it names that view.
 */
function namesView(loc: DriveLocation): boolean {
  return loc.tab != null || loc.version != null || loc.against != null || loc.ask != null;
}

/** Remake a tab as something else, with the step it left kept to come back to. */
function navigateIn(state: TabsState, id: string, body: TabBody): TabsState {
  const tab = state.tabs.find((t) => t.id === id);
  const next = replaceIn(state, id, body);
  if (!tab || sameStep(tab, body)) return next;
  const back = [...(state.history?.[id]?.back ?? []), bodyOf(tab)].slice(-MAX_HISTORY);
  return { ...next, history: { ...state.history, [id]: { back, forward: [] } } };
}

/**
 * A step onto `b` from `a` would change nothing on screen; two empty tabs
 * look alike. A Drive file's view and the list beside it count too, since a
 * memo's record, a past version or the memo list in place of the folder is
 * a step on the page and so a step here.
 */
function sameStep(a: TabBody, b: TabBody): boolean {
  if (a.kind === 'drive' && b.kind === 'drive') {
    return sameView(a, b) && (a.loc.tab ?? null) === (b.loc.tab ?? null) && !!a.loc.memos === !!b.loc.memos
      && (a.loc.version ?? null) === (b.loc.version ?? null) && (a.loc.against ?? null) === (b.loc.against ?? null);
  }
  return a.kind === 'empty' ? b.kind === 'empty' : sameView(a, b);
}

/** Drops a step that shows what the one before it shows, which a step onto it would not change. */
function collapseRepeats(steps: TabBody[]): TabBody[] {
  return steps.filter((step, at) => at === 0 || !sameStep(step, steps[at - 1]));
}

/**
 * `next` with the history of `from`, a tab folded into `intoId`, joined onto
 * that tab's: Back from the tab it folded into returns to where the reader
 * moved away from, as it would have without the fold. The surviving tab's own
 * steps stay on either side of that journey, since where it is now is where
 * both left off. Where the two journeys meet they can repeat a step (both
 * began on an empty tab). `step` is false for a fold no move of the reader's
 * made, which adds no step of its own.
 */
function foldHistory(prev: TabsState, next: TabsState, from: FileTab, intoId: string, step = true): TabsState {
  const kept = prev.history?.[intoId];
  const back = collapseRepeats([...(kept?.back ?? []), ...(prev.history?.[from.id]?.back ?? []), ...(step ? [bodyOf(from)] : [])]).slice(-MAX_HISTORY);
  return { ...next, history: { ...next.history, [intoId]: { back, forward: kept?.forward ?? [] } } };
}

/** Drops the steps that name a deleted file, so Back never lands on a read error. */
function forgetPathsIn(state: TabsState, paths: ReadonlySet<string>): TabsState {
  if (!state.history) return state;
  const keep = (body: TabBody) => body.kind !== 'file' || !paths.has(body.path);
  const history = Object.fromEntries(Object.entries(state.history).map(
    ([id, h]) => [id, { back: collapseRepeats(h.back.filter(keep)), forward: collapseRepeats(h.forward.filter(keep)) }],
  ));
  return { ...state, history };
}

/**
 * The nearest step a tab can take in `dir`. A step another tab now shows is
 * passed over, since going there would put the same file in two tabs, and so
 * is one onto what the tab already shows (left behind when a deleted file's
 * step between two visits goes), since it would only remount it.
 */
function historyStep(state: TabsState, id: string, dir: HistoryDirection): number {
  const steps = state.history?.[id]?.[dir] ?? [];
  const tab = state.tabs.find((t) => t.id === id);
  for (let at = steps.length - 1; at >= 0; at--) {
    if (tab && sameStep(tab, steps[at])) continue;
    if (!state.tabs.some((t) => t.id !== id && sameView(t, steps[at]))) return at;
  }
  return -1;
}

/**
 * Take the step. The tab comes back under a new id, so what the panel keeps
 * per tab (an editor's draft, a location to replay) starts over with what it
 * now shows, as it does for a tab opened fresh. Steps passed over are dropped.
 */
function stepIn(state: TabsState, id: string, dir: HistoryDirection): TabsState {
  const tab = state.tabs.find((t) => t.id === id);
  const at = historyStep(state, id, dir);
  if (!tab || at < 0) return state;
  const { [id]: history, ...rest } = state.history!;
  const steps = history[dir];
  const ahead = steps.slice(0, at);
  const behind = [...history[dir === 'back' ? 'forward' : 'back'], bodyOf(tab)].slice(-MAX_HISTORY);
  const fresh = newId();
  // Kept is the tab's, not the step's: a step recorded while the tab was on
  // loan comes back kept once the reader kept the tab, so the next link never
  // takes what they were reading.
  const step: FileTab = { ...steps[at], id: fresh };
  return {
    ...state,
    tabs: state.tabs.map((t) => (t.id === id ? (isOnLoan(tab) ? step : kept(step)) : t)),
    activeId: state.activeId === id ? fresh : state.activeId,
    history: { ...rest, [fresh]: dir === 'back' ? { back: ahead, forward: behind } : { back: behind, forward: ahead } },
  };
}

function patchIn(state: TabsState, id: string, fn: (tab: FileTab) => FileTab): TabsState {
  return { ...state, tabs: state.tabs.map((t) => (t.id === id ? fn(t) : t)) };
}

/**
 * Land a tab: the one that already matches is remade in place and brought to
 * the front; else an empty active tab is filled; else a new tab opens beside
 * the active one. With `loan` the active tab is also a target when it is the
 * on-loan tab of the opener's kind, and failing that whichever tab carries
 * that loan, so browsing never piles tabs up.
 */
function place(
  state: TabsState,
  match: (tab: FileTab) => boolean,
  make: (existing: FileTab | null) => TabBody,
  { loan = false }: { loan?: boolean } = {},
): TabsState {
  const existing = state.tabs.find(match);
  if (existing) return replaceIn(state, existing.id, make(existing));

  const body = make(null);
  const onLoan = (t: FileTab) => loan && isOnLoan(t) && t.kind === body.kind;
  const active = state.tabs.find((t) => t.id === state.activeId);
  const slot = active && (active.kind === 'empty' || onLoan(active)) ? active : state.tabs.find(onLoan);
  if (slot) return navigateIn(state, slot.id, body);

  const fresh: FileTab = { id: newId(), ...body };
  const at = state.tabs.findIndex((t) => t.id === state.activeId);
  const tabs = [...state.tabs];
  tabs.splice(at < 0 ? tabs.length : at + 1, 0, fresh);
  return { ...state, tabs, activeId: fresh.id };
}

/**
 * A strip is never empty: closing its last tab leaves the empty one. The
 * reader closing it from the strip takes the panel instead (`closeOut`);
 * this is every other close, such as a deleted file's tab.
 */
function closeIn(state: TabsState, id: string): TabsState {
  const at = state.tabs.findIndex((t) => t.id === id);
  if (at < 0) return state;
  const tabs = state.tabs.filter((t) => t.id !== id);
  if (!tabs.length) return { ...blankStrip(), key: state.key, handedFrom: state.handedFrom };
  const activeId = id === state.activeId ? (tabs[at] ?? tabs[at - 1]).id : state.activeId;
  const { [id]: _closed, ...history } = state.history ?? {};
  return { ...state, tabs, activeId, history };
}

/**
 * The panel's open tabs.
 *
 * Tabs persist per thread and come back as names only. Nothing is read until
 * a tab is activated, so reopening a thread with eight tabs costs one
 * request, not eight: the bodies live in the React Query cache, keyed by path,
 * and a tab is only a claim on one.
 *
 * A thread without a strip of its own starts empty. Inheriting the strip the
 * workspace showed last put a file from an earlier conversation beside a new
 * one about something else, which reads as the agent having opened it. A
 * fresh chat keeps its strip in memory until its first message mints a thread
 * id, and the strip carries over to that thread unchanged. Given `chat`, that
 * strip is held under the chat, so it outlives this mount.
 *
 * `persist: false` keeps a strip in memory for the life of the mount: it reads
 * and writes nothing, including the last chart symbol. Its identity still
 * follows the real workspace, so it is rebuilt when the panel is pointed at
 * another one rather than showing the first workspace's tabs under the
 * second's id.
 *
 * `active: false` is a panel that is mounted but not on screen: it reads its
 * strip and keeps it, but writes nothing while hidden. It writes again once it
 * is back on screen.
 */
export function useFileTabs(
  workspaceId: string,
  threadId?: string | null,
  { persist = true, active: onScreen = true, chat }: { persist?: boolean; active?: boolean; chat?: string } = {},
) {
  // Which strip this is. No key for an adapter mount (a share has no workspace
  // of its own), so two shares cannot inherit each other's tab strip.
  const freshKey = workspaceId ? tabsStorageKey(workspaceId, chat) : null;
  const key = workspaceId && threadId ? threadTabsStorageKey(workspaceId, threadId) : freshKey;

  // Where it is kept: a thread's in storage, a fresh chat's in memory under
  // the chat, and nowhere when it does not persist or has neither.
  const fresh = !threadId;
  const store = persist && (threadId || chat) ? key : null;

  // A thread with nothing saved yet takes what its chat held before the
  // thread existed: the panel may have been closed when the first message
  // minted it, so no mount saw the switch below.
  const held = (): TabsState | null => {
    const strip = chat && store && !fresh ? readPersisted(freshKey, true) : null;
    return strip && { ...strip, key, handedFrom: freshKey ?? undefined };
  };
  const [state, setState] = useState<TabsState>(() => {
    const own = readPersisted(store, fresh);
    return own ? { ...own, key } : held() ?? { ...blankStrip(), key };
  });

  // A thread switch is a different strip, not a reordering of this one. A
  // thread with nothing saved takes what its chat held, as above, or starts
  // empty. The thread a fresh chat in this workspace just became keeps what is
  // on screen instead (and its locations with it); the chat keys the panel by
  // instance, so the mount lives through that change. Switched during render, not in an effect: the
  // panel's target opens its file in a layout effect of the same update (a
  // link to another workspace's file moves the panel and opens the file at
  // once), and a switch landing after it would replace that tab with the new
  // workspace's empty strip.
  const [shownKey, setShownKey] = useState(key);
  if (shownKey !== key) {
    const minted = shownKey === freshKey;
    setShownKey(key);
    setState((prev) => {
      const own = readPersisted(store, fresh);
      if (own) return { ...own, key };
      // The strip on screen is the chat's held one, with its live locations.
      // Without a chat it was held by no one, so there is no copy to retire.
      if (minted) return { ...prev, key, handedFrom: chat ? shownKey ?? undefined : undefined };
      return held() ?? { ...blankStrip(), key };
    });
  }

  // Written only once the strip on screen is the one this key names; the
  // render between a key change and the switch above still holds the old strip.
  // A hidden panel saves one thing: a chat's held strip handed to the thread
  // its first message minted after the reader moved on, since the chat can be
  // dropped before it is shown again and the held copy goes with it.
  useEffect(() => {
    const handing = !!state.handedFrom && freshStrips.has(state.handedFrom);
    if (!store || (!onScreen && !handing) || state.key !== key) return;
    saveStrip(store, state, fresh);
  }, [state, key, store, onScreen, fresh]);

  /**
   * Close the last tab along with the panel. The emptied strip is written
   * here as well as set: the host can unmount the panel in the same update,
   * and the effect above never runs for a panel that is gone. It is set too,
   * because a host animating the panel out keeps this mount, and a reopen
   * inside that window gets it back.
   */
  const { handedFrom } = state;
  const closeOut = useCallback(() => {
    const blank = { ...blankStrip(), handedFrom };
    setState((prev) => ({ ...blank, key: prev.key }));
    if (store && onScreen) saveStrip(store, { ...blank, key: store }, fresh);
  }, [store, onScreen, fresh, handedFrom]);

  const activate = useCallback((id: string) => {
    setState((prev) => (prev.activeId === id ? prev : { ...prev, activeId: id }));
  }, []);

  const newTab = useCallback(() => {
    setState((prev) => {
      const tab = emptyTab();
      return { ...prev, tabs: [...prev.tabs, tab], activeId: tab.id };
    });
  }, []);

  /**
   * Bring a tab the tree can show beside to the front, for a folder asked for
   * with a chart or a tool result in front: the active tab if it already is
   * one, else the last file or empty tab in the strip, else a new empty tab.
   */
  const showListing = useCallback(() => {
    setState((prev) => {
      const active = prev.tabs.find((t) => t.id === prev.activeId);
      if (active && isListingTab(active)) return prev;
      const listing = prev.tabs.findLast(isListingTab);
      if (listing) return { ...prev, activeId: listing.id };
      const tab = emptyTab();
      return { ...prev, tabs: [...prev.tabs, tab], activeId: tab.id };
    });
  }, []);

  /** Apply `fn` to one tab; the tab's own kind decides what it means. */
  const patchTab = useCallback((id: string, fn: (tab: FileTab) => FileTab) => {
    setState((prev) => patchIn(prev, id, fn));
  }, []);

  /**
   * Open a file, or activate the tab already showing it. A pinned open never
   * lands on a loaned tab, so pinning one file and clicking another never
   * overwrites the first; re-opening a kept tab as a preview leaves it kept.
   */
  const openFile = useCallback((path: string, { pin = false, location = null }: OpenFileOptions = {}) => {
    setState((prev) => place(prev, (t) => t.kind === 'file' && t.path === path, (existing) => {
      const kept = existing?.kind === 'file' ? existing : null;
      return {
        kind: 'file',
        path,
        preview: kept ? (pin ? false : kept.preview) : !pin,
        location: location ?? kept?.location ?? null,
        locationSeq: (kept?.locationSeq ?? 0) + (location ? 1 : 0),
      };
    }, { loan: !pin }));
  }, []);

  /** A second open of a singleton kind comes back to the tab it already has. */
  /**
   * `focus: false` is for what the agent starts, a watch or browsing, with the
   * reader on something else: the tab joins the end of the strip and the reader
   * stays on what they were reading.
   */
  const openSingleton = useCallback((kind: SingletonKind, { focus = true }: { focus?: boolean } = {}) => {
    setState((prev) => {
      if (focus) return place(prev, (t) => t.kind === kind, () => ({ kind }));
      return prev.tabs.some((t) => t.kind === kind) ? prev : { ...prev, tabs: [...prev.tabs, { id: newId(), kind }] };
    });
  }, []);
  const openSettings = useCallback(() => openSingleton('settings'), [openSingleton]);
  const openMemory = useCallback(() => openSingleton('memory'), [openSingleton]);
  const openMemo = useCallback(() => openSingleton('memo'), [openSingleton]);
  const openStatus = useCallback((options?: { focus?: boolean }) => openSingleton('status', options), [openSingleton]);
  const openBrowser = useCallback((options?: { focus?: boolean }) => openSingleton('browser', options), [openSingleton]);

  /**
   * One tab per port: a second open of a running app comes back to the tab it
   * already has. A re-open that names nothing keeps the labelling the tab has,
   * so the tree's own click cannot blank a title the agent gave it.
   */
  const openPreview = useCallback(({ port, title, path, command }: PreviewSpec) => {
    setState((prev) => place(prev, (t) => t.kind === 'preview' && t.port === port, (existing) => ({
      ...(existing?.kind === 'preview' ? existing : { kind: 'preview', port }),
      ...(title !== undefined && { title }),
      ...(path !== undefined && { previewPath: path }),
      ...(command !== undefined && { command }),
    })));
  }, []);

  /**
   * One tab per symbol: asking for GOOGL again comes back to the GOOGL tab. A
   * re-open that names an interval moves the chart there; one that does not
   * leaves the tab on whatever interval it was last looked at on. The
   * workspace is the ask's each time, so the tab draws the annotations the
   * card that opened it was about.
   */
  const openChart = useCallback(({ symbol, timeframe, workspaceId: chartWorkspaceId }: ChartTabSpec) => {
    const ticker = chartTicker(symbol);
    if (!ticker) return;
    if (persist) rememberChartSymbol(workspaceId, ticker);
    setState((prev) => place(prev, (t) => t.kind === 'chart' && t.symbol === ticker, (existing) => ({
      kind: 'chart',
      symbol: ticker,
      timeframe: timeframe ?? (existing?.kind === 'chart' ? existing.timeframe : DEFAULT_TIMEFRAME),
      ...(chartWorkspaceId && { workspaceId: chartWorkspaceId }),
    })));
  }, [workspaceId, persist]);

  /**
   * Point a chart tab at another symbol, keeping its place in the strip and
   * its interval. If another tab already shows that symbol the two fold into
   * one, so the symbol rule holds without the user closing anything.
   */
  const retargetChart = useCallback((id: string, symbol: string) => {
    const ticker = chartTicker(symbol);
    if (!ticker) return;
    if (persist) rememberChartSymbol(workspaceId, ticker);
    setState((prev) => {
      const tab = prev.tabs.find((t) => t.id === id);
      if (tab?.kind !== 'chart') return prev;
      const other = prev.tabs.find((t) => t.kind === 'chart' && t.symbol === ticker && t.id !== id);
      if (other) {
        // The switch is the reader's, so the tab keeps its interval and the
        // drawings it was showing; the other tab's workspace goes with it.
        const folded = patchIn(closeIn(prev, id), other.id, (t) => {
          if (t.kind !== 'chart') return t;
          const { workspaceId: _dropped, ...rest } = t;
          return { ...rest, timeframe: tab.timeframe, ...(tab.workspaceId && { workspaceId: tab.workspaceId }) };
        });
        // So is its history.
        return { ...foldHistory(prev, folded, tab, other.id), activeId: other.id };
      }
      const { id: _id, ...chart } = tab;
      return navigateIn(prev, id, { ...chart, symbol: ticker });
    });
  }, [workspaceId, persist]);

  /**
   * One tab per tool call: the row clicked again comes back to its tab. A
   * first open lands on the loaned tool tab, so reading through a turn's rows
   * never piles tabs up.
   */
  const openTool = useCallback(({ toolCallId }: ToolTabSpec) => {
    setState((prev) => place(prev, (t) => t.kind === 'tool' && t.toolCallId === toolCallId, (existing) => ({
      kind: 'tool',
      toolCallId,
      preview: existing?.kind === 'tool' ? existing.preview : true,
    }), { loan: true }));
  }, []);

  /**
   * One tab per place in a Drive: a second open comes back to its tab, and a
   * first lands on the loaned Drive tab unless pinned, as a file's does. A
   * view the open names (a memo's record, a version) moves the tab there, a
   * step Back undoes, as a move inside the tab is; one that names none leaves
   * it where the reader was.
   */
  const openDrive = useCallback(({ loc, pin = false }: { loc: DriveLocation; pin?: boolean }) => {
    const key = drivePlaceKey(loc);
    const match = (t: FileTab) => t.kind === 'drive' && drivePlaceKey(t.loc) === key;
    const make = (existing: FileTab | null): TabBody => {
      const was = existing?.kind === 'drive' ? existing : null;
      return {
        kind: 'drive',
        // An open names a path, so the tab learns the id of whatever is there.
        loc: was && !namesView(loc) ? { ...loc, tab: was.loc.tab, version: was.loc.version, against: was.loc.against } : loc,
        preview: was ? (pin ? false : was.preview) : !pin,
      };
    };
    setState((prev) => {
      const existing = prev.tabs.find(match);
      return existing ? navigateIn(prev, existing.id, make(existing)) : place(prev, match, make, { loan: !pin });
    });
  }, []);

  /**
   * Move a Drive tab to another place, as a link in a browser tab does: a
   * step back can undo, and the tab is kept from then on. `replace` corrects
   * where the tab is instead (the file renamed under it, an address the file
   * reads otherwise), adding no step and leaving it on loan. A place has one
   * tab, so a move onto a place another tab shows folds the two, journeys
   * and all.
   */
  const goDrive = useCallback((id: string, loc: DriveLocation, { replace = false }: { replace?: boolean } = {}) => {
    setState((prev) => {
      const tab = prev.tabs.find((t) => t.id === id);
      if (tab?.kind !== 'drive') return prev;
      const body: TabBody = {
        kind: 'drive',
        loc,
        // The id names the file a correction keeps; a move elsewhere learns its own.
        ...(replace && tab.entryId && { entryId: tab.entryId }),
        preview: replace ? tab.preview : false,
      };
      const key = drivePlaceKey(loc);
      const other = prev.tabs.find((t) => t.id !== id && t.kind === 'drive' && drivePlaceKey(t.loc) === key);
      // The tab moved gives way to the one already there, as an open of the
      // place comes back to it, taking it to the view named; a move the reader
      // made keeps it. Only a loaned tab there gives way to a kept one instead.
      if (other && !(isOnLoan(other) && !isOnLoan(tab))) {
        // A correction says which file is at the place now, so the tab there
        // takes that file's id; its own named the file that was.
        // A move the reader made names its whole place, the document view
        // included; a correction keeps the view the tab there shows unless it
        // names one.
        const folded = foldHistory(prev, patchIn(closeIn(prev, id), other.id, (t) => (t.kind !== 'drive' ? t : {
          ...t,
          ...((!replace || namesView(loc)) && { loc }),
          ...(!replace && { preview: false }),
          ...(replace && tab.entryId && { entryId: tab.entryId }),
        })), tab, other.id, !replace);
        return prev.activeId === id ? { ...folded, activeId: other.id } : folded;
      }
      // A correction can land after the reader moved to another tab (a trash
      // that took a moment): it leaves the tab in front where it is, unless
      // that is the loaned tab giving way, whose place this tab now shows.
      const moved = replace ? { ...replaceIn(prev, id, body), activeId: prev.activeId } : navigateIn(prev, id, body);
      if (!other) return moved;
      const closed = closeIn(moved, other.id);
      return prev.activeId === other.id ? { ...closed, activeId: id } : closed;
    });
  }, []);

  /** Keeps the open file's id on its Drive tab, to find it again after a move. */
  const trackDrive = useCallback((id: string, entryId: string) => {
    setState((prev) => patchIn(prev, id, (t) => (t.kind === 'drive' && t.entryId !== entryId ? { ...t, entryId } : t)));
  }, []);

  /** One tab per turn; a first open lands on the loaned sources tab. */
  const openSources = useCallback((messageId: string) => {
    setState((prev) => place(prev, (t) => t.kind === 'sources' && t.messageId === messageId, (existing) => ({
      kind: 'sources',
      messageId,
      preview: existing?.kind === 'sources' ? existing.preview : true,
    }), { loan: true }));
  }, []);

  const pinTab = useCallback((id: string) => {
    patchTab(id, kept);
  }, [patchTab]);

  const closeTab = useCallback((id: string) => setState((prev) => closeIn(prev, id)), []);
  /** Several at once, each closed as `closeTab` would, so the one left in front is the neighbour of the last to go. */
  const closeTabs = useCallback((ids: readonly string[]) => setState((prev) => ids.reduce(closeIn, prev)), []);

  /** What a step from this tab would show, or null where it has none to take. */
  const historyTarget = useCallback((id: string, dir: HistoryDirection): TabBody | null => {
    const at = historyStep(state, id, dir);
    return at < 0 ? null : state.history![id][dir][at];
  }, [state]);

  const forgetPaths = useCallback((paths: readonly string[]) => {
    setState((prev) => forgetPathsIn(prev, new Set(paths)));
  }, []);

  const stepHistory = useCallback((id: string, dir: HistoryDirection) => {
    // A step to another ticker is as much the reader's pick as a retarget is.
    const target = historyTarget(id, dir);
    if (persist && target?.kind === 'chart') rememberChartSymbol(workspaceId, target.symbol);
    setState((prev) => stepIn(prev, id, dir));
  }, [historyTarget, workspaceId, persist]);

  /** Drop a tab's location: the focus chip's dismiss, which must survive a tab switch. */
  const clearLocation = useCallback((id: string) => {
    patchTab(id, (t) => (t.kind === 'file' ? { ...t, location: null } : t));
  }, [patchTab]);

  const activeTab = state.tabs.find((t) => t.id === state.activeId) ?? state.tabs[0];
  const openPaths = useMemo(
    () => new Set(state.tabs.flatMap((t) => (t.kind === 'file' ? [t.path] : []))),
    [state.tabs],
  );

  // One object per strip change: the api lands in effect deps and memo deps
  // downstream, and each member is already stable on its own.
  return useMemo(() => ({
    tabs: state.tabs,
    activeTab,
    activeId: activeTab.id,
    openPaths,
    activate,
    openFile,
    pinTab,
    closeTab,
    closeTabs,
    closeOut,
    newTab,
    showListing,
    openSettings,
    openMemory,
    openMemo,
    openStatus,
    openBrowser,
    openPreview,
    openChart,
    retargetChart,
    openTool,
    openSources,
    openDrive,
    goDrive,
    trackDrive,
    patchTab,
    clearLocation,
    historyTarget,
    stepHistory,
    forgetPaths,
  }), [
    state.tabs, activeTab, openPaths,
    activate, openFile, pinTab, closeTab, closeTabs, closeOut, newTab, showListing, openSettings, openMemory, openMemo, openStatus, openBrowser,
    openPreview, openChart, retargetChart, openTool, openSources, openDrive, goDrive, trackDrive, patchTab, clearLocation, historyTarget, stepHistory, forgetPaths,
  ]);
}

export type FileTabsApi = ReturnType<typeof useFileTabs>;
