import {
  Archive,
  ChartCandlestick,
  Copy,
  ExternalLink,
  FileText,
  Link2,
  MessageSquare,
  MessageSquareQuote,
  PencilLine,
  Pin,
  PinOff,
  Search,
  Sparkles,
} from 'lucide-react';
import type { MatchRange, SearchFileHit, SearchThreadHit, SearchTurnHit } from '@/api/search';
import type { SlashCommand } from '@/components/ui/chat-input.types';
import { fileGlyph } from '@/pages/ChatAgent/components/filePanel/fileMeta';
import { sentMessageWords } from '@/pages/ChatAgent/components/messageList/sentMessage';
import type { Thread } from '@/types/api';
import type { PaletteAction, PaletteItem, Place } from './types';

type T = (key: string, options?: Record<string, unknown>) => string;

/** How rows name workspaces, agents and times, as the rest of the app does. */
export interface Names {
  /** The workspace's name as the app shows it (Home is the Chief of Staff), else `given`. */
  workspaceName: (workspaceId: string, given?: string) => string;
  /** Whether the agent that answers in this workspace is the Chief of Staff. */
  isHome: (workspaceId: string) => boolean;
  when: (iso: string | null | undefined) => string;
}

/**
 * Where new work goes: the workspace on screen, else Home and its Chief of
 * Staff. `pending` while that is still being looked up; with Home unread for
 * good there is nowhere, so nothing offers to start a thread.
 */
export interface NewWork {
  workspaceId: string | undefined;
  pending: boolean;
  canStartThread: boolean;
  /** The agent new work goes to, as rows name it; unset while pending. */
  where: string | undefined;
}

/** What rows need from the app: its words, its names, and what a row does there. */
export interface Verbs extends Names {
  t: T;
  newWork: NewWork;
  /** A thread, in its workspace; at a turn, or with words left unsent in its composer. */
  openThread: (threadId: string, workspaceId: string, also?: { focusTurn?: number; draftMessage?: string }) => void;
  /** A new thread in the workspace (where new work goes when unset), its composer holding a draft. */
  newThread: (workspaceId: string | undefined, draft?: { message?: string; commands?: SlashCommand[] }) => void;
  openFile: (workspaceId: string, path: string) => void;
  openChart: (symbol: string) => void;
  copy: (text: string) => void;
  /** Opens an app path in a new tab; null in the desktop shell, which sends a new window to the system browser, signed out. */
  openInNewTab: ((path: string) => void) | null;
  renameThread: (thread: ThreadRef, title: string) => void;
  pinThread: (thread: ThreadRef, pinned: boolean) => void;
  /**
   * At once, or once confirmed for a thread with a run going. Once the server
   * has archived the thread on screen, the next one in its workspace opens in
   * its place, as from the sidebar.
   */
  archiveThread: (thread: ThreadRef) => void;
}

type Actions = [PaletteAction, ...PaletteAction[]];

export function runs(id: string, label: string, icon: PaletteAction['icon'], run: () => void, startsThread = false): PaletteAction {
  return startsThread ? { id, label, icon, effect: 'run', run, startsThread } : { id, label, icon, effect: 'run', run };
}

/** A file's link: the chat and the workspace page both open the file panel at `?file=`. */
export const fileHref = (workspaceId: string, path: string) => `/chat/${workspaceId}?${new URLSearchParams({ file: path })}`;

function inNewTab(path: string, v: Verbs): PaletteAction[] {
  const open = v.openInNewTab;
  return open ? [runs('new-tab', v.t('palette.do.openInNewTab'), ExternalLink, () => open(path))] : [];
}

function threadElsewhere(threadId: string, v: Verbs): PaletteAction[] {
  const path = `/chat/t/${threadId}`;
  return [...inNewTab(path, v), runs('copy-link', v.t('palette.do.copyLink'), Link2, () => v.copy(`${window.location.origin}${path}`))];
}

/** A thread as its row shows it, from the app's own list or from the server's search. */
export interface ThreadRef {
  threadId: string;
  workspaceId: string;
  /** What the row calls it: its title, or with none its first message (the app's lists give its words, a search hit the message as sent). Empty with neither. */
  title: string;
  /** The title the thread holds, empty for one without. */
  savedTitle: string;
  spans?: [number, number][];
  /** Where it runs: its automation, or its workspace. */
  where: string;
  at: string | null | undefined;
  /** Pinned to the top of its workspace. */
  pinned: boolean;
}

/**
 * What a thread's row can change about it, after the ways to open it: its
 * title (in the palette's field, starting from its title, or for an untitled
 * thread from its first message's words, as the recent rows show them: a
 * search hit's stands for the message as sent, markup and all), its pin (named for what it would do now, as the theme
 * action is), and whether it is archived.
 */
function threadChanges(ref: ThreadRef, v: Verbs): PaletteAction[] {
  return [
    {
      id: 'rename',
      label: v.t('nav.renameThread'),
      icon: PencilLine,
      effect: 'edit',
      value: ref.savedTitle || sentMessageWords(ref.title),
      placeholder: v.t('palette.rename.placeholder'),
      maxLength: 255, // ThreadUpdateRequest.title, as the sidebar's and the dialog's fields
      save: (title) => v.renameThread(ref, title),
    },
    runs('pin', ref.pinned ? v.t('nav.unpinThread') : v.t('nav.pinToTop'), ref.pinned ? PinOff : Pin, () => v.pinThread(ref, !ref.pinned)),
    runs('archive', v.t('nav.archiveThread'), Archive, () => v.archiveThread(ref)),
  ];
}

export function threadOfList(thread: Thread, names: Names): ThreadRef {
  return {
    threadId: thread.thread_id,
    workspaceId: thread.workspace_id,
    title: thread.title || sentMessageWords(thread.first_query_content ?? ''),
    savedTitle: thread.title ?? '',
    where: names.workspaceName(thread.workspace_id),
    at: thread.updated_at,
    pinned: !!thread.is_pinned,
  };
}

export function threadOfHit(hit: SearchThreadHit, names: Names): ThreadRef {
  return {
    threadId: hit.thread_id,
    workspaceId: hit.workspace_id,
    title: hit.title,
    savedTitle: hit.has_title ? hit.title : '',
    spans: hit.title ? hit.match : undefined,
    // An automation's thread says which automation runs in it: its title is
    // usually the automation's own words, the same on every run.
    where: hit.automation_name ?? names.workspaceName(hit.workspace_id, hit.workspace_name),
    at: hit.updated_at,
    pinned: hit.is_pinned,
  };
}

/** The thread a conversation match is in. When it was last active the hit does not say. */
function threadOfTurn(hit: SearchTurnHit, names: Names): ThreadRef {
  return {
    threadId: hit.thread_id,
    workspaceId: hit.workspace_id,
    title: hit.thread_title,
    savedTitle: hit.has_title ? hit.thread_title : '',
    where: names.workspaceName(hit.workspace_id, hit.workspace_name),
    at: null,
    pinned: hit.is_pinned,
  };
}

/** A thread's row without its actions: how it looks wherever it shows. */
function threadFace(ref: ThreadRef, v: Verbs) {
  return {
    icon: MessageSquare,
    title: ref.title || v.t('palette.untitledThread'),
    meta: [ref.where, v.when(ref.at)].filter(Boolean).join(' · '),
    pinned: ref.pinned || undefined,
  };
}

export function threadRow(ref: ThreadRef, v: Verbs): PaletteItem {
  const { threadId, workspaceId } = ref;
  const face = threadFace(ref, v);
  return {
    id: `thread:${threadId}`,
    ...face,
    spans: ref.spans,
    preview: { kind: 'thread', threadId },
    place: { kind: 'thread', threadId, workspaceId, title: face.title },
    actions: [
      runs('open', v.t('palette.do.open'), MessageSquare, () => v.openThread(threadId, workspaceId)),
      ...threadElsewhere(threadId, v),
      ...threadChanges(ref, v),
    ],
  };
}

/**
 * A turn that matched. In the search of its own thread every row would name
 * the same thread, so the matched words lead, then who said them and when;
 * elsewhere the thread is the first line and the words the second, after who
 * said them, and Tab searches that thread.
 */
export function turnRow(hit: SearchTurnHit, v: Verbs, { inThread = false } = {}): PaletteItem {
  const { thread_id: threadId, workspace_id: workspaceId, turn_index: turnIndex } = hit;
  const thread = threadOfTurn(hit, v);
  const speaker = hit.side === 'user' ? v.t('palette.you') : v.isHome(workspaceId) ? v.t('agents.chiefOfStaff') : v.t('agents.analyst');
  const actions: Actions = [
    runs('open-at-turn', v.t('palette.do.openAtTurn'), MessageSquareQuote, () => v.openThread(threadId, workspaceId, { focusTurn: turnIndex })),
    runs('open-thread', v.t('palette.do.openThread'), MessageSquare, () => v.openThread(threadId, workspaceId)),
    ...threadElsewhere(threadId, v),
    ...threadChanges(thread, v),
  ];
  const row = {
    id: `turn:${threadId}:${turnIndex}:${hit.side}`,
    icon: MessageSquareQuote,
    preview: { kind: 'turn', threadId, turnIndex, side: hit.side },
    actions,
  } as const;
  if (inThread) {
    return { ...row, title: hit.snippet, spans: hit.matches, meta: [speaker, v.when(hit.turn_at)].filter(Boolean).join(' · ') };
  }
  const { title, pinned } = threadFace(thread, v);
  return {
    ...row,
    title,
    pinned,
    meta: [v.workspaceName(workspaceId, hit.workspace_name), v.when(hit.turn_at)].filter(Boolean).join(' · '),
    detail: { label: speaker, text: hit.snippet, spans: hit.matches },
    place: { kind: 'thread', threadId, workspaceId, title },
  };
}

/** The spans past `from`, counted from there. */
export function spansFrom(spans: readonly MatchRange[], from: number): [number, number][] {
  return spans.flatMap(([start, end]): [number, number][] => (end > from ? [[Math.max(start, from) - from, end - from]] : []));
}

/**
 * A file that matched: its name, bold where the query matched it, then the
 * folder it sits in and its workspace. The server's spans run over the whole
 * path, so the name's are the part past the last `/`.
 */
export function fileRow(hit: SearchFileHit, v: Verbs): PaletteItem {
  const { workspace_id: workspaceId, path } = hit;
  const cut = path.lastIndexOf('/') + 1;
  const spans = spansFrom(hit.match, cut);
  return {
    id: `file:${workspaceId}:${path}`,
    icon: fileGlyph(path),
    title: path.slice(cut) || hit.name,
    spans: spans.length > 0 ? spans : undefined,
    meta: [path.slice(0, Math.max(cut - 1, 0)), v.workspaceName(workspaceId, hit.workspace_name)].filter(Boolean).join(' · '),
    actions: [
      runs('open', v.t('palette.do.open'), FileText, () => v.openFile(workspaceId, path)),
      ...inNewTab(fileHref(workspaceId, path), v),
      runs('copy-path', v.t('palette.do.copyPath'), Copy, () => v.copy(path)),
    ],
  };
}

/** `name` may be empty: the row then shows the name its quote carries. In the ticker's own view it offers no way back into it. */
export function tickerRow(symbol: string, name: string, meta: string | undefined, v: Verbs, { inView = false } = {}): PaletteItem {
  const mentioning: PaletteAction = {
    id: 'mentioning',
    label: v.t('palette.do.threadsMentioning', { symbol }),
    icon: Search,
    effect: 'open',
    view: { kind: 'ticker', symbol, via: 'drill' },
  };
  return {
    id: `ticker:${symbol}`,
    icon: ChartCandlestick,
    symbol,
    title: name,
    meta,
    preview: { kind: 'ticker', symbol },
    actions: [
      runs('chart', v.t('palette.do.openChart'), ChartCandlestick, () => v.openChart(symbol)),
      ...(v.newWork.canStartThread
        ? [runs('new-thread', v.t('palette.do.newThreadAbout', { symbol }), Sparkles, () => v.newThread(undefined, { message: `${symbol} ` }), true)]
        : []),
      ...(inView ? [] : [mentioning]),
      runs('copy-symbol', v.t('palette.do.copySymbol'), Copy, () => v.copy(symbol)),
    ],
  };
}

/**
 * What ⌘↵ (and the Ask row) hands to the agent: `text`, to where new work
 * goes, or in a place's scope to that place's agent, still unsent (a new
 * thread in the workspace, or the thread's own composer; the place is known,
 * so nothing waits). Nothing to ask with no words.
 */
export function askRow(place: Place | null, text: string, v: Verbs): PaletteItem | null {
  const words = text.trim();
  if (!words || (!place && !v.newWork.canStartThread)) return null;
  const row = (meta: string | undefined, action: PaletteAction): PaletteItem => ({
    id: 'ask',
    icon: Sparkles,
    accent: true,
    title: v.t('palette.askTitle', { query: words }),
    meta,
    actions: [action],
  });
  const ask = (run: () => void, startsThread = false) => runs('ask', v.t('palette.do.ask'), Sparkles, run, startsThread);
  if (!place) return row(v.newWork.where, ask(() => v.newThread(undefined, { message: words }), true));
  const agent = v.isHome(place.workspaceId)
    ? v.t('agents.chiefOfStaff')
    : v.t('palette.analystIn', { workspace: place.kind === 'workspace' ? place.name : v.workspaceName(place.workspaceId) });
  return place.kind === 'workspace'
    ? row(agent, ask(() => v.newThread(place.workspaceId, { message: words })))
    : row(v.t('palette.inThisThread', { agent }), ask(() => v.openThread(place.threadId, place.workspaceId, { draftMessage: words })));
}
