import type { MemoTab, ReadLocation } from '@/lib/driveLinks';
import type { DriveAttribution, DriveEntry, DriveMemoMark, DriveVersion } from '@/types/drive';

/** A memo's views, the document first. */
export type MemoView = 'document' | MemoTab;

/** The most tickers a memo carries, and the shape of one, as the server checks them. */
export const MAX_TICKERS = 20;
const TICKER = /^[A-Z0-9][A-Z0-9.:/-]{0,23}$/;

/**
 * Tickers as typed (comma or space separated) in the server's spelling:
 * upper case, each once, in the order typed. `bad` names the ones the server
 * would refuse, so the field can say so before anything is sent.
 */
export function parseTickers(typed: string): { tickers: string[]; bad: string[]; tooMany: boolean } {
  const tickers: string[] = [];
  const bad: string[] = [];
  for (const raw of typed.split(/[\s,]+/)) {
    const ticker = raw.trim().toUpperCase();
    if (!ticker || tickers.includes(ticker) || bad.includes(ticker)) continue;
    if (TICKER.test(ticker)) tickers.push(ticker);
    else bad.push(ticker);
  }
  return { tickers, bad, tooMany: tickers.length > MAX_TICKERS };
}

export function sameTickers(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((t, i) => t === b[i]);
}

/**
 * What a file is to the memo features: a memo, a file that was one (its
 * record kept, reached from its menu), or a plain file. Every surface reads
 * this, never the raw mark.
 */
export type MemoState =
  | { kind: 'memo'; mark: DriveMemoMark }
  | { kind: 'former'; mark: DriveMemoMark }
  | { kind: 'plain' };

const PLAIN: MemoState = { kind: 'plain' };

export function memoState(entry: Pick<DriveEntry, 'memo'> | null | undefined): MemoState {
  const mark = entry?.memo;
  if (!mark) return PLAIN;
  return mark.marked ? { kind: 'memo', mark } : { kind: 'former', mark };
}

/** The version a final memo pins; null when it is not final or names none. */
export function pinnedSeq(mark: DriveMemoMark): number | null {
  return mark.status === 'final' ? mark.final_seq : null;
}

/** The version a final memo pinned, when the memo has been revised past it; null otherwise. */
export function revisedSince(mark: DriveMemoMark, head: number | null): number | null {
  const pinned = pinnedSeq(mark);
  return pinned !== null && head !== null && head > pinned ? pinned : null;
}

/** The parts of the address that pick a file's view. */
export type FileAddress = Pick<ReadLocation, 'tab' | 'version' | 'against'>;

export interface FileView {
  view: MemoView;
  /** The version the view shows in place of the head (the document, or the one Changes shows); null is the head. */
  version: number | null;
  /** The version Changes compares with; null is the one before. */
  against: number | null;
}

/**
 * The one reading of the address for an open file, and the only place it is
 * folded. A memo has four views; a former memo its record beside its
 * document; a plain file its document alone. Only a file with a record has
 * versions to name, and a version that is the head is the head. Changes
 * compares with the version before unless told otherwise, and a comparison
 * with itself, or with a version `kept` no longer holds, is that default. An
 * open editor is the document.
 */
export function resolveFileView(entry: DriveEntry | undefined, at: FileAddress, editing: boolean, kept?: readonly number[] | null): FileView {
  const state = memoState(entry);
  const head = entry?.version ?? null;
  let view: MemoView = 'document';
  if (!editing && state.kind === 'memo') view = at.tab ?? 'document';
  else if (!editing && state.kind === 'former' && at.tab === 'record') view = 'record';
  const named = state.kind !== 'plain' && at.version !== head ? at.version : null;
  const version = view === 'document' || view === 'changes' ? named : null;
  const shown = version ?? head;
  const comparable = at.against !== null && at.against !== shown && (!kept || kept.includes(at.against));
  const against = view === 'changes' && comparable ? at.against : null;
  return { view, version, against };
}

/** A resolved view as the address writes it. */
export function fileAddress(view: FileView): FileAddress {
  return { tab: view.view === 'document' ? null : view.view, version: view.version, against: view.against };
}

/** Until when the file's last save can be undone, as a time; null when it cannot be now. */
export function undoDeadline(entry: Pick<DriveEntry, 'kind' | 'undo_until'>, now: number): number | null {
  if (entry.kind !== 'file' || !entry.undo_until) return null;
  const until = new Date(entry.undo_until).getTime();
  return until > now ? until : null;
}

export interface AuthorRun {
  seq: number;
  version: DriveVersion | undefined;
  lines: string[];
}

/** The text cut into the runs the attribution names, in order. Lines it does not cover are left out. */
export function authorRuns(text: string, attribution: Pick<DriveAttribution, 'runs' | 'versions'>): AuthorRun[] {
  const lines = text.split('\n');
  // A text ending in a newline has no line after it.
  if (lines.length > 1 && lines.at(-1) === '') lines.pop();
  const bySeq = new Map(attribution.versions.map((v) => [v.seq, v]));
  return [...attribution.runs]
    .sort((a, b) => a.start - b.start)
    .map((run) => ({ seq: run.seq, version: bySeq.get(run.seq), lines: lines.slice(run.start - 1, run.start - 1 + run.count) }));
}
