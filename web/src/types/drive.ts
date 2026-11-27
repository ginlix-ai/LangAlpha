/** Shapes of the Drive API (`/api/v1/drive`). */

export type DriveActorKind = 'person' | 'agent' | 'system';

export interface DriveModifiedBy {
  kind: DriveActorKind;
  user_id: string;
  /** `main` or a subagent's name; null where the caller is not known (a computer's CLI). */
  agent: string | null;
  via: string;
  thread_id: string | null;
  /** The thread's title, where the server names it; null for an untitled or unreadable one, and where a route names none (a folder listing, the trash, the change feed). */
  thread_title: string | null;
}

export interface DriveEntry {
  id: string;
  /** No leading or trailing slash. */
  path: string;
  name: string;
  kind: 'file' | 'folder';
  size: number | null;
  sha256: string | null;
  content_type: string | null;
  is_text: boolean | null;
  /** The head version's seq; null for a folder. */
  version: number | null;
  created_at: string;
  modified_at: string;
  modified_by: DriveModifiedBy | null;
  /** Set on a trashed entry, as a change feed's delete notice. */
  deleted_at: string | null;
  /**
   * The memo mark: null when the file was never a memo; `marked` false once
   * un-marked, its record kept. Read it through `memoState`.
   */
  memo: DriveMemoMark | null;
  /** Until when Undo is offered for the last save; null when it is not. */
  undo_until: string | null;
}

export type DriveMemoStatus = 'draft' | 'final';

export interface DriveMemoMark {
  marked: boolean;
  status: DriveMemoStatus | null;
  tickers: string[];
  /** The version pinned when the status last became final; null unless final. */
  final_seq: number | null;
  /** When the status last became final (the pin's date); null unless final. */
  final_at: string | null;
}

export interface DriveUsage {
  bytes: number;
  entries: number;
  /** Null when the person's allowance has no cap on bytes. */
  max_bytes: number | null;
  /** Bytes only the trash holds. They do not count against `max_bytes`. */
  trash_bytes?: number;
  max_entries: number;
  /** The most one request carries whole. */
  max_file_bytes: number;
  /** The largest file an upload may carry, in parts where the store takes them. */
  max_upload_bytes?: number;
}

export interface DriveSpace {
  id: string;
  kind: 'personal' | 'team' | 'org';
  created_at: string;
  usage: DriveUsage;
  /** The space's change cursor now: where a `changes?since=` poll starts. */
  change_seq: number;
  /** Days a trashed item is kept before it goes for good. */
  trash_days: number;
}

export interface DriveVersionActor extends DriveModifiedBy {
  run_id: string | null;
  workspace_id: string | null;
}

export interface DriveVersion {
  seq: number;
  size: number;
  sha256: string;
  created_at: string;
  actor: DriveVersionActor;
  /** Why the version was made: a person's own note, or the reason of the change it belongs to. */
  note: string | null;
  /**
   * The agent's change (one check-out of the memo, with one reason) this
   * version was saved under; the versions of one change share it and its
   * note. Null for a version made outside one.
   */
  change_id: string | null;
  restored_from: number | null;
  /** Whether this version's bytes are text (it decides how the version opens). */
  is_text: boolean;
  /** When this old version goes; null for the current one and one kept without a time limit. */
  expires_at: string | null;
  /** How this version's text is read for a comparison: its own, read out of a PDF or Word file, or not at all. */
  text: DriveTextSource;
}

/** A file's versions, newest first, and what its history holds. */
export interface DriveVersions {
  versions: DriveVersion[];
  /** Bytes only the old versions hold: what clearing the history frees. */
  history_bytes: number;
  /** The file is too large to keep earlier copies of: saving over it replaces it. */
  too_large_for_history: boolean;
  /** How many versions a clear would drop: every one but the head and what the file keeps besides (a memo's pinned final). */
  clearable: number;
  /** The file's mark as this answer read it, so a clear can say which pinned final its count left out. */
  memo?: DriveMemoMark | null;
}

/** What a person was shown when asked to clear a file's history: the clear is bound to it. */
export interface DriveClearShown {
  /** The current version: only versions below it go. */
  head: number | null;
  /** The pinned final the count left out, null for none: the clear is refused once it has moved. */
  final: number | null;
}

export interface DriveTrashItem {
  trash_id: string;
  /**
   * The item's top entry: the file or folder deleted, or for a burst of
   * deletes by one actor (several top entries in one item) the one with the
   * shortest path.
   */
  entry: DriveEntry;
  /** Every entry the item holds, `entry` and anything under or beside it included. */
  entries: number;
  deleted_at: string;
  deleted_by: string | null;
}

export interface DriveTrashPage {
  items: DriveTrashItem[];
  /** The trash holds more than the page asked for. */
  more: boolean;
}

export interface DriveChanges {
  entries: DriveEntry[];
  cursor: number;
  resync: boolean;
}

/** What the record of a version can be compared on: its own text, text read out of a PDF or Word file, or none. */
export type DriveTextSource = 'text' | 'extracted' | 'none';

/** One version in a file's record, with its line counts against the version before where they are known. */
export interface DriveRecordVersion extends DriveVersion {
  type: 'version';
  added: number | null;
  removed: number | null;
}

interface DriveRecordEventBase {
  type: 'event';
  id: number;
  created_at: string;
  actor: DriveVersionActor;
  note: string | null;
}

/** A change to the memo mark, or a clear of earlier versions, in a file's record. */
export type DriveRecordEvent = DriveRecordEventBase & (
  | { kind: 'unmark' }
  /** The mark, with the status and tickers it was given. */
  | { kind: 'mark'; status_after: DriveMemoStatus | null; tickers_after: string[]; final_seq: number | null }
  /** Versions dropped. */
  | { kind: 'clear'; cleared: number }
  | {
    kind: 'update';
    status_before: DriveMemoStatus | null;
    status_after: DriveMemoStatus | null;
    tickers_before: string[];
    tickers_after: string[];
    /** Set when the event pinned a version as final. */
    final_seq: number | null;
  }
);

export type DriveRecordItem = DriveRecordVersion | DriveRecordEvent;

/** A page of a file's record, newest first; `next` is the cursor of the page after. */
export interface DriveRecordPage {
  items: DriveRecordItem[];
  next: string | null;
  /** Set when the server ran out of time counting lines: reading again fills more of them in. */
  counting?: boolean;
}

export interface DriveMemo {
  entry: DriveEntry;
}

export interface DriveMemoPage {
  memos: DriveMemo[];
  next: string | null;
}

export type DriveMemoSort = 'revised' | 'name' | 'status';

export interface DriveMemoQuery {
  sort: DriveMemoSort;
  order: 'asc' | 'desc';
  status: DriveMemoStatus | null;
  ticker: string | null;
}

/** The text of one version, its own or read out of a PDF or Word file. */
export interface DriveVersionText {
  seq: number;
  sha256: string;
  source: Exclude<DriveTextSource, 'none'>;
  status: 'ok' | 'empty' | 'failed' | 'too_large';
  text: string | null;
  truncated: boolean;
}

/** Which version wrote each run of the current text's lines (1-based, `count` lines from `start`). */
export interface DriveAttribution {
  seq: number;
  sha256: string;
  runs: { start: number; count: number; seq: number }[];
  /** Only the versions the runs name. */
  versions: DriveVersion[];
}

/** The `type` of the error body every Drive route answers with. */
export const DRIVE_ERROR_TYPES = [
  'not_found',
  'exists',
  'changed',
  'is_folder',
  'not_folder',
  'not_empty',
  'invalid_path',
  // A bad ticker, status or page cursor.
  'invalid',
  'invalid_range',
  'too_large',
  'over_quota',
  'too_many_entries',
  'forbidden',
  'unavailable',
  // A file sent in parts.
  'too_many_uploads',
  'upload_ended',
  'parts_missing',
  'bad_part',
  'bad_digest',
  'mismatch',
  // Memos.
  'not_memo',
  'not_text',
] as const;

export type DriveErrorType = (typeof DRIVE_ERROR_TYPES)[number];
