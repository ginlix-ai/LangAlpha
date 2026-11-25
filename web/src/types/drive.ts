/** Shapes of the Drive API (`/api/v1/drive`). */

export type DriveActorKind = 'person' | 'agent' | 'system';

export interface DriveModifiedBy {
  kind: DriveActorKind;
  user_id: string;
  /** `main` or a subagent's name; null where the caller is not known (a computer's CLI). */
  agent: string | null;
  via: string;
  thread_id: string | null;
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
  note: string | null;
  restored_from: number | null;
  /** Whether this version's bytes are text, where the server says (it decides how the version opens). */
  is_text?: boolean;
  /** When this old version goes; null for the current one and one kept without a time limit. */
  expires_at?: string | null;
}

/** A file's versions, newest first, and what its history holds. */
export interface DriveVersions {
  versions: DriveVersion[];
  /** Bytes only the old versions hold: what clearing the history frees. */
  history_bytes: number;
  /** The file is too large to keep earlier copies of: saving over it replaces it. */
  too_large_for_history: boolean;
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

/** The `type` of the error body every Drive route answers with. */
export const DRIVE_ERROR_TYPES = [
  'not_found',
  'exists',
  'changed',
  'is_folder',
  'not_folder',
  'not_empty',
  'invalid_path',
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
] as const;

export type DriveErrorType = (typeof DRIVE_ERROR_TYPES)[number];
