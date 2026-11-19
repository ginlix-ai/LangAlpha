import type { ShareAudience } from './api';

export type LibraryKind = 'thread' | 'file' | 'skill';

/** One item of the organization library: a thread or file whose link its
 *  publisher keeps on, opened through that link, or a snapshot of a skill a
 *  member installs into their own account. */
export interface LibraryItem {
  item_id: string;
  kind: LibraryKind;
  title: string;
  summary: string;
  publisher_name: string | null;
  /** The viewer published it, so may remove it. */
  mine: boolean;
  /** Where it opens in the app; null when its link has no address. */
  url: string | null;
  /** A file's name; null for a thread. */
  name: string | null;
  audience: ShareAudience;
  published_at: string;
  updated_at: string;
  /** The name a skill installs under; null for a thread or a file. */
  skill_name: string | null;
  /** The viewer installed this skill from the library. */
  installed: boolean;
  /** The viewer's installed copy is older than the library's. */
  update_available: boolean;
}

export interface LibraryPage {
  items: LibraryItem[];
  next_cursor: string | null;
  /** Whether the viewer may publish; the organization may allow only its admins. */
  can_publish: boolean;
}

export interface PublishedLookup {
  item: LibraryItem | null;
  can_publish: boolean;
  /** Set when a shared workspace's managers, not the organization, are why
   *  the caller may not publish it: they let only managers, or no one. */
  workspace_rule?: 'managers' | 'none';
}

export type LibraryTarget =
  | { kind: 'thread'; threadId: string }
  | { kind: 'file'; artifactId: string }
  | { kind: 'skill'; skillName: string };
