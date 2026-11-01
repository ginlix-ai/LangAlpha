/**
 * A workspace file's record: one per file, one `/a/<code>`, its published
 * versions and the owner's comments on the live file. Every path here is
 * workspace-relative.
 * `types/api.ts` re-exports them all.
 */

/** Who cut a version: the owner's Publish or Update, the end of a turn for a file kept updated, or the snapshot of a link shared before versions. */
export type ArtifactVersionOrigin = 'owner' | 'keep_updated' | 'legacy';

/** One published version: an immutable copy of the file and the files it reads. */
export interface ArtifactVersion {
  seq: number;
  /** The artifact's path when the version was cut. */
  entry_path: string;
  origin: ArtifactVersionOrigin;
  /** The entry document's `<title>` when cut. */
  title: string | null;
  /** The owner's note on Publish or Update. */
  label: string | null;
  byte_len: number;
  published_at: string;
}

/** A file's record, as its owner reads it: 404 when it is not theirs. */
export interface Artifact {
  artifact_id: string;
  code: string;
  /** App-relative, `/a/<code>`. */
  url: string;
  workspace_id: string;
  /** Workspace-relative, canonical. */
  path: string;
  name: string;
  /** Whether visitors can open the link; they see the latest version. */
  shared: boolean;
  shared_at: string | null;
  keep_updated: boolean;
  /** 0 until the first publish. */
  latest_seq: number;
  /** Where its comments go until unbound. */
  bound_thread_id: string | null;
  open_comments: number;
  /** Comments holding an agent message the owner has not read (comments, not messages). */
  unread: number;
  created_at: string;
  /**
   * A link shared before versions whose files are past the caps: visitors get a
   * 404 until the owner publishes (with the files under the version caps).
   */
  legacy_refused?: 'too_many_files' | 'too_many_bytes' | null;
}

/** `GET /workspaces/{id}/artifacts?path=`: null for a file never recorded. */
export interface ArtifactLookup {
  artifact: Artifact | null;
}

/** A thread a comment could go to. */
export interface ArtifactThread {
  thread_id: string;
  title: string | null;
  workspace_id: string;
  /** The owner's, not archived, and in the artifact's workspace or the owner's Home. */
  usable: boolean;
  /** A Chief of Staff thread, in the owner's Home. */
  home: boolean;
}

/** Which rule picked the default: bound thread, the thread beside the panel, the creator, or a new thread. */
export type ArtifactDestinationRule = 'bound' | 'beside' | 'creator' | 'new';

/** Where a comment goes unless the owner picks another thread, and the candidates for the "To:" menu. */
export interface ArtifactDestinations {
  default: {
    rule: ArtifactDestinationRule;
    /** Null with rule `new`. */
    thread: ArtifactThread | null;
    /** Where a new thread would be made. */
    workspace_id: string;
  };
  bound: ArtifactThread | null;
  beside: ArtifactThread | null;
  creator: ArtifactThread | null;
}

export interface ArtifactDrift {
  added: string[];
  removed: string[];
  changed: string[];
}

/** Why a file goes with a publish: the entry itself, or what it links, styles, reads or runs. */
export type ShareFileReason = 'entry' | 'page' | 'style' | 'markdown' | 'script';

export interface ShareFileEntry {
  path: string;
  size: number;
  reason: ShareFileReason;
}

/** The live file against the latest version: what the crumb's "changed since" and the Publish dialog read. */
export interface ArtifactStatus {
  latest_seq: number;
  /** The entry as served now, secrets redacted; equals a version file's `sha256` when unchanged. */
  live_sha256: string;
  /** What a publish now would carry, the entry first. */
  files: ShareFileEntry[];
  total_size: number;
  /** Null before the first publish. */
  changed: boolean | null;
  drift: ArtifactDrift | null;
}

export interface PublishArtifactRequest {
  /** The paths the owner reviewed (`ArtifactStatus.files`); a mismatch is 409 `files_changed`. */
  files: string[];
  label?: string | null;
  /** Left out, the setting is kept; a new share starts with it off. */
  keep_updated?: boolean;
}

export interface PublishArtifactResponse {
  artifact: Artifact;
  /** The version cut, or the latest when nothing changed. */
  version: ArtifactVersion;
  /** False when the files equal the latest version's. */
  cut: boolean;
}

/**
 * Where the owner's viewer frames a published version from (read only).
 * The frame URL is `base + 'v/' + seq + '/' + <entry path>`.
 */
export interface ArtifactGrant {
  /** Absolute on the artifact's own origin when `isolated`, else a path relative to the API base. */
  base: string;
  expires_in: number;
  /** Whether the version has an origin of its own, and so storage that lasts. */
  isolated: boolean;
}

/** One element of an area, named as an element anchor names it. */
export interface ArtifactCommentAreaElement {
  path?: string;
  label?: string;
  quote?: string;
}

/** Where on an anchored element, or area, the pin sits, as fractions of its box. */
export interface AnchorPoint {
  x: number;
  y: number;
}

/** The fields an element, text or named anchor may carry; an area's are not among them. */
interface SpotAnchorFields {
  /** A CSS path from the nearest ancestor with an id. */
  path?: string;
  label?: string;
  /** The selected words, or the element's text as it showed. */
  quote?: string;
  prefix?: string;
  suffix?: string;
  detail?: string;
  point?: AnchorPoint;
  elements?: never;
  count?: never;
  rect?: never;
}

/** A clicked element, found again by its path or its text. */
export interface ElementAnchor extends SpotAnchorFields {
  kind: 'element';
}

/** Selected words, found again by them and the words around them. */
export interface TextAnchor extends SpotAnchorFields {
  kind: 'text';
}

/** A spot the file names itself. */
export interface NamedAnchor extends SpotAnchorFields {
  kind: 'named';
}

/** An area dragged around several elements. */
export interface AreaAnchor {
  kind: 'area';
  /** Its first elements (2 to 8), in document order. */
  elements: ArtifactCommentAreaElement[];
  /** How many elements it holds in all. */
  count: number;
  /** Names its parts. */
  label?: string;
  point?: AnchorPoint;
  /** Where it sat, as fractions of the document's scroll size. */
  rect?: { x: number; y: number; w: number; h: number };
  path?: never;
  quote?: never;
  prefix?: never;
  suffix?: never;
  detail?: never;
}

/**
 * What the owner pinned a comment to on the live file, read off it so it can
 * be found again after the file changes. The comment's `path` names the file.
 */
export type ArtifactCommentAnchor = ElementAnchor | TextAnchor | NamedAnchor | AreaAnchor;

export interface ArtifactCommentMessage {
  message_id: string;
  /** The message's place in its comment's thread; what a read cursor counts. */
  seq: number;
  author: 'user' | 'agent';
  body: string;
  /** When a send carried an owner's message; null while none has. */
  sent_at: string | null;
  request_key: string | null;
  /** An owner's message: the thread it was sent to. An agent's: the thread it answered from. */
  thread_id: string | null;
  created_at: string;
}

/**
 * `note` is never sent and has no thread; `queued` waits for its thread's next
 * send; `sent` means everything the owner wrote in it has reached the agent.
 */
export type ArtifactCommentDelivery = 'note' | 'queued' | 'sent';

export interface ArtifactComment {
  comment_id: string;
  artifact_id: string;
  /** The pin's number on the file, never reused. */
  number: number;
  /** The file's path when the comment was made. */
  path: string;
  /** The live file's hash when made; compare with `ArtifactStatus.live_sha256`. */
  file_sha256: string | null;
  /** The document's title when made. */
  title: string | null;
  anchor: ArtifactCommentAnchor;
  status: 'open' | 'resolved';
  delivery: ArtifactCommentDelivery;
  /** Queued: the thread it waits for. Sent: the thread it last went to. Null for a note, or once that thread is deleted. */
  thread_id: string | null;
  thread_title: string | null;
  thread_archived: boolean;
  created_at: string;
  updated_at: string;
  resolved_at: string | null;
  messages: ArtifactCommentMessage[];
  /** The `seq` of the newest message the owner has seen; 0 for none. */
  read_through: number;
  /** The agent's messages past `read_through`. */
  unread: number;
}

/** A comment waiting for a thread's next send, on any artifact. */
export interface QueuedArtifactComment extends ArtifactComment {
  workspace_id: string;
}

export interface CreateArtifactCommentRequest {
  anchor: ArtifactCommentAnchor;
  body: string;
  delivery: 'note' | 'queued';
  /** Required to queue (409 `destination_required` without it). */
  thread_id?: string;
  /** `ArtifactStatus.live_sha256` as the view loaded it. */
  file_sha256?: string;
  /** The document's title as the bridge read it. */
  title?: string;
}

export interface AddArtifactCommentMessageRequest {
  body: string;
  /** Left out, the comment keeps waiting where it was, or becomes a note once everything before was sent. */
  delivery?: 'note' | 'queued';
  /** Where it waits when queued; defaults to the thread it already waits for. */
  thread_id?: string;
}

export interface UpdateArtifactCommentRequest {
  delivery?: 'note' | 'queued';
  /** Moves a queued comment to another thread's queue. */
  thread_id?: string;
  status?: 'open' | 'resolved';
}

/** Where the person reads when they send comments: the chat on screen, or not (panel expanded, phone overlay, `/a/`, another thread). */
export type ArtifactReading = 'chat' | 'file';

/** What a chat send adds to carry comments. */
export interface ArtifactCommentSendFields {
  /** Up to 50 distinct ids; needs `request_key`, and not on a HITL answer, edit or regenerate. */
  artifact_comment_ids?: string[];
  /** Required with `artifact_comment_ids`. */
  reading?: ArtifactReading;
}

/**
 * The comments a send carries, as the client keeps them for the request and
 * for a resend of it: which, and where the person read as they went
 * (`commentSendOf` in `artifactComments/artifactCommentSend`).
 */
export interface CommentSend {
  ids: string[];
  reading: ArtifactReading;
}

/** One comment as a sent message shows it in place of the rendered block. */
export interface ShownArtifactComment {
  comment_id: string;
  artifact_id: string;
  workspace_id: string;
  path: string;
  title: string | null;
  number: number;
  /** The words this send carried, clipped to 500. */
  text: string;
  follow_up: boolean;
  label: string | null;
  quote: string | null;
}

/** `artifact_comments` on a query row's metadata and on steer events. */
export interface ArtifactCommentsShown {
  /** What the person typed. */
  note: string;
  reading: ArtifactReading;
  comments: ShownArtifactComment[];
}

/** User-feed event: an artifact's comments changed. */
export interface ArtifactCommentEvent {
  v: 1;
  type: 'artifact_comment';
  artifact_id: string;
  workspace_id: string;
  path: string;
  /** The destination of the changed comments, whose queue to read again. */
  thread_id: string | null;
  comment_ids: string[];
}

/** User-feed event: an artifact's record changed (publish, Update, keep updated, unshare, binding). */
export interface ArtifactRecordEvent {
  v: 1;
  type: 'artifact';
  artifact_id: string;
  workspace_id: string;
  path: string;
  shared: boolean;
  latest_seq: number;
}

/** `/a/<code>` for the owner of a file link: the live file with comments. */
export interface SharedArtifactOwnerMetadata {
  kind: 'file';
  access: 'owner';
  artifact_id: string;
  workspace_id: string;
  name: string;
  path: string;
  shared: boolean;
  latest_seq: number;
  /** The owner's wsfiles grant prefix; frame `buildServeUrl(frame_base, path)` with `?inject=comments`. */
  frame_base: string;
  expires_in: number;
}

/** `/a/<code>` for anyone else: the latest version, read only. */
export interface SharedArtifactVisitorMetadata {
  kind: 'file';
  access: 'public';
  artifact_id: string;
  name: string;
  /** The version's entry path. */
  path: string;
  title: string | null;
  seq: number;
  /** Frame `frame_base + 'v/' + seq + '/' + <path>`, sandboxed as `ArtifactGrant`. */
  frame_base: string;
  isolated: boolean;
  expires_in: number;
}

export type SharedArtifactMetadata = SharedArtifactOwnerMetadata | SharedArtifactVisitorMetadata;
