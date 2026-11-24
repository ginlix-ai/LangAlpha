/**
 * One editing session on a Drive text file, as a reducer so its rules are
 * pinned apart from the page.
 *
 * The session's `sitting` is minted when the editor opens and sent with every
 * save until it closes, which is what makes the saves of one sitting one
 * version on the server; nothing in here changes it. `base` is the content the
 * draft was written over, and its hash is each save's `If-Match`. A save the
 * server refuses because the file moved on (412) raises `conflict` and keeps
 * the draft; the other content is the file's head, which the page already
 * reads, so the person chooses: keep theirs, take the other, or compare.
 *
 * Every step of a save carries the request that started it, and the reducer
 * takes it only for the session that sent it: a save that lands after its
 * editor closed, reopened or moved to another file changes nothing.
 */

export interface Content {
  /** The sha256 hex of the bytes, the file's version token. */
  sha: string;
  content: string;
}

/** One save as sent: the session's sitting and file, the text, and the hash it writes over. */
export interface SaveRequest {
  sitting: string;
  path: string;
  text: string;
  ifMatch: string;
}

/** Where a save stands: none in flight, one sent, or one waiting out a busy server to go again. */
export type SaveStatus = 'idle' | 'saving' | 'retrying';

export interface EditSession {
  mode: 'editing';
  /** The file the session is for. */
  path: string;
  sitting: string;
  /** The editor model's path, kept for the session and disposed with it. */
  modelPath: string;
  base: Content;
  /**
   * The hash the last save wrote over. The entry and the head catch up with a
   * save a render after it lands, so for that moment they still name this
   * hash, which is behind the session, not ahead of it.
   */
  replaced: string | null;
  draft: string;
  save: SaveStatus;
  /** When the last save landed, for the quiet "Saved" mark. */
  savedAt: number | null;
  /** A save found the file changed underneath, or the person asked to review the change. */
  conflict: boolean;
}

export type EditState = { mode: 'viewing' } | EditSession;

export type EditAction =
  | { type: 'open'; path: string; sitting: string; modelPath: string; base: Content }
  | { type: 'change'; draft: string }
  | { type: 'saving'; req: SaveRequest }
  /** The server was busy; the same request goes again. */
  | { type: 'retrying'; req: SaveRequest }
  | { type: 'saved'; req: SaveRequest; sha: string; at: number }
  | { type: 'save-failed'; req: SaveRequest }
  /** The server refused the request because the file moved on. */
  | { type: 'conflict'; req: SaveRequest }
  /** The person asked to see the change that landed underneath. */
  | { type: 'review' }
  /** Keep the draft: save it over their content, `req` matched on their hash. */
  | { type: 'keep-mine'; theirs: Content; req: SaveRequest }
  /** Drop the draft for their content and keep editing from it. */
  | { type: 'take-theirs'; theirs: Content }
  /** The file changed while the editor was open with nothing unsaved: edit on from the new content. */
  | { type: 'rebase'; base: Content }
  | { type: 'close' };

export const VIEWING: EditState = { mode: 'viewing' };

export function isDirty(state: EditState): boolean {
  return state.mode === 'editing' && state.draft !== state.base.content;
}

export function editReducer(state: EditState, action: EditAction): EditState {
  if (action.type === 'open') {
    return {
      mode: 'editing',
      path: action.path,
      sitting: action.sitting,
      modelPath: action.modelPath,
      base: action.base,
      replaced: null,
      draft: action.base.content,
      save: 'idle',
      savedAt: null,
      conflict: false,
    };
  }
  if (state.mode !== 'editing') return state;
  if ('req' in action && (action.req.sitting !== state.sitting || action.req.path !== state.path)) return state;
  switch (action.type) {
    case 'change':
      return { ...state, draft: action.draft };
    case 'saving':
      return { ...state, save: 'saving' };
    case 'retrying':
      return { ...state, save: 'retrying' };
    case 'saved':
      return {
        ...state,
        save: 'idle',
        savedAt: action.at,
        base: { sha: action.sha, content: action.req.text },
        replaced: state.base.sha,
        conflict: false,
      };
    case 'save-failed':
      return { ...state, save: 'idle' };
    case 'conflict':
      return { ...state, save: 'idle', conflict: true };
    case 'review':
      return { ...state, conflict: true };
    case 'keep-mine':
      return { ...state, base: action.theirs, conflict: false, save: 'saving' };
    case 'take-theirs':
      return { ...state, base: action.theirs, draft: action.theirs.content, conflict: false };
    case 'rebase':
      // Only a clean draft follows the file; a dirty one waits for the person.
      if (state.draft !== state.base.content || state.save !== 'idle') return state;
      // The text is someone else's now, so the person's last save no longer speaks for it.
      return { ...state, base: action.base, draft: action.base.content, savedAt: null };
    case 'close':
      return VIEWING;
    default:
      return state;
  }
}

/** Whether a hash the page reads is one the session has already moved past. */
export function isPast(session: EditSession, sha: string): boolean {
  return sha === session.base.sha || sha === session.replaced;
}

/** A sitting id for one editor session. */
export function newSitting(): string {
  const id = typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return `web-${id}`;
}
