import type { ArtifactComment, ArtifactCommentAnchor } from '@/types/api';
import type { CommentTarget } from '../../../../utils/api/artifacts';
import type { FrameRect, PageHover, PagePick } from '../pageProtocol';
import { filterShowing, showsDraft, type ListFilter } from '../../../artifactComments/commentModel';

export interface Draft {
  anchor: ArtifactCommentAnchor;
  /** Where the spot was when picked, until the page reports where it is now. */
  rect: FrameRect;
  /** The file it is on: the one the frame held when it was picked. */
  path: string;
  /** That file's title when it was picked, null where it has none. */
  title: string | null;
  /**
   * Words picked in comment mode. A page that cannot mark them keeps them
   * selected instead, which the draft clears when it goes.
   */
  selected?: boolean;
}

/**
 * What is open over the page: a new comment being written, or a comment's
 * card. A draft picked again elsewhere is a new box, numbered, so the old one
 * leaves where it was while the words move to the new one.
 */
export type Open = { kind: 'draft'; draft: Draft; serial: number } | { kind: 'card'; id: string };

/** Which box a close is for: the draft by its number, a card by its comment. */
export type BoxOf = { kind: 'draft'; serial: number } | { kind: 'card'; id: string };

/**
 * What is open over the page and in the list, and the words being written.
 * One state for both, since most moves change both: a card opened from the
 * chat goes to the list when the list shows, and a draft carried onto a
 * version that hides its spot waits there.
 */
export interface BoxState {
  /** The document the state was last told of: what the page says belongs to the one that said it. */
  doc: string;
  /** Whether the list showed when last told. */
  listOpen: boolean;
  /** A card or the draft went to the list while it was shut: the layer opens it, and the list showing settles this. */
  listWanted: boolean;
  open: Open | null;
  /** The last draft's number. */
  serial: number;
  /** A comment being written that waits in the list: the version it was carried onto shows its spot out of view, or not at all. */
  listed: { draft: Draft; serial: number } | null;
  /** The words being written, in the draft's box or its card in the list. */
  text: string;
  /** "For the agent" on the comment being written, which moves with its words. */
  forAgent: boolean;
  /** Where it goes when not the default. */
  to: CommentTarget | null;
  /** The owner asked to leave a draft with words in it: its footer asks whether to throw them away. */
  confirmDiscard: boolean;
  /**
   * In comment mode, what is under the pointer. Dropped when a box goes, as
   * it was for the spot under the pointer before the box opened; the page
   * reports the next one as the pointer moves back into it.
   */
  hover: PageHover | null;
  /** A card the chat asked for whose pin is still being scrolled into view; a new ask is a new object. */
  landing: { id: string } | null;
  /** The card the chat brought forward, which marks its arrival once. */
  arrivalId: string | null;
  /** The pin the owner just made, which pops out of the draft's spot. */
  madeId: string | null;
  /** The document a box stayed open into, until it has said where the box's pin is now. */
  carried: string | null;
  filter: ListFilter;
  /** The comment whose card is open in the list. */
  expandedId: string | null;
}

export type BoxAction =
  /** The file reloaded with the agent's edit, or a link moved the frame to another. */
  | { type: 'docChanged'; doc: string }
  | { type: 'listShown'; open: boolean; comments: readonly ArtifactComment[] }
  | { type: 'hover'; hover: PageHover | null }
  /** A spot picked on the page at `path`, or words: a new draft's box opens on it. */
  | { type: 'pick'; pick: PagePick; path: string; selected: boolean }
  /** A press on a pin opens its card. */
  | { type: 'pinPressed'; id: string }
  /** The chat brought a comment forward whose pin is in view. */
  | { type: 'brought'; id: string }
  /** The chat brought a comment forward whose pin is scrolled to first. */
  | { type: 'awaitLanding'; id: string }
  /** The scroll landed the pin, or gave up; null when the comment went meanwhile. */
  | { type: 'landed'; id: string | null }
  | { type: 'closeBox'; of: BoxOf; keepHover: boolean }
  | { type: 'endDraft'; keepHover: boolean }
  /** A press on the page, as a click away does in a design tool. */
  | { type: 'pressAway' }
  /** Esc in a draft, or its card's close in the list. */
  | { type: 'leave' }
  | { type: 'askDiscard' }
  | { type: 'keepWriting' }
  | { type: 'write'; text: string }
  | { type: 'forAgent'; on: boolean }
  | { type: 'to'; target: CommentTarget | null }
  /** The draft's box goes into the list, its words with it. */
  | { type: 'listDraft' }
  /** A box carried onto a new document has its pin in view there. */
  | { type: 'placed' }
  | { type: 'saved'; id: string }
  /** Open a comment's card in the list; one the list keeps whatever its filter says keeps the filter. */
  | { type: 'expand'; comment: ArtifactComment; kept: boolean }
  | { type: 'collapse' }
  | { type: 'filter'; filter: ListFilter }
  | { type: 'deleted' };

export function initialBox({ doc, listOpen }: { doc: string; listOpen: boolean }): BoxState {
  return {
    doc,
    listOpen,
    listWanted: false,
    open: null,
    serial: 0,
    listed: null,
    text: '',
    forAgent: true,
    to: null,
    confirmDiscard: false,
    hover: null,
    landing: null,
    arrivalId: null,
    madeId: null,
    carried: null,
    filter: 'open',
    expandedId: null,
  };
}

/** Whether a press on the page closes or asks something, which then swallows the pick its click reports. */
export function pressCloses(state: BoxState): boolean {
  return state.open !== null;
}

/** A new box, or none, with nothing left of how the last one came or went. */
function show(state: BoxState, open: Open | null): BoxState {
  return { ...state, open, landing: null, arrivalId: null, confirmDiscard: false, carried: null };
}

/**
 * The draft is done with: saved, or let go. A press on the page keeps the
 * outline (`keepHover`), since the pointer is on it already.
 */
function endDraft(state: BoxState, keepHover: boolean): BoxState {
  return {
    ...show(state, null),
    listed: null,
    text: '',
    forAgent: true,
    to: null,
    hover: keepHover ? state.hover : null,
  };
}

/**
 * Closes this box if it is still the open one. Radix can report a box
 * dismissed after the press that dismissed it has opened another (a chat
 * line naming a comment, pressed while a card is open), and a close that
 * took whatever was open then shut the card just asked for.
 */
function closeBox(state: BoxState, of: BoxOf, keepHover: boolean): BoxState {
  const prev = state.open;
  const mine = of.kind === 'card'
    ? prev?.kind === 'card' && prev.id === of.id
    : prev?.kind === 'draft' && prev.serial === of.serial;
  const next = { ...state, open: mine ? null : prev, hover: keepHover ? state.hover : null };
  if (of.kind === 'card') return { ...next, arrivalId: state.arrivalId === of.id ? null : state.arrivalId };
  // A draft is only dismissed with no words in it; words there now are the
  // next draft's, carried over from this one.
  return { ...next, text: state.text.trim() ? state.text : '' };
}

function expand(state: BoxState, comment: ArtifactComment, kept: boolean): BoxState {
  return {
    ...show(state, null),
    listWanted: !state.listOpen,
    filter: kept ? state.filter : filterShowing(comment, state.filter),
    expandedId: comment.comment_id,
  };
}

export function boxReducer(state: BoxState, action: BoxAction): BoxState {
  switch (action.type) {
    case 'docChanged':
      // A comment being written outlives the document: the agent can edit the
      // file while the owner types, and the words are theirs. Its spot is
      // looked for again on the new document, and it stays on the file it was
      // picked on. Its box stays where it was, as an open card's does, until
      // the new document says where its pin is now.
      return { ...state, doc: action.doc, hover: null, madeId: null, carried: state.open ? action.doc : null };
    case 'listShown': {
      // Opening the list takes the open card into it, where cards open while it
      // shows. Closing it lets an empty draft waiting there go; one with words
      // waits for the list to open again.
      const next = { ...state, listOpen: action.open, listWanted: false };
      const open = state.open;
      if (action.open && open?.kind === 'card') {
        const comment = action.comments.find((c) => c.comment_id === open.id);
        return {
          ...show(next, null),
          filter: comment ? filterShowing(comment, state.filter) : state.filter,
          expandedId: open.id,
        };
      }
      if (action.open && state.listed) return showsDraft(state.filter) ? next : { ...next, filter: 'open' };
      if (!action.open && state.listed && !state.text.trim()) return { ...next, listed: null };
      return next;
    }
    case 'hover':
      return { ...state, hover: action.hover };
    case 'pick': {
      // Words already written move with the box to the new spot, and whether
      // they are for the agent and where with them; a new comment starts for
      // the agent, to the default.
      const fresh = !state.text.trim();
      const draft: Draft = { anchor: action.pick.anchor, rect: action.pick.rect, path: action.path, title: action.pick.title };
      if (action.selected) draft.selected = true;
      const serial = state.serial + 1;
      return {
        ...show(state, { kind: 'draft', draft, serial }),
        serial,
        listed: null,
        forAgent: fresh ? true : state.forAgent,
        to: fresh ? null : state.to,
      };
    }
    case 'pinPressed':
      // Words left in an empty box go; a draft waiting in the list keeps its own.
      return { ...show(state, { kind: 'card', id: action.id }), text: state.listed ? state.text : '' };
    case 'brought':
      return { ...show(state, { kind: 'card', id: action.id }), arrivalId: action.id };
    case 'awaitLanding':
      return { ...show(state, null), landing: { id: action.id } };
    case 'landed':
      if (action.id === null) return { ...state, landing: null };
      return { ...state, landing: null, open: { kind: 'card', id: action.id }, arrivalId: action.id };
    case 'closeBox':
      return closeBox(state, action.of, action.keepHover);
    case 'endDraft':
      return endDraft(state, action.keepHover);
    case 'pressAway':
      // The spot under the pointer stays outlined, so the outline glides there
      // from the picked one: the page reported it while the box was open, and
      // reports each spot once. A draft waiting in the list is not over the page.
      if (state.open?.kind === 'draft') {
        // As a press outside the box on this side asks: a second press does
        // not answer for the reader.
        return state.text.trim() ? { ...state, confirmDiscard: true } : endDraft(state, true);
      }
      if (state.open?.kind === 'card') return closeBox(state, state.open, true);
      return state;
    case 'leave':
      // Words already written are not thrown away without asking.
      return !state.text.trim() || state.confirmDiscard ? endDraft(state, false) : { ...state, confirmDiscard: true };
    case 'askDiscard':
      return { ...state, confirmDiscard: true };
    case 'keepWriting':
      return { ...state, confirmDiscard: false };
    case 'write':
      return { ...state, text: action.text, confirmDiscard: false };
    case 'forAgent':
      return { ...state, forAgent: action.on };
    case 'to':
      return { ...state, to: action.target };
    case 'listDraft': {
      const open = state.open;
      if (open?.kind !== 'draft') return state;
      return {
        ...show(state, null),
        listWanted: !state.listOpen,
        listed: { draft: open.draft, serial: open.serial },
        filter: showsDraft(state.filter) ? state.filter : 'open',
      };
    }
    case 'placed':
      return { ...state, carried: null };
    case 'saved':
      return { ...endDraft(state, false), madeId: action.id };
    case 'expand':
      return expand(state, action.comment, action.kept);
    case 'collapse':
      return { ...state, expandedId: null };
    case 'filter':
      return { ...state, filter: action.filter };
    case 'deleted':
      return { ...show(state, null), expandedId: null };
    default:
      return state;
  }
}
