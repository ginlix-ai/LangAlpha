import { useReducer, type Dispatch } from 'react';
import { back, enterPlace, isDrill, leave, openLens, PREFIXES, ROOT, typeText, type Frame } from './scope';
import type { Lens, PaletteAction, Place } from './types';

type EditAction = Extract<PaletteAction, { effect: 'edit' }>;

/**
 * What a key or click asked to run, by what it picked: a row (none for the
 * row that leads once the rows settle), Ask (⌘↵), or one of a row's actions.
 * Typing, moving the selection by key or pointer, or changing view while it
 * waits drops it.
 */
export type Wait = { kind: 'row'; id: string | null } | { kind: 'ask' } | { kind: 'action'; of: string; action: string };

interface State {
  /** The views, the one shown last; each one back is what going back returns to. */
  frames: readonly Frame[];
  /**
   * The row whose actions are open (→), or null for the results. By id, so
   * its actions are this render's: one that starts a thread names the place
   * as now known, not as it was when → was pressed.
   */
  actionsOf: string | null;
  actionIndex: number;
  /**
   * One of those actions editing the row in the field (a rename), as it was
   * picked, and the text it holds. It holds what saving needs, so a refetch
   * that drops the row from the list leaves the edit whole. The view under it
   * keeps its field and selection: ending the edit returns to it.
   */
  edit: { action: EditAction; text: string } | null;
  /** What Enter (or ⌘↵) asked for while rows, or the place new work goes, were still on their way. */
  waiting: Wait | null;
  /** The wait has run long enough that a server that may still lead (`serverMayLead`) is no longer waited for. */
  waitedOut: boolean;
  /** The keys typed before the field had focus, as last seen. */
  typed: string;
  open: boolean;
}

export type PaletteDispatch = Dispatch<Event>;

type Event =
  | { type: 'opened'; open: boolean; typed: string }
  | { type: 'buffered'; typed: string }
  | { type: 'type'; text: string }
  | { type: 'choose'; id: string }
  | { type: 'lead'; id: string }
  | { type: 'enter'; place: Place }
  | { type: 'open'; lens: Lens }
  | { type: 'back' }
  | { type: 'leave' }
  | { type: 'actions'; of: string | null }
  | { type: 'actionIndex'; index: number }
  | { type: 'edit'; action: EditAction }
  | { type: 'editText'; text: string }
  | { type: 'endEdit' }
  | { type: 'wait'; wait: Wait | null }
  | { type: 'waitedOut'; wait: Wait };

/** The field's first text: a leading prefix (`$`, `/`, …) opens its kind's view. */
const start = (typed: string, open: boolean): State => ({
  frames: typeText([{ spec: ROOT, query: '', chosenId: null, lead: null }], typed),
  actionsOf: null,
  actionIndex: 0,
  edit: null,
  waiting: null,
  waitedOut: false,
  typed,
  open,
});

/** The views changed: whatever was picked or waiting was for the last one. */
const moved = (state: State, frames: readonly Frame[] | null): State =>
  !frames || frames === state.frames ? state : { ...state, frames, actionsOf: null, edit: null, waiting: null };

const withTop = (state: State, change: Partial<Frame>): State => {
  const top = state.frames[state.frames.length - 1];
  return { ...state, frames: [...state.frames.slice(0, -1), { ...top, ...change }] };
};

function reduce(state: State, event: Event): State {
  const top = state.frames[state.frames.length - 1];
  switch (event.type) {
    case 'opened':
      // Reopened before its close finished, the body was never unmounted: a
      // new open starts afresh all the same.
      return event.open ? start(event.typed, true) : { ...state, open: false };
    case 'buffered': {
      // Keys typed before the field had focus are its first text. More may
      // come after it mounted and before it took focus (an IME's text lands
      // as its composition ends): they replace the text while nothing else
      // edited it, and join it otherwise, as the field would read with a
      // prefix's chip back as the prefix it was typed as. (Only these: a
      // row's place is entered from the field, by which time it has focus
      // and no more keys are buffered.)
      const next = { ...state, typed: event.typed };
      const typedKind = state.frames.length === 2 && top.spec.lens?.via === 'prefix' ? top.spec.lens.kind : null;
      if (state.frames.length !== 1 && !typedKind) return next;
      const shown = typedKind ? `${PREFIXES[typedKind]}${top.query}` : top.query;
      if (shown === state.typed) return reduce(next, { type: 'type', text: event.typed });
      if (event.typed.startsWith(state.typed)) return reduce(next, { type: 'type', text: shown + event.typed.slice(state.typed.length) });
      return next;
    }
    case 'type':
      return { ...state, frames: typeText(state.frames, event.text), actionsOf: null, edit: null, waiting: null };
    case 'choose':
      // A key or the pointer moved the selection: whatever Enter was waiting
      // to run was picked from where it was, so it runs nothing now.
      return { ...withTop(state, { chosenId: event.id }), waiting: null };
    case 'lead':
      return withTop(state, { lead: { query: top.query, id: event.id } });
    case 'enter':
      return moved(state, enterPlace(state.frames, event.place));
    case 'open':
      return moved(state, openLens(state.frames, event.lens));
    case 'back':
      return moved(state, back(state.frames));
    case 'leave':
      return moved(state, leave(state.frames));
    case 'actions':
      return { ...state, actionsOf: event.of, actionIndex: 0, edit: null, waiting: null };
    case 'actionIndex':
      return { ...state, actionIndex: event.index, edit: null, waiting: null };
    case 'edit':
      return state.actionsOf ? { ...state, edit: { action: event.action, text: event.action.value }, waiting: null } : state;
    case 'editText':
      return state.edit ? { ...state, edit: { ...state.edit, text: event.text } } : state;
    case 'endEdit':
      return { ...state, edit: null };
    case 'wait':
      return { ...state, waiting: event.wait, waitedOut: false };
    case 'waitedOut':
      return state.waiting === event.wait ? { ...state, waitedOut: true } : state;
  }
}

/**
 * The palette's view stack and selection: one frame per view (the results,
 * a scope, a kind, a drill-in), each holding its field and selection, a
 * row's actions over the top one, and an edit of that row over its actions.
 * Going back pops. Mounted with the dialog's content, so every open starts
 * from an empty field.
 */
export function usePaletteView(open: boolean, typed: string) {
  const [state, dispatch] = useReducer(reduce, typed, (first) => start(first, open));
  if (open !== state.open) dispatch({ type: 'opened', open, typed });
  else if (typed !== state.typed) dispatch({ type: 'buffered', typed });
  const top = state.frames[state.frames.length - 1];
  return {
    ...state,
    top,
    /** A drill-in is on top: ← and Esc step back out of it. */
    drilled: isDrill(top.spec),
    dispatch,
  };
}
