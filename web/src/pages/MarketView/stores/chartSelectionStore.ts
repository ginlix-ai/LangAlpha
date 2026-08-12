/**
 * Chart selection store — the regions / price levels the user picked on the
 * MarketView chart to hand to the agent, each with an optional note.
 *
 * This is the reverse of {@link chartAnnotationStore}: that one holds what the
 * agent drew; this one holds what the *user* picked. Unlike that store, this
 * one keeps a *list* — the user can stack several selections (each scoped to the
 * `(symbol, timeframe)` chart it was drawn on) and attach a per-selection note
 * before sending. Notes ride to the agent as each selection's `label`, separate
 * from the chat message text.
 *
 * It is a module singleton (not React context) because the two send sites that
 * read it — the desktop `MarketChatPanel` and the mobile FAB path in
 * `MarketView` — do not share a provider, so both need a context-free snapshot
 * (`getConfirmedFor`) at send time.
 *
 * Lifecycle (Confirm-to-add):
 *   drag/click on chart → `beginDraft()` (status:'pending', inline editor opens)
 *   editor "Add"        → `confirm(id, note)` (status:'confirmed', chip shown)
 *   editor ✕ (discard)  → `remove(id)` (selection removed entirely)
 *   click-outside / Esc → `closeEditor()` if confirmed (kept); `remove(id)` if a
 *                         never-confirmed draft (nothing to keep)
 *   chip / pin click    → `openEditor(id)` (re-edit a confirmed note)
 *   send / instance swap → `clearAll()`
 */

import { useSyncExternalStore } from 'react';

export type SelectionType = 'region' | 'price_level';

/** 'pending' = drawn, editor open, not yet added. 'confirmed' = added (chip + rides on send). */
export type SelectionStatus = 'pending' | 'confirmed';

/** One OHLCV bar inside a region selection. Times are ISO8601 (agent-ready). */
export interface SelectionBar {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface ChartSelection {
  /** Stable id for keying, removal, and editor targeting. */
  id: string;
  /** Uppercased ticker the selection was drawn on. */
  symbol: string;
  /** Normalized, agent-writable timeframe (e.g. '1day'). */
  timeframe: string;
  selectionType: SelectionType;
  /** ISO8601 — region only. */
  timeStart?: string;
  /** ISO8601 — region only. */
  timeEnd?: string;
  priceLow: number;
  /** For `price_level`, equals `priceLow`. */
  priceHigh: number;
  /** OHLCV bars within the region (region only), capped — see MAX_SELECTION_BARS. */
  bars: SelectionBar[];
  /** True when `bars` was downsampled to the cap. */
  barsTruncated: boolean;
  /**
   * Cropped JPEG of the selected region (data URL), region only. Used to build
   * the multimodal image item on send; not carried in `ChartSelectionSnapshot`
   * — the durable replay copy is the persisted attachment, not this base64.
   */
  croppedImage?: string;
  /** The user's note for this selection. Sent to the agent as `label`. */
  comment: string;
  status: SelectionStatus;
}

/** What the chart supplies when a new selection is drawn (the store fills the rest). */
export type DraftSelectionInput = Omit<ChartSelection, 'id' | 'status' | 'comment'> & {
  comment?: string;
};

/**
 * Serializable summary of a confirmed selection attached to a sent user
 * message (mirrors the widget-snapshot pattern). Carries the card-face fields
 * plus the agent-context detail (time bounds + the OHLCV bars the agent
 * received) so the message can render a "how the agent sees it" preview. The
 * region's cropped screenshot is not carried here — it rides the multimodal
 * attachment channel on send and persists as an attachment for replay.
 */
export interface ChartSelectionSnapshot {
  selectionType: SelectionType;
  symbol: string;
  timeframe: string;
  priceLow: number;
  priceHigh: number;
  comment?: string;
  /** ISO8601 — region only. */
  timeStart?: string;
  /** ISO8601 — region only. */
  timeEnd?: string;
  /** OHLCV bars the agent received for this selection (already capped). */
  bars: SelectionBar[];
  /** True when `bars` was downsampled to the cap. */
  barsTruncated: boolean;
}

/**
 * The text to send when the user typed nothing. Falls back to the note of a
 * *single* selection so the user message carries its instruction instead of
 * being blank. With multiple selections we promote nothing — joining their
 * notes into one message would be ambiguous; each note still rides along as
 * its selection's label. Returns `message` unchanged when it already has text,
 * there isn't exactly one selection, or that selection has no note.
 */
export function promoteSelectionComment(message: string, selections: ChartSelection[]): string {
  if (message.trim()) return message;
  if (selections.length !== 1) return message;
  return selections[0].comment.trim() || message;
}

/** Derive the message snapshot (card + agent-context preview) from a selection. */
export function toSelectionSnapshot(sel: ChartSelection): ChartSelectionSnapshot {
  const comment = sel.comment?.trim();
  return {
    selectionType: sel.selectionType,
    symbol: sel.symbol,
    timeframe: sel.timeframe,
    priceLow: sel.priceLow,
    priceHigh: sel.priceHigh,
    ...(comment ? { comment } : {}),
    ...(sel.timeStart ? { timeStart: sel.timeStart } : {}),
    ...(sel.timeEnd ? { timeEnd: sel.timeEnd } : {}),
    bars: sel.bars,
    barsTruncated: sel.barsTruncated,
  };
}

/**
 * True when `sel` is a confirmed selection for the given chart instance — the
 * exact set that rides on send. Symbol match is case-insensitive (selections
 * store an uppercased symbol; callers may pass either case). Shared by the
 * store's `getConfirmedFor` and the chip / overlay / sendable-content readers
 * so the predicate is defined once.
 */
export function isConfirmedFor(
  sel: ChartSelection,
  symbol: string | null | undefined,
  timeframe: string | null | undefined,
): boolean {
  return (
    sel.status === 'confirmed' &&
    sel.symbol === (symbol ?? '').toUpperCase() &&
    sel.timeframe === timeframe
  );
}

interface SelectionState {
  /** All selections (pending + confirmed), in draw order. */
  selections: ChartSelection[];
  /** Id of the selection whose inline editor is open, or null. */
  activeId: string | null;
}

const EMPTY_STATE: SelectionState = { selections: [], activeId: null };

let state: SelectionState = EMPTY_STATE;

const listeners = new Set<() => void>();

function emit(): void {
  for (const listener of listeners) listener();
}

let idCounter = 0;
function nextId(): string {
  idCounter += 1;
  return `sel-${idCounter}`;
}

/**
 * Module-level store API. Safe to call from anywhere — pointer handlers,
 * effects, the chat send path.
 */
export const chartSelectionStore = {
  subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  },

  /** Stable snapshot for `useSyncExternalStore`. */
  getState(): SelectionState {
    return state;
  },

  getAll(): ChartSelection[] {
    return state.selections;
  },

  getActiveId(): string | null {
    return state.activeId;
  },

  /** Confirmed selections for one chart instance — exactly what rides on send. */
  getConfirmedFor(symbol: string | null | undefined, timeframe: string | null | undefined): ChartSelection[] {
    return state.selections.filter((s) => isConfirmedFor(s, symbol, timeframe));
  },

  /**
   * Start a new draft selection and open its inline editor. Discards any prior
   * un-confirmed draft (only one editor open at a time). Returns the new id.
   */
  beginDraft(input: DraftSelectionInput): string {
    const id = nextId();
    const selection: ChartSelection = {
      ...input,
      id,
      comment: input.comment ?? '',
      status: 'pending',
    };
    const kept = state.selections.filter((s) => s.status !== 'pending');
    state = { selections: [...kept, selection], activeId: id };
    emit();
    return id;
  },

  /** Add a draft to the context (the editor's "Add"): save note, mark confirmed, close editor. */
  confirm(id: string, comment: string): void {
    let changed = false;
    const selections = state.selections.map((s) => {
      if (s.id !== id) return s;
      changed = true;
      return { ...s, comment, status: 'confirmed' as const };
    });
    if (!changed) return;
    state = { selections, activeId: state.activeId === id ? null : state.activeId };
    emit();
  },

  /** Update a (confirmed) selection's note — the editor's "Save". */
  setComment(id: string, comment: string): void {
    let changed = false;
    const selections = state.selections.map((s) => {
      if (s.id !== id) return s;
      changed = true;
      return { ...s, comment };
    });
    if (!changed) return;
    state = { ...state, selections };
    emit();
  },

  /** Remove one selection (the editor's "Cancel" on a draft, or a chip's ✕). */
  remove(id: string): void {
    if (!state.selections.some((s) => s.id === id)) return;
    state = {
      selections: state.selections.filter((s) => s.id !== id),
      activeId: state.activeId === id ? null : state.activeId,
    };
    emit();
  },

  /** Open the inline editor for a selection (re-edit a confirmed note). */
  openEditor(id: string): void {
    if (state.activeId === id) return;
    if (!state.selections.some((s) => s.id === id)) return;
    state = { ...state, activeId: id };
    emit();
  },

  /** Close the inline editor without changing the selections. */
  closeEditor(): void {
    if (state.activeId === null) return;
    state = { ...state, activeId: null };
    emit();
  },

  /** Drop every selection (on send, or when the chart instance changes). */
  clearAll(): void {
    if (state.selections.length === 0 && state.activeId === null) return;
    state = EMPTY_STATE;
    emit();
  },

  /** Test-only: wipe state + ids and notify (keeps live subscriptions intact). */
  _resetForTesting(): void {
    state = EMPTY_STATE;
    idCounter = 0;
    emit();
  },
};

/** Subscribe a component to the full selection state. */
export function useChartSelections(): SelectionState {
  return useSyncExternalStore(
    chartSelectionStore.subscribe,
    chartSelectionStore.getState,
    () => EMPTY_STATE,
  );
}
