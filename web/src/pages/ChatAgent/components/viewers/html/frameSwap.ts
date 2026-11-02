/**
 * The documents a bridged frame holds while one replaces another: the one
 * loading (`current`) and the one the reader still sees (`behind`). Pure, so
 * `BridgedFrame` can read the state a pass is about to have before React
 * has it.
 */

/** A document as it was framed: the address is pinned when the key changes. */
export interface FramedDoc {
  key: string;
  src: string;
  /** The origin its messages arrive from (`messageOrigin`). */
  origin: string;
  sandbox: string;
}

/** What is known of the document loading, each a fact that stays once true. */
interface Loading {
  doc: FramedDoc;
  loaded: boolean;
  /** Its bridge said it finished building itself. */
  settled: boolean;
  /** Put back where the reader was: it has grown at least that far. */
  landed: boolean;
  /** Took too long to go still; it replaces the one on screen anyway. */
  overdue: boolean;
  /** Ready, and waited out the few frames a fresh frame is behind its own picture. */
  held: boolean;
}

export interface FrameSwap {
  current: Loading | null;
  behind: FramedDoc | null;
}

export type FrameSwapAction =
  | { type: 'frame'; doc: FramedDoc }
  | { type: 'loaded' | 'settled' | 'landed' | 'overdue' | 'held'; key: string }
  /** The document behind has been faded over: drop it. */
  | { type: 'drop'; key: string };

export const NO_SWAP: FrameSwap = { current: null, behind: null };

/** Ready to replace the one on screen: loaded, and with one behind it, built or past waiting. */
export function isReady({ current, behind }: FrameSwap): boolean {
  if (!current?.loaded) return false;
  return !behind || current.settled || current.landed || current.overdue;
}

/** On screen: the first load as soon as it is in, a later one once it has waited out the hold. */
export function isShown(state: FrameSwap): boolean {
  return isReady(state) && (!state.behind || !!state.current?.held);
}

function fresh(doc: FramedDoc): Loading {
  return { doc, loaded: false, settled: false, landed: false, overdue: false, held: false };
}

export function frameSwap(state: FrameSwap, action: FrameSwapAction): FrameSwap {
  const { current, behind } = state;
  switch (action.type) {
    case 'frame': {
      if (current?.doc.key === action.doc.key) return state;
      // The document behind asked for again is the one on screen, already shown.
      if (behind?.key === action.doc.key) {
        return { current: { doc: behind, loaded: true, settled: true, landed: true, overdue: true, held: true }, behind: null };
      }
      // Only a document that was shown is worth holding: one replaced before
      // it was ready leaves the one before it on screen.
      return { current: fresh(action.doc), behind: current && isShown(state) ? current.doc : behind };
    }
    case 'drop':
      return behind?.key === action.key ? { current, behind: null } : state;
    default: {
      if (current?.doc.key !== action.key || current[action.type]) return state;
      return { current: { ...current, [action.type]: true }, behind };
    }
  }
}
