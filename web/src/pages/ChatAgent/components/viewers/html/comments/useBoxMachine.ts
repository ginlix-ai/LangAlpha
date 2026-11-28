import { useEffect, useEffectEvent, useReducer, type Dispatch } from 'react';
import { boxReducer, type BoxAction, type BoxState } from './boxReducer';

interface BoxEffects {
  /** A move put a card or the draft in the list while it was shut. */
  openList: () => void;
  /** A draft holding words selected in the page went (`BoxState.selectionLetGo`). */
  clearSelection: () => void;
}

/**
 * The layer's `boxReducer`, with what its moves do outside it. A move that
 * puts a card or the draft in the list opens the list in the same update,
 * so the list shows them in the commit that took them there: opened from an
 * effect, a commit later, the card would be open nowhere between, and its
 * read asked again once it showed. A selection let go is dropped after the
 * commit, whichever way its draft went, a move made while rendering included.
 *
 * `dispatchBox` is the plain dispatch, for what the layer is told while it
 * renders (the document, the list, the mode), which opens no list.
 */
export function useBoxMachine(
  init: () => BoxState,
  { openList, clearSelection }: BoxEffects,
): { box: BoxState; dispatch: (action: BoxAction) => void; dispatchBox: Dispatch<BoxAction> } {
  const [box, dispatchBox] = useReducer(boxReducer, undefined, init);
  const dispatch = (action: BoxAction) => {
    dispatchBox(action);
    if (boxReducer(box, action).listWanted) openList();
  };
  const letGo = box.selectionLetGo;
  const clear = useEffectEvent(clearSelection);
  useEffect(() => {
    if (letGo > 0) clear();
  }, [letGo]);
  return { box, dispatch, dispatchBox };
}
