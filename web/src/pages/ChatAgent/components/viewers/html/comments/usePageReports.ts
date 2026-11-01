import { useEffect, useEffectEvent, useMemo, useRef, useState } from 'react';
import type { FrameView } from '../BridgedFrame';
import type { FrameRect, PageAnchorItem, PageHover, PagePick, PageSelection, PageToViewer } from '../pageProtocol';
import type { PageBridge } from '../usePageBridge';
import { DRAFT_ID } from './pinPlaces';

const NO_POSITIONS: ReadonlyMap<string, FrameRect | null> = new Map();

/** What the owner does on the page that opens or closes something over it, which the layer decides. */
export interface PageActs {
  /** The pointer moved onto something else in comment mode; null over nothing, and always outside the mode. */
  onHover: (hover: PageHover | null) => void;
  /** A press on the page: whether it closed or asked anything, which swallows the pick its own click reports. */
  onPress: () => boolean;
  /** An element, an area or words picked in comment mode; `selected` for words. */
  onPick: (pick: PagePick, selected: boolean) => void;
  /** C or Esc pressed inside the frame, taken as they are outside it. */
  onKey: (key: 'Escape' | 'c') => void;
}

interface PageReportsOptions extends PageActs {
  /** The spots to find on the document, whose positions it reports back. */
  anchors: PageAnchorItem[];
  /** The draft's number: its spot, reported under one id whatever draft it is, counts only once reported for this one. */
  draftSerial: number;
}

/** What the page reports, scoped to the document that said it. */
export interface PageReports {
  /** The document on screen: the frame's session and the view's key. */
  doc: string;
  /**
   * Where the page last said each spot is. The last document's answer stays
   * drawn, faded out, until the next one has its own: pins then ride a
   * version change rather than blinking out.
   */
  positions: ReadonlyMap<string, FrameRect | null>;
  /** `positions` is the answer of the document on screen. */
  fresh: boolean;
  /** The document that last finished building itself. */
  settledDoc: string | null;
  /** The document on screen finished building itself: a spot it still does not show is not coming. */
  settled: boolean;
  /** Words selected outside comment mode, which the chip offers. */
  selection: PageSelection | null;
  /** The rectangle the reader is dragging out, while they do. */
  areaDrag: FrameRect | null;
  scrolling: boolean;
  /**
   * Counts the pointer's moves onto the page, where this document stops
   * hearing it: a pin it rested on lets its preview go.
   */
  pointerIn: number;
  /** A spot drawn at `rect` until the page reports it: a new pin in the draft's place. */
  placeSpot: (id: string, rect: FrameRect) => void;
  clearSelection: () => void;
}

/** Reports that hold only for the document that made them. */
interface Scoped {
  doc: string;
  selection: PageSelection | null;
  areaDrag: FrameRect | null;
  scrolling: boolean;
}

function emptyScope(doc: string): Scoped {
  return { doc, selection: null, areaDrag: null, scrolling: false };
}

/**
 * The page's side of the comment layer: the spots it is asked to find and
 * whether clicks pick, and what it reports back. The document only reports:
 * what is under the pointer, what was pressed, clicked or selected, where each
 * anchor sits, how it scrolls and when it has finished building itself. What
 * a report means for the boxes over the page is the layer's (`PageActs`).
 *
 * What the document told us holds for the document that told it: the file
 * reloaded with the agent's edit, or a link to another file, starts over,
 * so each report is kept with the document it came from.
 */
export function usePageReports(
  bridge: PageBridge,
  view: FrameView,
  mode: boolean,
  { anchors, draftSerial, ...acts }: PageReportsOptions,
): PageReports {
  const doc = `${bridge.session}:${view.key}`;
  const [scoped, setScoped] = useState<Scoped>(() => emptyScope(doc));
  const current = scoped.doc === doc ? scoped : emptyScope(doc);
  const update = (patch: Partial<Omit<Scoped, 'doc'>>) => {
    setScoped((prev) => ({ ...(prev.doc === doc ? prev : emptyScope(doc)), ...patch }));
  };
  const [reported, setReported] = useState<{ doc: string; serial: number; map: ReadonlyMap<string, FrameRect | null> }>(
    () => ({ doc, serial: draftSerial, map: NO_POSITIONS }),
  );
  const [settledDoc, setSettledDoc] = useState<string | null>(null);
  const [pointerIn, setPointerIn] = useState(0);
  // A draft picked since the last report has its spot looked for afresh.
  const positions = useMemo(() => {
    if (reported.serial === draftSerial || !reported.map.has(DRAFT_ID)) return reported.map;
    const map = new Map(reported.map);
    map.delete(DRAFT_ID);
    return map;
  }, [reported, draftSerial]);
  const fresh = reported.doc === doc;

  const asked = useMemo(() => new Set(anchors.map((item) => item.id)), [anchors]);
  const anchorsKey = useMemo(() => JSON.stringify(anchors), [anchors]);
  const postAnchors = useEffectEvent(() => bridge.post({ type: 'page:anchors', items: anchors }));
  useEffect(() => {
    if (bridge.ready) postAnchors();
  }, [bridge, anchorsKey]);

  useEffect(() => {
    if (bridge.ready) bridge.post({ type: 'page:comment-mode', on: mode });
  }, [bridge, mode]);

  // The number of the last press that closed or asked something. The pick
  // its own click reports carries that number and is swallowed rather than
  // opening a box, so the next click picks. Any other pick, a key's (which
  // has no press) or one after a drag or a press outside the mode, picks.
  const closingPress = useRef<number | null>(null);
  const opens = (press: number | null) => mode && (press === null || press !== closingPress.current);

  const onMessage = useEffectEvent((message: PageToViewer) => {
    switch (message.type) {
      case 'page:hover':
        acts.onHover(mode ? message.hover : null);
        return;
      case 'page:press':
        closingPress.current = acts.onPress() ? message.press : null;
        return;
      case 'page:pick':
        // Only while the owner asked clicks to pick: a page cannot open the box itself.
        if (opens(message.press)) acts.onPick(message, false);
        return;
      case 'page:area-drag': {
        // A drag whose press closed a box opens nothing, so it draws nothing.
        const { drag } = message;
        if (!drag) update({ areaDrag: null });
        else if (opens(drag.press)) update({ areaDrag: drag.rect });
        return;
      }
      case 'page:area':
        // A pick of several elements at once, paired with its press as a click's is.
        if (opens(message.press)) acts.onPick(message, false);
        return;
      case 'page:selection': {
        const next = message.selection;
        if (!next) update({ selection: null });
        else if (next.compose) {
          // Words dragged over or clicked in comment mode open their box at
          // once, with no chip, paired with their press as a pick is. Words
          // that open nothing are let go, so the page keeps no selection
          // that no box is on.
          if (opens(next.press)) acts.onPick(next, true);
          else bridge.post({ type: 'page:clear-selection' });
        } else update({ selection: next });
        return;
      }
      case 'page:positions': {
        // Only the spots this view asked about count.
        const next = new Map<string, FrameRect | null>();
        for (const item of message.items) if (asked.has(item.id)) next.set(item.id, item.rect);
        setReported({ doc, serial: draftSerial, map: next });
        return;
      }
      case 'page:settled':
        setSettledDoc(doc);
        return;
      case 'page:pointer-in':
        setPointerIn((n) => n + 1);
        return;
      case 'page:scroll':
        // A document not yet on screen is only being put back where the
        // reader was; that is not the reader scrolling.
        update({ scrolling: message.moving && view.shown });
        return;
      case 'page:key':
        acts.onKey(message.key);
        return;
      default:
    }
  });
  const { subscribe } = bridge;
  useEffect(() => subscribe((message) => onMessage(message)), [subscribe]);

  return {
    doc,
    positions,
    fresh,
    settledDoc,
    settled: fresh && settledDoc === doc,
    selection: current.selection,
    areaDrag: current.areaDrag,
    scrolling: current.scrolling,
    pointerIn,
    placeSpot: (id, rect) => setReported((prev) => ({ ...prev, map: new Map(prev.map).set(id, rect) })),
    clearSelection: () => update({ selection: null }),
  };
}
