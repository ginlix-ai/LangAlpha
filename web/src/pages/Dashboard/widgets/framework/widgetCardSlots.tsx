import { createContext, useContext, type ReactNode } from 'react';
import { createPortal } from 'react-dom';

interface Slots {
  sub: HTMLElement | null;
  meta: HTMLElement | null;
}

const SlotsContext = createContext<Slots>({ sub: null, meta: null });

/** The frame's edge, handed to the widget inside it. The widget knows what
 *  the edge should say (a count, the feed's state) and the frame owns where
 *  it is drawn, so the widget renders into the frame's spans through a portal
 *  rather than lifting its data up a level. */
export function WidgetCardSlots({ sub, meta, children }: Slots & { children: ReactNode }) {
  return <SlotsContext value={{ sub, meta }}>{children}</SlotsContext>;
}

/** Beside the title in the card's edge: the brief's kind. */
export function WidgetCardSub({ children }: { children: ReactNode }) {
  const el = useContext(SlotsContext).sub;
  return el ? createPortal(children, el) : null;
}

/** At the far end of the card's edge: a count, a window, a way out. */
export function WidgetCardMeta({ children }: { children: ReactNode }) {
  const el = useContext(SlotsContext).meta;
  return el ? createPortal(children, el) : null;
}
