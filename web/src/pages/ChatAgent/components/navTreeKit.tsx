/* eslint-disable react-refresh/only-export-components -- a kit is an object of components, chosen at runtime */
/**
 * The nav tree's motion and drag layer, swapped in after first paint.
 *
 * framer-motion and dnd-kit are the bulk of what the first screen would
 * otherwise wait for, and the sidebar tree is the only part of it that uses
 * them. So the tree paints with STATIC_KIT, plain elements laid out exactly
 * like the interactive ones, and `useNavTreeKit` loads ./navTreeKitInteractive
 * at idle (sooner on the first pointer or focus in the tree) and swaps it in.
 *
 * The swap remounts every row, so it waits until a remount would show nothing:
 * no row hovered, pressed, focused from the keyboard or typed in, selected or
 * holding an open menu, no dialog or menu that could hand focus back to a row,
 * and no transition mid-flight. Focus a click left on a row, and CSS animation
 * clocks, carry across it. A drag begun on the static tree is handed to
 * dnd-kit as it crosses the activation distance, so the first drag after a
 * load still lands.
 */
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ComponentType,
  type CSSProperties,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
  type RefObject,
} from 'react';
import { flushSync } from 'react-dom';
import type { DragEndEvent, DragStartEvent, Modifier } from '@dnd-kit/core';

/** dnd-kit's gate: `true` disables both aspects, the object form keeps a row droppable. */
export type DragGate = boolean | { draggable: boolean; droppable: boolean };

export interface TreeRootProps {
  ids: string[];
  onDragStart: (event: DragStartEvent) => void;
  onDragEnd: (event: DragEndEvent) => void;
  onDragCancel: () => void;
  clampOverlay: Modifier;
  /** The lift preview, or null while nothing is dragged. */
  overlay: ReactNode;
  children: ReactNode;
}

export interface SectionProps {
  wsId: string;
  disabled: DragGate;
  /** The header row spreads `dragHandleProps`; `isDragging` collapses the section while lifted. */
  children: (args: { dragHandleProps: Record<string, unknown>; isDragging: boolean }) => ReactNode;
}

export interface NavTreeKit {
  /** Drag context, sortable list and lift preview around every section. */
  TreeRoot: ComponentType<TreeRootProps>;
  /** One workspace section: header row plus its thread block. */
  Section: ComponentType<SectionProps>;
  /** A workspace's thread block, opening and closing on its height. */
  Collapse: ComponentType<{ show: boolean; children: ReactNode }>;
  /** The thread rows, so archived and arriving rows collapse out and in. */
  ThreadList: ComponentType<{ children: ReactNode }>;
  /** One thread row. `order` is the list's id signature: rows glide to a new
   *  slot when it changes and are not re-measured otherwise. */
  ThreadItem: ComponentType<{ order: string; children: ReactNode }>;
  /** The thread pin button's glyph, which pops when the pin flips. */
  PinGlyph: ComponentType<{ pinned: boolean; children: ReactNode }>;
}

/** dnd-kit's activation distance, which the static tree's drag handover mirrors. */
export const DRAG_ACTIVATION_DISTANCE = 8;

/** Marks a section's drag handle in both kits, so a drag can cross the swap. */
export const DRAG_HANDLE_ATTR = 'data-drag-handle';

/** A section at rest, as the sortable one styles itself when nothing is dragged. */
export const SECTION_STYLE: CSSProperties = { position: 'relative', opacity: 1 };

// What useSortable gives a draggable header, less the aria-describedby that
// points into DndContext: tab order and roles stay put across the swap.
const STATIC_HANDLE_ATTRS = {
  role: 'button',
  tabIndex: 0,
  'aria-disabled': false,
  'aria-roledescription': 'sortable',
  [DRAG_HANDLE_ATTR]: '',
};

function StaticTreeRoot({ children }: TreeRootProps) {
  return <>{children}</>;
}

function StaticSection({ wsId, disabled, children }: SectionProps) {
  const { onStaticPress } = useNavTree();
  const draggable = !(typeof disabled === 'boolean' ? disabled : disabled.draggable);
  return (
    <div style={SECTION_STYLE} data-ws-id={wsId}>
      {children({
        dragHandleProps: draggable ? { ...STATIC_HANDLE_ATTRS, onPointerDown: onStaticPress } : {},
        isDragging: false,
      })}
    </div>
  );
}

// The shape the interactive kit's measured fold (components/ui/Disclosure)
// has at rest, open or shut, so the swap between the two moves nothing.
function StaticFold({ open, children }: { open: boolean; children: ReactNode }) {
  return (
    <div hidden={!open} inert={!open || undefined}>
      <div className="flow-root">{open ? children : null}</div>
    </div>
  );
}

function StaticCollapse({ show, children }: { show: boolean; children: ReactNode }) {
  return <StaticFold open={show}>{children}</StaticFold>;
}

function StaticThreadList({ children }: { children: ReactNode }) {
  return <>{children}</>;
}

// The interactive row glides in a wrapper of its own, around the fold it
// arrives and leaves in.
function StaticThreadItem({ children }: { order: string; children: ReactNode }) {
  return (
    <div>
      <StaticFold open>{children}</StaticFold>
    </div>
  );
}

function StaticPinGlyph({ children }: { pinned: boolean; children: ReactNode }) {
  return <span className="flex">{children}</span>;
}

const STATIC_KIT: NavTreeKit = {
  TreeRoot: StaticTreeRoot,
  Section: StaticSection,
  Collapse: StaticCollapse,
  ThreadList: StaticThreadList,
  ThreadItem: StaticThreadItem,
  PinGlyph: StaticPinGlyph,
};

interface NavTreeValue {
  kit: NavTreeKit;
  /** The static header's pointerdown, which watches for a drag to hand over. The interactive kit ignores it. */
  onStaticPress: (event: ReactPointerEvent<HTMLElement>) => void;
}

export const NavTreeContext = createContext<NavTreeValue>({ kit: STATIC_KIT, onStaticPress: () => {} });

export function useNavTree(): NavTreeValue {
  return useContext(NavTreeContext);
}

let loadedKit: NavTreeKit | null = null;
let loadingKit: Promise<NavTreeKit> | null = null;

/** Starts or joins the interactive kit's download. A failed load clears itself, so the next trigger retries. */
export function loadNavTreeKit(): Promise<NavTreeKit> {
  loadingKit ??= import('./navTreeKitInteractive').then(
    (module) => (loadedKit = module.default),
    (error: unknown) => {
      loadingKit = null;
      throw error;
    },
  );
  return loadingKit;
}

const IDLE_TIMEOUT_MS = 2000;
// Where requestIdleCallback is missing, long enough to clear the first render.
const IDLE_FALLBACK_MS = 300;
const RETRY_MS = 250;

function whenIdle(run: () => void): () => void {
  if (typeof window.requestIdleCallback === 'function') {
    const id = window.requestIdleCallback(run, { timeout: IDLE_TIMEOUT_MS });
    return () => window.cancelIdleCallback(id);
  }
  const id = window.setTimeout(run, IDLE_FALLBACK_MS);
  return () => window.clearTimeout(id);
}

/** CSS animations and transitions running on the rows, in document order. */
function rowAnimations(root: HTMLElement): Animation[] {
  if (typeof root.getAnimations !== 'function') return [];
  return root.getAnimations({ subtree: true }).filter((animation) => {
    const target = (animation.effect as KeyframeEffect | null)?.target;
    return !!target?.closest('[data-ws-id]');
  });
}

function findSection(root: HTMLElement, wsId: string): HTMLElement | undefined {
  return Array.from(root.querySelectorAll<HTMLElement>('[data-ws-id]')).find((el) => el.dataset.wsId === wsId);
}

/** Set by ./NavigationRows on each element that takes focus or animates, unique within its section. */
const NAV_KEY_ATTR = 'data-nav-key';

/** An element's identity in either tree: its section and its own nav key. */
interface NavId {
  wsId: string;
  key: string;
}

function identify(el: Element): NavId | null {
  const key = el.getAttribute(NAV_KEY_ATTR);
  const wsId = el.closest<HTMLElement>('[data-ws-id]')?.dataset.wsId;
  return key !== null && wsId ? { wsId, key } : null;
}

function findTwin(root: HTMLElement, { wsId, key }: NavId): Element | undefined {
  const keyed = findSection(root, wsId)?.querySelectorAll(`[${NAV_KEY_ATTR}]`) ?? [];
  return Array.from(keyed).find((el) => el.getAttribute(NAV_KEY_ATTR) === key);
}

/**
 * Names a row's CSS animation by the element it runs on, so its twin in the
 * other tree is found by identity rather than by position: a title fading in
 * when the tree swaps has no twin, and would shift every match after it.
 */
function clockKey(animation: CSSAnimation): string | null {
  const effect = animation.effect as KeyframeEffect | null;
  const id = effect?.target ? identify(effect.target) : null;
  return id && JSON.stringify([id.wsId, id.key, effect?.pseudoElement ?? '', animation.animationName]);
}

/** The start time of each row animation that has one; one not started yet (begun this frame, or in a hidden tab) has none to hand on. */
function rowClocks(root: HTMLElement): Map<string, CSSNumberish> {
  const clocks = new Map<string, CSSNumberish>();
  for (const animation of rowAnimations(root)) {
    if (!(animation instanceof CSSAnimation) || animation.startTime === null) continue;
    const key = clockKey(animation);
    if (key) clocks.set(key, animation.startTime);
  }
  return clocks;
}

/**
 * Remounts the rows through `commit`, handing what the old ones had to their
 * twins. A remounted row restarts its CSS animations (the running subagent
 * pulse), so each takes its predecessor's start time; one without a
 * predecessor clock starts on its own, as a null start time would hold it
 * paused at its first frame. Focus a click left on a row falls to the body
 * with it, and moves to the twin so the next Tab still starts from there.
 */
function remountCarrying(root: HTMLElement, commit: () => void): void {
  const doc = root.ownerDocument;
  const clocks = rowClocks(root);
  const focused = doc.activeElement;
  const id = focused && root.contains(focused) ? identify(focused) : null;
  commit();
  for (const animation of rowAnimations(root)) {
    const key = animation instanceof CSSAnimation ? clockKey(animation) : null;
    const start = key ? clocks.get(key) : undefined;
    if (start !== undefined) animation.startTime = start;
  }
  const twin = id && doc.activeElement === doc.body ? findTwin(root, id) : undefined;
  if (twin instanceof HTMLElement) twin.focus({ preventScroll: true });
}

/**
 * Focus a remount must not move: keyboard focus, and a field being typed in,
 * which matches :focus-visible however it was focused. A click's focus on a
 * row can move to the row's twin instead. Nothing matches :focus-visible
 * while another tab or window has focus, so then the two cannot be told apart.
 */
function holdsFocus(el: Element): boolean {
  if (!el.ownerDocument.hasFocus()) return true;
  try {
    return el.matches(':focus-visible');
  } catch {
    return true;
  }
}

function swapWouldShow(root: HTMLElement): boolean {
  const doc = root.ownerDocument;
  const focused = doc.activeElement;
  const selection = doc.getSelection();
  const overlays = doc.querySelectorAll(
    '[role="dialog"], [role="alertdialog"], [role="menu"], [data-radix-popper-content-wrapper]',
  );
  return (
    root.querySelector('[data-ws-id]:hover, [data-ws-id] [data-state="open"]') !== null ||
    (focused !== null && root.contains(focused) && focused.closest('[data-ws-id]') !== null && holdsFocus(focused)) ||
    (selection !== null && !selection.isCollapsed && root.contains(selection.anchorNode)) ||
    // A dialog or menu opened from a row hands focus back to it on close,
    // which for a menu comes after its exit animation, once its trigger has
    // stopped reading as open.
    Array.from(overlays).some((overlay) => !overlay.contains(root)) ||
    rowAnimations(root).some((a) => a instanceof CSSTransition && a.playState === 'running')
  );
}

function swallowNextClick(doc: Document): void {
  const stop = (event: Event) => event.stopPropagation();
  doc.addEventListener('click', stop, { capture: true, once: true });
  window.setTimeout(() => doc.removeEventListener('click', stop, { capture: true }), 50);
}

interface Point {
  x: number;
  y: number;
}

/**
 * The one press the static tree follows, from its pointer going down in the
 * tree until that pointer lets go; other pointers meanwhile are not followed.
 * Any press holds the swap. A primary press on a drag handle keeps where it
 * began, and past the activation distance becomes a drag to hand to dnd-kit,
 * carrying where the pointer is.
 */
type Press = { pointerId: number; pointerType: string } & (
  | { kind: 'press' }
  | { kind: 'handle'; origin: Point & { wsId: string } }
  | { kind: 'drag'; origin: Point & { wsId: string }; at: Point }
);

/**
 * The kit a nav tree renders with, rooted at `rootRef`: static until the
 * interactive kit has loaded and a swap would be invisible, interactive from
 * the start once it has loaded.
 */
export function useNavTreeKit(rootRef: RefObject<HTMLElement | null>): NavTreeValue {
  const [kit, setKit] = useState<NavTreeKit>(() => loadedKit ?? STATIC_KIT);
  const pressRef = useRef<((event: PointerEvent, handle: HTMLElement) => void) | null>(null);

  const onStaticPress = useCallback((event: ReactPointerEvent<HTMLElement>) => {
    pressRef.current?.(event.nativeEvent, event.currentTarget);
  }, []);

  useEffect(() => {
    const root = rootRef.current;
    if (kit !== STATIC_KIT || !root) return;
    const doc = root.ownerDocument;
    let done = false;
    let retry: number | undefined;
    let press: Press | null = null;

    const swap = (next: NavTreeKit) => {
      done = true;
      window.clearTimeout(retry);
      remountCarrying(root, () => flushSync(() => setKit(next)));
    };

    // The press is replayed on the new header where it began, then the move
    // that made it a drag, which is already past dnd-kit's activation distance.
    // dnd-kit only starts on that move, and measures the lift preview as the
    // start commits, so the start commits at rest and the move is sent again
    // to carry the row to the pointer.
    const handOff = ({ pointerId, pointerType, origin, at }: Extract<Press, { kind: 'drag' }>, next: NavTreeKit) => {
      press = null;
      swap(next);
      const handle = findSection(root, origin.wsId)?.querySelector<HTMLElement>(`[${DRAG_HANDLE_ATTR}]`);
      if (!handle) return;
      const init = {
        bubbles: true,
        cancelable: true,
        composed: true,
        pointerId,
        pointerType,
        isPrimary: true,
        buttons: 1,
      };
      const move = () => new PointerEvent('pointermove', { ...init, button: -1, clientX: at.x, clientY: at.y });
      flushSync(() => {
        handle.dispatchEvent(new PointerEvent('pointerdown', { ...init, button: 0, clientX: origin.x, clientY: origin.y }));
        doc.dispatchEvent(move());
      });
      doc.dispatchEvent(move());
    };

    // Runs when the kit arrives, when a held swap re-polls and when a press
    // becomes a drag: a drag is handed over, any other press or a remount that
    // would show waits, and otherwise the kit swaps in.
    const step = () => {
      window.clearTimeout(retry);
      if (done || !loadedKit) return;
      if (press?.kind === 'drag') handOff(press, loadedKit);
      else if (press || swapWouldShow(root)) retry = window.setTimeout(step, RETRY_MS);
      else swap(loadedKit);
    };

    const load = () => {
      if (!done) loadNavTreeKit().then(step, () => {});
    };

    // The root sees a press before the header's React handler below does. A
    // pointer is primary only while no other of its type is down, so a
    // followed press of that type was released unseen, as touch can be.
    const onPointerDown = ({ pointerId, pointerType, isPrimary }: PointerEvent) => {
      if (!press || press.pointerId === pointerId || (isPrimary && press.pointerType === pointerType)) {
        press = { kind: 'press', pointerId, pointerType };
      }
      load();
    };

    pressRef.current = (event, handle) => {
      const wsId = handle.closest<HTMLElement>('[data-ws-id]')?.dataset.wsId;
      if (done || !wsId || press?.pointerId !== event.pointerId || event.button !== 0 || !event.isPrimary) return;
      press = { ...press, kind: 'handle', origin: { wsId, x: event.clientX, y: event.clientY } };
    };

    const onPointerMove = (event: PointerEvent) => {
      if (done || press?.pointerId !== event.pointerId) return;
      // A button released outside the window sends no pointerup here; the
      // next move over the page is the first to show it is up.
      if (event.buttons === 0) {
        press = null;
        return;
      }
      if (press.kind === 'press') return;
      // The primary button let go while another is held: still a press, no longer a drag.
      if ((event.buttons & 1) === 0) {
        press = { kind: 'press', pointerId: press.pointerId, pointerType: press.pointerType };
        return;
      }
      const { pointerId, pointerType, origin } = press;
      const distance = Math.hypot(event.clientX - origin.x, event.clientY - origin.y);
      if (press.kind === 'handle' && distance <= DRAG_ACTIVATION_DISTANCE) return;
      press = { kind: 'drag', pointerId, pointerType, origin, at: { x: event.clientX, y: event.clientY } };
      step();
    };

    const onPointerEnd = (event: PointerEvent) => {
      if (press?.pointerId !== event.pointerId) return;
      // A drag the kit arrived too late for: it must not land as a click
      // that toggles the row, which dnd-kit would have swallowed too.
      if (press.kind === 'drag') swallowNextClick(doc);
      press = null;
    };

    // Switching tab or window mid-press can swallow the release, and dnd-kit
    // drops a drag on the same signals.
    const onAbandon = () => {
      press = null;
    };

    root.addEventListener('pointerenter', load);
    root.addEventListener('focusin', load);
    root.addEventListener('pointerdown', onPointerDown, true);
    doc.addEventListener('pointermove', onPointerMove, true);
    doc.addEventListener('pointerup', onPointerEnd, true);
    doc.addEventListener('pointercancel', onPointerEnd, true);
    doc.addEventListener('visibilitychange', onAbandon);
    window.addEventListener('blur', onAbandon);
    const cancelIdle = loadedKit ? () => {} : whenIdle(load);
    // Not from here: a swap flushes synchronously, which React refuses to do
    // from inside an effect.
    if (loadedKit) retry = window.setTimeout(step, 0);

    return () => {
      done = true;
      pressRef.current = null;
      window.clearTimeout(retry);
      cancelIdle();
      root.removeEventListener('pointerenter', load);
      root.removeEventListener('focusin', load);
      root.removeEventListener('pointerdown', onPointerDown, true);
      doc.removeEventListener('pointermove', onPointerMove, true);
      doc.removeEventListener('pointerup', onPointerEnd, true);
      doc.removeEventListener('pointercancel', onPointerEnd, true);
      doc.removeEventListener('visibilitychange', onAbandon);
      window.removeEventListener('blur', onAbandon);
    };
  }, [kit, rootRef]);

  return useMemo(() => ({ kit, onStaticPress }), [kit, onStaticPress]);
}
