import { Suspense, createContext, lazy, useCallback, useContext, useEffect, useMemo, useRef, useState, type ComponentType, type ReactNode } from 'react';
import { clientOs, type ClientOs } from '@/lib/clientOs';
import { focusWithoutRing } from '@/lib/inputModality';
import { PREFIXES } from './prefixes';
import type { Kind } from './types';

// Off the entry chunk: nothing of the palette is needed until it opens. A
// palette whose code cannot load (offline, or a deploy removed it, which
// index.html's preload listener reports) closes again rather than taking the
// app down with it, and the next open asks for the code afresh: a lazy
// component keeps whatever its one load gave it, so a new one is made.
const loadPalette = () => import('./CommandPalette');
interface PaletteProps {
  open: boolean;
  onClose: () => void;
  typed: string;
  returnFocus: () => HTMLElement | null;
  onUnavailable: () => void;
}
const lazyPalette = () => lazy<ComponentType<PaletteProps>>(() => loadPalette().catch(() => ({ default: Unavailable })));

function Unavailable({ open, onClose, onUnavailable }: PaletteProps) {
  useEffect(() => {
    if (!open) return;
    onClose();
    onUnavailable();
  }, [open, onClose, onUnavailable]);
  return null;
}

/** Where an open starts. */
export interface PaletteStart {
  /**
   * The kind's view, as if its prefix were typed after ⌘K: Backspace in the
   * empty field widens it to everything.
   */
  kind: Kind;
}

interface CommandPaletteControls {
  open: (start?: PaletteStart) => void;
  close: () => void;
  isOpen: boolean;
  /** Fetches the palette's code ahead of the first open, for a pointer resting on its button. */
  preload: () => void;
}

const CommandPaletteContext = createContext<CommandPaletteControls | null>(null);

/**
 * Whether ⌘K (Ctrl+K on Windows) should open the palette here. Elsewhere,
 * either does: an iPad reads as neither, and its keyboard has ⌘. A code editor
 * keeps the key: Monaco starts its own chords with ⌘K, and taking it from it
 * would leave those chords unreachable while the editor has focus.
 */
function opensPalette(e: KeyboardEvent, os: ClientOs): boolean {
  if (e.key.toLowerCase() !== 'k' || e.altKey || e.shiftKey) return false;
  const meta = e.metaKey && !e.ctrlKey;
  const ctrl = e.ctrlKey && !e.metaKey;
  if (!(os === 'mac' ? meta : os === 'windows' ? ctrl : meta || ctrl)) return false;
  const target = e.target as Element | null;
  return !target?.closest?.('.monaco-editor');
}

/**
 * The command palette's host: the global ⌘K shortcut, and the open state any
 * surface (the sidebar's search row) can reach through `useCommandPalette`.
 * Mounted once inside the router, beside the app's shell.
 */
export function CommandPaletteProvider({ children }: { children: ReactNode }) {
  const [isOpen, setIsOpen] = useState(false);
  const [CommandPalette, setCommandPalette] = useState(lazyPalette);
  // Mounted from the first open on, so a close plays its exit rather than
  // cutting to nothing; what it shows is rebuilt on every open.
  const [used, setUsed] = useState(false);
  if (isOpen && !used) setUsed(true);
  // A new lazy component for code that failed to load, mounted only from the
  // next open, so its one load is that open's and not a moment after the miss.
  const retryPalette = useCallback(() => {
    setUsed(false);
    setCommandPalette(() => lazyPalette());
  }, []);
  // What is typed between ⌘K and the field taking focus: a reader types the
  // moment they press it, and the palette can take longer than a keystroke
  // to draw. Collected until focus is inside the palette, then handed to it.
  const [typed, setTyped] = useState('');
  const bufferingRef = useRef(false);
  // Read by the shortcut's listener, which toggles without a render between
  // presses; kept beside the state so an updater never has to write a ref.
  const isOpenRef = useRef(false);
  // Where focus was when the palette was asked to open, which it hands back
  // on close. Taken here, before the shortcut moves focus to the stand-in.
  const returnToRef = useRef<HTMLElement | null>(null);
  // A stand-in field that holds focus from ⌘K until the palette's own takes
  // it. Swallowing keydowns keeps plain keys off the page, but an IME's text
  // arrives as a composition, not keys, and lands in whatever has focus: a
  // composer under the palette would take Chinese or Japanese typed straight
  // after ⌘K. With focus here the composition happens in this field, and
  // each finished one joins what was typed. Redirecting focus beats trying
  // to cancel the composition elsewhere (compositionstart cannot be
  // cancelled, and beforeinput for it is not cancellable in every browser).
  const standInRef = useRef<HTMLInputElement>(null);
  const composingRef = useRef(false);
  const setOpen = useCallback((next: boolean, first = '') => {
    const was = document.activeElement;
    if (next && !isOpenRef.current && was instanceof HTMLElement && was !== standInRef.current && !was.closest('.command-palette')) {
      returnToRef.current = was === document.body ? null : was;
    }
    isOpenRef.current = next;
    bufferingRef.current = next;
    if (next) setTyped(first);
    setIsOpen(next);
    // Closed before the palette took focus (Escape on a cold open, code that
    // failed to load): the stand-in hands focus back itself.
    if (!next && was === standInRef.current) {
      standInRef.current!.value = '';
      focusWithoutRing(returnToRef.current);
    }
  }, []);
  const open = useCallback((start?: PaletteStart) => setOpen(true, start ? PREFIXES[start.kind] : ''), [setOpen]);
  const close = useCallback(() => setOpen(false), [setOpen]);
  const preload = useCallback(() => void loadPalette().catch(() => {}), []);
  const returnFocus = useCallback(() => returnToRef.current, []);

  // A finished composition, or text that reached the stand-in some other
  // way (a dead key), joins what was typed. Mid-composition the text is the
  // IME's own; it settles as the composition ends, which also happens when
  // the palette's field takes focus from here.
  const flushStandIn = useCallback(() => {
    const field = standInRef.current;
    if (!field || composingRef.current || !field.value) return;
    const text = field.value;
    field.value = '';
    if (bufferingRef.current) setTyped((was) => was + text);
  }, []);

  // Its code is fetched once the page is idle, so the first ⌘K opens at once
  // and the keys typed straight after it land in the field. A failed fetch is
  // tried again by the first open.
  useEffect(() => {
    // Mounted closed once fetched: a lazy component suspends on its first
    // render even with its code in hand, which would drop keys typed then.
    const warm = () =>
      void loadPalette()
        .then(() => setUsed(true))
        .catch(() => {});
    if (typeof window.requestIdleCallback === 'function') {
      const id = window.requestIdleCallback(warm, { timeout: 5_000 });
      return () => window.cancelIdleCallback(id);
    }
    const id = window.setTimeout(warm, 2_000);
    return () => window.clearTimeout(id);
  }, []);

  useEffect(() => {
    const os = clientOs();
    const onKeyDown = (e: KeyboardEvent) => {
      if (opensPalette(e, os)) {
        e.preventDefault();
        // Held down, the key repeats: one press is one toggle.
        if (e.repeat) return;
        const opening = !isOpenRef.current;
        setOpen(opening);
        // Unless the palette's field has it already (a reopen during the close).
        if (opening && !(document.activeElement as Element | null)?.closest?.('.command-palette')) {
          standInRef.current?.focus({ preventScroll: true });
        }
        return;
      }
      if (!bufferingRef.current) return;
      // Focus inside the palette (its field took it, or kept it through a
      // reopen during the close) reads its own keys from here on.
      if ((document.activeElement as Element | null)?.closest?.('.command-palette')) {
        bufferingRef.current = false;
        return;
      }
      if (e.metaKey || e.ctrlKey || e.altKey || e.isComposing) return;
      if (e.key.length === 1) {
        e.preventDefault();
        setTyped((was) => was + e.key);
      } else if (e.key === 'Backspace') {
        e.preventDefault();
        setTyped((was) => was.slice(0, -1));
      } else if (e.key === 'Enter' || e.key === 'Escape') {
        // Meant for the palette, which has nothing to pick yet: neither may
        // reach the page under it, where Enter would send a draft.
        e.preventDefault();
        e.stopPropagation();
        if (e.key === 'Escape') close();
      }
    };
    // Capture: a field that handles its own keys still lets ⌘K through.
    window.addEventListener('keydown', onKeyDown, true);
    return () => window.removeEventListener('keydown', onKeyDown, true);
  }, [close, setOpen]);

  const value = useMemo(() => ({ open, close, isOpen, preload }), [open, close, isOpen, preload]);

  return (
    <CommandPaletteContext.Provider value={value}>
      {children}
      <input
        ref={standInRef}
        // Unseen, where the palette's field will be, so an IME's candidate
        // window opens beside it rather than in a corner of the screen.
        className="pointer-events-none fixed left-1/2 top-[min(14vh,7.5rem)] size-px border-0 p-0 opacity-0"
        aria-hidden
        tabIndex={-1}
        autoComplete="off"
        onCompositionStart={() => {
          composingRef.current = true;
        }}
        onCompositionEnd={() => {
          composingRef.current = false;
          flushStandIn();
        }}
        onInput={flushStandIn}
        onBlur={() => {
          composingRef.current = false;
          flushStandIn();
        }}
      />
      {used && (
        <Suspense fallback={null}>
          <CommandPalette open={isOpen} onClose={close} typed={typed} returnFocus={returnFocus} onUnavailable={retryPalette} />
        </Suspense>
      )}
    </CommandPaletteContext.Provider>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function useCommandPalette(): CommandPaletteControls | null {
  return useContext(CommandPaletteContext);
}

/** The shortcut as the reader's keyboard labels it. */
// eslint-disable-next-line react-refresh/only-export-components
export function paletteShortcut(): string {
  return clientOs() === 'mac' ? '⌘K' : 'Ctrl K';
}

/** The shortcut for `aria-keyshortcuts`: only those this keyboard opens it with. */
// eslint-disable-next-line react-refresh/only-export-components
export function paletteKeyShortcuts(): string {
  const os = clientOs();
  return os === 'mac' ? 'Meta+K' : os === 'windows' ? 'Control+K' : 'Meta+K Control+K';
}
