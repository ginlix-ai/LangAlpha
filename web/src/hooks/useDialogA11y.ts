import { useEffect, useEffectEvent, useRef, type MouseEvent as ReactMouseEvent } from 'react';
import { useStableHandler } from './useStableHandler';

/**
 * Keyboard and focus behaviour for a hand-rolled modal overlay.
 *
 * The overlays in this app are raw fixed-position divs rather than Radix
 * dialogs, so nothing supplies the three things a modal owes a keyboard user:
 * focus that starts inside it, focus that cannot leave while it is open, and
 * Escape to close. Attach the returned ref to the dialog element and pair it
 * with `role="dialog"`, `aria-modal="true"` and an `aria-labelledby`.
 */

/**
 * Every open dialog, in mount order, innermost last.
 *
 * Two overlays can be open at once, in either of two shapes: an install outcome
 * opens *beside* the plugin detail that launched it, and the MCP server form
 * opens *inside* the sandbox settings panel. Only the dialog on top acts on a
 * key or a focus change. That is what lets one Escape close one layer: a
 * containing panel would otherwise hear the key as its own too, and every
 * listener sees it, since they all sit on the document.
 */
const openDialogs: HTMLElement[] = [];

/**
 * A dialog left mounted in a view that has since been hidden (a cached chat
 * view, once the user moves to another thread) is not open as far as the page
 * is concerned. It cannot take focus, and an Escape it claimed would throw away
 * a form nobody can see.
 */
const isShown = (node: HTMLElement) => node.checkVisibility?.() ?? true;

const isTop = (node: HTMLElement) => openDialogs.findLast(isShown) === node;

/**
 * Expose the dialog that is acting, and hide the ones it covers.
 *
 * The trap below already holds keyboard focus in the acting dialog, but
 * assistive tech reads the tree, not the focus ring: with two `aria-modal`
 * dialogs exposed it announces both and marks neither as covered. A panel that
 * contains the acting dialog is left alone, since hiding it would hide both.
 *
 * Which dialog acts changes with visibility as well as with the stack, so this
 * runs on both. Otherwise going back to a thread reveals a dialog still marked
 * hidden from the tree it is trapping focus in.
 */
function reconcileExposure() {
  const top = openDialogs.findLast(isShown);
  for (const node of openDialogs) {
    if (top && (node === top || node.contains(top))) node.removeAttribute('aria-hidden');
    else node.setAttribute('aria-hidden', 'true');
  }
}

const ABOVE_DIALOGS = 'data-above-dialogs';

/**
 * Spread onto a layer that paints above every dialog, such as the toast
 * viewport. Focus landing there was meant to go there, so the dialog below
 * leaves it be.
 */
export const aboveDialogs = { [ABOVE_DIALOGS]: '' } as const;

/**
 * Whether an element outside the dialog lies *under* it rather than in a layer
 * opened over it.
 *
 * A menu or popover opened from inside the dialog portals to <body> after the
 * app root the dialog renders in, and what happens there is that layer's
 * business. Everything else is the page the dialog covers. Focus does land
 * there: a menu that launched the dialog hands focus back to its trigger once
 * its exit animation ends, well after the dialog took it.
 */
function isUnderneath(el: Element, dialog: HTMLElement) {
  // A raised layer is not the page behind, even though it renders inside the
  // same app root. Reclaiming focus from a toast action would put that action
  // out of reach of anyone not using a mouse.
  if (el.closest(`[${ABOVE_DIALOGS}]`)) return false;
  const layerOf = (n: Node) => {
    while (n.parentNode && n.parentNode !== document.body) n = n.parentNode;
    return n;
  };
  const theirs = layerOf(el);
  const ours = layerOf(dialog);
  return theirs === ours || !!(theirs.compareDocumentPosition(ours) & Node.DOCUMENT_POSITION_FOLLOWING);
}

export function useDialogA11y<T extends HTMLElement>(onClose: () => void) {
  const ref = useRef<T>(null);
  const close = useEffectEvent(onClose);

  useEffect(() => {
    const node = ref.current;
    if (!node) return;

    // Restore focus to whatever opened the dialog, so dismissing it does not
    // dump the caret back at the top of the document.
    let opener = document.activeElement as HTMLElement | null;
    // tabIndex is the property, not the attribute: it reads -1 for anything
    // deliberately taken out of the tab order, which is how a visually hidden
    // control (the file input behind a dropzone) stays out of the trap.
    const focusables = () =>
      Array.from(
        node.querySelectorAll<HTMLElement>(
          'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]'
        )
      ).filter((el) => el.tabIndex >= 0);

    // On the stack before taking focus: the dialog underneath is listening for
    // focus that leaves it, and would pull this move straight back.
    openDialogs.push(node);
    reconcileExposure();
    // A dialog can stop being the acting one without the stack moving at all:
    // switching threads hides the cached view one sits in and reveals another,
    // and no React render reaches the dialog itself. Watch the box, which
    // `display: none` collapses, and settle the exposure again when it flips.
    const boxWatch =
      typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(() => reconcileExposure());
    boxWatch?.observe(node);
    const focusFirst = () => (focusables()[0] ?? node).focus();
    focusFirst();
    let lastFocused = node.contains(document.activeElement) ? (document.activeElement as HTMLElement) : null;
    // Refused when a menu item opens the dialog: Radix commits the selection
    // while the menu is still open, and its focus trap pulls focus straight
    // back. The menu has closed by the next task, so ask once more then, unless
    // focus has gone somewhere on purpose in between.
    const retry = lastFocused
      ? undefined
      : window.setTimeout(() => {
          const active = document.activeElement;
          if (isTop(node) && (!active || active === document.body || active === opener)) focusFirst();
        });

    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key !== 'Escape' && e.key !== 'Tab') return;
      if (!isTop(node)) return;
      // Handled already, by a layer inside the dialog that owns the key (an
      // inline editor cancelling on Escape), or part of an IME composition,
      // where Escape abandons the candidate and must not also discard the form.
      if (e.defaultPrevented || e.isComposing || e.keyCode === 229) return;

      const target = e.target instanceof Element ? e.target : null;
      // A key with nothing behind it. That is what every key press looks like
      // once `document.activeElement` has fallen to <body>, which is where the
      // browser puts it when the focused control is unmounted — a wizard
      // advancing a step does exactly that, and the dialog would spend the rest
      // of the flow unable to hear Escape at all if it only listened on itself.
      const fromNowhere = !target || target === document.body || target === document.documentElement;
      // Listening on the document is what makes those keys reachable, and the
      // price is hearing keys that are not ours. One from a layer opened over
      // the dialog belongs to that layer: the Radix menus inside these overlays
      // portal to <body>, so their Escape must close the menu and leave the
      // dialog standing.
      if (!fromNowhere && !node.contains(target) && !isUnderneath(target, node)) return;

      if (e.key === 'Escape') {
        // The document is the last stop before the window, so this still shields
        // the window-level Escape handlers on the page behind — closing a dialog
        // must not also clear the bulk selection underneath it.
        e.stopPropagation();
        close();
        return;
      }
      const items = focusables();
      if (items.length === 0) {
        e.preventDefault();
        return;
      }
      const first = items[0];
      const last = items[items.length - 1];
      const active = document.activeElement;
      // Wrap at both ends, and pull focus back in if it somehow escaped —
      // the dialog is not inert to the rest of the page, so a stray Tab
      // would otherwise walk into the content behind it.
      if (e.shiftKey && (active === first || !node.contains(active))) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && (active === last || !node.contains(active))) {
        e.preventDefault();
        first.focus();
      }
    };

    const onFocusIn = (e: FocusEvent) => {
      if (!isTop(node) || !(e.target instanceof HTMLElement)) return;
      if (node.contains(e.target)) {
        lastFocused = e.target;
        return;
      }
      if (!isUnderneath(e.target, node)) return;
      // Focus moved behind the dialog, most often a menu restoring its trigger.
      // Take it back rather than leave a screen reader announcing a control the
      // user cannot reach. When the menu item that opened the dialog is gone,
      // that trigger is the right place to return to on close.
      if (!opener?.isConnected) opener = e.target;
      // Back where the user was, or the top of the dialog when that control has
      // gone or been disabled since (Save, mid-save).
      lastFocused?.focus();
      if (!node.contains(document.activeElement)) focusFirst();
    };

    document.addEventListener('keydown', onKeyDown);
    document.addEventListener('focusin', onFocusIn);
    return () => {
      window.clearTimeout(retry);
      document.removeEventListener('keydown', onKeyDown);
      document.removeEventListener('focusin', onFocusIn);
      boxWatch?.disconnect();
      const i = openDialogs.indexOf(node);
      if (i !== -1) openDialogs.splice(i, 1);
      // Unhide before restoring focus, never after: the opener is typically a
      // control inside the dialog underneath, and moving focus into a subtree
      // still marked aria-hidden is the exact state this is here to avoid.
      reconcileExposure();
      // Only if the opener is still in the document: uninstalling from a detail
      // overlay removes the row that opened it, and focusing a detached node
      // silently drops focus to <body>, stranding keyboard users at the top of
      // the page instead of where they were.
      if (opener?.isConnected && opener !== document.body) opener.focus?.();
      // Focus still nowhere: the opener went with what this dialog did (a
      // removed row), or was in a panel made inert while this one was open
      // over it. A dialog still open underneath takes it back, not <body>.
      if (!document.activeElement || document.activeElement === document.body) {
        openDialogs.findLast(isShown)?.focus();
      }
    };
  }, []);

  return ref;
}

/**
 * How far the pointer may travel between press and release and still count as
 * a click, in CSS pixels. Past it the gesture is a drag, such as a selection
 * swept across the dialog that starts and ends just outside its edges.
 */
const CLICK_SLOP_PX = 5;

/**
 * Backdrop click-to-dismiss that survives a drag.
 *
 * A bare `onClick` on the backdrop is wrong here: the browser fires click on
 * the nearest common ancestor of the press and the release, so selecting a URL
 * inside the dialog and releasing past its edge (or pressing outside and
 * releasing inside) lands a click on the backdrop and discards whatever the
 * user had typed. Dismissal needs the whole gesture on the backdrop: the press,
 * the release, and next to no travel between them.
 *
 * Spread the result onto the backdrop element. The target checks also replace
 * the inner `stopPropagation` these overlays used to need, so the dialog body
 * no longer has to know it sits on a dismissable ground.
 */
export function useBackdropDismiss<T extends HTMLElement>(onClose: () => void) {
  const press = useRef<{ x: number; y: number } | null>(null);
  const releasedOnBackdrop = useRef(false);
  const close = useStableHandler(onClose);
  return {
    onMouseDown: (e: ReactMouseEvent<T>) => {
      press.current = e.button === 0 && e.target === e.currentTarget ? { x: e.clientX, y: e.clientY } : null;
      releasedOnBackdrop.current = false;
    },
    onMouseUp: (e: ReactMouseEvent<T>) => {
      releasedOnBackdrop.current = e.target === e.currentTarget;
    },
    onClick: (e: ReactMouseEvent<T>) => {
      const start = press.current;
      const released = releasedOnBackdrop.current;
      press.current = null;
      releasedOnBackdrop.current = false;
      if (!start || !released || e.target !== e.currentTarget) return;
      if (Math.hypot(e.clientX - start.x, e.clientY - start.y) > CLICK_SLOP_PX) return;
      close();
    },
  };
}
