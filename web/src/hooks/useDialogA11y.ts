import { useEffect, useRef, type MouseEvent as ReactMouseEvent } from 'react';
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
 * Two overlays can be open at once — an install outcome opens over the plugin
 * detail that launched it — and they are DOM *siblings*, not ancestors. Neither
 * can see the other's events, so while every key still has an element behind it
 * the two never collide. A key from nowhere (see below) has no element to
 * arbitrate with, and that is the only case that needs this: the dialog opened
 * last claims it, so one Escape closes the top overlay and nothing else.
 */
const openDialogs: HTMLElement[] = [];

export function useDialogA11y<T extends HTMLElement>(onClose: () => void) {
  const ref = useRef<T>(null);
  const close = useStableHandler(onClose);

  useEffect(() => {
    const node = ref.current;
    if (!node) return;

    // Restore focus to whatever opened the dialog, so dismissing it does not
    // dump the caret back at the top of the document.
    const opener = document.activeElement as HTMLElement | null;
    // tabIndex is the property, not the attribute: it reads -1 for anything
    // deliberately taken out of the tab order, which is how a visually hidden
    // control (the file input behind a dropzone) stays out of the trap.
    const focusables = () =>
      Array.from(
        node.querySelectorAll<HTMLElement>(
          'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]'
        )
      ).filter((el) => el.tabIndex >= 0);

    (focusables()[0] ?? node).focus();
    openDialogs.push(node);
    // Both dialogs stay in the DOM while one covers the other. Keyboard focus
    // is already contained in the top one by the trap below, but assistive
    // tech reads the tree, not the focus ring: with two `aria-modal` dialogs
    // exposed it announces both and marks neither as covered.
    openDialogs[openDialogs.length - 2]?.setAttribute('aria-hidden', 'true');

    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key !== 'Escape' && e.key !== 'Tab') return;

      const target = e.target as Node | null;
      const fromInside = !!target && node.contains(target);
      // A key with nothing behind it. That is what every key press looks like
      // once `document.activeElement` has fallen to <body>, which is where the
      // browser puts it when the focused control is unmounted — a wizard
      // advancing a step does exactly that, and the dialog would spend the rest
      // of the flow unable to hear Escape at all if it only listened on itself.
      const fromNowhere =
        !target || target === document.body || target === document.documentElement;
      // Listening on the document is what makes those keys reachable, and the
      // price is hearing keys that are not ours. One from a real element
      // outside the dialog belongs to whoever owns that element: the Radix
      // menus inside these overlays portal to <body>, so their Escape must
      // close the menu and leave the dialog standing.
      if (!fromInside && !fromNowhere) return;
      if (fromNowhere && openDialogs[openDialogs.length - 1] !== node) return;

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

    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('keydown', onKeyDown);
      const i = openDialogs.indexOf(node);
      if (i !== -1) openDialogs.splice(i, 1);
      // Unhide before restoring focus, never after: the opener is typically a
      // control inside the dialog underneath, and moving focus into a subtree
      // still marked aria-hidden is the exact state this is here to avoid.
      openDialogs[openDialogs.length - 1]?.removeAttribute('aria-hidden');
      // Only if the opener is still in the document: uninstalling from a detail
      // overlay removes the row that opened it, and focusing a detached node
      // silently drops focus to <body>, stranding keyboard users at the top of
      // the page instead of where they were.
      if (opener?.isConnected) opener.focus?.();
    };
  }, [close]);

  return ref;
}

/**
 * Backdrop click-to-dismiss that survives a drag.
 *
 * A bare `onClick` on the backdrop is wrong here: the browser fires click on
 * the nearest common ancestor of the press and the release, so selecting a URL
 * inside the dialog and releasing past its edge lands a click on the backdrop
 * and discards whatever the user had typed. Requiring both ends of the gesture
 * to land on the backdrop makes it mean what it looks like.
 *
 * Spread the result onto the backdrop element. The `currentTarget` checks also
 * replace the inner `stopPropagation` these overlays used to need, so the
 * dialog body no longer has to know it sits on a dismissable ground.
 */
export function useBackdropDismiss<T extends HTMLElement>(onClose: () => void) {
  const pressedBackdrop = useRef(false);
  const close = useStableHandler(onClose);
  return {
    onMouseDown: (e: ReactMouseEvent<T>) => {
      pressedBackdrop.current = e.target === e.currentTarget;
    },
    onClick: (e: ReactMouseEvent<T>) => {
      if (!pressedBackdrop.current || e.target !== e.currentTarget) return;
      pressedBackdrop.current = false;
      close();
    },
  };
}
