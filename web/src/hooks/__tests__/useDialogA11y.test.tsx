import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { aboveDialogs, useBackdropDismiss, useDialogA11y } from '../useDialogA11y';

function Dialog({ onClose }: { onClose: () => void }) {
  const ref = useDialogA11y<HTMLDivElement>(onClose);
  return (
    <div ref={ref} role="dialog" aria-modal="true" aria-label="probe" tabIndex={-1}>
      <button>first</button>
      <button>middle</button>
      <button>last</button>
    </div>
  );
}

/**
 * A dialog whose content is swapped for another step, which is what the install
 * flow does the moment install starts. The control the user was on is unmounted,
 * and the browser drops `document.activeElement` to <body>.
 */
function SteppedDialog({ onClose, step }: { onClose: () => void; step: number }) {
  const ref = useDialogA11y<HTMLDivElement>(onClose);
  return (
    <div ref={ref} role="dialog" aria-modal="true" aria-label="stepped" tabIndex={-1}>
      {/* Distinct keys so React unmounts the old step rather than reusing its
          DOM node, which is what swapping one step component for another does. */}
      {step === 0 ? (
        <div key="source">
          <button>install</button>
        </div>
      ) : (
        <div key="progress">
          <button>cancel</button>
        </div>
      )}
    </div>
  );
}

describe('useDialogA11y', () => {
  it('moves focus into the dialog on open', () => {
    render(<Dialog onClose={vi.fn()} />);
    expect(document.activeElement).toBe(screen.getByText('first'));
  });

  it('closes on Escape', () => {
    const onClose = vi.fn();
    render(<Dialog onClose={onClose} />);
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('wraps Tab from the last control back to the first', () => {
    render(<Dialog onClose={vi.fn()} />);
    screen.getByText('last').focus();
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Tab' });
    expect(document.activeElement).toBe(screen.getByText('first'));
  });

  it('wraps Shift+Tab from the first control to the last', () => {
    render(<Dialog onClose={vi.fn()} />);
    screen.getByText('first').focus();
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Tab', shiftKey: true });
    expect(document.activeElement).toBe(screen.getByText('last'));
  });

  it('leaves Tab alone in the middle of the dialog', () => {
    render(<Dialog onClose={vi.fn()} />);
    const middle = screen.getByText('middle');
    middle.focus();
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Tab' });
    expect(document.activeElement).toBe(middle);
  });

  it('returns focus to the opener on unmount', () => {
    const opener = document.createElement('button');
    document.body.appendChild(opener);
    opener.focus();
    const { unmount } = render(<Dialog onClose={vi.fn()} />);
    expect(document.activeElement).not.toBe(opener);
    unmount();
    expect(document.activeElement).toBe(opener);
    opener.remove();
  });

  /**
   * These dispatch from the document rather than from the dialog node, because
   * that is where the event actually originates once focus has been lost. A case
   * that fires on the dialog node cannot fail on any of this: the node is on the
   * propagation path by construction, so it tests the one path that was never
   * broken.
   */
  describe('after a step change drops focus to <body>', () => {
    it('still closes on Escape', () => {
      const onClose = vi.fn();
      const { rerender } = render(<SteppedDialog onClose={onClose} step={0} />);
      expect(document.activeElement).toBe(screen.getByText('install'));

      rerender(<SteppedDialog onClose={onClose} step={1} />);
      expect(document.activeElement).toBe(document.body);

      fireEvent.keyDown(document.body, { key: 'Escape' });
      expect(onClose).toHaveBeenCalledTimes(1);
    });

    it('pulls focus back in on Tab instead of letting it walk out', () => {
      const { rerender } = render(<SteppedDialog onClose={vi.fn()} step={0} />);
      rerender(<SteppedDialog onClose={vi.fn()} step={1} />);
      expect(document.activeElement).toBe(document.body);

      const handled = !fireEvent.keyDown(document.body, { key: 'Tab' });
      expect(handled).toBe(true);
      expect(document.activeElement).toBe(screen.getByText('cancel'));
    });
  });

  it('closes only the newest dialog when two are open and focus is nowhere', () => {
    const closeOuter = vi.fn();
    const closeInner = vi.fn();
    // Siblings, not nested: this is how an install outcome opens over the
    // detail overlay that launched it.
    const { rerender } = render(
      <>
        <Dialog onClose={closeOuter} />
        <SteppedDialog onClose={closeInner} step={0} />
      </>
    );
    rerender(
      <>
        <Dialog onClose={closeOuter} />
        <SteppedDialog onClose={closeInner} step={1} />
      </>
    );
    expect(document.activeElement).toBe(document.body);

    fireEvent.keyDown(document.body, { key: 'Escape' });
    expect(closeInner).toHaveBeenCalledTimes(1);
    expect(closeOuter).not.toHaveBeenCalled();
  });

  /**
   * Both dialogs stay mounted while one covers the other, so a screen reader
   * would otherwise be handed two live `aria-modal` dialogs with nothing
   * saying which is on top.
   */
  it('hides the dialog underneath from assistive tech, and restores it', () => {
    function Pair({ second }: { second: boolean }) {
      return (
        <>
          <Dialog onClose={vi.fn()} />
          {second && <SteppedDialog onClose={vi.fn()} step={0} />}
        </>
      );
    }
    const { rerender } = render(<Pair second={false} />);
    expect(screen.getByLabelText('probe')).not.toHaveAttribute('aria-hidden');

    rerender(<Pair second />);
    expect(screen.getByLabelText('probe')).toHaveAttribute('aria-hidden', 'true');
    expect(screen.getByLabelText('stepped')).not.toHaveAttribute('aria-hidden');
    // Focus arrives in the new dialog and stays: the one underneath is guarding
    // against focus that leaves it, and must already know it is covered.
    expect(document.activeElement).toBe(screen.getByText('install'));

    // Restored on the way back out, or the detail overlay stays invisible to a
    // screen reader for the rest of its life.
    rerender(<Pair second={false} />);
    expect(screen.getByLabelText('probe')).not.toHaveAttribute('aria-hidden');
  });

  it('ignores Escape from a layer outside the dialog', () => {
    const onClose = vi.fn();
    render(<Dialog onClose={onClose} />);
    // Stands in for a Radix menu, which portals to <body> and so raises its
    // keys from outside the dialog node. Its Escape closes the menu, not us.
    const portal = document.createElement('button');
    document.body.appendChild(portal);
    portal.focus();

    fireEvent.keyDown(portal, { key: 'Escape' });
    expect(onClose).not.toHaveBeenCalled();
    portal.remove();
  });

  /**
   * The MCP server form opens inside the sandbox settings panel, so both are on
   * the stack and the panel *contains* the form. Every key from the form is
   * "inside" the panel too; only the stack can say which of them it is for.
   */
  describe('with a dialog open inside another', () => {
    function Panel({ inner, onClose, onCloseInner }: {
      inner: boolean;
      onClose: () => void;
      onCloseInner: () => void;
    }) {
      const ref = useDialogA11y<HTMLDivElement>(onClose);
      return (
        <div ref={ref} role="dialog" aria-modal="true" aria-label="panel" tabIndex={-1}>
          <button>panel control</button>
          {inner && <Dialog onClose={onCloseInner} />}
        </div>
      );
    }

    it('closes only the inner dialog on Escape', () => {
      const closePanel = vi.fn();
      const closeInner = vi.fn();
      // The panel mounts first, as it does in the app: the form is opened from it.
      const { rerender } = render(<Panel inner={false} onClose={closePanel} onCloseInner={closeInner} />);
      rerender(<Panel inner onClose={closePanel} onCloseInner={closeInner} />);

      fireEvent.keyDown(screen.getByText('first'), { key: 'Escape' });
      expect(closeInner).toHaveBeenCalledTimes(1);
      expect(closePanel).not.toHaveBeenCalled();
    });

    it('does not hide the panel from assistive tech, since that would hide the dialog too', () => {
      const { rerender } = render(<Panel inner={false} onClose={vi.fn()} onCloseInner={vi.fn()} />);
      rerender(<Panel inner onClose={vi.fn()} onCloseInner={vi.fn()} />);
      expect(screen.getByLabelText('panel')).not.toHaveAttribute('aria-hidden');
    });
  });

  /**
   * A dropdown item that opens a dialog closes its menu, and Radix hands focus
   * back to the menu's trigger once the exit animation ends: after the dialog
   * has already focused itself, and onto a control the dialog now covers.
   */
  it('takes back focus handed to the page behind it, and returns there on close', () => {
    function Page({ open }: { open: boolean }) {
      return (
        <>
          <button>row menu</button>
          {open && <Dialog onClose={vi.fn()} />}
        </>
      );
    }
    // The menu item that opened the dialog, gone by the time focus comes back.
    const item = document.createElement('button');
    document.body.appendChild(item);
    item.focus();
    const { rerender } = render(<Page open />);
    item.remove();

    screen.getByText('row menu').focus();
    expect(document.activeElement).toBe(screen.getByText('first'));

    rerender(<Page open={false} />);
    expect(document.activeElement).toBe(screen.getByText('row menu'));
  });

  /**
   * Radix commits a menu selection while the menu is still open, so the dialog
   * mounts inside the click and its first focus meets the menu's trap, which
   * pulls focus back to the item. The menu is gone by the next task.
   */
  it('asks for focus again once the menu that opened it lets go', async () => {
    const container = document.body.appendChild(document.createElement('div'));
    const item = document.body.appendChild(document.createElement('button'));
    item.focus();
    const trap = (e: FocusEvent) => {
      if (e.target !== item) item.focus();
    };
    document.addEventListener('focusin', trap, true);
    render(<Dialog onClose={vi.fn()} />, { container });
    document.removeEventListener('focusin', trap, true);
    expect(document.activeElement).toBe(item);

    item.remove();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(document.activeElement).toBe(screen.getByText('first'));
    container.remove();
  });

  /**
   * A cached chat view stays mounted under display:none when the user moves to
   * another thread, and a dialog open in it goes with it. jsdom has no layout,
   * so the test stands in for what the browser reports about that dialog.
   */
  it('stands down while the view it was opened in is hidden', () => {
    const onClose = vi.fn();
    render(
      <>
        <button>next thread</button>
        <Dialog onClose={onClose} />
      </>,
    );
    const dialog = screen.getByRole('dialog');
    dialog.checkVisibility = () => false;

    const page = screen.getByText('next thread');
    page.focus();
    expect(document.activeElement).toBe(page);
    expect(fireEvent.keyDown(page, { key: 'Tab' })).toBe(true);
    fireEvent.keyDown(page, { key: 'Escape' });
    expect(onClose).not.toHaveBeenCalled();

    dialog.checkVisibility = () => true;
    fireEvent.keyDown(page, { key: 'Escape' });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('leaves Escape alone when a control inside has already handled it', () => {
    const onClose = vi.fn();
    function WithInlineEditor() {
      const ref = useDialogA11y<HTMLDivElement>(onClose);
      return (
        <div ref={ref} role="dialog" aria-modal="true" aria-label="probe" tabIndex={-1}>
          {/* An inline rename that cancels itself on Escape. */}
          <input
            aria-label="rename"
            onKeyDown={(e) => {
              if (e.key === 'Escape') e.preventDefault();
            }}
          />
        </div>
      );
    }
    render(<WithInlineEditor />);
    fireEvent.keyDown(screen.getByLabelText('rename'), { key: 'Escape' });
    expect(onClose).not.toHaveBeenCalled();
  });

  it('does not close on the Escape that abandons an IME composition', () => {
    const onClose = vi.fn();
    render(<Dialog onClose={onClose} />);
    fireEvent.keyDown(screen.getByText('first'), { key: 'Escape', isComposing: true });
    expect(onClose).not.toHaveBeenCalled();
  });

  /**
   * Two threads, each left with a dialog open. Coming back to the first one
   * makes its dialog the one acting again, and the tree has to say so: the
   * global ResizeObserver stub never calls back, so drive the callback the
   * hook registered on the box.
   */
  it('moves exposure to the dialog a thread switch brings back', () => {
    const callbacks: ResizeObserverCallback[] = [];
    const real = window.ResizeObserver;
    window.ResizeObserver = class {
      constructor(cb: ResizeObserverCallback) {
        callbacks.push(cb);
      }
      observe() {}
      unobserve() {}
      disconnect() {}
    } as unknown as typeof ResizeObserver;
    const boxesChanged = () => callbacks.forEach((cb) => cb([], {} as ResizeObserver));

    try {
      render(
        <>
          <Dialog onClose={vi.fn()} />
          <Dialog onClose={vi.fn()} />
        </>,
      );
      // `hidden: true`, because the point of the assertion below is that one of
      // the two is out of the accessibility tree.
      const [first, second] = screen.getAllByRole('dialog', { hidden: true });
      expect(first.getAttribute('aria-hidden')).toBe('true');
      expect(second.hasAttribute('aria-hidden')).toBe(false);

      second.checkVisibility = () => false;
      boxesChanged();
      expect(first.hasAttribute('aria-hidden')).toBe(false);
      expect(second.getAttribute('aria-hidden')).toBe('true');
    } finally {
      window.ResizeObserver = real;
    }
  });

  it('leaves focus on a layer that paints above the dialogs', () => {
    render(
      <>
        <div {...aboveDialogs}>
          <button>reload</button>
        </div>
        <Dialog onClose={vi.fn()} />
      </>,
    );
    const raised = screen.getByText('reload');
    raised.focus();
    expect(document.activeElement).toBe(raised);
  });
});

/**
 * Press, release and click are dispatched separately on purpose: that is what
 * a drag looks like to the DOM. The browser fires one `click` on the nearest
 * common ancestor of the press and the release, which is the backdrop whenever
 * either end is outside the panel, so `click` alone cannot tell a dismissal
 * from a text selection that ended past the panel's edge.
 */
describe('useBackdropDismiss', () => {
  function BackdropDialog({ onClose }: { onClose: () => void }) {
    const backdrop = useBackdropDismiss<HTMLDivElement>(onClose);
    return (
      <div data-testid="backdrop" {...backdrop}>
        <div data-testid="panel">
          <input defaultValue="half-typed url" />
        </div>
      </div>
    );
  }

  function gesture(down: HTMLElement, up: HTMLElement, from = { x: 20, y: 20 }, to = from) {
    fireEvent.mouseDown(down, { clientX: from.x, clientY: from.y });
    fireEvent.mouseUp(up, { clientX: to.x, clientY: to.y });
    // The click goes to the common ancestor; here that is always the backdrop.
    fireEvent.click(screen.getByTestId('backdrop'), { clientX: to.x, clientY: to.y });
  }

  it('closes when the press and the release both land on the backdrop', () => {
    const onClose = vi.fn();
    render(<BackdropDialog onClose={onClose} />);
    const backdrop = screen.getByTestId('backdrop');
    gesture(backdrop, backdrop);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('still closes on a click that wobbles a few pixels', () => {
    const onClose = vi.fn();
    render(<BackdropDialog onClose={onClose} />);
    const backdrop = screen.getByTestId('backdrop');
    gesture(backdrop, backdrop, { x: 20, y: 20 }, { x: 23, y: 22 });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('does not close when a drag started inside the panel', () => {
    const onClose = vi.fn();
    render(<BackdropDialog onClose={onClose} />);
    gesture(screen.getByTestId('panel'), screen.getByTestId('backdrop'));
    expect(onClose).not.toHaveBeenCalled();
  });

  it('does not close when a drag started outside and ended inside the panel', () => {
    const onClose = vi.fn();
    render(<BackdropDialog onClose={onClose} />);
    gesture(screen.getByTestId('backdrop'), screen.getByTestId('panel'));
    expect(onClose).not.toHaveBeenCalled();
  });

  it('does not close when a drag swept across the panel from one side to the other', () => {
    const onClose = vi.fn();
    render(<BackdropDialog onClose={onClose} />);
    const backdrop = screen.getByTestId('backdrop');
    gesture(backdrop, backdrop, { x: 20, y: 200 }, { x: 620, y: 210 });
    expect(onClose).not.toHaveBeenCalled();
  });

  it('ignores a press with a button other than the primary one', () => {
    const onClose = vi.fn();
    render(<BackdropDialog onClose={onClose} />);
    const backdrop = screen.getByTestId('backdrop');
    fireEvent.mouseDown(backdrop, { button: 2 });
    fireEvent.mouseUp(backdrop, { button: 2 });
    fireEvent.click(backdrop);
    expect(onClose).not.toHaveBeenCalled();
  });

  it('ignores a click that bubbled up from the panel', () => {
    const onClose = vi.fn();
    render(<BackdropDialog onClose={onClose} />);
    const panel = screen.getByTestId('panel');
    fireEvent.mouseDown(panel);
    fireEvent.mouseUp(panel);
    fireEvent.click(panel);
    expect(onClose).not.toHaveBeenCalled();
  });
});
