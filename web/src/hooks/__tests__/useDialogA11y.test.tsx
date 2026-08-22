import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { useBackdropDismiss, useDialogA11y } from '../useDialogA11y';

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
});

/**
 * The press and the release are dispatched separately on purpose: that is what
 * a drag out of the dialog actually looks like to the DOM. The browser then
 * fires one `click` on the nearest common ancestor, which is the backdrop, so
 * a backdrop that only listens for `click` cannot tell a dismissal from a
 * text selection that ended past the panel's edge.
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

  it('closes when the press and the release both land on the backdrop', () => {
    const onClose = vi.fn();
    render(<BackdropDialog onClose={onClose} />);
    const backdrop = screen.getByTestId('backdrop');
    fireEvent.mouseDown(backdrop);
    fireEvent.click(backdrop);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('does not close when a drag started inside the panel', () => {
    const onClose = vi.fn();
    render(<BackdropDialog onClose={onClose} />);
    fireEvent.mouseDown(screen.getByTestId('panel'));
    fireEvent.click(screen.getByTestId('backdrop'));
    expect(onClose).not.toHaveBeenCalled();
  });

  it('ignores a click that bubbled up from the panel', () => {
    const onClose = vi.fn();
    render(<BackdropDialog onClose={onClose} />);
    const panel = screen.getByTestId('panel');
    fireEvent.mouseDown(panel);
    fireEvent.click(panel);
    expect(onClose).not.toHaveBeenCalled();
  });
});
