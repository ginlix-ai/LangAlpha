import { describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';
import { useSlashKey } from '../useSlashKey';

function Page({ onSlash }: { onSlash: () => void }) {
  useSlashKey(onSlash);
  return (
    <>
      <input aria-label="field" />
      <div role="dialog">
        <button type="button">in dialog</button>
      </div>
    </>
  );
}

const press = (target: EventTarget = document.body) => {
  const e = new KeyboardEvent('keydown', { key: '/', bubbles: true, cancelable: true });
  target.dispatchEvent(e);
  return e;
};

describe('useSlashKey', () => {
  it('takes / pressed on the page', () => {
    const onSlash = vi.fn();
    render(<Page onSlash={onSlash} />);
    expect(press().defaultPrevented).toBe(true);
    expect(onSlash).toHaveBeenCalledTimes(1);
  });

  it('leaves / to a text field, to an open dialog, and to whatever took it first', () => {
    const onSlash = vi.fn();
    const { getByLabelText, getByText } = render(<Page onSlash={onSlash} />);
    getByLabelText('field').focus();
    press(document.activeElement!);
    getByText('in dialog').focus();
    press(document.activeElement!);
    // A capture listener (the palette buffering keys while it opens) took it.
    const takeIt = (e: Event) => e.preventDefault();
    window.addEventListener('keydown', takeIt, true);
    (document.activeElement as HTMLElement).blur();
    press();
    window.removeEventListener('keydown', takeIt, true);
    expect(onSlash).not.toHaveBeenCalled();
  });
});
