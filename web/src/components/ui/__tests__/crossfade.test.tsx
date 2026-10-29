/**
 * A state leaving a crossfade is still painted for its exit beat, over or
 * beside the one replacing it. It must already be gone for everyone else: a
 * screen reader read both names, and a click on the part of the old state the
 * new one does not cover reached a control that was no longer there.
 */
import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import { InPlaceSwap, SwapCell } from '../crossfade';

describe('SwapCell', () => {
  it('takes the leaving state out of reach while it fades', () => {
    const cell = (key: string) => (
      <SwapCell as="div" swapKey={key}>
        <button type="button">{key}</button>
      </SwapCell>
    );
    const { rerender } = render(cell('watchlist'));
    rerender(cell('portfolio'));

    const leaving = screen.getByText('watchlist').parentElement!;
    const arriving = screen.getByText('portfolio').parentElement!;
    expect(leaving).toHaveAttribute('aria-hidden', 'true');
    expect(leaving).toHaveAttribute('inert');
    expect(arriving).not.toHaveAttribute('aria-hidden');
    expect(arriving).not.toHaveAttribute('inert');
    expect(screen.getAllByRole('button')).toHaveLength(1);
  });

  it('lets the last state go when there is nothing to replace it', () => {
    const { rerender } = render(<SwapCell swapKey="task">Reading filings</SwapCell>);
    rerender(<SwapCell swapKey={null}>Reading filings</SwapCell>);

    expect(screen.getByText('Reading filings')).toHaveAttribute('inert');
  });
});

describe('InPlaceSwap', () => {
  it('keeps the state not shown out of reach', () => {
    render(<InPlaceSwap swapped={false} from="Copy" to="Copied" />);

    expect(screen.getByText('Copied')).toHaveAttribute('aria-hidden', 'true');
    expect(screen.getByText('Copied')).toHaveAttribute('inert');
    expect(screen.getByText('Copy')).not.toHaveAttribute('inert');
  });
});
