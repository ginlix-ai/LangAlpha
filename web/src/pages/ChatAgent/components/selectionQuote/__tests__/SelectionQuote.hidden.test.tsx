import { act, fireEvent, render, screen } from '@testing-library/react';
import { createRef } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { makeQuoteAnchor, type RectAnchor } from '../quoteAnchor';
import { SelectionQuote } from '../SelectionQuote';

const anchor = makeQuoteAnchor<RectAnchor>({ kind: 'rect', text: 'Revenue grew', rect: { left: 10, top: 10, bottom: 20 } });
const props = { anchor, boundsRef: createRef<HTMLElement>(), label: 'From a.md', placeholder: 'Reply', addLabel: 'Add', onReply: () => {}, onAdd: () => {} };

describe('SelectionQuote on a surface that cannot take a quote for now', () => {
  it('keeps a reply being written, out of sight, for when it can', () => {
    const onDismiss = vi.fn();
    const { rerender } = render(<SelectionQuote {...props} composing onDismiss={onDismiss} />);
    fireEvent.change(screen.getByLabelText('Reply'), { target: { value: 'how much?' } });

    rerender(<SelectionQuote {...props} composing hidden onDismiss={onDismiss} />);
    expect(screen.queryByRole('textbox')).toBeNull();
    expect(onDismiss).not.toHaveBeenCalled();

    rerender(<SelectionQuote {...props} composing onDismiss={onDismiss} />);
    expect(screen.getByLabelText<HTMLTextAreaElement>('Reply').value).toBe('how much?');
  });

  it('closes a menu, which holds nothing', () => {
    const onDismiss = vi.fn();
    act(() => {
      render(<SelectionQuote {...props} composing={false} hidden onDismiss={onDismiss} />);
    });
    expect(onDismiss).toHaveBeenCalledOnce();
  });
});
