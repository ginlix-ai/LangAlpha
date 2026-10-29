/**
 * A row hands its place to a card only when it leaves because its call landed
 * as that card. A row already folding for another reason keeps folding where
 * it is when the card lands later, so the card does not take its place too.
 */
import React from 'react';
import { describe, expect, it } from 'vitest';
import { render } from '@testing-library/react';
import { AnimatePresence } from '@/lib/framer';
import { LiveRow } from '../LiveRow';
import { CardCallsContext } from '../cardCalls';

const NONE: ReadonlySet<string> = new Set();
const Q1: ReadonlySet<string> = new Set(['q1']);

function Zone({ show, cards }: { show: boolean; cards: ReadonlySet<string> }) {
  return (
    <CardCallsContext.Provider value={cards}>
      <AnimatePresence>
        {show && (
          <LiveRow key="row" calls={['q1']}>
            <span>Getting a quote</span>
          </LiveRow>
        )}
      </AnimatePresence>
    </CardCallsContext.Provider>
  );
}

const row = (container: HTMLElement) => container.querySelector<HTMLElement>('[data-call-row="q1"]');

describe('LiveRow hand-over', () => {
  it('hands over when it leaves in the commit its card lands', () => {
    const { container, rerender } = render(<Zone show cards={NONE} />);
    rerender(<Zone show={false} cards={Q1} />);
    expect(row(container)?.hasAttribute('data-handed-over')).toBe(true);
  });

  it('keeps folding when its card lands after it left', () => {
    const { container, rerender } = render(<Zone show cards={NONE} />);
    rerender(<Zone show={false} cards={NONE} />);
    expect(row(container)?.hasAttribute('data-handed-over')).toBe(false);
    rerender(<Zone show={false} cards={Q1} />);
    expect(row(container)?.hasAttribute('data-handed-over')).toBe(false);
  });
});
