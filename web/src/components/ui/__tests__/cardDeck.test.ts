// @vitest-environment node
/**
 * The deck geometry, and the one rule three separate copies of it each got
 * wrong: the offset has to ride `transform`, because that is the only property
 * any of the decks transitions. A card moved by `top` teleports.
 */
import { describe, it, expect } from 'vitest';
import { deckHeight, deckPeekLayers, deckSlot, type DeckGeometry } from '../cardDeck';

const G: DeckGeometry = {
  cardHeight: 68,
  cardGap: 8,
  peekStep: 6,
  maxPeekLayers: 2,
  peekScaleStep: 0.02,
  minPeekScale: 0.9,
};

describe('card deck geometry', () => {
  it('carries the fanned offset in the transform and sets no top', () => {
    const shut = deckSlot(1, 3, false, G);
    const open = deckSlot(1, 3, true, G);
    expect(shut.style.transform).toBe('translateY(6px) scale(0.98)');
    expect(open.style.transform).toBe('translateY(76px) scale(1)');
    expect(open.style).not.toHaveProperty('top');
  });

  it('stacks to the cards it shows, and to the peeks it hints at', () => {
    expect(deckHeight(1, false, G)).toBe(68);
    expect(deckHeight(3, false, G)).toBe(68 + 2 * 6);
    expect(deckHeight(3, true, G)).toBe(3 * 76 - 8);
  });

  it('caps the peek at maxPeekLayers however deep the deck goes', () => {
    expect(deckPeekLayers(1, G)).toBe(0);
    expect(deckPeekLayers(9, G)).toBe(2);
    // Past the cap a card parks under the deepest peek rather than spilling
    // below the stack, and stays mounted so opening is one continuous slide.
    const buried = deckSlot(5, 9, false, G);
    expect(buried.style.transform).toBe(deckSlot(2, 9, false, G).style.transform);
    expect(buried.style.opacity).toBe(0);
  });

  it('answers to the pointer on the front card, and on every card once open', () => {
    expect(deckSlot(0, 3, false, G).interactive).toBe(true);
    expect(deckSlot(1, 3, false, G).interactive).toBe(false);
    expect(deckSlot(1, 3, true, G).interactive).toBe(true);
    expect(deckSlot(1, 3, false, G).style.pointerEvents).toBe('none');
  });

  it('draws the front card over the ones behind it', () => {
    expect(deckSlot(0, 3, false, G).style.zIndex).toBe(3);
    expect(deckSlot(2, 3, false, G).style.zIndex).toBe(1);
  });
});
