/**
 * What the deck's front card is called, in the locale a reader actually hears.
 *
 * The deck's own suite mocks `t` to return its key, which is right for asserting
 * behavior and useless for asserting a label. These cases run the real locale
 * resources, because the defect they lock was in the copy: an `aria-label`
 * replaces the name rendered inside the same button, so a label that only
 * counted the files left the front one unnameable.
 */
import { describe, expect, it, vi } from 'vitest';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import i18n from '@/i18n';
import { TurnFileCards } from '../TurnFileCards';
import type { TurnFile } from '../../../utils/turnFiles';

const files: TurnFile[] = [
  { path: 'results/review.md' },
  { path: 'results/deck.pptx' },
];

const label = () => screen.getAllByRole('button')[0].getAttribute('aria-label') ?? '';

/** The deck's own controls, since a test may put a button outside it too. */
const deckButtons = () => Array.from(screen.getByTestId('turn-files').querySelectorAll('button'));

/** Let one animation frame pass, which is the beat the deck's listeners wait. */
const frame = () => act(async () => { await new Promise((r) => requestAnimationFrame(() => r(null))); });

describe('collapsed deck card, accessible name', () => {
  it('leads with the file it is showing, so speech input can say it', () => {
    render(<TurnFileCards files={files} onOpenFile={vi.fn()} />);

    // Voice control matches a prefix of the accessible name, so the filename
    // has to come first, not be buried after the count.
    expect(label()).toBe('review.md, show all 2 files from this turn');
  });

  it('still says how many files the card stands for', () => {
    render(<TurnFileCards files={[...files, { path: 'results/model.py' }]} onOpenFile={vi.fn()} />);
    expect(label()).toContain('3');
  });

  it('names the file plainly when the deck holds only one', () => {
    render(<TurnFileCards files={[files[0]]} onOpenFile={vi.fn()} />);
    expect(label()).toBe('Open results/review.md');
  });

  it('keeps the focus on the deck when Escape shuts it', async () => {
    // Shutting the deck unmounts every card but the front one. A reader who had
    // tabbed onto one of those was dropped on `document.body`, so the next Tab
    // restarted at the top of the page rather than carrying on past the deck.
    render(<TurnFileCards files={files} onOpenFile={vi.fn()} />);
    fireEvent.click(screen.getAllByRole('button')[0]);
    // The collapse listeners attach a frame late, so the click that opened the
    // deck cannot immediately shut it again.
    await frame();

    const fanned = screen.getAllByRole('button');
    const behindTheFront = fanned[fanned.length - 1];
    act(() => behindTheFront.focus());
    expect(document.activeElement).toBe(behindTheFront);

    fireEvent.keyDown(document, { key: 'Escape' });
    await frame();

    // The deck really did shut: the cards behind the front one are gone.
    expect(deckButtons()).toHaveLength(1);
    expect(document.activeElement).toBe(deckButtons()[0]);
  });

  it('leaves the focus alone when a click elsewhere shuts the deck', async () => {
    // The control: a click is the reader putting the focus somewhere of their
    // own accord, and the deck has no business pulling it back.
    render(<TurnFileCards files={files} onOpenFile={vi.fn()} />);
    fireEvent.click(screen.getAllByRole('button')[0]);
    await frame();

    const outside = document.createElement('button');
    document.body.appendChild(outside);
    act(() => outside.focus());
    fireEvent.mouseDown(outside);
    await frame();

    expect(deckButtons()).toHaveLength(1);
    expect(document.activeElement).toBe(outside);
    outside.remove();
  });

  it('keeps the name in zh-CN, where the count string is a different shape', async () => {
    await i18n.changeLanguage('zh-CN');
    try {
      render(<TurnFileCards files={files} onOpenFile={vi.fn()} />);
      expect(label()).toBe('review.md，展开本轮的 2 个文件');
    } finally {
      // Unmount first: switching back under a live tree re-renders outside act.
      cleanup();
      await i18n.changeLanguage('en-US');
    }
  });
});
