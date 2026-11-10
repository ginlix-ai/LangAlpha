/** How much a replay page asks for on the screen it opens on. */
import { describe, it, expect } from 'vitest';
import {
  HISTORY_PAGE_TURNS,
  HISTORY_RELOAD_MARGIN_TURNS,
  JUMP_PAGE_TURNS,
  jumpPageBudget,
  newestPageReach,
  pageBudget,
  screenPageBudget,
} from '../pageBudget';

// A desktop screen 1000px tall: a floor of 5 turns, 3 MB.
const SCREEN = screenPageBudget(false, 1000);

describe('screenPageBudget', () => {
  it('gives a desktop 3 MB and a floor of a turn per 400px, between 5 and 10', () => {
    expect(screenPageBudget(false, 1000)).toEqual({ minTurns: 5, maxBytes: 3_000_000 });
    expect(screenPageBudget(false, 2801)).toEqual({ minTurns: 8, maxBytes: 3_000_000 });
    expect(screenPageBudget(false, 900)).toMatchObject({ minTurns: 5 });
    expect(screenPageBudget(false, 9000)).toMatchObject({ minTurns: 10 });
  });

  it('gives a phone 1.5 MB and a floor between 3 and 10', () => {
    expect(screenPageBudget(true, 700)).toEqual({ minTurns: 3, maxBytes: 1_500_000 });
    expect(screenPageBudget(true, 1201)).toEqual({ minTurns: 4, maxBytes: 1_500_000 });
    expect(screenPageBudget(true, 0)).toMatchObject({ minTurns: 3 });
    expect(screenPageBudget(true, 6000)).toMatchObject({ minTurns: 10 });
  });
});

describe('pageBudget', () => {
  it('asks for a page of turns, stopped by the bytes, never fewer than the screen fills', () => {
    expect(pageBudget(SCREEN)).toEqual({ limit: HISTORY_PAGE_TURNS, minTurns: 5, maxBytes: 3_000_000 });
  });

  it('delivers a reach whole, whatever its bytes', () => {
    expect(pageBudget(SCREEN, 12)).toEqual({ limit: HISTORY_PAGE_TURNS, minTurns: 12, maxBytes: 3_000_000 });
    expect(pageBudget(SCREEN, 46)).toEqual({ limit: 46, minTurns: 46, maxBytes: 3_000_000 });
    expect(pageBudget(SCREEN, 3)).toMatchObject({ limit: HISTORY_PAGE_TURNS, minTurns: 5 });
  });
});

describe('jumpPageBudget', () => {
  it('reaches a near turn in one page', () => {
    expect(jumpPageBudget(SCREEN, 3)).toEqual({ limit: HISTORY_PAGE_TURNS, minTurns: 5, maxBytes: 3_000_000 });
    expect(jumpPageBudget(SCREEN, 12)).toEqual({ limit: HISTORY_PAGE_TURNS, minTurns: 12, maxBytes: 3_000_000 });
  });

  it('lets the bytes stop a far reach after a page of turns, and never asks past its cap', () => {
    expect(jumpPageBudget(SCREEN, 46)).toEqual({ limit: 46, minTurns: HISTORY_PAGE_TURNS, maxBytes: 3_000_000 });
    expect(jumpPageBudget(SCREEN, 2000)).toEqual({ limit: JUMP_PAGE_TURNS, minTurns: HISTORY_PAGE_TURNS, maxBytes: 3_000_000 });
  });
});

describe('newestPageReach', () => {
  it('reaches nowhere on a first open', () => {
    expect(newestPageReach(null, 80)).toBe(0);
  });

  it('covers every turn back to where it reaches, plus the margin it is given', () => {
    // Turns 40..80 on screen: 41 turns, and the margin for turns landing
    // between the status read and the replay.
    expect(newestPageReach(40, 80, HISTORY_RELOAD_MARGIN_TURNS)).toBe(41 + HISTORY_RELOAD_MARGIN_TURNS);
    expect(newestPageReach(40, 80)).toBe(41);
  });

  it('reaches nowhere past a fork that left the newest turn behind the oldest loaded one', () => {
    expect(newestPageReach(50, 10)).toBe(0);
  });
});
