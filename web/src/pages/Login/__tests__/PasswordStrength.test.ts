import { describe, it, expect } from 'vitest';
import { scorePassword } from '@/pages/Login/PasswordStrength';

// scorePassword bands (see PasswordStrength.tsx):
//   len < 8                                  -> 0
//   len >= 16  OR  (len >= 12 && classes>=3) -> 3
//   len >= 10 && classes >= 2                -> 2
//   otherwise                                -> 1
// where `classes` counts the present character sets among lower, upper, digit,
// and symbol. Each table below pins one band, hitting the boundaries between it
// and its neighbours.
describe('scorePassword', () => {
  it.each([
    ['', 'empty'],
    ['aB3$x', '5 chars, 4 classes'],
    ['aB3$xyz', '7 chars, one below the floor'],
  ])('scores 0 under the 8-char minimum (%s — %s)', (pw) => {
    expect(scorePassword(pw)).toBe(0);
  });

  it.each([
    ['abcdefgh', '8 chars, single class'],
    ['aB3$defg', '8 chars, all four classes — length caps it at 1'],
    ['aB3$defgh', '9 chars, all four classes'],
    ['abcdefghij', '10 chars, single class (needs 2 for a 2)'],
    ['abcdefghijk', '11 chars, single class'],
    ['abcdefghijkl', '12 chars, single class (needs 3 for a 3)'],
    ['a'.repeat(15), '15 chars, single class — one below the len-16 jump'],
  ])('scores 1 for the weak band (%s — %s)', (pw) => {
    expect(scorePassword(pw)).toBe(1);
  });

  it.each([
    ['abcdefghi1', '10 chars, 2 classes — lower boundary'],
    ['abcdefghij1', '11 chars, 2 classes'],
    ['abcdefghij12', '12 chars, only 2 classes — not enough for a 3'],
    ['abcdefghijklm12', '15 chars, 2 classes'],
  ])('scores 2 for medium length with 2 classes (%s — %s)', (pw) => {
    expect(scorePassword(pw)).toBe(2);
  });

  it.each([
    ['abcABC123def', '12 chars, 3 classes — the len>=12 boundary'],
    ['Abcdefghij12345', '15 chars, 3 classes'],
    ['a'.repeat(16), '16 chars, single class — length alone reaches strong'],
    ['aB3$aB3$aB3$aB3$aB3$', '20 chars, all four classes'],
  ])('scores 3 for long or long-and-varied passwords (%s — %s)', (pw) => {
    expect(scorePassword(pw)).toBe(3);
  });
});
