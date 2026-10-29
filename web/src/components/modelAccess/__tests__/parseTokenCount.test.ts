import { describe, expect, it } from 'vitest';
import { parseTokenCount } from '../CustomModelFields';

describe('parseTokenCount', () => {
  it.each([
    ['128K', 128_000],
    ['1m', 1_000_000],
    ['1.5M', 1_500_000],
    ['131,072', 131_072],
    [' 32768 ', 32_768],
  ])('reads %s as the tokens a provider\'s docs mean', (raw, tokens) => {
    expect(parseTokenCount(raw)).toBe(tokens);
  });

  it.each(['', 'lots', '128KB', '-5', '0'])('refuses %j', (raw) => {
    expect(parseTokenCount(raw)).toBeNull();
  });
});
