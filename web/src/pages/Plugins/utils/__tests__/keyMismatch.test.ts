// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { keyMismatch } from '../pluginSchemas';

const BD = '^bd_[A-Za-z0-9]{24}$';

describe('keyMismatch', () => {
  it('names the prefix only when the value lacks it', () => {
    expect(keyMismatch('xx_test', BD)).toEqual({ kind: 'prefix', prefix: 'bd_' });
    // The regression: a value with the prefix and the wrong length was told
    // it should start with bd_.
    expect(keyMismatch('bd_short', BD)).toEqual({ kind: 'length', actual: 8, min: 27, max: 27 });
    expect(keyMismatch(`bd_${'a'.repeat(23)}!`, BD)).toEqual({ kind: 'characters' });
  });

  it('reads open and bounded lengths, and gives up on other shapes', () => {
    expect(keyMismatch('short', '^[A-Za-z0-9%_-]{20,}$')).toEqual({
      kind: 'length',
      actual: 5,
      min: 20,
      max: null,
    });
    expect(keyMismatch('sk-ab', '^sk-\\w{8,12}$')).toEqual({ kind: 'length', actual: 5, min: 11, max: 15 });
    expect(keyMismatch('bd_x-y', '^bd_[a-z]{4}-[0-9]{8}$')).toEqual({ kind: 'format' });
  });
});
