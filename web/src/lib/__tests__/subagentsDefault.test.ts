// @vitest-environment node
/**
 * The default reads off only for an explicit false, matching how the server
 * reads the key for a thread with no value of its own; anything else, absent
 * or malformed, is on.
 */
import { describe, it, expect } from 'vitest';
import { readSubagentsDefault, subagentsDefaultPatch } from '../subagentsDefault';

describe('readSubagentsDefault', () => {
  it('is off only for an explicit false', () => {
    expect(readSubagentsDefault({ other_preference: { subagents_default: false } })).toBe(false);
  });

  it('is on when the key is true, absent, or not a boolean', () => {
    expect(readSubagentsDefault({ other_preference: { subagents_default: true } })).toBe(true);
    expect(readSubagentsDefault({ other_preference: {} })).toBe(true);
    expect(readSubagentsDefault({ other_preference: { subagents_default: 'false' } })).toBe(true);
    expect(readSubagentsDefault({ other_preference: { subagents_default: null } })).toBe(true);
  });

  it('is on with no preferences, or an other_preference that is not an object', () => {
    expect(readSubagentsDefault(null)).toBe(true);
    expect(readSubagentsDefault(undefined)).toBe(true);
    expect(readSubagentsDefault({})).toBe(true);
    expect(readSubagentsDefault({ other_preference: [false] })).toBe(true);
    expect(readSubagentsDefault({ other_preference: 'off' })).toBe(true);
  });
});

describe('subagentsDefaultPatch', () => {
  it('sends the one key alone, so the shallow merge leaves its siblings standing', () => {
    expect(subagentsDefaultPatch(false)).toEqual({ other_preference: { subagents_default: false } });
    expect(subagentsDefaultPatch(true)).toEqual({ other_preference: { subagents_default: true } });
  });
});
