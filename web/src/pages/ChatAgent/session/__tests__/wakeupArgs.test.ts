// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { quietArg, wakeupArgListed } from '../wakeupArgs';

// The rule the server marks a quiet turn changed by, read the same way here
// so the progress line, the quiet fold and the server agree on a call.
describe('wakeupArgListed', () => {
  it('counts a non-empty list, and not an empty one', () => {
    expect(wakeupArgListed({ triggers: [{ at: '2030-01-02T10:00' }] }, 'triggers')).toBe(true);
    expect(wakeupArgListed({ triggers: [] }, 'triggers')).toBe(false);
    expect(wakeupArgListed({ cancel: ['w_ab12'] }, 'cancel')).toBe(true);
  });

  it('reads a list that arrived as its JSON text', () => {
    expect(wakeupArgListed({ cancel: '["w_ab12"]' }, 'cancel')).toBe(true);
    expect(wakeupArgListed({ cancel: '[]' }, 'cancel')).toBe(false);
  });

  it('counts text that is not JSON after all when it is not empty', () => {
    expect(wakeupArgListed({ cancel: '[w_ab12' }, 'cancel')).toBe(true);
    expect(wakeupArgListed({ cancel: 'w_ab12' }, 'cancel')).toBe(true);
    expect(wakeupArgListed({ cancel: '' }, 'cancel')).toBe(false);
  });

  it('counts an object only when it has keys', () => {
    expect(wakeupArgListed({ triggers: { at: 'x' } }, 'triggers')).toBe(true);
    expect(wakeupArgListed({ triggers: {} }, 'triggers')).toBe(false);
  });

  it('reads nothing from arguments that are not an object or lack the key', () => {
    expect(wakeupArgListed(null, 'cancel')).toBe(false);
    expect(wakeupArgListed('cancel', 'cancel')).toBe(false);
    expect(wakeupArgListed([], 'cancel')).toBe(false);
    expect(wakeupArgListed({ triggers: [1] }, 'cancel')).toBe(false);
  });
});

describe('quietArg', () => {
  it('trims the line a quiet end passed and is empty otherwise', () => {
    expect(quietArg({ quiet: '  nothing new ' })).toBe('nothing new');
    expect(quietArg({ quiet: 3 })).toBe('');
    expect(quietArg({})).toBe('');
    expect(quietArg(undefined)).toBe('');
  });
});
