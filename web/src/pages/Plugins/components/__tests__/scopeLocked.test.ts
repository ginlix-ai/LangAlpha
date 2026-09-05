import { describe, expect, it } from 'vitest';
import { scopeLocked } from '../ScopeControl';

/**
 * The checklist adds a workspace deny-marker with one click and removes it
 * with another, but the removal goes through a re-enable that 409s whenever
 * the account tier already subtracts the row. A row can be subtracted two
 * ways: its own switch, or the package that ships it being off. Reading only
 * `enabled` leaves an interactive control that makes a change it cannot undo.
 */
describe('scopeLocked', () => {
  it('leaves a live row interactive', () => {
    expect(scopeLocked({ enabled: true, plugin_enabled: true })).toBe(false);
  });

  it('locks a row switched off on its own', () => {
    expect(scopeLocked({ enabled: false })).toBe(true);
  });

  it('locks a live row whose package is switched off', () => {
    // The case the server flag alone cannot see: a bundled server keeps
    // enabled:true and travels with plugin_enabled:false.
    expect(scopeLocked({ enabled: true, plugin_enabled: false })).toBe(true);
  });

  it('does not lock a row that belongs to no package', () => {
    // Absent and null both mean "no package", not "package is off".
    expect(scopeLocked({ enabled: true })).toBe(false);
    expect(scopeLocked({ enabled: true, plugin_enabled: null })).toBe(false);
  });
});
