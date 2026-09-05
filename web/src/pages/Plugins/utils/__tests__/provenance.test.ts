/**
 * The three questions every deck on the Plugins page asks about a row, and
 * the pill predicate that reads them.
 *
 * They exist because a row carries two independent switches: its own
 * `enabled`, and the package's. Only the pair says what the agent sees, and
 * reading the wrong one is invisible in the type system and invisible on
 * screen until a package is switched off, which is a state no default
 * fixture is in. That is exactly how a deck came to report every skill of a
 * switched-off bundle as enabled while its own badge said none were.
 */
import { describe, expect, it } from 'vitest';

import {
  isEffectivelyEnabled,
  isPluginOwned,
  isPluginSuppressed,
  type PluginProvenancedRow,
} from '../provenance';
import { matchesStateFilter } from '../../components/ListControls';

function row(fields: Partial<Record<string, unknown>>): PluginProvenancedRow {
  return { name: 'r', enabled: true, ...fields } as PluginProvenancedRow;
}

describe('package ownership', () => {
  it('is the presence of an owning package, not its state', () => {
    expect(isPluginOwned(row({ plugin_name: 'yfinance' }))).toBe(true);
    expect(
      isPluginOwned(row({ plugin_name: 'yfinance', plugin_enabled: false })),
    ).toBe(true);
    expect(isPluginOwned(row({}))).toBe(false);
  });

  it('treats an empty package name as no owner', () => {
    expect(isPluginOwned(row({ plugin_name: '' }))).toBe(false);
  });
});

describe('suppression', () => {
  it('needs both an owner and that owner switched off', () => {
    expect(
      isPluginSuppressed(row({ plugin_name: 'y', plugin_enabled: false })),
    ).toBe(true);
    expect(
      isPluginSuppressed(row({ plugin_name: 'y', plugin_enabled: true })),
    ).toBe(false);
    // No owner, so nothing can be holding it down however the flag reads.
    expect(isPluginSuppressed(row({ plugin_enabled: false }))).toBe(false);
  });

  it('reads unknown package state as not suppressed', () => {
    // A row we cannot prove is held down is shown as it is, rather than
    // greyed out on a field the tier never sends.
    expect(isPluginSuppressed(row({ plugin_name: 'y' }))).toBe(false);
  });
});

describe('effective state', () => {
  it('is the row switched on and its package not holding it down', () => {
    expect(
      isEffectivelyEnabled(row({ plugin_name: 'y', plugin_enabled: true })),
    ).toBe(true);
    expect(
      isEffectivelyEnabled(row({ plugin_name: 'y', plugin_enabled: false })),
    ).toBe(false);
  });

  it('separates the two switches rather than collapsing them', () => {
    // The pair the count bug turned on: switched on by the user, off by the
    // package. `enabled` alone answers true and the deck reports it live.
    const suppressed = row({
      enabled: true,
      plugin_name: 'y',
      plugin_enabled: false,
    });
    expect(suppressed.enabled).toBe(true);
    expect(isEffectivelyEnabled(suppressed)).toBe(false);

    // And the reverse: the package is on, the user switched the row off.
    expect(
      isEffectivelyEnabled(
        row({ enabled: false, plugin_name: 'y', plugin_enabled: true }),
      ),
    ).toBe(false);
  });
});

describe('the state pills', () => {
  it('reads on and off from the stored flag, not the effective one', () => {
    // Deliberate, and shared with the MCP tab: On and Off are what the user
    // set. Suppression is a third state, which is why the attention pill
    // exists rather than folding suppressed rows into Off.
    expect(matchesStateFilter('on', true, true)).toBe(true);
    expect(matchesStateFilter('off', true, true)).toBe(false);
  });

  it('routes a row needing a human to attention regardless of its flag', () => {
    expect(matchesStateFilter('attention', true, true)).toBe(true);
    expect(matchesStateFilter('attention', false, true)).toBe(true);
    expect(matchesStateFilter('attention', true, false)).toBe(false);
  });

  it('defaults attention to false so a list without one is unaffected', () => {
    expect(matchesStateFilter('attention', true)).toBe(false);
    expect(matchesStateFilter('on', true)).toBe(true);
  });

  it('keeps every row under all', () => {
    expect(matchesStateFilter('all', false, false)).toBe(true);
    expect(matchesStateFilter('all', true, true)).toBe(true);
  });
});
