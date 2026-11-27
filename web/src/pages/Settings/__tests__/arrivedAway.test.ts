/**
 * A portal page Settings cannot show here goes to the portal itself, unless
 * the window was only narrowed on it: that keeps the same history entry, and
 * the app stays.
 */
import { describe, expect, it } from 'vitest';
import { runAuthResets } from '@/lib/authResets';
import { arrivesAway, noteFramed } from '../arrivedAway';
import type { SettingsView } from '../sections';

const away: SettingsView = { kind: 'away', page: '/billing' };
const section: SettingsView = { kind: 'section', section: 'general' };

describe('a portal page Settings cannot show', () => {
  it('goes to the portal on an entry never shown framed', () => {
    expect(arrivesAway(away, 'entry-1')).toBe(true);
  });

  it('stays on the entry just shown framed, which the window narrowed on', () => {
    noteFramed('entry-2');
    expect(arrivesAway(away, 'entry-2')).toBe(false);
  });

  it('goes to the portal on another entry, as Back or a link reaches', () => {
    noteFramed('entry-3');
    expect(arrivesAway(away, 'entry-4')).toBe(true);
  });

  it('forgets the framed entry at sign-out', () => {
    noteFramed('entry-5');
    runAuthResets();
    expect(arrivesAway(away, 'entry-5')).toBe(true);
  });

  it('is never reached from a section of Settings', () => {
    expect(arrivesAway(section, 'entry-6')).toBe(false);
  });
});
