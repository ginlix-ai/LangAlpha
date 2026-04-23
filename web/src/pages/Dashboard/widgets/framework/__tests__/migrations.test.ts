import { describe, it, expect } from 'vitest';
import { migrateDashboardPrefs } from '../migrations';
import { DASHBOARD_PREFS_VERSION } from '../../types';

describe('migrateDashboardPrefs', () => {
  it('returns null for null / undefined / non-object input', () => {
    expect(migrateDashboardPrefs(null)).toBeNull();
    expect(migrateDashboardPrefs(undefined)).toBeNull();
    expect(migrateDashboardPrefs('not an object')).toBeNull();
    expect(migrateDashboardPrefs(42)).toBeNull();
  });

  it('renames legacy agent.input widget type to agent.conversation', () => {
    const out = migrateDashboardPrefs({
      widgets: [{ id: 'a', type: 'agent.input', config: {} }],
    });
    expect(out?.widgets[0].type).toBe('agent.conversation');
  });

  it('leaves non-legacy widget types untouched', () => {
    const out = migrateDashboardPrefs({
      widgets: [{ id: 'a', type: 'news.feed', config: {} }],
    });
    expect(out?.widgets[0].type).toBe('news.feed');
  });

  it('coerces non-array widgets to []', () => {
    const out = migrateDashboardPrefs({ widgets: 'not an array' as unknown });
    expect(out?.widgets).toEqual([]);
  });

  it('coerces array layouts to {}', () => {
    const out = migrateDashboardPrefs({ layouts: [] as unknown });
    expect(out?.layouts).toEqual({});
  });

  it('preserves valid layouts as-is', () => {
    const layouts = { lg: [{ i: 'a', x: 0, y: 0, w: 4, h: 4 }] };
    const out = migrateDashboardPrefs({ layouts });
    expect(out?.layouts).toEqual(layouts);
  });

  it("defaults unknown mode to 'classic'", () => {
    expect(migrateDashboardPrefs({ mode: 'bogus' as unknown })?.mode).toBe('classic');
    expect(migrateDashboardPrefs({})?.mode).toBe('classic');
  });

  it("honors explicit 'custom' mode", () => {
    expect(migrateDashboardPrefs({ mode: 'custom' })?.mode).toBe('custom');
  });

  it('always stamps the current version', () => {
    const out = migrateDashboardPrefs({ version: 0 as unknown, widgets: [] });
    expect(out?.version).toBe(DASHBOARD_PREFS_VERSION);
  });

  it('drops malformed history', () => {
    expect(migrateDashboardPrefs({ history: 'nope' as unknown })?.history).toBeUndefined();
  });

  it('preserves a valid history array', () => {
    const history = [{ widgets: [], layouts: {} }];
    expect(migrateDashboardPrefs({ history })?.history).toEqual(history);
  });
});
