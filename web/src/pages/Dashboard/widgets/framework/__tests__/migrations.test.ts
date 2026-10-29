import { describe, it, expect } from 'vitest';
import { migrateDashboardPrefs } from '../migrations';
import { DASHBOARD_PREFS_VERSION } from '../../types';
// Side-effect: load every widget definition so getWidget() returns schemas
// inside `sanitizeConfig`. Without this, the registry is empty and schemas
// silently no-op (the production app reaches the registry transitively via
// Dashboard; tests have to do it explicitly).
import '../../index';
import { getWidget, listWidgets } from '../WidgetRegistry';

/** A stored record already on the current version, so only sanitizing applies. */
const migrate = (fields: Record<string, unknown>) =>
  migrateDashboardPrefs({ version: DASHBOARD_PREFS_VERSION, ...fields });

describe('migrateDashboardPrefs', () => {
  it('returns null for null / undefined / non-object input', () => {
    expect(migrateDashboardPrefs(null)).toBeNull();
    expect(migrateDashboardPrefs(undefined)).toBeNull();
    expect(migrateDashboardPrefs('not an object')).toBeNull();
    expect(migrateDashboardPrefs(42)).toBeNull();
  });

  it('renames legacy agent.input widget type to agent.conversation', () => {
    const out = migrate({
      widgets: [{ id: 'a', type: 'agent.input', config: {} }],
    });
    expect(out?.widgets[0].type).toBe('agent.conversation');
  });

  it('leaves non-legacy widget types untouched', () => {
    const out = migrate({
      widgets: [{ id: 'a', type: 'news.feed', config: {} }],
    });
    expect(out?.widgets[0].type).toBe('news.feed');
  });

  it('coerces non-array widgets to []', () => {
    const out = migrate({ widgets: 'not an array' as unknown });
    expect(out?.widgets).toEqual([]);
  });

  it('coerces array layouts to {}', () => {
    const out = migrate({ layouts: [] as unknown });
    expect(out?.layouts).toEqual({});
  });

  it('preserves valid layouts as-is', () => {
    const layouts = { lg: [{ i: 'a', x: 0, y: 0, w: 4, h: 4 }] };
    const out = migrate({ layouts });
    expect(out?.layouts).toEqual(layouts);
  });

  describe('versions', () => {
    const widgets = [{ id: 'a', type: 'news.feed', config: {} }];
    const layouts = { lg: [{ i: 'a', x: 0, y: 0, w: 4, h: 4 }] };

    it('reads a v1 Classic record as no saved layout, even with widgets in it', () => {
      expect(migrateDashboardPrefs({ version: 1, mode: 'classic', widgets, layouts })).toBeNull();
    });

    it('reads a record with no version and no custom mode as no saved layout', () => {
      expect(migrateDashboardPrefs({})).toBeNull();
      expect(migrateDashboardPrefs({ widgets, layouts })).toBeNull();
      expect(migrateDashboardPrefs({ version: 1, widgets })).toBeNull();
    });

    it('moves a v1 Custom record to v2, keeping its widgets and dropping mode', () => {
      const out = migrateDashboardPrefs({ version: 1, mode: 'custom', widgets, layouts });
      expect(out).not.toBeNull();
      expect(out?.version).toBe(DASHBOARD_PREFS_VERSION);
      expect(out?.widgets).toEqual(widgets);
      expect(out?.layouts).toEqual(layouts);
      expect(out).not.toHaveProperty('mode');
    });

    it('passes a v2 record through', () => {
      const history = [{ widgets: [], layouts: {} }];
      const out = migrateDashboardPrefs({
        version: DASHBOARD_PREFS_VERSION, widgets, layouts, lastBreakpoint: 'md', history,
      });
      expect(out).toEqual({ version: DASHBOARD_PREFS_VERSION, widgets, layouts, lastBreakpoint: 'md', history });
    });
  });

  it('drops malformed history', () => {
    expect(migrate({ history: 'nope' as unknown })?.history).toBeUndefined();
  });

  it('preserves a valid history array', () => {
    const history = [{ widgets: [], layouts: {} }];
    expect(migrate({ history })?.history).toEqual(history);
  });

  // ---------------------------------------------------------------------------
  // layouts shape filter — per-breakpoint validation
  // ---------------------------------------------------------------------------
  describe('layouts per-breakpoint shape filter', () => {
    it('keeps real RGL arrays as-is', () => {
      const layouts = { lg: [{ i: 'a', x: 0, y: 0, w: 4, h: 4 }] };
      expect(migrate({ layouts })?.layouts).toEqual(layouts);
    });

    it('drops breakpoints whose value is an object instead of an array', () => {
      // Without the per-bp guard this passed straight through and crashed
      // reconcileLayouts (it `.filter()`s each breakpoint).
      const out = migrate({
        layouts: { lg: { foo: 'bar' }, md: [{ i: 'a', x: 0, y: 0, w: 4, h: 4 }] },
      });
      expect(out?.layouts.lg).toBeUndefined();
      expect(out?.layouts.md?.length).toBe(1);
    });

    it('drops breakpoints whose value is a primitive', () => {
      const out = migrate({
        layouts: { lg: 'garbage', md: 42, sm: null },
      });
      expect(out?.layouts).toEqual({});
    });
  });

  // ---------------------------------------------------------------------------
  // isValidWidgetInstance: shape filter
  // ---------------------------------------------------------------------------
  describe('widget instance shape filter', () => {
    it('drops null entries', () => {
      const out = migrate({
        widgets: [null, { id: 'a', type: 'news.feed', config: {} }],
      });
      expect(out?.widgets.length).toBe(1);
      expect(out?.widgets[0].id).toBe('a');
    });

    it('drops string / number entries', () => {
      const out = migrate({
        widgets: ['garbage', 42, { id: 'a', type: 'news.feed', config: {} }],
      });
      expect(out?.widgets.length).toBe(1);
    });

    it('drops entries missing an id', () => {
      const out = migrate({
        widgets: [{ type: 'news.feed', config: {} }, { id: 'b', type: 'news.feed', config: {} }],
      });
      expect(out?.widgets.length).toBe(1);
      expect(out?.widgets[0].id).toBe('b');
    });

    it('drops entries missing a type or config', () => {
      const out = migrate({
        widgets: [
          { id: 'a', type: 'news.feed' /* no config */ },
          { id: 'b', /* no type */ config: {} },
          { id: 'c', type: 'news.feed', config: null /* invalid */ },
          { id: 'd', type: 'news.feed', config: {} },
        ],
      });
      expect(out?.widgets.length).toBe(1);
      expect(out?.widgets[0].id).toBe('d');
    });
  });

  // ---------------------------------------------------------------------------
  // sanitizeConfig: per-widget Zod schema validation
  // ---------------------------------------------------------------------------
  describe('sanitizeConfig (Zod-driven)', () => {
    it('passes a valid config through unchanged', () => {
      const cfg = { symbols: ['NASDAQ:NVDA'], displayMode: 'compact' as const };
      const out = migrate({
        widgets: [{ id: 'a', type: 'tv.ticker-tape', config: cfg }],
      });
      expect(out?.widgets[0].config).toMatchObject(cfg);
    });

    it('coerces an unknown enum value via .catch()', () => {
      const out = migrate({
        widgets: [
          {
            id: 'a',
            type: 'tv.ticker-tape',
            config: { symbols: ['NASDAQ:NVDA'], displayMode: 'foobar' },
          },
        ],
      });
      // The kept symbol is what proves the field-level catch ran: the
      // whole-config fallback also yields 'adaptive', but with no symbols.
      expect(out?.widgets[0].config).toEqual({ symbols: ['NASDAQ:NVDA'], displayMode: 'adaptive' });
    });

    // Each bad field sits beside a good one that differs from defaultConfig,
    // because the catch defaults equal defaultConfig: a config that recovered
    // field by field and one that fell back wholesale look the same otherwise.
    it.each([
      ['displayMode', { symbols: ['NASDAQ:NVDA'], displayMode: 42 }, { symbols: ['NASDAQ:NVDA'], displayMode: 'adaptive' }],
      ['symbols', { displayMode: 'compact' }, { symbols: [], displayMode: 'compact' }],
    ])('recovers a bad %s value via its own .catch() and keeps the other field', (_field, config, expected) => {
      const out = migrate({
        widgets: [{ id: 'a', type: 'tv.ticker-tape', config }],
      });
      expect(out?.widgets[0].config).toEqual(expected);
    });

    it('falls back to defaultConfig when top-level config shape is unparseable', () => {
      // Array config slips past the shape filter (typeof === 'object' and
      // !== null) but z.object().safeParse([]) fails at the top level — no
      // per-field .catch() can recover it. Sanitize must fall back to
      // defaultConfig wholesale.
      const out = migrate({
        widgets: [{ id: 'a', type: 'tv.ticker-tape', config: [] as unknown as Record<string, unknown> }],
      });
      const def = getWidget('tv.ticker-tape')?.defaultConfig as object;
      expect(out?.widgets[0].config).toEqual(def);
    });

    it('preserves widgets whose definition has no schema (back-compat)', () => {
      // No widget type uses a missing schema today (all 30 register one), but
      // the back-compat branch must still hold for future no-schema widgets.
      // Simulate by stamping an unknown type that getWidget returns undefined
      // for; sanitize should pass it through after the rename pass.
      // (This widget will fail the rename map, then sanitize sees no def and
      // returns the widget as-is.)
      const out = migrate({
        widgets: [{ id: 'a', type: 'made-up.no-schema', config: { freeform: true } }],
      });
      expect(out?.widgets[0].config).toEqual({ freeform: true });
    });

    it('rename happens BEFORE sanitize (legacy type uses new schema)', () => {
      // agent.input → agent.conversation. ConversationConfigSchema accepts {}.
      const out = migrate({
        widgets: [{ id: 'a', type: 'agent.input', config: {} }],
      });
      expect(out?.widgets[0].type).toBe('agent.conversation');
    });

    it('drops invalid symbols from array fields (no duplicate-default fallout)', () => {
      // Regression: per-element `.catch(default)` produced duplicates of the
      // catch default for every bad entry — `[bad, bad, AAPL]` became
      // `[SPY, SPY, AAPL]`. Now we drop bad entries instead.
      const out = migrate({
        widgets: [
          {
            id: 'a',
            type: 'tv.ticker-tape',
            config: {
              symbols: ['NASDAQ:NVDA', 'b@d sym', 42, null, 'NASDAQ:AAPL'],
              displayMode: 'compact',
            },
          },
        ],
      });
      const symbols = (out?.widgets[0].config as { symbols?: string[] }).symbols;
      expect(symbols).toEqual(['NASDAQ:NVDA', 'NASDAQ:AAPL']);
    });

    it('dedupes symbol arrays across the transform', () => {
      const out = migrate({
        widgets: [
          {
            id: 'a',
            type: 'tv.ticker-tape',
            config: { symbols: ['NVDA', 'NVDA', 'AAPL', 'NVDA'], displayMode: 'compact' },
          },
        ],
      });
      const symbols = (out?.widgets[0].config as { symbols?: string[] }).symbols;
      expect(symbols).toEqual(['NVDA', 'AAPL']);
    });

    it('forex currencies fall back to the full default list when all entries are bad', () => {
      const out = migrate({
        widgets: [
          {
            id: 'a',
            type: 'tv.forex-heatmap',
            config: { currencies: ['BAD1', 'BAD2', 42, null] },
          },
        ],
      });
      const currencies = (out?.widgets[0].config as { currencies?: string[] }).currencies ?? [];
      // Should be the rich default, not ['USD'].
      expect(currencies.length).toBeGreaterThanOrEqual(9);
      expect(currencies).toContain('USD');
      expect(currencies).toContain('CNY');
    });

    it('TV symbol regex accepts underscored exchange prefixes (futures, FX)', () => {
      // Regression: the original regex omitted `_` and silently rewrote
      // CME_MINI:ES1! and FX_IDC:EURUSD to the schema's catch default,
      // wiping out user-configured futures/FX symbols on every prefs load.
      const out = migrate({
        widgets: [
          {
            id: 'a',
            type: 'tv.single-ticker',
            config: { symbol: 'CME_MINI:ES1!' },
          },
          {
            id: 'b',
            type: 'tv.single-ticker',
            config: { symbol: 'FX_IDC:EURUSD' },
          },
        ],
      });
      expect((out?.widgets[0].config as { symbol?: string }).symbol).toBe('CME_MINI:ES1!');
      expect((out?.widgets[1].config as { symbol?: string }).symbol).toBe('FX_IDC:EURUSD');
    });

    it('every registered widget can round-trip its own defaultConfig (no drift)', () => {
      // REGRESSION-CRITICAL: catches schema-vs-defaultConfig drift. Every
      // widget's defaultConfig MUST satisfy its own schema; otherwise users
      // who add a widget via the gallery would have it instantly clobbered.
      const types = listWidgets().map((d) => d.type);
      expect(types.length).toBeGreaterThan(0);
      for (const type of types) {
        const def = getWidget(type)!;
        if (!def.configSchema) continue;
        const result = def.configSchema.safeParse(def.defaultConfig);
        expect(result.success, `defaultConfig for ${type} failed schema parse`).toBe(true);
      }
    });
  });
});
