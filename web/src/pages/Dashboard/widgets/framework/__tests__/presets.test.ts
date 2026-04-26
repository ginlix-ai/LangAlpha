import { describe, it, expect } from 'vitest';
import i18n from '@/i18n';
import { PRESETS_META, getPreset, type PresetId } from '../../presets';
import { getWidget, listWidgets } from '../WidgetRegistry';
import '../../index'; // side-effect: register all widgets

// Presets drift silently if a layout item's `i` doesn't map to a widget, or if
// a widget type is not registered. Either mistake renders a ghost cell at
// runtime. These invariants keep us honest at preset-authoring time.
describe('presets', () => {
  PRESETS_META.forEach((meta) => {
    describe(`${meta.id}`, () => {
      const preset = getPreset(meta.id as PresetId);

      it('every layout item maps to a widget id (and vice versa)', () => {
        const widgetIds = new Set(preset.widgets.map((w) => w.id));
        const lgIds = new Set((preset.layouts.lg ?? []).map((l) => l.i));
        expect(lgIds).toEqual(widgetIds);
      });

      it('every widget type resolves to a registered definition', () => {
        for (const w of preset.widgets) {
          expect(getWidget(w.type)).toBeDefined();
        }
      });

      it('widget ids are unique within the preset', () => {
        const ids = preset.widgets.map((w) => w.id);
        expect(new Set(ids).size).toBe(ids.length);
      });
    });
  });

  it('every PRESETS_META id resolves via getPreset', () => {
    for (const m of PRESETS_META) {
      expect(() => getPreset(m.id)).not.toThrow();
    }
  });

  it('md layout mirrors lg layout (same ids)', () => {
    for (const meta of PRESETS_META) {
      const p = getPreset(meta.id as PresetId);
      const lgIds = new Set((p.layouts.lg ?? []).map((l) => l.i));
      const mdIds = new Set((p.layouts.md ?? []).map((l) => l.i));
      expect(mdIds).toEqual(lgIds);
    }
  });

  // REGRESSION-CRITICAL: the existing `trader` preset must stay exactly as
  // it was before `trader-tv` landed. Any accidental mutation (e.g. copying
  // from the wrong factory when adding the new preset) would silently change
  // the layout for users who already have it applied.
  it('existing `trader` preset layout is the NVDA chart grid (regression)', () => {
    const p = getPreset('trader');
    const types = p.widgets.map((w) => w.type).sort();
    expect(types).toEqual(
      [
        'chart.symbol',
        'chart.symbol',
        'chart.symbol',
        'chart.symbol',
        'markets.overview',
        'news.feed',
        'personal.portfolioWatchlist',
      ].sort(),
    );
    // Four NVDA / SPY charts are present in the classic trader preset
    const chartSymbols = p.widgets
      .filter((w) => w.type === 'chart.symbol')
      .map((w) => (w.config as { symbol?: string }).symbol);
    expect(chartSymbols.filter((s) => s === 'NVDA').length).toBe(3);
    expect(chartSymbols).toContain('SPY');
  });

  it('new `trader-tv` preset includes the expected TV widget types', () => {
    const p = getPreset('trader-tv');
    const types = p.widgets.map((w) => w.type);
    expect(types).toContain('tv.ticker-tape');
    expect(types).toContain('tv.stock-heatmap');
    expect(types).toContain('tv.symbol-spotlight');
    expect(types).toContain('tv.movers');
    expect(types).toContain('tv.economic-events');
    expect(types).toContain('tv.technicals');
    expect(types).toContain('markets.miniChartGrid');
  });

  // REGRESSION-CRITICAL: every preset's widget configs must round-trip cleanly
  // through the widget's Zod schema. If a preset author seeds a config that
  // doesn't satisfy the schema, sanitizeConfig will silently rewrite it to
  // defaultConfig on first load — the user gets a different layout than the
  // one we shipped. Catch the drift here at preset-authoring time.
  it('every preset widget config validates against its schema', () => {
    for (const meta of PRESETS_META) {
      const p = getPreset(meta.id as PresetId);
      for (const w of p.widgets) {
        const def = getWidget(w.type);
        if (!def?.configSchema) continue;
        const result = def.configSchema.safeParse(w.config);
        expect(
          result.success,
          `preset "${meta.id}" widget "${w.id}" (${w.type}) failed schema validation: ${
            result.success ? '' : JSON.stringify(result.error)
          }`,
        ).toBe(true);
      }
    }
  });

  // REGRESSION-CRITICAL: every preset must declare i18n keys for all
  // user-visible chrome (name/tag/description/bestFor + per-pill keys), and
  // every key must resolve to a non-empty string in BOTH locales. Rendering
  // is i18n-only after the literal cleanup — a missing key surfaces as the
  // raw key path on the dialog ('dashboard.widgets.presets.foo.name') and
  // the user sees the wrong copy.
  describe('preset i18n key coverage', () => {
    PRESETS_META.forEach((meta) => {
      it(`${meta.id}: declares all required i18n keys`, () => {
        expect(meta.nameKey, `missing nameKey on ${meta.id}`).toBeTruthy();
        expect(meta.tagKey, `missing tagKey on ${meta.id}`).toBeTruthy();
        expect(meta.descriptionKey, `missing descriptionKey on ${meta.id}`).toBeTruthy();
        expect(meta.bestForKey, `missing bestForKey on ${meta.id}`).toBeTruthy();
        expect(meta.pillKeys, `missing pillKeys on ${meta.id}`).toBeTruthy();
      });

      it(`${meta.id}: every key resolves to a non-empty string in en-US AND zh-CN`, () => {
        const keys = [meta.nameKey, meta.tagKey, meta.descriptionKey, meta.bestForKey, ...(meta.pillKeys ?? [])];
        for (const key of keys) {
          if (!key) continue;
          for (const lng of ['en-US', 'zh-CN']) {
            const resolved = i18n.t(key, { lng });
            expect(resolved.length, `key ${key} empty under ${lng}`).toBeGreaterThan(0);
            expect(resolved, `key ${key} unresolved under ${lng}`).not.toBe(key);
          }
        }
      });
    });
  });
});

// Same shape for widget definitions: every registered widget must have a
// titleKey that resolves in both locales. Without this, the gallery /
// widget-frame would render the raw key path. descriptionKey is optional
// (some widgets have no blurb) but if declared, must resolve.
describe('widget definition i18n key coverage', () => {
  for (const def of listWidgets()) {
    it(`${def.type}: titleKey resolves in en-US and zh-CN`, () => {
      expect(def.titleKey, `missing titleKey on ${def.type}`).toBeTruthy();
      for (const lng of ['en-US', 'zh-CN']) {
        const resolved = i18n.t(def.titleKey, { lng });
        expect(resolved.length, `${def.titleKey} empty under ${lng}`).toBeGreaterThan(0);
        expect(resolved, `${def.titleKey} unresolved under ${lng}`).not.toBe(def.titleKey);
      }
    });
    it(`${def.type}: descriptionKey (when set) resolves in en-US and zh-CN`, () => {
      if (!def.descriptionKey) return;
      for (const lng of ['en-US', 'zh-CN']) {
        const resolved = i18n.t(def.descriptionKey, { lng });
        expect(resolved.length, `${def.descriptionKey} empty under ${lng}`).toBeGreaterThan(0);
        expect(resolved, `${def.descriptionKey} unresolved under ${lng}`).not.toBe(def.descriptionKey);
      }
    });
  }
});
