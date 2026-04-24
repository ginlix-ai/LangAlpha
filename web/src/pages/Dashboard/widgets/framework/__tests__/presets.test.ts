import { describe, it, expect } from 'vitest';
import { PRESETS_META, getPreset, type PresetId } from '../../presets';
import { getWidget } from '../WidgetRegistry';
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
});
