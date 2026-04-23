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
});
