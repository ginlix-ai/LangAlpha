import { Calculator } from 'lucide-react';
import { describe, expect, it } from 'vitest';
import type { PluginInfo } from '@/pages/ChatAgent/utils/api/plugins';
import { skillMark } from '../pluginSurface';

/**
 * The order a skill's tile falls back through: its own icon, then its
 * package's mark, then the book every skill shares.
 */

function plugin(over: Partial<PluginInfo>): PluginInfo {
  return {
    name: 'pack',
    version: null,
    description: '',
    author: null,
    homepage: null,
    source_type: 'bundled',
    source_ref: null,
    enabled: true,
    installed_at: null,
    updated_at: null,
    components: [],
    ...over,
  };
}

const srcs = (m: ReturnType<typeof skillMark>) => m.art.map((a) => a.src.replace(/^.*(?=\/api\/)/, ''));

describe('skillMark', () => {
  it('draws a glyph the web ships, and nothing that could fail to load', () => {
    const m = skillMark({ icon_glyph: 'calculator' }, plugin({ icon_url: '/api/v1/plugins/pack/icon' }));
    expect(m.glyph).toBe(Calculator);
    expect(m.art).toEqual([]);
    expect(m.kind).toBe('skill');
  });

  it("puts a vendor mark ahead of the package's", () => {
    const m = skillMark(
      { icon_url: '/api/v1/plugins/pack/skills/x-api/icon' },
      plugin({ icon_url: '/api/v1/plugins/pack/icon' }),
    );
    expect(m.glyph).toBeUndefined();
    expect(srcs(m)).toEqual(['/api/v1/plugins/pack/skills/x-api/icon', '/api/v1/plugins/pack/icon']);
    // A wrapper's art can fail too, and its skills are still skills.
    expect(m.kind).toBe('skill');
  });

  it('falls to our own mark inside one of our bundles', () => {
    const m = skillMark({ icon_glyph: 'no-such-glyph' }, plugin({}));
    expect(m.glyph).toBeUndefined();
    expect(m.art).toEqual([]);
    expect(m.kind).toBe('langalpha');
  });

  it("an installed package's mark is its kind, so the skill keeps the book", () => {
    const m = skillMark({}, plugin({ source_type: 'zip' }));
    expect(m.kind).toBe('skill');
  });

  it('a skill no package owns is the book', () => {
    expect(skillMark({ icon_glyph: 'constructor' })).toEqual({ art: [], kind: 'skill' });
  });
});
