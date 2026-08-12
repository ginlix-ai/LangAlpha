/**
 * Coverage for the `get_market_overview` display wiring (review-fix pair):
 *
 * 1. `TOOL_DISPLAY_CONFIG['get_market_overview']` must use its own
 *    `marketOverview` i18n key — not the legacy `marketIndices` key it was
 *    accidentally aliased to, which rendered "Market Indices" for the
 *    consolidated tool.
 * 2. `INLINE_ARTIFACT_TOOLS` must contain `get_market_overview` now that a
 *    `market_overview` entry exists in the artifact→card map
 *    (`INLINE_ARTIFACT_MAP` in ActivityBlock.tsx / MessageList.tsx), so
 *    completed calls render the composite card instead of a plain tool row.
 *    It must still contain the two legacy tool names so historical thread
 *    replay keeps rendering their cards.
 */
import { describe, it, expect } from 'vitest';
import { TOOL_DISPLAY_CONFIG, getDisplayName, getInProgressText } from '../toolDisplayConfig';
import { INLINE_ARTIFACT_TOOLS } from '../charts/InlineArtifactCards';

const tIdentity = (key: string, opts?: Record<string, unknown>) => {
  if (opts && typeof opts === 'object') {
    let out = key;
    for (const [k, v] of Object.entries(opts)) {
      out = out.replace(new RegExp(`{{\\s*${k}\\s*}}`, 'g'), String(v));
    }
    return out;
  }
  return key;
};

describe('get_market_overview — display config', () => {
  it('uses its own marketOverview i18n key, not the legacy marketIndices key', () => {
    expect(TOOL_DISPLAY_CONFIG.get_market_overview.i18nKey).toBe('marketOverview');
  });

  it('resolves the dedicated translation key via getDisplayName', () => {
    expect(getDisplayName('get_market_overview', tIdentity)).toBe('toolArtifact.tool.marketOverview');
  });

  it('resolves the dedicated in-progress translation key', () => {
    expect(getInProgressText('get_market_overview', undefined, tIdentity)).toBe(
      'toolArtifact.inProgress.fetchingMarketOverview',
    );
  });

  it('legacy get_market_indices keeps its original marketIndices key', () => {
    expect(TOOL_DISPLAY_CONFIG.get_market_indices.i18nKey).toBe('marketIndices');
  });
});

describe('INLINE_ARTIFACT_TOOLS — get_market_overview routing', () => {
  it('routes get_market_overview to the compact-artifact block', () => {
    expect(INLINE_ARTIFACT_TOOLS.has('get_market_overview')).toBe(true);
  });

  it('still routes the legacy pre-consolidation tool names for SSE replay', () => {
    expect(INLINE_ARTIFACT_TOOLS.has('get_market_indices')).toBe(true);
    expect(INLINE_ARTIFACT_TOOLS.has('get_sector_performance')).toBe(true);
  });
});
