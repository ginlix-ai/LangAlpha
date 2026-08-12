/**
 * Pure-function coverage for the tool classification + label helpers.
 * Focus is on the memo write/edit branch added for Fix #4 — confirming
 * `categorizeTool` distinguishes a memo `Write/Edit` from a memo `Read`,
 * and that the completed-row title surfaces the right verb so a future
 * regression letting the agent mutate memos doesn't render as "Read memo".
 */
import { describe, it, expect } from 'vitest';
import {
  categorizeTool,
  getCompletedRowTitle,
  getCompletedSummary,
  getInProgressText,
  getToolIcon,
} from '../toolDisplayConfig';

// Identity translator — surfaces the i18n key so we can assert which
// branch fired without depending on an actual locale bundle. Mirrors the
// pattern used in MemoPanel.test.tsx.
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

describe('categorizeTool — memo classification', () => {
  it('classifies a Write to a memo path as memoWrite', () => {
    expect(
      categorizeTool('Write', { args: { file_path: '.agents/user/memo/x.md' } })
    ).toBe('memoWrite');
  });

  it('classifies an Edit to a memo path (with sandbox-root prefix) as memoWrite', () => {
    expect(
      categorizeTool('Edit', { args: { file_path: '/home/workspace/.agents/user/memo/x.md' } })
    ).toBe('memoWrite');
  });

  it('keeps a Read on a memo path as memo (read bucket)', () => {
    expect(
      categorizeTool('Read', { args: { file_path: '.agents/user/memo/x.md' } })
    ).toBe('memo');
  });

  it('classifies user-profile reads + writes into profileRead / profileWrite buckets', () => {
    expect(
      categorizeTool('Read', { args: { file_path: '.agents/user/profile/portfolio.json' } })
    ).toBe('profileRead');
    expect(
      categorizeTool('Read', { args: { file_path: '.agents/user/profile/watchlist.json' } })
    ).toBe('profileRead');
    expect(
      categorizeTool('Write', { args: { file_path: '.agents/user/profile/preference.json' } })
    ).toBe('profileWrite');
    expect(
      categorizeTool('Edit', { args: { file_path: '.agents/user/profile/portfolio.json' } })
    ).toBe('profileWrite');
  });

  it('does not change file/memory categorization', () => {
    expect(
      categorizeTool('Read', { args: { file_path: 'work/scratch.md' } })
    ).toBe('fileRead');
    expect(
      categorizeTool('Write', { args: { file_path: '.agents/user/memory/risk.md' } })
    ).toBe('memoryWrite');
    expect(
      categorizeTool('Read', { args: { file_path: '.agents/user/memory/memory.md' } })
    ).toBe('memoryRead');
  });
});

describe('getCompletedRowTitle — memo write/edit verbs', () => {
  it('returns the wroteMemo i18n key for Write on a memo path', () => {
    const title = getCompletedRowTitle(
      'Write',
      { args: { file_path: '.agents/user/memo/x.md' } },
      tIdentity,
    );
    expect(title).toBe('toolArtifact.completed.wroteMemo');
  });

  it('returns the updatedMemo i18n key for Edit on a memo path', () => {
    const title = getCompletedRowTitle(
      'Edit',
      { args: { file_path: '.agents/user/memo/x.md' } },
      tIdentity,
    );
    expect(title).toBe('toolArtifact.completed.updatedMemo');
  });

  it('still returns the readMemo key for Read on a memo path', () => {
    const title = getCompletedRowTitle(
      'Read',
      { args: { file_path: '.agents/user/memo/x.md' } },
      tIdentity,
    );
    expect(title).toBe('toolArtifact.completed.readMemo');
  });
});

describe('getInProgressText — memo write/edit progress phrases', () => {
  it('emits writingMemoSlug for Write on a memo path', () => {
    const out = getInProgressText(
      'Write',
      { args: { file_path: '.agents/user/memo/notes.md' } },
      tIdentity,
    );
    expect(out).toBe('toolArtifact.inProgress.writingMemoSlug');
  });

  it('emits updatingMemoSlug for Edit on a memo path', () => {
    const out = getInProgressText(
      'Edit',
      { args: { file_path: '.agents/user/memo/notes.md' } },
      tIdentity,
    );
    expect(out).toBe('toolArtifact.inProgress.updatingMemoSlug');
  });
});

describe('getToolIcon — memo write/edit icon variant', () => {
  it('uses a different icon for memo writes vs memo reads', () => {
    const readIcon = getToolIcon('Read', { file_path: '.agents/user/memo/x.md' });
    const writeIcon = getToolIcon('Write', { file_path: '.agents/user/memo/x.md' });
    expect(readIcon).not.toBe(writeIcon);
  });

  it('uses the same icon for both Edit and Write on memo paths', () => {
    const editIcon = getToolIcon('Edit', { file_path: '.agents/user/memo/x.md' });
    const writeIcon = getToolIcon('Write', { file_path: '.agents/user/memo/x.md' });
    expect(editIcon).toBe(writeIcon);
  });
});

describe('chart annotation — symbol + interval headline', () => {
  it('summarizes a draw as "SYMBOL · <interval label>"', () => {
    const summary = getCompletedSummary(
      'draw_chart_annotation',
      { args: { symbol: 'nvda', timeframe: '1hour', annotation: { type: 'trendline' } } },
    );
    expect(summary).toBe('NVDA · 1H');
  });

  it('defaults a missing timeframe to 1D (the server-side default)', () => {
    const summary = getCompletedSummary('draw_chart_annotation', { args: { symbol: 'AAPL' } });
    expect(summary).toBe('AAPL · 1D');
  });

  it('summarizes manage_chart_annotations the same way', () => {
    const summary = getCompletedSummary(
      'manage_chart_annotations',
      { args: { symbol: 'TSLA', timeframe: '1day', action: 'clear' } },
    );
    expect(summary).toBe('TSLA · 1D');
  });

  it('falls back to the raw timeframe for an unmapped interval', () => {
    const summary = getCompletedSummary('draw_chart_annotation', { args: { symbol: 'MSFT', timeframe: '1week' } });
    expect(summary).toBe('MSFT · 1week');
  });

  it('returns null when the draw has no symbol (falls through to generic summary)', () => {
    // No symbol → no chart instance to name; must not emit "undefined · 1D".
    expect(getCompletedSummary('draw_chart_annotation', { args: {} })).toBeNull();
    expect(getCompletedSummary('manage_chart_annotations', { args: { timeframe: '1hour' } })).toBeNull();
  });

  it('labels the row "Annotate Chart" / "Manage Annotations"', () => {
    expect(getCompletedRowTitle('draw_chart_annotation', { args: { symbol: 'NVDA' } }, tIdentity)).toBe(
      'toolArtifact.tool.annotateChart',
    );
    expect(getCompletedRowTitle('manage_chart_annotations', { args: { symbol: 'NVDA' } }, tIdentity)).toBe(
      'toolArtifact.tool.manageAnnotations',
    );
  });
});

describe('user-profile — entity-aware labels for portfolio/watchlist/preference', () => {
  const ENTITIES = ['portfolio', 'watchlist', 'preference'] as const;

  for (const entity of ENTITIES) {
    const filePath = `.agents/user/profile/${entity}.json`;

    it(`returns "read_${entity}" completed-row title for Read on ${entity}.json`, () => {
      const title = getCompletedRowTitle('Read', { args: { file_path: filePath } }, tIdentity);
      expect(title).toBe(`toolArtifact.completed.read_${entity}`);
    });

    it(`returns "updated_${entity}" completed-row title for Write on ${entity}.json`, () => {
      const title = getCompletedRowTitle('Write', { args: { file_path: filePath } }, tIdentity);
      expect(title).toBe(`toolArtifact.completed.updated_${entity}`);
    });

    it(`returns "updated_${entity}" completed-row title for Edit on ${entity}.json`, () => {
      const title = getCompletedRowTitle('Edit', { args: { file_path: filePath } }, tIdentity);
      expect(title).toBe(`toolArtifact.completed.updated_${entity}`);
    });

    it(`emits "reading_${entity}" in-progress phrase for Read on ${entity}.json`, () => {
      const out = getInProgressText('Read', { args: { file_path: filePath } }, tIdentity);
      expect(out).toBe(`toolArtifact.inProgress.reading_${entity}`);
    });

    it(`emits "updating_${entity}" in-progress phrase for Write on ${entity}.json`, () => {
      const out = getInProgressText('Write', { args: { file_path: filePath } }, tIdentity);
      expect(out).toBe(`toolArtifact.inProgress.updating_${entity}`);
    });
  }
});
