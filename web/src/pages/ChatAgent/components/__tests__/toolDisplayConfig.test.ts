/**
 * Pure-function coverage for the tool classification + label helpers.
 * Focus is on the memo write/edit branch added for Fix #4 — confirming
 * `categorizeTool` distinguishes a memo `Write/Edit` from a memo `Read`,
 * and that the completed-row title surfaces the right verb so a future
 * regression letting the agent mutate memos doesn't render as "Read memo".
 */
import { describe, it, expect } from 'vitest';
import { Clock, Contact, Send, User, Wrench } from 'lucide-react';
import {
  categorizeTool,
  getCompletedRowTitle,
  getCompletedSummary,
  getDisplayName,
  getInProgressText,
  getPreparingText,
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

  it('classifies automations reads + writes into automationsRead / automationsWrite buckets', () => {
    const file_path = '.agents/user/automations/morning-brief.json';
    expect(categorizeTool('Read', { args: { file_path } })).toBe('automationsRead');
    expect(categorizeTool('Write', { args: { file_path } })).toBe('automationsWrite');
    expect(categorizeTool('Edit', { args: { file_path } })).toBe('automationsWrite');
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

describe('user-data — entity-aware labels for portfolio/watchlist/preference', () => {
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

    it(`uses the User icon for ${entity}.json`, () => {
      expect(getToolIcon('Read', { file_path: filePath })).toBe(User);
      expect(getToolIcon('Write', { file_path: filePath })).toBe(User);
    });
  }
});

describe('automations: entity-aware labels for an automation file', () => {
  const call = { args: { file_path: '.agents/user/automations/morning-brief.json' } };

  it('titles a completed Read "read_automations" and a Write or Edit "updated_automations"', () => {
    expect(getCompletedRowTitle('Read', call, tIdentity)).toBe('toolArtifact.completed.read_automations');
    expect(getCompletedRowTitle('Write', call, tIdentity)).toBe('toolArtifact.completed.updated_automations');
    expect(getCompletedRowTitle('Edit', call, tIdentity)).toBe('toolArtifact.completed.updated_automations');
  });

  it('emits "reading_automations" / "updating_automations" in-progress phrases', () => {
    expect(getInProgressText('Read', call, tIdentity)).toBe('toolArtifact.inProgress.reading_automations');
    expect(getInProgressText('Edit', call, tIdentity)).toBe('toolArtifact.inProgress.updating_automations');
  });

  it('names the automation file in the summary pill, since the title does not', () => {
    expect(getCompletedSummary('Read', call)).toBe('morning-brief');
    expect(getCompletedSummary('Edit', { args: { file_path: '/home/workspace/.agents/user/automations/aapl.json' } })).toBe('aapl');
  });

  it('carries no pill for a profile file, whose title names it', () => {
    expect(getCompletedSummary('Read', { args: { file_path: '.agents/user/profile/portfolio.json' } })).toBeNull();
  });

  it('uses the Clock icon check_automations uses', () => {
    expect(getToolIcon('Read', call.args)).toBe(Clock);
    expect(getToolIcon('Edit', call.args)).toBe(Clock);
  });

  it('falls back to English without a translator', () => {
    expect(getCompletedRowTitle('Read', call)).toBe('Read automations');
    expect(getInProgressText('Write', call)).toBe('updating automations...');
  });
});

describe('messaging tools: a named step, not a bare wrench', () => {
  const send = (args: Record<string, unknown>) => ({ args: { text: 'hi', ...args } });

  it('draws each tool with its own icon', () => {
    expect(getToolIcon('send_message')).toBe(Send);
    expect(getToolIcon('list_message_targets')).toBe(Contact);
    expect(getToolIcon('send_message')).not.toBe(Wrench);
    expect(getToolIcon('list_message_targets')).not.toBe(Wrench);
  });

  it('names both tools, translated and not', () => {
    expect(getDisplayName('send_message', tIdentity)).toBe('toolArtifact.tool.sendMessage');
    expect(getDisplayName('list_message_targets', tIdentity)).toBe('toolArtifact.tool.messageTargets');
    expect(getDisplayName('send_message')).toBe('Send Message');
    expect(getCompletedRowTitle('list_message_targets', { args: {} })).toBe('Message Targets');
  });

  it('summarizes a send by the app its target names', () => {
    expect(getCompletedSummary('send_message', send({ target: 'telegram:-100123' }))).toBe('Telegram');
    expect(getCompletedSummary('send_message', send({ target: 'slack:T1/C2' }))).toBe('Slack');
    expect(getCompletedSummary('send_message', send({ target: 'imessage' }))).toBe('iMessage');
    expect(getCompletedSummary('send_message', send({ target: 'Discord:G/C' }))).toBe('Discord');
    expect(getCompletedSummary('send_message', send({ target: 'matrix:!room' }))).toBe('Matrix');
  });

  it('carries no summary for a send to the turn\'s own conversation', () => {
    expect(getCompletedSummary('send_message', send({}))).toBeNull();
    expect(getCompletedSummary('send_message', send({ target: '' }))).toBeNull();
  });

  it('says where a send is going while it runs', () => {
    expect(getInProgressText('send_message', send({ target: 'discord:G/C' }))).toBe('sending to Discord...');
    expect(getInProgressText('send_message', send({}))).toBe('sending message...');
    expect(getInProgressText('send_message', send({ target: 'slack:T/C' }), tIdentity)).toBe('toolArtifact.inProgress.sendingTo');
    expect(getInProgressText('list_message_targets', { args: {} })).toBe('listing message targets...');
    expect(getInProgressText('list_message_targets', { args: {} }, tIdentity)).toBe('toolArtifact.inProgress.listingMessageTargets');
  });

  it('says what is being prepared before the call is written', () => {
    expect(getPreparingText('send_message', 10)).toBe('composing message...');
    expect(getPreparingText('send_message', 2400)).toBe('composing message (~2.4 KB)...');
    expect(getPreparingText('list_message_targets', 0)).toBe('preparing request...');
  });
});
