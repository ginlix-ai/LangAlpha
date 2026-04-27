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
