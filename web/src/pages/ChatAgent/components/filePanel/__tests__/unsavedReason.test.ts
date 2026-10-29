import { describe, it, expect, beforeAll } from 'vitest';

import i18n from '@/i18n';

import { shownPath, unsavedReasonLabel } from '../unsavedReason';

const t = i18n.t.bind(i18n);

beforeAll(async () => {
  await i18n.changeLanguage('en-US');
});

describe('unsavedReasonLabel', () => {
  it('sizes a too-large file when the size is known', () => {
    expect(unsavedReasonLabel(t, { reason: 'too_large', size: 6_549_825_126 }, 'en-US')).toBe(
      'Too large to back up here (6.1 GB). Move or split it.',
    );
  });

  it('uses the plain reason when there is no size to show', () => {
    expect(unsavedReasonLabel(t, { reason: 'too_large', size: null }, 'en-US')).toBe(i18n.t('filePanel.unsavedReason.too_large'));
    expect(unsavedReasonLabel(t, { reason: 'changed' }, 'en-US')).toBe('Changed while saving. The next backup retries.');
  });
});

describe('path_too_long', () => {
  it('names the reason with a fix the user can make', () => {
    expect(unsavedReasonLabel(t, { reason: 'path_too_long', size: 3 }, 'en-US')).toBe(
      'Its path is too long to back up. Shorten its folder names.',
    );
  });

  it('keeps both ends of a path too long for a sentence', () => {
    const deep = `${'nest/'.repeat(900)}leaf.txt`;
    const shown = shownPath(deep);
    expect(shown.length).toBeLessThanOrEqual(120);
    expect(shown.startsWith('nest/')).toBe(true);
    expect(shown.endsWith('leaf.txt')).toBe(true);
    expect(shownPath('a/b.txt')).toBe('a/b.txt');
  });
});
