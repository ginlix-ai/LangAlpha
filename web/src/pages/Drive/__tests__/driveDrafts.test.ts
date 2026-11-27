// @vitest-environment node
import { beforeEach, describe, expect, it } from 'vitest';
import { runAuthResets } from '@/lib/authResets';
import { DRAFT_MAX_CHARS, DRAFTS_MAX, draftOf, keepDraft } from '../driveDrafts';

const base = { sha: 'a'.repeat(64), content: 'base' };

beforeEach(() => runAuthResets());

describe('driveDrafts: bounds', () => {
  it('does not keep a draft past the size bound, and forgets the one it held', () => {
    keepDraft('e1', { base, draft: 'small', note: '' });
    keepDraft('e1', { base, draft: 'x'.repeat(DRAFT_MAX_CHARS), note: '' });
    expect(draftOf('e1')).toBeUndefined();
  });

  it('keeps the newest drafts, dropping the one written longest ago', () => {
    for (let i = 0; i <= DRAFTS_MAX; i += 1) keepDraft(`e${i}`, { base, draft: `d${i}`, note: '' });
    expect(draftOf('e0')).toBeUndefined();
    expect(draftOf('e1')?.draft).toBe('d1');
    expect(draftOf(`e${DRAFTS_MAX}`)?.draft).toBe(`d${DRAFTS_MAX}`);
  });
});
