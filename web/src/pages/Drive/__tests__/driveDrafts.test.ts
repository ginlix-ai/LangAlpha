import { beforeEach, describe, expect, it } from 'vitest';
import { runAuthResets } from '@/lib/authResets';
import { DRAFT_MAX_CHARS, draftOf, dropDraft, keepDraft } from '../driveDrafts';

const base = { sha: 'a'.repeat(64), content: 'base' };

beforeEach(() => runAuthResets());

describe('driveDrafts: bounds', () => {
  it('does not keep a draft past the size bound, and forgets the one it held', () => {
    keepDraft('e1', { base, draft: 'small', note: '', sitting: 's' });
    keepDraft('e1', { base, draft: 'x'.repeat(DRAFT_MAX_CHARS), note: '', sitting: 's' });
    expect(draftOf('e1')).toBeUndefined();
  });

  it('keeps every draft, however many files have one', () => {
    for (let i = 0; i <= 40; i += 1) keepDraft(`e${i}`, { base, draft: `d${i}`, note: '', sitting: 's' });
    expect(draftOf('e0')?.draft).toBe('d0');
    expect(draftOf('e40')?.draft).toBe('d40');
  });
});

describe('driveDrafts: leaving the window', () => {
  const unload = () => {
    const event = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(event);
    return event.defaultPrevented;
  };

  it('asks before the window goes while a draft is kept, whatever editor wrote it', () => {
    expect(unload()).toBe(false);
    keepDraft('e1', { base, draft: 'unsaved', note: '', sitting: 's' });
    expect(unload()).toBe(true);
    dropDraft('e1');
    expect(unload()).toBe(false);
  });

  it('stops asking once the account changes, since its drafts go with it', () => {
    keepDraft('e1', { base, draft: 'unsaved', note: '', sitting: 's' });
    runAuthResets();
    expect(unload()).toBe(false);
  });
});
