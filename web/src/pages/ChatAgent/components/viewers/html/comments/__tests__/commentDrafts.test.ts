/**
 * A comment being written is kept per workspace and per file or chart, so a
 * draft never comes back on another workspace's file of the same path, and
 * what storage holds is trusted only as far as a page's own report would be.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { runAuthResets } from '@/lib/authResets';
import { storageUser } from '@/lib/userStorage';
import { dropDraft, dropSentDraft, keepDraft, readDraft, type KeptDraft } from '../commentDrafts';

const draft = (path: string, text = 'Check this'): KeptDraft => ({
  draft: { anchor: { kind: 'element', path: '#total', label: 'Total' }, rect: { x: 1, y: 2, width: 30, height: 10 }, path, title: 'Report' },
  text,
  forAgent: false,
  to: { threadId: 'th-1' },
});

afterEach(() => {
  localStorage.clear();
  runAuthResets();
});

describe('kept comment drafts', () => {
  it('come back as they were written, for their workspace and file alone', () => {
    keepDraft('ws-a', 'file', draft('reports/q3.html'));
    expect(readDraft('ws-a', 'file', 'reports/q3.html')).toEqual(draft('reports/q3.html'));
    expect(readDraft('ws-b', 'file', 'reports/q3.html')).toBeNull();
    expect(readDraft('ws-a', 'file', 'reports/q4.html')).toBeNull();
    expect(readDraft('ws-a', 'chart', 'reports/q3.html')).toBeNull();
  });

  it('keep one per file, the latest words', () => {
    keepDraft('ws-a', 'file', draft('q3.html', 'first'));
    keepDraft('ws-a', 'file', draft('q3.html', 'second'));
    expect(readDraft('ws-a', 'file', 'q3.html')?.text).toBe('second');
  });

  it('go once dropped', () => {
    const entry = keepDraft('ws-a', 'file', draft('q3.html'))!;
    dropDraft('ws-a', 'file', 'q3.html', entry);
    expect(readDraft('ws-a', 'file', 'q3.html')).toBeNull();
  });

  it('stay when another tab wrote there since this one kept it', () => {
    const entry = keepDraft('ws-a', 'file', draft('q3.html', 'mine'))!;
    const [key] = Object.keys(localStorage).filter((k) => k.includes('commentDraft:'));
    localStorage.setItem(key, JSON.stringify({ ...JSON.parse(entry), text: 'theirs, newer' }));
    dropDraft('ws-a', 'file', 'q3.html', entry);
    expect(readDraft('ws-a', 'file', 'q3.html')?.text).toBe('theirs, newer');
  });

  it('are not trusted past what a page could report', () => {
    keepDraft('ws-a', 'file', draft('q3.html'));
    runAuthResets(); // read as a new tab would, from storage alone
    const [key] = Object.keys(localStorage).filter((k) => k.includes('commentDraft:'));
    const stored = JSON.parse(localStorage.getItem(key)!);
    localStorage.setItem(key, JSON.stringify({ ...stored, spot: { anchor: { kind: 'element' }, rect: stored.spot.rect } }));
    expect(readDraft('ws-a', 'file', 'q3.html')).toBeNull();
    localStorage.setItem(key, '{not json');
    expect(readDraft('ws-a', 'file', 'q3.html')).toBeNull();
  });

  it('come back with the default send when only how they were to be sent is unreadable', () => {
    keepDraft('ws-a', 'file', draft('q3.html'));
    runAuthResets(); // read as a new tab would, from storage alone
    const [key] = Object.keys(localStorage).filter((k) => k.includes('commentDraft:'));
    const stored = JSON.parse(localStorage.getItem(key)!);
    localStorage.setItem(key, JSON.stringify({ ...stored, to: {}, forAgent: 'yes', title: 7 }));
    expect(readDraft('ws-a', 'file', 'q3.html')).toEqual({
      ...draft('q3.html'),
      draft: { ...draft('q3.html').draft, title: null },
      forAgent: true,
      to: null,
    });
  });
});

describe('a store that refuses writes', () => {
  it('still keeps the draft for this tab, and lets it go when dropped', () => {
    const refuse = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new DOMException('full', 'QuotaExceededError');
    });
    try {
      const entry = keepDraft('ws-a', 'file', draft('q3.html', 'Kept anyway'))!;
      expect(Object.keys(localStorage).some((k) => k.includes('commentDraft:'))).toBe(false);
      expect(readDraft('ws-a', 'file', 'q3.html')?.text).toBe('Kept anyway');
      dropDraft('ws-a', 'file', 'q3.html', entry);
      expect(readDraft('ws-a', 'file', 'q3.html')).toBeNull();
    } finally {
      refuse.mockRestore();
    }
  });

  it('is read past an older copy the refused write did not replace', () => {
    keepDraft('ws-a', 'file', draft('q3.html', 'first'));
    const refuse = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new DOMException('full', 'QuotaExceededError');
    });
    try {
      keepDraft('ws-a', 'file', draft('q3.html', 'second'));
      expect(readDraft('ws-a', 'file', 'q3.html')?.text).toBe('second');
    } finally {
      refuse.mockRestore();
    }
  });
});

describe('a sent draft', () => {
  const sent = (text: string, user = storageUser(), anchor = draft('q3.html').draft.anchor) => ({
    user, workspaceId: 'ws-a', kind: 'file' as const, path: 'q3.html', anchor, text,
  });
  /** Moves what the signed-in account keeps for q3.html under `user`, as if they had signed out. */
  const keptBy = (user: string) => {
    const [key] = Object.keys(localStorage).filter((k) => k.includes('commentDraft:'));
    const own = key.slice(key.indexOf('commentDraft:'));
    localStorage.setItem(`user:${user}:${own}`, localStorage.getItem(key)!);
    localStorage.removeItem(key);
    return `user:${user}:${own}`;
  };

  it('goes from where it was kept once its send comes back', () => {
    keepDraft('ws-a', 'file', draft('q3.html', 'Check this'));
    dropSentDraft(sent('Check this'));
    expect(readDraft('ws-a', 'file', 'q3.html')).toBeNull();
  });

  it('leaves words written there since, which are a new draft', () => {
    keepDraft('ws-a', 'file', draft('q3.html', 'Check this, and the margin'));
    dropSentDraft(sent('Check this'));
    expect(readDraft('ws-a', 'file', 'q3.html')?.text).toBe('Check this, and the margin');
  });

  it('leaves the draft of an account signed in since', () => {
    keepDraft('ws-a', 'file', draft('q3.html', 'Check this'));
    dropSentDraft(sent('Check this', 'someone-who-signed-out'));
    expect(readDraft('ws-a', 'file', 'q3.html')?.text).toBe('Check this');
  });

  it('leaves newer words another tab kept while the send was out', () => {
    keepDraft('ws-a', 'file', draft('q3.html', 'Check this'));
    const [key] = Object.keys(localStorage).filter((k) => k.includes('commentDraft:'));
    const theirs = JSON.parse(localStorage.getItem(key)!);
    localStorage.setItem(key, JSON.stringify({ ...theirs, text: 'Check this, and the margin' }));
    dropSentDraft(sent('Check this'));
    expect(readDraft('ws-a', 'file', 'q3.html')?.text).toBe('Check this, and the margin');
  });

  it('leaves the same words kept on another spot', () => {
    keepDraft('ws-a', 'file', draft('q3.html', 'Check this'));
    dropSentDraft(sent('Check this', storageUser(), { kind: 'element', path: '#margin', label: 'Margin' }));
    expect(readDraft('ws-a', 'file', 'q3.html')?.text).toBe('Check this');
  });

  it('goes from the sender\'s own storage once they have signed out, while it is still the sent draft', () => {
    keepDraft('ws-a', 'file', draft('q3.html', 'Check this'));
    const theirs = keptBy('signed-out');
    dropSentDraft(sent('Check this', 'signed-out'));
    expect(localStorage.getItem(theirs)).toBeNull();
  });

  it('leaves the sender\'s draft changed since, after they have signed out', () => {
    keepDraft('ws-a', 'file', draft('q3.html', 'Check this, and the margin'));
    const theirs = keptBy('signed-out');
    dropSentDraft(sent('Check this', 'signed-out'));
    expect(localStorage.getItem(theirs)).not.toBeNull();
  });
});
