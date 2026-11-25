/**
 * A place keeps one draft. A draft in hand goes along to a file that keeps
 * none, and stays kept where it was written when moving to one that keeps
 * its own, which comes back instead: neither is written over.
 */
import { act, renderHook } from '@testing-library/react';
import { useReducer } from 'react';
import { afterEach, describe, expect, it } from 'vitest';
import { runAuthResets } from '@/lib/authResets';
import { boxReducer, type BoxAction } from '../boxReducer';
import { keepDraft, readDraft } from '../commentDrafts';
import { openingBox, useKeptDraft } from '../useKeptDraft';

const WS = 'ws-a';

function kept(path: string, text: string) {
  keepDraft(WS, 'file', {
    draft: { anchor: { kind: 'element', path: '#a' }, rect: { x: 1, y: 2, width: 3, height: 4 }, path, title: null },
    text,
    forAgent: true,
    to: null,
  });
}

function setup(start: string) {
  const { result, rerender } = renderHook(({ path }: { path: string }) => {
    const shows = { workspaceId: WS, kind: 'file' as const, path };
    const [box, dispatchBox] = useReducer(boxReducer, { doc: 'd', listOpen: false }, (s) => openingBox(s, shows));
    const draft = (box.open?.kind === 'draft' ? box.open.draft : null) ?? box.listed?.draft ?? null;
    useKeptDraft({ ...shows, box, draft, dispatchBox, askedHere: false });
    return { box, draft, dispatchBox };
  }, { initialProps: { path: start } });
  const act_ = (action: BoxAction) => act(() => result.current.dispatchBox(action));
  return { result, moveTo: (path: string) => rerender({ path }), act: act_ };
}

afterEach(() => {
  localStorage.clear();
  runAuthResets();
});

describe('a draft in hand moving to another file', () => {
  it('stays kept where it was written where that file keeps its own, which comes back', () => {
    kept('a.html', 'words on a');
    kept('b.html', 'words on b');
    const { result, moveTo } = setup('a.html');
    expect(result.current.draft?.path).toBe('a.html');

    moveTo('b.html');
    expect(result.current.draft?.path).toBe('b.html');
    expect(result.current.box.text).toBe('words on b');
    expect(readDraft(WS, 'file', 'a.html')?.text).toBe('words on a');

    moveTo('a.html');
    expect(result.current.box.text).toBe('words on a');
    expect(readDraft(WS, 'file', 'b.html')?.text).toBe('words on b');
  });

  it('goes along where the file keeps none, still kept where it was written', () => {
    kept('a.html', 'words on a');
    const { result, moveTo } = setup('a.html');

    moveTo('c.html');
    expect(result.current.draft?.path).toBe('a.html');
    expect(result.current.box.text).toBe('words on a');
    expect(readDraft(WS, 'file', 'a.html')?.text).toBe('words on a');
  });

  it('is let go once discarded after going along', () => {
    kept('a.html', 'words on a');
    const { result, moveTo, act: dispatch } = setup('a.html');
    moveTo('c.html');

    dispatch({ type: 'endDraft', keepHover: false });
    expect(result.current.draft).toBeNull();
    expect(readDraft(WS, 'file', 'a.html')).toBeNull();
  });
});

describe('a save coming back', () => {
  it('leaves words another tab wrote at the place while it was out', () => {
    kept('a.html', 'words on a');
    const { result, act: dispatch } = setup('a.html');
    const serial = result.current.box.open?.kind === 'draft' ? result.current.box.open.serial : -1;
    const [key] = Object.keys(localStorage).filter((k) => k.includes('commentDraft:'));
    localStorage.setItem(key, JSON.stringify({ ...JSON.parse(localStorage.getItem(key)!), text: 'newer, from another tab' }));

    dispatch({ type: 'saved', id: 'c-1', serial });
    expect(result.current.draft).toBeNull();
    expect(readDraft(WS, 'file', 'a.html')?.text).toBe('newer, from another tab');
  });
});
