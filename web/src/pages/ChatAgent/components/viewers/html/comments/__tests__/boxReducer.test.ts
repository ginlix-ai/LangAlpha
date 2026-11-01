// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { ArtifactComment } from '@/types/api';
import type { PageHover, PagePick } from '../../pageProtocol';
import { boxReducer, initialBox, pressCloses, type BoxAction, type BoxState } from '../boxReducer';

const RECT = { x: 10, y: 20, width: 100, height: 30 };
const HOVER: PageHover = { rect: RECT, label: 'Revenue', word: false };

function pick(quote = 'Q3'): PagePick {
  return { anchor: { kind: 'text', quote }, rect: RECT, title: 'Report' };
}

function comment(id: string, overrides: Partial<ArtifactComment> = {}): ArtifactComment {
  return {
    comment_id: id,
    artifact_id: 'a-1',
    number: 1,
    path: 'report.html',
    file_sha256: null,
    title: null,
    anchor: { kind: 'element', path: '#a' },
    status: 'open',
    delivery: 'note',
    thread_id: null,
    thread_title: null,
    thread_archived: false,
    created_at: '2026-10-30T00:00:00Z',
    updated_at: '2026-10-30T00:00:00Z',
    resolved_at: null,
    messages: [],
    read_through: 0,
    unread: 0,
    ...overrides,
  };
}

function run(...actions: BoxAction[]): BoxState {
  return actions.reduce(boxReducer, initialBox({ doc: '1:a', listOpen: false }));
}

const picked: BoxAction = { type: 'pick', pick: pick(), path: 'report.html', selected: false };

describe('a pick', () => {
  it('opens a numbered draft box on the spot, for the agent, to the default', () => {
    const state = run(picked);
    expect(state.open).toEqual({
      kind: 'draft',
      serial: 1,
      draft: { anchor: { kind: 'text', quote: 'Q3' }, rect: RECT, path: 'report.html', title: 'Report' },
    });
    expect(state).toMatchObject({ forAgent: true, to: null, text: '' });
  });

  it('carries words already written to the new spot, with where they go', () => {
    const state = run(
      picked,
      { type: 'write', text: 'check this' },
      { type: 'forAgent', on: false },
      { type: 'to', target: { threadId: 't-2' } },
      { type: 'pick', pick: pick('Q4'), path: 'report.html', selected: true },
    );
    expect(state.open).toMatchObject({ kind: 'draft', serial: 2, draft: { selected: true } });
    expect(state).toMatchObject({ text: 'check this', forAgent: false, to: { threadId: 't-2' } });
  });

  it('starts a comment with no words afresh', () => {
    const state = run(picked, { type: 'forAgent', on: false }, picked);
    expect(state.forAgent).toBe(true);
  });

  it('takes the place of a draft waiting in the list', () => {
    const state = run(picked, { type: 'listDraft' }, picked);
    expect(state.listed).toBeNull();
    expect(state.open).toMatchObject({ kind: 'draft', serial: 2 });
  });
});

describe('a press on the page', () => {
  it('lets an empty draft go and keeps the outline under the pointer', () => {
    const before = run({ type: 'hover', hover: HOVER }, picked);
    expect(pressCloses(before)).toBe(true);
    const state = boxReducer(before, { type: 'pressAway' });
    expect(state.open).toBeNull();
    expect(state.hover).toBe(HOVER);
  });

  it('asks before throwing written words away, and a second press does not answer', () => {
    const asked = run(picked, { type: 'write', text: 'words' }, { type: 'pressAway' });
    expect(asked.confirmDiscard).toBe(true);
    expect(asked.open?.kind).toBe('draft');
    expect(boxReducer(asked, { type: 'pressAway' })).toMatchObject({ confirmDiscard: true, text: 'words' });
  });

  it('closes an open card, keeping the outline', () => {
    const state = run({ type: 'pinPressed', id: 'c-1' }, { type: 'hover', hover: HOVER }, { type: 'pressAway' });
    expect(state.open).toBeNull();
    expect(state.hover).toBe(HOVER);
  });

  it('does nothing with no box open, so its click picks', () => {
    const state = run();
    expect(pressCloses(state)).toBe(false);
    expect(boxReducer(state, { type: 'pressAway' })).toBe(state);
  });
});

describe('leaving a draft', () => {
  it('lets an empty one go, and the outline with it', () => {
    const state = run({ type: 'hover', hover: HOVER }, picked, { type: 'leave' });
    expect(state.open).toBeNull();
    expect(state.hover).toBeNull();
  });

  it('asks first when there are words, and goes on the second', () => {
    const asked = run(picked, { type: 'write', text: 'words' }, { type: 'leave' });
    expect(asked).toMatchObject({ confirmDiscard: true, text: 'words' });
    const left = boxReducer(asked, { type: 'leave' });
    expect(left).toMatchObject({ open: null, text: '', confirmDiscard: false });
  });

  it('writing again takes the question back', () => {
    const state = run(picked, { type: 'write', text: 'a' }, { type: 'leave' }, { type: 'write', text: 'ab' });
    expect(state.confirmDiscard).toBe(false);
  });
});

describe('a close Radix reports', () => {
  it('closes only the box it names, not one opened since', () => {
    const state = run({ type: 'pinPressed', id: 'c-1' }, { type: 'brought', id: 'c-2' }, {
      type: 'closeBox', of: { kind: 'card', id: 'c-1' }, keepHover: false,
    });
    expect(state.open).toEqual({ kind: 'card', id: 'c-2' });
    expect(state.arrivalId).toBe('c-2');
  });

  it('leaves the words of a draft that replaced the one closed', () => {
    const state = run(picked, { type: 'write', text: 'kept' }, picked, {
      type: 'closeBox', of: { kind: 'draft', serial: 1 }, keepHover: false,
    });
    expect(state.open).toMatchObject({ kind: 'draft', serial: 2 });
    expect(state.text).toBe('kept');
  });

  it('drops the blank left in a draft it closes', () => {
    const state = run(picked, { type: 'write', text: '  ' }, { type: 'closeBox', of: { kind: 'draft', serial: 1 }, keepHover: false });
    expect(state).toMatchObject({ open: null, text: '' });
  });
});

describe('a new document', () => {
  it('carries an open box onto it, and forgets the pin just made', () => {
    const state = run(picked, { type: 'saved', id: 'c-9' }, picked, { type: 'docChanged', doc: '2:a' });
    expect(state).toMatchObject({ doc: '2:a', carried: '2:a', madeId: null });
    expect(state.open?.kind).toBe('draft');
  });

  it('carries nothing when no box is open, and drops the outline', () => {
    const state = run({ type: 'hover', hover: HOVER }, { type: 'docChanged', doc: '2:a' });
    expect(state).toMatchObject({ carried: null, hover: null });
  });

  it('settles a carried box once its pin is in view there', () => {
    const state = run(picked, { type: 'docChanged', doc: '2:a' }, { type: 'placed' });
    expect(state.carried).toBeNull();
  });
});

describe('the list', () => {
  it('takes the open card in when it opens, in a filter that shows it', () => {
    const resolved = comment('c-1', { status: 'resolved' });
    const state = run({ type: 'filter', filter: 'unread' }, { type: 'pinPressed', id: 'c-1' }, {
      type: 'listShown', open: true, comments: [resolved],
    });
    expect(state).toMatchObject({ open: null, expandedId: 'c-1', filter: 'resolved', listOpen: true });
  });

  it('shows a draft waiting there under Open when the filter would hide it', () => {
    const state = run(picked, { type: 'listDraft' }, { type: 'filter', filter: 'resolved' }, { type: 'listShown', open: true, comments: [] });
    expect(state.filter).toBe('open');
  });

  it('lets an empty waiting draft go when it closes, and keeps one with words', () => {
    const empty = run(picked, { type: 'listDraft' }, { type: 'listShown', open: true, comments: [] }, { type: 'listShown', open: false, comments: [] });
    expect(empty.listed).toBeNull();
    const written = run(
      picked,
      { type: 'write', text: 'words' },
      { type: 'listDraft' },
      { type: 'listShown', open: true, comments: [] },
      { type: 'listShown', open: false, comments: [] },
    );
    expect(written.listed?.serial).toBe(1);
  });

  it('takes a draft in with its number and words, and the box closes', () => {
    const state = run({ type: 'filter', filter: 'unread' }, picked, { type: 'write', text: 'words' }, { type: 'listDraft' });
    expect(state).toMatchObject({ open: null, filter: 'open', text: 'words' });
    expect(state.listed?.serial).toBe(1);
  });

  it('keeps the filter for a card the list keeps, and moves it for any other', () => {
    const resolved = comment('c-1', { status: 'resolved' });
    expect(run({ type: 'expand', comment: resolved, kept: true }).filter).toBe('open');
    expect(run({ type: 'expand', comment: resolved, kept: false })).toMatchObject({ filter: 'resolved', expandedId: 'c-1' });
  });

  it('is wanted open for a card expanded while it is shut, until it shows', () => {
    const wanted = run({ type: 'expand', comment: comment('c-1'), kept: false });
    expect(wanted.listWanted).toBe(true);
    expect(boxReducer(wanted, { type: 'listShown', open: true, comments: [] }).listWanted).toBe(false);
  });

  it('is wanted open for a draft taken in while it is shut', () => {
    expect(run(picked, { type: 'listDraft' }).listWanted).toBe(true);
  });

  it('is not wanted when nothing goes to it', () => {
    expect(run({ type: 'listDraft' }).listWanted).toBe(false);
  });

  it('is not wanted again for a card expanded while it shows', () => {
    const state = run({ type: 'listShown', open: true, comments: [] }, { type: 'expand', comment: comment('c-1'), kept: false });
    expect(state.listWanted).toBe(false);
  });

  it('shuts the card of a deleted comment wherever it was open', () => {
    const state = run({ type: 'expand', comment: comment('c-1'), kept: false }, { type: 'deleted' });
    expect(state).toMatchObject({ open: null, expandedId: null });
  });
});

describe('a pin pressed', () => {
  it('opens its card and lets words left in an empty box go', () => {
    const state = run(picked, { type: 'write', text: ' ' }, { type: 'pinPressed', id: 'c-1' });
    expect(state).toMatchObject({ open: { kind: 'card', id: 'c-1' }, text: '' });
  });

  it('leaves the words of a draft waiting in the list', () => {
    const state = run(picked, { type: 'write', text: 'words' }, { type: 'listDraft' }, { type: 'pinPressed', id: 'c-1' });
    expect(state.text).toBe('words');
    expect(state.listed).not.toBeNull();
  });
});

describe('a comment the chat brings forward', () => {
  it('opens at once with its arrival when its pin is in view', () => {
    expect(run({ type: 'brought', id: 'c-1' })).toMatchObject({ open: { kind: 'card', id: 'c-1' }, arrivalId: 'c-1' });
  });

  it('waits for the scroll, then opens with its arrival', () => {
    const waiting = run(picked, { type: 'awaitLanding', id: 'c-1' });
    expect(waiting).toMatchObject({ open: null, landing: { id: 'c-1' } });
    expect(boxReducer(waiting, { type: 'landed', id: 'c-1' })).toMatchObject({
      open: { kind: 'card', id: 'c-1' }, arrivalId: 'c-1', landing: null,
    });
  });

  it('opens nothing when the comment went while it waited', () => {
    const state = run({ type: 'awaitLanding', id: 'c-1' }, { type: 'landed', id: null });
    expect(state).toMatchObject({ open: null, landing: null });
  });

  it('asks the scroll anew each time, so a second ask waits its own time', () => {
    const first = run({ type: 'awaitLanding', id: 'c-1' });
    const second = boxReducer(first, { type: 'awaitLanding', id: 'c-1' });
    expect(second.landing).toEqual(first.landing);
    expect(second.landing).not.toBe(first.landing);
  });
});

describe('a saved draft', () => {
  it('closes, marks the pin it made, and the next comment starts afresh', () => {
    const state = run(
      { type: 'hover', hover: HOVER },
      picked,
      { type: 'write', text: 'words' },
      { type: 'forAgent', on: false },
      { type: 'saved', id: 'c-9' },
    );
    expect(state).toMatchObject({ open: null, madeId: 'c-9', text: '', forAgent: true, to: null, hover: null, listed: null });
  });
});
