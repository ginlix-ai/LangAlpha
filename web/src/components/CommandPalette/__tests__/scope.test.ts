// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { PREFIXES } from '../prefixes';
import { back, canEnter, enterPlace, leave, openLens, ROOT, scopeOf, searchWithin, typeInto, typeText, type Frame } from '../scope';
import type { Kind, Lens, Place, ViewSpec } from '../types';

const semis: Place = { kind: 'workspace', workspaceId: 'ws1', name: 'Semis' };
const macro: Place = { kind: 'workspace', workspaceId: 'ws2', name: 'Macro' };
const margins: Place = { kind: 'thread', threadId: 't1', workspaceId: 'ws1', title: 'NVDA margins' };
const elsewhere: Place = { kind: 'thread', threadId: 't9', workspaceId: 'ws2', title: 'Rates' };
const prefix = (kind: Kind, place: Place | null = null): ViewSpec => ({ place, lens: { kind, via: 'prefix' } });
const drill = (kind: Kind, place: Place | null = null): ViewSpec => ({ place, lens: { kind, via: 'drill' } });
const at = (place: Place): ViewSpec => ({ place, lens: null });
const frame = (spec: ViewSpec, query = '', chosenId: string | null = null): Frame => ({ spec, query, chosenId, lead: null });
const root = (query = '', chosenId: string | null = null) => frame(ROOT, query, chosenId);
const specs = (frames: readonly Frame[]) => frames.map((entry) => entry.spec);
const tickers: Lens = { kind: 'ticker', symbol: 'NVDA', via: 'drill' };

describe('typeInto', () => {
  it('turns a leading $ at the top level into the tickers chip, the rest the query', () => {
    expect(typeInto(ROOT, '$NVDA')).toEqual({ enter: 'tickers', text: 'NVDA' });
    expect(typeInto(ROOT, '$')).toEqual({ enter: 'tickers', text: '' });
    expect(typeInto(ROOT, '$ nvda')).toEqual({ enter: 'tickers', text: 'nvda' });
  });

  it('leaves a $ anywhere but the start, or typed in a drilled-in view, as text', () => {
    expect(typeInto(ROOT, 'buy $NVDA')).toEqual({ enter: null, text: 'buy $NVDA' });
    expect(typeInto(drill('threads'), '$NVDA')).toEqual({ enter: null, text: '$NVDA' });
  });

  it('drops a repeated $ in the tickers scope, and keeps it as text in a place', () => {
    expect(typeInto(prefix('tickers'), '$AMD')).toEqual({ enter: null, text: 'AMD' });
    expect(typeInto(at(semis), '$AMD')).toEqual({ enter: null, text: '$AMD' });
  });
});

describe('typeInto, for every prefix', () => {
  const kinds = Object.keys(PREFIXES) as Kind[];

  it('makes each prefix typed first at the top level its chip, alone or with words, typed or pasted', () => {
    for (const kind of kinds) {
      const char = PREFIXES[kind];
      expect(typeInto(ROOT, char)).toEqual({ enter: kind, text: '' });
      expect(typeInto(ROOT, `${char}nvda margins`)).toEqual({ enter: kind, text: 'nvda margins' });
      expect(typeInto(ROOT, `  ${char} nvda`)).toEqual({ enter: kind, text: 'nvda' });
      expect(typeInto(ROOT, `${char}${char}nvda`)).toEqual({ enter: kind, text: 'nvda' });
    }
  });

  it('keeps a prefix after the first word as text, and leading spaces with no prefix as typed', () => {
    for (const char of Object.values(PREFIXES)) expect(typeInto(ROOT, `nvda ${char}x`)).toEqual({ enter: null, text: `nvda ${char}x` });
    expect(typeInto(ROOT, '  nvda')).toEqual({ enter: null, text: '  nvda' });
  });

  it('drops the closing quote of a quoted phrase pasted whole', () => {
    expect(typeInto(ROOT, '"send to both"')).toEqual({ enter: 'turns', text: 'send to both' });
    expect(typeInto(prefix('turns'), 'send"')).toEqual({ enter: null, text: 'send"' });
  });

  it('narrows a workspace by what it holds, and keeps the other prefixes as text there', () => {
    expect(typeInto(at(semis), '/deck')).toEqual({ enter: 'files', text: 'deck' });
    expect(typeInto(at(semis), '#')).toEqual({ enter: 'threads', text: '' });
    expect(typeInto(at(semis), '"capex')).toEqual({ enter: 'turns', text: 'capex' });
    for (const text of ['$NVDA', '@macro', '>theme']) expect(typeInto(at(semis), text)).toEqual({ enter: null, text });
  });

  it('keeps every prefix as text in a thread, in another kind, and in a drilled-in view', () => {
    expect(typeInto(at(margins), '/x')).toEqual({ enter: null, text: '/x' });
    expect(typeInto(prefix('files'), '#x')).toEqual({ enter: null, text: '#x' });
    expect(typeInto(prefix('files'), '/x')).toEqual({ enter: null, text: 'x' });
    expect(typeInto(drill('files'), '/x')).toEqual({ enter: null, text: '/x' });
  });
});

describe('typeText', () => {
  it('opens a prefix’s view over the one it was typed in, which keeps its own field', () => {
    const frames = typeText([root('nv', 'a')], '$nv');
    expect(specs(frames)).toEqual([ROOT, prefix('tickers')]);
    expect(frames[0]).toEqual(root('nv', 'a'));
    expect(frames[1].query).toBe('nv');
  });

  it('otherwise edits the top field and lets the selection go', () => {
    expect(typeText([root('nv', 'a')], 'nvda')).toEqual([root('nvda')]);
  });
});

describe('a kind over a workspace', () => {
  it('stacks over the workspace it narrows, and leaving it returns there', () => {
    const frames = typeText([root('semis'), frame(at(semis), 'x')], '/');
    expect(specs(frames)).toEqual([ROOT, at(semis), prefix('files', semis)]);
    expect(leave(frames)).toEqual([root('semis'), frame(at(semis), 'x')]);
    expect(scopeOf(frames)).toEqual(prefix('files', semis));
  });

  it('gives way to a row entered from it: the thread over its workspace, which leaving returns to', () => {
    const frames = enterPlace([root(), frame(at(semis)), frame(prefix('threads', semis), 'nv')], margins);
    expect(specs(frames)).toEqual([ROOT, at(semis), at(margins)]);
    expect(specs(enterPlace([root(), frame(prefix('threads'))], margins))).toEqual([ROOT, at(margins)]);
    expect(specs(enterPlace([root(), frame(prefix('workspaces'))], macro))).toEqual([ROOT, at(macro)]);
    expect(canEnter([root(), frame(at(semis)), frame(prefix('threads', semis))], elsewhere)).toBe(true);
  });
});

describe('enterPlace', () => {
  it('pushes a first place over the top level, which keeps what it held', () => {
    const frames = enterPlace([root('semis', 'workspace:ws1')], semis);
    expect(frames).toEqual([root('semis', 'workspace:ws1'), frame(at(semis))]);
  });

  it('stacks a thread over its own workspace, so leaving it returns there', () => {
    const frames = enterPlace([root(), frame(at(semis), 'send')], margins);
    expect(specs(frames)).toEqual([ROOT, at(semis), at(margins)]);
    expect(leave(frames)).toEqual([root(), frame(at(semis), 'send')]);
  });

  it('replaces any other place, keeping where the first was entered from', () => {
    expect(enterPlace([root('a'), frame(at(semis))], macro)).toEqual([root('a'), frame(at(macro))]);
    expect(enterPlace([root('a'), frame(at(semis))], elsewhere)).toEqual([root('a'), frame(at(elsewhere))]);
    expect(enterPlace([root('a'), frame(at(semis)), frame(at(margins))], elsewhere)).toEqual([root('a'), frame(at(elsewhere))]);
  });

  it('keeps a drill-in under a thread entered from it, so leaving the thread returns to it', () => {
    const frames = enterPlace([root('nv'), frame(drill('threads'), 'nv')], margins);
    expect(specs(frames)).toEqual([ROOT, drill('threads'), at(margins)]);
    expect(specs(enterPlace([root(), frame(at(semis)), frame(drill('turns', semis))], margins))).toEqual([
      ROOT,
      at(semis),
      drill('turns', semis),
      at(margins),
    ]);
  });

  it('changes nothing for the place already entered', () => {
    const frames = [root(), frame(at(semis)), frame(at(margins))];
    expect(enterPlace(frames, { ...margins })).toBe(frames);
    expect(canEnter(frames, margins)).toBe(false);
    expect(canEnter(frames, undefined)).toBe(false);
    expect(canEnter([root(), frame(at(semis))], margins)).toBe(true);
  });
});

describe('openLens and back', () => {
  it('opens a kind with the field’s words, in the same place', () => {
    const frames = openLens([root(), frame(at(semis), 'nv')], { kind: 'files', via: 'drill' });
    expect(frames.at(-1)).toEqual(frame(drill('files', semis), 'nv'));
  });

  it('brings the results back as they were for the same words, or for the words typed since', () => {
    const opened = openLens([root('nv', 'thread:a')], { kind: 'threads', via: 'drill' });
    expect(back(opened)).toEqual([root('nv', 'thread:a')]);
    const edited = [opened[0], { ...opened[1], query: 'nvda' }];
    expect(back(edited)).toEqual([root('nvda')]);
  });

  it('opens a ticker’s view empty, and puts the field back as it was on the way out', () => {
    const opened = openLens([root('nv', 'ticker:NVDA')], tickers);
    expect(opened[1].query).toBe('');
    expect(back([opened[0], { ...opened[1], query: 'margins' }])).toEqual([root('nv', 'ticker:NVDA')]);
  });

  it('leaves a scope and any drill-in over it at once, as its chip’s ×', () => {
    const frames = [root('x'), frame(at(semis)), frame(drill('threads', semis))];
    expect(leave(frames)).toEqual([root('x')]);
    expect(scopeOf(frames)).toEqual(at(semis));
    expect(leave([root(), frame(drill('threads'))])).toBeNull();
    expect(back([root()])).toBeNull();
  });
});

describe('searchWithin', () => {
  it('limits the server search to the place', () => {
    expect(searchWithin(semis)).toEqual({ workspaceId: 'ws1' });
    expect(searchWithin(margins)).toEqual({ threadId: 't1' });
    expect(searchWithin(null)).toEqual({});
  });
});
