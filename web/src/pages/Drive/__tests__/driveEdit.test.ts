// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { editReducer, isDirty, isPast, VIEWING, type EditAction, type EditState, type SaveRequest } from '../driveEdit';

const base = { sha: 'a'.repeat(64), content: 'one\n' };
const theirs = { sha: 'b'.repeat(64), content: 'theirs\n' };
const open = (sitting = 'web-1', path = 'a.md'): EditAction => ({ type: 'open', path, sitting, modelPath: `m-${sitting}`, base });
const req = (text: string, ifMatch = base.sha, sitting = 'web-1', path = 'a.md'): SaveRequest => ({ sitting, path, text, ifMatch });

function run(...actions: EditAction[]): EditState {
  return actions.reduce(editReducer, VIEWING);
}

function editing(state: EditState) {
  if (state.mode !== 'editing') throw new Error('not editing');
  return state;
}

describe('a Drive editing session', () => {
  it('keeps one sitting and one editor model across every save, and saves over the last one each time', () => {
    const first = req('one\ntwo\n');
    const second = req('one\ntwo\nthree\n', 'c'.repeat(64));
    const s = run(
      open(),
      { type: 'change', draft: first.text },
      { type: 'saving', req: first },
      { type: 'saved', req: first, sha: 'c'.repeat(64), at: 1 },
      { type: 'change', draft: second.text },
      { type: 'saving', req: second },
      { type: 'retrying', req: second },
      { type: 'saved', req: second, sha: 'd'.repeat(64), at: 2 },
    );
    const e = editing(s);
    expect(e.sitting).toBe('web-1');
    expect(e.modelPath).toBe('m-web-1');
    expect(e.base).toEqual({ sha: 'd'.repeat(64), content: second.text });
    // The hash it just wrote over is behind it, not news: the page catches up a render later.
    expect(isPast(e, 'c'.repeat(64))).toBe(true);
    expect(isPast(e, base.sha)).toBe(false);
    expect(e.save).toBe('idle');
    expect(isDirty(s)).toBe(false);
  });

  it('folds a busy server into the save it is waiting on', () => {
    const r = req('x');
    expect(editing(run(open(), { type: 'saving', req: r })).save).toBe('saving');
    expect(editing(run(open(), { type: 'saving', req: r }, { type: 'retrying', req: r })).save).toBe('retrying');
    expect(editing(run(open(), { type: 'retrying', req: r }, { type: 'save-failed', req: r })).save).toBe('idle');
  });

  it('keeps the draft when the file changed underneath', () => {
    const r = req('mine\n');
    const e = editing(run(open(), { type: 'change', draft: r.text }, { type: 'saving', req: r }, { type: 'conflict', req: r }));
    expect(e.draft).toBe('mine\n');
    expect(e.conflict).toBe(true);
    expect(e.save).toBe('idle');
    expect(e.sitting).toBe('web-1');
  });

  it('keeping mine saves over their version: the draft stays and their hash becomes the If-Match', () => {
    const refused = req('mine\n');
    const again = req('mine\n', theirs.sha);
    const e = editing(run(
      open(),
      { type: 'change', draft: 'mine\n' },
      { type: 'saving', req: refused },
      { type: 'conflict', req: refused },
      { type: 'keep-mine', theirs, req: again },
    ));
    expect(e).toMatchObject({ draft: 'mine\n', base: theirs, conflict: false, save: 'saving' });
    const saved = editing(editReducer(e, { type: 'saved', req: again, sha: 'c'.repeat(64), at: 1 }));
    expect(saved).toMatchObject({ base: { sha: 'c'.repeat(64), content: 'mine\n' }, replaced: theirs.sha, save: 'idle' });
  });

  it('taking theirs drops the draft for their content and edits on from it', () => {
    const r = req('mine\n');
    const s = run(open(), { type: 'change', draft: r.text }, { type: 'conflict', req: r }, { type: 'take-theirs', theirs });
    const e = editing(s);
    expect(e.draft).toBe('theirs\n');
    expect(e.base).toEqual(theirs);
    expect(isDirty(s)).toBe(false);
  });

  it('follows a change underneath only while nothing is unsaved', () => {
    const r = req('one\n');
    const followed = editing(run(
      open(),
      { type: 'saving', req: r },
      { type: 'saved', req: r, sha: 'c'.repeat(64), at: 1 },
      { type: 'rebase', base: theirs },
    ));
    expect(followed.draft).toBe('theirs\n');
    // Someone else's text: the person's earlier save no longer speaks for it.
    expect(followed.savedAt).toBeNull();
    const dirty = editing(run(open(), { type: 'change', draft: 'mine\n' }, { type: 'rebase', base: theirs }));
    expect(dirty.draft).toBe('mine\n');
    expect(dirty.base).toEqual(base);
  });

  it('starts a new sitting each time the editor opens', () => {
    expect(editing(run(open('web-1'), { type: 'close' }, open('web-2'))).sitting).toBe('web-2');
  });

  it('ignores every step of a save sent by another session', () => {
    const old = req('old text\n');
    // Closed and reopened: a new sitting on the same file.
    const reopened = run(open('web-1'), { type: 'saving', req: old }, { type: 'close' }, open('web-2'));
    // Moved to another file under the same sitting id would still not match.
    const moved = run(open('web-1'), { type: 'saving', req: old }, open('web-1', 'b.md'));
    for (const s of [reopened, moved]) {
      for (const late of [
        { type: 'saved', req: old, sha: 'c'.repeat(64), at: 1 },
        { type: 'conflict', req: old },
        { type: 'save-failed', req: old },
        { type: 'retrying', req: old },
        { type: 'keep-mine', theirs, req: old },
      ] satisfies EditAction[]) {
        expect(editReducer(s, late)).toBe(s);
      }
    }
    expect(editing(reopened)).toMatchObject({ base, save: 'idle', conflict: false, savedAt: null });
  });

  describe('what changed, beside Save', () => {
    it('starts empty, or with the note a kept draft comes back with', () => {
      expect(editing(run(open())).note).toBe('');
      expect(editing(run({ ...open(), note: 'trimmed the risks' } as EditAction)).note).toBe('trimmed the risks');
    });

    it('goes with the save that carried it; one typed meanwhile stays for the next', () => {
      const first = { ...req('two\n'), note: 'cut the intro' };
      const carried = run(open(), { type: 'note', note: ' cut the intro ' }, { type: 'saving', req: first }, { type: 'saved', req: first, sha: 'c'.repeat(64), at: 1 });
      expect(editing(carried).note).toBe('');
      const retyped = run(open(), { type: 'note', note: 'cut the intro' }, { type: 'saving', req: first }, { type: 'note', note: 'and the outro' }, { type: 'saved', req: first, sha: 'c'.repeat(64), at: 1 });
      expect(editing(retyped).note).toBe('and the outro');
    });

    it('is dropped with the edits when theirs is taken', () => {
      const s = run(open(), { type: 'change', draft: 'mine\n' }, { type: 'note', note: 'mine' }, { type: 'take-theirs', theirs });
      expect(editing(s).note).toBe('');
    });

    it("is left alone by another session's save", () => {
      const other = { ...req('x', base.sha, 'web-2'), note: 'theirs' };
      const s = run(open(), { type: 'note', note: 'theirs' }, { type: 'saved', req: other, sha: 'c'.repeat(64), at: 1 });
      expect(editing(s).note).toBe('theirs');
    });
  });
});
