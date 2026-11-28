// @vitest-environment node
/**
 * Where a comment on a Drive file goes: the chat that last worked on the file
 * is the default and says so, a new thread is a Chief of Staff chat, and a
 * chat picked from the search joins the menu.
 */
import type { TFunction } from 'i18next';
import { describe, expect, it } from 'vitest';
import type { ArtifactDestinations, ArtifactThread } from '@/types/api';
import { commentDestinations } from '../destinations';

const t = ((key: string) => key) as unknown as TFunction;
const HOME = 'ws-home';

const thread = (id: string, over: Partial<ArtifactThread> = {}): ArtifactThread => ({
  thread_id: id, title: `T ${id}`, workspace_id: 'ws-1', usable: true, home: false, ...over,
});

function driveAnswer(over: Partial<ArtifactDestinations> = {}): ArtifactDestinations {
  return {
    default: { rule: 'worked', thread: thread('th-worked'), workspace_id: HOME },
    bound: null,
    beside: null,
    creator: null,
    worked: thread('th-worked'),
    home_workspace_id: HOME,
    ...over,
  };
}

describe('a Drive file\'s destinations', () => {
  it('defaults to the chat that worked on the file, tagged once, with a new Chief of Staff chat after it', () => {
    const { fallback, choices } = commentDestinations(t, driveAnswer(), null);
    expect(fallback).toEqual({ threadId: 'th-worked' });
    expect(choices.map((c) => [c.key, c.isDefault, c.tag ?? null])).toEqual([
      [expect.stringContaining('th-worked'), true, 'pageViewer:comments.workedTag'],
      [expect.stringContaining(HOME), false, null],
    ]);
    expect(choices[1].label).toBe('pageViewer:comments.newChiefOfStaff');
  });

  it("defaults to the chat beside a Drive tab, with the chat that worked on the file after it, tagged", () => {
    const answer = driveAnswer({ default: { rule: 'beside', thread: thread('th-beside'), workspace_id: HOME }, beside: thread('th-beside') });
    const { fallback, choices } = commentDestinations(t, answer, { threadId: 'th-beside' });
    expect(fallback).toEqual({ threadId: 'th-beside' });
    expect(choices.map((c) => [c.label, c.isDefault, c.tag ?? null]).slice(0, 2)).toEqual([
      ['pageViewer:comments.thisThread', true, null],
      [expect.any(String), false, 'pageViewer:comments.workedTag'],
    ]);
  });

  it('names a new thread a Chief of Staff chat where no chat worked on the file', () => {
    const answer = driveAnswer({ default: { rule: 'new', thread: null, workspace_id: HOME }, worked: null });
    const { fallback, choices, nameOf } = commentDestinations(t, answer, null);
    expect(fallback).toEqual({ newThreadIn: HOME });
    expect(choices).toHaveLength(1);
    expect(choices[0]).toMatchObject({ isDefault: true, label: 'pageViewer:comments.newChiefOfStaff' });
    expect(nameOf({ newThreadIn: HOME })).toBe('pageViewer:comments.newChiefOfStaff');
  });

  it('tags the worked chat beside a default of another rule', () => {
    const answer = driveAnswer({ default: { rule: 'bound', thread: thread('th-bound'), workspace_id: HOME }, bound: thread('th-bound') });
    const { choices } = commentDestinations(t, answer, null);
    const worked = choices.find((c) => c.key.includes('th-worked'));
    const bound = choices.find((c) => c.key.includes('th-bound'));
    expect(bound).toMatchObject({ isDefault: true });
    expect(bound?.tag).toBeUndefined();
    expect(worked).toMatchObject({ isDefault: false, tag: 'pageViewer:comments.workedTag' });
  });

  it('names a new thread by where it would be made, not by the kind of file', () => {
    const inWorkspace = driveAnswer({ default: { rule: 'new', thread: null, workspace_id: 'ws-1' }, worked: null, home_workspace_id: null });
    expect(commentDestinations(t, inWorkspace, null).choices[0].label).toBe('pageViewer:comments.newThread');
    // A file in the owner's Home folder: its new thread is a Chief of Staff chat too.
    const inHome = driveAnswer({ default: { rule: 'new', thread: null, workspace_id: HOME }, worked: null, home_workspace_id: HOME });
    expect(commentDestinations(t, inHome, null).choices[0].label).toBe('pageViewer:comments.newChiefOfStaff');
  });
});
