// @vitest-environment node
/**
 * Every rule for where a file comment goes, one row each. Each control that
 * sends one (the comment box and its "To:" menu, a card's ⋯ menu and reply
 * box, Send now, Send all, the header's bind, the docked composer) reads
 * `commentDestinations`, so the rows here are what they all agree on.
 */
import type { TFunction } from 'i18next';
import { describe, expect, it } from 'vitest';
import type { ArtifactDestinationRule, ArtifactDestinations, ArtifactThread } from '@/types/api';
import { commentDestinations, defaultsTo, type CommentIntent, type CommentRoute } from '../destinations';
import type { CommentTarget } from '../../../utils/api/artifacts';

const t = ((key: string) => key) as unknown as TFunction;

const BESIDE = 'th-beside';
const thread = (id: string, over: Partial<ArtifactThread> = {}): ArtifactThread => ({
  thread_id: id, title: `T ${id}`, workspace_id: 'ws-1', usable: true, home: false, ...over,
});

/** The server's answer: `to` is the default thread, or null for a new one. */
function answer(rule: ArtifactDestinationRule, to: string | null, over: Partial<ArtifactDestinations> = {}): ArtifactDestinations {
  return {
    default: { rule, thread: to ? thread(to) : null, workspace_id: 'ws-1' },
    bound: null,
    beside: thread(BESIDE),
    creator: null,
    ...over,
  };
}

const NEW: CommentTarget = { newThreadIn: 'ws-1' };
const at = (id: string): CommentTarget => ({ threadId: id });
const send = (to: CommentTarget): CommentRoute => ({ kind: 'send', to });
const queue = (threadId: string): CommentRoute => ({ kind: 'queue', threadId });

describe('the default destination', () => {
  it.each<[string, ArtifactDestinations | undefined, CommentTarget | null]>([
    ['the server\'s default thread (bound, beside or creator, by the setting)', answer('beside', BESIDE), at(BESIDE)],
    ['a bound file goes to its bound thread whatever the setting', answer('bound', 'th-bound', { bound: thread('th-bound') }), at('th-bound')],
    ['"A new thread" (or no thread to take it) is a new thread in the file\'s workspace', answer('new', null), NEW],
    ['nothing while the server has not answered for the record', undefined, null],
  ])('%s', (_rule, destinations, fallback) => {
    expect(commentDestinations(t, destinations, BESIDE).fallback).toEqual(fallback);
  });
});

describe('where each intent takes the words', () => {
  const noThread = { thread_id: null, thread_archived: false };
  const waiting = { thread_id: 'th-waits', thread_archived: false };
  const went = { thread_id: 'th-went', thread_archived: false };
  const archived = { thread_id: 'th-gone', thread_archived: true };
  it.each<[string, ArtifactDestinations | undefined, { thread_id: string | null; thread_archived: boolean } | null, CommentIntent, CommentTarget | null, CommentRoute | null]>([
    ['a note is kept, whatever is known', undefined, null, 'note', null, { kind: 'note' }],
    ['a new comment sends to the default', answer('beside', BESIDE), null, 'send', null, send(at(BESIDE))],
    ['a new comment sends where the "To:" menu picked', answer('beside', BESIDE), null, 'send', at('th-other'), send(at('th-other'))],
    ['a new comment queues for the default thread', answer('creator', 'th-maker'), null, 'queue', null, queue('th-maker')],
    ['a new comment cannot queue for a new thread: "A new thread" always sends', answer('new', null), null, 'queue', null, null],
    ['a new comment picked to a new thread cannot queue', answer('beside', BESIDE), null, 'queue', NEW, null],
    ['a new comment sends to a new thread', answer('new', null), null, 'send', null, send(NEW)],
    ['Send now on a queued comment goes to the thread it waits for', answer('beside', BESIDE), waiting, 'send', null, send(at('th-waits'))],
    ['Send now on a note, which waits for no thread, goes to the default', answer('creator', 'th-maker'), noThread, 'send', null, send(at('th-maker'))],
    ['Send now on a queued comment whose thread is gone goes to the default', answer('new', null), noThread, 'send', null, send(NEW)],
    ['a reply sent goes to the thread the comment went to', answer('beside', BESIDE), went, 'send', null, send(at('th-went'))],
    ['a reply queued waits for the thread the comment went to', answer('new', null), went, 'queue', null, queue('th-went')],
    ['Send now on a comment whose thread was archived goes to the default', answer('beside', BESIDE), archived, 'send', null, send(at(BESIDE))],
    ['Add to queue on a comment whose thread was archived waits for the default', answer('beside', BESIDE), archived, 'queue', null, queue(BESIDE)],
    ['Add to queue on a note waits for the default thread', answer('beside', BESIDE), noThread, 'queue', null, queue(BESIDE)],
    ['Add to queue is not offered where the default is a new thread', answer('new', null), noThread, 'queue', null, null],
    ['a comment\'s own thread needs no answer from the server', undefined, waiting, 'send', null, send(at('th-waits'))],
    ['nothing that needs the default goes while it is unknown', undefined, noThread, 'send', null, null],
    ['a new comment does not go while the default is unknown', undefined, null, 'queue', null, null],
  ])('%s', (_rule, destinations, comment, intent, picked, route) => {
    expect(commentDestinations(t, destinations, BESIDE).targetFor(comment, intent, picked)).toEqual(route);
  });
});

describe('binding the file to the thread beside', () => {
  it.each<[string, ArtifactDestinations | undefined, string | null, boolean]>([
    ['is offered where the thread beside is usable and the file is not bound to it', answer('creator', 'th-maker'), BESIDE, true],
    ['is offered over another binding', answer('bound', 'th-bound', { bound: thread('th-bound') }), BESIDE, true],
    ['is not offered where the file is bound to it already', answer('bound', BESIDE, { bound: thread(BESIDE) }), BESIDE, false],
    ['is not offered where the server says the thread beside is not usable', answer('new', null, { beside: thread(BESIDE, { usable: false }) }), BESIDE, false],
    ['is not offered where the server names no thread beside (not the owner\'s, or gone)', answer('new', null, { beside: null }), BESIDE, false],
    ['is not offered with no chat beside', answer('new', null, { beside: null }), null, false],
    ['is not offered while the server has not answered', undefined, BESIDE, false],
  ])('%s', (_rule, destinations, beside, canBind) => {
    expect(commentDestinations(t, destinations, beside).canBind).toBe(canBind);
  });
});

describe('the "To:" menu', () => {
  it('lists the default first, then the thread beside and the bound thread, once each and only where usable', () => {
    const { choices } = commentDestinations(t, answer('creator', 'th-maker', {
      bound: thread('th-gone', { usable: false }),
    }), BESIDE);
    expect(choices.map((c) => [c.key, c.isDefault, c.label])).toEqual([
      ['thread:th-maker', true, 'T th-maker'],
      [`thread:${BESIDE}`, false, 'pageViewer.comments.thisThread'],
    ]);
  });

  it('offers a new thread as the default where the setting sends there, and the thread beside after it', () => {
    const { choices } = commentDestinations(t, answer('new', null), BESIDE);
    expect(choices.map((c) => [c.key, c.isDefault])).toEqual([['new', true], [`thread:${BESIDE}`, false]]);
  });

  it('offers nothing while the server has not answered', () => {
    expect(commentDestinations(t, undefined, BESIDE).choices).toEqual([]);
  });
});

describe('the composer docking under the file', () => {
  it.each<[string, CommentTarget | null, string | undefined, boolean]>([
    ['docks where the file\'s comments go to the chat\'s thread', at(BESIDE), BESIDE, true],
    ['stays in the chat where they go to another thread (bound, or their maker\'s)', at('th-maker'), BESIDE, false],
    ['stays in the chat where they start a new thread', NEW, BESIDE, false],
    ['stays in the chat with no thread yet', at(BESIDE), undefined, false],
  ])('%s', (_rule, fallback, chatThread, docks) => {
    expect(defaultsTo(fallback, chatThread)).toBe(docks);
  });
});
